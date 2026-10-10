//! The retry loop that writes one epoch's markers into every target partition.
//!
//! [`MarkerFanout`] reads a fresh metadata image per attempt, groups the
//! partitions that still carry no marker by their leader, appends locally or
//! through the transport seam, and repeats until every target is marked or the
//! deadline passes. The offsets it collects are the cut.

use std::{collections::BTreeMap, sync::Arc};

use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_units::{Time, convert::TimeExt as _};
use krabka_verified::{BarrierPlacementDecision, barrier_placement_decision};
use tokio::time::Instant;
use tracing::warn;

use super::{RemoteMarkerWriter, append_marker, backoff_for, group_by_leader};
use crate::{
    barrier::{
        config::BarrierConfig, marker::BarrierMarker, metrics::BrokerBarrierMetrics,
        state::TargetPartition,
    },
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
};

/// The leader of `target` in `image`.
fn leader_in(image: &MetadataImage, target: &TargetPartition) -> Option<NodeId> {
    image
        .partition(&target.topic, target.partition.get())
        .map(|p| p.leader)
}

/// Everything one marker fan-out needs.
pub(crate) struct MarkerFanout<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) partitions: &'a PartitionRegistry,
    pub(crate) controller: &'a Arc<dyn MetadataSource>,
    pub(crate) remote: Option<&'a Arc<dyn RemoteMarkerWriter>>,
    pub(crate) metrics: &'a BrokerBarrierMetrics,
    pub(crate) config: &'a BarrierConfig,
}

impl MarkerFanout<'_> {
    /// Write `marker` into every partition of `targets`, and collect the
    /// offsets.
    ///
    /// The function reads a fresh metadata image per attempt, so a leader that
    /// moves between two attempts still gets the marker. It returns when every
    /// target carries a marker, or when `timeout` passes. The caller publishes
    /// a partial cut for whatever is absent from the result.
    ///
    /// The caller supplies `timeout` rather than the fan-out reading it from
    /// the config, because a `TriggerBarrier` request carries its own bound.
    pub(crate) async fn run(
        &self,
        marker: &BarrierMarker,
        targets: Vec<TargetPartition>,
        timeout: Time,
    ) -> BTreeMap<TargetPartition, Offset> {
        let deadline = Instant::now() + timeout.to_std();
        let mut pending = targets;
        let mut placed: BTreeMap<TargetPartition, Offset> = BTreeMap::new();
        let mut attempt: u32 = 0;

        while !pending.is_empty() {
            let image = self.controller.current_image();
            let plan = group_by_leader(&pending, |target| leader_in(&image, target));
            for (leader, group) in plan {
                match leader {
                    None => {}
                    Some(leader) if leader == self.node_id => {
                        self.place_local(marker, &image, &group, &mut placed).await;
                    }
                    Some(leader) => {
                        self.place_remote(leader, marker, &group, &mut placed).await;
                    }
                }
            }
            pending.retain(|target| !placed.contains_key(target));
            if pending.is_empty() {
                break;
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = backoff_for(
                attempt,
                self.config.retry_backoff,
                self.config.retry_backoff_max,
            )
            .to_std()
            .min(deadline - now);
            tokio::time::sleep(wait).await;
            attempt = attempt.saturating_add(1);
        }
        placed
    }

    /// Append the marker into every partition this broker leads.
    async fn place_local(
        &self,
        marker: &BarrierMarker,
        image: &MetadataImage,
        targets: &[TargetPartition],
        placed: &mut BTreeMap<TargetPartition, Offset>,
    ) {
        for target in targets {
            if placed.contains_key(target) {
                continue;
            }
            let Some(metadata) = image.partition(&target.topic, target.partition.get()) else {
                continue;
            };
            let Some(partition) = self.partitions.get(&target.topic, target.partition) else {
                warn!(
                    topic = target.topic,
                    partition = target.partition.get(),
                    "barrier target is led locally but is not open here"
                );
                continue;
            };
            match append_marker(
                &partition,
                marker,
                self.node_id,
                metadata.leader_epoch.get(),
            )
            .await
            {
                Ok(offset) => {
                    self.metrics.marker_written(&target.topic);
                    placed.insert(target.clone(), offset);
                }
                Err(error) => {
                    warn!(
                        topic = target.topic,
                        partition = target.partition.get(),
                        %error,
                        "barrier marker append failed"
                    );
                }
            }
        }
    }

    /// Ask `leader` to append the marker into every partition it leads.
    async fn place_remote(
        &self,
        leader: NodeId,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
        placed: &mut BTreeMap<TargetPartition, Offset>,
    ) {
        let Some(remote) = self.remote else {
            warn!(
                %leader,
                count = targets.len(),
                "no barrier marker transport; the remote partitions stay unmarked"
            );
            return;
        };
        match remote.write_markers(leader, marker, targets).await {
            Ok(placements) => {
                for placement in placements {
                    let requested = targets.contains(&placement.target)
                        && !placed.contains_key(&placement.target);
                    if barrier_placement_decision(requested, placement.offset.get())
                        == BarrierPlacementDecision::Accept
                    {
                        self.metrics.marker_written(&placement.target.topic);
                        placed.insert(placement.target, placement.offset);
                    } else {
                        warn!(
                            topic = placement.target.topic,
                            partition = placement.target.partition.get(),
                            offset = placement.offset.get(),
                            "ignored an invalid barrier marker placement"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(%leader, %error, "barrier marker request to the leader failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_log::Offset;
    use krabka_metadata::NodeId;
    use krabka_units::{millis, secs};
    use tempfile::tempdir;

    use super::{MarkerFanout, RemoteMarkerWriter};
    use crate::{
        barrier::{
            config::BarrierConfig,
            injection::{
                MarkerPlacement, MockRemoteMarkerWriter,
                test_support::{at, fast_config, marker, source},
            },
            metrics::BrokerBarrierMetrics,
            test_support::{open_partition, topic_records},
        },
        error::BrokerError,
        metrics::BrokerMetrics,
        partition_registry::PartitionRegistry,
    };

    fn test_fanout<'a>(
        partitions: &'a PartitionRegistry,
        controller: &'a Arc<dyn crate::metadata_source::MetadataSource>,
        remote: Option<&'a Arc<dyn RemoteMarkerWriter>>,
        metrics: &'a BrokerBarrierMetrics,
        config: &'a BarrierConfig,
    ) -> MarkerFanout<'a> {
        MarkerFanout {
            node_id: NodeId(1),
            partitions,
            controller,
            remote,
            metrics,
            config,
        }
    }

    async fn place_local(
        registry: &PartitionRegistry,
    ) -> BTreeMap<crate::barrier::state::TargetPartition, Offset> {
        let controller = source(&topic_records("orders", 2, NodeId(1)));
        let metrics = BrokerBarrierMetrics::new(BrokerMetrics::new());
        let config = fast_config();
        test_fanout(registry, &controller, None, &metrics, &config)
            .run(
                &marker(),
                vec![at("orders", 0), at("orders", 1)],
                config.injection_timeout,
            )
            .await
    }

    async fn place_one(
        fanout: MarkerFanout<'_>,
    ) -> BTreeMap<crate::barrier::state::TargetPartition, Offset> {
        fanout
            .run(
                &marker(),
                vec![at("orders", 0)],
                fanout.config.injection_timeout,
            )
            .await
    }

    async fn check_remote_placement(
        registry: &PartitionRegistry,
        controller: &Arc<dyn crate::metadata_source::MetadataSource>,
        remote: MockRemoteMarkerWriter,
        metrics: &BrokerBarrierMetrics,
        config: &BarrierConfig,
        offset: Offset,
    ) {
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);
        let fanout = test_fanout(registry, controller, Some(&remote), metrics, config);
        let placed = place_one(fanout).await;
        assert!(placed == maplit::btreemap! {at("orders", 0) => offset});
    }

    macro_rules! remote_fixture {
        ($registry:ident, $controller:ident, $metrics:ident, $config:ident = $configuration:expr) => {
            let $registry = PartitionRegistry::new();
            let $controller = source(&topic_records("orders", 1, NodeId(2)));
            let $metrics = BrokerBarrierMetrics::new(BrokerMetrics::new());
            let $config = $configuration;
        };
    }

    fn placements(
        targets: &[crate::barrier::state::TargetPartition],
        offset: Offset,
    ) -> Vec<MarkerPlacement> {
        targets
            .iter()
            .map(|target| MarkerPlacement {
                target: target.clone(),
                offset,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_local_fan_out_marks_every_partition_and_returns_its_offset() {
        let dir = tempdir().expect("tempdir");
        let registry = PartitionRegistry::new();
        for p in 0..2 {
            open_partition(
                &registry,
                dir.path(),
                crate::test_support::StandalonePartitionSetup {
                    partition: krabka_ids::PartitionIndex(p),
                    ..Default::default()
                },
            );
            registry
                .get("orders", PartitionIndex(p))
                .expect("the partition is open")
                .install_leader_change(NodeId(1).get(), 3)
                .await;
        }
        let placed = place_local(&registry).await;
        assert!(placed.len() == 2);
        assert!(placed[&at("orders", 0)] == Offset(0));
        assert!(placed[&at("orders", 1)] == Offset(0));

        // The record at the returned offset is this epoch's marker, and the
        // batch carries the partition's leader epoch rather than a zero.
        for p in 0..2 {
            let partition = registry
                .get("orders", PartitionIndex(p))
                .expect("the partition is open");
            let read = partition
                .read_log(Offset(0), krabka_units::mebibytes(1))
                .expect("read the log back");
            let batch = &read.batches[0];
            crate::barrier::test_support::check_control_marker(batch, &marker());
        }
    }

    #[tokio::test]
    async fn a_partition_that_is_not_open_locally_stays_unmarked() {
        let dir = tempdir().expect("tempdir");
        let registry = PartitionRegistry::new();
        open_partition(
            &registry,
            dir.path(),
            crate::test_support::StandalonePartitionSetup::default(),
        );
        registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open")
            .install_leader_change(NodeId(1).get(), 3)
            .await;
        let placed = place_local(&registry).await;
        assert!(placed.keys().cloned().collect::<Vec<_>>() == vec![at("orders", 0)]);
    }

    #[tokio::test]
    async fn a_remote_partition_goes_through_the_transport_seam() {
        remote_fixture!(registry, controller, metrics, config = fast_config());

        let mut remote = MockRemoteMarkerWriter::new();
        remote
            .expect_write_markers()
            .times(1)
            .returning(|leader, _marker, targets| {
                assert!(leader == NodeId(2));
                Ok(placements(targets, Offset(77)))
            });
        check_remote_placement(
            &registry,
            &controller,
            remote,
            &metrics,
            &config,
            Offset(77),
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_remote_placements_do_not_enter_the_cut() {
        remote_fixture!(registry, controller, metrics, config = fast_config());

        let mut remote = MockRemoteMarkerWriter::new();
        remote
            .expect_write_markers()
            .times(1)
            .returning(|_leader, _marker, _targets| {
                Ok(vec![
                    MarkerPlacement {
                        target: at("orders", 0),
                        offset: Offset(-1),
                    },
                    MarkerPlacement {
                        target: at("unrequested", 0),
                        offset: Offset(9),
                    },
                ])
            });
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);
        let fanout = test_fanout(&registry, &controller, Some(&remote), &metrics, &config);
        let mut placed = BTreeMap::new();

        fanout
            .place_remote(NodeId(2), &marker(), &[at("orders", 0)], &mut placed)
            .await;

        assert!(placed.is_empty());
    }

    #[tokio::test]
    async fn the_fan_out_retries_a_leader_that_failed_once() {
        remote_fixture!(
            registry,
            controller,
            metrics,
            config = BarrierConfig {
                injection_timeout: secs(30),
                retry_backoff: millis(1),
                retry_backoff_max: millis(1),
                ..BarrierConfig::default()
            }
        );

        let mut remote = MockRemoteMarkerWriter::new();
        let mut calls = 0;
        remote
            .expect_write_markers()
            .times(2)
            .returning(move |_leader, _marker, targets| {
                calls += 1;
                if calls == 1 {
                    return Err(BrokerError::Replication("leader is mid-election".into()));
                }
                Ok(placements(targets, Offset(12)))
            });
        check_remote_placement(
            &registry,
            &controller,
            remote,
            &metrics,
            &config,
            Offset(12),
        )
        .await;
    }

    #[tokio::test]
    async fn a_deadline_that_runs_out_returns_what_it_placed() {
        remote_fixture!(registry, controller, metrics, config = fast_config());

        let mut remote = MockRemoteMarkerWriter::new();
        remote
            .expect_write_markers()
            .returning(|_leader, _marker, _targets| {
                Err(BrokerError::Replication("leader is down".into()))
            });
        let remote: Arc<dyn RemoteMarkerWriter> = Arc::new(remote);

        let fanout = test_fanout(&registry, &controller, Some(&remote), &metrics, &config);
        let placed = place_one(fanout).await;
        assert!(placed.is_empty());
    }
}
