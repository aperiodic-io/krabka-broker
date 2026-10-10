//! Per-leader-partition ISR maintenance. It compares each in-sync follower's
//! last-caught-up time against `replica_lag_time_max`, as Kafka's
//! `Partition.maybeShrinkIsr` does, admits each out-of-sync follower that
//! Kafka's `Partition.needsExpandIsr` would, and proposes the resulting
//! `AlterPartition` shrink or expand, stamped with the committed partition
//! epoch, to the CONTROLLER listener of the active controller.
//!
//! Each pass also hands the partition the [`LeaderPolicy`] its metadata image
//! implies -- `min.insync.replicas`, the lag bound and every replica's
//! fencing and broker epoch -- which the leader's high watermark reads
//! between passes.
//!
//! [`LeaderPolicy`]: crate::replica_state::LeaderPolicy

use std::sync::Arc;

use krabka_raft::NodeId;
use krabka_units::{Time, convert::TimeExt as _};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{partition::Partition, partition_registry::PartitionRegistry};

mod alter_partition;
mod proposal;
mod request_builder;
#[cfg(test)]
mod test_support;

use self::{
    alter_partition::{ControllerLink, send_alter_partition},
    proposal::compute_proposal,
    request_builder::IsrChange,
};

pub(crate) struct Config {
    /// How a leader reaches the CONTROLLER listener of the active controller.
    pub dialer: crate::controller_endpoint::ControllerDialer,
    pub node_id: NodeId,
    pub scan_interval: Time,
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub replica_lag_time_max: Time,
    /// This broker's static `min.insync.replicas`, the last resort when the
    /// image names no value for a topic.
    pub default_min_insync_replicas: i32,
    pub broker_id: i32,
    pub shutdown: CancellationToken,
    /// Bumped on each proposed shrink or expand.
    pub metrics: crate::metrics::BrokerMetrics,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(cfg.scan_interval.to_std());
    // Reused across ticks to avoid re-allocating the snapshot Vec each second.
    // Holds cheap `Arc<Partition>` clones (no String allocation, no second
    // registry lookup). Cleared and refilled each tick.
    let mut snapshot: Vec<Arc<Partition>> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            () = cfg.shutdown.cancelled() => return,
        }
        // Snapshot the partition values as cheap Arc clones in a single
        // iteration, then iterate the owned `Vec` so we never hold a shard
        // guard across a yield point.
        snapshot.clear();
        snapshot.extend(cfg.partitions.arcs());
        for part in snapshot.drain(..) {
            if part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                != cfg.node_id
            {
                continue;
            }
            let image = cfg.controller.current_image();
            let Some(record) = image.partition(&part.topic, part.index.get()) else {
                continue;
            };
            let policy = crate::replica_state::LeaderPolicy::from_image(
                &image,
                cfg.node_id,
                record,
                cfg.replica_lag_time_max.to_std(),
                cfg.default_min_insync_replicas,
            );
            let proposal = compute_proposal(&part, record, policy).await;
            let recovering = image.leader_recovery_state(&part.topic, part.index.get())
                == krabka_metadata::LeaderRecoveryState::Recovering;
            let (new_isr, leader_epoch, partition_epoch) = match proposal {
                Some(proposal) => {
                    // Classify the proposal as shrink/expand using the ISRs captured
                    // inside `compute_proposal`'s single lock scope. `compute_proposal`
                    // already filtered for "actually changed", so at least one of these
                    // bumps fires. Reusing its captured `prev_isr` avoids a second
                    // `replica_state` lock and closes the TOCTOU window where the ISR
                    // could change between the two acquisitions.
                    let prev_isr: std::collections::HashSet<NodeId> =
                        proposal.prev_isr.iter().copied().collect();
                    let next_isr: std::collections::HashSet<NodeId> =
                        proposal.new_isr.iter().copied().collect();
                    if prev_isr.difference(&next_isr).next().is_some() {
                        cfg.metrics.isr_shrinks_total.inc();
                    }
                    if next_isr.difference(&prev_isr).next().is_some() {
                        cfg.metrics.isr_expands_total.inc();
                    }
                    (
                        proposal.new_isr,
                        proposal.leader_epoch.0,
                        proposal.partition_epoch,
                    )
                }
                None if recovering => (
                    record.isr.clone(),
                    record.leader_epoch.0,
                    record.partition_epoch,
                ),
                None => continue,
            };
            let link = ControllerLink {
                controller: &cfg.controller,
                broker_id: cfg.broker_id,
                dialer: &cfg.dialer,
            };
            let change = IsrChange {
                topic: &part.topic,
                partition: part.index.get(),
                new_isr,
                leader_epoch,
                partition_epoch,
            };
            if let Err(e) = send_alter_partition(&link, &change).await {
                warn!(topic = %part.topic, partition = part.index.get(), error = %e,
                    "AlterPartition propose failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::Ordering, time::Duration};

    use krabka_ids::PartitionIndex;
    use krabka_metadata::MetadataImage;
    use krabka_units::{hours, secs};
    use tempfile::tempdir;

    use super::*;
    use crate::isr_maintenance::test_support::{
        IsrSetup, fake_source, fixture_partition, partition, set_replica_state, topic,
    };

    /// One scan of a leader partition whose only follower last caught up
    /// 30 s ago, and has not fetched since, must classify the proposal as
    /// exactly one shrink and zero expands.
    ///
    /// `scan_interval` is an hour on purpose. `tokio::time::interval` fires
    /// its first tick immediately, so the loop performs exactly one scan and
    /// then parks until shutdown. A short interval would let the loop rescan
    /// while the test is still waiting on the metric: `TestMetadataSource`
    /// never applies the `AlterPartition`, so the partition's ISR still holds
    /// the lagging follower on the next tick and the shrink is counted again.
    /// That is what produced the intermittent `2 == 1`.
    #[tokio::test]
    async fn run_bumps_shrink_metric_for_leader_partition() {
        let log_dir = tempdir().unwrap();
        let part = fixture_partition(
            log_dir.path(),
            crate::test_support::StandalonePartitionSetup {
                topic: "t",
                ..Default::default()
            },
        );
        part.current_leader.store(1, Ordering::Release);
        set_replica_state(
            &part,
            IsrSetup {
                leader_epoch: krabka_ids::LeaderEpoch(10),
                stale_followers: &[(NodeId(2), Duration::from_secs(30))],
                ..Default::default()
            },
        )
        .await;
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&topic("t", uuid::Uuid::from_u128(1)));
        image.apply(&partition(IsrSetup {
            leader_epoch: krabka_ids::LeaderEpoch(10),
            partition_epoch: crate::test_support::PartitionEpoch(4),
            ..Default::default()
        }));

        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("t".into(), PartitionIndex(0), part);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(fake_source(image, None));
        let metrics = crate::metrics::BrokerMetrics::default();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(Config {
            dialer: crate::test_support::plaintext_controller_dialer(),
            node_id: NodeId(1),
            scan_interval: hours(1),
            partitions,
            controller,
            replica_lag_time_max: secs(5),
            default_min_insync_replicas: 1,
            broker_id: 1,
            shutdown: shutdown.clone(),
            metrics: metrics.clone(),
        }));

        // The scan is a single immediate tick, but the spawned task still has
        // to be polled to completion of that tick. Poll for the counter rather
        // than sleeping for a guessed duration; the bound only has to outlast
        // scheduler starvation on a loaded machine, never a real interval.
        tokio::time::timeout(Duration::from_secs(30), async {
            while metrics.isr_shrinks_total.get() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("leader partition should be scanned and classified as a shrink");

        shutdown.cancel();
        task.await.unwrap();
        assert2::assert!((metrics.isr_shrinks_total.get()) == (1));
        assert2::assert!((metrics.isr_expands_total.get()) == (0));
    }
}
