//! The periodic broker gauge sampler and the stretch-cluster leader-drift
//! count it reports. The sampler walks the metadata image on a timer, so it is
//! kept apart from the event-driven liveness services that share the same
//! startup phase.

use std::sync::Arc;

use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use crate::{config::BrokerConfig, partition_registry::PartitionRegistry};

/// Epoch milliseconds for the KIP-835 time-since-last-applied-record gauge.
fn epoch_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn metadata_applied_lag_ms(applied_timestamp_ms: i64, is_leader: bool, now_ms: i64) -> i64 {
    if is_leader || applied_timestamp_ms <= 0 {
        0
    } else {
        now_ms.saturating_sub(applied_timestamp_ms)
    }
}

/// Counts the partitions `node_id` leads from a site other than the stretch
/// cluster's preferred leader site.
///
/// The count is zero when the cluster pins leadership to no site. A node
/// whose registration names no rack does not sit in the preferred site, so
/// its partitions count as drift. `node_id` leads every partition the count
/// considers, so one rack lookup covers all of them.
fn leader_site_drift_partitions(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> usize {
    let Some(preferred) = crate::config_keys::resolve_preferred_leader_site(image) else {
        return 0;
    };
    let leader_site = image
        .broker(node_id)
        .and_then(|broker| broker.rack.as_deref());
    if leader_site == Some(preferred) {
        return 0;
    }
    image
        .all_partitions()
        .filter(|partition| partition.leader == node_id)
        .count()
}

fn update_metadata_health_metrics(
    metrics: &crate::metrics::BrokerMetrics,
    controller: &dyn crate::metadata_source::MetadataSource,
) {
    let quorum_snapshot = controller.quorum_snapshot();
    if let Some(snap) = &quorum_snapshot {
        for state_label in crate::metrics::RaftStateLabel::ALL {
            let is_active = i64::from(state_label.state == snap.current_state);
            metrics
                .raft_current_state
                .get_or_create(&state_label)
                .set(is_active);
        }
        metrics.raft_current_epoch.set(i64::from(snap.leader_epoch));
        metrics.raft_high_watermark.set(snap.high_watermark);
        metrics.raft_log_end_offset.set(snap.log_end_offset);
        metrics
            .raft_voters
            .set(i64::try_from(snap.voters.len()).unwrap_or(i64::MAX));
        metrics
            .raft_observers
            .set(i64::try_from(snap.observers.len()).unwrap_or(i64::MAX));
    }
    let applied_timestamp_ms = controller.last_applied_record_timestamp_ms();
    metrics
        .metadata_last_applied_timestamp_ms
        .set(applied_timestamp_ms);
    let is_leader = quorum_snapshot.as_ref().is_some_and(|snap| snap.is_leader);
    let applied_lag_ms = metadata_applied_lag_ms(applied_timestamp_ms, is_leader, epoch_now_ms());
    metrics.metadata_last_applied_lag_ms.set(applied_lag_ms);
    let (processing_avg_us, processing_max_us) = controller.pending_record_processing_time_us();
    metrics
        .pending_record_processing_time_us_avg
        .set(i64::try_from(processing_avg_us).unwrap_or(i64::MAX));
    metrics
        .pending_record_processing_time_us_max
        .set(i64::try_from(processing_max_us).unwrap_or(i64::MAX));

    let applied_offset = controller.current_metadata_offset();
    let committed_offset = controller.quorum_committed_offset();
    metrics.metadata_last_applied_offset.set(applied_offset);
    let metadata_lag = committed_offset.saturating_sub(applied_offset).max(0);
    metrics.metadata_lag_records.set(metadata_lag);
    metrics.set_metadata_load_error_count(controller.metadata_load_error_count());
}

/// Spawns the timer that samples the broker's cluster gauges.
///
/// `storage` is what this broker holds locally: the partitions it hosts and
/// the health of the directories they sit in. They travel together because
/// one pass samples both, and neither is reachable from the metadata image.
///
/// `observability` is the registry the sample is written to and the health
/// state the broker-state gauge reads; they travel together because every
/// sample writes the first from the second.
pub(super) fn spawn_broker_gauge_updater(
    storage: (
        Arc<PartitionRegistry>,
        crate::log_dir_status::LogDirRegistry,
    ),
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    node_id: krabka_metadata::NodeId,
    observability: (
        crate::metrics::BrokerMetrics,
        Option<crate::health::HealthState>,
    ),
    config: &BrokerConfig,
    shutdown: CancellationToken,
) {
    let (partitions, log_dir_status) = storage;
    let (metrics, health) = observability;
    let poll_interval = config.gauge_poll_interval;
    let default_min_insync_replicas = config.default_min_insync_replicas;
    let static_voter_count = config.controller_quorum_voters.len();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(poll_interval.to_std());
        let mut previous_voted_directory = None;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                () = shutdown.cancelled() => return,
            }
            let led = partitions
                .arcs()
                .iter()
                .filter(|partition| {
                    partition
                        .current_leader
                        .load(std::sync::atomic::Ordering::Acquire)
                        == node_id
                })
                .count();
            metrics
                .partitions_led
                .set(i64::try_from(led).unwrap_or(i64::MAX));
            metrics
                .partitions_total
                .set(i64::try_from(partitions.len()).unwrap_or(i64::MAX));
            // The registry is the one place that knows a dir went offline: a
            // runtime write or fsync failure flips it there and nothing else
            // publishes the flip. `DescribeLogDirs` reports the same dirs,
            // but only to a client that asks.
            metrics.set_offline_log_dirs(log_dir_status.offline().len());
            let image = controller.current_image();
            let alive = liveness.alive_snapshot().await;
            let is_controller = controller
                .watch_leader()
                .borrow()
                .is_some_and(|leader| leader == node_id);
            let minimum_isr: std::collections::HashMap<&str, i32> = image
                .topics()
                .map(|topic| {
                    let minimum =
                        crate::config_keys::node_min_insync_replicas(&image, node_id, &topic.name)
                            .unwrap_or(default_min_insync_replicas);
                    (topic.name.as_str(), minimum)
                })
                .collect();
            let mut partition_health = (0_usize, 0_usize, 0_usize);
            for partition in image.all_partitions() {
                if partition.leader == node_id {
                    partition_health.0 +=
                        usize::from(partition.isr.len() < partition.replicas.len());
                    let minimum = minimum_isr
                        .get(partition.topic.as_str())
                        .copied()
                        .unwrap_or(default_min_insync_replicas);
                    partition_health.1 += usize::from(
                        i32::try_from(partition.isr.len()).unwrap_or(i32::MAX) < minimum,
                    );
                }
                partition_health.2 +=
                    usize::from(is_controller && !alive.contains(&partition.leader.0));
            }
            metrics
                .under_replicated_partitions
                .set(i64::try_from(partition_health.0).unwrap_or(i64::MAX));
            metrics
                .under_min_isr_partition_count
                .set(i64::try_from(partition_health.1).unwrap_or(i64::MAX));
            metrics
                .offline_partitions_count
                .set(i64::try_from(partition_health.2).unwrap_or(i64::MAX));
            metrics.leader_site_drift_partitions.set(
                i64::try_from(leader_site_drift_partitions(&image, node_id)).unwrap_or(i64::MAX),
            );
            metrics.witness_role.set(i64::from(u8::from(
                crate::config_keys::resolve_broker_witness(&image, node_id),
            )));
            metrics
                .active_controller
                .set(i64::from(u8::from(is_controller)));

            // Cluster state metrics published by the active controller (#390)
            if is_controller {
                let active = image
                    .brokers()
                    .filter(|b| alive.contains(&b.node_id.0))
                    .count();
                let fenced = image
                    .brokers()
                    .filter(|b| !alive.contains(&b.node_id.0))
                    .count();
                metrics
                    .active_brokers
                    .set(i64::try_from(active).unwrap_or(i64::MAX));
                metrics
                    .fenced_brokers
                    .set(i64::try_from(fenced).unwrap_or(i64::MAX));
                metrics
                    .global_topics
                    .set(i64::try_from(image.topics().count()).unwrap_or(i64::MAX));
                let mut total_partitions = 0_usize;
                let mut at_min_isr = 0_usize;
                let mut reassigning = 0_usize;
                let mut preferred_imbalance = 0_usize;
                for partition in image.all_partitions() {
                    total_partitions += 1;
                    let minimum = minimum_isr
                        .get(partition.topic.as_str())
                        .copied()
                        .unwrap_or(default_min_insync_replicas);
                    if i32::try_from(partition.isr.len()).unwrap_or(i32::MAX) == minimum {
                        at_min_isr += 1;
                    }
                    if !partition.adding_replicas.is_empty()
                        || !partition.removing_replicas.is_empty()
                    {
                        reassigning += 1;
                    }
                    if partition
                        .replicas
                        .first()
                        .is_some_and(|pref| *pref != partition.leader)
                    {
                        preferred_imbalance += 1;
                    }
                }
                metrics
                    .global_partitions
                    .set(i64::try_from(total_partitions).unwrap_or(i64::MAX));
                metrics
                    .at_min_isr_partition_count
                    .set(i64::try_from(at_min_isr).unwrap_or(i64::MAX));
                metrics
                    .reassigning_partitions
                    .set(i64::try_from(reassigning).unwrap_or(i64::MAX));
                metrics
                    .preferred_replica_imbalance
                    .set(i64::try_from(preferred_imbalance).unwrap_or(i64::MAX));
            } else {
                metrics.active_brokers.set(0);
                metrics.fenced_brokers.set(0);
                metrics.global_topics.set(0);
                metrics.global_partitions.set(0);
                metrics.at_min_isr_partition_count.set(0);
                metrics.reassigning_partitions.set(0);
                metrics.preferred_replica_imbalance.set(0);
            }

            update_metadata_health_metrics(&metrics, controller.as_ref());

            // Broker lifecycle state (#390)
            if let Some(h) = &health {
                metrics.broker_state.set(h.broker_state());
            }

            let ignored_static_voters =
                usize::from(image.kraft_version() >= 1).saturating_mul(static_voter_count);
            metrics
                .ignored_static_voters
                .set(i64::try_from(ignored_static_voters).unwrap_or(i64::MAX));
            let voted_directory = controller.voted_directory_id();
            if voted_directory != previous_voted_directory {
                if let Some(directory_id) = previous_voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&crate::metrics::DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(0);
                }
                if let Some(directory_id) = voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&crate::metrics::DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(1);
                }
                previous_voted_directory = voted_directory;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{millis, secs};

    use super::*;
    use crate::broker::test_support::fake_source;

    #[test]
    fn active_controller_applied_timestamp_lag_stays_zero() {
        assert!(metadata_applied_lag_ms(1_700_000_000_000, true, 1_700_000_100_000) == 0);
        assert!(metadata_applied_lag_ms(1_700_000_000_000, false, 1_700_000_100_000) == 100_000);
    }

    #[tokio::test]
    async fn broker_gauge_uses_configured_default_min_isr() {
        let node_id = krabka_metadata::NodeId(1);
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            crate::test_support::single_partition_topic("gauge-topic", uuid::Uuid::nil()),
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "gauge-topic".into(),
                partition: 0,
                leader: node_id,
                replicas: vec![node_id],
                isr: vec![node_id],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            },
        ));
        let (metrics, mut config) = gauge_fixture();
        config.default_min_insync_replicas = 2;
        let shutdown = CancellationToken::new();
        spawn_broker_gauge_updater(
            (
                Arc::new(PartitionRegistry::new()),
                crate::log_dir_status::LogDirRegistry::default(),
            ),
            Arc::new(fake_source(image, None)),
            Arc::new(crate::heartbeat::controller_state::ControllerLivenessState::new(secs(10))),
            node_id,
            (metrics.clone(), None),
            &config,
            shutdown.child_token(),
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while metrics.under_min_isr_partition_count.get() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("gauge observes configured minimum ISR");

        assert!(metrics.under_min_isr_partition_count.get() == 1);
        assert!(
            metrics.offline_partitions_count.get() == 0,
            "a non-controller has no cluster liveness view and must not invent an offline partition"
        );
        shutdown.cancel();
    }

    fn gauge_fixture() -> (crate::metrics::BrokerMetrics, BrokerConfig) {
        let metrics = crate::metrics::BrokerMetrics::new();
        let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
        config.gauge_poll_interval = millis(1);
        (metrics, config)
    }

    /// A dir that goes offline under live traffic has to reach the scrape.
    /// The registry is the only thing that knows about the flip, and
    /// `DescribeLogDirs` reports it only to a client that asks.
    #[tokio::test]
    async fn broker_gauge_publishes_the_offline_log_dir_count() {
        let dir = tempfile::tempdir().expect("log root");
        let log_dirs = crate::log_dir_status::LogDirRegistry::probe(&[dir.path().to_path_buf()]);
        let (metrics, config) = gauge_fixture();
        let shutdown = CancellationToken::new();
        spawn_broker_gauge_updater(
            (Arc::new(PartitionRegistry::new()), log_dirs.clone()),
            Arc::new(fake_source(
                krabka_metadata::MetadataImage::new(uuid::Uuid::nil()),
                None,
            )),
            Arc::new(crate::heartbeat::controller_state::ControllerLivenessState::new(secs(10))),
            krabka_metadata::NodeId(1),
            (metrics.clone(), None),
            &config,
            shutdown.child_token(),
        );

        // A healthy dir publishes the series at zero, so an operator can tell
        // "no dir is offline" apart from "this broker does not report".
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while metrics.partitions_total.get() != 0 || metrics.offline_log_dirs.get() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sampler ran at least once");

        log_dirs.mark_offline(dir.path(), "partition write/fsync failed: synthetic EIO");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while metrics.offline_log_dirs.get() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the flip reaches the gauge without a restart");

        shutdown.cancel();
    }

    /// An image where node 1 sits in `rack` and leads `led` partitions of one
    /// topic, and node 2 sits in `dc-a` and leads one more.
    fn stretch_image(rack: &str, led: i32) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node_id, node_rack) in [(1_u64, rack), (2_u64, "dc-a")] {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    rack: Some(node_rack.to_string()),
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: Some("dc-a".into()),
            },
        ));
        for partition in 0..=led {
            let leader = if partition < led { 1 } else { 2 };
            image.apply(&krabka_metadata::MetadataRecord::V1Partition(
                crate::handlers::test_support::replicated_partition(
                    crate::handlers::test_support::ReplicatedPartitionSetup {
                        topic: "stretch-topic",
                        partition: krabka_ids::PartitionIndex(partition),
                        leader: krabka_metadata::NodeId(leader),
                        ..Default::default()
                    },
                ),
            ));
        }
        image
    }

    #[test]
    fn leader_site_drift_counts_only_partitions_led_outside_the_preferred_site() {
        for (rack, led, expected) in [("dc-b", 2, 2), ("dc-a", 2, 0)] {
            let image = stretch_image(rack, led);

            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == expected,
                "rack {rack}"
            );
            // Node 2 leads one partition from the preferred site, so it never
            // drifts whatever node 1 does.
            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(2)) == 0,
                "rack {rack}"
            );
        }
    }

    #[test]
    fn leader_site_drift_is_zero_when_the_cluster_pins_no_leader_site() {
        let mut image = stretch_image("dc-b", 2);
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: None,
            },
        ));

        assert!(leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == 0);
    }
}
