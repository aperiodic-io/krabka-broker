//! The KIP-112 offline-log-dir failover that a heartbeat can trigger.
//!
//! A broker that reports offline log dirs is still alive, so the liveness
//! `alive` to `dead` failover never fires for it. This module maps the
//! reported dirs to the partitions they hold, moves leadership off them, and
//! records the loss on the reporting broker's registration so every node —
//! not just the controller leader — can see which replicas are offline.

/// Run the KIP-112 offline-dir failover for `broker`'s reported offline dirs.
/// It computes the partition changes, submits them together with the
/// registration update that persists the loss, and returns any offset-aware
/// recovery jobs (KIP-966) for the caller to enqueue. It runs on the
/// controller leader only, and the caller gates on leadership. A submit
/// failure is logged and does not propagate.
pub(crate) async fn failover_offline_dirs(
    controller: &std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
    broker: krabka_raft::NodeId,
    offline: &std::collections::HashSet<uuid::Uuid>,
    liveness: &crate::heartbeat::controller_state::ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> Vec<(String, i32, crate::config_keys::RecoveryStrategy)> {
    let image = controller.current_image();
    let plan = crate::leader_election::compute_offline_dir_failover_changes(
        &image, broker, offline, liveness, metrics,
    )
    .await;
    let mut changes = plan.changes;
    changes.extend(retire_offline_dirs(&image, broker, offline));
    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        tracing::warn!(error = %e, "offline-dir failover submit_change failed");
    }
    plan.recoveries
}

/// The `BrokerRegistrationChangeRecord` that drops `offline` from `broker`'s
/// online log dirs, at its broker epoch, or `None` when the registration
/// already lists none of them.
///
/// This is Kafka's `ReplicationControlManager.handleDirectoriesOffline`: it
/// writes the *surviving* directories, so a later `Metadata` or
/// `DescribeTopicPartitions` served by any node can tell that a replica sits
/// on a dead disk. The change keeps the KIP-903 broker epoch, so it never
/// looks like a re-registration to the fencing paths. The intersection test
/// makes it idempotent: a broker repeats its offline dirs on every heartbeat,
/// and only the first one writes a record.
fn retire_offline_dirs(
    image: &krabka_metadata::MetadataImage,
    broker: krabka_raft::NodeId,
    offline: &std::collections::HashSet<uuid::Uuid>,
) -> Option<krabka_metadata::MetadataRecord> {
    let registration = image.broker(broker)?;
    if !registration.log_dirs.iter().any(|d| offline.contains(d)) {
        return None;
    }
    let online: Vec<uuid::Uuid> = registration
        .log_dirs
        .iter()
        .copied()
        .filter(|d| !offline.contains(d))
        .collect();
    tracing::warn!(
        broker = broker.0,
        remaining_log_dirs = online.len(),
        "log dirs reported offline; retiring them from the broker registration",
    );
    Some(krabka_metadata::MetadataRecord::V1BrokerRegistrationChange(
        krabka_metadata::BrokerRegistrationChangeRecord {
            log_dirs: online,
            ..krabka_metadata::BrokerRegistrationChangeRecord::no_change(
                broker,
                registration.broker_epoch,
            )
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, PartitionRecord};
    use uuid::Uuid;

    use super::*;
    use crate::handlers::broker_heartbeat::test_support::{
        fake_source, image_with_dir_partition, liveness_with,
    };

    #[tokio::test]
    async fn failover_offline_dirs_submits_change_for_offline_leader() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // leader=1, replicas=[1,2], isr=[1,2]; broker 1's dir is `bad`.
        let img = two_replica_image(&[bad, good]);
        let (source, recoveries) = run_failover(img, bad).await;

        // Exactly one change must have been submitted (the new leader record):
        // broker 2 is elected (broker 1's dir is offline), the offline replica
        // is dropped from the ISR, and both epochs are bumped.
        let changes = source.submitted_records();
        let expected_changes = vec![MetadataRecord::V1Partition(PartitionRecord {
            isr: vec![krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(6),
            directories: vec![bad, good],
            partition_epoch: 1,
            ..crate::handlers::test_support::replicated_partition(
                crate::handlers::test_support::ReplicatedPartitionSetup {
                    topic: "t",
                    leader: krabka_audit::NodeId(2),
                    replicas: &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                    ..Default::default()
                },
            )
        })];
        assert!(changes == expected_changes);
        // No unclean recovery needed (broker 2 is alive and in ISR).
        assert!(recoveries == vec![]);
    }

    #[tokio::test]
    async fn failover_offline_dirs_no_change_when_dir_healthy() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // Both replicas are on `good` dir; reporting `bad` as offline is a no-op.
        let img = two_replica_image(&[good, good]);
        let (source, recoveries) = run_failover(img, bad).await;

        // No change submitted and no recovery needed.
        assert!(source.submitted_records().is_empty());
        assert!(recoveries.is_empty());
    }

    /// The registration record a broker publishes for `node_id` over `dirs`.
    fn registration(node_id: u64, dirs: &[Uuid]) -> krabka_metadata::BrokerRegistrationRecord {
        krabka_metadata::BrokerRegistrationRecord {
            broker_epoch: 11,
            incarnation_id: Uuid::from_u128(u128::from(node_id)),
            host: format!("broker-{node_id}"),
            log_dirs: dirs.to_vec(),
            ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
        }
    }

    #[tokio::test]
    async fn failover_offline_dirs_retires_the_dead_dir_from_the_registration() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // Both replicas sit on `good`, so no leadership moves: the registration
        // update is the only change the heartbeat produces.
        let img = registration_only_failover_image(good, &[good, bad]);
        let (source, _recoveries) = run_failover(img, bad).await;

        // Kafka's `handleDirectoriesOffline` change: the surviving dir, at the
        // KIP-903 epoch the broker is registered at.
        let expected = vec![MetadataRecord::V1BrokerRegistrationChange(
            krabka_metadata::BrokerRegistrationChangeRecord {
                node_id: krabka_audit::NodeId(1),
                broker_epoch: 11,
                fenced: krabka_metadata::FencingChange::None,
                in_controlled_shutdown: false,
                log_dirs: vec![good],
                cordoned_log_dirs: None,
            },
        )];
        assert!(source.submitted_records() == expected);
    }

    #[tokio::test]
    async fn failover_offline_dirs_does_not_rewrite_a_registration_twice() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        let img = registration_only_failover_image(good, &[good]);
        let (source, _recoveries) = run_failover(img, bad).await;

        assert!(source.submitted_records().is_empty());
    }
    fn registration_only_failover_image(
        good: Uuid,
        registered_dirs: &[Uuid],
    ) -> krabka_metadata::MetadataImage {
        let mut image = two_replica_image(&[good, good]);
        image.apply(&MetadataRecord::V1BrokerRegistration(registration(
            1,
            registered_dirs,
        )));
        image
    }

    fn two_replica_image(directories: &[Uuid]) -> krabka_metadata::MetadataImage {
        image_with_dir_partition(
            crate::handlers::broker_heartbeat::test_support::ElectionSetup {
                replicas: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                dirs: directories,
                ..Default::default()
            },
        )
    }

    async fn run_failover(
        img: krabka_metadata::MetadataImage,
        bad: Uuid,
    ) -> (
        Arc<crate::test_support::FakeMetadataSource>,
        Vec<(String, i32, crate::config_keys::RecoveryStrategy)>,
    ) {
        let source = fake_source(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::clone(&source) as _;
        let liveness = liveness_with(&[krabka_audit::NodeId(1), krabka_audit::NodeId(2)]).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let recoveries = failover_offline_dirs(
            &controller,
            krabka_audit::NodeId(1),
            &maplit::hashset! {bad},
            &liveness,
            &metrics,
        )
        .await;
        (source, recoveries)
    }
}
