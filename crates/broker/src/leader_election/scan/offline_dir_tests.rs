//! Tests for the offline-log-dir failover scan (KIP-112): the leader
//! election when the leader's directory fails, the plain ISR shrink for a
//! non-leader replica, idempotence after a completed failover, and the
//! empty-ISR branches that either defer to the recovery manager or fall
//! through to an unclean election.

use {
    super::*,
    crate::{
        config_keys::{UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
        leader_election::test_support::{
            TopicConfigOverride, expected_clean_election, expected_partition, img_with_partition,
            liveness_with_alive, one_partition_change,
        },
        metrics::BrokerMetrics,
    },
    assert2::{assert, check},
};

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct ReplicaSlot(usize);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct OfflinePartitionSetup<'a> {
    #[default(NodeId(1))]
    leader: NodeId,
    #[default(&[NodeId(1), NodeId(2)])]
    isr: &'a [NodeId],
    #[default(Some(ReplicaSlot(0)))]
    failed_replica: Option<ReplicaSlot>,
    configs: &'a [TopicConfigOverride<'a>],
}

/// An image with one partition on a failed log dir, and the two directory ids
/// its replicas were given.
struct OfflineImage {
    image: MetadataImage,
    /// The directory the failed replica sits on.
    bad: uuid::Uuid,
    /// The directory every other replica sits on.
    good: uuid::Uuid,
}

#[bon::builder]
fn offline_image(
    #[builder(into, default = NodeId (1))] leader: NodeId,
    #[builder(default = & [NodeId (1) , NodeId (2)])] isr: &'_ [NodeId],
    #[builder(required)] failed_replica: Option<usize>,
    #[builder(default)] configs: &'_ [TopicConfigOverride<'_>],
) -> OfflineImage {
    let setup = OfflinePartitionSetup {
        leader,
        isr,
        failed_replica: failed_replica.map(ReplicaSlot),
        configs,
    };
    let OfflinePartitionSetup {
        leader,
        isr,
        failed_replica,
        configs,
    } = setup;
    let bad = uuid::Uuid::from_u128(0xDEAD);
    let good = uuid::Uuid::from_u128(0x1);
    let mut directories = [good; 3];
    if let Some(index) = failed_replica {
        directories[index.0] = bad;
    }
    OfflineImage {
        image: img_with_partition()
            .leader(leader)
            .isr(isr)
            .dirs(&directories)
            .configs(configs)
            .call(),
        bad,
        good,
    }
}

fn unclean_election_image() -> OfflineImage {
    offline_image()
        .failed_replica(Some(0))
        .configs(&[TopicConfigOverride::new(
            UNCLEAN_LEADER_ELECTION_ENABLE,
            "true",
        )])
        .call()
}

async fn scan_offline_dir(
    image: &MetadataImage,
    broker: u64,
    bad: uuid::Uuid,
    alive: &[u64],
) -> FailoverPlan {
    scan_offline_dir_with_metrics(image, broker, bad, alive)
        .await
        .plan
}

async fn failed_replica_change(
    failed_slot: usize,
    broker: u64,
) -> (MetadataRecord, uuid::Uuid, uuid::Uuid) {
    let OfflineImage { image, bad, good } = offline_image()
        .isr(&[NodeId(1), NodeId(2), NodeId(3)])
        .failed_replica(Some(failed_slot))
        .call();
    let change = scan_offline_dir(&image, broker, bad, &[1, 2, 3])
        .await
        .changes
        .into_iter()
        .next()
        .expect("failed replica produces a partition change");
    (change, bad, good)
}

/// What an offline-dir scan produced, and the metrics it counted into.
struct ScanOutcome {
    plan: FailoverPlan,
    metrics: BrokerMetrics,
}

async fn scan_offline_dir_with_metrics(
    image: &MetadataImage,
    broker: u64,
    bad: uuid::Uuid,
    alive: &[u64],
) -> ScanOutcome {
    let liveness = liveness_with_alive(alive).await;
    let metrics = BrokerMetrics::new();
    let plan = compute_offline_dir_failover_changes(
        image,
        NodeId(broker),
        &maplit::hashset! {bad},
        &liveness,
        &metrics,
    )
    .await;
    ScanOutcome { plan, metrics }
}

#[tokio::test]
async fn offline_dir_elects_alive_isr_member_when_leader_dir_failed() {
    let (change, bad, good) = failed_replica_change(0, 1).await;
    let MetadataRecord::V1Partition(pr) = change else {
        panic!()
    };
    let expected = expected_clean_election(
        krabka_raft::NodeId(2),
        &[krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
        vec![bad, good, good],
    );
    assert!(pr == expected);
}

async fn assert_offline_dir_no_changes(setup: OfflinePartitionSetup<'_>) {
    let OfflineImage {
        image: img, bad, ..
    } = offline_image()
        .leader(setup.leader.0)
        .isr(setup.isr)
        .failed_replica(setup.failed_replica.map(|slot| slot.0))
        .configs(setup.configs)
        .call();
    let plan = scan_offline_dir(&img, 1, bad, &[1, 2, 3]).await;
    assert!(plan.changes.is_empty());
}

#[tokio::test]
async fn offline_dir_leaves_healthy_dir_partition_untouched() {
    assert_offline_dir_no_changes(OfflinePartitionSetup {
        isr: &[NodeId(1), NodeId(2), NodeId(3)],
        failed_replica: None,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
async fn offline_dir_shrinks_isr_for_non_leader_replica() {
    let (change, bad, good) = failed_replica_change(1, 2).await;
    let MetadataRecord::V1Partition(pr) = change else {
        panic!()
    };
    let expected = expected_partition()
        .leader(1)
        .isr(&[krabka_raft::NodeId(1), krabka_raft::NodeId(3)])
        .leader_epoch(5)
        .directories(vec![good, bad, good])
        .call();
    assert!(pr == expected);
}

#[tokio::test]
async fn offline_dir_idempotent_after_failover() {
    // After failover: broker 1's dir is bad but broker 1 is no longer
    // leader (broker 2 is), and broker 1 is not in ISR {2,3} either.
    assert_offline_dir_no_changes(OfflinePartitionSetup {
        leader: NodeId(2),
        isr: &[NodeId(2), NodeId(3)],
        ..Default::default()
    })
    .await;
}

#[yare::parameterized(
    balanced = { "Balanced", RecoveryStrategy::Balanced },
    aggressive = { "Aggressive", RecoveryStrategy::Aggressive },
)]
#[test_macro(tokio::test)]
async fn offline_dir_empty_isr_defers_offset_aware_strategies_to_urm(
    name: &str,
    strategy: RecoveryStrategy,
) {
    let OfflineImage {
        image: img, bad, ..
    } = offline_image()
        .failed_replica(Some(0))
        .configs(&[TopicConfigOverride::new(UNCLEAN_RECOVERY_STRATEGY, name)])
        .call();
    // Only node 3 is alive, and it is outside the ISR.
    let plan = scan_offline_dir(&img, 1, bad, &[3]).await;
    assert2::assert!(plan.changes.is_empty(), "{name}: {:?}", plan.changes);
    assert2::assert!(
        plan.recoveries == vec![("t".to_string(), 0, strategy)],
        "{name}: {:?}",
        plan.recoveries
    );
}

#[tokio::test]
async fn offline_dir_empty_isr_unclean_enabled_elects_out_of_isr_replica() {
    // Broker 1 is leader on bad dir, broker 2 (the only ISR peer) is dead,
    // broker 3 is alive and out-of-ISR.
    // unclean.leader.election.enable=true → elect broker 3, singleton ISR,
    // bump unclean_leader_elections_total.
    let OfflineImage {
        image: img,
        bad,
        good,
    } = unclean_election_image();
    let ScanOutcome { plan, metrics } = scan_offline_dir_with_metrics(&img, 1, bad, &[3]).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Must elect broker 3 (only alive out-of-ISR) with a singleton
    // ISR (unclean election) and a bumped leader_epoch.
    let expected =
        expected_clean_election(krabka_raft::NodeId(3), &[krabka_raft::NodeId(3)], vec![
            bad, good, good,
        ]);
    assert!(*pr == expected);
    assert!(
        metrics.unclean_leader_elections_total.get() == 1,
        "unclean counter must be bumped exactly once"
    );
}

#[tokio::test]
async fn offline_dir_empty_isr_no_unclean_leaves_partition_unavailable() {
    // Broker 1 is leader on bad dir, broker 2 dead, broker 3 alive but
    // not in ISR.  No recovery strategy, no unclean flag → no change.
    let OfflineImage {
        image: img, bad, ..
    } = offline_image().failed_replica(Some(0)).call();
    // only 3 alive, but not in ISR
    let plan = scan_offline_dir(&img, 1, bad, &[3]).await;
    assert!(
        plan.changes.is_empty(),
        "default-off must not emit any change; got {:?}",
        plan.changes
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn offline_dir_empty_isr_unclean_enabled_no_alive_replica_stays_unavailable() {
    // Broker 1 is leader on bad dir, ALL brokers are dead.
    // unclean enabled but no alive replica → no change.
    let OfflineImage {
        image: img, bad, ..
    } = unclean_election_image();
    let ScanOutcome { plan, metrics } = scan_offline_dir_with_metrics(&img, 1, bad, &[]).await;
    check!(
        plan.changes.is_empty(),
        "no alive replica → no election; got {:?}",
        plan.changes
    );
    check!(plan.recoveries.is_empty());
    check!(
        metrics.unclean_leader_elections_total.get() == 0,
        "no election means no counter bump"
    );
}
