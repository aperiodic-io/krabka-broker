//! The operator-triggered unclean election: `ElectLeaders` with
//! `election_type=1` promotes the first alive replica once every ISR member is
//! dead.
//!
//! The scenario forges the dead ISR with an injected `PartitionRecord` rather
//! than by stopping brokers, so the raft quorum stays intact throughout.

use assert2::assert;
use krabka_metadata::{MetadataRecord, PartitionRecord};

use crate::{
    cluster_lock,
    kafka_wire::ElectionExchangeSetup,
    support::{
        admin::{ElectionKind, ElectionSetup},
        registered_cluster, shutdown_cluster,
        topics::AssignedTopicSetup,
    },
    wait::{
        wait_partition_exists, wait_partition_isr_contains, wait_partition_isr_only,
        wait_partition_leader, wait_partition_record_known,
    },
    wire::{CLIENT_ID, create_topic_plaintext, drive_elect_leaders},
};

/// A 3-broker PLAINTEXT cluster with an rf=2 topic where `replicas = [1, 2]`.
///
/// The scenario injects metadata to simulate a dead ISR. It does not break the
/// raft quorum.
///
/// 1. Submit a `PartitionRecord` with `isr=[99]` directly. Broker 99 does not
///    exist, so liveness reports it as dead.
/// 2. Broker 1 is in the replicas but not in the ISR. It is alive, and the
///    controller knows its heartbeat.
/// 3. Send `ElectLeaders Unclean` with `election_type=1` over the wire.
/// 4. The handler checks whether any ISR member is alive. Member 99 is not
///    alive, so the partition is eligible for an unclean election. The first
///    alive replica in [1, 2] is broker 1, so the handler elects broker 1 as
///    leader and sets ISR=[1].
/// 5. Assert per-partition `error_code` = 0 and poll until leader == 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_election_via_wire_picks_alive_replica() {
    let _g = cluster_lock().lock().await;

    let cluster = registered_cluster(3).await;

    // Send ElectLeaders to the current raft leader. Any broker answers it the
    // same way now that the election reads replicated state, but the leader is
    // the node whose view cannot lag, so it is the one this case drives.
    let controller_id = cluster[0].0.wait_until_controller_leader().await;
    let leader_index = cluster
        .iter()
        .position(|(_, cfg, _)| cfg.node_id == controller_id)
        .expect("raft leader must be one of the brokers");
    let addr = cluster[leader_index].1.listen_addr;
    // The UNCLEAN election promotes "the first electable replica". A committed
    // registration record is not enough on its own: a broker starts fenced and
    // is published unfenced only after the controller has taken its first
    // heartbeat and confirmed metadata catch-up. Waiting only on registration
    // would race an ELIGIBLE_LEADERS_NOT_AVAILABLE answer, so settle on the
    // predicate the election reads.
    cluster[leader_index].0.wait_until_broker_electable(1).await;
    // Keep named references to avoid chained index+tuple accesses that
    // confuse the Rust 1.95 borrow-checker span computation.
    let h0 = &cluster[0].0;
    let h1 = &cluster[1].0;

    // Create rf=2 topic. Replicas=[1,2]; broker 1 is preferred.
    create_topic_plaintext(
        addr,
        AssignedTopicSetup {
            name: ("foo-unclean").into(),
            replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
        },
    )
    .await;
    wait_partition_exists(h0, "foo-unclean", 0).await;
    wait_partition_exists(h1, "foo-unclean", 0).await;

    // Read the current partition record so we can preserve replicas + epoch.
    let pr_before = wait_partition_record_known(h0, "foo-unclean", 0).await;
    eprintln!("partition before injection: {pr_before:?}");

    // Inject a PartitionRecord whose leader AND ISR are the dead phantom 99.
    // Broker 99 has no registration in the image, so it is never electable.
    //
    // Crucially, the leader must be a DEAD broker, not a live replica: a live
    // leader runs ISR management and would re-admit itself / caught-up replicas
    // before the manual election ran, healing the forged state (the partition
    // would then have a live ISR member → ELECTION_NOT_NEEDED). With a dead
    // leader (99, not in replicas) no live broker owns the partition, so the
    // forged ISR=[99] persists until the operator's unclean election. (Nothing
    // auto-elects either: failover is transition-triggered on AliveToDead, and
    // 99 never went alive→dead.)
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: "foo-unclean".to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: pr_before.replicas.clone(),
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    h0.submit_metadata_record_for_test(forged)
        .await
        .expect("inject forged PartitionRecord");

    // Wait for the injected ISR to propagate to the image. With a dead leader
    // it stays [99] (no live leader to repair it).
    wait_partition_isr_only(h0, "foo-unclean", 0, &[99]).await;

    // Drive ElectLeaders Unclean (election_type=1).
    // The algorithm finds: ISR=[99] — all dead → unclean eligible.
    // First alive in replicas=[1,2] → broker 1 → new leader=1, isr=[1].
    let result = drive_elect_leaders(
        addr,
        ElectionExchangeSetup {
            client_id: CLIENT_ID,
            election: ElectionSetup {
                topic: ("foo-unclean").to_string(),
                kind: ElectionKind::Unclean,
                ..Default::default()
            },
        },
    )
    .await;
    assert!(
        result == vec![(0, 0)],
        "expected error_code=0 for UNCLEAN election; got {result:?}"
    );

    // Poll until the metadata image reflects the new leader. The unclean
    // election makes broker 1 the leader with ISR=[1]; we assert leadership and
    // broker 1's ISR membership rather than an exact ISR={1}, because once
    // broker 1 leads, the other live replica (broker 2, caught up on the empty
    // log) is legitimately re-admitted to the ISR — asserting exactly [1] would
    // race that re-admission.
    wait_partition_leader(h0, "foo-unclean", 0, 1).await;
    wait_partition_isr_contains(h0, "foo-unclean", 0, 1).await;

    // Clean up.
    shutdown_cluster(cluster).await;
}
