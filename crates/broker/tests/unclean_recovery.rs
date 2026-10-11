// rustc 1.95 clippy ICEs on `clippy::pedantic` / `clippy::unwrap_in_result`
// for files that build Kafka wire frames with `.expect()` inside a
// `Result`-returning helper — same upstream annotate-snippets bug noted in
// `tests/elect_leaders.rs`. Suppress locally; the rest of the workspace
// still enforces the full lint gate.

//! KIP-966 end-to-end: offset-aware **unclean recovery** elects the survivor
//! that is known to be complete, and only falls back to the most complete
//! surviving log when there is no such replica. Neither answer is "the first
//! alive replica".
//!
//! The tests use a 3-broker PLAINTEXT cluster, for the same reason as
//! `tests/elect_leaders.rs`. A 3-node raft quorum survives one dead node, and
//! the authorizer compat shim lets the wire path through without SASL.
//!
//! Both cases run the same scenario, in `run_unclean_recovery`, and differ
//! only in the eligible-leader-replica set the partition carries and the
//! leader that must win:
//!
//! 1. A 3-broker cluster with topic "t", 1 partition, and RF=3, so the
//!    replicas are `[1, 2, 3]`.
//! 2. Set `unclean.recovery.strategy=Aggressive` on the topic, so the UNCLEAN
//!    election routes through the offset-aware Unclean Recovery Manager (URM).
//! 3. Take the partition offline. The test injects a `PartitionRecord` whose
//!    leader and ISR is a dead phantom node, 99. Liveness reports 99 dead, so
//!    the partition has no live leader, and its ISR holds no live member. A
//!    demotion of the leader to a broker that does not exist also stops the
//!    real replication fetchers, which keeps the next step deterministic.
//! 4. Force the replicas' local logs to **diverge** deterministically with the
//!    `produce_records_for_test` accessor. It appends directly to each
//!    broker's hosted partition log, so the per-broker LEOs differ: broker 2
//!    gets the strictly highest LEO, broker 3 the middle one, and broker 1 the
//!    lowest. All three keep `current_leader_epoch == 0`, so only
//!    `log_end_offset` decides the most-complete-log ranking.
//! 5. Publish the partition's eligible-leader-replica set, when the case has
//!    one. It is written last so that no controller path re-derives it between
//!    the write and the election.
//! 6. Trigger recovery with `ElectLeaders(UNCLEAN)` sent to the raft leader.
//!    The URM polls brokers 1, 2, and 3 over the real `GetReplicaLogInfo` wire
//!    path and elects one of them.
//! 7. Assert the new partition leader, and that the ISR collapses to it.
//!
//! With no ELR (`unclean_recovery_elects_longest_log_replica`) the winner is
//! broker 2, the highest LEO, and not broker 1, the first alive replica. With
//! an ELR of `{3}` (`unclean_recovery_prefers_an_eligible_leader_replica`) the
//! winner is broker 3, whose log is *shorter* than broker 2's: an ELR member
//! held every committed record when it left the ISR, so electing it loses
//! nothing, and a longer non-ELR log is only a guess. Broker 3 is neither the
//! first alive replica nor the most complete log, so the assertion separates
//! the ELR rule from both of the others.
//!
//! These tests are gated to non-Windows, to match the multi-broker test
//! convention. The openraft `debug_assert!` races on the hosted Windows
//! scheduler are unrelated.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_metadata::{
    BrokerConfigRecord, MetadataRecord, PartitionElrRecord, PartitionRecord, TopicConfigRecord,
};

mod kafka_wire;
mod support;

/// The controller-managed broker config that carries the witness role. It is
/// `crate::config_keys::BROKER_WITNESS`, which a test crate cannot name, and a
/// witness node publishes it for itself at registration.
const WITNESS_CONFIG_KEY: &str = "broker.witness";

// ─────────────────────────────────────────────────────────────────────────────
// Minimal wire helpers — bare TCP on PLAINTEXT, no SASL.
// Named election requests use the shared wire driver.
// ─────────────────────────────────────────────────────────────────────────────

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-unclean-test";

/// Creates a topic on a PLAINTEXT broker. The authorizer compat shim, with no
/// `super_users` and no ACLs, lets the request through.
///
/// The topic has one partition on `replicas`, in that order, so the tests
/// know which broker leads it: an automatic placement starts at a random
/// broker.
use crate::kafka_wire::create_assigned_topic_plaintext as create_topic_plaintext;
/// Drives `ElectLeaders` over a fresh PLAINTEXT connection. It asserts that
/// the top-level `error_code == 0`, and returns the per-partition
/// `(partition_id, error_code)` rows for `topic`.
use crate::kafka_wire::elect_leaders_plaintext as drive_elect_leaders;
use crate::kafka_wire::{AssignedTopicExchangeSetup, ElectionExchangeSetup};

// ─────────────────────────────────────────────────────────────────────────────
// Polling helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Waits until `handle` sees `(topic, partition)` in its metadata image.
async fn wait_partition_hosted(handle: &BrokerHandle, topic: &str, partition: i32) {
    // Event-driven: `has_partition` reads the committed metadata image, which is
    // exactly the source `wait_until_partition_present` subscribes to.
    handle.wait_until_partition_present(topic, partition).await;
}

use crate::support::{
    admin::{ElectionKind, ElectionSetup},
    partitions::{wait_partition_isr_only, wait_partition_leader},
    registered_cluster, shutdown_cluster,
    topics::AssignedTopicSetup,
};

/// The recovery strategy that routes an UNCLEAN election through the URM.
fn topic_config(topic: &str) -> MetadataRecord {
    MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_string(),
        overrides: [(
            "unclean.recovery.strategy".to_string(),
            "Aggressive".to_string(),
        )]
        .into_iter()
        .collect(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// The offset-aware unclean-recovery path elects the survivor with the
/// **highest LEO** when nothing better is known about any of them. That result
/// is different from a simple "first alive replica" pick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_elects_longest_log_replica() {
    run_unclean_recovery(None, None, 2).await;
}

/// KIP-966: a surviving eligible leader replica outranks a longer log that is
/// not one. Broker 3 is in the published ELR and holds fewer records than
/// broker 2, and broker 3 must still win: it is known to hold every committed
/// record, and broker 2's longer log is only the longest one that answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_prefers_an_eligible_leader_replica() {
    run_unclean_recovery(Some("3"), None, 3).await;
}

/// A witness replicates the partition and can be published as an eligible
/// leader replica, and it must still never lead: `broker.witness` means the
/// node serves no client, so a partition it leads is as unusable as the
/// offline one. Broker 3 is partition 0's only ELR member here and carries the
/// role, so recovery has to fall back to broker 2's longest surviving log --
/// the answer that is reported as data-losing rather than the one that is
/// reported as free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_never_elects_a_witness_from_the_elr() {
    run_unclean_recovery(Some("3"), Some(3), 2).await;
}

/// Leaves partition 0 of `topic` offline on an RF=3 cluster, with the three
/// surviving replicas holding logs of three different lengths.
///
/// `handles` are brokers 1, 2 and 3 in that order, and `addr` is any broker's
/// listener. On return the partition's leader is a node that never existed, so
/// nothing will elect a leader on its own, and broker 2 holds the strictly
/// highest LEO with broker 3 in the middle and broker 1 lowest.
async fn offline_partition_with_diverged_logs(
    addr: SocketAddr,
    topic: &str,
    handles: [&BrokerHandle; 3],
) {
    let [h1, h2, h3] = handles;
    // ── Create the RF=3 topic. Partition 0's replica assignment is
    //    [1, 2, 3]; broker 1 is the preferred/first. ──
    create_topic_plaintext(
        addr,
        AssignedTopicExchangeSetup {
            client_id: CLIENT_ID,
            topic: AssignedTopicSetup::three_replicas(topic),
        },
    )
    .await;
    wait_partition_hosted(h1, topic, 0).await;
    wait_partition_hosted(h2, topic, 0).await;
    wait_partition_hosted(h3, topic, 0).await;

    // Event-driven: await the partition's presence in the image, then read the
    // record the loop captured (h1 already saw it via `wait_partition_hosted`).
    h1.wait_until_partition_present(topic, 0).await;
    let pr_before = h1
        .partition_record_for_test(topic, 0)
        .expect("partition record present after wait_until_partition_present");
    eprintln!("partition before divergence: {pr_before:?}");
    assert!(
        pr_before.replicas
            == vec![
                krabka_broker::NodeId(1),
                krabka_broker::NodeId(2),
                krabka_broker::NodeId(3)
            ],
        "expected RF=3 replicas [1,2,3]; got {:?}",
        pr_before.replicas
    );

    // ── Set unclean.recovery.strategy=Aggressive so UNCLEAN routes through
    //    the offset-aware Unclean Recovery Manager. ──
    h1.submit_metadata_record_for_test(topic_config(topic))
        .await
        .expect("set unclean.recovery.strategy=Aggressive");

    // ── Take the partition offline FIRST: inject a PartitionRecord whose
    //    leader and ISR are a dead phantom node (99, never heartbeated →
    //    liveness=dead). Replicas stay [1, 2, 3] so the URM polls the three
    //    real survivors. Crucially, demoting the leader to a non-existent
    //    broker stops the real replication fetchers (no broker is leader),
    //    so the direct-append divergence we set up next stays deterministic
    //    — otherwise the leader's replicator races with our test appends. ──
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: vec![
            krabka_broker::NodeId(1),
            krabka_broker::NodeId(2),
            krabka_broker::NodeId(3),
        ],
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    h1.submit_metadata_record_for_test(forged)
        .await
        .expect("inject dead-leader PartitionRecord");
    wait_partition_isr_only(h1, topic, 0, &[99]).await;
    // `wait_partition_hosted` reads the metadata image. Each broker creates its
    // local partition, and installs the dead leader, on its own reconcile
    // after that, and the direct appends below need all three partitions.
    for handle in [h1, h2, h3] {
        handle
            .wait_until_local_partition_target(
                topic,
                0,
                krabka_broker::NodeId(99),
                pr_before.leader_epoch.next(),
            )
            .await;
    }
    // Give the supervisors a beat to observe the leader change and tear down
    // any in-flight replication fetchers before we diverge the logs.
    // intentional: fetcher teardown is a background reconcile action with no
    // metadata-image or metric signal to await; the barrier keeps the direct
    // appends deterministic.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Force the surviving replicas' local logs to DIVERGE deterministically.
    //    `produce_records_for_test` appends directly to each broker's hosted
    //    partition log, so we set known, distinct LEOs by adding a different
    //    count of records on top of whatever each already holds:
    //        broker 2 gets the most → strictly-highest LEO.
    //    All keep current_leader_epoch == 0, so selection is decided by LEO.
    //    Broker 2 is NOT the first-alive replica (that's broker 1), which is
    //    what makes the assertion meaningful. ──
    h1.produce_records_for_test(topic, 0, 2)
        .await
        .expect("produce to broker 1");
    h3.produce_records_for_test(topic, 0, 4)
        .await
        .expect("produce to broker 3");
    h2.produce_records_for_test(topic, 0, 20)
        .await
        .expect("produce to broker 2");
    let end1 = h1.local_log_end_offset(topic, 0).expect("leo b1");
    let end2 = h2.local_log_end_offset(topic, 0).expect("leo b2");
    let end3 = h3.local_log_end_offset(topic, 0).expect("leo b3");
    eprintln!("LEO end offsets: b1={end1} b2={end2} b3={end3}");
    assert!(
        end2 > end1 && end2 > end3,
        "broker 2 must hold the strictly-highest LEO (b1={end1} b2={end2} b3={end3})"
    );
}

/// A 3-broker PLAINTEXT cluster with an RF=3 topic, whose replicas are
/// `[1, 2, 3]`. It runs the scenario the module docs describe with `elr`
/// published as partition 0's eligible-leader-replica set, and asserts that
/// `expected_leader` wins the `ElectLeaders(UNCLEAN)` that follows.
async fn run_unclean_recovery(elr: Option<&str>, witness: Option<u64>, expected_leader: u64) {
    let _g = support::cluster_lock().lock().await;

    let cluster = registered_cluster(3).await;

    let topic = "t";
    let addr = cluster[0].1.listen_addr;
    let h1 = &cluster[0].0; // broker_id 1
    let h2 = &cluster[1].0; // broker_id 2
    let h3 = &cluster[2].0; // broker_id 3

    offline_partition_with_diverged_logs(addr, topic, [h1, h2, h3]).await;

    // ── Publish `broker.witness=true` for the node this case makes a witness,
    //    which is the path a witness node's own registration takes. It goes in
    //    before the ELR value below: a broker-config record carries no
    //    partition change, so no controller path recomputes the ELR from it,
    //    and this keeps the last write the one the URM reads. ──
    if let Some(id) = witness {
        h1.submit_metadata_record_for_test(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_broker::NodeId(id),
            config_name: WITNESS_CONFIG_KEY.to_string(),
            config_value: Some("true".to_string()),
        }))
        .await
        .expect("publish broker.witness for the witness node");
        // Event-driven: await the role in the image the URM reads.
        h1.wait_for_image(|img| {
            img.broker_config(krabka_broker::NodeId(id))
                .and_then(|configs| configs.get(WITNESS_CONFIG_KEY))
                .is_some_and(|value| value == "true")
        })
        .await;
    }

    // ── Publish the eligible-leader-replica set, if this case has one. ──
    if let Some(elr) = elr {
        let eligible_leader_replicas = elr
            .split(',')
            .map(|id| krabka_broker::NodeId(id.parse().expect("ELR broker id")))
            .collect();
        h1.submit_metadata_record_for_test(MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: topic.to_string(),
            partition: 0,
            eligible_leader_replicas,
            last_known_elr: vec![],
        }))
        .await
        .expect("publish the eligible leader replicas");
        h1.wait_for_image(|img| {
            img.partition_elr(topic, 0)
                .0
                .iter()
                .map(|id| id.get().to_string())
                .collect::<Vec<_>>()
                .join(",")
                == elr
        })
        .await;
    }

    // ── ElectLeaders(UNCLEAN) must reach the raft leader, the only node that
    //    runs the URM and has authoritative liveness state. Discover it. ──
    // Event-driven: await a non-zero elected controller leader (same watch channel
    // `controller_leader_id` reads), then resolve its listen address.
    let elect_addr = {
        let leader = h1.wait_until_controller_leader().await;
        let pos = cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("elected raft leader must be one of the cluster nodes");
        cluster[pos].1.listen_addr
    };
    eprintln!("sending ElectLeaders UNCLEAN to raft leader at {elect_addr}");

    // ── Trigger offset-aware recovery (election_type = 1 = UNCLEAN). ──
    let result = drive_elect_leaders(
        elect_addr,
        ElectionExchangeSetup {
            client_id: CLIENT_ID,
            election: ElectionSetup {
                topic: (topic).to_string(),
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

    // ── The load-bearing assertion: the URM elected the replica the rules
    //    name, and not merely the first alive one. ──
    wait_partition_leader(h1, topic, 0, expected_leader).await;
    let final_leader = h1
        .partition_leader_for_test(topic, 0)
        .expect("leader present");
    assert!(
        final_leader == expected_leader,
        "URM must elect broker {expected_leader} with elr={elr:?}; got \
         leader={final_leader}"
    );
    // ISR collapses to the singleton elected leader.
    wait_partition_isr_only(h1, topic, 0, &[expected_leader]).await;

    // Clean up.
    shutdown_cluster(cluster).await;
}
