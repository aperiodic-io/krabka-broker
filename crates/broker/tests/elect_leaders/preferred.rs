//! The operator-triggered preferred election: `ElectLeaders` with
//! `election_type=0` moves leadership back to `replicas[0]` once that replica
//! is alive and in the ISR again.

use {
    crate::{
        cluster_lock,
        kafka_wire::ElectionExchangeSetup,
        support::{
            admin::{ElectionKind, ElectionSetup},
            registered_cluster, shutdown_cluster,
            topics::AssignedTopicSetup,
        },
        wait::{wait_isr_contains, wait_partition_exists, wait_partition_leader},
        wire::{
            CLIENT_ID, create_topic_plaintext, drive_elect_all_partitions, drive_elect_leaders,
        },
    },
    assert2::assert,
};

/// A 3-broker PLAINTEXT cluster with an rf=2 topic where `replicas = [1, 2]`.
///
/// Scenario:
/// 1. Kill broker 1, the preferred replica. Broker 3 keeps the raft quorum
///    (2/3).
/// 2. Broker 2 becomes partition leader through the automatic on-broker-dead
///    path.
/// 3. Revive broker 1 with Rejoin. It catches up on replication and expands
///    back into the ISR.
/// 4. Send `ElectLeaders Preferred` with `election_type=0` over the wire.
/// 5. Assert per-partition `error_code` = 0. Poll until leader == 1 again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_election_via_wire_returns_success() {
    let _g = cluster_lock().lock().await;

    let mut cluster = registered_cluster(3).await;

    // All three brokers' addresses captured before any shutdowns.
    let broker1_addr = cluster[0].1.listen_addr;

    // Create a rf=2 topic. With 3 registered brokers the scheduler assigns
    // replicas [1, 2]; broker 1 is the preferred (first) replica.
    create_topic_plaintext(broker1_addr, AssignedTopicSetup {
        name: ("foo-preferred").into(),
        replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
    })
    .await;

    // Wait for all rf brokers to see the partition in their image.
    wait_partition_exists(&cluster[0].0, "foo-preferred", 0).await;
    wait_partition_exists(&cluster[1].0, "foo-preferred", 0).await;

    let initial_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap_or(1);
    eprintln!("initial partition leader: {initial_leader}");

    // Kill broker 1 (index 0). Raft quorum {2, 3} can still commit.
    let (dead_h, dead_cfg, dead_dir) = cluster.remove(0);
    dead_h.shutdown().await;

    // Wait for the surviving cluster to elect a new partition leader
    // (i.e., not broker 1).
    cluster[0]
        .0
        .wait_until_partition_leader_changed("foo-preferred", 0, krabka_broker::NodeId(1))
        .await;
    let new_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap();
    eprintln!("new partition leader after broker 1 death: {new_leader}");
    let _ = new_leader; // used for diagnostics

    // Revive broker 1 (Rejoin reads the existing raft log).
    // The surviving 2/3 quorum continues committing.
    let mut revived_cfg = dead_cfg.clone();
    revived_cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let revived_h = krabka_broker::Broker::start(revived_cfg)
        .await
        .expect("rejoin broker 1");

    // Wait for broker 1 to be back in the ISR on broker 2's view.
    // The ISR expand is committed by the surviving raft leader so broker 2
    // (or 3) must reflect it.
    wait_isr_contains(&cluster[0].0, "foo-preferred", 0, 1).await;

    // The ElectLeaders request MUST go to the raft leader, which is the only
    // broker with an authoritative liveness state (it receives all heartbeats).
    // After broker 1's revive it heartbeats to the raft leader, which marks
    // it as Alive. We discover which surviving broker is the raft leader and
    // send there.
    //
    // Inlined here rather than extracted into a helper to avoid a complex
    // `&[(BrokerHandle, BrokerConfig, TempDir)]` function signature that
    // triggers the Rust 1.95 annotate-snippets ICE in clippy::type_complexity.
    let elect_addr = {
        let leader = cluster[0].0.wait_until_controller_leader().await;
        let pos = cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("raft leader must be one of the surviving brokers");
        // A PREFERRED election refuses with PREFERRED_NOT_ALIVE until broker 1
        // is electable, and the ISR wait above does not imply that: the ISR
        // expand is a replicated record proposed by the data leader, while a
        // revived broker becomes electable only once its first post-revive
        // heartbeat reaches the controller and the controller publishes it
        // unfenced. Settling on the ISR alone races that, which is what made
        // this test flaky under load. Settle on the predicate the election
        // actually reads -- the replicated set, which trails the controller's
        // own registry by up to one liveness tick.
        cluster[pos].0.wait_until_broker_electable(1).await;
        cluster[pos].1.listen_addr
    };
    eprintln!("sending ElectLeaders Preferred to raft leader at {elect_addr}");

    // Now send ElectLeaders Preferred (election_type=0). Broker 1 is the
    // preferred replica (replicas[0]) and is now back in ISR and alive.
    let result = drive_elect_leaders(elect_addr, ElectionExchangeSetup {
        client_id: CLIENT_ID,
        election: ElectionSetup {
            topic: ("foo-preferred").to_string(),
            ..Default::default()
        },
    })
    .await;
    assert!(
        result == vec![(0, 0)],
        "expected error_code=0 for PREFERRED election; got {result:?}"
    );

    // Poll until the image reflects broker 1 as leader again.
    wait_partition_leader(&cluster[0].0, "foo-preferred", 0, 1).await;
    wait_partition_leader(&revived_h, "foo-preferred", 0, 1).await;

    // Clean up.
    revived_h.shutdown().await;
    shutdown_cluster(cluster).await;
    drop(dead_dir);
}

/// KIP-460: a request that names no topics asks about every partition, and
/// Kafka answers it only with the partitions it acted on --
/// `ReplicationControlManager.electLeaders` drops every `ELECTION_NOT_NEEDED`
/// row in that shape because "we do not return partitions which already have
/// the desired leader". Returning them makes
/// `kafka-leader-election --all-topic-partitions` on a healthy cluster print a
/// "valid replica already elected" line for every partition, internal topics
/// included, where Apache Kafka prints nothing at all.
///
/// The named shape is unaffected: a client that asked about one partition is
/// told what happened to it, code 84 included. Both halves are checked here,
/// against the same untouched cluster, so a change that filtered too much
/// would fail on the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn electing_every_partition_omits_the_ones_already_on_their_preferred_leader() {
    let _g = cluster_lock().lock().await;

    let cluster = registered_cluster(3).await;
    let broker1_addr = cluster[0].1.listen_addr;

    // Nothing has failed over, so every partition is already led by its
    // preferred replica and no election is needed anywhere.
    create_topic_plaintext(broker1_addr, AssignedTopicSetup {
        name: ("foo-all-partitions").into(),
        replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
    })
    .await;
    wait_partition_exists(&cluster[0].0, "foo-all-partitions", 0).await;

    let rows = drive_elect_all_partitions(broker1_addr, ElectionKind::Preferred).await;

    assert!(
        rows.is_empty(),
        "a cluster with nothing to elect must answer --all-topic-partitions with no rows, \
         got {rows:?}"
    );

    // The same partition, named explicitly, still reports why it was skipped.
    let named = drive_elect_leaders(broker1_addr, ElectionExchangeSetup {
        client_id: CLIENT_ID,
        election: ElectionSetup {
            topic: ("foo-all-partitions").to_string(),
            ..Default::default()
        },
    })
    .await;
    assert!(
        named == vec![(0, krabka_broker::codes::ELECTION_NOT_NEEDED)],
        "a named partition must still report ELECTION_NOT_NEEDED, got {named:?}"
    );
}
