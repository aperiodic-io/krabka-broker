//! The three-node quorum round-trip: produce through one broker, consume
//! through another, then kill the controller leader and confirm a survivor
//! still answers `Metadata`.
//!
//! It is the only durability case that exercises controller re-election after
//! the leader is killed, rather than the data path of a replicated partition,
//! so it stands on its own.

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE, docker_run_kafka_tool};

// Three-node quorum: produce on one node, consume on another, then kill
// the controller leader and assert the surviving brokers still answer
// Metadata. Same multi-thread runtime caveat as the other tests; we ask
// for 4 workers because we have three brokers + the test driver all
// making blocking docker calls.
//
// Fixed ports per node because docker containers must be able to reach
// the brokers via `host.docker.internal:<client-port>`. The advertised
// listener uses the same hostname so the AdminClient's post-Metadata
// reconnect resolves correctly. Controller ports use `127.0.0.1` for
// inter-broker traffic — all three Krabka brokers live on the host's
// loopback, so docker reachability is not required for the controller
// listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_jvm_round_trip() {
    const TOPIC: &str = "krabka-quorum-itest";

    crate::support::init_jvm_tracing("krabka_broker=debug,info");

    let client_ports = [9192u16, 9292, 9392];
    let controller_ports = [9193u16, 9293, 9393];

    let mut cluster =
        crate::support::start_jvm_cluster(client_ports, controller_ports, |_| {}).await;

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_2 = format!("host.docker.internal:{}", client_ports[1]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create topic via node 1.
    crate::jvm_acceptance::create_plain_console_topic_at(&bootstrap_1, TOPIC);

    // 2. Wait for the topic to propagate from node 1 (where kafka-topics
    //    created it) to node 2 (where we'll produce) by observing node 2's
    //    committed metadata image directly.
    cluster[1].0.wait_until_partition_present(TOPIC, 0).await;

    // 3. Produce via kafka-console-producer (JVM). The JVM AdminClient
    //    transparently follows the partition leader: it asks any node's
    //    Metadata for the leader of partition 0 and opens a fresh
    //    connection to that broker's advertised address. The
    //    Rust producer doesn't yet route across brokers per partition,
    //    so we use the JVM tool here; cross-broker producer routing is
    //    a follow-up that the Rust client will pick up.
    let mut producer_child_command =
        crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
            image: KAFKA_IMAGE,
            args: &[
                "kafka-console-producer",
                "--bootstrap-server",
                &bootstrap_2,
                "--topic",
                TOPIC,
            ],
            input: crate::support::ContainerInput::Attached,
            ..Default::default()
        });
    let producer_out = crate::support::jvm_stdin_output(&mut producer_child_command, b"a\nb\nc\n");
    assert!(
        producer_out.status.success(),
        "JVM producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 4. Consume via node 3.
    let out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["a", "b", "c"] {
        assert!(s.contains(needle), "missing {needle} in {s:?}");
    }

    // 5. Find the controller leader, kill it.
    let mut leader_idx = None;
    for (i, (h, _)) in cluster.iter().enumerate() {
        let want = u64::try_from(i + 1).unwrap();
        if h.controller_leader_id() == Some(krabka_broker::NodeId(want)) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("a leader exists");
    let (leader, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;
    // intentional: allow the surviving voters to run a controller re-election
    // after the leader was killed. There is no "controller leader changed"
    // awaiter, and a survivor's cached leader value can momentarily read stale,
    // so a fixed settle window is used rather than risk a stale-value wait.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 6. Survivor still answers Metadata via kafka-topics --list.
    let survivor_idx = (0..client_ports.len())
        .find(|i| *i != leader_idx)
        .expect("at least one survivor");
    let survivor_bootstrap = format!("host.docker.internal:{}", client_ports[survivor_idx]);
    let list_out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        &survivor_bootstrap,
    ]);
    let list_s = String::from_utf8_lossy(&list_out.stdout);
    assert!(
        list_s.contains(TOPIC),
        "topic missing after leader kill: {list_s:?}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
