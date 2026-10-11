//! The byte-for-byte replica comparison: produce 100 records into a
//! replication-factor-three topic, then confirm `kafka-dump-log` renders every
//! broker's local segment identically.
//!
//! It is the only durability case that reads the on-disk segments back through
//! the JVM tools, so it carries the container mount handling that the other
//! cases do not need.

use std::{io::Write as _, process::Command};

use assert2::assert;

use crate::{
    jvm_acceptance::KAFKA_IMAGE,
    prepare_replication_topic,
    support::{init_jvm_tracing, jvm_acks_all_producer, start_jvm_cluster},
};

// Replication byte-compare: stand up a 3-broker Krabka cluster, create a
// `replication-factor=3` topic, produce 100 records via the JVM
// `kafka-console-producer`, then run `kafka-dump-log` against each
// broker's local partition file and assert the three dumps are
// byte-identical.
//
// Why fixed ports + `host.docker.internal`: the JVM client opens a
// per-broker connection per partition leader, so every broker's
// advertised listener must be reachable from inside the Kafka tool
// container. The CI workflow already wires `host.docker.internal` on
// the host's `/etc/hosts` to the bridge gateway IP. Controller traffic
// uses host loopback (`127.0.0.1`) — Docker reachability is irrelevant
// for inter-broker.
//
// `kafka-dump-log` ships on the `mirror.gcr.io/confluentinc/cp-kafka:6.1.1` image
// alongside `kafka-topics` / `kafka-console-producer` — it's a standard
// Apache Kafka tool. We mount each broker's partition dir into a fresh
// container as `-v <host>:/data:ro` and dump the first segment file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_replication_byte_compare() {
    const TOPIC: &str = "krabka-replication-itest";

    init_jvm_tracing("krabka_broker=debug,info");

    // Distinct ports from `three_node_jvm_round_trip` (which uses
    // 9192/9292/9392 + 9193/9293/9393). Linux's TIME_WAIT keeps the prior
    // test's sockets bound for ~60s after teardown; running this test
    // back-to-back on the same ports causes `Broker::start` to either fail
    // to bind or to inherit half-closed peer state, which surfaces as
    // "no leader elected within 2 min" on the openraft side.
    let client_ports = [9492u16, 9592, 9692];
    let controller_ports = [9493u16, 9593, 9693];

    let cluster = start_jvm_cluster(client_ports, controller_ports, |_| {}).await;

    let (_bootstrap_1, bootstrap_all) =
        prepare_replication_topic(&cluster[0].0, TOPIC, &client_ports).await;

    // 3. Produce 100 records via kafka-console-producer with acks=all so
    //    each produce response gates on HW = LEO across the full ISR.
    //    Without this the producer returns after leader ack and we end up
    //    dumping followers before their replicators have caught up,
    //    making the byte-compare assert fail spuriously.
    let mut producer_child = jvm_acks_all_producer(KAFKA_IMAGE, &bootstrap_all, TOPIC);
    {
        let stdin = producer_child.stdin.as_mut().expect("stdin");
        for i in 0..100 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(producer_child.stdin.take());
    let prod_out = producer_child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // 4. Wait for replication lag to drain: every broker's local partition log
    //    must reach the full 100 records before we dump them. With acks=all the
    //    produce above already gated on HW=LEO across the ISR, so this resolves
    //    immediately; the awaiter reads each broker's local log end offset
    //    directly (which `kafka-topics --describe` cannot expose).
    for entry in cluster.iter().take(3) {
        entry.0.wait_until_local_log_end_offset(TOPIC, 0, 100).await;
    }

    // 5. For each broker, dump the local partition file via
    //    `kafka-dump-log`. The `-v <host>:/data:ro` mount makes the
    //    broker's on-disk partition directory visible to the tool
    //    container.
    let mut dumps = Vec::with_capacity(3);
    for (i, (_, dir)) in cluster.iter().enumerate() {
        let partition_dir = dir.path().join(format!("{TOPIC}-0"));
        let log_file = partition_dir.join("00000000000000000000.log");
        assert!(
            log_file.exists(),
            "broker {} missing log file: {log_file:?}",
            i + 1,
        );
        let mount = format!("{}:/data:ro", partition_dir.display());
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &mount,
                KAFKA_IMAGE,
                "kafka-dump-log",
                "--files",
                "/data/00000000000000000000.log",
                "--print-data-log",
            ])
            .output()
            .expect("spawn dump-log");
        assert!(
            out.status.success(),
            "dump-log failed for broker {}: stdout={}, stderr={}",
            i + 1,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        dumps.push(String::from_utf8_lossy(&out.stdout).to_string());
    }

    // 6. All three dumps should be byte-identical.
    assert!(dumps[0] == dumps[1], "broker 1 vs broker 2 dump differ");
    assert!(dumps[1] == dumps[2], "broker 2 vs broker 3 dump differ");

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
