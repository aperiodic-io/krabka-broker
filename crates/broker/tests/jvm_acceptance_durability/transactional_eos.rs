//! The transactional exactly-once case: a compiled JVM `KafkaProducer` commits
//! one transaction, aborts another, and appends a later record after rolling
//! each batch into its own segment. The `read_committed` and
//! `read_uncommitted` isolation levels must disagree in exactly that way.
//!
//! It is the only durability case that needs the split `EXTERNAL` and
//! `INTERNAL` listeners and the image that ships `javac`, which is why it is
//! its own file.

use std::{io::Write, process::Stdio};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use crate::jvm_acceptance::{KAFKA_IMAGE_TXN, TRANSACTIONAL_PRODUCER_JAVA, docker_run_kafka_tool};

/// A zombie-fencing probe (KIP-360): two JVM producers share one
/// `transactional.id`, the second takes the id over, and the first must be
/// fenced the next time it tries to write.
///
/// This is the fence Kafka documents on `transactional.id` itself: the second
/// `initTransactions` bumps the epoch, and the coordinator then refuses the
/// first producer's stale `(producer_id, producer_epoch)` on its
/// `AddPartitionsToTxn`. The JVM client turns both `PRODUCER_FENCED` and
/// `INVALID_PRODUCER_EPOCH` there into a fatal `ProducerFencedException`
/// (`TransactionManager.AddPartitionsToTxnHandler`), so that is the name the
/// probe expects. A broker that admits the stale epoch instead lets the zombie
/// write, which is the failure this case is here to catch.
///
/// The first producer acquires the id and then writes nothing until after the
/// takeover, so the takeover is the only thing that can have moved its epoch
/// and the refusal cannot be read as anything else.
///
/// The probe cannot instead drive `InitProducerId` with the stale identity:
/// the JVM client sends that request's `producer_id`/`producer_epoch` fields
/// only for its own internal KIP-360 epoch bump, and a second public
/// `initTransactions()` on a producer that has committed is refused locally --
/// `TransactionManager` allows the `INITIALIZING` transition only from
/// `UNINITIALIZED`, `COMMITTING_TRANSACTION` or `ABORTING_TRANSACTION`, so it
/// throws `IllegalStateException` before any request reaches the broker. The
/// broker's `PRODUCER_FENCED` answer on that request is pinned by the
/// `init_producer_id` unit tests instead.
///
/// The probe prints `ZOMBIEPROBE fenced=<exception simple name>`, or
/// `ZOMBIEPROBE fenced=none` when nothing fenced it at all.
const ZOMBIE_PRODUCER_JAVA: &str = krabka_macros::java_string_producer_source!(
    r#"

public final class ZombieProducer {
  public static void main(String[] args) throws Exception {
"#,
    "args[0]",
    r#"    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "zombie-tid");

    // The first instance acquires the id and stops there: it writes nothing
    // before the takeover, so the only thing that can move its epoch is the
    // takeover itself.
    KafkaProducer<String, String> first = new KafkaProducer<>(config);
    first.initTransactions();

    // A second instance of the same console producer takes the id over. Its
    // InitProducerId carries no identity, so it is admitted and bumps the
    // epoch; the first instance is now a zombie holding the old one.
    KafkaProducer<String, String> second = new KafkaProducer<>(config);
    second.initTransactions();

    // The zombie writes on. Its AddPartitionsToTxn carries the epoch the
    // takeover superseded, and the coordinator must refuse it.
    String fenced = "none";
    try {
      first.beginTransaction();
      first.send(new ProducerRecord<>(args[1], "zombie-stale")).get();
      first.commitTransaction();
    } catch (Throwable error) {
      Throwable cause = error;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      fenced = cause.getClass().getSimpleName();
    }
    System.out.println("ZOMBIEPROBE fenced=" + fenced);

    for (KafkaProducer<String, String> producer : java.util.List.of(second, first)) {
      try {
        producer.close(java.time.Duration.ofSeconds(5));
      } catch (Throwable ignored) {
        // A fenced producer may fail to close; the probe has its answer.
      }
    }
  }
}
"#
);

// Transactional EOS smoke: stand up a 3-broker Krabka cluster, compile and
// run a small official JVM KafkaProducer client that commits 6 records and
// aborts 2, appends a later record, then verifies read_committed and
// read_uncommitted isolation across sealed segments.
//
// Fixed external ports 9792/9892/9992, controller ports 9793/9893/9993,
// and loopback-only inter-broker ports 9794/9894/9994. The split listeners
// let Docker clients use host.docker.internal while host-side brokers use
// loopback, so local Linux runs do not need an /etc/hosts entry.
//
// Same multi-thread runtime caveat as the other multi-broker tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn transactional_console_producer_eos() {
    const TOPIC: &str = "krabka-txn-itest";
    const ZOMBIE_TOPIC: &str = "krabka-txn-zombie";
    /// Segment size every broker in this cluster boots with, so that each
    /// batch the transactional producer writes lands in a segment of its own
    /// and the abort marker's transaction index is sealed before the reads.
    ///
    /// It is a broker default, not a topic override: Kafka's topic-level
    /// `segment.bytes` has a 1 MiB floor that krabka matches, and the only
    /// topic config below it — Kafka 4.x's `internal.segment.bytes` — is
    /// rejected client-side by the `TopicCommand` in the image this suite
    /// pins. Kafka floors its broker-level `log.segment.bytes` at 1 MiB too,
    /// but that validates a parsed broker config; the harness sets the struct
    /// field directly, as Kafka's own tests do. 128 bytes is above one batch
    /// (a one-record batch or a marker is about 75 bytes; `UnifiedLog.append`
    /// refuses a batch larger than the segment with `RECORD_LIST_TOO_LARGE`) and
    /// below two, so no batch can ever fit twice.
    const SEGMENT_SIZE: krabka_units::ByteSize = krabka_units::bytes(128);

    crate::support::init_jvm_tracing("krabka_broker=debug,info");

    let client_ports = [9792u16, 9892, 9992];
    let controller_ports = [9793u16, 9893, 9993];
    let inter_broker_ports = [9794u16, 9894, 9994];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Parallel spawn — sequential startup deadlocks waiting for quorum.
    let mut tempdirs = Vec::with_capacity(3);
    let mut spawns = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let listen_addr = format!("0.0.0.0:{}", client_ports[i])
            .parse()
            .expect("static addr");
        let advertised_listener = format!("host.docker.internal:{}", client_ports[i]);
        let inter_broker_addr = format!("127.0.0.1:{}", inter_broker_ports[i])
            .parse()
            .expect("static addr");
        let cfg = BrokerConfig {
            // Neither topic below overrides a segment size, so both inherit
            // this one. See `SEGMENT_SIZE`.
            log_config: LogConfig {
                segment_size: SEGMENT_SIZE,
                ..LogConfig::default()
            },
            listeners: vec![
                krabka_broker::config::ListenerSpec {
                    name: "EXTERNAL".to_string(),
                    bind_addr: listen_addr,
                    advertised: advertised_listener.clone(),
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                    principal_mapper: krabka_broker::SslPrincipalMapper::default(),
                },
                krabka_broker::config::ListenerSpec {
                    name: "INTERNAL".to_string(),
                    bind_addr: inter_broker_addr,
                    advertised: inter_broker_addr.to_string(),
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                    principal_mapper: krabka_broker::SslPrincipalMapper::default(),
                },
            ],
            inter_broker_listener_name: "INTERNAL".to_string(),
            ..crate::support::jvm_broker_config(
                dir.path().to_path_buf(),
                crate::support::JvmBrokerSetup {
                    node: krabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
                    listen: listen_addr,
                    controller: format!("0.0.0.0:{}", controller_ports[i])
                        .parse()
                        .expect("static addr"),
                    advertised: advertised_listener.clone(),
                    voters: crate::support::controller_voters(&voters),
                },
            )
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (sp, dir) in spawns.into_iter().zip(tempdirs) {
        cluster.push((sp.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create the topic via node 1.
    crate::jvm_acceptance::create_plain_console_topic_at(&bootstrap_1, TOPIC);
    // Wait for the partition to materialize on the broker that leads it: the
    // producer's first batch must not land before the log exists with the
    // segment size the cluster booted with.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !cluster.iter().any(|(broker, _)| {
        broker
            .partition_log_config_for_test(TOPIC, 0)
            .is_some_and(|config| config.segment_size == SEGMENT_SIZE)
    }) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the transaction-log partition never materialized with a {SEGMENT_SIZE:?} segment"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // 2. Compile the small Java helper against the image's Kafka client jars.
    //    It writes one committed transaction, one aborted transaction, and a
    //    later record that seals the abort marker's transaction index.
    let mut producer = crate::support::jvm_docker_command(crate::support::JvmDockerSetup { image: "--entrypoint", args: &["bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/TransactionalProducer.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/TransactionalProducer.java; \
               java -cp "/tmp:$CP" TransactionalProducer "$1" "$2""#,
            "--",
            &bootstrap_1,
            TOPIC,
        ], input: crate::support::ContainerInput::Attached, ..Default::default() })
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn transactional Java producer");
    producer
        .stdin
        .as_mut()
        .expect("producer stdin")
        .write_all(TRANSACTIONAL_PRODUCER_JAVA.as_bytes())
        .expect("write Java helper");
    drop(producer.stdin.take());
    let producer_out = producer.wait_with_output().expect("wait Java producer");
    eprintln!(
        "KRABKA[test] transactional Java producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "transactional Java producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        String::from_utf8_lossy(&producer_out.stdout).contains("TXNPROBE OK"),
        "transactional Java producer did not report success: {}",
        String::from_utf8_lossy(&producer_out.stdout),
    );

    // 3. Brief pause to let commit markers propagate through the log.
    // intentional: transactional commit-marker propagation and LSO advance are
    // not in the metadata image and have no krabka awaiter/metric.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 4. read_committed must return the committed transaction and the later
    // record, even though the abort entry is now in a sealed segment.
    let committed_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "7",
        "--timeout-ms",
        "20000",
    ]);
    let committed_stdout = String::from_utf8_lossy(&committed_out.stdout);
    let committed: Vec<_> = committed_stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(
        committed
            == [
                "committed-0",
                "committed-1",
                "committed-2",
                "committed-3",
                "committed-4",
                "committed-5",
                "after-abort",
            ],
        "read_committed returned the wrong records: {committed_stdout}",
    );

    // 5. read_uncommitted must return both transactions and the later record
    // in log order.
    let uncommitted_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_uncommitted",
        "--from-beginning",
        "--max-messages",
        "9",
        "--timeout-ms",
        "20000",
    ]);
    let uncommitted_stdout = String::from_utf8_lossy(&uncommitted_out.stdout);
    let uncommitted: Vec<_> = uncommitted_stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(
        uncommitted
            == [
                "committed-0",
                "committed-1",
                "committed-2",
                "committed-3",
                "committed-4",
                "committed-5",
                "aborted-0",
                "aborted-1",
                "after-abort",
            ],
        "read_uncommitted returned the wrong records: {uncommitted_stdout}",
    );

    // 6. KIP-360: a second producer on one transactional.id fences the first,
    // and the first learns it as a fatal `ProducerFencedException` the next
    // time it writes.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        ZOMBIE_TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        &bootstrap_1,
    ]);
    let mut zombie = crate::support::jvm_docker_command(crate::support::JvmDockerSetup { image: "--entrypoint", args: &["bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/ZombieProducer.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/ZombieProducer.java; \
               java -cp "/tmp:$CP" ZombieProducer "$1" "$2""#,
            "--",
            &bootstrap_1,
            ZOMBIE_TOPIC,
        ], input: crate::support::ContainerInput::Attached, ..Default::default() })
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn zombie Java producer");
    zombie
        .stdin
        .as_mut()
        .expect("zombie stdin")
        .write_all(ZOMBIE_PRODUCER_JAVA.as_bytes())
        .expect("write Java zombie helper");
    drop(zombie.stdin.take());
    let zombie_out = zombie
        .wait_with_output()
        .expect("wait Java zombie producer");
    let zombie_stdout = String::from_utf8_lossy(&zombie_out.stdout);
    eprintln!(
        "KRABKA[test] zombie Java producer status={} stdout={zombie_stdout} stderr={}",
        zombie_out.status,
        String::from_utf8_lossy(&zombie_out.stderr),
    );
    assert!(
        zombie_out.status.success(),
        "zombie Java producer failed: stdout={zombie_stdout}, stderr={}",
        String::from_utf8_lossy(&zombie_out.stderr),
    );
    assert!(
        zombie_stdout.contains("ZOMBIEPROBE fenced=ProducerFencedException"),
        "the zombie producer must be fenced with ProducerFencedException; \
         `fenced=none` means it was allowed to write: {zombie_stdout}",
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
