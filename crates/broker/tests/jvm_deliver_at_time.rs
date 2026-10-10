//! JVM cross-validation: scheduled delivery needs no client change.
//!
//! This suite carries the load-bearing compatibility claim of KFC-1. The
//! feature adds no API key, no error code, and no request or response field: a
//! producer schedules a record by setting the `ProducerRecord` timestamp it has
//! been able to set since Kafka 0.10, and a consumer polls a topic that behaves
//! like any other topic with a slower leader. Every other test in this
//! repository checks that against krabka's own client, which krabka also wrote.
//! This one checks it against Apache Kafka's.
//!
//! The probe is a small Java program compiled in-container against the image's
//! Kafka client jars, the same arrangement `jvm_acceptance_durability` uses for
//! its transactional producer. It is stock client API throughout:
//! `KafkaProducer.send` with the five-argument `ProducerRecord`, and
//! `KafkaConsumer.assign` / `seekToBeginning` / `poll`.
//!
//! What the suite proves:
//!
//! 1. A JVM consumer that polls a scheduled topic before the record's timestamp
//!    passes reads nothing, and reads the record after it passes.
//! 2. The same producer call against a `delivery.mode=immediate` topic delivers
//!    the record at once, so the difference is the topic config and not the
//!    client.
//! 3. The record the consumer finally reads carries the timestamp the producer
//!    set, so the broker delivered the record the producer scheduled.
//! 4. `kafka-console-consumer`, with no configuration of any kind, reads the
//!    scheduled topic once the record is due.
//!
//! Point 2 is what distinguishes "the broker held the record back" from "the
//! broker was slow", and point 3 from "the broker delivered some other record".
//!
//! The broker runs on the host and advertises `host.docker.internal`, and the
//! JVM tools run in a container. [`jvm_acceptance`] documents that networking.
//!
//! ```text
//! cargo test -p krabka-broker --test jvm_deliver_at_time -- --ignored --nocapture
//! ```

mod jvm_acceptance;
mod support;

use std::{io::Write as _, process::Stdio};

use assert2::{assert, check};
use jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image, start_host_broker,
};
use krabka_broker::BrokerHandle;
use krabka_log::DeliveryPolicy;

const SCHEDULED_TOPIC: &str = "jvm-deliver-at-time-scheduled";
const IMMEDIATE_TOPIC: &str = "jvm-deliver-at-time-immediate";

/// The record body, which both topics carry, so a record served from the wrong
/// topic still has to satisfy the count and the timestamp.
const PAYLOAD: &str = "payload";

/// How far ahead of the produce the probe stamps its record.
///
/// The early poll has to finish before the delivery time or it proves nothing,
/// and it runs behind two container-side consumer constructions. The probe
/// reports the instant the early poll ended and the test checks it against the
/// delivery time, so a value too small for a loaded machine fails the run
/// rather than passing it.
const DELAY_MS: i64 = 30_000;

/// The probe's own report of what a stock JVM client saw.
///
/// The Java side only measures and prints; every assertion is made here.
const PROBE_JAVA: &str = krabka_macros::java_string_producer_source!(
    r#"
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public final class DeliverAtTimeProbe {
  private static final long POLL_BUDGET_MS = 4000;

  public static void main(String[] args) throws Exception {
    String bootstrap = args[0];
    String scheduled = args[1];
    String immediate = args[2];
    long delayMillis = Long.parseLong(args[3]);
    long deliverAt = System.currentTimeMillis() + delayMillis;

"#,
    "bootstrap",
    r#"    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      // The stock constructor that has carried a timestamp since Kafka 0.10.
      producer.send(new ProducerRecord<>(scheduled, 0, deliverAt, "k", "payload")).get();
      producer.send(new ProducerRecord<>(immediate, 0, deliverAt, "k", "payload")).get();
    }
    System.out.println("PROBE deliverAt " + deliverAt);

    poll("early", bootstrap, scheduled);
    poll("early", bootstrap, immediate);
    System.out.println("PROBE earlyEndedAt " + System.currentTimeMillis());

    long wakeAt = deliverAt + 5000;
    long naptime = wakeAt - System.currentTimeMillis();
    if (naptime > 0) {
      Thread.sleep(naptime);
    }

    poll("late", bootstrap, scheduled);
    poll("late", bootstrap, immediate);
    System.out.println("PROBE OK");
  }

  private static void poll(String phase, String bootstrap, String topic) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    TopicPartition partition = new TopicPartition(topic, 0);
    StringBuilder values = new StringBuilder();
    int count = 0;
    long timestamp = -1;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      consumer.assign(Collections.singletonList(partition));
      consumer.seekToBeginning(Collections.singletonList(partition));
      long deadline = System.currentTimeMillis() + POLL_BUDGET_MS;
      while (System.currentTimeMillis() < deadline && count == 0) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> record : records) {
          count++;
          timestamp = record.timestamp();
          values.append(record.value());
        }
      }
    }
    String served = values.length() == 0 ? "-" : values.toString();
    System.out.println("PROBE " + phase + " " + topic + " " + count + " " + timestamp + " " + served);
  }
}
"#
);

// Create `topic` with the given `delivery.mode`, over the wire, from the host.
//
// This does not go through `kafka-topics`, and it cannot. That tool validates
// `--config` names against the `LogConfig.configNames` set compiled into the
// client before it sends `CreateTopics`, so it rejects `delivery.mode` with
// `InvalidConfigurationException: Unknown topic config name` without ever
// reaching the broker. Bumping the image does not help the way it does for the
// KIP-405 keys in [`jvm_acceptance::create_tiered_topic`]: those became known
// to a later Kafka, and a krabka extension never will be. `AdminClient` carries
// no such list and passes the config through for the broker to validate, which
// is why a JVM application can set the key even though the shell wrapper
// cannot.
//
// None of that touches what this suite exists to prove. Scheduling a record is
// a producer action and reading one is a consumer action, and both stay stock;
// configuring the topic is an operator action, and the KFC records the tool
// limitation under Compatibility.
async fn create_topic(bootstrap: &str, topic: &str, mode: &str) {
    let client =
        support::client::connect_with_context(bootstrap, None, "client for the host listener")
            .await;
    support::client::create_configured_topic(
        &client,
        crate::support::topics::CreateTopicSetup {
            topic,
            configs: &[("delivery.mode", mode)],
            ..Default::default()
        },
    )
    .await;
}

// Wait until `delivery.mode` has reached the partition's own `LogConfig`.
//
// `CreateTopics` materializes the partition from the broker's base log config
// and the topic overrides land on the next supervisor reconcile, so a produce
// sent between the two would see an immediate-delivery log.
async fn wait_for_delivery_policy(broker: &BrokerHandle, topic: &str, policy: DeliveryPolicy) {
    broker
        .wait_for_metrics("delivery.mode reaches the partition LogConfig", |_| {
            broker
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| config.delivery_policy == policy)
        })
        .await;
}

// Compile and run the probe in the container, and return everything it printed.
fn run_probe(bootstrap: &str) -> String {
    let mut probe = crate::support::jvm_docker_command(crate::support::JvmDockerSetup { image: "--entrypoint", args: &["bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/DeliverAtTimeProbe.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/DeliverAtTimeProbe.java; \
               java -cp "/tmp:$CP" DeliverAtTimeProbe "$1" "$2" "$3" "$4""#,
            "--",
            bootstrap,
            SCHEDULED_TOPIC,
            IMMEDIATE_TOPIC,
            &DELAY_MS.to_string(),
        ], input: crate::support::ContainerInput::Attached, ..Default::default() })
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the Java deliver-at-time probe");
    probe
        .stdin
        .as_mut()
        .expect("probe stdin")
        .write_all(PROBE_JAVA.as_bytes())
        .expect("write the Java probe");
    drop(probe.stdin.take());

    let out = probe.wait_with_output().expect("wait for the Java probe");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    eprintln!(
        "KRABKA[test] deliver-at-time probe status={} stdout={stdout} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "the Java probe failed: stdout={stdout}, stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    stdout
}

// The value of the probe's one-number `PROBE <key> <value>` line.
fn probe_number(stdout: &str, key: &str) -> i64 {
    let prefix = format!("PROBE {key} ");
    stdout
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("the probe printed no `{key}` line: {stdout}"))
}

// The probe's measurement lines, in the order it printed them.
fn probe_readings(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("PROBE early ") || line.starts_with("PROBE late "))
        .map(ToOwned::to_owned)
        .collect()
}

// Read the scheduled topic with the stock console consumer, one record.
fn jvm_console_consume(bootstrap: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            SCHEDULED_TOPIC,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
        ],
    );
    support::jvm_output_lines(&out)
}

// A stock JVM producer schedules a record and a stock JVM consumer waits for
// it, with no client change on either side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_stock_jvm_client_schedules_and_waits_with_no_change() {
    let (broker, _dir) = start_host_broker().await;
    // The name the broker advertises, which is what a container resolves
    // through `--add-host=host.docker.internal:host-gateway`.
    let bootstrap = broker0_advertised();

    // Topic creation runs against the broker's own listener rather than the
    // advertised container name, because this client runs on the host.
    let host_bootstrap = broker.listen_addr().to_string();
    create_topic(&host_bootstrap, SCHEDULED_TOPIC, "scheduled").await;
    create_topic(&host_bootstrap, IMMEDIATE_TOPIC, "immediate").await;
    wait_for_delivery_policy(&broker, SCHEDULED_TOPIC, DeliveryPolicy::Scheduled).await;
    wait_for_delivery_policy(&broker, IMMEDIATE_TOPIC, DeliveryPolicy::Immediate).await;

    let stdout = tokio::task::spawn_blocking(move || run_probe(bootstrap))
        .await
        .expect("the probe task finishes");

    let deliver_at = probe_number(&stdout, "deliverAt");
    let early_ended_at = probe_number(&stdout, "earlyEndedAt");

    // Without this the early poll could have run after the delivery time, and
    // an empty read would then say nothing about scheduled delivery.
    check!(
        early_ended_at < deliver_at,
        "the early poll ended {} ms after the delivery time, so it proves nothing",
        early_ended_at - deliver_at
    );

    let expected = vec![
        format!("PROBE early {SCHEDULED_TOPIC} 0 -1 -"),
        format!("PROBE early {IMMEDIATE_TOPIC} 1 {deliver_at} {PAYLOAD}"),
        format!("PROBE late {SCHEDULED_TOPIC} 1 {deliver_at} {PAYLOAD}"),
        format!("PROBE late {IMMEDIATE_TOPIC} 1 {deliver_at} {PAYLOAD}"),
    ];
    check!(probe_readings(&stdout) == expected);
    check!(
        stdout.contains("PROBE OK"),
        "the probe did not run to completion: {stdout}"
    );

    // The same record, through the tool an operator would reach for, with no
    // property set on it at all.
    let served = tokio::task::spawn_blocking(move || jvm_console_consume(bootstrap))
        .await
        .expect("the console-consumer task finishes");
    check!(served == vec![PAYLOAD.to_owned()]);

    broker.shutdown().await;
}
