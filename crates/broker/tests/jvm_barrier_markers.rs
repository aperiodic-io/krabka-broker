//! JVM cross-validation: a real JVM consumer is unaffected by barrier markers.
//!
//! This suite carries the load-bearing compatibility claim of the barrier
//! feature. A barrier marker is a Kafka control record, and the whole design
//! rests on the promise that an ordinary consumer skips one without noticing.
//! Every other test in this repository checks that promise against krabka's
//! own client, which krabka also wrote. This one checks it against Apache
//! Kafka's.
//!
//! The broker runs on the host and advertises `host.docker.internal`, and the
//! JVM tools run in a container. [`jvm_acceptance`] documents that networking.
//!
//! What the suite proves, per partition:
//!
//! 1. The JVM consumer reads exactly the produced records, in produce order,
//!    and no marker reaches it as a record.
//! 2. The log end offset that the JVM tooling reports is larger than the record
//!    count, by exactly the number of markers injected into that partition. The
//!    markers hold real offsets, and the consumer stepped over them.
//! 3. Neither claim changes under `read_committed`. A barrier marker carries no
//!    producer id, so the isolation level has nothing to say about it.
//!
//! Point 2 is the one that distinguishes "the consumer skipped the markers"
//! from "the broker never wrote them". Without it a broker that dropped every
//! marker would pass.
//!
//! ```text
//! cargo test -p krabka-broker --test jvm_barrier_markers -- --ignored --nocapture
//! ```

mod jvm_acceptance;
mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use jvm_acceptance::{
    KAFKA_IMAGE_TXN, docker_run_kafka_tool_with_image, rlmm_broker0_advertised,
    start_host_broker_with,
};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::Client;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::krabka::barrier::{
    AlterBarrierGroupsRequest, AlterableBarrierGroup, CUT_STATUS_COMPLETE, TriggerBarrierRequest,
};

use crate::support::jvm_parse_offset;

const TOPIC: &str = "barrier-invisibility";
const BARRIER_GROUP: &str = "invisibility-cut";
const PARTITIONS: i32 = 3;
/// Records produced into each partition before the first cut, and again
/// between the two cuts. Every partition therefore ends with twice this many
/// records and two markers.
const RECORDS_PER_ROUND: i32 = 4;
/// How many cuts the test injects. Each one puts one marker in every
/// partition.
const CUTS: i64 = 2;

/// The value of record `index` in partition `partition`.
///
/// The value carries both coordinates, so a record served from the wrong
/// partition fails the comparison rather than passing by luck.
fn value_of(partition: i32, index: i32) -> String {
    format!("p{partition}-r{index}")
}

/// Produce one round of records into every partition.
async fn produce_round(producer: &Producer, round: i32) {
    for partition in 0..PARTITIONS {
        for index in 0..RECORDS_PER_ROUND {
            let value = value_of(partition, round * RECORDS_PER_ROUND + index);
            producer
                .send(ProducerRecord {
                    topic: TOPIC.to_string(),
                    partition: Some(partition),
                    key: None,
                    value: Some(value.into_bytes().into()),
                    headers: Vec::new(),
                    timestamp_ms: None,
                })
                .await
                .expect("produce");
        }
    }
    producer.flush().await.expect("flush");
}

/// Block until the barrier coordinator answers.
///
/// `__barrier_state` is provisioned asynchronously at startup, so a define
/// sent too early is refused with `COORDINATOR_NOT_AVAILABLE`. Waiting is what
/// keeps this suite from depending on that timing.
async fn wait_for_coordinator(client: &Client) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    loop {
        let answered = client
            .send(krabka_protocol::krabka::barrier::DescribeBarrierGroupsRequest::default())
            .await
            .is_ok();
        if answered {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the barrier coordinator never became available"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Define the barrier group over the topic.
async fn create_barrier_group(client: &Client) {
    let response = client
        .send(AlterBarrierGroupsRequest {
            groups: vec![AlterableBarrierGroup {
                group: BARRIER_GROUP.to_string(),
                topics: vec![TOPIC.to_string()],
                // Manual only. The test drives every injection itself, so a
                // scheduled one cannot add a marker mid-assertion.
                interval_ms: -1,
                retained_cuts: 8,
                delete: false,
                ..AlterableBarrierGroup::default()
            }],
            ..AlterBarrierGroupsRequest::default()
        })
        .await
        .expect("alter barrier groups");
    let result = response.results.first().expect("one result row");
    assert!(
        result.error_code == 0,
        "create group failed: code={} message={:?}",
        result.error_code,
        result.error_message
    );
}

/// Inject one cut and return the marker offset it took in each partition.
async fn inject_cut(client: &Client) -> BTreeMap<i32, i64> {
    let response = client
        .send(TriggerBarrierRequest {
            group: BARRIER_GROUP.to_string(),
            timeout_ms: 30_000,
            ..TriggerBarrierRequest::default()
        })
        .await
        .expect("trigger barrier");
    assert!(
        response.error_code == 0,
        "trigger failed: code={} message={:?}",
        response.error_code,
        response.error_message
    );
    assert!(
        response.status == CUT_STATUS_COMPLETE,
        "cut is partial, so a partition took no marker: {:?}",
        response.missing
    );

    let topic = response
        .topics
        .iter()
        .find(|t| t.topic == TOPIC)
        .expect("the cut names the topic");
    let offsets: BTreeMap<i32, i64> = topic
        .partitions
        .iter()
        .map(|p| (p.partition, p.offset))
        .collect();
    assert!(
        offsets.len() == usize::try_from(PARTITIONS).expect("partition count fits"),
        "the cut must name every partition, got {offsets:?}"
    );
    offsets
}

/// Read one partition from the beginning with the JVM console consumer, and
/// return the record values in the order it served them.
///
/// `expected` bounds `--max-messages`, so the consumer returns as soon as it
/// has that many rather than waiting out the timeout. A marker delivered as a
/// record would push a marker's bytes into this list and fail the comparison.
fn jvm_consume(partition: i32, expected: usize, isolation: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            &format!("host.docker.internal:{}", jvm_acceptance::host_port()),
            "--topic",
            TOPIC,
            "--partition",
            &partition.to_string(),
            "--offset",
            "earliest",
            "--max-messages",
            &expected.to_string(),
            "--consumer-property",
            &format!("isolation.level={isolation}"),
            "--timeout-ms",
            "30000",
        ],
    );
    support::jvm_output_lines(&out)
}

/// The log end offset that the JVM tooling reports for one partition.
///
/// `kafka-get-offsets` prints `topic:partition:offset`. The test calls the
/// wrapper script rather than the class, because `GetOffsetShell` changed
/// package in Kafka 3.4 and the script did not.
fn jvm_log_end_offset(partition: i32) -> i64 {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-get-offsets",
            "--bootstrap-server",
            &format!("host.docker.internal:{}", jvm_acceptance::host_port()),
            "--topic-partitions",
            &format!("{TOPIC}:{partition}"),
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&format!("{TOPIC}:{partition}:")))
        .unwrap_or_else(|| {
            panic!("kafka-get-offsets printed no row for partition {partition}: {stdout}")
        });
    jvm_parse_offset(line)
}

/// A JVM consumer reads across barrier markers and never sees one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn a_jvm_consumer_reads_across_barrier_markers_unchanged() {
    // One state partition at replication factor one, because this is a single
    // node: the 50-at-factor-3 default leaves the partition this group hashes
    // to without a leader, and the coordinator then refuses every request.
    let (_broker, _dir) = start_host_broker_with(|config| {
        config.barrier_state_num_partitions = 1;
        config.barrier_state_replication_factor = 1;
    })
    .await;
    // Despite the name, this accessor is broker 0 over loopback: the host-side
    // clients use it, and only the containers use the advertised name.
    let bootstrap = rlmm_broker0_advertised().to_string();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: TOPIC.to_string(),
                partitions: PARTITIONS,
                replicas: 1,
                configs: BTreeMap::default(),
                replica_assignments: BTreeMap::new(),
            }],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(10)),
        )
        .await
        .expect("create topic");

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer");
    let client = support::client::connect_owned(&bootstrap, "barrier-jvm", "client build").await;

    wait_for_coordinator(&client).await;
    create_barrier_group(&client).await;

    // Records, a cut, more records, a second cut. Every partition ends with
    // two rounds of records and two markers, and the markers are interleaved
    // rather than trailing, so a consumer that stops at the first one fails.
    produce_round(&producer, 0).await;
    let first_cut = inject_cut(&client).await;
    produce_round(&producer, 1).await;
    let second_cut = inject_cut(&client).await;

    let records_per_partition = i64::from(RECORDS_PER_ROUND) * 2;
    let expected_end_offset = records_per_partition + CUTS;

    for partition in 0..PARTITIONS {
        let first = first_cut[&partition];
        let second = second_cut[&partition];

        // The cut offsets are where the markers went. The first marker sits
        // after the first round, and the second after the second round, each
        // displaced by the markers already written.
        check!(
            first == i64::from(RECORDS_PER_ROUND),
            "partition {partition}: first marker offset"
        );
        check!(
            second == i64::from(RECORDS_PER_ROUND) * 2 + 1,
            "partition {partition}: second marker offset"
        );

        // The markers hold real offsets. Without this a broker that silently
        // dropped every marker would pass the consumption check below.
        let end_offset = jvm_log_end_offset(partition);
        check!(
            end_offset == expected_end_offset,
            "partition {partition}: the JVM tooling reports end offset {end_offset}, \
             and {records_per_partition} records plus {CUTS} markers is {expected_end_offset}"
        );

        for isolation in ["read_uncommitted", "read_committed"] {
            let served = jvm_consume(
                partition,
                usize::try_from(records_per_partition).expect("record count fits"),
                isolation,
            );
            let expected: Vec<String> = (0..RECORDS_PER_ROUND * 2)
                .map(|index| value_of(partition, index))
                .collect();
            check!(
                served == expected,
                "partition {partition} at {isolation}: the JVM consumer served {served:?}"
            );
        }
    }
}
