// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as `tests/mtls.rs` etc).

//! Prometheus `/metrics` HTTP endpoint.
//!
//! This test boots a broker with a metrics listener on `127.0.0.1:0`, scrapes
//! `/metrics`, exercises the Produce wire path, and verifies that the
//! topic-labelled counters go up. It proves that the chain from the registry,
//! to the handler, to the HTTP endpoint works end to end.
//!
//! It is gated to non-Windows. The broker handle's `metrics_addr()` is for
//! Linux and macOS by convention, which matches the other integration tests.

mod kafka_wire;

mod support;

use {
    crate::{
        kafka_wire::ExchangeSetup as WireExchangeSetup,
        support::{
            client::{metrics_config, scrape_metrics},
            fetch::{SinglePartitionFetchSetup, single_partition_fetch},
            records::{batch_from_records, value_record},
            topics::{ConfiguredTopicSetup, creatable_topic, create_topic_request},
            wire::HeaderEncoding as RequestHeaderEncoding,
        },
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerConfig, metrics::PartitionLabel},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            create_topics_response::CreateTopicsResponse,
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::ProduceResponse,
        },
    },
    std::time::Duration,
    tokio::net::TcpStream,
};

const TOPIC: &str = "metrics-it";
const FETCH_VERSION: i16 = 12;
const PRODUCE_VERSION: i16 = 9;
const CREATE_TOPICS_VERSION: i16 = 7;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-metrics-test";

async fn create_topic(addr: std::net::SocketAddr) { create_topic_named(addr, TOPIC).await; }

async fn create_topic_named(addr: std::net::SocketAddr, topic: &str) {
    let req = create_topic_request(creatable_topic(ConfiguredTopicSetup {
        name: (topic).into(),
        ..Default::default()
    }));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION).unwrap();
    let resp = kafka_wire::round_trip(
        &mut stream,
        WireExchangeSetup {
            api_key: WireApiKey(19),
            version: WireApiVersion(CREATE_TOPICS_VERSION),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &resp;
    let r = CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION).unwrap();
    assert!(r.topics[0].error_code == 0, "create: {:?}", r.topics[0]);
}

async fn produce_one(addr: std::net::SocketAddr) -> u64 {
    let (bytes, error_code) = produce_to(addr, TOPIC, 0).await;
    assert!(error_code == 0, "produce error code: {error_code}");
    bytes
}

async fn produce_to(addr: std::net::SocketAddr, topic: &str, partition: i32) -> (u64, i16) {
    let batch = batch_from_records(vec![value_record(
        0,
        Some(bytes::Bytes::from_static(b"hello")),
    )]);
    let part = PartitionProduceData {
        index: partition,
        records: Some(batch.into()),
        ..Default::default()
    };
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![part],
            ..Default::default()
        }],
        ..Default::default()
    };
    let (body_len, r): (usize, ProduceResponse) =
        kafka_wire::request_once(addr, &req, (0, PRODUCE_VERSION), CLIENT_ID, (1, true)).await;
    let topic = r.responses.into_iter().next().expect("one topic in resp");
    let part = topic
        .partition_responses
        .into_iter()
        .next()
        .expect("one partition in resp");
    (body_len as u64, part.error_code)
}

async fn fetch_one(addr: std::net::SocketAddr) {
    use krabka_protocol::owned::fetch_response::FetchResponse;
    let req = single_partition_fetch(SinglePartitionFetchSetup {
        topic: TOPIC.into(),
        topic_id: krabka_protocol::primitives::uuid::Uuid::default(),
        ..Default::default()
    });
    let (_, r): (usize, FetchResponse) =
        kafka_wire::request_once(addr, &req, (1, FETCH_VERSION), CLIENT_ID, (1, true)).await;
    assert!(r.error_code == 0, "fetch top-level: {r:?}");
}

async fn scrape(addr: std::net::SocketAddr) -> String { scrape_metrics(addr).await }

async fn wait_until_absent(addr: std::net::SocketAddr, needle: &str) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let body = scrape(addr).await;
            if !body.contains(needle) {
                return body;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("metrics label {needle:?} was not evicted"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_client_labels_leave_the_metrics_body() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    let handle = Broker::start(cfg).await.unwrap();
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle.metrics_addr().expect("metrics listener");

    let (_, unknown_error) = produce_to(kafka_addr, "invented-topic", 0).await;
    check!(unknown_error != 0);
    create_topic_named(kafka_addr, TOPIC).await;
    handle.wait_until_partition_present(TOPIC, 0).await;
    let body = wait_until_absent(metrics_addr, "invented-topic").await;
    check!(!body.contains("invented-topic"));

    let (_, partition_error) = produce_to(kafka_addr, TOPIC, 99).await;
    check!(partition_error != 0);
    create_topic_named(kafka_addr, "eviction-trigger").await;
    handle
        .wait_until_partition_present("eviction-trigger", 0)
        .await;
    let pair = format!("topic=\"{TOPIC}\",partition=\"99\"");
    let body = wait_until_absent(metrics_addr, &pair).await;
    check!(!body.contains(&pair));

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_serves_openmetrics_and_counters_tick() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = metrics_config(log_dir.path().to_path_buf());

    let handle = Broker::start(cfg).await.unwrap();
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // Initial scrape: gauges and scalar counters are present
    // immediately, even pre-traffic. (Topic-labelled `Family` metrics
    // need at least one label entry to emit a series — those are
    // verified after we drive traffic below.)
    let body = scrape(metrics_addr).await;
    for needle in [
        "krabka_broker_partitions_led",
        "krabka_broker_active_controller",
        "krabka_broker_isr_shrinks_total",
        "krabka_broker_isr_expands_total",
        // The replica-lag rollup is a scalar, so it renders before any
        // follower exists and shows that the family reached the registry.
        "krabka_broker_replica_lag_max_records",
    ] {
        assert!(body.contains(needle), "missing {needle} in:\n{body}");
    }
    assert!(body.contains("# EOF"), "no EOF marker in:\n{body}");

    // Drive a CreateTopics + Produce + Fetch so the topic-labelled
    // counters get at least one entry and start emitting series.
    create_topic(kafka_addr).await;
    // Wait for the partition to materialize in the metadata image (the
    // partition writer is ready on commit) instead of guessing a duration.
    handle.wait_until_partition_present(TOPIC, 0).await;
    produce_one(kafka_addr).await;
    handle.wait_until_high_watermark(TOPIC, 0, 1).await;
    fetch_one(kafka_addr).await;

    // Wait for the background gauge sampler (1s tick) to publish
    // partitions_led rather than sleeping past one tick. active_controller
    // is set in the same sampler iteration, so it is live once this holds.
    handle
        .wait_for_metrics("partitions_led >= 1", |m| m.partitions_led.get() >= 1)
        .await;

    let body = scrape(metrics_addr).await;
    for needle in [
        "krabka_broker_topic_bytes_in_total",
        "krabka_broker_topic_bytes_out_total",
        "krabka_broker_topic_produce_requests_total",
        "krabka_broker_topic_fetch_requests_total",
        "krabka_broker_messages_in_total",
    ] {
        assert!(
            body.contains(needle),
            "post-traffic scrape missing {needle} in:\n{body}"
        );
    }
    // `produce_one` writes a single v2 record into the
    // topic, so `messages_in_total{topic=TOPIC}` must read exactly 1.
    let messages_needle = format!("krabka_broker_messages_in_total{{topic=\"{TOPIC}\"}} 1");
    check!(
        body.contains(&messages_needle),
        "messages_in_total should be 1 for the lone produced record, body:\n{body}"
    );
    check!(
        body.contains(&format!(
            "krabka_broker_topic_produce_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "produce-requests counter missing topic label:\n{body}"
    );
    check!(
        body.contains(&format!(
            "krabka_broker_topic_fetch_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "fetch-requests counter missing topic label:\n{body}"
    );
    // partitions_led >= 1 once the partition writer is up.
    let led_line = body
        .lines()
        .find(|l| l.starts_with("krabka_broker_partitions_led "))
        .expect("partitions_led series present");
    let value: i64 = led_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value >= 1,
        "partitions_led should be >=1, got line: {led_line}"
    );
    // Single-node broker: it's its own controller leader.
    assert!(
        body.contains("krabka_broker_active_controller 1"),
        "active_controller should be 1 on single-node, body:\n{body}"
    );

    handle.shutdown().await;
}

/// Confirms that the per-partition counters appear next to the topic-level
/// ones, and that the disk-scanner gauge reports non-zero values for
/// materialized partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_level_metrics_and_disk_gauge_render() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = metrics_config(log_dir.path().to_path_buf());
    // Enable the disk scanner with a 1s tick so the gauge gets a
    // chance to populate within the test's wait window.
    cfg.partition_disk_scan_interval = krabka_units::secs(1);

    let handle = Broker::start(cfg).await.unwrap();
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // Create the topic + produce a record so the per-partition
    // counters fire and the on-disk segment exists for the scanner.
    create_topic(kafka_addr).await;
    // Wait for the partition to materialize (writer ready on commit)
    // instead of guessing a duration.
    handle.wait_until_partition_present(TOPIC, 0).await;
    produce_one(kafka_addr).await;

    // Wait for the disk scanner to publish a non-zero on-disk size for the
    // materialized partition rather than sleeping past its 1s tick.
    handle
        .wait_for_metrics("partition_disk_bytes > 0", |m| {
            m.partition_disk_bytes
                .get_or_create(&PartitionLabel {
                    topic: TOPIC.into(),
                    partition: 0,
                })
                .get()
                > 0
        })
        .await;

    let body = scrape(metrics_addr).await;

    // Topic-level still present.
    let topic_needle = format!("krabka_broker_topic_bytes_in_total{{topic=\"{TOPIC}\"}}");
    assert!(
        body.contains(&topic_needle),
        "missing topic-level bytes_in in:\n{body}"
    );

    // Partition-level present with non-zero value.
    let partition_needle =
        format!("krabka_broker_partition_bytes_in_total{{topic=\"{TOPIC}\",partition=\"0\"}}");
    assert!(
        body.contains(&partition_needle),
        "missing partition-level bytes_in in:\n{body}"
    );
    // Per-partition CPU micros counter must be emitted with
    // the topic/partition label set after the produce + fetch path
    // runs. Value is timing-dependent (could be 0 if the handler was
    // sub-microsecond), so we only assert presence of the series name,
    // not a specific value.
    let cpu_needle = format!("krabka_broker_partition_cpu_micros_total{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&cpu_needle),
        "missing partition_cpu_micros_total in:\n{body}"
    );
    // Confirm the value is non-zero.
    let part_line = body
        .lines()
        .find(|l| l.starts_with(&partition_needle))
        .expect("partition_bytes_in line present");
    let value: u64 = part_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value > 0,
        "partition_bytes_in should be >0 after produce, got line: {part_line}"
    );

    // Disk gauge: emitted for at least one (topic, partition) of TOPIC.
    let disk_prefix = format!("krabka_broker_partition_disk_bytes{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&disk_prefix),
        "missing partition_disk_bytes for materialized partition in:\n{body}"
    );

    let _ = tokio::time::timeout(Duration::from_secs(10), handle.shutdown()).await;
}
