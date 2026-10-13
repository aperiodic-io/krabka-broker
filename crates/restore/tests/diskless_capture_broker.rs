//! `krabka-backup capture` against a live diskless broker.
//!
//! The broker enforces Kafka's internal-topic rule: a `Produce` to an internal
//! topic succeeds only for the `__admin_client` client id (and the broker's own
//! index writers), and anything else gets `INVALID_TOPIC_EXCEPTION` (17). A
//! capture that published into `__diskless_wal_index` therefore failed against
//! every cluster that had one. This suite boots a single diskless broker in
//! process, lets it flush a committed prefix into the index, and requires the
//! capture to freeze that prefix without writing anything.

use {
    assert2::{assert, check},
    bytes::Bytes,
    krabka_backup::{
        archive::ArchiveArgs,
        capture::capture_key,
        manifest::{DISKLESS_WAL_INDEX, GROUP_OFFSETS, MANIFEST, Manifest},
        run,
    },
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, NodeId, RemoteStorageBackend, RlmmKind,
    },
    krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch},
    },
    krabka_remote_storage::diskless::{DisklessPartitionCapture, DisklessWalCapture},
    std::{
        collections::BTreeMap,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    uuid::Uuid,
};

/// The diskless data topic the broker flushes into the index.
const TOPIC: &str = "events";

/// How many single-record `acks=all` produces the case commits.
const RECORDS: i64 = 3;

/// Kafka `NOT_LEADER_OR_FOLLOWER`, returned before any append.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

/// Kafka `UNKNOWN_TOPIC_OR_PARTITION`, also returned before any append.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// A single-broker cluster that runs a diskless WAL and its topic-backed flush
/// index, and the directories that must outlive it.
struct DisklessBroker {
    handle: BrokerHandle,
    bootstrap: String,
    _log_dir: TempDir,
    _object_store: TempDir,
}

/// Boot one broker that can host a diskless topic on its own.
///
/// It needs a rack, because WAL placement refuses a voter with none, a WAL
/// quorum of one, an object store to flush into, and a topic-backed metadata
/// log with one partition and one replica, because the flush index is a Kafka
/// topic this broker alone must host. The ports are bound up front: the index
/// client bootstraps against the data listener, and the controller address is
/// the static voter set, so neither can be `:0`.
async fn start_diskless_broker() -> DisklessBroker {
    let data = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the data listener");
    let controller = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the controller listener");
    let data_addr: SocketAddr = data.local_addr().expect("data address");
    let controller_addr: SocketAddr = controller.local_addr().expect("controller address");
    let log_dir = TempDir::new().expect("broker log dir");
    let object_store = TempDir::new().expect("object store dir");

    let mut config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
    config.rack = Some("rack-a".to_owned());
    config.diskless_wal_local_replica_count = 1;
    config.diskless_wal_flush_interval = krabka_units::millis(50);
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: object_store.path().to_path_buf(),
    });
    config.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: data_addr.to_string(),
        num_partitions: 1,
        replication: 1,
        min_isr: 1,
        snapshot_interval: krabka_units::hours(1),
        ..KafkaRlmmConfig::default()
    });

    let handle = Broker::start_with_listeners(config, Some(controller), Some(data))
        .await
        .expect("the diskless broker starts");
    handle.wait_until_brokers_registered(1).await;
    handle.wait_until_diskless_flusher_ready().await;
    DisklessBroker {
        handle,
        bootstrap: data_addr.to_string(),
        _log_dir: log_dir,
        _object_store: object_store,
    }
}

/// Create the one-partition diskless topic and return its id.
async fn create_diskless_topic(bootstrap: &str) -> Uuid {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin client");
    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: TOPIC.to_owned(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::from([("krabka.diskless".to_owned(), "true".to_owned())]),
                replica_assignments: BTreeMap::new(),
            }],
            TopicMutationOptions::default(),
        )
        .await
        .expect("CreateTopics");
    let outcome = outcomes.first().expect("one CreateTopics outcome");
    assert!(outcome.error.is_none(), "CreateTopics failed: {outcome:?}");
    outcome.topic_id.expect("a created topic id")
}

/// Produce `RECORDS` single-record batches with `acks=all`, retrying only the
/// errors a broker returns before it appends.
async fn produce(bootstrap: &str, topic_id: Uuid) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("diskless-capture-producer")
        .build()
        .await
        .expect("client");
    let deadline = Instant::now() + Duration::from_mins(1);
    for index in 0..RECORDS {
        loop {
            // The flusher expires batches older than `retention.ms` out of the
            // object store, so each batch carries the wall clock.
            let now_ms = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("the test clock is after the epoch")
                    .as_millis(),
            )
            .expect("the test clock fits an i64");
            let batch = RecordBatch {
                base_timestamp: now_ms,
                max_timestamp: now_ms,
                records: vec![Record {
                    value: Some(Bytes::from(format!("record-{index}"))),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            };
            let response = client
                .send(ProduceRequest {
                    acks: -1,
                    timeout_ms: 30_000,
                    topic_data: vec![TopicProduceData {
                        name: TOPIC.into(),
                        topic_id: WireUuid(topic_id.into_bytes()),
                        partition_data: vec![PartitionProduceData {
                            index: 0,
                            records: Some(batch.into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("Produce");
            match response.responses[0].partition_responses[0].error_code {
                0 => break,
                NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_OR_PARTITION => {
                    assert!(
                        Instant::now() <= deadline,
                        "record {index} found no diskless leader: {response:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                error_code => panic!("produce of record {index} failed with {error_code}"),
            }
        }
    }
}

/// Wait until the broker's own index projection says the committed prefix is
/// flushed, which is the state a capture must freeze.
async fn await_flushed(broker: &BrokerHandle) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let state = broker.diskless_flush_state_for_test(TOPIC, 0).await;
        if let Some((diskless, _, _, _, high_watermark, frontier)) = state {
            assert!(diskless, "the topic must be diskless");
            if high_watermark == RECORDS && frontier == Some(RECORDS) {
                return;
            }
        }
        assert!(
            Instant::now() <= deadline,
            "the flusher never published the committed prefix; last state was {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_freezes_a_live_diskless_index_without_producing_to_it() {
    let broker = start_diskless_broker().await;
    let topic_id = create_diskless_topic(&broker.bootstrap).await;
    produce(&broker.bootstrap, topic_id).await;
    await_flushed(&broker.handle).await;
    let backup_root = TempDir::new().expect("backup root");
    let archive = ArchiveArgs {
        local: Some(backup_root.path().to_path_buf()),
        ..ArchiveArgs::default()
    };

    let id = run::capture(None, Some(&broker.bootstrap), &archive)
        .await
        .expect("capture a cluster that has a diskless WAL index");

    let manifest: Manifest = serde_json::from_slice(
        &std::fs::read(backup_root.path().join(capture_key(&id, MANIFEST))).expect("manifest"),
    )
    .expect("decode the manifest");
    let names: Vec<&str> = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.name.as_str())
        .collect();
    check!(names == vec![GROUP_OFFSETS, DISKLESS_WAL_INDEX]);
    let capture = DisklessWalCapture::from_slice(
        &std::fs::read(
            backup_root
                .path()
                .join(capture_key(&id, DISKLESS_WAL_INDEX)),
        )
        .expect("diskless capture"),
    )
    .expect("decode the diskless capture");
    check!(capture.source_cutoffs.len() == 1);
    check!(capture.source_cutoffs[0] > 0);
    // Every committed offset is covered once, in order, by the captured
    // ranges. The object keys are the flusher's to choose.
    let covered: Vec<(i64, i64)> = capture
        .partitions
        .iter()
        .flat_map(|partition| &partition.ranges)
        .map(|range| (range.entry.first_offset, range.entry.last_offset))
        .collect();
    check!(covered.first().map(|range| range.0) == Some(0));
    check!(covered.last().map(|range| range.1) == Some(RECORDS - 1));
    check!(
        covered.windows(2).all(|pair| pair[1].0 == pair[0].1 + 1),
        "the captured ranges leave a gap: {covered:?}"
    );
    let partitions: Vec<DisklessPartitionCapture> = capture
        .partitions
        .into_iter()
        .map(|partition| DisklessPartitionCapture {
            ranges: Vec::new(),
            ..partition
        })
        .collect();
    assert!(
        partitions
            == vec![DisklessPartitionCapture {
                topic: TOPIC.to_owned(),
                topic_id,
                partition: 0,
                delete_floor: 0,
                recovery_cutoff: RECORDS,
                ranges: Vec::new(),
            }]
    );

    broker.handle.shutdown().await;
}
