//! The client-visible half of the round trip: a broker booted on a restore
//! output answers `ListOffsets`, `Fetch` and `OffsetForLeaderEpoch` the way
//! KFC-3 says it must, and keeps answering after a restart.
//!
//! `partition_data.rs` proves the restored bytes are right by reopening the
//! directory with a fresh `krabka_log::Log`. That is the storage layer talking
//! to itself. This file is the same claim made where it is actually promised
//! -- "Offsets Are the Contract" and "Leader Epochs Come Back Through the
//! Batches" in `docs/KFCs/KFC-3-point-in-time-restore.md` -- so every
//! assertion here is a wire answer from a running broker:
//!
//! * `ListOffsets EARLIEST` returns the archive's first surviving offset, and
//!   `payments-1`, whose oldest archived segment begins at offset 2, is what
//!   keeps that from passing by returning a hard-coded zero.
//! * `ListOffsets LATEST` returns the archive's end offset, so the restored
//!   high watermark is the archived one and not zero.
//! * `Fetch` from earliest returns the archived batches, compared as whole
//!   `RecordBatch` values against [`PartitionFixture::expected_batches`], so a
//!   batch that came back at a shifted offset or with a rewritten header fails
//!   here.
//! * `OffsetForLeaderEpoch` for the epoch the archived batches carry answers
//!   with that epoch's end offset rather than the `-1` a partition with no
//!   epoch history gives, which is the answer a KIP-320 consumer needs to
//!   validate its position instead of resetting it.
//!
//! One case is deliberately absent. Every partition here archives at leader
//! epoch 0, which is also the epoch the restore seeds each `PartitionRecord`
//! with, so an archive whose batches sit ABOVE the restored metadata epoch is
//! untested -- today the broker answers `UNKNOWN_LEADER_EPOCH` for it. See the
//! Test Plan in `docs/KFCs/KFC-3-point-in-time-restore.md`.
//!
//! The restart case then shuts the broker down, starts another on the same
//! directory, and repeats the fetch -- the second boot replays an already
//! populated `__cluster_metadata-0` rather than seeding one, which nothing else
//! exercises. Producing one batch into the restarted broker and finding it at
//! the archived end offset is what proves the restored high watermark seeded
//! the next append rather than merely being reported.

use std::sync::Arc;

use assert2::{assert, check};
use bytes::Bytes;
use krabka_audit::signing::FileEd25519Signer;
use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        offset_for_leader_epoch_request::{
            OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
        },
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Attributes, Record, RecordBatch},
};
use krabka_restore::RestoreReport;
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use uuid::Uuid;

use crate::{
    args::ControllerListener,
    fixture::{Fixture, PartitionFixture, build_fixture},
};

/// `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;
/// `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`: an ordinary client, not a
/// follower.
const CONSUMER_REPLICA_ID: i32 = -1;
/// `OffsetForLeaderEpochRequest`'s replica id for a consumer, the value
/// KIP-320 position validation sends.
const CONSUMER_OFLE_REPLICA_ID: i32 = -1;
/// Fetch enough per partition that the fixture's whole archived history
/// arrives in one response.
const PARTITION_MAX_BYTES: i32 = 1_048_576;

/// The archive, restored into a fresh directory, with a broker running on it.
///
/// The fixture and the target directory are kept alive as fields: both are
/// `TempDir`s, and dropping either deletes the tree out from under the running
/// broker.
struct RestoredCluster {
    fixture: Fixture,
    target: tempfile::TempDir,
    log_dir: std::path::PathBuf,
    controller: ControllerListener,
    broker: BrokerHandle,
    client: Client,
    report: RestoreReport,
}

impl RestoredCluster {
    /// Restore the round-trip fixture and boot a broker on the result.
    async fn start() -> Self {
        Self::start_with(build_fixture(), &[]).await
    }

    async fn start_with(fixture: Fixture, extra: &[&str]) -> Self {
        let (target, log_dir, mut controller, report) =
            crate::restored::restore_for_controller(fixture.archive_root.path(), extra).await;

        let (broker, client) =
            boot(&mut controller, BrokerConfig::for_tests(log_dir.clone())).await;
        Self {
            fixture,
            target,
            log_dir,
            controller,
            broker,
            client,
            report,
        }
    }

    /// Shut the broker down and start another one on the same directory.
    ///
    /// `BootstrapMode::Rejoin` is what a restarting node uses: the raft log
    /// the first boot wrote is already there, and `Bootstrap` refuses to seed
    /// a cluster over it. `BrokerHandle::shutdown` consumes the handle, so
    /// this takes and returns the whole cluster rather than mutating in place.
    async fn restart(self) -> Self {
        let Self {
            fixture,
            target,
            log_dir,
            mut controller,
            broker,
            client,
            report,
        } = self;
        drop(client);
        broker.shutdown().await;
        let mut config = BrokerConfig::for_tests(log_dir.clone());
        config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
        let (broker, client) = boot(&mut controller, config).await;
        Self {
            fixture,
            target,
            log_dir,
            controller,
            broker,
            client,
            report,
        }
    }

    async fn shutdown(self) {
        self.broker.shutdown().await;
        drop(self.target);
    }
}

/// Start a broker under `config`, its controller listening on `controller`,
/// and connect a client to it.
async fn boot(controller: &mut ControllerListener, config: BrokerConfig) -> (BrokerHandle, Client) {
    let broker = controller.start(config).await;
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("restore-consume-test")
        .build()
        .await
        .expect("client");
    (broker, client)
}

/// Wait until every archived partition is hosted with its restored history
/// visible, so a read taken next measures the restore rather than a partition
/// that has not opened yet.
async fn await_restored_partitions(broker: &BrokerHandle, fixture: &Fixture) {
    for partition in fixture.partitions() {
        broker
            .wait_until_partition_present(partition.topic, partition.partition)
            .await;
        broker
            .wait_until_high_watermark(partition.topic, partition.partition, partition.end_offset())
            .await;
    }
}

/// One `ListOffsets` row for `partition` at one timestamp sentinel.
async fn list_offset(client: &Client, partition: &PartitionFixture, timestamp: i64) -> i64 {
    let mut response = client
        .send(ListOffsetsRequest {
            replica_id: CONSUMER_REPLICA_ID,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: partition.topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition.partition,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    let row = response.topics.remove(0).partitions.remove(0);
    check!(
        row.error_code == 0,
        "ListOffsets({}-{}, {timestamp}): {row:?}",
        partition.topic,
        partition.partition,
    );
    row.offset
}

/// Fetch `partition` from `fetch_offset` and return the batches the broker
/// answered with.
async fn fetch_from(
    client: &Client,
    partition: &PartitionFixture,
    topic_id: Uuid,
    fetch_offset: i64,
) -> Vec<RecordBatch> {
    let response = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            topics: vec![FetchTopic {
                topic: partition.topic.into(),
                topic_id: wire_uuid(topic_id),
                partitions: vec![FetchPartition {
                    partition: partition.partition,
                    fetch_offset,
                    partition_max_bytes: PARTITION_MAX_BYTES,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    assert!(let Some(topic) = response.responses.first());
    assert!(let Some(part) = topic.partitions.first());
    check!(
        part.error_code == 0,
        "Fetch({}-{}, {fetch_offset}): {part:?}",
        partition.topic,
        partition.partition,
    );
    part.records
        .as_ref()
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .expect("a restored partition answers a Fetch with v2 batches")
        .to_vec()
}

/// Ask the broker where `epoch` ended for `partition`.
async fn end_offset_for_epoch(client: &Client, partition: &PartitionFixture, epoch: i32) -> i64 {
    let mut response = client
        .send(OffsetForLeaderEpochRequest {
            replica_id: CONSUMER_OFLE_REPLICA_ID,
            topics: vec![OffsetForLeaderTopic {
                topic: partition.topic.into(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: partition.partition,
                    current_leader_epoch: -1,
                    leader_epoch: epoch,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetForLeaderEpoch");
    let row = response.topics.remove(0).partitions.remove(0);
    check!(
        row.error_code == 0,
        "OffsetForLeaderEpoch({}-{}, {epoch}): {row:?}",
        partition.topic,
        partition.partition,
    );
    row.end_offset
}

/// The archived `Uuid` as the 16 wire bytes a `Fetch` or `Produce` names a
/// topic by. `krabka-protocol` keeps its own `Uuid` newtype and defines no
/// conversion from the `uuid` crate's.
fn wire_uuid(id: Uuid) -> krabka_protocol::primitives::uuid::Uuid {
    krabka_protocol::primitives::uuid::Uuid(id.into_bytes())
}

/// A single-record batch to append through the restarted broker.
fn one_record_batch(value: &str) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: 1_700_000_100_000,
        max_timestamp: 1_700_000_100_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        }],
    }
}

/// 6. A broker booted on a restore output serves the archived history to a
/// client: the offset bounds the archive held, the batches it held, and an
/// epoch answer a KIP-320 consumer can validate against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_broker_serves_the_archived_offsets_batches_and_epoch() {
    let cluster = RestoredCluster::start().await;
    await_restored_partitions(&cluster.broker, &cluster.fixture).await;

    for partition in cluster.fixture.partitions() {
        let topic_id = cluster.fixture.topic_id(partition.topic);
        let label = format!("{}-{}", partition.topic, partition.partition);

        // The archive's own bounds, not zero and not a fresh log's end.
        check!(
            list_offset(&cluster.client, partition, EARLIEST_TIMESTAMP).await
                == partition.base_offset(),
            "EARLIEST {label}"
        );
        check!(
            list_offset(&cluster.client, partition, LATEST_TIMESTAMP).await
                == partition.end_offset(),
            "LATEST {label}"
        );

        // Every archived batch, at the offset it was archived at.
        check!(
            fetch_from(
                &cluster.client,
                partition,
                topic_id,
                partition.base_offset()
            )
            .await
                == partition.expected_batches(),
            "Fetch from earliest {label}"
        );

        // The epoch the archived batches carry is still open on the restored
        // partition -- nothing has produced at a higher one -- so KIP-101's
        // rule makes its end offset the log end offset. A consumer that
        // rejoins with that epoch and a position inside the restored history
        // therefore validates instead of truncating.
        check!(
            end_offset_for_epoch(&cluster.client, partition, archived_epoch(partition)).await
                == partition.end_offset(),
            "OffsetForLeaderEpoch {label}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rotated_key_worm_archive_restores_boots_and_reads_original_offsets() {
    let fixture = build_fixture();
    let keys = tempfile::tempdir().expect("key dir");
    let mut signers = Vec::new();
    let mut key_paths = Vec::new();
    for id in ["old", "new"] {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer =
            Arc::new(FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), id.to_owned()).unwrap());
        let path = keys.path().join(format!("{id}.pub"));
        std::fs::write(&path, signer.public_key()).expect("write public key");
        signers.push(signer);
        key_paths.push(path.display().to_string());
    }
    let heads = fixture.sign_worm(&signers);
    let mut extra = vec![
        "--worm-key-id".to_owned(),
        "old".to_owned(),
        "--worm-public-key".to_owned(),
        key_paths[0].clone(),
        "--worm-key-id".to_owned(),
        "new".to_owned(),
        "--worm-public-key".to_owned(),
        key_paths[1].clone(),
    ];
    for (partition, head) in heads {
        extra.push("--worm-expect-head".to_owned());
        extra.push(format!("{partition}={head}"));
    }
    let refs = extra.iter().map(String::as_str).collect::<Vec<_>>();
    let cluster = RestoredCluster::start_with(fixture, &refs).await;
    await_restored_partitions(&cluster.broker, &cluster.fixture).await;

    let authentication = cluster
        .report
        .authentication
        .as_ref()
        .expect("authentication report");
    check!(authentication.trusted_keys == 2);
    check!(authentication.manifests > 0);
    check!(authentication.objects > 0);
    for partition in cluster.fixture.partitions() {
        check!(
            fetch_from(
                &cluster.client,
                partition,
                cluster.fixture.topic_id(partition.topic),
                partition.base_offset(),
            )
            .await
                == partition.expected_batches()
        );
    }
    cluster.shutdown().await;
}

/// 7. The restored directory is bootable more than once, and the restored high
/// watermark is where the next append starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_broker_restarts_and_appends_at_the_archived_end_offset() {
    const TOPIC: &str = "payments";
    const PARTITION: i32 = 1;

    let cluster = RestoredCluster::start().await;
    await_restored_partitions(&cluster.broker, &cluster.fixture).await;
    let cluster = cluster.restart().await;
    await_restored_partitions(&cluster.broker, &cluster.fixture).await;

    // The whole archived history is still there after the second boot, which
    // is the claim the restart exists to make.
    for partition in cluster.fixture.partitions() {
        let topic_id = cluster.fixture.topic_id(partition.topic);
        check!(
            fetch_from(
                &cluster.client,
                partition,
                topic_id,
                partition.base_offset()
            )
            .await
                == partition.expected_batches(),
            "Fetch after restart {}-{}",
            partition.topic,
            partition.partition,
        );
    }

    // Appending through the restarted broker: the record lands at the
    // archived end offset, so the restored high watermark seeded the offset
    // assignment rather than the log restarting from zero.
    let partition = cluster.fixture.partition(TOPIC, PARTITION);
    let expected_offset = partition.end_offset();
    let topic_id = cluster.fixture.topic_id(TOPIC);
    let produced = cluster
        .client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id: wire_uuid(topic_id),
                partition_data: vec![PartitionProduceData {
                    index: PARTITION,
                    records: Some(krabka_protocol::records::RecordsPayload::V2(vec![
                        one_record_batch("after-restore"),
                    ])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    assert!(let Some(topic) = produced.responses.first());
    assert!(let Some(part) = topic.partition_responses.first());
    check!(part.error_code == 0, "Produce: {part:?}");
    check!(part.base_offset == expected_offset);

    // The `acks=1` ack is sent before the writer recomputes the high
    // watermark (`crates/broker/src/partition_writer/produce.rs` acks every
    // append, then advances the HW), so a Fetch issued the instant Produce
    // returns can still be capped at the pre-append watermark and answer with
    // the archived batches alone. Waiting for the watermark to cover the new
    // record is a condition wait on real state, not a delay.
    cluster
        .broker
        .wait_until_high_watermark(TOPIC, PARTITION, expected_offset + 1)
        .await;

    // And the appended record reads back at that offset, after the archived
    // batches, through the same fetch path.
    let batches = fetch_from(
        &cluster.client,
        partition,
        topic_id,
        partition.base_offset(),
    )
    .await;
    assert!(let Some(appended) = batches.last());
    check!(appended.base_offset == expected_offset);
    check!(
        appended.records.first().and_then(|r| r.value.clone())
            == Some(Bytes::from_static(b"after-restore"))
    );
    check!(batches.len() == partition.expected_batches().len() + 1);

    cluster.shutdown().await;
}

/// The `partition_leader_epoch` every batch this fixture archived carries.
///
/// The fixture builds each partition with one `krabka_log::Log` that never
/// changes leader, so the whole archived history sits at that log's epoch.
fn archived_epoch(partition: &PartitionFixture) -> i32 {
    partition
        .expected_batches()
        .first()
        .expect("every fixture partition archives at least one batch")
        .partition_leader_epoch
}
