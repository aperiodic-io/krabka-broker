//! The `log_start_offset` an accepted `Produce` answers with.
//!
//! Kafka fills `LogAppendInfo.logStartOffset` from `UnifiedLog`'s own pointer
//! at append time, and `ReplicaManager` copies it straight into the partition
//! row, so an accepted produce reports where the partition's log actually
//! starts. Only the rows that refuse *before* any append carry the -1 that
//! `LogAppendInfo.UNKNOWN_LOG_APPEND_INFO` supplies.
//!
//! # Why the log is trimmed first
//!
//! A partition nobody has trimmed starts at 0, and 0 is also what a broker
//! that never filled the field at all would answer once the wire default
//! stopped being -1. So the case moves the pointer off 0 with a
//! `DeleteRecords` before it produces, and pins the produced row against the
//! low watermark that trim reported. A hardcoded 0 fails it, and so does the
//! -1 this suite was written for.
//!
//! # The values
//!
//! Settled against the pinned `mirror.gcr.io/apache/kafka:4.3.1` with a raw
//! `Produce v8`. Four single-record batches, then a
//! `kafka-delete-records --offset-json-file` to offset 3, which reported
//! `low_watermark: 3`, then one more produce:
//!
//! ```text
//! error_code=0 base_offset=4 log_append_time_ms=-1 log_start_offset=3
//! ```
//!
//! Those are the numbers this case asserts.
//!
//! Replaying the fourth batch's exact producer id, epoch and sequence — the
//! idempotent-retry path, which answers the already-assigned offset rather
//! than appending — answered with the same real `log_start_offset`, so the
//! dedup row carries it too. The dedup row also carries the retained batch's
//! timestamp in `log_append_time_ms`: `UnifiedLog.append` sets
//! `logAppendTime` from `BatchMetadata.timestamp`, which is the batch max
//! timestamp on a `CreateTime` topic.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        delete_records_response::DeleteRecordsPartitionResult,
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};

use crate::support::{
    offsets::{delete_records_partition, delete_records_request, delete_records_topic},
    records::{batch_from_records, value_record},
    topics::{creatable_topic, create_topic_request},
};

mod support;

/// Records appended before the trim, so the partition ends at offset 4.
const APPENDED_BEFORE_TRIM: i64 = 4;

/// Where the `DeleteRecords` moves the partition's log start. It is neither 0
/// nor the -1 sentinel, which is the whole point of the fixture.
const TRIM_TO: i64 = 3;

/// The idempotent producer id the retry case sends under. Any non-negative
/// value works; the broker learns it from the first batch that carries it.
const PRODUCER_ID: i64 = 777;

/// The max timestamp of every batch this suite sends. A duplicate row answers
/// with it in `log_append_time_ms`.
const BATCH_MAX_TIMESTAMP: i64 = 12_345;

async fn seeded_orders() -> (support::InProcess, WireUuid) {
    let process = Box::pin(support::start()).await;
    let topic = create_topic(&process.broker, &process.client, "orders").await;
    for offset in 0..APPENDED_BEFORE_TRIM {
        check!(
            produce(&process.client, "orders", topic).await == accepted(offset, 0),
            "the untrimmed partition starts at 0"
        );
    }
    (process, topic)
}

/// An accepted produce reports the partition's real log start offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_produce_reports_the_trimmed_log_start_offset() {
    let (p, topic_id) = seeded_orders().await;

    check!(
        trim(&p.client, "orders", TRIM_TO).await
            == DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: TRIM_TO,
                error_code: codes::NONE,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );

    assert!(
        produce(&p.client, "orders", topic_id).await == accepted(APPENDED_BEFORE_TRIM, TRIM_TO)
    );

    p.broker.shutdown().await;
}

/// An idempotent retry is an accepted produce too, so its row carries the same
/// real log start offset rather than the sentinel.
///
/// The retry is recognized by `(producer_id, epoch, sequence)` and answered
/// with the offset the first send was already assigned, without appending
/// anything. That row is built on a different code path from the append's, so
/// a fix that reached only the append would leave this one at -1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idempotent_retry_reports_the_trimmed_log_start_offset() {
    let (p, topic_id) = seeded_orders().await;

    check!(trim(&p.client, "orders", TRIM_TO).await.low_watermark == TRIM_TO);

    let first = produce_as(&p.client, "orders", topic_id, PRODUCER_ID, 0).await;
    check!(first == accepted(APPENDED_BEFORE_TRIM, TRIM_TO));

    // The same identity again. The log end offset does not move, so the row
    // below is the dedup path's and not a second append's.
    assert!(
        produce_as(&p.client, "orders", topic_id, PRODUCER_ID, 0).await
            == PartitionProduceResponse {
                log_append_time_ms: BATCH_MAX_TIMESTAMP,
                ..accepted(APPENDED_BEFORE_TRIM, TRIM_TO)
            }
    );
    check!(p.broker.local_log_end_offset("orders", 0) == Some(APPENDED_BEFORE_TRIM + 1));

    p.broker.shutdown().await;
}

/// The partition row an accepted produce answers with.
use crate::support::produce::expected_partition_success as accepted;

/// Create a one-partition topic and wait for its partition to exist locally.
async fn create_topic(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    name: &str,
) -> WireUuid {
    let response = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: name.to_owned(),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
    support::topic_id_for(client, name).await
}

/// Produce one single-record batch and hand back the whole partition row.
async fn produce(client: &Client, topic: &str, topic_id: WireUuid) -> PartitionProduceResponse {
    produce_as(client, topic, topic_id, -1, -1).await
}

/// Produce one single-record batch under the idempotent identity
/// `(producer_id, base_sequence)` and hand back the whole partition row.
///
/// A `producer_id` of -1 is "not idempotent", which is what [`produce`] sends.
async fn produce_as(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    producer_id: i64,
    base_sequence: i32,
) -> PartitionProduceResponse {
    let batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: BATCH_MAX_TIMESTAMP,
        producer_id,
        producer_epoch: 0,
        base_sequence,
        ..batch_from_records(vec![value_record(0, Some(Bytes::from_static(b"frame")))])
    };
    crate::support::client::produce_batch(
        client,
        batch,
        crate::support::client::BatchProduceSetup {
            topic,
            topic_id,
            ..Default::default()
        },
    )
    .await
}

/// Trim partition 0 of `topic` to `offset` and hand back the whole row.
async fn trim(client: &Client, topic: &str, offset: i64) -> DeleteRecordsPartitionResult {
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(
                topic.to_owned(),
                vec![delete_records_partition(0, offset)],
            )],
            5_000,
        ))
        .await
        .expect("DeleteRecords");
    response.topics[0].partitions[0].clone()
}
