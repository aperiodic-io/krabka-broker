//! The two requests every case sends, and the vocabulary it judges the answers
//! by.
//!
//! A case in this suite asks "where does this partition end, as each isolation
//! level sees it right now", and [`EndOfPartition`] answers with both readings
//! at once so the case compares one whole struct rather than two offsets in
//! sequence. [`matched_row`], [`refused_row`] and [`latest_row`] build the
//! expected values, spelling out every field of the response so an isolation
//! level that quietly changed the error code or the timestamp fails here too.
//!
//! The rest is the setup each case shares: creating the one-partition topic,
//! producing the records that precede the transaction, and waiting until the
//! log has settled so a reading taken next measures the state the case meant.

use assert2::check;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    list_offsets_request::ListOffsetsRequest, list_offsets_response::ListOffsetsPartitionResponse,
};

use crate::support::{
    offsets::{list_offset_partition, single_partition_list_offsets},
    topics::{creatable_topic, create_topic_request},
};

/// Request timestamp sentinel (-1) asking for the end of the partition.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;
/// Request `replica_id` (-1) that marks an ordinary client. Kafka's
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`.
const CONSUMER_REPLICA_ID: i32 = -1;
/// Request `isolation_level` (0). Kafka's `IsolationLevel.READ_UNCOMMITTED`.
const READ_UNCOMMITTED: i8 = 0;
/// Request `isolation_level` (1). Kafka's `IsolationLevel.READ_COMMITTED`.
const READ_COMMITTED: i8 = 1;

/// The end of one partition as the two isolation levels see it at one instant.
///
/// The pair is one value so a case asserts against a whole expected struct
/// rather than against two offsets in sequence, which is what makes "they
/// disagree here and agree there" a single readable claim.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct EndOfPartition {
    pub(super) read_uncommitted: ListOffsetsPartitionResponse,
    pub(super) read_committed: ListOffsetsPartitionResponse,
}

/// The whole row a partition 0 answers with for a sentinel that matched a
/// record: the offset it matched and that record's timestamp.
///
/// Every partition this suite builds stays on leader epoch 0 for its whole
/// life, so a resolved match reports that epoch. A refusal (`offset == -1`,
/// Kafka's `Unknown` selection outcome) reports the unknown-epoch sentinel
/// instead, regardless of the live epoch: Kafka never fills the epoch for an
/// answer it did not resolve.
pub(super) fn matched_row(offset: i64, timestamp: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: codes::NONE,
        timestamp,
        offset,
        leader_epoch: if offset < 0 { -1 } else { 0 },
        ..Default::default()
    }
}

/// The whole row a partition 0 answers with when the record a sentinel matched
/// sits at or above the request's bound.
///
/// `ReplicaManager.fetchOffset` builds this with
/// `buildErrorResponse(Errors.NONE, partition)`, so the refusal reports *no
/// error*: a client is told the partition has no answer for it, not that the
/// partition is unavailable. Asserting the error code is `NONE` here is what
/// keeps a future implementation from turning a fence into a retryable failure
/// that would spin a consumer forever.
pub(super) fn refused_row() -> ListOffsetsPartitionResponse {
    matched_row(-1, -1)
}

/// The whole `LATEST` row a healthy partition 0 answers with.
///
/// `LATEST` matches no record, so the response echoes Kafka's
/// `UNKNOWN_TIMESTAMP` (-1), but it does report the partition's live leader
/// epoch: Kafka fills it from `Partition.getLeaderEpoch` rather than leaving
/// it unknown. Spelling the full row out is what makes an isolation level
/// that quietly changed the error code or the timestamp fail here too.
pub(super) fn latest_row(offset: i64) -> ListOffsetsPartitionResponse {
    matched_row(offset, -1)
}

pub(super) async fn create_topic(client: &Client, name: &str) {
    let response = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (name).into(),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    check!(
        response.topics[0].error_code == codes::NONE,
        "create_topic({name}): {response:?}"
    );
}

/// One `ListOffsets` for partition 0 of `topic` at one sentinel and one
/// isolation level.
async fn list_offset(
    client: &Client,
    topic: &str,
    timestamp: i64,
    isolation_level: i8,
) -> ListOffsetsPartitionResponse {
    let mut response = client
        .send(ListOffsetsRequest {
            replica_id: CONSUMER_REPLICA_ID,
            isolation_level,
            timeout_ms: 5_000,
            ..single_partition_list_offsets(topic, list_offset_partition(0, timestamp))
        })
        .await
        .expect("ListOffsets");
    response.topics.remove(0).partitions.remove(0)
}

/// Ask one sentinel of `topic` at both isolation levels.
pub(super) async fn both_levels(client: &Client, topic: &str, timestamp: i64) -> EndOfPartition {
    EndOfPartition {
        read_uncommitted: list_offset(client, topic, timestamp, READ_UNCOMMITTED).await,
        read_committed: list_offset(client, topic, timestamp, READ_COMMITTED).await,
    }
}

/// Ask for the end of `topic` at both isolation levels.
pub(super) async fn end_of_partition(client: &Client, topic: &str) -> EndOfPartition {
    both_levels(client, topic, LATEST_TIMESTAMP).await
}

/// Wait until partition 0 of `topic` holds `offset` records and has committed
/// all of them, so a `ListOffsets` taken next is reading settled state.
///
/// Both bounds are needed. The log end offset says the append landed, and the
/// high watermark says it is acknowledged -- and the last stable offset a
/// `read_committed` client is answered with is capped at the high watermark, so
/// a reading taken before the watermark caught up would measure the wrong
/// thing.
pub(super) async fn wait_for_settled_log(broker: &BrokerHandle, topic: &str, offset: i64) {
    broker
        .wait_until_local_log_end_offset(topic, 0, offset)
        .await;
    broker.wait_until_high_watermark(topic, 0, offset).await;
}

fn record(topic: &str, value: &'static str) -> ProducerRecord {
    crate::support::producer::producer_record(crate::support::producer::ProducerRecordSetup {
        topic: (topic).into(),
        value: Some(Bytes::from_static(value.as_bytes())),
        ..Default::default()
    })
}

pub(super) async fn send_ok(producer: &Producer, topic: &str, value: &'static str) {
    producer
        .send(record(topic, value))
        .await
        .expect("produce acknowledged");
}

/// Produce one record carrying an explicit `timestamp_ms`.
///
/// The timestamp cases need to know what they are looking up, and a record left
/// to the producer's wall clock cannot be named in an assertion. Fixed
/// timestamps also keep the ordinary records far below the transactional ones,
/// so a lookup can aim either side of the bound on purpose.
pub(super) async fn send_at(
    producer: &Producer,
    topic: &str,
    value: &'static str,
    timestamp_ms: i64,
) {
    producer
        .send(ProducerRecord {
            timestamp_ms: Some(timestamp_ms),
            ..record(topic, value)
        })
        .await
        .expect("produce acknowledged");
}
