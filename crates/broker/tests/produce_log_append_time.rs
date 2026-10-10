//! KIP-32's `message.timestamp.type` on the produce path: what a
//! `LogAppendTime` topic stores, what it answers in
//! `ProduceResponse.logAppendTimeMs`, and what the two
//! `message.timestamp.{before,after}.max.ms` windows refuse.
//!
//! # The claim
//!
//! Kafka's `LogValidator` rewrites three header fields of every batch on a
//! `LogAppendTime` topic — the timestamp-type attribute bit, `maxTimestamp`,
//! and the CRC — and `UnifiedLog.append` copies the clock reading it used into
//! `LogAppendInfo.logAppendTime`, which `ReplicaManager` puts in the partition
//! row as `logAppendTimeMs`. A `CreateTime` topic leaves both alone, and its
//! rows carry the `-1` that `LogAppendInfo` starts at.
//!
//! Every case here drives the real wire path against a live broker:
//! `CreateTopics` with the override, `Produce`, and `ListOffsets` by time. The
//! `CreateTime` half of each pair is the control. Without it a broker that
//! stamped every topic, or refused every timestamp, would pass this file.
//!
//! # Real time, not a mock clock
//!
//! The stamp is the broker's own clock at append, and no wire request can
//! reach a seam for it. So each case brackets the produce between two readings
//! of the same wall clock and asserts the stamp landed inside that window.
//! That is an assertion on the value, not on how long the test took: a
//! hardcoded constant, a producer timestamp echoed back, and the `-1` sentinel
//! all fail it.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        list_offsets_request::ListOffsetsRequest,
        produce_response::{BatchIndexAndErrorMessage, LeaderIdAndEpoch, PartitionProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, TimestampType},
};

use crate::support::{
    offsets::{list_offset_partition, single_partition_list_offsets},
    records::{batch_from_records, value_record},
};

mod support;

/// The producer's own timestamp on every batch these cases send. It is far
/// enough in the past that no clock reading can collide with it, so a case
/// that finds it stored on a `LogAppendTime` topic has caught a broker that
/// did not stamp.
const PRODUCER_TIMESTAMP_MS: i64 = 1_000;

/// The window the two rejection cases configure, in milliseconds. An hour is
/// wide enough that no test-run scheduling delay reaches it and narrow enough
/// that [`PRODUCER_TIMESTAMP_MS`] sits far outside it.
const WINDOW_MS: i64 = 3_600_000;

// This broker's wall clock, the one the log stamps from.
krabka_macros::unix_millis_fixture!(
    now_ms,
    "the clock is at or after the epoch",
    "a millisecond clock reading fits in i64"
);

/// A `LogAppendTime` topic answers the clock reading it stamped, and stores
/// that same reading rather than the producer's timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_append_time_topic_reports_the_stamp_it_wrote() {
    let p = support::start().await;
    let topic_id = ready_topic(
        &p.broker,
        &p.client,
        "audit",
        &[("message.timestamp.type", "LogAppendTime")],
    )
    .await;

    let before = now_ms();
    let row = produce(&p.client, "audit", topic_id, PRODUCER_TIMESTAMP_MS).await;
    let after = now_ms();

    check!(
        (before..=after).contains(&row.log_append_time_ms),
        "the row's stamp is the broker's clock at append, not {}",
        row.log_append_time_ms
    );
    assert!(row == accepted(0, row.log_append_time_ms));
    p.broker.wait_until_high_watermark("audit", 0, 1).await;
    // The stamp reached the records too, so a `ListOffsets` by time answers in
    // append time: the producer's own timestamp finds the batch, because it is
    // older than everything stored, and one millisecond past the stamp finds
    // nothing.
    check!(
        offset_for_time(&p.client, "audit", PRODUCER_TIMESTAMP_MS).await
            == Some((0, row.log_append_time_ms))
    );
    check!(offset_for_time(&p.client, "audit", row.log_append_time_ms + 1).await == None);

    p.broker.shutdown().await;
}

/// The default topic stamps nothing: the row carries `-1` and the records keep
/// the producer's own timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_time_topic_reports_no_stamp() {
    let p = support::start().await;
    let topic_id = ready_topic(&p.broker, &p.client, "orders", &[]).await;

    let row = produce(&p.client, "orders", topic_id, PRODUCER_TIMESTAMP_MS).await;

    assert!(row == accepted(0, -1));
    // `ListOffsets` answers out of the committed prefix, so the query waits for
    // the high watermark to cover the append rather than racing it.
    p.broker.wait_until_high_watermark("orders", 0, 1).await;
    check!(
        offset_for_time(&p.client, "orders", PRODUCER_TIMESTAMP_MS).await
            == Some((0, PRODUCER_TIMESTAMP_MS)),
        "a CreateTime topic keeps the producer's timestamp"
    );

    p.broker.shutdown().await;
}

/// `message.timestamp.before.max.ms` refuses a record older than the window
/// and admits one inside it, which is Kafka's `INVALID_TIMESTAMP` (32) over
/// the whole batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timestamp_older_than_the_window_is_refused() {
    let p = support::start().await;
    let topic_id = ready_topic(
        &p.broker,
        &p.client,
        "metrics",
        &[("message.timestamp.before.max.ms", &WINDOW_MS.to_string())],
    )
    .await;

    let refused = produce(&p.client, "metrics", topic_id, PRODUCER_TIMESTAMP_MS).await;
    check!(refused == refusal(PRODUCER_TIMESTAMP_MS, &refused));
    check!(
        p.broker.local_log_end_offset("metrics", 0) == Some(0),
        "a refused batch must not have appended"
    );

    let accepted_row = produce(&p.client, "metrics", topic_id, now_ms()).await;
    assert!(accepted_row.error_code == codes::NONE);
    assert!(accepted_row.base_offset == 0);

    p.broker.shutdown().await;
}

/// `message.timestamp.after.max.ms` refuses a record further ahead of the
/// broker's clock than the window allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timestamp_newer_than_the_window_is_refused() {
    let p = support::start().await;
    let topic_id = ready_topic(
        &p.broker,
        &p.client,
        "forecasts",
        &[("message.timestamp.after.max.ms", &WINDOW_MS.to_string())],
    )
    .await;

    let ahead_ms = now_ms() + 2 * WINDOW_MS;
    let refused = produce(&p.client, "forecasts", topic_id, ahead_ms).await;
    check!(refused == refusal(ahead_ms, &refused));

    let accepted_row = produce(&p.client, "forecasts", topic_id, now_ms()).await;
    assert!(accepted_row.error_code == codes::NONE);

    p.broker.shutdown().await;
}

/// A `LogAppendTime` topic ignores the windows, as Kafka's `validateTimestamp`
/// does: it tests a record's timestamp only under `CreateTime`, because the
/// stamp overwrites every producer timestamp anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_append_time_ignores_the_timestamp_windows() {
    let p = support::start().await;
    let topic_id = ready_topic(
        &p.broker,
        &p.client,
        "traces",
        &[
            ("message.timestamp.type", "LogAppendTime"),
            ("message.timestamp.before.max.ms", &WINDOW_MS.to_string()),
        ],
    )
    .await;

    let before = now_ms();
    // Older than `before.max.ms` allows, and admitted regardless.
    let row = produce(&p.client, "traces", topic_id, PRODUCER_TIMESTAMP_MS).await;
    let after = now_ms();

    check!((before..=after).contains(&row.log_append_time_ms));
    assert!(row == accepted(0, row.log_append_time_ms));

    p.broker.shutdown().await;
}

/// The partition row an accepted produce answers with.
fn accepted(base_offset: i64, log_append_time_ms: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset,
        log_append_time_ms,
        log_start_offset: 0,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// The partition row a single-record batch stamped `timestamp_ms`, outside the
/// window, answers with: Kafka's `RecordValidationException` row from
/// `ReplicaManager.appendToLocalLog`. The base offset and append time are the
/// -1 of `LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo`, the log start
/// offset is the partition's own, and the batch's one record carries
/// `LogValidator.validateTimestamp`'s message under the fixed top-level
/// message of `processRecordErrors`.
///
/// The per-record message ends with the window the broker computed from its
/// own clock, which the test cannot know, so it is taken from `actual` once
/// everything before the window has been checked.
fn refusal(timestamp_ms: i64, actual: &PartitionProduceResponse) -> PartitionProduceResponse {
    let prefix = format!(
        "Timestamp {timestamp_ms} of message with offset 0 is out of range. The timestamp should \
         be within ["
    );
    let message = actual
        .record_errors
        .first()
        .and_then(|error| error.batch_index_error_message.clone())
        .filter(|message| message.starts_with(&prefix));
    PartitionProduceResponse {
        index: 0,
        error_code: codes::INVALID_TIMESTAMP,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: 0,
        record_errors: vec![BatchIndexAndErrorMessage {
            batch_index: 0,
            batch_index_error_message: Some(message.unwrap_or(prefix)),
            ..BatchIndexAndErrorMessage::default()
        }],
        error_message: Some(
            "One or more records have been rejected due to invalid timestamp".into(),
        ),
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// Create a one-partition topic with `overrides`, wait until the overrides have
/// reached the metadata image the produce path reads and, for a
/// `LogAppendTime` topic, the partition's own log config, then return its id.
async fn ready_topic(
    broker: &BrokerHandle,
    client: &Client,
    name: &str,
    overrides: &[(&str, &str)],
) -> WireUuid {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: overrides
                    .iter()
                    .map(|(key, value)| CreatableTopicConfig {
                        name: (*key).to_owned(),
                        value: Some((*value).to_owned()),
                        ..CreatableTopicConfig::default()
                    })
                    .collect(),
                ..CreatableTopic::default()
            }],
            timeout_ms: 5_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == codes::NONE,
        "{:?}",
        response.topics[0].error_message
    );
    broker.wait_until_partition_present(name, 0).await;
    let expected: Vec<(String, String)> = overrides
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    broker
        .wait_for_image(|image| {
            expected.iter().all(|(key, value)| {
                image
                    .topic_config(name)
                    .and_then(|configs| configs.get(key))
                    .is_some_and(|stored| stored == value)
            })
        })
        .await;
    // The stamp is the log's, so a `LogAppendTime` case must also wait for the
    // override to travel from the image into the partition's own `LogConfig`.
    if overrides.contains(&("message.timestamp.type", "LogAppendTime")) {
        broker
            .wait_for_metrics(
                "message.timestamp.type reaches the partition LogConfig",
                |_| {
                    broker
                        .partition_log_config_for_test(name, 0)
                        .is_some_and(|config| {
                            config.message_timestamp_type == TimestampType::LogAppendTime
                        })
                },
            )
            .await;
    }
    support::topic_id_for(client, name).await
}

/// Produce one single-record batch whose only record carries `timestamp_ms`,
/// and hand back the whole partition row.
async fn produce(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    timestamp_ms: i64,
) -> PartitionProduceResponse {
    let batch = RecordBatch {
        last_offset_delta: 0,
        base_timestamp: timestamp_ms,
        max_timestamp: timestamp_ms,
        ..batch_from_records(vec![Record {
            timestamp_delta: 0,
            ..value_record(0, Some(Bytes::from_static(b"frame")))
        }])
    };
    let response = crate::support::produce::send_batch(
        &client,
        batch,
        crate::support::produce::SinglePartitionProduceSetup {
            topic: topic.to_owned(),
            topic_id,
            ..Default::default()
        },
    )
    .await;
    response.responses[0].partition_responses[0].clone()
}

/// `ListOffsets` by time: the first offset at or after `timestamp_ms`, with the
/// timestamp stored for it. `None` is Kafka's "no such offset" answer, the row
/// whose offset is -1.
async fn offset_for_time(client: &Client, topic: &str, timestamp_ms: i64) -> Option<(i64, i64)> {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(
                topic.to_owned(),
                list_offset_partition(0, timestamp_ms),
            )
        })
        .await
        .expect("ListOffsets");
    let row = &response.topics[0].partitions[0];
    assert!(row.error_code == codes::NONE);
    (row.offset >= 0).then_some((row.offset, row.timestamp))
}
