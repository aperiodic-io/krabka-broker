//! Handler tests for how the `ShareFetch` byte limit bounds acquisition.
//!
//! Kafka reads the log first and acquires second: `SharePartition.acquire`
//! acquires no offset past the last batch that the read returned. Every
//! offset in `AcquiredRecords` therefore has its record in `Records`. The Java
//! share consumer treats an acquired offset with no record as a gap, and
//! acknowledges it as `Gap`, which archives the record without delivery.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        share_fetch_request::{
            AcknowledgementBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::{PartitionData, ShareFetchResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{decode_response, encode_request, peer, principal, start_broker_no_audit_with},
};

/// The number of batches that each topic holds.
const BATCHES: i64 = 4;

/// The records in each batch.
const RECORDS_PER_BATCH: i64 = 2;

/// Produce v12 names the topic.
const PRODUCE_VERSION: i16 = 12;

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_with_delivery_attempts(5).await
}

async fn start_with_delivery_attempts(attempts: i16) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.max_delivery_attempts = attempts;
    })
    .await
}

async fn start_with_fetch_cap(cap: i32) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.fetch_max_bytes = ByteSize::from_bytes(u64::try_from(cap).expect("positive cap"));
    })
    .await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_with_partitions(broker, name, 1).await
}

async fn create_topic_with_partitions(
    broker: &BrokerHandle,
    name: &str,
    partitions: i32,
) -> WireUuid {
    crate::handlers::test_support::create_topic(
        broker,
        crate::handlers::test_support::ClientTopicSetup {
            client_id: "share-fetch-byte-limit-test",
            name,
            partitions: crate::handlers::test_support::TopicPartitionCount(partitions),
        },
    )
    .await
}

/// Appends [`BATCHES`] batches of the same size, each in its own produce.
async fn produce_batches(broker: &BrokerHandle, topic: &str) {
    for _ in 0..BATCHES {
        produce_batch(broker, topic, 0).await;
    }
}

/// Appends one batch of [`RECORDS_PER_BATCH`] records to `partition`.
async fn produce_batch(broker: &BrokerHandle, topic: &str, partition: i32) {
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("producer"),
        client_id = "producer-client"
    );
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(RecordsPayload::V2(vec![RecordBatch {
                    last_offset_delta: i32::try_from(RECORDS_PER_BATCH - 1).expect("small batch"),
                    records: (0..RECORDS_PER_BATCH)
                        .map(|delta| Record {
                            offset_delta: i32::try_from(delta).expect("small batch"),
                            value: Some(Bytes::from(vec![b'v'; 256])),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let request_bytes = encode_request(&request, PRODUCE_VERSION);
    let response_bytes = crate::handlers::produce::handle(
        &shared,
        PRODUCE_VERSION,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    let response: ProduceResponse = decode_response(&response_bytes, PRODUCE_VERSION);
    let row = &response.responses[0].partition_responses[0];
    assert!(row.error_code == codes::NONE, "{response:?}");
}

/// The size in bytes of the first batch of `topic`. Every batch has this size.
fn batch_size(broker: &BrokerHandle, topic: &str) -> i32 {
    let shared = broker.broker_arc_for_test();
    let partition = shared
        .partitions
        .get(topic, PartitionIndex(0))
        .expect("local partition");
    let log = partition.log.lock().expect("log lock");
    let read = log
        .read_raw(Offset(0), Offset(1), ByteSize::from_bytes(0))
        .expect("read the first batch");
    i32::try_from(read.total).expect("small batch")
}

async fn share_fetch(
    broker: &BrokerHandle,
    group: &str,
    member: &str,
    epoch: i32,
    topic_id: WireUuid,
    (max_records, max_bytes): (i32, i32),
    acknowledgements: &[(i64, i64, i8)],
) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes,
        max_records,
        batch_size: max_records,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
                acknowledgement_batches: acknowledgements
                    .iter()
                    .map(
                        |&(first_offset, last_offset, ack_type)| AcknowledgementBatch {
                            first_offset,
                            last_offset,
                            acknowledge_types: vec![ack_type],
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    send_share_fetch(broker, &request).await
}

async fn send_share_fetch(
    broker: &BrokerHandle,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    crate::handlers::test_support::share_fetch_wire(
        broker,
        request,
        crate::handlers::test_support::ShareWireSetup {
            version: krabka_ids::ApiVersion(
                krabka_protocol::owned::share_fetch_request::MAX_VERSION,
            ),
            ..Default::default()
        },
    )
    .await
}

/// The one partition row of `response`. An incremental response leaves out a
/// partition with nothing new, which reads as an empty row.
fn partition(response: &ShareFetchResponse) -> PartitionData {
    response
        .responses
        .first()
        .and_then(|topic| topic.partitions.first())
        .cloned()
        .unwrap_or_default()
}

/// The `(first, last)` offsets of every acquired row.
fn acquired(row: &PartitionData) -> Vec<(i64, i64)> {
    crate::handlers::test_support::acquired_share_records(row)
}

/// The offsets of every record that the row carries.
fn record_offsets(row: &PartitionData) -> Vec<i64> {
    row.records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .records
                        .iter()
                        .map(move |record| batch.base_offset + i64::from(record.offset_delta))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What one scenario observes: the offsets that the limited fetch acquired,
/// the record offsets that it carried, and the offsets that a later unlimited
/// fetch by another member acquired.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    acquired: Vec<(i64, i64)>,
    records: Vec<i64>,
    remainder: Vec<(i64, i64)>,
}

/// One scenario: the record and byte limits of the first fetch, in batch
/// sizes, and the outcome that Kafka gives.
struct Case {
    name: &'static str,
    max_records: i32,
    /// The byte limit as `(numerator, denominator)` of one batch size.
    max_bytes_in_batches: (i32, i32),
    expected: Outcome,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "all-batches-fit",
            max_records: 500,
            max_bytes_in_batches: (4, 1),
            expected: Outcome {
                acquired: vec![(0, 7)],
                records: (0..=7).collect(),
                remainder: Vec::new(),
            },
        },
        Case {
            name: "one-batch-fits",
            max_records: 500,
            max_bytes_in_batches: (1, 1),
            expected: Outcome {
                acquired: vec![(0, 1)],
                records: vec![0, 1],
                remainder: vec![(2, 7)],
            },
        },
        Case {
            name: "less-than-one-batch",
            max_records: 500,
            max_bytes_in_batches: (1, 2),
            expected: Outcome {
                acquired: vec![(0, 1)],
                records: vec![0, 1],
                remainder: vec![(2, 7)],
            },
        },
        Case {
            // `batch_optimized`: MaxRecords is a soft limit that rounds up to
            // the end of the log batch holding the third record.
            name: "records-limit-below-bytes-limit",
            max_records: 3,
            max_bytes_in_batches: (4, 1),
            expected: Outcome {
                acquired: vec![(0, 3)],
                records: (0..=3).collect(),
                remainder: vec![(4, 7)],
            },
        },
    ]
}

#[tokio::test]
async fn every_acquired_offset_has_its_record_in_the_response() {
    let (broker, _dir) = start().await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let topic = format!("byte-limit-{}", case.name);
        let group = format!("group-{}", case.name);
        let topic_id = create_topic(&broker, &topic).await;
        crate::test_support::initialize_share_state(
            &broker,
            &group,
            uuid::Uuid::from_bytes(topic_id.0),
            0,
        )
        .await;
        // Open both sessions on the empty log, so the share partition starts
        // at offset 0 under the default `latest` reset.
        for member in ["limited", "unlimited"] {
            let opened =
                share_fetch(&broker, &group, member, 0, topic_id, (500, 1 << 20), &[]).await;
            assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
        }
        produce_batches(&broker, &topic).await;
        let size = batch_size(&broker, &topic);
        let (numerator, denominator) = case.max_bytes_in_batches;

        let limited = share_fetch(
            &broker,
            &group,
            "limited",
            1,
            topic_id,
            (case.max_records, size * numerator / denominator),
            &[],
        )
        .await;
        let rest = share_fetch(
            &broker,
            &group,
            "unlimited",
            1,
            topic_id,
            (500, 1 << 20),
            &[],
        )
        .await;

        actual.push((
            case.name,
            Outcome {
                acquired: acquired(&partition(&limited)),
                records: record_offsets(&partition(&limited)),
                remainder: acquired(&partition(&rest)),
            },
        ));
        expected.push((case.name, case.expected));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The broker cap applies to `ShareFetch`'s whole-response budget, and the
/// first complete batch can exceed it so the member still makes progress.
#[tokio::test]
async fn broker_fetch_cap_limits_share_fetch_and_keeps_first_batch_progress() {
    let (broker, _dir) = start_with_fetch_cap(1).await;
    let topic = "broker-fetch-cap";
    let group = "broker-fetch-cap-group";
    let topic_id = create_topic(&broker, topic).await;
    crate::test_support::initialize_share_state(
        &broker,
        group,
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let opened = share_fetch(&broker, group, "member", 0, topic_id, (500, 1 << 20), &[]).await;
    assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    produce_batches(&broker, topic).await;

    let response = share_fetch(&broker, group, "member", 1, topic_id, (500, 1 << 20), &[]).await;
    let row = partition(&response);
    assert!(acquired(&row) == vec![(0, 1)]);
    assert!(record_offsets(&row) == vec![0, 1]);
    broker.shutdown().await;
}

/// The acknowledge type `Release`.
const RELEASE: i8 = 2;

/// The log read starts at the first record that the partition can still
/// deliver, so a released record at the delivery limit must be archived
/// before the read. Otherwise it stays `Available`, the window cannot grow
/// past it, and the partition never delivers a later record.
#[tokio::test]
async fn a_record_at_the_delivery_limit_does_not_stall_the_partition() {
    let (broker, _dir) = start_with_delivery_attempts(1).await;
    let topic_id = create_topic(&broker, "delivery-limit").await;
    crate::test_support::initialize_share_state(
        &broker,
        "g",
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let opened = share_fetch(&broker, "g", "m", 0, topic_id, (500, 1 << 20), &[]).await;
    assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    produce_batches(&broker, "delivery-limit").await;
    let first = share_fetch(&broker, "g", "m", 1, topic_id, (500, 1 << 20), &[]).await;
    produce_batches(&broker, "delivery-limit").await;

    // Release every record at its only delivery attempt, and fetch.
    let after_release = share_fetch(
        &broker,
        "g",
        "m",
        2,
        topic_id,
        (500, 1 << 20),
        &[(0, 7, RELEASE)],
    )
    .await;

    assert!(
        (
            acquired(&partition(&first)),
            acquired(&partition(&after_release))
        ) == (vec![(0, 7)], vec![(8, 15)])
    );
    broker.shutdown().await;
}

/// A `ShareFetch` of every partition of `topic_id`, with no wait.
use crate::handlers::test_support::{ShareSessionEpoch, TopicPartitionCount};

#[derive(Clone, Copy)]
struct ShareResponseByteLimit(i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FetchPartitionsSetup<'a> {
    #[default("g")]
    group: &'a str,
    #[default(ShareSessionEpoch(0))]
    epoch: ShareSessionEpoch,
    #[default(WireUuid::ZERO)]
    topic_id: WireUuid,
    partitions: TopicPartitionCount,
    #[default(ShareResponseByteLimit(1 << 20))]
    maximum: ShareResponseByteLimit,
}

fn fetch_partitions(setup: FetchPartitionsSetup<'_>) -> ShareFetchRequest {
    let FetchPartitionsSetup {
        group,
        epoch,
        topic_id,
        partitions,
        maximum,
    } = setup;
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch.0,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: maximum.0,
        max_records: 500,
        batch_size: 500,
        topics: vec![FetchTopic {
            topic_id,
            partitions: (0..partitions.0)
                .map(|partition_index| FetchPartition {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// What a fetch over several partitions carried: how many rows held records,
/// and how many batches they held in all.
#[derive(Debug, PartialEq, Eq)]
struct Spread {
    rows_with_records: usize,
    batches: usize,
}

fn spread(response: &ShareFetchResponse) -> Spread {
    let batches: Vec<usize> = response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| {
            row.records
                .as_ref()
                .and_then(RecordsPayload::as_v2)
                .map_or(0, <[RecordBatch]>::len)
        })
        .filter(|count| *count > 0)
        .collect();
    Spread {
        rows_with_records: batches.len(),
        batches: batches.iter().sum(),
    }
}

/// Kafka's `ReplicaManager.readFromLog` lets only the first partition that
/// returns records exceed the byte budget, by the one batch that its read
/// starts with (`minOneMessage`). A later partition whose next batch does not
/// fit what is left returns no records, so the response exceeds `MaxBytes`
/// by at most one batch however many partitions it covers.
#[tokio::test]
async fn only_the_first_partition_may_exceed_the_byte_budget() {
    const PARTITIONS: i32 = 3;
    let (broker, _dir) = start().await;

    // `(name, MaxBytes as (numerator, denominator) of one batch, expected)`.
    let cases = [
        (
            "half-a-batch",
            (1, 2),
            Spread {
                rows_with_records: 1,
                batches: 1,
            },
        ),
        (
            "half-a-batch-each",
            (3, 2),
            Spread {
                rows_with_records: 1,
                batches: 1,
            },
        ),
        (
            "a-batch-each",
            (3, 1),
            Spread {
                rows_with_records: 3,
                batches: 3,
            },
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, (numerator, denominator), want) in cases {
        let topic = format!("spread-{name}");
        let group = format!("group-{name}");
        let topic_id = create_topic_with_partitions(&broker, &topic, PARTITIONS).await;
        for partition in 0..PARTITIONS {
            crate::test_support::initialize_share_state(
                &broker,
                &group,
                uuid::Uuid::from_bytes(topic_id.0),
                partition,
            )
            .await;
        }
        let opened = send_share_fetch(
            &broker,
            &fetch_partitions(FetchPartitionsSetup {
                group: &group,
                topic_id,
                partitions: TopicPartitionCount(PARTITIONS),
                ..Default::default()
            }),
        )
        .await;
        assert!(spread(&opened).rows_with_records == 0, "{opened:?}");
        for partition in 0..PARTITIONS {
            produce_batch(&broker, &topic, partition).await;
        }
        let size = batch_size(&broker, &topic);

        let limited = send_share_fetch(
            &broker,
            &fetch_partitions(FetchPartitionsSetup {
                group: &group,
                epoch: ShareSessionEpoch(1),
                topic_id,
                partitions: TopicPartitionCount(PARTITIONS),
                maximum: ShareResponseByteLimit(size * numerator / denominator),
            }),
        )
        .await;

        actual.push((name, spread(&limited)));
        expected.push((name, want));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// Sends a `ShareAcknowledge` from `member` that releases `[first, last]`.
async fn release(
    broker: &BrokerHandle,
    group: &str,
    member: &str,
    epoch: i32,
    topic_id: WireUuid,
    (first_offset, last_offset): (i64, i64),
) {
    let version = krabka_protocol::owned::share_acknowledge_request::MAX_VERSION;
    let request = crate::handlers::test_support::acknowledge_batches_request(
        crate::handlers::test_support::AcknowledgementSetup {
            member,

            partition: crate::handlers::test_support::AcknowledgementPartitionSetup::single_batch(
                krabka_log::Offset(first_offset)..=krabka_log::Offset(last_offset),
                crate::handlers::test_support::AcknowledgementCode(RELEASE),
            ),
            ..crate::handlers::test_support::AcknowledgementSetup::for_topic_session(
                group,
                crate::handlers::test_support::ShareSessionEpoch(epoch),
                topic_id,
            )
        },
    );
    let response = crate::handlers::test_support::share_acknowledge_wire(
        broker,
        &request,
        crate::handlers::test_support::ShareWireSetup {
            version: krabka_ids::ApiVersion(version),
            ..Default::default()
        },
    )
    .await;
    let row = &response.responses[0].partitions[0];
    assert!(
        (response.error_code, row.error_code) == (codes::NONE, codes::NONE),
        "{response:?}"
    );
}

/// A pass that acquires part of a batch carries the whole batch. When
/// `MinBytes` holds the request for a later pass, and that pass acquires the
/// rest of the batch, its read starts inside the same batch and returns it
/// again. Kafka reads the log once per response, so the response carries the
/// batch once, with both acquired runs.
#[tokio::test]
async fn a_batch_acquired_over_two_passes_is_carried_once() {
    let (broker, _dir) = start().await;
    let topic_id = create_topic(&broker, "two-passes").await;
    crate::test_support::initialize_share_state(
        &broker,
        "g",
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    for member in ["waiting", "other"] {
        let opened = share_fetch(&broker, "g", member, 0, topic_id, (500, 1 << 20), &[]).await;
        assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    }
    // One batch at offsets 0-1. `other` holds it, then releases offset 0.
    produce_batch(&broker, "two-passes", 0).await;
    let held = share_fetch(&broker, "g", "other", 1, topic_id, (500, 1 << 20), &[]).await;
    assert!(acquired(&partition(&held)) == vec![(0, 1)]);
    release(&broker, "g", "other", 2, topic_id, (0, 0)).await;

    // The first pass acquires offset 0 and falls short of `MinBytes`. While
    // the request waits, `other` releases offset 1, which the last pass
    // acquires.
    let mut request = fetch_partitions(FetchPartitionsSetup {
        epoch: ShareSessionEpoch(1),
        topic_id,
        ..Default::default()
    });
    request.member_id = Some("waiting".into());
    request.min_bytes = 1 << 20;
    request.max_wait_ms = 1_000;
    let (response, ()) = tokio::join!(send_share_fetch(&broker, &request), async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        release(&broker, "g", "other", 3, topic_id, (1, 1)).await;
    });

    let row = partition(&response);
    assert!((acquired(&row), record_offsets(&row)) == (vec![(0, 0), (1, 1)], vec![0, 1]));
    broker.shutdown().await;
}
