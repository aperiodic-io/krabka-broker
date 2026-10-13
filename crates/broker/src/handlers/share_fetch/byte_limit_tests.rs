//! Handler tests for how the `ShareFetch` byte limit bounds acquisition.
//!
//! Kafka reads the log first and acquires second: `SharePartition.acquire`
//! acquires no offset past the last batch that the read returned. Every
//! offset in `AcquiredRecords` therefore has its record in `Records`. The Java
//! share consumer treats an acquired offset with no record as a gap, and
//! acknowledges it as `Gap`, which archives the record without delivery.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        handlers::{
            produce::handle,
            test_support::{
                AcknowledgementCode, AcknowledgementPartitionSetup, AcknowledgementSetup,
                OffsetRun, ShareSessionEpoch, StartedBroker, TopicPartitionCount,
                acknowledge_batches_request, acquired_share_records,
                create_topic as create_topic_from_test_support, produce_partition_request,
                share_acknowledge_wire, share_fetch_wire,
            },
        },
        test_support::{
            decode_response, encode_request, initialize_share_state, start_broker_no_audit_with,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_protocol::{
        owned::{
            produce_response::ProduceResponse,
            share_fetch_request::{
                AcknowledgementBatch, FetchPartition, FetchTopic, ShareFetchRequest,
            },
            share_fetch_response::{PartitionData, ShareFetchResponse},
        },
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch, RecordsPayload},
    },
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
    std::sync::Arc,
};

/// The number of batches that each topic holds.
const BATCHES: i64 = 4;

/// The records in each batch.
const RECORDS_PER_BATCH: i64 = 2;

/// Produce v12 names the topic.
const PRODUCE_VERSION: i16 = 12;

async fn start() -> StartedBroker { start_with_delivery_attempts(5).await }

async fn start_with_delivery_attempts(attempts: i16) -> StartedBroker {
    let (broker, dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.max_delivery_attempts = attempts;
    })
    .await;
    StartedBroker { broker, dir }
}

async fn start_with_fetch_cap(cap: i32) -> StartedBroker {
    let (broker, dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.fetch_max_bytes = ByteSize::from_bytes(u64::try_from(cap).expect("positive cap"));
    })
    .await;
    StartedBroker { broker, dir }
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_with_partitions(broker, name, 1).await
}

async fn create_topic_with_partitions(
    broker: &BrokerHandle,
    name: &str,
    partitions: i32,
) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("share-fetch-byte-limit-test")
        .name(name)
        .partitions(partitions)
        .call()
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
    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["producer"];
        client ["producer-client"];
    };
    let request = produce_partition_request()
        .topic(topic.to_owned())
        .partition(partition)
        .records(RecordsPayload::V2(vec![RecordBatch {
            last_offset_delta: i32::try_from(RECORDS_PER_BATCH - 1).expect("small batch"),
            records: (0..RECORDS_PER_BATCH)
                .map(|delta| Record {
                    offset_delta: i32::try_from(delta).expect("small batch"),
                    value: Some(Bytes::from(vec![b'v'; 256])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }]))
        .acks(-1)
        .call();
    let request_bytes = encode_request(&request, PRODUCE_VERSION);
    let response_bytes = handle(
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

#[derive(Clone, Copy)]
struct FetchAcknowledgement {
    first_offset: i64,
    last_offset: i64,
    ack_type: i8,
}

#[bon::builder]
async fn share_fetch(
    broker: &BrokerHandle,
    group: &str,
    member: &str,
    topic_id: [u8; 16],
    #[builder(default)] epoch: i32,
    #[builder(default = 500)] max_records: i32,
    #[builder(default = 1 << 20)] max_bytes: i32,
    #[builder(default)] acknowledgements: Vec<FetchAcknowledgement>,
) -> ShareFetchResponse {
    let acknowledgement_batches = acknowledgements
        .into_iter()
        .map(|ack| AcknowledgementBatch {
            first_offset: ack.first_offset,
            last_offset: ack.last_offset,
            acknowledge_types: vec![ack.ack_type],
            ..Default::default()
        })
        .collect();
    let request = super::test_fetch_request()
        .group_id(group.into())
        .member_id(member.into())
        .topic_id(topic_id)
        .share_session_epoch(epoch)
        .max_records(max_records)
        .max_bytes(max_bytes)
        .acknowledgement_batches(acknowledgement_batches)
        .call();
    send_share_fetch(broker, &request).await
}

async fn send_share_fetch(
    broker: &BrokerHandle,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    share_fetch_wire()
        .broker(broker)
        .request(request)
        .version(krabka_protocol::owned::share_fetch_request::MAX_VERSION)
        .call()
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

/// The first and last offsets of every acquired row.
fn acquired(row: &PartitionData) -> Vec<OffsetRun> { acquired_share_records(row) }

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
    acquired: Vec<OffsetRun>,
    records: Vec<i64>,
    remainder: Vec<OffsetRun>,
}

/// A fraction of one batch size.
#[derive(Clone, Copy)]
struct BatchFraction {
    numerator: i32,
    denominator: i32,
}

/// One scenario: the record and byte limits of the first fetch, in batch
/// sizes, and the outcome that Kafka gives.
struct Case {
    name: &'static str,
    max_records: i32,
    /// The byte limit as a fraction of one batch size.
    max_bytes_in_batches: BatchFraction,
    expected: Outcome,
}

fn one_batch_outcome() -> Outcome {
    Outcome {
        acquired: vec![OffsetRun::new(0, 1)],
        records: vec![0, 1],
        remainder: vec![OffsetRun::new(2, 7)],
    }
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "all-batches-fit",
            max_records: 500,
            max_bytes_in_batches: BatchFraction {
                numerator: 4,
                denominator: 1,
            },
            expected: Outcome {
                acquired: vec![OffsetRun::new(0, 7)],
                records: (0..=7).collect(),
                remainder: Vec::new(),
            },
        },
        Case {
            name: "one-batch-fits",
            max_records: 500,
            max_bytes_in_batches: BatchFraction {
                numerator: 1,
                denominator: 1,
            },
            expected: one_batch_outcome(),
        },
        Case {
            name: "less-than-one-batch",
            max_records: 500,
            max_bytes_in_batches: BatchFraction {
                numerator: 1,
                denominator: 2,
            },
            expected: one_batch_outcome(),
        },
        Case {
            // `batch_optimized`: MaxRecords is a soft limit that rounds up to
            // the end of the log batch holding the third record.
            name: "records-limit-below-bytes-limit",
            max_records: 3,
            max_bytes_in_batches: BatchFraction {
                numerator: 4,
                denominator: 1,
            },
            expected: Outcome {
                acquired: vec![OffsetRun::new(0, 3)],
                records: (0..=3).collect(),
                remainder: vec![OffsetRun::new(4, 7)],
            },
        },
    ]
}

#[tokio::test]
async fn every_acquired_offset_has_its_record_in_the_response() {
    let StartedBroker { broker, dir: _dir } = start().await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let topic = format!("byte-limit-{}", case.name);
        let group = format!("group-{}", case.name);
        let topic_id = create_topic(&broker, &topic).await;
        initialize_share_state(&broker, &group, uuid::Uuid::from_bytes(topic_id.0), 0).await;
        // Open both sessions on the empty log, so the share partition starts
        // at offset 0 under the default `latest` reset.
        for member in ["limited", "unlimited"] {
            let opened = share_fetch()
                .broker(&broker)
                .group(&group)
                .member(member)
                .topic_id(topic_id.0)
                .call()
                .await;
            assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
        }
        produce_batches(&broker, &topic).await;
        let size = batch_size(&broker, &topic);
        let BatchFraction {
            numerator,
            denominator,
        } = case.max_bytes_in_batches;

        let limited = share_fetch()
            .broker(&broker)
            .group(&group)
            .member("limited")
            .topic_id(topic_id.0)
            .epoch(1)
            .max_records(case.max_records)
            .max_bytes(size * numerator / denominator)
            .call()
            .await;
        let rest = share_fetch()
            .broker(&broker)
            .group(&group)
            .member("unlimited")
            .topic_id(topic_id.0)
            .epoch(1)
            .call()
            .await;

        actual.push((case.name, Outcome {
            acquired: acquired(&partition(&limited)),
            records: record_offsets(&partition(&limited)),
            remainder: acquired(&partition(&rest)),
        }));
        expected.push((case.name, case.expected));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The broker cap applies to `ShareFetch`'s whole-response budget, and the
/// first complete batch can exceed it so the member still makes progress.
#[tokio::test]
async fn broker_fetch_cap_limits_share_fetch_and_keeps_first_batch_progress() {
    let StartedBroker { broker, dir: _dir } = start_with_fetch_cap(1).await;
    let topic = "broker-fetch-cap";
    let group = "broker-fetch-cap-group";
    let topic_id = create_topic(&broker, topic).await;
    initialize_share_state(&broker, group, uuid::Uuid::from_bytes(topic_id.0), 0).await;
    let opened = share_fetch()
        .broker(&broker)
        .group(group)
        .member("member")
        .topic_id(topic_id.0)
        .call()
        .await;
    assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    produce_batches(&broker, topic).await;

    let response = share_fetch()
        .broker(&broker)
        .group(group)
        .member("member")
        .topic_id(topic_id.0)
        .epoch(1)
        .call()
        .await;
    let row = partition(&response);
    assert!(acquired(&row) == vec![OffsetRun::new(0, 1)]);
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
    let StartedBroker { broker, dir: _dir } = start_with_delivery_attempts(1).await;
    let topic_id = create_topic(&broker, "delivery-limit").await;
    initialize_share_state(&broker, "g", uuid::Uuid::from_bytes(topic_id.0), 0).await;
    let opened = share_fetch()
        .broker(&broker)
        .group("g")
        .member("m")
        .topic_id(topic_id.0)
        .call()
        .await;
    assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    produce_batches(&broker, "delivery-limit").await;
    let first = share_fetch()
        .broker(&broker)
        .group("g")
        .member("m")
        .topic_id(topic_id.0)
        .epoch(1)
        .call()
        .await;
    produce_batches(&broker, "delivery-limit").await;

    // Release every record at its only delivery attempt, and fetch.
    let after_release = share_fetch()
        .broker(&broker)
        .group("g")
        .member("m")
        .topic_id(topic_id.0)
        .epoch(2)
        .acknowledgements(vec![FetchAcknowledgement {
            first_offset: 0,
            last_offset: 7,
            ack_type: RELEASE,
        }])
        .call()
        .await;

    assert!(
        (
            acquired(&partition(&first)),
            acquired(&partition(&after_release))
        ) == (vec![OffsetRun::new(0, 7)], vec![OffsetRun::new(8, 15)])
    );
    broker.shutdown().await;
}

#[derive(derive_more::From, derive_more::Into)]
/// A `ShareFetch` of every partition of `topic_id`, with no wait.
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

#[bon::builder]
fn fetch_partitions(
    #[builder(default = "g")] group: &'_ str,
    #[builder(into, default = ShareSessionEpoch (0))] epoch: ShareSessionEpoch,
    #[builder(default = WireUuid :: ZERO)] topic_id: WireUuid,
    #[builder(into, default)] partitions: TopicPartitionCount,
    #[builder(into, default = ShareResponseByteLimit (1 << 20))] maximum: ShareResponseByteLimit,
) -> ShareFetchRequest {
    let setup = FetchPartitionsSetup {
        group,
        epoch,
        topic_id,
        partitions,
        maximum,
    };
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
    let StartedBroker { broker, dir: _dir } = start().await;

    // `(name, MaxBytes as (numerator, denominator) of one batch, expected)`.
    let cases = [
        ("half-a-batch", (1, 2), Spread {
            rows_with_records: 1,
            batches: 1,
        }),
        ("half-a-batch-each", (3, 2), Spread {
            rows_with_records: 1,
            batches: 1,
        }),
        ("a-batch-each", (3, 1), Spread {
            rows_with_records: 3,
            batches: 3,
        }),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, (numerator, denominator), want) in cases {
        let topic = format!("spread-{name}");
        let group = format!("group-{name}");
        let topic_id = create_topic_with_partitions(&broker, &topic, PARTITIONS).await;
        for partition in 0..PARTITIONS {
            initialize_share_state(
                &broker,
                &group,
                uuid::Uuid::from_bytes(topic_id.0),
                partition,
            )
            .await;
        }
        let opened = send_share_fetch(
            &broker,
            &fetch_partitions()
                .group(&group)
                .topic_id(topic_id)
                .partitions(PARTITIONS)
                .call(),
        )
        .await;
        assert!(spread(&opened).rows_with_records == 0, "{opened:?}");
        for partition in 0..PARTITIONS {
            produce_batch(&broker, &topic, partition).await;
        }
        let size = batch_size(&broker, &topic);

        let limited = send_share_fetch(
            &broker,
            &fetch_partitions()
                .group(&group)
                .epoch(1)
                .topic_id(topic_id)
                .partitions(PARTITIONS)
                .maximum(size * numerator / denominator)
                .call(),
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
    OffsetRun {
        first_offset,
        last_offset,
    }: OffsetRun,
) {
    let version = krabka_protocol::owned::share_acknowledge_request::MAX_VERSION;
    let request = acknowledge_batches_request()
        .group(
            AcknowledgementSetup::for_topic_session(group, ShareSessionEpoch(epoch), topic_id)
                .group,
        )
        .member(member)
        .epoch(
            AcknowledgementSetup::for_topic_session(group, ShareSessionEpoch(epoch), topic_id)
                .epoch,
        )
        .topic_id(
            AcknowledgementSetup::for_topic_session(group, ShareSessionEpoch(epoch), topic_id)
                .topic_id,
        )
        .partition(AcknowledgementPartitionSetup::single_batch(
            krabka_log::Offset(first_offset)..=krabka_log::Offset(last_offset),
            AcknowledgementCode(RELEASE),
        ))
        .mode(
            AcknowledgementSetup::for_topic_session(group, ShareSessionEpoch(epoch), topic_id).mode,
        )
        .call();
    let response = share_acknowledge_wire()
        .broker(broker)
        .request(&request)
        .version(version)
        .call()
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
    let StartedBroker { broker, dir: _dir } = start().await;
    let topic_id = create_topic(&broker, "two-passes").await;
    initialize_share_state(&broker, "g", uuid::Uuid::from_bytes(topic_id.0), 0).await;
    for member in ["waiting", "other"] {
        let opened = share_fetch()
            .broker(&broker)
            .group("g")
            .member(member)
            .topic_id(topic_id.0)
            .call()
            .await;
        assert!(partition(&opened).error_code == codes::NONE, "{opened:?}");
    }
    // One batch at offsets 0-1. `other` holds it, then releases offset 0.
    produce_batch(&broker, "two-passes", 0).await;
    let held = share_fetch()
        .broker(&broker)
        .group("g")
        .member("other")
        .topic_id(topic_id.0)
        .epoch(1)
        .call()
        .await;
    assert!(acquired(&partition(&held)) == vec![OffsetRun::new(0, 1)]);
    release(&broker, "g", "other", 2, topic_id, OffsetRun::new(0, 0)).await;

    // The first pass acquires offset 0 and falls short of `MinBytes`. While
    // the request waits, `other` releases offset 1, which the last pass
    // acquires.
    let mut request = fetch_partitions().epoch(1).topic_id(topic_id).call();
    request.member_id = Some("waiting".into());
    request.min_bytes = 1 << 20;
    request.max_wait_ms = 1_000;
    let (response, ()) = tokio::join!(send_share_fetch(&broker, &request), async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        release(&broker, "g", "other", 3, topic_id, OffsetRun::new(1, 1)).await;
    });

    let row = partition(&response);
    assert!(
        (acquired(&row), record_offsets(&row))
            == (vec![OffsetRun::new(0, 0), OffsetRun::new(1, 1)], vec![0, 1])
    );
    broker.shutdown().await;
}
