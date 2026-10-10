//! The produce and fetch traffic, over real connections.
//!
//! Both directions go through a `krabka_client_core::Client` on purpose. A
//! diskless `acks=all` produce is only durable because the high watermark
//! waits on the WAL quorum, and that gate lives in the Produce handler; a
//! cold read is only served because the Fetch handler falls back to the
//! object store on `OFFSET_OUT_OF_RANGE`. Driving either through an in-process
//! shortcut would skip the code the suite exists to cover.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest},
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::{
    CLIENT_PRINCIPAL, PASSWORD, TOPIC, support,
    support::{
        fetch::{fetch_partition, single_partition_fetch},
        offsets::{
            delete_records_partition, delete_records_request, delete_records_topic,
            list_offset_partition, single_partition_list_offsets,
        },
        records::batch_from_records,
    },
};

/// Kafka `NOT_LEADER_OR_FOLLOWER`. The Produce handler returns it before it
/// appends anything, so a retry cannot duplicate a record.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Kafka `UNKNOWN_TOPIC_OR_PARTITION`. Also pre-append.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Kafka `OFFSET_OUT_OF_RANGE`.
pub(crate) const OFFSET_OUT_OF_RANGE: i16 = 1;
/// Request timestamp sentinel (-2): resolve the earliest available offset.
/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;

/// A log read back over the wire.
pub(crate) struct FetchedLog {
    /// `(offset, value)` for every record, in offset order.
    pub(crate) records: Vec<(i64, Bytes)>,
    /// The re-encoded record batches, concatenated in offset order. Two reads
    /// of the same offsets produce identical bytes here only if the broker
    /// serving them holds the identical batches, which is the byte-exactness
    /// the failover case asserts.
    pub(crate) bytes: Bytes,
}

/// One record carrying `value`, stamped with the wall clock the way a real
/// producer stamps a batch.
///
/// The timestamp is not incidental. `retention.ms` on a diskless topic reads a
/// batch's `max_timestamp` off the WAL index, and this suite's topic runs the
/// default seven-day window, so a batch left at the epoch would be expired out
/// of the object store before any case could read it back.
fn stamped_batch(value: Bytes) -> RecordBatch {
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the test clock is after the epoch")
            .as_millis(),
    )
    .expect("the test clock fits an i64 of milliseconds");
    RecordBatch {
        base_timestamp: now_ms,
        max_timestamp: now_ms,
        ..batch_from_records(vec![Record {
            value: Some(value),
            ..Record::default()
        }])
    }
}

/// The value this suite produces at `index`.
pub(crate) fn value_at(index: usize) -> Bytes {
    Bytes::from(format!("diskless-e2e-record-{index:04}"))
}

/// Produce `values` one record at a time with `acks=all`, and require every
/// one of them to be acknowledged.
///
/// One record per request is deliberate: each request is a separate WAL
/// quorum round, so `count` acknowledgements mean `count` committed quorum
/// rounds rather than one.
///
/// A `REQUEST_TIMED_OUT` is **not** retried. It means the append landed
/// but the quorum never committed it, which is the failure this
/// suite exists to catch, so it fails the test instead of being papered over.
pub(crate) async fn produce_all(client: &Client, topic_id: WireUuid, values: &[Bytes]) {
    for (index, value) in values.iter().enumerate() {
        produce_one(client, topic_id, value.clone(), index).await;
    }
}

async fn produce_one(client: &Client, topic_id: WireUuid, value: Bytes, index: usize) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let batch = stamped_batch(value.clone());
        let response = crate::support::produce::send_batch(
            &client,
            batch,
            crate::support::produce::SinglePartitionProduceSetup {topic: (TOPIC).into(), topic_id, ..crate::support::produce::SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()},
        )
        .await;
        let partition = &response.responses[0].partition_responses[0];
        match partition.error_code {
            0 => return,
            NOT_LEADER_OR_FOLLOWER | UNKNOWN_TOPIC_OR_PARTITION => {
                assert!(
                    Instant::now() <= deadline,
                    "record {index} never found a diskless leader: {response:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            error_code => panic!(
                "acks=all produce of record {index} failed with error_code {error_code}: \
                 {response:?}"
            ),
        }
    }
}

/// The same diskless read shape is used for data and for a raw offset error.
fn data_fetch(
    topic_id: WireUuid,
    offset: krabka_ids::Offset,
) -> krabka_protocol::owned::fetch_request::FetchRequest {
    single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
        topic: TOPIC.into(),
        topic_id,
        partition: fetch_partition(crate::support::fetch::FetchPartitionSetup {
            offset,
            maximum: crate::support::fetch::FetchByteLimit(4 * 1024 * 1024),
            ..Default::default()
        }),
        limits: crate::support::fetch::FetchLimits::wait_for_data(
            crate::support::fetch::RequestWaitMillis(500),
        ),
    })
}

/// Read `expected` records back from `bootstrap`, starting at `start_offset`.
///
/// The loop retries until the deadline because a just-promoted broker settles
/// its metadata asynchronously, and because a cold read returns one covering
/// batch per round trip rather than the whole range at once.
pub(crate) async fn fetch_log(
    bootstrap: &str,
    topic_id: WireUuid,
    start_offset: i64,
    expected: usize,
    timeout: Duration,
) -> FetchedLog {
    let client = support::sasl_client(bootstrap, CLIENT_PRINCIPAL, PASSWORD).await;
    let deadline = Instant::now() + timeout;
    let mut records: Vec<(i64, Bytes)> = Vec::with_capacity(expected);
    let mut bytes = BytesMut::new();
    let mut next = start_offset;

    while records.len() < expected {
        let response = client
            .send(data_fetch(topic_id, krabka_ids::Offset(next)))
            .await
            .expect("Fetch");
        let partition = response
            .responses
            .first()
            .and_then(|topic| topic.partitions.first());

        if let Some(partition) = partition {
            assert!(
                partition.error_code != OFFSET_OUT_OF_RANGE,
                "offset {next} is below the log start ({}) and the object store did not \
                 serve it either: {partition:?}",
                partition.log_start_offset
            );
            if let Some(batches) = partition.records.as_ref().and_then(|r| r.as_v2()) {
                for batch in batches {
                    if records.len() >= expected {
                        break;
                    }
                    // A batch whose base offset is already consumed adds
                    // nothing: the cold-read path answers with the batch that
                    // *covers* the requested offset, which can start earlier.
                    if batch.base_offset < next {
                        continue;
                    }
                    batch.encode(&mut bytes).expect("re-encode fetched batch");
                    for record in &batch.records {
                        let offset = batch.base_offset + i64::from(record.offset_delta);
                        records.push((offset, record.value.clone().unwrap_or_default()));
                        next = offset + 1;
                    }
                }
            }
        }

        if records.len() >= expected {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "{bootstrap} served {}/{expected} records from offset {start_offset} before the \
             deadline (stalled at offset {next})",
            records.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Whole batches go into `bytes`, so a batch that straddled `expected`
    // would leave the byte comparison covering more records than the record
    // comparison. This suite produces one record per batch, so it never does;
    // fail loudly rather than compare two different ranges if that changes.
    assert!(
        records.len() == expected,
        "a batch straddled the requested record count: got {} records for {expected}",
        records.len()
    );

    FetchedLog {
        records,
        bytes: bytes.freeze(),
    }
}

/// Produce into the topic until `stop` is cancelled, ignoring every error.
///
/// The [`crate::restart`] case uses this to keep the leader's flusher busy
/// while it crashes the broker underneath it. Which of these records were
/// acknowledged is decided by exactly when the crash landed, so nothing is
/// asserted about them -- they exist to make the crash land mid-flush.
///
/// Each request is a full `acks=all` quorum round trip, so the loop already
/// self-throttles on the network; the short sleep only keeps it from
/// monopolising a worker thread on a small runtime.
pub(crate) async fn produce_until_stopped(
    client: Client,
    topic_id: WireUuid,
    stop: tokio_util::sync::CancellationToken,
) {
    let mut index = 0usize;
    while !stop.is_cancelled() {
        let batch = stamped_batch(Bytes::from(format!("diskless-e2e-churn-{index:04}")));
        let _ = client
            .send(crate::support::produce::batch_request(
                batch,
                crate::support::produce::SinglePartitionProduceSetup {
                    topic: (TOPIC).into(),
                    topic_id,
                    ..crate::support::produce::SinglePartitionProduceSetup::replicated()
                },
            ))
            .await;
        index += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Assert that a read-back log carries exactly the values this suite produced,
/// at consecutive offsets from `start_offset`.
pub(crate) fn assert_matches_produced(log: &FetchedLog, start_offset: i64, expected: usize) {
    let want: Vec<(i64, Bytes)> = (0..expected)
        .map(|index| {
            (
                start_offset + i64::try_from(index).expect("small count"),
                value_at(index),
            )
        })
        .collect();
    assert!(log.records == want);
}

/// Delete every record below `offset`, and return the low watermark the broker
/// answers with.
///
/// The request goes to `bootstrap` directly rather than through a leader
/// lookup: only the leader trims, so a case that means to delete records sends
/// this to the broker it already resolved as the leader. `DeleteRecords`
/// carries no topic id at any version, so this names the topic and there is
/// nothing to pass.
pub(crate) async fn delete_records_below(bootstrap: &str, offset: i64) -> i64 {
    let client = support::sasl_client(bootstrap, CLIENT_PRINCIPAL, PASSWORD).await;
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(
                TOPIC,
                vec![delete_records_partition(0, offset)],
            )],
            10_000,
        ))
        .await
        .expect("DeleteRecords");
    let partition = &response.topics[0].partitions[0];
    assert!(
        partition.error_code == 0,
        "DeleteRecords below offset {offset} failed: {response:?}"
    );
    partition.low_watermark
}

/// The offset `ListOffsets(EARLIEST)` answers for the suite's partition.
pub(crate) async fn earliest_offset(bootstrap: &str) -> i64 {
    let client = support::sasl_client(bootstrap, CLIENT_PRINCIPAL, PASSWORD).await;
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(
                TOPIC,
                ListOffsetsPartition {
                    current_leader_epoch: -1,
                    ..list_offset_partition(0, EARLIEST_TIMESTAMP)
                },
            )
        })
        .await
        .expect("ListOffsets");
    let partition = &response.topics[0].partitions[0];
    assert!(
        partition.error_code == 0,
        "ListOffsets(EARLIEST) failed: {response:?}"
    );
    partition.offset
}

/// The error code one `Fetch` of `offset` answers with, without retrying.
///
/// [`fetch_log`] refuses `OFFSET_OUT_OF_RANGE`, which is exactly the answer a
/// case about a deleted offset is asserting, so that case reads the raw code
/// through here instead.
pub(crate) async fn fetch_error_code(bootstrap: &str, topic_id: WireUuid, offset: i64) -> i16 {
    let client = support::sasl_client(bootstrap, CLIENT_PRINCIPAL, PASSWORD).await;
    let response = client
        .send(data_fetch(topic_id, krabka_ids::Offset(offset)))
        .await
        .expect("Fetch");
    response.responses[0].partitions[0].error_code
}
