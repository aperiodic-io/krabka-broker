//! Handler tests for the high watermark that a follower `Fetch` reports.
//!
//! Kafka's `Partition.fetchRecords` reads a follower's records first, and only
//! then records the follower's fetch offset (`updateFollowerFetchState`). When
//! that first read answers the fetch, the row carries the high watermark from
//! before the follower's own fetch offset counted toward it. When the fetch
//! parks, `DelayedFetch.onComplete` reads every partition again, on a wake or
//! on expiry, and the row carries the high watermark that the fetch moved.
//!
//! So the follower learns a high watermark one fetch after the leader.
//! `kafka-replica-verification.sh` reads the high watermark of every replica
//! and reports the gap between them as replica lag. Its system test expects a
//! nonzero lag while records arrive.
//!
//! The broker under test is node 1 and leads every partition. Node 2 follows
//! and never fetches on its own, so only the fetches of a case move the high
//! watermark. Each case appends two one-record batches, at offsets 0 and 1.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_log::Offset;
use krabka_protocol::{
    Decode,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    },
    records::{RecordBatch, RecordsPayload},
};
use krabka_units::prelude::mebibytes;

use super::{encode_fetch_response, handle};
use crate::{
    broker::BrokerHandle,
    codes,
    fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
    partition::Partition,
    test_support::{encode_request, peer, principal, start_broker_no_audit_with},
};

/// The node id of the follower.
const FOLLOWER: u64 = 2;

/// The log end offset after the two batches each case appends.
const LOG_END: i64 = 2;

/// The last `Fetch` version that names a topic and carries `ReplicaId` as a
/// field of its own, which keeps the rows comparable by topic name.
const VERSION: i16 = 12;

/// How the leader answers the follower fetch of a case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// The fetch asks not to wait, so the first read answers it.
    FirstRead,
    /// The fetch asks to wait for one byte, but the first read already finds
    /// a batch, so it answers the fetch.
    FirstReadWithRecords,
    /// The fetch waits for one byte, nothing arrives, and the wait expires.
    Expiry,
    /// The fetch waits for one byte, and a third batch wakes it.
    Wake,
}

#[derive(Debug, Clone, Copy)]
struct Case {
    answer: Answer,
    /// The offset that the follower fetches from, which is also the offset
    /// that the leader records for it.
    fetch_offset: i64,
    max_wait_ms: i32,
    /// The high watermark that the row reports.
    reported: i64,
    /// The partition's high watermark after the fetch.
    recorded: i64,
}

/// The observable result of one case.
#[derive(Debug, PartialEq)]
struct Outcome {
    case: String,
    response: FetchResponse,
    high_watermark: Offset,
}

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| {
        // Node 2 never fetches on its own. Keep it in the ISR for the whole
        // test, so only the fetches of a case move the high watermark.
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await
}

/// A batch of one record with `value`.
use crate::test_support::single_value_batch as batch;

/// Create `topic` with replicas 1 and 2, led by this broker, wait until node
/// 2 is in the ISR, and append two batches.
async fn partition(broker: &BrokerHandle, topic: &str, topic_id: u128) -> Arc<Partition> {
    crate::handlers::test_support::seed_partition_replicas(
        broker,
        crate::handlers::test_support::ReplicatedTopicSetup {
            topic,
            topic_id: uuid::Uuid::from_u128(topic_id),
            ..Default::default()
        },
    )
    .await;

    wait_for_local_partition!(
        (shared, partition),
        broker,
        topic,
        partition,
        partition
            .replica_state
            .lock()
            .await
            .isr
            .contains(&krabka_raft::NodeId(FOLLOWER)),
        "the broker leads the partition with node 2 in the ISR"
    );

    for value in [&b"first"[..], &b"second"[..]] {
        partition
            .produce_batch(batch(value))
            .await
            .expect("append a batch");
    }
    partition
}

/// The batch that the log stores at `offset`.
fn stored_batch(partition: &Partition, offset: i64) -> RecordBatch {
    let read = partition
        .log
        .lock()
        .expect("partition log lock")
        .read(Offset(offset), mebibytes(1))
        .expect("read the log");
    read.batches
        .into_iter()
        .next()
        .expect("a batch at the offset")
}

/// A sessionless one-row follower fetch of partition 0 of `topic`.
fn request(topic: &str, case: Case) -> FetchRequest {
    FetchRequest {
        replica_id: i32::try_from(FOLLOWER).expect("small node id"),
        max_wait_ms: case.max_wait_ms,
        min_bytes: 1,
        max_bytes: 1_048_576,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: case.fetch_offset,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn fetch(broker: &BrokerHandle, request: &FetchRequest) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("replicator"),
        client_id = "fetch-follower"
    );
    let request_bytes = encode_request(request, VERSION);
    let (response, response_version) = handle(&shared, VERSION, 7, &request_bytes, &ctx)
        .await
        .expect("handle fetch");
    let wire = encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = wire.as_ref();
    let decoded = FetchResponse::decode(&mut cursor, VERSION).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}

/// Append a third batch after the leader records the follower at the log
/// end. The leader records it after the first read of the fetch, so the
/// batch arrives while the fetch parks or just before it parks.
async fn append_after_the_position(partition: Arc<Partition>) {
    while partition.high_watermark().await < Offset(LOG_END) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    partition
        .produce_batch(batch(b"third"))
        .await
        .expect("append the third batch");
}

fn expected(case: Case, label: String, topic: &str, records: RecordsPayload) -> Outcome {
    Outcome {
        case: label,
        response: FetchResponse {
            error_code: codes::NONE,
            session_id: INVALID_SESSION_ID,
            responses: vec![FetchableTopicResponse {
                topic: topic.to_owned(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: codes::NONE,
                    high_watermark: case.reported,
                    last_stable_offset: case.reported,
                    log_start_offset: 0,
                    aborted_transactions: None,
                    preferred_read_replica: -1,
                    records: Some(records),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        high_watermark: Offset(case.recorded),
    }
}

#[tokio::test]
async fn a_follower_learns_the_high_watermark_it_moved_one_fetch_later() {
    let cases = [
        // Caught up, and answered at once: the row still reports 0.
        Case {
            answer: Answer::FirstRead,
            fetch_offset: LOG_END,
            max_wait_ms: 0,
            reported: 0,
            recorded: LOG_END,
        },
        // The batch at offset 1 is already there: the row carries it and
        // reports the high watermark from before the fetch offset 1 counted.
        Case {
            answer: Answer::FirstReadWithRecords,
            fetch_offset: 1,
            max_wait_ms: 10_000,
            reported: 0,
            recorded: 1,
        },
        // Caught up, parked, and expired: the read on expiry reports the high
        // watermark that the fetch moved.
        Case {
            answer: Answer::Expiry,
            fetch_offset: LOG_END,
            max_wait_ms: 100,
            reported: LOG_END,
            recorded: LOG_END,
        },
        // Caught up, parked, and woken by the third batch: the read on the
        // wake carries that batch and the high watermark that the fetch moved.
        Case {
            answer: Answer::Wake,
            fetch_offset: LOG_END,
            max_wait_ms: 10_000,
            reported: LOG_END,
            recorded: LOG_END,
        },
    ];

    let (broker, _dir) = start().await;
    topic_case_outcomes!(
        (actual, want),
        (index, case, label, name),
        "follower-watermark",
        cases,
        {
            let partition = partition(&broker, &name, index + 1).await;

            let appender = (case.answer == Answer::Wake)
                .then(|| tokio::spawn(append_after_the_position(Arc::clone(&partition))));
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                fetch(&broker, &request(&name, case)),
            )
            .await
            .expect("the fetch is answered");
            if let Some(appender) = appender {
                appender.await.expect("append task");
            }

            let records = match case.answer {
                Answer::FirstRead | Answer::Expiry => RecordsPayload::Legacy(Bytes::new()),
                Answer::FirstReadWithRecords => {
                    RecordsPayload::V2(vec![stored_batch(&partition, 1)])
                }
                Answer::Wake => RecordsPayload::V2(vec![stored_batch(&partition, LOG_END)]),
            };
            actual.push(Outcome {
                case: label.clone(),
                response,
                high_watermark: partition.high_watermark().await,
            });
            want.push(expected(case, label, &name, records));
        }
    );
    broker.shutdown().await;

    assert!(actual == want);
}
