//! A promotion copies the producer state of the log into the produce-path
//! tracker.
//!
//! A Kafka follower updates its producer state for each batch it replicates
//! (`UnifiedLog.appendAsFollower`). A new leader thus answers a retry of a
//! replicated batch as a duplicate (`ProducerStateEntry.findDuplicateBatch`),
//! and it takes the next sequence after that batch. A follower's tracker does
//! not see replicated data batches, so `Partition::install_local_leadership`
//! copies them from the log. Whatever the tracker held before the promotion,
//! the new leader decides from the producer state at its log end.

use {
    crate::{
        partition::{Partition, test_support::test_partition},
        producer_state::{
            Checked, Decision, NO_EARLIER_BATCHES, ProducerEntry, ProducerState, RetainedBatch,
            SequenceContext,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_log::Offset,
    krabka_protocol::records::{Record, RecordBatch},
    std::sync::Arc,
    tokio::sync::Notify,
};

const TOPIC: &str = "t";
const PARTITION: krabka_ids::PartitionIndex = krabka_ids::PartitionIndex(0);
const PRODUCER_ID: i64 = 42;
const TIMESTAMP: i64 = 1_700_000_000_000;

/// Store the three-record batch of `producer_id` at `base_sequence` at the log
/// end, as a follower stores a batch it replicates.
fn replicate(partition: &Partition, producer_id: i64, base_sequence: i32) {
    let records = (0..3)
        .map(|delta| Record {
            offset_delta: delta,
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        })
        .collect();
    let mut batch = RecordBatch {
        last_offset_delta: 2,
        base_timestamp: TIMESTAMP,
        max_timestamp: TIMESTAMP,
        producer_id,
        producer_epoch: 0,
        base_sequence,
        records,
        ..RecordBatch::default()
    };
    let mut log = partition.log.lock().expect("log mutex");
    let log_end = log.log_end_offset();
    log.append_at(&mut batch, log_end).expect("append");
}

/// The three-record batch of [`PRODUCER_ID`] at `base_sequence` that the log
/// holds at `base_offset`.
fn retained(base_sequence: i32, base_offset: i64) -> RetainedBatch {
    RetainedBatch {
        base_sequence,
        last_sequence: base_sequence + 2,
        base_offset,
        last_offset: base_offset + 2,
        timestamp: TIMESTAMP,
    }
}

/// The tracker's answer to the batch of [`PRODUCER_ID`] at `base_sequence`.
async fn decide(tracker: &ProducerState, base_sequence: i32) -> Checked {
    tracker
        .check_batch(
            TOPIC,
            PARTITION,
            SequenceContext::RELEASED,
            (PRODUCER_ID, 0),
            (base_sequence, 2),
        )
        .await
}

/// A three-record batch that the tracker holds before the promotion.
#[derive(Clone, Copy)]
struct Tracked {
    producer_id: i64,
    base_sequence: i32,
    base_offset: i64,
}

#[tokio::test]
async fn a_promotion_decides_from_the_producer_state_of_the_log() {
    let cases = [
        ("a follower that never led", None),
        (
            "a broker that led an earlier term",
            Some(Tracked {
                producer_id: PRODUCER_ID,
                base_sequence: 0,
                base_offset: 0,
            }),
        ),
        (
            "an entry of a batch that was cut from the log",
            Some(Tracked {
                producer_id: 77,
                base_sequence: 0,
                base_offset: 9,
            }),
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, tracked) in cases {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        partition.install_replication_target(None, 2, 4).await;
        replicate(&partition, PRODUCER_ID, 0);
        replicate(&partition, PRODUCER_ID, 3);
        let tracker = ProducerState::new();
        if let Some(tracked) = tracked {
            tracker
                .commit(
                    TOPIC,
                    PARTITION,
                    (tracked.producer_id, 0),
                    (tracked.base_sequence, 2),
                    (tracked.base_offset, TIMESTAMP, false),
                )
                .await;
        }

        partition
            .install_local_leadership(&tracker, None, 1, 5)
            .await
            .expect("promote");

        actual.push((
            name,
            tracker.snapshot(TOPIC, PARTITION).await,
            [
                decide(&tracker, 3).await,
                decide(&tracker, 0).await,
                decide(&tracker, 6).await,
            ],
            partition.log_end_offset(),
        ));
        let mut earlier = NO_EARLIER_BATCHES;
        earlier[0] = Some(retained(0, 0));
        expected.push((
            name,
            vec![(PRODUCER_ID, ProducerEntry {
                epoch: 0,
                last_sequence: 5,
                last_offset: 5,
                base_offset: 3,
                last_timestamp: TIMESTAMP,
                entry_timestamp: TIMESTAMP,
                current_txn_first_offset: None,
                earlier,
            })],
            [
                Checked {
                    decision: Decision::Duplicate { base_offset: 3 },
                    duplicate: Some(retained(3, 3)),
                },
                Checked {
                    decision: Decision::Duplicate { base_offset: 0 },
                    duplicate: Some(retained(0, 0)),
                },
                Checked {
                    decision: Decision::Append,
                    duplicate: None,
                },
            ],
            Offset(6),
        ));
    }
    assert!(actual == expected);
}
