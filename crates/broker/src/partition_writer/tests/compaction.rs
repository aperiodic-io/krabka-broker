//! Writer-loop tests for the Compact arm: the batches that a compaction pass
//! keeps for the producers of a partition, on a leader and on a follower.
//!
//! Kafka's cleaner reads the last record of each active producer from the
//! producer state of the log: `Cleaner.cleanSegments` calls
//! `UnifiedLog.lastRecordsOfActiveProducers`. A leader updates that state for
//! each batch that it appends, and a follower for each batch that it
//! replicates, so the two replicas keep the same batches.

use assert2::check;
use bytes::{Bytes, BytesMut};
use krabka_log::{CleanupPolicy, LogConfig, Offset, ProducerId, VerbatimBatch};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::{bytes, convert::TimeExt as _, gibibytes, millis};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    replica_state::ReplicaState,
    test_support::UnixMillis,
    txn::marker::{MarkerType, build_marker_batch},
};

/// The idempotent producer: `(id, epoch)`.
const IDEMPOTENT: ProducerIdentity = ProducerIdentity {
    id: ProducerId(9_101),
    epoch: ProducerEpoch(0),
};

/// The transactional producer. Its commit marker is at the next epoch, as a
/// transaction-version-2 coordinator writes it.
const TRANSACTIONAL: ProducerIdentity = ProducerIdentity {
    id: ProducerId(9_102),
    epoch: ProducerEpoch(3),
};

/// Compaction passes for each row. The third pass is the first that can drop
/// a commit marker: the first pass still meets a batch of its transaction, the
/// second stamps its delete horizon, and the third finds that horizon passed.
const PASSES: usize = 3;

/// How the batches of a row reach the partition log.
#[derive(Clone, Copy, Debug)]
enum Role {
    /// A leader. The writer appends each batch at the next offset, and the
    /// row then records a data batch in the tracker, as the produce handler
    /// does after the append (`AppendCommit::record`).
    Leader,
    /// A follower. A data batch arrives verbatim and a control batch decoded,
    /// at the offset of the leader, as `replicator/response.rs` sends them.
    Follower,
}

krabka_macros::compacted_batch!(Kept);

#[derive(Clone, Copy)]
struct ProducerEpoch(i16);

#[derive(Clone, Copy, Default)]
struct ProducerSequence(i32);

#[derive(Clone, Copy)]
struct ProducerIdentity {
    id: ProducerId,
    epoch: ProducerEpoch,
}

#[derive(Clone, Copy)]
struct ProducerBatchIdentity {
    identity: ProducerIdentity,
    sequence: ProducerSequence,
}

#[derive(Clone, Copy, Default)]
enum BatchTransaction {
    #[default]
    Ordinary,
    Transactional,
}

#[derive(Clone, Copy, Default)]
enum ProducerExpiry {
    #[default]
    Active,
    Swept,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RecordSetup<'a> {
    producer: Option<ProducerBatchIdentity>,
    transaction: BatchTransaction,
    #[default(("k", "v"))]
    payload: (&'a str, &'a str),
    timestamp: UnixMillis,
}

/// A one-record data batch, ordinary and anonymous by default.
fn record(setup: RecordSetup<'_>) -> RecordBatch {
    let RecordSetup {
        producer,
        transaction,
        payload: (key, value),
        timestamp,
    } = setup;
    let (producer_id, producer_epoch, base_sequence) = producer.map_or((-1, -1, -1), |producer| {
        (
            producer.identity.id.0,
            producer.identity.epoch.0,
            producer.sequence.0,
        )
    });
    RecordBatch {
        attributes: Attributes::default()
            .with_transactional(matches!(transaction, BatchTransaction::Transactional)),
        base_timestamp: timestamp.0,
        max_timestamp: timestamp.0,
        producer_id,
        producer_epoch,
        base_sequence,
        records: vec![Record {
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// One history of a partition: its batches in log order, whether the
/// producer-expiry sweep runs before compaction, and the batches that every
/// replica keeps.
struct History {
    name: &'static str,
    batches: Vec<RecordBatch>,
    expiry: ProducerExpiry,
    want: Vec<Kept>,
}

/// Each history ends with a batch that stays in the active segment, which no
/// pass rewrites.
fn histories(now_ms: UnixMillis) -> Vec<History> {
    let idempotent = |sequence| {
        Some(ProducerBatchIdentity {
            identity: IDEMPOTENT,
            sequence,
        })
    };
    let superseded = vec![
        record(RecordSetup {
            producer: idempotent(ProducerSequence(0)),
            payload: ("a", "p-a"),
            timestamp: now_ms,
            ..Default::default()
        }),
        record(RecordSetup {
            producer: idempotent(ProducerSequence(1)),
            payload: ("b", "p-b"),
            timestamp: now_ms,
            ..Default::default()
        }),
        record(RecordSetup {
            payload: ("a", "a-2"),
            timestamp: now_ms,
            ..Default::default()
        }),
        record(RecordSetup {
            payload: ("b", "b-3"),
            timestamp: now_ms,
            ..Default::default()
        }),
        record(RecordSetup {
            payload: ("c", "c-4"),
            timestamp: now_ms,
            ..Default::default()
        }),
    ];
    let committed = vec![
        record(RecordSetup {
            producer: Some(ProducerBatchIdentity {
                identity: TRANSACTIONAL,
                sequence: ProducerSequence::default(),
            }),
            transaction: BatchTransaction::Transactional,
            payload: ("a", "t-a"),
            timestamp: now_ms,
        }),
        build_marker_batch(
            TRANSACTIONAL.id,
            TRANSACTIONAL.epoch.0 + 1,
            Offset(1),
            MarkerType::Commit,
            17,
        ),
        record(RecordSetup {
            payload: ("a", "a-2"),
            timestamp: now_ms,
            ..Default::default()
        }),
        record(RecordSetup {
            payload: ("c", "c-3"),
            timestamp: now_ms,
            ..Default::default()
        }),
    ];
    vec![
        // Kafka keeps the batch that holds the last sequence of an active
        // producer, as an empty batch when compaction removes its records.
        History {
            name: "the last batch of an active idempotent producer",
            want: vec![
                Kept::header(1, &superseded[1]),
                Kept::whole(2, &superseded[2]),
                Kept::whole(3, &superseded[3]),
                Kept::whole(4, &superseded[4]),
            ],
            batches: superseded.clone(),
            expiry: ProducerExpiry::Active,
        },
        // `ProducerStateManager.removeExpiredProducers` removed the producer,
        // so nothing keeps its batches.
        History {
            name: "the last batch of an expired idempotent producer",
            want: vec![
                Kept::whole(2, &superseded[2]),
                Kept::whole(3, &superseded[3]),
                Kept::whole(4, &superseded[4]),
            ],
            batches: superseded,
            expiry: ProducerExpiry::Swept,
        },
        // A marker that moves the epoch removes the batches from the producer
        // state, so the marker is the last record of the producer. Kafka keeps
        // it, as an empty control batch once its delete horizon has passed,
        // because it holds the epoch that fences the producer.
        History {
            name: "a commit marker at the next epoch",
            want: vec![
                Kept::header(1, &committed[1]),
                Kept::whole(2, &committed[2]),
                Kept::whole(3, &committed[3]),
            ],
            batches: committed,
            expiry: ProducerExpiry::Active,
        },
    ]
}

/// Append `batch` at `offset` the way `role` does.
async fn append(
    role: Role,
    tx: &mpsc::Sender<WriterMessage>,
    producer_state: &ProducerState,
    mut batch: RecordBatch,
    offset: i64,
) {
    batch.base_offset = offset;
    let control = batch.attributes.is_control_batch();
    match role {
        Role::Leader => {
            let (ack, ack_rx) = oneshot::channel();
            let data = if control {
                ProduceData::OwnedControl(batch.clone())
            } else {
                ProduceData::Owned(batch.clone())
            };
            tx.send(WriterMessage::Produce(ProduceJob {
                data,
                ack,
                producer_check: None,
            }))
            .await
            .expect("send produce");
            let appended = ack_rx.await.expect("produce ack").expect("produce");
            check!(appended.base_offset == offset);
            if !control && batch.producer_id >= 0 {
                producer_state
                    .commit(
                        "t",
                        PartitionIndex(0),
                        (batch.producer_id, batch.producer_epoch),
                        (batch.base_sequence, batch.last_offset_delta),
                        (
                            offset,
                            batch.max_timestamp,
                            batch.attributes.is_transactional(),
                        ),
                    )
                    .await;
            }
        }
        Role::Follower if control => {
            let (ack, ack_rx) = oneshot::channel();
            tx.send(WriterMessage::Replicate { batch, ack })
                .await
                .expect("send replicate");
            ack_rx.await.expect("replicate ack").expect("replicate");
        }
        Role::Follower => {
            let mut wire = BytesMut::new();
            batch.encode(&mut wire).expect("encode");
            let verbatim = VerbatimBatch {
                bytes: wire.freeze(),
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                leader_epoch: krabka_log::LeaderEpoch(batch.partition_leader_epoch),
                producer_id: ProducerId(batch.producer_id),
                producer_epoch: batch.producer_epoch,
                base_sequence: batch.base_sequence,
                is_transactional: batch.attributes.is_transactional(),
            };
            let (ack, ack_rx) = oneshot::channel();
            tx.send(WriterMessage::ReplicateVerbatim {
                batch: verbatim,
                base_offset: Offset(offset),
                ack,
            })
            .await
            .expect("send replicate verbatim");
            ack_rx
                .await
                .expect("replicate verbatim ack")
                .expect("replicate verbatim");
        }
    }
}

/// The batches that `role` keeps of `history` after [`PASSES`] compaction
/// passes with the high watermark at the log end.
async fn compacted(role: Role, history: &History, now_ms: i64) -> Vec<Kept> {
    let dir = tempdir().expect("tempdir");
    // One batch for each segment, and no delete retention, so the passes can
    // drop a marker.
    let config = LogConfig {
        cleanup_policy: CleanupPolicy::Compact,
        segment_size: bytes(1),
        delete_retention: millis(0),
        ..LogConfig::default()
    };
    let log = Arc::new(Mutex::new(Log::open(dir.path(), config).expect("open log")));
    let producer_state = Arc::new(ProducerState::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            replica_state: replica_state.clone(),
            producer_state: producer_state.clone(),
            ..Default::default()
        },
    );

    for (offset, batch) in (0..).zip(&history.batches) {
        append(role, &tx, &producer_state, batch.clone(), offset).await;
    }
    let log_end = log.lock().expect("log lock").log_end_offset();
    replica_state.lock().await.hw = log_end;
    if matches!(history.expiry, ProducerExpiry::Swept) {
        // The maintenance loop sweeps the tracker and the log of every
        // hosted partition (`spawn_producer_expiry`).
        let expiration = crate::config::BrokerConfig::default().producer_id_expiration;
        let later = now_ms + expiration.millis_i64();
        producer_state.expire_older_than(later, expiration).await;
        log.lock()
            .expect("log lock")
            .remove_expired_producers(later, expiration.millis_i64());
    }
    for _ in 0..PASSES {
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Compact { ack })
            .await
            .expect("send compact");
        ack_rx.await.expect("compact ack").expect("compact");
    }

    let kept = log
        .lock()
        .expect("log lock")
        .read(Offset(0), gibibytes(1))
        .expect("read")
        .batches
        .iter()
        .map(Kept::of)
        .collect();
    drop(tx);
    writer.await.expect("writer join");
    kept
}

/// Kafka's `Cleaner.cleanInto` keeps the last record of each producer in the
/// producer state of the log (`isBatchLastRecordOfProducer`), and that state
/// is the same on a leader and on a follower that has its log. A follower
/// therefore keeps the same batches as its leader.
#[tokio::test]
async fn compaction_keeps_the_last_record_of_each_active_producer_on_every_replica() {
    let now_ms = crate::time_util::now_ms();
    for history in histories(UnixMillis(now_ms)) {
        for role in [Role::Leader, Role::Follower] {
            let kept = compacted(role, &history, now_ms).await;
            check!(kept == history.want, "{}, {role:?}", history.name);
        }
    }
}
