use assert2::assert;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use super::*;
use crate::{
    TransactionalBatch,
    log::test_support::{
        AppendPath as Path, append_path as append, control_key, control_value, test_log,
    },
};

// A data batch of `producer` (`(id, epoch)`) with `records` records from
// `base_sequence`, whose max timestamp is `max_timestamp`.
krabka_macros::producer_batch_fixture!(data, ::bytes::Bytes::from_static(b"v"));

#[derive(Clone, Copy, Default)]
enum MarkerOutcome {
    #[default]
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MarkerCoordinatorEpoch(i32);

/// A commit or abort marker with an explicit producer, coordinator, and timestamp.
#[derive(Clone, Copy)]
struct MarkerSetup {
    producer: BatchProducer,
    outcome: MarkerOutcome,
    coordinator_epoch: MarkerCoordinatorEpoch,
    timestamp: BatchTimestamp,
}

impl Default for MarkerSetup {
    fn default() -> Self {
        Self {
            producer: BatchProducer::from_wire((7, 0)),
            outcome: MarkerOutcome::Commit,
            coordinator_epoch: MarkerCoordinatorEpoch(0),
            timestamp: BatchTimestamp(1_000),
        }
    }
}

fn marker(setup: MarkerSetup) -> RecordBatch {
    let MarkerSetup {
        producer,
        outcome,
        coordinator_epoch,
        timestamp,
    } = setup;
    RecordBatch {
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        base_timestamp: timestamp.0,
        max_timestamp: timestamp.0,
        producer_id: producer.id.0,
        producer_epoch: producer.epoch.0,
        records: vec![Record {
            key: Some(control_key(match outcome {
                MarkerOutcome::Commit => 1,
                MarkerOutcome::Abort => 0,
            })),
            value: Some(control_value(coordinator_epoch.0)),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

#[derive(Clone, Copy)]
enum ProducerHistory {
    MissingId,
    Idempotent,
    Committed,
    Aborted,
    MarkerOnly,
    SeveralProducers,
}

/// Identical input histories exercised by both producer-state projections.
fn producer_history(history: ProducerHistory) -> Vec<RecordBatch> {
    match history {
        ProducerHistory::MissingId => vec![data(ProducerBatchSetup {
            producer: BatchProducer::from_wire((-1, -1)),
            base_sequence: BatchSequence(-1),
            records: BatchRecordCount(2),
            ..Default::default()
        })],
        ProducerHistory::Idempotent => vec![
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((7, 2)),
                records: BatchRecordCount(3),
                ..Default::default()
            }),
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((7, 2)),
                base_sequence: BatchSequence(3),
                records: BatchRecordCount(2),
                max_timestamp: BatchTimestamp(2_000),
                ..Default::default()
            }),
        ],
        ProducerHistory::Committed => vec![
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((9, 1)),
                records: BatchRecordCount(2),
                transaction: BatchTransactionMode::Transactional,
                ..Default::default()
            }),
            marker(MarkerSetup {
                producer: BatchProducer::from_wire((9, 1)),
                coordinator_epoch: MarkerCoordinatorEpoch(5),
                timestamp: BatchTimestamp(3_000),
                ..Default::default()
            }),
        ],
        ProducerHistory::Aborted => vec![
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((10, 1)),
                records: BatchRecordCount(2),
                transaction: BatchTransactionMode::Transactional,
                ..Default::default()
            }),
            marker(MarkerSetup {
                producer: BatchProducer::from_wire((10, 2)),
                outcome: MarkerOutcome::Abort,
                coordinator_epoch: MarkerCoordinatorEpoch(6),
                timestamp: BatchTimestamp(4_000),
            }),
        ],
        ProducerHistory::MarkerOnly => vec![marker(MarkerSetup {
            producer: BatchProducer::from_wire((11, 3)),
            coordinator_epoch: MarkerCoordinatorEpoch(7),
            timestamp: BatchTimestamp(5_000),
            ..Default::default()
        })],
        ProducerHistory::SeveralProducers => vec![
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((30, 0)),
                ..Default::default()
            }),
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((20, 0)),
                max_timestamp: BatchTimestamp(2_000),
                ..Default::default()
            }),
            data(ProducerBatchSetup {
                producer: BatchProducer::from_wire((25, 4)),
                max_timestamp: BatchTimestamp(3_000),
                transaction: BatchTransactionMode::Transactional,
                ..Default::default()
            }),
        ],
    }
}

#[derive(Clone, Copy)]
struct ActiveProducerSetup {
    producer_id: ProducerId,
    producer_epoch: BatchProducerEpoch,
    last_sequence: BatchSequence,
    last_timestamp: BatchTimestamp,
    coordinator_epoch: MarkerCoordinatorEpoch,
    current_txn_start_offset: Option<Offset>,
}

impl Default for ActiveProducerSetup {
    fn default() -> Self {
        Self {
            producer_id: ProducerId(7),
            producer_epoch: BatchProducerEpoch(0),
            last_sequence: BatchSequence(0),
            last_timestamp: BatchTimestamp(1_000),
            coordinator_epoch: MarkerCoordinatorEpoch(-1),
            current_txn_start_offset: None,
        }
    }
}

fn active(setup: ActiveProducerSetup) -> ActiveProducer {
    let ActiveProducerSetup {
        producer_id,
        producer_epoch,
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_txn_start_offset,
    } = setup;
    ActiveProducer {
        producer_id,
        producer_epoch: producer_epoch.0,
        last_sequence: last_sequence.0,
        last_timestamp: last_timestamp.0,
        coordinator_epoch: coordinator_epoch.0,
        current_txn_start_offset,
    }
}

/// Compare a projection after each append path, replay and producer snapshot load.
fn check_append_paths<T: std::fmt::Debug + PartialEq>(
    name: &str,
    batches: &[RecordBatch],
    want: &T,
    read: impl Fn(&Log) -> T,
) {
    for path in [Path::Leader, Path::Follower, Path::Verbatim] {
        let (dir, mut log) = test_log();
        for batch in batches.iter().cloned() {
            append(&mut log, path, batch);
        }
        assert!(read(&log) == *want, "{name}, {path:?}");

        drop(log);
        let replayed = crate::test_support::open_log(dir.path());
        assert!(read(&replayed) == *want, "{name}, {path:?}, replayed");

        replayed.close();
        let loaded = crate::test_support::open_log(dir.path());
        assert!(
            read(&loaded) == *want,
            "{name}, {path:?}, from the snapshot"
        );
    }
}

/// Kafka's `UnifiedLog.activeProducers` reads the `ProducerStateEntry` of
/// each producer: the last sequence of its last batch at its epoch, the max
/// timestamp of that batch or of the marker after it, the coordinator epoch
/// of its last marker and the first offset of its open transaction. A leader
/// append, a follower append and a verbatim follower append give the same
/// answer, and so do a reopen that replays the log and a reopen that loads
/// the producer-state snapshot.
#[test]
fn active_producers_report_the_producer_state_of_every_append_path() {
    let cases: Vec<(&str, Vec<RecordBatch>, Vec<ActiveProducer>)> = vec![
        (
            "a batch without a producer id",
            producer_history(ProducerHistory::MissingId),
            vec![],
        ),
        (
            "idempotent batches",
            producer_history(ProducerHistory::Idempotent),
            vec![active(ActiveProducerSetup {
                producer_epoch: BatchProducerEpoch(2),
                last_sequence: BatchSequence(4),
                last_timestamp: BatchTimestamp(2_000),
                ..Default::default()
            })],
        ),
        (
            "an open transaction",
            vec![
                data(ProducerBatchSetup {
                    producer: BatchProducer::from_wire((8, 0)),
                    records: BatchRecordCount(2),
                    transaction: BatchTransactionMode::Transactional,
                    ..Default::default()
                }),
                data(ProducerBatchSetup {
                    producer: BatchProducer::from_wire((8, 0)),
                    base_sequence: BatchSequence(2),
                    max_timestamp: BatchTimestamp(1_500),
                    transaction: BatchTransactionMode::Transactional,
                    ..Default::default()
                }),
            ],
            vec![active(ActiveProducerSetup {
                producer_id: ProducerId(8),
                last_sequence: BatchSequence(2),
                last_timestamp: BatchTimestamp(1_500),
                current_txn_start_offset: Some(Offset(0)),
                ..Default::default()
            })],
        ),
        (
            "a commit at the same epoch (transaction version 1)",
            producer_history(ProducerHistory::Committed),
            vec![active(ActiveProducerSetup {
                producer_id: ProducerId(9),
                producer_epoch: BatchProducerEpoch(1),
                last_sequence: BatchSequence(1),
                last_timestamp: BatchTimestamp(3_000),
                coordinator_epoch: MarkerCoordinatorEpoch(5),
                ..Default::default()
            })],
        ),
        (
            "an abort at a bumped epoch (transaction version 2)",
            producer_history(ProducerHistory::Aborted),
            vec![active(ActiveProducerSetup {
                producer_id: ProducerId(10),
                producer_epoch: BatchProducerEpoch(2),
                last_sequence: BatchSequence(-1),
                last_timestamp: BatchTimestamp(4_000),
                coordinator_epoch: MarkerCoordinatorEpoch(6),
                ..Default::default()
            })],
        ),
        (
            "a marker without a data batch",
            producer_history(ProducerHistory::MarkerOnly),
            vec![active(ActiveProducerSetup {
                producer_id: ProducerId(11),
                producer_epoch: BatchProducerEpoch(3),
                last_sequence: BatchSequence(-1),
                last_timestamp: BatchTimestamp(5_000),
                coordinator_epoch: MarkerCoordinatorEpoch(7),
                ..Default::default()
            })],
        ),
        (
            "a transaction after a commit",
            vec![
                data(ProducerBatchSetup {
                    producer: BatchProducer::from_wire((12, 0)),
                    transaction: BatchTransactionMode::Transactional,
                    ..Default::default()
                }),
                marker(MarkerSetup {
                    producer: BatchProducer::from_wire((12, 0)),
                    coordinator_epoch: MarkerCoordinatorEpoch(2),
                    timestamp: BatchTimestamp(2_000),
                    ..Default::default()
                }),
                data(ProducerBatchSetup {
                    producer: BatchProducer::from_wire((12, 0)),
                    base_sequence: BatchSequence(1),
                    max_timestamp: BatchTimestamp(3_000),
                    transaction: BatchTransactionMode::Transactional,
                    ..Default::default()
                }),
            ],
            vec![active(ActiveProducerSetup {
                producer_id: ProducerId(12),
                last_sequence: BatchSequence(1),
                last_timestamp: BatchTimestamp(3_000),
                coordinator_epoch: MarkerCoordinatorEpoch(2),
                current_txn_start_offset: Some(Offset(2)),
                ..Default::default()
            })],
        ),
        (
            "several producers, in producer id order",
            producer_history(ProducerHistory::SeveralProducers),
            vec![
                active(ActiveProducerSetup {
                    producer_id: ProducerId(20),
                    last_timestamp: BatchTimestamp(2_000),
                    ..Default::default()
                }),
                active(ActiveProducerSetup {
                    producer_id: ProducerId(25),
                    producer_epoch: BatchProducerEpoch(4),
                    last_timestamp: BatchTimestamp(3_000),
                    current_txn_start_offset: Some(Offset(2)),
                    ..Default::default()
                }),
                active(ActiveProducerSetup {
                    producer_id: ProducerId(30),
                    ..Default::default()
                }),
            ],
        ),
    ];
    for (name, batches, want) in cases {
        check_append_paths(name, &batches, &want, Log::active_producers);
    }
}

/// Kafka's `UnifiedLog.lastRecordsOfActiveProducers`, which the cleaner
/// reads: the last offset of the last data batch of each producer, or none
/// when a marker at a new epoch has cleared its batches, and the epoch of the
/// producer. A leader append, a follower append and a verbatim follower append
/// give the same answer, and so do the two kinds of reopen.
#[test]
fn last_records_of_active_producers_follow_every_append_path() {
    let last = |producer_id, last_data_offset: Option<i64>, producer_epoch| {
        (
            ProducerId(producer_id),
            ProducerLastRecord {
                last_data_offset: last_data_offset.map(Offset),
                producer_epoch,
            },
        )
    };
    let cases: Vec<(
        &str,
        Vec<RecordBatch>,
        HashMap<ProducerId, ProducerLastRecord>,
    )> = vec![
        (
            "a batch without a producer id",
            producer_history(ProducerHistory::MissingId),
            HashMap::new(),
        ),
        (
            "idempotent batches",
            producer_history(ProducerHistory::Idempotent),
            HashMap::from([last(7, Some(4), 2)]),
        ),
        (
            "a commit at the same epoch (transaction version 1)",
            producer_history(ProducerHistory::Committed),
            HashMap::from([last(9, Some(1), 1)]),
        ),
        (
            "an abort at a bumped epoch (transaction version 2)",
            producer_history(ProducerHistory::Aborted),
            HashMap::from([last(10, None, 2)]),
        ),
        (
            "a marker without a data batch",
            producer_history(ProducerHistory::MarkerOnly),
            HashMap::from([last(11, None, 3)]),
        ),
        (
            "several producers",
            producer_history(ProducerHistory::SeveralProducers),
            HashMap::from([
                last(20, Some(1), 0),
                last(25, Some(2), 4),
                last(30, Some(0), 0),
            ]),
        ),
    ];
    for (name, batches, want) in cases {
        check_append_paths(name, &batches, &want, Log::last_records_of_active_producers);
    }
}

/// Kafka's `ProducerStateManager.isProducerExpired`: a producer expires when
/// it has no open transaction and `producer.id.expiration.ms` or more has
/// passed since its last timestamp. A producer with an open transaction never
/// expires.
#[test]
fn remove_expired_producers_keeps_open_transactions_and_recent_producers() {
    const EXPIRATION_MS: i64 = 1_000;
    // (case, now_ms, the producers that stay)
    let cases = [
        ("before any producer expires", 1_999, vec![1, 2, 3]),
        (
            "the idle producer at the exact expiration",
            2_000,
            vec![2, 3],
        ),
        ("both idle producers", 4_000, vec![3]),
        ("long after every write", i64::MAX, vec![3]),
    ];
    for (name, now_ms, staying) in cases {
        let (_dir, mut log) = test_log();
        log.append(&mut data(ProducerBatchSetup {
            producer: BatchProducer::from_wire((1, 0)),
            ..Default::default()
        }))
        .unwrap();
        log.append(&mut data(ProducerBatchSetup {
            producer: BatchProducer::from_wire((2, 0)),
            max_timestamp: BatchTimestamp(1_500),
            transaction: BatchTransactionMode::Transactional,
            ..Default::default()
        }))
        .unwrap();
        log.append(&mut marker(MarkerSetup {
            producer: BatchProducer::from_wire((2, 0)),
            coordinator_epoch: MarkerCoordinatorEpoch(4),
            timestamp: BatchTimestamp(3_000),
            ..Default::default()
        }))
        .unwrap();
        log.append(&mut data(ProducerBatchSetup {
            producer: BatchProducer::from_wire((3, 0)),
            transaction: BatchTransactionMode::Transactional,
            ..Default::default()
        }))
        .unwrap();
        let before = log.active_producers();

        log.remove_expired_producers(now_ms, EXPIRATION_MS);

        let want: Vec<ActiveProducer> = before
            .into_iter()
            .filter(|producer| staying.contains(&producer.producer_id.get()))
            .collect();
        assert!(log.active_producers() == want, "{name}");
        // An expired producer leaves no state behind: its coordinator epoch
        // does not fence a later marker.
        for producer_id in [1, 2, 3] {
            let kept = staying.contains(&producer_id);
            let (_, coordinator_epoch, _) = log.transaction_marker_state(ProducerId(producer_id));
            assert!(
                (coordinator_epoch == 4) == (kept && producer_id == 2),
                "{name}, producer {producer_id}"
            );
        }
    }
}

/// An expired producer starts again from no state: its retained batches are
/// gone too, so the next batch at the same epoch is its only batch.
#[test]
fn an_expired_producer_starts_again_without_its_retained_batches() {
    let (_dir, mut log) = test_log();
    log.append(&mut data(ProducerBatchSetup {
        producer: BatchProducer::from_wire((5, 0)),
        ..Default::default()
    }))
    .unwrap();
    log.append(&mut data(ProducerBatchSetup {
        producer: BatchProducer::from_wire((5, 0)),
        base_sequence: BatchSequence(1),
        ..Default::default()
    }))
    .unwrap();

    log.remove_expired_producers(10_000, 1_000);
    log.append(&mut data(ProducerBatchSetup {
        producer: BatchProducer::from_wire((5, 0)),
        base_sequence: BatchSequence(2),
        max_timestamp: BatchTimestamp(20_000),
        ..Default::default()
    }))
    .unwrap();

    let recovered = log.recovered_producers();
    assert!(recovered.len() == 1);
    assert!(recovered[0].earlier.is_empty());
    assert!(
        log.active_producers()
            == vec![active(ActiveProducerSetup {
                producer_id: ProducerId(5),
                last_sequence: BatchSequence(2),
                last_timestamp: BatchTimestamp(20_000),
                ..Default::default()
            })]
    );
}

/// Kafka's `removeExpiredProducers` also drops verification state that is
/// `producer.id.expiration.ms` or more old, so an append must start a new
/// verification.
#[test]
fn remove_expired_producers_drops_old_verification_state() {
    let batch = TransactionalBatch {
        producer_id: ProducerId(6),
        producer_epoch: 0,
        base_sequence: 0,
        is_transactional: true,
        is_control: false,
    };
    // (case, now_ms, whether the guard still verifies the append)
    let cases = [
        ("a fresh verification", 1_999, true),
        ("a verification at the expiration", 2_000, false),
    ];
    for (name, now_ms, verifies) in cases {
        let (_dir, mut log) = test_log();
        let guard = log
            .maybe_start_transaction_verification(batch, false, (1_000, 86_400_000))
            .unwrap();

        log.remove_expired_producers(now_ms, 1_000);

        assert!(
            log.check_transactional_append(batch, guard).is_ok() == verifies,
            "{name}"
        );
    }
}
