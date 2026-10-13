use {
    super::{Checked, Decision, ProducerState, RetainedBatch, SequenceContext},
    assert2::assert,
    krabka_ids::PartitionIndex,
    krabka_log::{Log, LogConfig, Offset},
    krabka_units::prelude::bytes,
};

/// `state.check_batch` for partition `t-0` outside a transaction, as every
/// test in this module and in `truncation_replay` asks it.
pub(super) async fn check(
    state: &ProducerState,
    producer: (i64, i16),
    sequences: (i32, i32),
) -> Checked {
    state
        .check_batch(
            "t",
            PartitionIndex(0),
            SequenceContext::RELEASED,
            producer,
            sequences,
        )
        .await
}

use super::test_support::{opened_log, producer_batch as batch};

#[tokio::test]
async fn reopening_rebuilds_only_the_snapshot_seed_and_uncovered_tail() {
    for count in 0..=7 {
        let (dir, config, mut log) = opened_log();
        log.append(&mut batch().call()).unwrap();
        log.append(&mut batch().sequence(1).call()).unwrap();
        log.take_producer_snapshot().unwrap();
        for sequence in 2..2 + count {
            log.append(&mut batch().sequence(sequence).call()).unwrap();
        }
        drop(log);
        let reopened = Log::open(dir.path(), config).unwrap();
        let state = rebuilt(&reopened).await;
        // The older covered batch still exists, but the snapshot stores only
        // the last batch and replay begins after both covered batches.
        let covered = check(&state, (42, 7), (0, 0)).await;
        assert!(covered.decision == Decision::OutOfOrder);
        assert!(covered.duplicate == None);
        let seed = check(&state, (42, 7), (1, 0)).await;
        if count < 5 {
            assert!(seed.decision == Decision::Duplicate { base_offset: 1 });
            assert!(
                seed.duplicate
                    == Some(RetainedBatch {
                        base_sequence: 1,
                        last_sequence: 1,
                        base_offset: 1,
                        last_offset: 1,
                        timestamp: 0
                    })
            );
        } else {
            assert!(!matches!(seed.decision, Decision::Duplicate { .. }));
            assert!(seed.duplicate == None);
        }
        for sequence in 2..2 + count {
            let checked = check(&state, (42, 7), (sequence, 0)).await;
            if 2 + count - sequence <= 5 {
                assert!(
                    checked.decision
                        == Decision::Duplicate {
                            base_offset: i64::from(sequence)
                        }
                );
                assert!(
                    checked.duplicate
                        == Some(RetainedBatch {
                            base_sequence: sequence,
                            last_sequence: sequence,
                            base_offset: i64::from(sequence),
                            last_offset: i64::from(sequence),
                            timestamp: 0
                        })
                );
            } else {
                assert!(!matches!(checked.decision, Decision::Duplicate { .. }));
                assert!(checked.duplicate == None);
            }
        }
    }
}

#[tokio::test]
async fn a_surviving_snapshot_preserves_a_retry_below_the_local_floor() {
    let dir = tempfile::tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    log.append(&mut batch().delta(1).call()).unwrap();
    log.append(&mut batch().producer(99).call()).unwrap();
    log.take_producer_snapshot().unwrap();
    assert!(log.trim_to_offset(Offset(2)).unwrap() == Offset(2));
    drop(log);
    let reopened = Log::open(dir.path(), config).unwrap();
    assert!(reopened.log_start_offset() == Offset(2));
    assert!(reopened.local_log_start_offset() == Offset(2));
    let state = rebuilt(&reopened).await;
    let checked = check(&state, (42, 7), (0, 1)).await;
    check_duplicate(&checked, (0, 1), (0, 1));
}

/// Rebuild partition t-0's producer state from the current log.
pub(super) async fn rebuilt(log: &Log) -> ProducerState {
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), log)
        .await
        .unwrap();
    state
}

/// Assert the complete retained-batch witness for a duplicate retry.
pub(super) fn check_duplicate(checked: &Checked, sequence: (i32, i32), offsets: (i64, i64)) {
    assert!(
        checked.decision
            == Decision::Duplicate {
                base_offset: offsets.0
            }
    );
    assert!(
        checked.duplicate
            == Some(RetainedBatch {
                base_sequence: sequence.0,
                last_sequence: sequence.1,
                base_offset: offsets.0,
                last_offset: offsets.1,
                timestamp: 0,
            })
    );
}
