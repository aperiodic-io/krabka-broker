use {
    super::{
        Decision, RetainedBatch,
        snapshot_tail::{check, check_duplicate, rebuilt},
        test_support::{
            ProducerBatchSetup, ProducerEpoch, ProducerSequence, RecordOffsets, SequenceDelta,
            producer_batch,
        },
    },
    crate::{
        producer_state::test_support::opened_log,
        test_support::{EmptyPayloadBatchSetup, default_records_batch},
    },
    assert2::assert,
};

#[tokio::test]
async fn rebuilt_producer_retries_never_name_a_truncated_batch() {
    for cut in 0..=5 {
        let (dir, config, mut log) = opened_log();
        for sequence in [0, 2] {
            log.append(&mut producer_batch(ProducerBatchSetup {
                delta: SequenceDelta(1),
                sequence: ProducerSequence(sequence),
                ..Default::default()
            }))
            .unwrap();
        }
        log.append(&mut default_records_batch(EmptyPayloadBatchSetup::default()))
            .unwrap();
        log.truncate_to(krabka_log::Offset(cut)).unwrap();
        let end = log.log_end_offset().0;
        let expected_end = match cut {
            0 | 1 => 0,
            2 | 3 => 2,
            4 => 4,
            _ => 5,
        };
        assert!(end == expected_end);
        let state = rebuilt(&log).await;
        for sequence in [0, 2] {
            let checked = check(&state, (42, 7), (sequence, 1)).await;
            if i64::from(sequence) + 2 <= end {
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
                            last_sequence: sequence + 1,
                            base_offset: i64::from(sequence),
                            last_offset: i64::from(sequence) + 1,
                            timestamp: 0,
                        })
                );
            } else {
                assert!(!matches!(checked.decision, Decision::Duplicate { .. }));
                assert!(checked.duplicate == None);
            }
        }
        drop(log);
        let reopened = krabka_log::Log::open(dir.path(), config).unwrap();
        let state = rebuilt(&reopened).await;
        // A snapshot-only reopen retains just the latest batch metadata.
        // Qualify that actual recovered window without assuming earlier slots.
        let recovered = reopened.recovered_producers();
        for producer in &recovered {
            let row = producer.entry;
            let checked = check(
                &state,
                (row.producer_id.0, row.producer_epoch),
                (
                    krabka_verified::decrement_sequence(row.last_sequence, row.offset_delta),
                    row.offset_delta,
                ),
            )
            .await;
            let witness = checked.duplicate.unwrap();
            assert!(witness.base_offset == row.last_offset.0 - i64::from(row.offset_delta));
            assert!(witness.last_offset < end);
        }
        assert!(recovered.is_empty() == (end == 0));
    }
}

#[tokio::test]
async fn deleting_the_tail_reintroduces_an_older_sequence_alias() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    for (sequence, delta) in [(0, 0), (1, i32::MAX - 1), (0, 0), (1, 0), (2, 0), (3, 0)] {
        log.append(&mut producer_batch(ProducerBatchSetup {
            sequence: ProducerSequence(sequence),
            delta: SequenceDelta(delta),
            offsets: RecordOffsets::FinalOffsetOnly,
            ..Default::default()
        }))
        .unwrap();
    }
    let state = rebuilt(&log).await;
    let checked = check(&state, (42, 7), (0, 0)).await;
    assert!(
        checked.decision
            == Decision::Duplicate {
                base_offset: i64::from(i32::MAX) + 1
            }
    );
    // The middle alias survives, but replay of the shorter log brings back
    // the oldest alias that the previous five-slot window had evicted.
    let cut = log.log_end_offset().0 - 1;
    log.truncate_to(krabka_log::Offset(cut)).unwrap();
    let state = rebuilt(&log).await;
    let checked = check(&state, (42, 7), (0, 0)).await;
    check_duplicate(&checked, (0, 0), (0, 0));
}

#[tokio::test]
async fn deleting_a_newer_epoch_restores_the_surviving_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    for epoch in [6, 7] {
        log.append(&mut producer_batch(ProducerBatchSetup {
            delta: SequenceDelta(1),
            epoch: ProducerEpoch(epoch),
            ..Default::default()
        }))
        .unwrap();
    }
    let state = rebuilt(&log).await;
    let checked = check(&state, (42, 6), (0, 1)).await;
    assert!(checked.decision == Decision::Fenced);
    log.truncate_to(krabka_log::Offset(3)).unwrap();
    assert!(log.log_end_offset().0 == 2);
    let state = rebuilt(&log).await;
    let checked = check(&state, (42, 6), (0, 1)).await;
    check_duplicate(&checked, (0, 1), (0, 1));
}
