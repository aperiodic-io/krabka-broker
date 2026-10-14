use super::*;

#[yare::parameterized(
    ordinary = { 100, 50, 150 },
    upper_saturation = { i64::MAX - 1, 50, i64::MAX },
    lower_saturation = { i64::MIN + 1, -50, i64::MIN },
)]
fn compute_horizon_saturates_at_i64_bounds(timestamp: i64, lag: i64, expected: i64) {
    assert2::assert!(compute_horizon(timestamp, lag) == expected);
}

#[test]
fn compaction_decode_step_truth_table() {
    for remaining_before in 0..=2 {
        for remaining_after in 0..=2 {
            for decode_succeeded in [false, true] {
                let expected = if remaining_before == 0 {
                    CompactionDecodeStep::Done
                } else if decode_succeeded && remaining_after < remaining_before {
                    CompactionDecodeStep::Continue
                } else {
                    CompactionDecodeStep::Corrupt
                };
                assert2::assert!(
                    compaction_decode_step(remaining_before, decode_succeeded, remaining_after)
                        == expected
                );
            }
        }
    }
}

#[test]
fn retain_decision_distinguishes_expired_and_live_horizons() {
    let tombstone = record(true, false);
    let live_value = record(true, true);

    assert2::assert!(
        retain_decision(
            tombstone,
            batch(false, Some(10)),
            true,
            TxnDataState::NotTransactional,
            10,
            50
        ) == RetainDecision::Delete
    );
    assert2::assert!(
        retain_decision(
            tombstone,
            batch(false, Some(10)),
            true,
            TxnDataState::NotTransactional,
            9,
            50
        ) == RetainDecision::Keep
    );
    assert2::assert!(
        retain_decision(
            live_value,
            batch(false, None),
            true,
            TxnDataState::NotTransactional,
            100,
            50
        ) == RetainDecision::Keep
    );
    assert2::assert!(
        retain_decision(
            record(false, true),
            batch(false, None),
            true,
            TxnDataState::NotTransactional,
            100,
            50
        ) == RetainDecision::Delete
    );
}

#[test]
fn retain_decision_stamps_new_tombstone_and_expired_control_marker() {
    assert2::assert!(
        retain_decision(
            record(true, false),
            batch(false, None),
            true,
            TxnDataState::NotTransactional,
            100,
            50
        ) == RetainDecision::SetHorizon(150)
    );
    assert2::assert!(
        retain_decision(
            record(true, false),
            batch(true, Some(10)),
            false,
            TxnDataState::DataFullyGone,
            10,
            50
        ) == RetainDecision::Delete
    );
    assert2::assert!(
        retain_decision(
            record(true, false),
            batch(true, Some(10)),
            false,
            TxnDataState::DataFullyGone,
            9,
            50
        ) == RetainDecision::Keep
    );
    assert2::assert!(
        retain_decision(
            record(true, false),
            batch(true, Some(10)),
            false,
            TxnDataState::DataSurvives,
            10,
            50
        ) == RetainDecision::Keep
    );
}
