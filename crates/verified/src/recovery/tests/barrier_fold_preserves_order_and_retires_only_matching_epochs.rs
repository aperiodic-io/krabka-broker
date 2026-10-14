use super::*;

#[test]
fn barrier_fold_preserves_order_and_retires_only_matching_epochs() {
    use {
        BarrierRecoveryFoldAction::{ClearPending, KeepPending, RemoveCut, SetPending, UpsertCut},
        BarrierRecoveryRecordKind::{Cut, InjectionStart},
    };

    assert2::check!(
        barrier_recovery_fold_action(InjectionStart, true, 7, None, false) == SetPending
    );
    assert2::check!(
        barrier_recovery_fold_action(Cut, true, 7, Some(7), false)
            == UpsertCut {
                retire_pending: true,
            }
    );
    assert2::check!(
        barrier_recovery_fold_action(InjectionStart, true, 7, None, true) == KeepPending
    );
    assert2::check!(
        barrier_recovery_fold_action(InjectionStart, false, 6, Some(7), false) == KeepPending
    );
    assert2::check!(
        barrier_recovery_fold_action(InjectionStart, false, 7, Some(7), false) == ClearPending
    );
    assert2::check!(barrier_recovery_fold_action(Cut, false, 7, Some(9), true) == RemoveCut);
}

#[yare::parameterized(
    no_pending = { false, Some(3), 3, true, BarrierRecoveryFinalizeDecision::NoPending },
    negative_epoch = { true, Some(3), -1, true, BarrierRecoveryFinalizeDecision::MalformedPending },
    invalid_targets = {
        true,
        Some(3),
        3,
        false,
        BarrierRecoveryFinalizeDecision::MalformedPending,
    },
    unknown_coordinator = {
        true,
        None,
        3,
        true,
        BarrierRecoveryFinalizeDecision::UnknownCoordinator,
    },
    fenced_coordinator = {
        true,
        Some(2),
        3,
        true,
        BarrierRecoveryFinalizeDecision::FencedCoordinator,
    },
    matching_epoch = { true, Some(3), 3, true, BarrierRecoveryFinalizeDecision::FinalizePartial },
    zero_epoch = { true, Some(0), 0, true, BarrierRecoveryFinalizeDecision::FinalizePartial },
    newer_coordinator = {
        true,
        Some(4),
        3,
        true,
        BarrierRecoveryFinalizeDecision::FinalizePartial,
    },
)]
fn barrier_finalization_is_partial_only_for_a_valid_owned_pending_cut(
    has_pending: bool,
    current: Option<i32>,
    frozen: i32,
    targets_valid: bool,
    expected: BarrierRecoveryFinalizeDecision,
) {
    assert2::check!(
        barrier_recovery_finalize_decision(has_pending, current, frozen, targets_valid) == expected
    );
}

#[yare::parameterized(
    inside = { "inside the window", 10, 1, 10, 12, false, false, ReplayRecordDecision::Apply(11) },
    first = {
        "first offset of the window",
        10,
        0,
        10,
        12,
        false,
        false,
        ReplayRecordDecision::Apply(10),
    },
    below = { "below the window start", 8, 1, 10, 12, false, false, ReplayRecordDecision::Skip },
    exclusive_end = {
        "at the exclusive end",
        10,
        2,
        10,
        12,
        false,
        false,
        ReplayRecordDecision::Skip,
    },
    negative_delta = { "negative delta", 10, -1, 0, 12, false, false, ReplayRecordDecision::Skip },
    overflow = {
        "unrepresentable offset",
        i64::MAX,
        1,
        0,
        i64::MAX,
        false,
        false,
        ReplayRecordDecision::Skip,
    },
    control_on_data_pass = {
        "control record on the data pass",
        10,
        0,
        10,
        12,
        true,
        false,
        ReplayRecordDecision::Skip,
    },
    data_on_control_pass = {
        "data record on the control pass",
        10,
        0,
        10,
        12,
        false,
        true,
        ReplayRecordDecision::Skip,
    },
    control_on_control_pass = {
        "control record on the control pass",
        10,
        0,
        10,
        12,
        true,
        true,
        ReplayRecordDecision::Apply(10),
    },
)]
fn record_replay_is_bounded_and_type_separated(
    case: &'static str,
    batch_base: i64,
    delta: i32,
    from: i64,
    end: i64,
    control: bool,
    replay_control: bool,
    expected: ReplayRecordDecision,
) {
    assert2::check!(
        replay_record_decision(batch_base, delta, from, end, control, replay_control) == expected,
        "{case}"
    );
}

#[test]
fn cursor_and_downgrade_decisions_are_fail_closed() {
    use ReplayCursorDecision::{Advance, Stop};

    assert2::assert!(replay_cursor_decision(10, Some(11)) == Advance(11));
    assert2::assert!(replay_cursor_decision(10, Some(10)) == Stop);
    assert2::assert!(replay_cursor_decision(10, Some(9)) == Stop);
    assert2::assert!(replay_cursor_decision(10, None) == Stop);
    assert2::assert!(should_capture_first_downgrade(false, true));
    assert2::assert!(!should_capture_first_downgrade(true, true));
}

#[yare::parameterized(
    missing_batch = { 10, 20, None, ReplayCursorDecision::Stop },
    within_end = { 10, 20, Some((10, 2)), ReplayCursorDecision::Advance(13) },
    later_batch = { 10, 20, Some((15, 1)), ReplayCursorDecision::Advance(17) },
    before_cursor = { 10, 20, Some((9, 2)), ReplayCursorDecision::Stop },
    negative_delta = { 10, 20, Some((10, -1)), ReplayCursorDecision::Stop },
    at_end = { 10, 12, Some((10, 2)), ReplayCursorDecision::Stop },
    empty_at_end = { 20, 20, Some((20, 0)), ReplayCursorDecision::Stop },
    final_representable = {
        i64::MAX - 1,
        i64::MAX,
        Some((i64::MAX - 1, 0)),
        ReplayCursorDecision::Advance(i64::MAX),
    },
    overflow = { i64::MAX - 1, i64::MAX, Some((i64::MAX - 1, 1)), ReplayCursorDecision::Stop },
)]
fn batch_cursor_is_bounded_progress_or_stop(
    cursor: i64,
    end: i64,
    batch: Option<(i64, i32)>,
    expected: ReplayCursorDecision,
) {
    assert2::assert!(replay_batch_cursor_decision(cursor, end, batch) == expected);
}
