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

#[test]
fn barrier_finalization_is_partial_only_for_a_valid_owned_pending_cut() {
    use BarrierRecoveryFinalizeDecision::{
        FencedCoordinator, FinalizePartial, MalformedPending, NoPending, UnknownCoordinator,
    };

    for (facts, expected) in [
        ((false, Some(3), 3, true), NoPending),
        ((true, Some(3), -1, true), MalformedPending),
        ((true, Some(3), 3, false), MalformedPending),
        ((true, None, 3, true), UnknownCoordinator),
        ((true, Some(2), 3, true), FencedCoordinator),
        ((true, Some(3), 3, true), FinalizePartial),
        ((true, Some(0), 0, true), FinalizePartial),
        ((true, Some(4), 3, true), FinalizePartial),
    ] {
        let (has_pending, current, frozen, targets_valid) = facts;
        assert2::check!(
            barrier_recovery_finalize_decision(has_pending, current, frozen, targets_valid,)
                == expected
        );
    }
}

#[test]
fn record_replay_is_bounded_and_type_separated() {
    use ReplayRecordDecision::{Apply, Skip};

    for (case, (batch_base, delta, from, end, control, replay_control), expected) in [
        (
            "inside the window",
            (10, 1, 10, 12, false, false),
            Apply(11),
        ),
        (
            "first offset of the window",
            (10, 0, 10, 12, false, false),
            Apply(10),
        ),
        ("below the window start", (8, 1, 10, 12, false, false), Skip),
        ("at the exclusive end", (10, 2, 10, 12, false, false), Skip),
        ("negative delta", (10, -1, 0, 12, false, false), Skip),
        (
            "unrepresentable offset",
            (i64::MAX, 1, 0, i64::MAX, false, false),
            Skip,
        ),
        (
            "control record on the data pass",
            (10, 0, 10, 12, true, false),
            Skip,
        ),
        (
            "data record on the control pass",
            (10, 0, 10, 12, false, true),
            Skip,
        ),
        (
            "control record on the control pass",
            (10, 0, 10, 12, true, true),
            Apply(10),
        ),
    ] {
        assert2::check!(
            replay_record_decision(batch_base, delta, from, end, control, replay_control)
                == expected,
            "{case}"
        );
    }
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

#[test]
fn batch_cursor_is_bounded_progress_or_stop() {
    use ReplayCursorDecision::{Advance, Stop};

    for (cursor, end, batch, expected) in [
        (10, 20, None, Stop),
        (10, 20, Some((10, 2)), Advance(13)),
        (10, 20, Some((15, 1)), Advance(17)),
        (10, 20, Some((9, 2)), Stop),
        (10, 20, Some((10, -1)), Stop),
        (10, 12, Some((10, 2)), Stop),
        (20, 20, Some((20, 0)), Stop),
        (
            i64::MAX - 1,
            i64::MAX,
            Some((i64::MAX - 1, 0)),
            Advance(i64::MAX),
        ),
        (i64::MAX - 1, i64::MAX, Some((i64::MAX - 1, 1)), Stop),
    ] {
        assert2::assert!(replay_batch_cursor_decision(cursor, end, batch) == expected);
    }
}
