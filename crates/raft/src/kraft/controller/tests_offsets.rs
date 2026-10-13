//! Unit tests for the offset arithmetic in [`super::offsets`]: the append
//! invariant, the apply window a high-watermark advance opens, and the
//! half-open ranges that bound a committed read.

use {
    super::*,
    crate::kraft::controller::offsets::{
        append_result_is_consistent, assigned_record_offset, batch_base_in_apply_window,
        committed_records_since_snapshot, expected_hwm_after_advance,
        fetch_batch_committed_before_hwm, fetch_offset_has_records, hwm_advanced_as_expected,
        hwm_reaches_waiter, leader_alone_is_majority, metadata_fetch_offset_in_committed_window,
        snapshot_bytes_reached, snapshot_interval_reached, snapshot_time_reached,
        submit_waiter_need_offset, validate_append_result,
    },
    assert2::assert,
};

#[test]
fn submit_offset_helpers_use_base_plus_blob_count() {
    for (_case, base, count, want) in [
        ("empty submission", 9, 0, 9),
        ("three-record submission", 9, 3, 12),
    ] {
        assert2::assert!(assigned_record_offset(Offset(base), count) == want);
        assert2::assert!(
            submit_waiter_need_offset(Offset(base), usize::try_from(count).unwrap()) == want
        );
    }
}

#[test]
fn append_result_must_match_previous_log_end_and_advance_log() {
    for (_case, expected_base, returned_base, log_end_after, want) in [
        ("matching advancing append", 4, 4, 5, true),
        ("negative returned base", 4, -1, 5, false),
        ("mismatched returned base", 4, 5, 5, false),
        ("log did not advance", 4, 4, 4, false),
    ] {
        assert2::assert!(
            append_result_is_consistent(
                Offset(expected_base),
                Offset(returned_base),
                Offset(log_end_after)
            ) == want
        );
    }
    assert2::assert!(validate_append_result("test", Offset(4), Offset(4), Offset(5)).is_ok());
    assert2::assert!(validate_append_result("test", Offset(4), Offset(-1), Offset(4)).is_err());
}

/// Only a leader that is itself the one voter commits its own append. A leader
/// that its own uncommitted `VotersRecord` removed, leaving one other voter,
/// waits for that voter's fetch.
#[test]
fn only_a_sole_voter_leader_commits_alone() {
    // (majority, leader is a voter, commits alone)
    let cases = [
        (1, true, true),
        (1, false, false),
        (0, true, false),
        (2, true, false),
        (2, false, false),
    ];
    let decided: Vec<bool> = cases
        .iter()
        .map(|&(majority, voter, _)| leader_alone_is_majority(majority, voter))
        .collect();
    let expected: Vec<bool> = cases.iter().map(|&(.., want)| want).collect();
    assert!(decided == expected);
}

#[test]
fn apply_window_includes_only_newly_committed_batch_bases() {
    for (_case, base_offset, prev_hwm, applied_hwm, want) in [
        ("first newly committed batch", 5, 5, 6, true),
        ("interior newly committed batch", 6, 5, 8, true),
        ("already applied batch", 4, 5, 8, false),
        ("exclusive applied boundary", 8, 5, 8, false),
    ] {
        assert2::assert!(
            batch_base_in_apply_window(base_offset, Offset(prev_hwm), Offset(applied_hwm)) == want
        );
    }
}

#[test]
fn snapshot_threshold_uses_positive_hwm_delta_from_last_snapshot() {
    for (_case, hwm, last_snapshot_end, want) in [
        ("positive committed delta", 10, 4, 6),
        ("snapshot ahead of HWM", 4, 10, 0),
    ] {
        assert2::assert!(
            committed_records_since_snapshot(Offset(hwm), Offset(last_snapshot_end)) == want
        );
    }
    for (_case, advanced, interval, want) in [
        ("exact threshold", 3, 3, true),
        ("above threshold", 4, 3, true),
        ("below threshold", 2, 3, false),
        ("disabled cap never reached", 5, 0, false),
    ] {
        assert2::assert!(snapshot_interval_reached(advanced, interval) == want);
    }
}

#[test]
fn snapshot_bytes_and_time_caps_disable_at_zero_and_trigger_at_or_past_threshold() {
    for (_case, bytes_since_snapshot, max_bytes_between_snapshots, want) in [
        ("exact threshold", 20, 20, true),
        ("above threshold", 21, 20, true),
        ("below threshold", 19, 20, false),
        ("disabled cap never reached", u64::MAX, 0, false),
    ] {
        assert2::assert!(
            snapshot_bytes_reached(bytes_since_snapshot, max_bytes_between_snapshots) == want
        );
    }
    for (_case, elapsed_ms, max_snapshot_interval_ms, want) in [
        ("exact threshold", 3_600_000, 3_600_000, true),
        ("above threshold", 3_600_001, 3_600_000, true),
        ("below threshold", 3_599_999, 3_600_000, false),
        ("disabled cap never reached", u64::MAX, 0, false),
    ] {
        assert2::assert!(snapshot_time_reached(elapsed_ms, max_snapshot_interval_ms) == want);
    }
}

#[test]
fn expected_hwm_after_advance_is_monotonic_and_clamped_to_log_end() {
    for (_case, prev_hwm, new_hwm, log_end, want) in [
        ("clamp above log end", 2, 5, 4, 4),
        ("prevent regression", 2, 1, 4, 2),
        ("ordinary advance", 2, 3, 4, 3),
    ] {
        assert2::assert!(
            expected_hwm_after_advance(Offset(prev_hwm), Offset(new_hwm), Offset(log_end)) == want
        );
    }
    for (_case, applied_hwm, expected_hwm, want) in [
        ("exact expected HWM", 4, 4, true),
        ("beyond expected HWM", 5, 4, true),
        ("below expected HWM", 3, 4, false),
    ] {
        assert2::assert!(
            hwm_advanced_as_expected(Offset(applied_hwm), Offset(expected_hwm)) == want
        );
    }
}

#[test]
fn waiter_resolution_requires_hwm_to_reach_need_offset() {
    for (_case, hwm, need_offset, want) in [
        ("HWM reaches waiter", 5, 5, true),
        ("HWM passes waiter", 6, 5, true),
        ("HWM below waiter", 4, 5, false),
    ] {
        assert2::assert!(hwm_reaches_waiter(Offset(hwm), Offset(need_offset)) == want);
    }
}

#[test]
fn metadata_fetch_window_is_committed_half_open_range() {
    for (_case, fetch_offset, hwm, want) in [
        ("first committed offset", 0, 1, true),
        ("last committed offset", 4, 5, true),
        ("negative offset", -1, 5, false),
        ("exclusive HWM boundary", 5, 5, false),
    ] {
        assert2::assert!(
            metadata_fetch_offset_in_committed_window(Offset(fetch_offset), Offset(hwm)) == want
        );
    }
    assert2::assert!(fetch_batch_committed_before_hwm(4, Offset(5)));
    assert2::assert!(!fetch_batch_committed_before_hwm(5, Offset(5)));
}

#[test]
fn fetch_record_offsets_are_inside_log_window_only() {
    for (_case, fetch_offset, log_end, want) in [
        ("first available record", 0, 1, true),
        ("last available record", 4, 5, true),
        ("negative offset", -1, 5, false),
        ("exclusive log-end boundary", 5, 5, false),
    ] {
        assert2::assert!(fetch_offset_has_records(Offset(fetch_offset), Offset(log_end)) == want);
    }
}
