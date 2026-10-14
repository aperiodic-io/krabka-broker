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
};

#[yare::parameterized(empty_submission = { 9, 0, 9 }, three_record_submission = { 9, 3, 12 })]
fn submit_offset_helpers_use_base_plus_blob_count(base: i64, count: i64, want: i64) {
    assert2::assert!(assigned_record_offset(Offset(base), count) == want);
    assert2::assert!(
        submit_waiter_need_offset(Offset(base), usize::try_from(count).unwrap()) == Offset(want)
    );
}

#[yare::parameterized(
    matching_advancing_append = { 4, 4, 5, true },
    negative_returned_base = { 4, -1, 5, false },
    mismatched_returned_base = { 4, 5, 5, false },
    log_did_not_advance = { 4, 4, 4, false },
)]
fn append_result_must_match_previous_log_end_and_advance_log(
    expected_base: i64,
    returned_base: i64,
    log_end_after: i64,
    want: bool,
) {
    assert2::assert!(
        append_result_is_consistent(
            Offset(expected_base),
            Offset(returned_base),
            Offset(log_end_after)
        ) == want
    );
    assert2::assert!(
        validate_append_result(
            "test",
            Offset(expected_base),
            Offset(returned_base),
            Offset(log_end_after)
        )
        .is_ok()
            == want
    );
}

/// Only a leader that is itself the one voter commits its own append. A leader
/// that its own uncommitted `VotersRecord` removed, leaving one other voter,
/// waits for that voter's fetch.
#[yare::parameterized(
    sole_voter_is_leader = { 1,
    true,
    true },
    sole_voter_is_elsewhere = { 1,
    false,
    false },
    zero_majority = { 0,
    true,
    false },
    multi_voter_leader_is_voter = { 2,
    true,
    false },
    multi_voter_leader_is_not_voter = { 2,
    false,
    false }
)]
fn only_a_sole_voter_leader_commits_alone(majority: usize, voter: bool, want: bool) {
    assert2::assert!(leader_alone_is_majority(majority, voter) == want);
}

#[yare::parameterized(
    first_newly_committed_batch = { 5,
    5,
    6,
    true },
    interior_newly_committed_batch = { 6,
    5,
    8,
    true },
    already_applied_batch = { 4,
    5,
    8,
    false },
    exclusive_applied_boundary = { 8,
    5,
    8,
    false }
)]
fn apply_window_includes_only_newly_committed_batch_bases(
    base_offset: i64,
    prev_hwm: i64,
    applied_hwm: i64,
    want: bool,
) {
    assert2::assert!(
        batch_base_in_apply_window(base_offset, Offset(prev_hwm), Offset(applied_hwm)) == want
    );
}

#[yare::parameterized(positive_committed_delta = { 10, 4, 6 }, snapshot_ahead_of_hwm = { 4, 10, 0 })]
fn snapshot_threshold_uses_positive_hwm_delta_from_last_snapshot(
    hwm: i64,
    last_snapshot_end: i64,
    want: u64,
) {
    assert2::assert!(
        committed_records_since_snapshot(Offset(hwm), Offset(last_snapshot_end)) == want
    );
}

#[yare::parameterized(
    interval_exact_threshold = { 3,
    3,
    true },
    interval_above_threshold = { 4,
    3,
    true },
    interval_below_threshold = { 2,
    3,
    false },
    interval_disabled_cap = { 5,
    0,
    false }
)]
fn snapshot_interval_cap_uses_at_or_past_threshold(advanced: u64, interval: u64, want: bool) {
    assert2::assert!(snapshot_interval_reached(advanced, interval) == want);
}

#[yare::parameterized(
    bytes_exact_threshold = { 20, 20, true },
    bytes_above_threshold = { 21, 20, true },
    bytes_below_threshold = { 19, 20, false },
    bytes_disabled_cap = { u64::MAX, 0, false },
)]
fn snapshot_bytes_cap_disables_at_zero_and_triggers_at_threshold(
    bytes_since_snapshot: u64,
    max_bytes_between_snapshots: u64,
    want: bool,
) {
    assert2::assert!(
        snapshot_bytes_reached(bytes_since_snapshot, max_bytes_between_snapshots) == want
    );
}

#[yare::parameterized(
    time_exact_threshold = { 3_600_000, 3_600_000, true },
    time_above_threshold = { 3_600_001, 3_600_000, true },
    time_below_threshold = { 3_599_999, 3_600_000, false },
    time_disabled_cap = { u64::MAX, 0, false },
)]
fn snapshot_time_cap_disables_at_zero_and_triggers_at_threshold(
    elapsed_ms: u64,
    max_snapshot_interval_ms: u64,
    want: bool,
) {
    assert2::assert!(snapshot_time_reached(elapsed_ms, max_snapshot_interval_ms) == want);
}

#[yare::parameterized(
    clamp_above_log_end = { 2,
    5,
    4,
    Offset(4) },
    prevent_regression = { 2,
    1,
    4,
    Offset(2) },
    ordinary_advance = { 2,
    3,
    4,
    Offset(3) }
)]
fn expected_hwm_after_advance_is_monotonic_and_clamped_to_log_end(
    prev_hwm: i64,
    new_hwm: i64,
    log_end: i64,
    want: Offset,
) {
    assert2::assert!(
        expected_hwm_after_advance(Offset(prev_hwm), Offset(new_hwm), Offset(log_end)) == want
    );
}

#[yare::parameterized(
    exact_expected_hwm = { 4,
    4,
    true },
    beyond_expected_hwm = { 5,
    4,
    true },
    below_expected_hwm = { 3,
    4,
    false }
)]
fn applied_hwm_is_checked_against_expected_frontier(
    applied_hwm: i64,
    expected_hwm: i64,
    want: bool,
) {
    assert2::assert!(hwm_advanced_as_expected(Offset(applied_hwm), Offset(expected_hwm)) == want);
}

#[yare::parameterized(
    waiter_at_hwm = { 5,
    5,
    true },
    waiter_below_hwm = { 6,
    5,
    true },
    waiter_above_hwm = { 4,
    5,
    false }
)]
fn waiter_resolution_requires_hwm_to_reach_need_offset(hwm: i64, need_offset: i64, want: bool) {
    assert2::assert!(hwm_reaches_waiter(Offset(hwm), Offset(need_offset)) == want);
}

#[yare::parameterized(
    first_committed_offset = { 0, 1, true, true },
    last_committed_offset = { 4, 5, true, true },
    negative_offset = { -1, 5, false, true },
    exclusive_hwm_boundary = { 5, 5, false, false },
)]
fn metadata_fetch_window_is_committed_half_open_range(
    fetch_offset: i64,
    hwm: i64,
    want_window: bool,
    want_batch: bool,
) {
    assert2::assert!(
        metadata_fetch_offset_in_committed_window(Offset(fetch_offset), Offset(hwm)) == want_window
    );
    assert2::assert!(fetch_batch_committed_before_hwm(fetch_offset, Offset(hwm)) == want_batch);
}

#[yare::parameterized(
    first_available_record = { 0, 1, true },
    last_available_record = { 4, 5, true },
    negative_offset = { -1, 5, false },
    exclusive_log_end_boundary = { 5, 5, false },
)]
fn fetch_record_offsets_are_inside_log_window_only(fetch_offset: i64, log_end: i64, want: bool) {
    assert2::assert!(fetch_offset_has_records(Offset(fetch_offset), Offset(log_end)) == want);
}
