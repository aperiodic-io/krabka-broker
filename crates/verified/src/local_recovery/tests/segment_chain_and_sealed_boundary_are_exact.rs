use super::*;

#[test]
fn segment_chain_and_sealed_boundary_are_exact() {
    assert2::check!(local_recovery_segment_chain(&[]));
    assert2::check!(local_recovery_segment_chain(&[0, 10, i64::MAX]));
    assert2::check!(!local_recovery_segment_chain(&[-1, 10]));
    assert2::check!(!local_recovery_segment_chain(&[0, 10, 10]));
    assert2::check!(local_recovery_sealed_last(0, 10) == Some(9));
    assert2::check!(local_recovery_sealed_last(5, 10) == Some(9));
    assert2::check!(local_recovery_sealed_last(-1, 10) == None);
    assert2::check!(local_recovery_sealed_last(10, 10) == None);
}

/// Every directory state a crash can leave at a swap's base, named by the
/// compaction step it interrupted.
#[yare::parameterized(
    rewrite_in_flight = { true, false, true, LocalRecoverySwapAction::AbortSwap },
    stale_cleaned_log = { true, true, true, LocalRecoverySwapAction::AbortSwap },
    cleaned_log_without_original = { true, false, false, LocalRecoverySwapAction::AbortSwap },
    cleaned_and_swap_without_original = { true, true, false, LocalRecoverySwapAction::AbortSwap },
    committed_originals_untouched = { false, true, true, LocalRecoverySwapAction::CompleteSwap },
    committed_original_deleted = { false, true, false, LocalRecoverySwapAction::CompleteSwap },
    promoted_log_sidecars_pending = {
        false,
        false,
        true,
        LocalRecoverySwapAction::PromoteSidecars,
    },
    no_log = { false, false, false, LocalRecoverySwapAction::Reject },
)]
fn swap_action_completes_every_committed_swap(
    log_cleaned_exists: bool,
    log_swap_exists: bool,
    final_log_exists: bool,
    expected: LocalRecoverySwapAction,
) {
    let facts = LocalRecoverySwapFacts {
        log_cleaned_exists,
        log_swap_exists,
        final_log_exists,
    };
    assert2::check!(local_recovery_swap_action(facts) == expected);
}

/// A swap at 0 whose records end before 20 replaces the segments at 0 and
/// 10, and never the next segment at 20 or one below its base.
#[yare::parameterized(
    base_segment = { 0, 20, 0, true },
    interior_segment = { 0, 20, 10, true },
    last_before_next = { 0, 20, 19, true },
    next_segment_excluded = { 0, 20, 20, false },
    earlier_segment_excluded = { 10, 20, 0, false },
    empty_range_base = { 10, 10, 10, true },
    after_empty_range = { 10, 10, 11, false },
)]
fn swap_replaces_exactly_the_segments_it_covers(
    swap_base: i64,
    swap_next: i64,
    segment_base: i64,
    expected: bool,
) {
    assert2::check!(local_recovery_swap_replaces(swap_base, swap_next, segment_base) == expected);
}

#[test]
fn batch_step_keeps_only_bounded_progress() {
    let step = local_recovery_batch_step(100, 200, 10, 12, 2, 50).unwrap();
    assert2::check!(step.valid_end == 150);
    assert2::check!(step.last_offset == 14);
    assert2::check!(step.next_offset == 15);
    assert2::check!(local_recovery_batch_step(100, 150, 10, 12, 2, 50).is_some());
    assert2::check!(local_recovery_batch_step(100, 200, 10, 10, 0, 0) == None);
    assert2::check!(local_recovery_batch_step(100, 149, 10, 12, 2, 50) == None);
    assert2::check!(local_recovery_batch_step(100, 200, 13, 12, 2, 50) == None);
    assert2::check!(local_recovery_batch_step(100, 200, 10, 10, -1, 50) == None);
    assert2::check!(local_recovery_batch_step(u64::MAX, u64::MAX, 0, 0, 0, 1) == None);
    assert2::check!(local_recovery_batch_step(0, u64::MAX, i64::MAX, i64::MAX, 0, 1) == None);
}

#[yare::parameterized(
    negative_base = { -1, 0, None },
    zero_single_record = { 0, 0, Some(1) },
    empty_at_base = { 10, 9, Some(0) },
    ordinary_range = { 10, 20, Some(11) },
    before_empty_range = { 10, 8, None },
    signed_overflow = { 0, i64::MAX, None },
    largest_frontier = { 0, i64::from(u32::MAX) - 1, Some(i64::from(u32::MAX)) },
    first_too_large_frontier = { 0, i64::from(u32::MAX), None },
    shifted_largest_frontier = { 5, i64::from(u32::MAX) + 4, Some(i64::from(u32::MAX)) },
    shifted_too_large_frontier = { 5, i64::from(u32::MAX) + 5, None },
)]
fn index_frontier_covers_empty_boundary_and_overflow(
    segment_base: i64,
    last_offset: i64,
    expected: Option<i64>,
) {
    assert2::check!(local_recovery_index_frontier(segment_base, last_offset) == expected);
}
