use {super::LocalTruncationPlan, creusot_std::prelude::*};

/// Admit one batch at the expected logical frontier or beyond it, and compute its
/// inclusive last offset and exclusive successor without signed overflow. A base
/// past the frontier is a hole in the offsets, which a compacted log has and a
/// follower of one replicates; a base below it is a duplicate.
#[ensures(match result {
    Some((last, next)) => expected_base@ >= 0
        && supplied_base@ >= expected_base@
        && last_offset_delta@ >= 0
        && last@ == supplied_base@ + last_offset_delta@
        && next@ == last@ + 1
        && supplied_base@ <= last@
        && last@ < next@,
    None => expected_base@ < 0
        || supplied_base@ < expected_base@
        || last_offset_delta@ < 0
        || supplied_base@ + last_offset_delta@ > i64::MAX@
        || supplied_base@ + last_offset_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn local_append_coordinates(
    expected_base: i64,
    supplied_base: i64,
    last_offset_delta: i32,
) -> Option<(i64, i64)> {
    if expected_base < 0 || supplied_base < expected_base || last_offset_delta < 0 {
        return None;
    }
    let last = supplied_base.checked_add(i64::from(last_offset_delta))?;
    let next = last.checked_add(1)?;
    Some((last, next))
}

/// Keep exactly the sealed-segment prefix whose bases precede the cut and keep
/// the current active segment exactly when its base also precedes the cut.
#[requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < sealed_bases@.len() ==> sealed_bases@[i] < sealed_bases@[j])]
#[ensures(result.retained_sealed@ <= sealed_bases@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.retained_sealed@ ==>
    sealed_bases@[i]@ < cut@)]
#[ensures(forall<i: Int> result.retained_sealed@ <= i && i < sealed_bases@.len() ==>
    sealed_bases@[i]@ >= cut@)]
#[ensures(result.keep_active == match active_base {
    Some(base) => base@ < cut@,
    None => false,
})]
#[must_use]
pub fn local_truncation_plan(
    sealed_bases: &[i64],
    active_base: Option<i64>,
    cut: i64,
) -> LocalTruncationPlan {
    let mut retained_sealed = 0usize;
    #[invariant(retained_sealed@ <= sealed_bases@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < retained_sealed@ ==>
        sealed_bases@[i]@ < cut@)]
    #[variant(sealed_bases@.len() - retained_sealed@)]
    while retained_sealed < sealed_bases.len() && sealed_bases[retained_sealed] < cut {
        retained_sealed += 1;
    }
    LocalTruncationPlan {
        retained_sealed,
        keep_active: match active_base {
            Some(base) => base < cut,
            None => false,
        },
    }
}

/// Convert an absolute cut to a segment-relative offset without signed or
/// `u32` overflow. Segment bases and log cuts are nonnegative Kafka offsets.
#[ensures(match result {
    Some(relative) => segment_base@ >= 0
        && cut@ >= segment_base@
        && relative@ == cut@ - segment_base@
        && relative@ <= u32::MAX@,
    None => segment_base@ < 0
        || cut@ < segment_base@
        || cut@ - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn truncation_relative_offset(segment_base: i64, cut: i64) -> Option<u32> {
    if segment_base < 0 || cut < segment_base {
        return None;
    }
    let relative = cut.abs_diff(segment_base);
    if relative > u64::from(u32::MAX) {
        None
    } else {
        // The clamp is the identity here; it hands the cast a range the
        // compiler can see.
        Some(relative.min(0xffff_ffff) as u32)
    }
}

/// One decoded batch belongs to the exact retained prefix iff its inclusive
/// last offset is below the exclusive cut.
#[ensures(result == (batch_last@ < cut@))]
#[must_use]
pub const fn truncation_batch_retained(batch_last: i64, cut: i64) -> bool { batch_last < cut }

/// Clamp a dependent frontier to the new log end.
#[ensures(result@ <= frontier@)]
#[ensures(result@ <= new_end@)]
#[ensures(result@ == frontier@ || result@ == new_end@)]
#[must_use]
pub const fn truncation_frontier(frontier: i64, new_end: i64) -> i64 {
    if frontier < new_end {
        frontier
    } else {
        new_end
    }
}

/// A future log may replace the current log only at the exact same frontier.
#[ensures(result == (current_leo@ == future_leo@))]
#[must_use]
pub const fn future_log_swap_admission(current_leo: i64, future_leo: i64) -> bool {
    current_leo == future_leo
}
