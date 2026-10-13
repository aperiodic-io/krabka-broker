#[cfg(creusot)]
use super::{Int, invariant, logic};
use {
    super::{LocalRecoveryStep, LocalRecoverySwapAction, LocalRecoverySwapFacts},
    crate::restore::restore_batch_step,
    creusot_std::prelude::*,
};

open_logic! {
/// A `.log.swap` is authoritative once it exists without its `.log.cleaned`.
///
/// Kafka's `LocalLog.replaceSegments` renames `.cleaned` to `.swap` before it
/// deletes a single old segment, and `LogLoader.load` completes every such
/// swap rather than discarding it, because the old segments may be gone.
pub fn swap_committed(facts: LocalRecoverySwapFacts) -> bool {
    pearlite! { facts.log_swap_exists && !facts.log_cleaned_exists }
}
}

/// Classify one swap set: abort it only while it is uncommitted, and complete
/// it whenever it is committed, whether or not the original log still exists.
///
/// Aborting is safe only because the host deletes no replaced segment before
/// the `.cleaned`-to-`.swap` rename of the log is durable (host
/// responsibility: `atomic_swap` fsyncs the directory between the two steps).
#[ensures((result == LocalRecoverySwapAction::AbortSwap) == facts.log_cleaned_exists)]
#[ensures((result == LocalRecoverySwapAction::CompleteSwap) == swap_committed(facts))]
#[ensures((result == LocalRecoverySwapAction::PromoteSidecars)
    == (!facts.log_cleaned_exists && !facts.log_swap_exists && facts.final_log_exists))]
#[ensures((result == LocalRecoverySwapAction::Reject)
    == (!facts.log_cleaned_exists && !facts.log_swap_exists && !facts.final_log_exists))]
#[must_use]
pub const fn local_recovery_swap_action(facts: LocalRecoverySwapFacts) -> LocalRecoverySwapAction {
    if facts.log_cleaned_exists {
        LocalRecoverySwapAction::AbortSwap
    } else if facts.log_swap_exists {
        LocalRecoverySwapAction::CompleteSwap
    } else if facts.final_log_exists {
        LocalRecoverySwapAction::PromoteSidecars
    } else {
        LocalRecoverySwapAction::Reject
    }
}

/// Whether completing a swap based at `swap_base`, whose records end before
/// `swap_next`, replaces the segment based at `segment_base`.
///
/// This is `LogLoader.load`'s second pass: a segment whose base lies in
/// `[swap_base, swap_next)` was compacted into the swap and is deleted. The
/// swap's own base is always replaced, even by an empty swap, because the
/// swap is renamed onto it.
#[ensures(result == (segment_base@ == swap_base@
    || (swap_base@ < segment_base@ && segment_base@ < swap_next@)))]
#[must_use]
pub const fn local_recovery_swap_replaces(
    swap_base: i64,
    swap_next: i64,
    segment_base: i64,
) -> bool {
    segment_base == swap_base || (swap_base < segment_base && segment_base < swap_next)
}

/// Validate the discovered segment bases as one strictly ordered,
/// nonnegative chain.
#[ensures(result == (forall<i: Int> 0 <= i && i < bases@.len() ==>
    bases@[i]@ >= 0
        && (i == 0 || bases@[i - 1]@ < bases@[i]@)))]
#[must_use]
pub fn local_recovery_segment_chain(bases: &[i64]) -> bool {
    let mut i = 0usize;
    #[invariant(i@ <= bases@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        bases@[j]@ >= 0
            && (j == 0 || bases@[j - 1]@ < bases@[j]@))]
    #[variant(bases@.len() - i@)]
    while i < bases.len() {
        if bases[i] < 0 || (i > 0 && bases[i - 1] >= bases[i]) {
            return false;
        }
        i += 1;
    }
    true
}

/// Close a sealed segment exactly one offset before the next segment base.
#[ensures(match result {
    Some(last) => base@ >= 0 && next_base@ > base@ && last@ == next_base@ - 1,
    None => base@ < 0 || next_base@ <= base@,
})]
#[must_use]
pub fn local_recovery_sealed_last(base: i64, next_base: i64) -> Option<i64> {
    if base < 0 || next_base <= base {
        None
    } else {
        Some(next_base - 1)
    }
}

/// Admit one completely decoded batch into the maximal valid tail prefix.
/// Compaction gaps are allowed, but overlap, empty byte progress, file-bound
/// overflow, malformed offsets, and signed offset overflow stop the prefix.
#[ensures(match result {
    Some(step) => position@ <= file_end@
        && encoded_len@ > 0
        && step.valid_end@ == position@ + encoded_len@
        && step.valid_end@ <= file_end@
        && batch_base@ >= expected_offset@
        && last_offset_delta@ >= 0
        && step.last_offset@ == batch_base@ + last_offset_delta@
        && step.next_offset@ == step.last_offset@ + 1
        && step.next_offset@ > expected_offset@,
    None => position@ > file_end@
        || encoded_len@ == 0
        || position@ + encoded_len@ > u64::MAX@
        || position@ + encoded_len@ > file_end@
        || batch_base@ < expected_offset@
        || last_offset_delta@ < 0
        || batch_base@ + last_offset_delta@ > i64::MAX@
        || batch_base@ + last_offset_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn local_recovery_batch_step(
    position: u64,
    file_end: u64,
    expected_offset: i64,
    batch_base: i64,
    last_offset_delta: i32,
    encoded_len: u64,
) -> Option<LocalRecoveryStep> {
    if position > file_end || encoded_len == 0 {
        return None;
    }
    let valid_end = position.checked_add(encoded_len)?;
    if valid_end > file_end {
        return None;
    }
    let next_offset = restore_batch_step(expected_offset, batch_base, last_offset_delta)?;
    Some(LocalRecoveryStep {
        valid_end,
        last_offset: next_offset - 1,
        next_offset,
    })
}

/// The exclusive frontier of a recovered segment, relative to its base, when
/// it fits Kafka's 32-bit relative-offset index.
///
/// The host uses the result as an admission check on tail recovery: a segment
/// whose recovered records reach past the `u32` index range is corrupt. The
/// frontier is returned as `i64` so that one executable body is the one
/// Creusot proves; its range is what the contract pins.
#[ensures(match result {
    Some(frontier) => segment_base@ >= 0
        && last_offset@ >= segment_base@ - 1
        && frontier@ == last_offset@ + 1 - segment_base@
        && 0 <= frontier@
        && frontier@ <= u32::MAX@,
    None => segment_base@ < 0
        || last_offset@ < segment_base@ - 1
        || last_offset@ + 1 - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn local_recovery_index_frontier(segment_base: i64, last_offset: i64) -> Option<i64> {
    if segment_base < 0 || last_offset < segment_base - 1 {
        return None;
    }
    // `segment_base >= 0`, so the subtraction cannot overflow, and the
    // increment happens only once the result is inside the `u32` range.
    let relative_last = last_offset - segment_base;
    if relative_last >= i64::from(u32::MAX) {
        None
    } else {
        Some(relative_last + 1)
    }
}
