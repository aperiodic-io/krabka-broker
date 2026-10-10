use creusot_std::prelude::*;

#[cfg(creusot)]
use super::{count_ge, count_ge_prefix, hwm_member_at, lemma_hwm_member_maximal};

open_logic! {
/// The current watermark and follower acknowledgements fit within the log.
fn watermark_inputs_bounded(current: Int, end: Int, followers: Seq<i64>) -> bool {
    pearlite! {
        current <= end && (forall<k: Int> 0 <= k && k < followers.len() ==> followers[k]@ <= end)
    }
}
}

#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[ensures(result == (count_ge(log_end@, follower_offsets@, cand@, leader_counts) >= majority@))]
pub(super) fn candidate_has_majority(
    log_end: i64,
    follower_offsets: &[i64],
    cand: i64,
    majority: usize,
    leader_counts: bool,
) -> bool {
    let mut count: usize = 0;
    if leader_counts && log_end >= cand && count < majority {
        count += 1;
    }

    let n = follower_offsets.len();
    let mut j = 0;
    #[invariant(j@ <= n@)]
    #[invariant({
        let seen = count_ge_prefix(log_end@, follower_offsets@, cand@, j@ + 1, leader_counts);
        count@ == if seen < majority@ { seen } else { majority@ }
    })]
    #[invariant(count@ <= majority@)]
    #[variant(n@ - j@)]
    while j < n {
        let x = follower_offsets[j];
        if x >= cand && count < majority {
            count += 1;
        }
        j += 1;
    }

    count >= majority
}

/// The HWM as the majority-th largest match offset across the leader's own log
/// end, when `leader_counts`, and every follower's acknowledged fetch offset.
///
/// The leader-completeness rule of Raft Fig.8 and KIP-595 gates this value: the
/// HWM may only advance once the majority offset is strictly past
/// `epoch_start_offset`. The HWM never regresses below `current_hwm`.
///
/// When `leader_counts` is `false` (a leader that its own `VotersRecord`
/// removed), the followers alone must be able to reach `majority`, which is
/// the second precondition.
///
/// The function computes the majority-th largest by its definition, and not by
/// a sort. That definition is the greatest member m of
/// `{log_end} U follower_offsets` with at least `majority` members >= m. Voter
/// counts are tiny, at most about 7, and the Creusot proof quantifies over a
/// loop that mirrors the definition.
#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(leader_counts || majority@ <= follower_offsets@.len())]
#[requires(watermark_inputs_bounded(current_hwm@, log_end@, follower_offsets@))]
#[ensures(result@ >= current_hwm@)]
#[ensures(result@ <= log_end@)]
#[ensures(forall<v: Int> v > epoch_start_offset@
    && count_ge(log_end@, follower_offsets@, v, leader_counts) >= majority@
    ==> v <= result@)]
#[ensures(result@ > current_hwm@
    ==> result@ > epoch_start_offset@
        && count_ge(log_end@, follower_offsets@, result@, leader_counts) >= majority@)]
#[must_use]
pub fn recompute_high_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    epoch_start_offset: i64,
    current_hwm: i64,
    leader_counts: bool,
) -> i64 {
    let n = follower_offsets.len();
    let mut majority_offset = i64::MIN;
    if candidate_has_majority(log_end, follower_offsets, log_end, majority, leader_counts) {
        majority_offset = log_end;
    }

    let mut i = 0;
    #[invariant(i@ <= n@)]
    #[invariant(majority_offset@ <= log_end@)]
    #[invariant(majority_offset@ == -9223372036854775807 - 1
        || count_ge(log_end@, follower_offsets@, majority_offset@, leader_counts) >= majority@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@ + 1
        && count_ge(log_end@, follower_offsets@, hwm_member_at(log_end@, follower_offsets@, k), leader_counts) >= majority@
        ==> hwm_member_at(log_end@, follower_offsets@, k) <= majority_offset@)]
    #[variant(n@ - i@)]
    while i < n {
        let cand = follower_offsets[i];
        if cand > majority_offset
            && candidate_has_majority(log_end, follower_offsets, cand, majority, leader_counts)
        {
            majority_offset = cand;
        }
        i += 1;
    }
    #[cfg(creusot)]
    lemma_hwm_member_maximal(
        log_end,
        follower_offsets,
        majority,
        majority_offset,
        leader_counts,
    );
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}

/// The watermark a majority has acknowledged, never below `current`.
///
/// This is [`recompute_high_watermark`] with the leader's log end always
/// counted and no leader-epoch gate. The result is the greater of `current`
/// and the majority frontier: the greatest member of
/// `{log_end} U follower_offsets` that at least `majority` members reach.
/// Passing `current` as both the gate and the floor is what makes it that: the
/// frontier is taken only when it passes `current`, and `current` otherwise.
///
/// A caller that needs Raft's current-term rule, the gate on the leader's
/// first record of its own epoch, uses [`recompute_high_watermark`] instead.
#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(watermark_inputs_bounded(current@, log_end@, follower_offsets@))]
#[ensures(result@ >= current@)]
#[ensures(result@ <= log_end@)]
#[ensures(forall<v: Int> count_ge(log_end@, follower_offsets@, v, true) >= majority@
    ==> v <= result@)]
#[ensures(result@ > current@
    ==> count_ge(log_end@, follower_offsets@, result@, true) >= majority@)]
#[must_use]
pub fn majority_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    current: i64,
) -> i64 {
    recompute_high_watermark(log_end, follower_offsets, majority, current, current, true)
}
