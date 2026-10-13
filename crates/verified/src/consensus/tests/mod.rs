use {super::*, assert2::check, proptest::prelude::*};

/// The production implementation that this kernel replaced: sort
/// descending, take the majority-th largest, gate on `epoch_start`, and
/// clamp monotonic.
fn hwm_sort_oracle(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    epoch_start_offset: i64,
    current_hwm: i64,
    leader_counts: bool,
) -> i64 {
    let mut match_offsets: Vec<i64> = Vec::with_capacity(follower_offsets.len() + 1);
    if leader_counts {
        match_offsets.push(log_end);
    }
    match_offsets.extend_from_slice(follower_offsets);
    match_offsets.sort_unstable_by(|a, b| b.cmp(a));
    let majority_offset = match_offsets[majority - 1];
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}

mod leader_counts_toward_a_candidate_only_when_its_log_reaches_it;

mod failover_action_covers_clean_unclean_recovery_and_shrink_paths;
