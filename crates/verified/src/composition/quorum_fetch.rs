use creusot_std::prelude::*;

use super::{FetchWatermarks, WalFetchSupport, fetch_visibility, wal_voter_set_valid};
use crate::consensus::{majority_size, recompute_high_watermark};

/// A newly advanced consensus watermark gives every consumer fetch limit
/// quorum support. An inherited, unchanged watermark needs prior-epoch evidence.
#[cfg_attr(creusot, requires(1 <= majority@ && majority@ <= followers@.len() + 1))]
#[cfg_attr(creusot, requires(leader_counts || majority@ <= followers@.len()))]
#[cfg_attr(creusot, requires(current@ <= w.log_end@))]
#[cfg_attr(creusot, requires(forall<i: Int> 0 <= i && i < followers@.len()
    ==> followers@[i]@ <= w.log_end@))]
#[cfg_attr(creusot, ensures(current@ <= result.0@ && result.0@ <= w.log_end@))]
#[cfg_attr(creusot, ensures(result.1@ <= result.0@))]
#[cfg_attr(creusot, ensures(result.1@ == result.0@.min(w.lso@).min(w.deliverable@)))]
#[cfg_attr(creusot, ensures(forall<v: Int> v > epoch_start@
    && crate::consensus::count_ge(w.log_end@, followers@, v, leader_counts) >= majority@
    ==> v <= result.0@))]
#[cfg_attr(creusot, ensures(result.0@ > current@ ==>
    crate::consensus::count_ge(w.log_end@, followers@, result.1@, leader_counts) >= majority@))]
pub(super) fn quorum_commit_bounds_fetch(
    followers: &[i64],
    majority: usize,
    epoch_start: i64,
    current: i64,
    leader_counts: bool,
    w: FetchWatermarks,
) -> (i64, i64) {
    let hw = recompute_high_watermark(
        w.log_end,
        followers,
        majority,
        epoch_start,
        current,
        leader_counts,
    );
    let limit =
        fetch_visibility(false, true, FetchWatermarks { hw, ..w }, w.log_start).limit_offset;
    #[cfg(creusot)]
    if hw > current {
        proof_assert!({
            crate::consensus::lemma_count_ge_prefix_monotone(
                w.log_end@, followers@, limit@, hw@, followers@.len() + 1, leader_counts,
            );
            crate::consensus::count_ge(w.log_end@, followers@, limit@, leader_counts) >= majority@
        });
    }
    (hw, limit)
}

/// The actual installer, explicit durable votes, quorum kernel, and consumer
/// Fetch compose into a concrete set of distinct supporting nodes. A floor
/// raised only to log start exposes no retained records; an inherited watermark
/// still needs prior durability evidence. Reported offsets must name actual
/// durable prefixes of the same log, including the local node's fsynced vote.
#[requires(w.log_start@ <= w.log_end@ && current@ <= w.log_end@)]
#[ensures((result != None)
    == (voters@.len() == reported@.len() && voters@.len() == expected@ && expected@ > 0
        && voters@[0] == local_node
        && crate::sequence::distinct(voters@)))]
#[ensures(match result {
    None => true,
    Some((hw, limit, _)) => current@ <= hw@ && w.log_start@ <= hw@
        && hw@ <= w.log_end@ && limit@ == hw@.min(w.lso@).min(w.deliverable@),
})]
#[ensures(match result {
    None => true,
    Some((_, _, supporters)) => crate::consensus::supporting_nodes_distinct(supporters@),
})]
#[ensures(match result {
    None => true,
    Some((_, limit, supporters)) => crate::consensus::supporting_voter_witnesses(supporters@, voters@, limit@, |(i, j): (Int, Int)| supporters@[i].1@ == reported@[j]@.min(w.log_end@)),
})]
#[ensures(match result {
    None => true,
    Some((hw, limit, supporters)) => hw@ > current@ && limit@ > w.log_start@
        ==> supporters@.len() >= voters@.len() / 2 + 1,
})]
#[ensures(match result {
    None => true,
    Some((hw, _, _)) => forall<v: Int> current@ < v && w.log_start@ < v && v <= w.log_end@
        && crate::consensus::count_ge(w.log_end@, reported@, v, false) >= voters@.len() / 2 + 1
        ==> v <= hw@,
})]
pub(super) fn installed_wal_quorum_bounds_fetch(
    voters: &[u64],
    reported: &[i64],
    local_node: u64,
    expected: usize,
    current: i64,
    w: FetchWatermarks,
) -> Option<WalFetchSupport> {
    if voters.len() != reported.len() || !wal_voter_set_valid(voters, local_node, expected) {
        return None;
    }
    let mut ends: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= voters@.len() && ends@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> ends@[j]@ == reported@[j]@.min(w.log_end@))]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        ends.push(reported[i].min(w.log_end));
        i += 1;
    }
    #[cfg(creusot)]
    proof_assert!(forall<v: Int> v <= w.log_end@ ==> {
        crate::consensus::lemma_explicit_vote_count_equal(
            w.log_end@, reported@, ends@, v, ends@.len() + 1,
        );
        crate::consensus::count_ge(w.log_end@, reported@, v, false)
            == crate::consensus::count_ge(w.log_end@, ends@, v, false)
    });
    let floor = current.max(w.log_start);
    let majority = majority_size(voters.len());
    let (hw, limit) = quorum_commit_bounds_fetch(&ends, majority, floor, floor, false, w);
    let mut supporters: Vec<(u64, i64)> = Vec::new();
    i = 0;
    #[invariant(i@ <= voters@.len())]
    #[invariant(supporters@.len() <= i@)]
    #[invariant(supporters@.len()
        == crate::consensus::count_ge_prefix(w.log_end@, ends@, limit@, i@ + 1, false))]
    #[invariant(forall<j: Int> 0 <= j && j < supporters@.len()
        ==> supporters@[j].1@ >= limit@
            && exists<k: Int> 0 <= k && k < i@
                && supporters@[j].0 == voters@[k] && supporters@[j].1 == ends@[k])]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < supporters@.len()
        ==> supporters@[j].0 != supporters@[k].0)]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        if ends[i] >= limit {
            supporters.push((voters[i], ends[i]));
        }
        i += 1;
    }
    Some((hw, limit, supporters))
}
