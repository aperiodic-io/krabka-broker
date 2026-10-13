use {
    super::{
        FetchWatermarks, WalCopyBatch, covering_copy_preserves_logical_fetch,
        installed_wal_quorum_bounds_fetch, wal_voter_set_valid,
    },
    creusot_std::prelude::*,
};
mod copy_vote;
use copy_vote::authenticated_copy_vote;
#[cfg(creusot)]
mod service;
#[cfg(creusot)]
mod spec;
#[cfg(creusot)]
use spec::{copy_admitted, reference_valid};

// Authenticated node, request epoch, durable completion, actual replica bytes.
type WalCopyObservation = (Option<u64>, i32, bool, Vec<WalCopyBatch>);
type ByteQuorum = (i64, i64, Vec<(u64, i64)>, Vec<i64>);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn complete_copy(
    observation: WalCopyObservation,
    voter: u64,
    epoch: i32,
    source: Seq<WalCopyBatch>,
) -> bool {
    pearlite! {
        observation.2 && observation.0 == Some(voter)
        && (observation.1@ < 0 || observation.1 == epoch)
        && observation.3@.len() == source.len()
        && (forall<j: Int> 0 <= j && j < source.len() ==>
            super::wal_copy::copy_batch_equal(observation.3@[j], source[j]))
    }
}

/// Derive explicit votes from admitted actual copies, then consume installed
/// identities, quorum recomputation and consumer Fetch. A fresh advance exposing
/// retained records carries a distinct majority of authenticated, durably
/// completed, byte-identical prefix witnesses. Invalid or incomplete copies
/// contribute only the inherited logical floor. Source decoding, current-epoch
/// coherence, accurate completion observations and actual I/O remain external.
#[requires(0 <= w.log_start@ && w.log_start@ <= w.log_end@ && current@ <= w.log_end@)]
#[ensures((result != None) == (
    reference_valid(source@, w.log_start@, w.log_end@)
    && crate::composition::wal_copy::wal_copy_byte_count(source@, source@.len()) <= u64::MAX@
    && voters@.len() == observations@.len() && voters@.len() == placement.1@ && placement.1@ > 0
    && voters@[0] == placement.0
    && crate::sequence::distinct(voters@)))]
#[ensures(match result {
    None => true,
    Some((hw, limit, _, reports)) => current@ <= hw@ && w.log_start@ <= hw@ && hw@ <= w.log_end@
        && limit@ == hw@.min(w.lso@).min(w.deliverable@) && reports@.len() == voters@.len()
        && forall<i: Int> 0 <= i && i < reports@.len() ==> reports@[i]@ ==
            if copy_admitted(voters@, voters@[i], placement.0, placement.2, source@, observations@[i]) {
                if observations@[i].3@.len() == 0 { w.log_start@ }
                else { source@[observations@[i].3@.len() - 1].0@ + source@[observations@[i].3@.len() - 1].1@ + 1 }
            } else { w.log_start@ },
})]
#[ensures(match result {
    None => true,
    Some((_, _, supporters, _)) => crate::consensus::supporting_nodes_distinct(supporters@),
})]
#[ensures(match result {
    None => true,
    Some((hw, limit, supporters, _)) =>
        (hw@ > current@ && limit@ > w.log_start@ ==> supporters@.len() >= voters@.len() / 2 + 1)
        && (crate::consensus::supporting_voter_witnesses(supporters@, voters@, limit@, |(i, j): (Int, Int)| (limit@ > w.log_start@ ==> observations@[j].2
        && observations@[j].0 == Some(voters@[j])
        && (observations@[j].1@ < 0 || observations@[j].1 == placement.2)
        && 0 < observations@[j].3@.len() && observations@[j].3@.len() <= source@.len()
        && supporters@[i].1@ == source@[observations@[j].3@.len() - 1].0@
            + source@[observations@[j].3@.len() - 1].1@ + 1
        && (forall<k: Int> 0 <= k && k < observations@[j].3@.len() ==>
            super::wal_copy::copy_batch_equal(observations@[j].3@[k], source@[k]))))),
})]
#[ensures(match result {
    None => true,
    Some((hw, _, _, reports)) => forall<v: Int> current@ < v && w.log_start@ < v && v <= w.log_end@
        && crate::consensus::count_ge(w.log_end@, reports@, v, false) >= voters@.len() / 2 + 1 ==> v <= hw@,
})]
#[ensures(match result {
    None => true,
    Some((hw, _, _, _)) => (forall<i: Int> 0 <= i && i < observations@.len() ==>
        complete_copy(observations@[i], voters@[i], placement.2, source@)) ==> hw == w.log_end,
})]
pub(super) fn durable_matching_copies_bound_fetch(
    voters: &[u64],
    observations: &[WalCopyObservation],
    placement: (u64, usize, i32), // local node, expected size, leader epoch
    source: &[WalCopyBatch],
    current: i64,
    w: FetchWatermarks,
) -> Option<ByteQuorum> {
    if voters.len() != observations.len() || !wal_voter_set_valid(voters, placement.0, placement.1)
    {
        return None;
    }
    covering_copy_preserves_logical_fetch(
        source,
        source,
        (w.log_start, w.log_start),
        w.log_end,
        0,
        u64::MAX,
        w.log_start,
    )?;
    proof_assert!(source@.subsequence(0, source@.len()) == source@);
    let mut reports: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= voters@.len() && reports@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> w.log_start@ <= reports@[j]@ && reports@[j]@ <= w.log_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> reports@[j]@ ==
        if copy_admitted(voters@, voters@[j], placement.0, placement.2, source@, observations@[j]) {
            if observations@[j].3@.len() == 0 { w.log_start@ }
            else { source@[observations@[j].3@.len() - 1].0@ + source@[observations@[j].3@.len() - 1].1@ + 1 }
        } else { w.log_start@ })]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        let report = authenticated_copy_vote(
            voters,
            voters[i],
            placement.0,
            placement.2,
            (source, w.log_start, w.log_end),
            &observations[i],
        )
        .unwrap_or(w.log_start);
        reports.push(report);
        i += 1;
    }
    proof_assert!(forall<i: Int> 0 <= i && i < observations@.len()
        && complete_copy(observations@[i], voters@[i], placement.2, source@)
        ==> reports@[i] == w.log_end);
    #[cfg(creusot)]
    proof_assert!({
        if forall<i: Int> 0 <= i && i < reports@.len() ==> reports@[i] == w.log_end {
            service::lemma_complete_votes_count(w.log_end@, reports@, w.log_end@, reports@.len());
            crate::consensus::count_ge(w.log_end@, reports@, w.log_end@, false) == reports@.len()
        } else { true }
    });
    let (hw, limit, supporters) =
        installed_wal_quorum_bounds_fetch(voters, &reports, placement.0, placement.1, current, w)?;
    Some((hw, limit, supporters, reports))
}
