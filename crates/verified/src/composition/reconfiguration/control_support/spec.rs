use {
    super::super::spec::{expected_member, grant_count, has_node, membership_matches_change},
    crate::reconfiguration::{
        CurrentVoterSet, ReconfigurationState, TargetVoter, VoterChangeKind, VoterChangeRequest,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn control_record_count(version: u16, kind: VoterChangeKind) -> Int {
    pearlite! { match kind {
        VoterChangeKind::FinalizeKraftVersion => 2,
        VoterChangeKind::Update => if version@ == 0 { 0 } else { 1 },
        VoterChangeKind::Add | VoterChangeKind::Remove => 1,
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn next_size(count: Int, kind: VoterChangeKind) -> Int {
    pearlite! { match kind { VoterChangeKind::Add => count + 1,
    VoterChangeKind::Remove => count - 1, _ => count } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Count reports reaching the actual exclusive batch end, only for real IDs.
#[variant(count)]
pub fn prefix_count(
    old: Seq<u64>,
    reports: Seq<(i64, i64)>,
    kind: VoterChangeKind,
    node: u64,
    end: Int,
    new: bool,
    count: Int,
) -> Int {
    pearlite! { if count <= 0 { 0 } else {
        prefix_count(old, reports, kind, node, end, new, count - 1)
        + if count - 1 < old.len() {
            if new { if reports[count - 1].1@ >= end
                && expected_member(old, old.len(), kind, node, old[count - 1]) { 1 } else { 0 } }
            else { if reports[count - 1].0@ >= end { 1 } else { 0 } }
        } else { if new && reports[count - 1].1@ >= end
            && !has_node(old, old.len(), node)
            && expected_member(old, old.len(), kind, node, node) { 1 } else { 0 } }
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Both voter sets must report the actual exclusive control-batch end.
pub(super) fn control_prefix_majorities(
    old: Seq<u64>,
    reports: Seq<(i64, i64)>,
    kind: VoterChangeKind,
    node: u64,
    end: Int,
) -> bool {
    pearlite! {
        prefix_count(old, reports, kind, node, end, false, reports.len()) >= old.len() / 2 + 1
        && prefix_count(old, reports, kind, node, end, true, reports.len()) >= next_size(old.len(), kind) / 2 + 1
    }
}

/// Bridge actual prefix reports to the previous composition's grant ledger.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count <= votes.len() && votes.len() == reports.len())]
#[requires(forall<i: Int> 0 <= i && i < votes.len() ==>
    votes[i].0 == (reports[i].0@ >= end) && votes[i].1 == (reports[i].1@ >= end))]
#[requires(membership_matches_change(old, next, kind, node))]
#[ensures(grant_count(old, next, votes, node, new, count)
    == prefix_count(old, reports, kind, node, end, new, count))]
#[variant(count)]
pub fn prefix_grants_agree(
    old: Seq<u64>,
    next: Seq<u64>,
    votes: Seq<(bool, bool)>,
    reports: Seq<(i64, i64)>,
    kind: VoterChangeKind,
    node: u64,
    end: Int,
    new: bool,
    count: Int,
) {
    if count > 0 {
        prefix_grants_agree(old, next, votes, reports, kind, node, end, new, count - 1);
    }
}

#[logic(open)]
pub fn control_request_admitted(
    old: Seq<u64>,
    state: ReconfigurationState,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
) -> bool {
    pearlite! {
        super::super::spec::valid_old(old)
        && super::super::spec::membership_coherent(old, node, target.membership, request.kind)
        && match crate::reconfiguration::voter_reconfiguration_rejection(state.0, state.1, request, target) {
            Some(_) => false, None => true,
        }
    }
}

#[logic(open)]
pub(super) fn control_inputs_coherent(
    old: Seq<u64>,
    context: CurrentVoterSet,
    reports: Seq<(i64, i64)>,
) -> bool {
    pearlite! { context.voter_count@ == old.len() && reports.len() == old.len() + 1 }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The common voter belongs to both sets and reports the actual control prefix.
pub(super) fn common_prefix_reported(
    old: Seq<u64>,
    reports: Seq<(i64, i64)>,
    next: Seq<u64>,
    common: u64,
    end: Int,
) -> bool {
    pearlite! { exists<i: Int> 0 <= i && i < old.len() && old[i] == common
    && reports[i].0@ >= end && reports[i].1@ >= end
    && has_node(next, next.len(), common) }
}

/// Preflight needs admission alone; a writing request also needs prefix support.
#[logic(open)]
pub(super) fn control_supported_request(
    old: Seq<u64>,
    state: ReconfigurationState,
    request: VoterChangeRequest,
    candidate: (u64, TargetVoter),
    support: bool,
) -> bool {
    pearlite! {
        control_request_admitted(old, state, request, candidate.0, candidate.1)
        && (control_record_count(state.1.kraft_version, request.kind) == 0 || support)
    }
}
