use {
    super::{FailoverAction, FailoverFacts, FailoverRecovery, LiveIsr, RecoveryCandidate},
    creusot_std::prelude::*,
};

/// Majority size for a voter set: `floor(n / 2) + 1`.
#[ensures(result@ == voter_count@ / 2 + 1)]
#[must_use]
pub const fn majority_size(voter_count: usize) -> usize { voter_count / 2 + 1 }

/// Whether the unique grants from current voters reach a majority.
#[ensures(result == (current_grants@ >= voter_count@ / 2 + 1))]
#[must_use]
pub const fn election_has_quorum(voter_count: usize, current_grants: usize) -> bool {
    current_grants >= majority_size(voter_count)
}

/// Select the failover action class by a fixed precedence ladder.
///
/// What the contract proves is the ladder, and only the ladder. For a dead
/// leader it picks, in order: a clean election when a live ISR member can
/// lead; otherwise, and only when no ISR member is live, an election from the
/// eligible leader replicas; otherwise the last known leader; otherwise the
/// configured offset-aware recovery; otherwise the KIP-841 election when it is
/// available; otherwise the partition stays unavailable. A live ISR of
/// witnesses only stops the ladder at unavailable. For a live leader it only
/// shrinks the ISR or leaves it alone. Each outcome is pinned to exactly one
/// combination of [`FailoverFacts`].
///
/// Why each rung is safe is not part of the proof. It rests on the host's
/// classification and on Kafka's semantics: an ISR member holds every
/// committed record; a KIP-966 eligible leader replica left the ISR while the
/// partition still held `min.insync.replicas` members, so it holds every
/// committed record too, and neither the `unclean.leader.election.enable`
/// toggle nor `unclean.recovery.strategy` gates it. Its rung is reachable only
/// once the live ISR is empty, the same guard Apache Kafka's
/// `PartitionChangeBuilder.isValidNewLeader` puts on its `targetElr` disjunct.
/// The last known leader is the one replica that led when the partition lost
/// its leader, but it may have lost an unflushed tail, so electing it is an
/// unclean election that Kafka takes without either toggle: its rung sits
/// directly under the ELR one, exactly where `electAnyLeader` and
/// `electPreferredLeader` put `canElectLastKnownLeader`, ahead of the
/// KIP-841 branch.
///
/// `unclean_election_available` joins the KIP-841 toggle and the existence of
/// a replica that can serve, because the ladder never separates them: an
/// election that is allowed with nobody to elect and one that has a candidate
/// and no permission are the same unavailable partition.
#[ensures(match result {
    FailoverAction::ElectClean => facts.leader_dead && facts.live_isr == LiveIsr::Electable,
    FailoverAction::ElectFromElr => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && facts.out_of_isr.has_electable_elr,
    FailoverAction::ElectLastKnown => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && !facts.out_of_isr.has_electable_elr
        && facts.out_of_isr.last_known_leader_electable,
    FailoverAction::Recover(selected) => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && !facts.out_of_isr.has_electable_elr
        && !facts.out_of_isr.last_known_leader_electable
        && facts.out_of_isr.recovery != FailoverRecovery::None
        && selected == facts.out_of_isr.recovery,
    FailoverAction::ElectUnclean => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && !facts.out_of_isr.has_electable_elr
        && !facts.out_of_isr.last_known_leader_electable
        && facts.out_of_isr.recovery == FailoverRecovery::None
        && facts.out_of_isr.unclean_election_available,
    FailoverAction::Unavailable => facts.leader_dead
        && (facts.live_isr == LiveIsr::WitnessesOnly
            || (facts.live_isr == LiveIsr::Empty
                && !facts.out_of_isr.has_electable_elr
                && !facts.out_of_isr.last_known_leader_electable
                && facts.out_of_isr.recovery == FailoverRecovery::None
                && !facts.out_of_isr.unclean_election_available)),
    FailoverAction::ShrinkIsr => !facts.leader_dead && facts.isr_shrunk,
    FailoverAction::NoChange => !facts.leader_dead && !facts.isr_shrunk,
})]
#[must_use]
pub fn failover_action(facts: FailoverFacts) -> FailoverAction {
    if !facts.leader_dead {
        return if facts.isr_shrunk {
            FailoverAction::ShrinkIsr
        } else {
            FailoverAction::NoChange
        };
    }
    match facts.live_isr {
        LiveIsr::Electable => return FailoverAction::ElectClean,
        LiveIsr::WitnessesOnly => return FailoverAction::Unavailable,
        LiveIsr::Empty => {}
    }
    let out_of_isr = facts.out_of_isr;
    if out_of_isr.has_electable_elr {
        return FailoverAction::ElectFromElr;
    }
    if out_of_isr.last_known_leader_electable {
        return FailoverAction::ElectLastKnown;
    }
    match out_of_isr.recovery {
        FailoverRecovery::Balanced | FailoverRecovery::Aggressive => {
            FailoverAction::Recover(out_of_isr.recovery)
        }
        FailoverRecovery::None if out_of_isr.unclean_election_available => {
            FailoverAction::ElectUnclean
        }
        FailoverRecovery::None => FailoverAction::Unavailable,
    }
}

/// `a` ranks at or above `b` as a KIP-966 recovery candidate: the higher last
/// written leader epoch first, then the higher log end offset, then the lower
/// broker ID.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn ranks_ge(a: RecoveryCandidate, b: RecoveryCandidate) -> bool {
    pearlite! {
        a.last_epoch@ > b.last_epoch@
            || (a.last_epoch@ == b.last_epoch@
                && (a.log_end_offset@ > b.log_end_offset@
                    || (a.log_end_offset@ == b.log_end_offset@ && a.broker_id@ <= b.broker_id@)))
    }
}

/// Select the replica with highest `(last leader epoch, log end offset)` and
/// lowest broker ID as the deterministic tie-breaker.
///
/// The contract states that ranking once, as `ranks_ge`: the returned index
/// ranks at or above every candidate. `None` is returned exactly for an empty
/// slice.
#[ensures(match result {
    None => candidates@.len() == 0,
    Some(best) => best@ < candidates@.len()
        && forall<j: Int> 0 <= j && j < candidates@.len()
            ==> ranks_ge(candidates@[best@], candidates@[j]),
})]
#[must_use]
pub fn select_best_recovery_replica(candidates: &[RecoveryCandidate]) -> Option<usize> {
    let n = candidates.len();
    if n == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= n@)]
    #[invariant(best@ < i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> ranks_ge(candidates@[best@], candidates@[j]))]
    #[variant(n@ - i@)]
    while i < n {
        let candidate = candidates[i];
        let current = candidates[best];
        if candidate.last_epoch > current.last_epoch
            || (candidate.last_epoch == current.last_epoch
                && candidate.log_end_offset > current.log_end_offset)
            || (candidate.last_epoch == current.last_epoch
                && candidate.log_end_offset == current.log_end_offset
                && candidate.broker_id < current.broker_id)
        {
            best = i;
        }
        i += 1;
    }
    Some(best)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Members of `{log_end} U s` with value >= `v`. This is the
/// majority-replication witness.
pub fn hwm_member_at(log_end: Int, s: Seq<i64>, k: Int) -> Int {
    pearlite! {
        if k == 0 { log_end } else { s[k - 1]@ }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(limit)]
pub fn count_ge_prefix(log_end: Int, s: Seq<i64>, v: Int, limit: Int, leader_counts: bool) -> Int {
    pearlite! {
        if limit <= 0 {
            0
        } else {
            count_ge_prefix(log_end, s, v, limit - 1, leader_counts)
                + (if (leader_counts || limit > 1)
                    && hwm_member_at(log_end, s, limit - 1) >= v { 1 } else { 0 })
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
#[variant(s.len())]
pub fn count_ge(log_end: Int, s: Seq<i64>, v: Int, leader_counts: bool) -> Int {
    pearlite! { count_ge_prefix(log_end, s, v, s.len() + 1, leader_counts) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= left.len() + 1 && limit <= right.len() + 1)]
#[requires(forall<i: Int> 0 <= i && i < limit - 1
    ==> (left[i]@ >= threshold) == (right[i]@ >= threshold))]
#[ensures(count_ge_prefix(log_end, left, threshold, limit, false)
    == count_ge_prefix(log_end, right, threshold, limit, false))]
#[variant(limit)]
pub fn lemma_explicit_vote_count_equal(
    log_end: Int,
    left: Seq<i64>,
    right: Seq<i64>,
    threshold: Int,
    limit: Int,
) {
    if limit > 0 {
        lemma_explicit_vote_count_equal(log_end, left, right, threshold, limit - 1);
    }
}
