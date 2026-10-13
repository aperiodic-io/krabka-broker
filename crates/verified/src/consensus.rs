//! KIP-595 consensus decision kernels, extracted from `krabka-kraft-core` so
//! Creusot can verify them (the host crate's `Instant`/async surface is
//! untranslatable). The functions carry their Creusot preconditions,
//! postconditions, invariants, variants, and supporting lemmas directly beside
//! the executable bodies.

use creusot_std::prelude::*;

/// Offset-aware recovery mode resolved for one partition.
#[krabka_macros::model_type]
pub enum FailoverRecovery {
    None,
    Balanced,
    Aggressive,
}

/// Safety action selected after the host classifies live ISR and replicas.
#[krabka_macros::model_type]
pub enum FailoverAction {
    ElectClean,
    ElectFromElr,
    /// The partition's single last-known leader comes back as an unclean
    /// leader (Kafka's `canElectLastKnownLeader`).
    ElectLastKnown,
    Recover(FailoverRecovery),
    ElectUnclean,
    Unavailable,
    ShrinkIsr,
    NoChange,
}

/// What the live members of the partition's ISR can do once the leader is
/// gone. Every live ISR member holds every committed record; a witness is a
/// live member that never leads.
#[krabka_macros::model_type]
pub enum LiveIsr {
    /// No ISR member is live.
    Empty,
    /// Every live ISR member is a witness, so none can lead.
    WitnessesOnly,
    /// A live ISR member that is not a witness can lead.
    Electable,
}

/// The out-of-ISR options, consulted only when no ISR member is live.
#[krabka_macros::model_type]
pub struct OutOfIsrFacts {
    /// A live KIP-966 eligible leader replica can lead.
    pub has_electable_elr: bool,
    /// Kafka's `canElectLastKnownLeader`, less the empty-ISR test that
    /// [`LiveIsr::Empty`] already carries: the target ELR is empty, the
    /// partition's `lastKnownElr` holds exactly one replica, and that replica
    /// is live and can lead.
    pub last_known_leader_electable: bool,
    /// The topic's resolved offset-aware recovery strategy.
    pub recovery: FailoverRecovery,
    /// The KIP-841 out-of-ISR election is both permitted by
    /// `unclean.leader.election.enable` and has a live replica to elect.
    pub unclean_election_available: bool,
}

/// What the host established about one partition before the failover
/// decision. The kernel does not see replica sets; every field is the host's
/// classification of them.
#[krabka_macros::model_type]
pub struct FailoverFacts {
    /// The partition's leader is the replica that went away.
    pub leader_dead: bool,
    /// The live ISR is smaller than the recorded ISR.
    pub isr_shrunk: bool,
    /// What the live ISR members can do.
    pub live_isr: LiveIsr,
    /// The options left when no ISR member is live.
    pub out_of_isr: OutOfIsrFacts,
}

/// One surviving replica's log as KIP-966 unclean recovery ranks it, from its
/// `GetReplicaLogInfo` answer.
#[krabka_macros::model_type]
pub struct RecoveryCandidate {
    /// The leader epoch of the last record the replica wrote.
    pub last_epoch: i32,
    /// The offset one past the replica's last record.
    pub log_end_offset: i64,
    /// The replica's broker id, which breaks a tie deterministically.
    pub broker_id: u64,
}

mod failover;
#[cfg(creusot)]
pub use failover::{
    count_ge, count_ge_prefix, hwm_member_at, lemma_explicit_vote_count_equal, ranks_ge,
};
pub use failover::{
    election_has_quorum, failover_action, majority_size, select_best_recovery_replica,
};

mod log_is_up_to_date;
pub use log_is_up_to_date::{election_jitter_ms, log_is_up_to_date};
#[cfg(creusot)]
pub use log_is_up_to_date::{
    least_hwm_member_ge_index, lemma_count_ge_prefix_monotone, lemma_count_ge_prefix_nonnegative,
    lemma_hwm_member_maximal, lemma_hwm_threshold_has_member,
};

mod watermark;
#[cfg(any(creusot, test))]
use watermark::candidate_has_majority;
pub use watermark::{majority_watermark, recompute_high_watermark};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every concrete supporting report belongs to a different node.
pub(crate) fn supporting_nodes_distinct(supporters: Seq<(u64, i64)>) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < j && j < supporters.len() ==> supporters[i].0 != supporters[j].0 }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The majority bound includes the leader's own slot.
pub(crate) fn valid_hwm_majority(majority: Int, members: Int) -> bool {
    pearlite! { 1 <= majority && majority <= members + 1 }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Each reported supporting frontier reaches the limit and names a voter with concrete evidence.
pub(crate) fn supporting_voter_witnesses(
    supporters: Seq<(u64, i64)>,
    voters: Seq<u64>,
    limit: Int,
    evidence: creusot_std::logic::Mapping<(Int, Int), bool>,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < supporters.len() ==> supporters[i].1@ >= limit
    && exists<j: Int> 0 <= j && j < voters.len() && supporters[i].0 == voters[j]
        && evidence.get((i, j)) }
}

#[cfg(test)]
mod tests;
