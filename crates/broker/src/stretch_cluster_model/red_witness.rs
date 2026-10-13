//! The RED witness: a deliberately pre-witness controller decision, and the
//! two `#[should_panic]` tests that prove the model can fail.
//!
//! A model that no configuration can break is not evidence, so the broken
//! controller and the runs that expose it stay together in one file.

use {
    super::{
        config::StretchModel,
        election::production_elect,
        runner::{
            PINNED_UNIQUE_STATES_RED_LEGACY_ELECT, PINNED_UNIQUE_STATES_RED_MIN_INSYNC_ONE, run,
        },
    },
    crate::{
        config_keys::RecoveryStrategy, elr::state::PartitionElr, leader_election::FailoverDecision,
    },
    krabka_metadata::PartitionRecord,
    krabka_raft::NodeId,
    std::collections::HashSet,
};

/// The pre-witness controller decision. It is the same shape as
/// [`failover_one`], and it takes the first alive in-sync member with no
/// witness filter. Leadership can then land on a node that serves no client.
fn legacy_elect(
    record: &PartitionRecord,
    dead: NodeId,
    alive: &HashSet<NodeId>,
    _witnesses: &HashSet<NodeId>,
    _elr: &PartitionElr,
    _strategy: RecoveryStrategy,
    _unclean_enabled: bool,
) -> FailoverDecision {
    let alive_isr: Vec<NodeId> = record
        .isr
        .iter()
        .filter(|node| **node != dead && alive.contains(node))
        .copied()
        .collect();
    if record.leader == dead {
        // BUG: no witness filter on the leader pick.
        return match alive_isr.first().copied() {
            Some(leader) => FailoverDecision::Elect {
                leader,
                isr: alive_isr,
                unclean: false,
            },
            None => FailoverDecision::Unavailable,
        };
    }
    if alive_isr.len() < record.isr.len() {
        return FailoverDecision::ShrinkIsr { isr: alive_isr };
    }
    FailoverDecision::NoChange
}

#[test]
#[should_panic(expected = "leader_never_witness")]
fn red_witness_unaware_election_elects_a_witness() {
    // Both data sites go down. The witness is then the only alive in-sync
    // member, and the pre-witness pick hands it leadership. The real
    // `failover_one` answers `Unavailable` there instead.
    run(
        StretchModel::three_sites(2, legacy_elect),
        "red_legacy_elect",
        PINNED_UNIQUE_STATES_RED_LEGACY_ELECT,
    );
}

#[test]
#[should_panic(expected = "minority_never_commits")]
fn red_min_insync_one_commits_in_a_minority() {
    // `min.insync.replicas=1` lets a lone surviving replica commit an
    // `acks=all` write while its site holds one voter of three. The value 2 is
    // what keeps every commit across two of the three sites, which is a voter
    // majority. This proves that `minority_never_commits` is a real gate and
    // not a tautology of the model.
    run(
        StretchModel::three_sites(1, production_elect),
        "red_min_insync_one",
        PINNED_UNIQUE_STATES_RED_MIN_INSYNC_ONE,
    );
}
