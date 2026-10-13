//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (`select_leader`). See
//! `crates/broker/docs/replication-isr-design.md`.
//!
//! Each failover configuration also runs with a data-bearing witness in the
//! replica set. The witness stays in every emitted ISR, and no reachable state
//! has a witness leader.
//!
//! The failover search moves brokers down and up, runs the real
//! `failover_one` over the partition, and re-admits a revived follower to the
//! ISR through the real leader-side and controller-side ISR decisions, so a
//! partition does not stay at the singleton ISR its first out-of-ISR election
//! left. After every change it recomputes the published KIP-966
//! eligible-leader set with the real maintenance rule and hands that set to
//! the next `failover_one`. Every configuration checks that each decision
//! took the highest rung the pre-state offered, so neither the offset-aware
//! recovery nor the KIP-841 election is chosen while a lossless rung was open,
//! and that it removed only the dead broker from the ISR, as Kafka's
//! `handleBrokerFenced` does. No reachable state has a leader outside its ISR,
//! even when a follower's failover runs while the leader is down and its own
//! failover has not.
//! At `min.insync.replicas` 1 the rule publishes a set only for an ISR that
//! has emptied outright; the `failover_*elr*` configurations run at 2, where a
//! replica leaves the ISR into the set in the ordinary course, and witness
//! that the ELR rung is taken.
//!
//! The winner-selection model runs once per published eligible-leader set, and
//! checks the ordering the KIP-966 rule imposes: which replica is elected out
//! of which group, and whether the election reports itself as losing data. It
//! holds no logs, so it cannot check that the replica so elected really is
//! complete -- that claim is about ELR maintenance, and `data_path_model`'s
//! `data_elr` configuration is where it is checked. See [`recovery_model`] for
//! the split.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! `within_boundary` + `target_state_count` fence each run. You MUST run these
//! models under the host memory watchdog while you tune the bounds.
//!
//! # Module layout
//!
//! This file is the module root. It holds the checked configurations, one per
//! test. Each child holds one concern: [`failover_state`] the failover search
//! space and its projection onto a `PartitionRecord`, [`elr`] the seam onto
//! the real ELR maintenance rule, [`decision`] the
//! safety invariants of one `failover_one` result, [`failover_model`] the
//! stateright [`Model`](stateright::Model) implementation that drives them,
//! [`recovery_state`] and [`recovery_model`] the same pair for KIP-966 winner
//! selection, and [`runner`] the checker bounds.

// This root is itself reached through a `#[path]` declaration in
// `leader_election`, which makes it a module-directory owner, so a bare
// `mod child;` would resolve against `src/` instead of this file's stem
// directory. Each child therefore names its file explicitly.
#[path = "leader_failover_model/decision.rs"]
mod decision;
#[path = "leader_failover_model/elr.rs"]
mod elr;
#[path = "leader_failover_model/failover_model.rs"]
mod failover_model;
#[path = "leader_failover_model/failover_state.rs"]
mod failover_state;
#[path = "leader_failover_model/recovery_model.rs"]
mod recovery_model;
#[path = "leader_failover_model/recovery_state.rs"]
mod recovery_state;
#[path = "leader_failover_model/runner.rs"]
mod runner;

use {
    self::{
        failover_state::FailoverModel,
        recovery_state::RecoveryModel,
        runner::{
            PINNED_UNIQUE_STATES_ELR_RECOVER, PINNED_UNIQUE_STATES_ELR_UNCLEAN,
            PINNED_UNIQUE_STATES_FAILOVER_RECOVER, PINNED_UNIQUE_STATES_FAILOVER_SAFE,
            PINNED_UNIQUE_STATES_FAILOVER_UNCLEAN, PINNED_UNIQUE_STATES_OFFSET_RECOVERY,
            PINNED_UNIQUE_STATES_WITNESS_ELR_UNCLEAN, PINNED_UNIQUE_STATES_WITNESS_RECOVER,
            PINNED_UNIQUE_STATES_WITNESS_SAFE, PINNED_UNIQUE_STATES_WITNESS_UNCLEAN, run_failover,
            run_recovery,
        },
    },
    crate::config_keys::RecoveryStrategy,
};

/// The three-site stretch shape: replica 2 is a data-bearing witness. It is
/// not `replicas[0]`, so the initial leader is a data replica.
const WITNESS_REPLICA: [u64; 1] = [2];

/// `min.insync.replicas` of 1, Kafka's default: the KIP-966 rule clears the
/// eligible-leader set on every change that leaves an ISR member, so the set
/// is published only once the ISR has emptied.
const NO_ELR: usize = 1;

/// `min.insync.replicas` of 2 on three replicas: the smallest configuration in
/// which a replica can leave an ISR that is about to fall below min ISR, which
/// is how KIP-966 makes it eligible.
const ELR: usize = 2;

#[test]
fn failover_safe() {
    // unclean disabled: a clean election (or unavailability) is the only path;
    // the decision asserts guarantee no out-of-ISR election ever happens.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false, &[], NO_ELR),
        "failover_safe",
        PINNED_UNIQUE_STATES_FAILOVER_SAFE,
    );
}

#[test]
fn failover_unclean() {
    // KIP-841: out-of-ISR election permitted when ISR is empty.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &[], NO_ELR),
        "failover_unclean",
        PINNED_UNIQUE_STATES_FAILOVER_UNCLEAN,
    );
}

#[test]
fn failover_recover() {
    // KIP-966: empty-ISR leader death defers to offset-aware recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false, &[], NO_ELR),
        "failover_recover",
        PINNED_UNIQUE_STATES_FAILOVER_RECOVER,
    );
}

#[test]
fn failover_witness_safe() {
    // Same as `failover_safe`, with a witness in the replica set. Leadership
    // skips the witness, and the witness stays in the ISR.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false, &WITNESS_REPLICA, NO_ELR),
        "failover_witness_safe",
        PINNED_UNIQUE_STATES_WITNESS_SAFE,
    );
}

#[test]
fn failover_witness_unclean() {
    // The KIP-841 out-of-ISR pick must skip the witness too.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &WITNESS_REPLICA, NO_ELR),
        "failover_witness_unclean",
        PINNED_UNIQUE_STATES_WITNESS_UNCLEAN,
    );
}

#[test]
fn failover_witness_recover() {
    // With a witness present, an ISR that holds only live witnesses is
    // `Unavailable`, and only a truly empty ISR reaches KIP-966 recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false, &WITNESS_REPLICA, NO_ELR),
        "failover_witness_recover",
        PINNED_UNIQUE_STATES_WITNESS_RECOVER,
    );
}

#[test]
fn failover_elr_unclean() {
    // KIP-966: with the live ISR empty a surviving ELR member is elected
    // cleanly, ahead of the KIP-841 election, which is taken only once no ELR
    // member can lead.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &[], ELR),
        "failover_elr_unclean",
        PINNED_UNIQUE_STATES_ELR_UNCLEAN,
    );
}

#[test]
fn failover_elr_recover() {
    // The ELR rung also outranks the offset-aware recovery, which may pick an
    // incomplete log.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false, &[], ELR),
        "failover_elr_recover",
        PINNED_UNIQUE_STATES_ELR_RECOVER,
    );
}

#[test]
fn failover_witness_elr_unclean() {
    // A witness can be published as eligible -- it leaves an under-min-ISR set
    // like any replica -- and is still never elected out of it.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true, &WITNESS_REPLICA, ELR),
        "failover_witness_elr_unclean",
        PINNED_UNIQUE_STATES_WITNESS_ELR_UNCLEAN,
    );
}

/// KIP-966 winner selection, over every published eligible-leader set the
/// three-replica partition can have, with and without a witness among them.
///
/// The ELR is not part of the search state -- it was published before the poll
/// began -- so it is swept here instead: each subset of `{1,2,3}` is one
/// exhaustive run of the response fan-out, including the sets that name a
/// replica which never answers and the ones whose only member is the witness.
#[test]
fn offset_recovery() {
    for published in 0u8..8 {
        let eligible: Vec<i32> = (1..=3)
            .filter(|id| published & (1 << (id - 1)) != 0)
            .collect();
        for witness_ids in [&[][..], &WITNESS_REPLICA[..]] {
            let label = format!("offset_recovery elr={eligible:?} witnesses={witness_ids:?}");
            // Every published-ELR subset explores the same response fan-out,
            // so all 16 configs share one pinned count.
            run_recovery(
                RecoveryModel::offset_recovery(&eligible, witness_ids),
                &label,
                PINNED_UNIQUE_STATES_OFFSET_RECOVERY,
            );
        }
    }
}
