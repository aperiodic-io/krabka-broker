//! Share-group state pruning and offset-mutation decisions.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, invariant};

/// Admission result for an administrative share-offset mutation.
#[krabka_macros::model_type(runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub enum ShareOffsetMutationDecision {
    NotCoordinator,
    NonEmptyGroup,
    Unrequested,
    FencedLeaderEpoch,
    StateEpochOverflow,
    ExactRetry,
    Apply { next_state_epoch: i32 },
}

/// The ordered coordinator, membership, request, and retry gate established
/// by the host before the epoch checks.
#[krabka_macros::model_type(runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub enum ShareOffsetMutationGate {
    NotCoordinator,
    NonEmptyGroup,
    Unrequested,
    Admissible { exact_retry: bool },
}

/// Admit a requested share-offset mutation and choose its next state epoch.
///
/// The caller supplies the data-partition leader epoch captured while it
/// planned the request and a fresh value read immediately before mutation.
/// An exact retry is a no-op. Every other admitted mutation advances the
/// durable share-state epoch by exactly one, or fails closed at `i32::MAX`.
#[must_use]
#[ensures(match result {
    ShareOffsetMutationDecision::Apply { next_state_epoch } => {
        gate == ShareOffsetMutationGate::Admissible { exact_retry: false }
            && observed_leader_epoch@ == current_leader_epoch@
            && state_epoch@ < i32::MAX@
            && next_state_epoch@ == state_epoch@ + 1
    }
    ShareOffsetMutationDecision::NotCoordinator => {
        gate == ShareOffsetMutationGate::NotCoordinator
    }
    ShareOffsetMutationDecision::NonEmptyGroup => {
        gate == ShareOffsetMutationGate::NonEmptyGroup
    }
    ShareOffsetMutationDecision::Unrequested => gate == ShareOffsetMutationGate::Unrequested,
    ShareOffsetMutationDecision::FencedLeaderEpoch => {
        (exists<retry: bool> gate == ShareOffsetMutationGate::Admissible { exact_retry: retry })
            && observed_leader_epoch@ != current_leader_epoch@
    }
    ShareOffsetMutationDecision::StateEpochOverflow => {
        gate == ShareOffsetMutationGate::Admissible { exact_retry: false }
            && observed_leader_epoch@ == current_leader_epoch@
            && state_epoch@ == i32::MAX@
    }
    ShareOffsetMutationDecision::ExactRetry => {
        gate == ShareOffsetMutationGate::Admissible { exact_retry: true }
            && observed_leader_epoch@ == current_leader_epoch@
    }
})]
pub const fn share_offset_mutation_decision(
    gate: ShareOffsetMutationGate,
    observed_leader_epoch: i32,
    current_leader_epoch: i32,
    state_epoch: i32,
) -> ShareOffsetMutationDecision {
    let exact_retry = match gate {
        ShareOffsetMutationGate::NotCoordinator => {
            return ShareOffsetMutationDecision::NotCoordinator;
        }
        ShareOffsetMutationGate::NonEmptyGroup => {
            return ShareOffsetMutationDecision::NonEmptyGroup;
        }
        ShareOffsetMutationGate::Unrequested => {
            return ShareOffsetMutationDecision::Unrequested;
        }
        ShareOffsetMutationGate::Admissible { exact_retry } => exact_retry,
    };
    if observed_leader_epoch != current_leader_epoch {
        return ShareOffsetMutationDecision::FencedLeaderEpoch;
    }
    if exact_retry {
        return ShareOffsetMutationDecision::ExactRetry;
    }
    match state_epoch.checked_add(1) {
        Some(next_state_epoch) => ShareOffsetMutationDecision::Apply { next_state_epoch },
        None => ShareOffsetMutationDecision::StateEpochOverflow,
    }
}

/// Return the minimum latest-snapshot offset across every live share key.
#[ensures(match result {
    Some(frontier) => offsets@.len() > 0
        && (forall<i: Int> 0 <= i && i < offsets@.len()
            ==> frontier@ <= offsets@[i]@)
        && (exists<i: Int> 0 <= i && i < offsets@.len()
            && frontier@ == offsets@[i]@),
    None => offsets@.len() == 0,
})]
#[must_use]
pub fn share_prune_frontier(offsets: &[i64]) -> Option<i64> {
    if matches!(offsets.len(), 0) {
        return None;
    }
    let mut frontier = offsets[0];
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= offsets@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==> frontier@ <= offsets@[k]@)]
    #[invariant(exists<k: Int> 0 <= k && k < i@ && frontier@ == offsets@[k]@)]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if offsets[i] < frontier {
            frontier = offsets[i];
        }
        i += 1;
    }
    Some(frontier)
}

#[cfg(test)]
mod tests {
    use super::{
        ShareOffsetMutationDecision, ShareOffsetMutationGate, share_offset_mutation_decision,
        share_prune_frontier,
    };

    #[test]
    fn offset_mutation_fails_closed_and_advances_exactly_once() {
        use ShareOffsetMutationDecision::{
            Apply, ExactRetry, FencedLeaderEpoch, NonEmptyGroup, NotCoordinator,
            StateEpochOverflow, Unrequested,
        };

        for (facts, expected) in [
            (
                (ShareOffsetMutationGate::NotCoordinator, 3, 3, 7),
                NotCoordinator,
            ),
            (
                (ShareOffsetMutationGate::NonEmptyGroup, 3, 3, 7),
                NonEmptyGroup,
            ),
            ((ShareOffsetMutationGate::Unrequested, 3, 3, 7), Unrequested),
            (
                (
                    ShareOffsetMutationGate::Admissible { exact_retry: false },
                    2,
                    3,
                    7,
                ),
                FencedLeaderEpoch,
            ),
            (
                (
                    ShareOffsetMutationGate::Admissible { exact_retry: false },
                    3,
                    3,
                    i32::MAX,
                ),
                StateEpochOverflow,
            ),
            (
                (
                    ShareOffsetMutationGate::Admissible { exact_retry: true },
                    3,
                    3,
                    7,
                ),
                ExactRetry,
            ),
            (
                (
                    ShareOffsetMutationGate::Admissible { exact_retry: false },
                    3,
                    3,
                    7,
                ),
                Apply {
                    next_state_epoch: 8,
                },
            ),
        ] {
            let (gate, observed, current, state) = facts;
            assert2::check!(
                share_offset_mutation_decision(gate, observed, current, state) == expected
            );
        }
    }

    #[test]
    fn prune_frontier_covers_empty_single_ties_and_missing_snapshots() {
        for (offsets, expected) in [
            (&[][..], None),
            (&[42][..], Some(42)),
            (&[100, 30, 75, 30, 200][..], Some(30)),
            (&[0, 5, 9][..], Some(0)),
            (&[i64::MAX, i64::MIN, 0][..], Some(i64::MIN)),
        ] {
            assert2::check!(share_prune_frontier(offsets) == expected);
        }
    }
}
