//! KIP-595 Vote wire and membership admission decisions.

use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Whether the signed Vote fields can be converted without aliasing a Kafka
    /// sentinel to a real node or epoch.
    pub enum VoteWireDecision {
        Reject,
        Accept,
    }

    /// Whether unsigned consensus fields fit Kafka's signed Vote wire fields.
    pub enum VoteEncodeDecision {
        Reject,
        Accept,
    }
}

/// Every unsigned consensus field of a Vote fits Kafka's signed `int32` wire
/// field, so encoding it neither clamps nor wraps into another identity or
/// epoch.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn vote_fields_fit_int32(
    voter_id: u64,
    candidate_id: u64,
    candidate_epoch: u32,
    last_epoch: u32,
) -> bool {
    pearlite! {
        voter_id@ <= i32::MAX@
            && candidate_id@ <= i32::MAX@
            && candidate_epoch@ <= i32::MAX@
            && last_epoch@ <= i32::MAX@
    }
}

/// Reject values that would otherwise be clamped and alias another identity
/// or epoch when encoded as Kafka `int32` fields. The one ensures pins both
/// variants: `Accept` exactly when `vote_fields_fit_int32` holds.
#[ensures((result == VoteEncodeDecision::Accept)
    == vote_fields_fit_int32(voter_id, candidate_id, candidate_epoch, last_epoch))]
#[must_use]
pub fn vote_encode_decision(
    voter_id: u64,
    candidate_id: u64,
    candidate_epoch: u32,
    last_epoch: u32,
) -> VoteEncodeDecision {
    if voter_id > 2_147_483_647
        || candidate_id > 2_147_483_647
        || candidate_epoch > 2_147_483_647
        || last_epoch > 2_147_483_647
    {
        VoteEncodeDecision::Reject
    } else {
        VoteEncodeDecision::Accept
    }
}

/// Every signed identity and epoch field of a Vote is a real value rather than
/// a negative Kafka sentinel. Zero is a legitimate broker ID and epoch.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn vote_fields_nonnegative(
    voter_id: i32,
    candidate_id: i32,
    candidate_epoch: i32,
    last_epoch: i32,
) -> bool {
    pearlite! {
        voter_id@ >= 0 && candidate_id@ >= 0 && candidate_epoch@ >= 0 && last_epoch@ >= 0
    }
}

/// Validate the signed identity and epoch fields before conversion to the
/// unsigned consensus types. The one ensures pins both variants: `Accept`
/// exactly when `vote_fields_nonnegative` holds.
#[ensures((result == VoteWireDecision::Accept)
    == vote_fields_nonnegative(voter_id, candidate_id, candidate_epoch, last_epoch))]
#[must_use]
pub fn vote_wire_decision(
    voter_id: i32,
    candidate_id: i32,
    candidate_epoch: i32,
    last_epoch: i32,
) -> VoteWireDecision {
    if voter_id < 0 || candidate_id < 0 || candidate_epoch < 0 || last_epoch < 0 {
        VoteWireDecision::Reject
    } else {
        VoteWireDecision::Accept
    }
}

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Admission shared by binding votes and pre-votes before epoch/log checks.
    pub enum VoteAdmissionDecision {
        /// The Vote targets another voter and must be ignored without a reply.
        IgnoreWrongTarget,
        /// The target is local, but the local node or candidate lacks membership.
        Deny,
        /// The exact target and both membership requirements hold.
        Consider,
    }

    /// Whom one Vote request is addressed to, against this node.
    pub struct VoteTarget {
        /// The `VoterId` the request is addressed to.
        pub voter_id: u64,
        /// This node's ID.
        pub local_id: u64,
        /// The request's `VoterDirectoryId` names this node's directory.
        pub directory_matches: bool,
    }

    /// The cluster and voter-set memberships one Vote request needs.
    pub struct VoteMembership {
        /// The request carries no cluster ID, or this node's.
        pub cluster_matches: bool,
        /// This node is a voter.
        pub local_is_voter: bool,
        /// The candidate is a voter.
        pub candidate_is_voter: bool,
    }
}

/// Classify exact recipient and membership admission. The target alone
/// decides `IgnoreWrongTarget`; for the exact target, the memberships decide
/// between `Deny` and `Consider`.
#[ensures((result == VoteAdmissionDecision::IgnoreWrongTarget)
    == (target.voter_id@ != target.local_id@ || !target.directory_matches))]
#[ensures((result == VoteAdmissionDecision::Deny) == (
    target.voter_id@ == target.local_id@ && target.directory_matches
        && (!membership.cluster_matches
            || !membership.local_is_voter
            || !membership.candidate_is_voter)
))]
#[ensures((result == VoteAdmissionDecision::Consider) == (
    target.voter_id@ == target.local_id@ && target.directory_matches
        && membership.cluster_matches
        && membership.local_is_voter
        && membership.candidate_is_voter
))]
#[must_use]
pub fn vote_admission_decision(
    target: VoteTarget,
    membership: VoteMembership,
) -> VoteAdmissionDecision {
    if target.voter_id != target.local_id || !target.directory_matches {
        VoteAdmissionDecision::IgnoreWrongTarget
    } else if !membership.cluster_matches
        || !membership.local_is_voter
        || !membership.candidate_is_voter
    {
        VoteAdmissionDecision::Deny
    } else {
        VoteAdmissionDecision::Consider
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn wire_fields_reject_negatives_without_rejecting_zero() {
        use VoteWireDecision::{Accept, Reject};

        for (voter, candidate, epoch, last_epoch, expected) in [
            (0, 0, 0, 0, Accept),
            (-1, 0, 0, 0, Reject),
            (0, -1, 0, 0, Reject),
            (0, 0, -1, 0, Reject),
            (0, 0, 0, -1, Reject),
        ] {
            check!(vote_wire_decision(voter, candidate, epoch, last_epoch) == expected);
        }
    }

    #[test]
    fn encode_fields_reject_values_above_signed_wire_maximum() {
        use VoteEncodeDecision::{Accept, Reject};

        let max_id = 2_147_483_647_u64;
        let max_epoch = 2_147_483_647_u32;
        for (voter, candidate, epoch, last_epoch, expected) in [
            (max_id, max_id, max_epoch, max_epoch, Accept),
            (max_id + 1, 0, 0, 0, Reject),
            (0, max_id + 1, 0, 0, Reject),
            (0, 0, max_epoch + 1, 0, Reject),
            (0, 0, 0, max_epoch + 1, Reject),
        ] {
            check!(vote_encode_decision(voter, candidate, epoch, last_epoch) == expected);
        }
    }

    #[test]
    fn admission_requires_the_exact_target_and_both_memberships() {
        use VoteAdmissionDecision::{Consider, Deny, IgnoreWrongTarget};

        for (target, local, target_dir, cluster, local_voter, candidate_voter, expected) in [
            (1, 2, true, true, true, true, IgnoreWrongTarget),
            (0, 1, true, true, true, true, IgnoreWrongTarget),
            (1, 1, false, true, true, true, IgnoreWrongTarget),
            (0, 0, true, true, true, true, Consider),
            (1, 1, true, false, true, true, Deny),
            (1, 1, true, true, false, true, Deny),
            (1, 1, true, true, true, false, Deny),
            (1, 1, true, true, true, true, Consider),
        ] {
            let target = VoteTarget {
                voter_id: target,
                local_id: local,
                directory_matches: target_dir,
            };
            let membership = VoteMembership {
                cluster_matches: cluster,
                local_is_voter: local_voter,
                candidate_is_voter: candidate_voter,
            };
            check!(
                vote_admission_decision(target, membership) == expected,
                "{target:?} {membership:?}"
            );
        }
    }
}
