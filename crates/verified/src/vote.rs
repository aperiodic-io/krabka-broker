//! KIP-595 Vote wire and membership admission decisions.

use creusot_std::prelude::*;

/// Whether the signed Vote fields can be converted without aliasing a Kafka
/// sentinel to a real node or epoch.
#[krabka_macros::model_type]
pub enum VoteWireDecision {
    Reject,
    Accept,
}

/// Whether unsigned consensus fields fit Kafka's signed Vote wire fields.
#[krabka_macros::model_type]
pub enum VoteEncodeDecision {
    Reject,
    Accept,
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

/// Admission shared by binding votes and pre-votes before epoch/log checks.
#[krabka_macros::model_type]
pub enum VoteAdmissionDecision {
    /// The Vote targets another voter and must be ignored without a reply.
    IgnoreWrongTarget,
    /// The target is local, but the local node or candidate lacks membership.
    Deny,
    /// The exact target and both membership requirements hold.
    Consider,
}

/// Whom one Vote request is addressed to, against this node.
#[krabka_macros::model_type]
pub struct VoteTarget {
    /// The `VoterId` the request is addressed to.
    pub voter_id: u64,
    /// This node's ID.
    pub local_id: u64,
    /// The request's `VoterDirectoryId` names this node's directory.
    pub directory_matches: bool,
}

/// The cluster and voter-set memberships one Vote request needs.
#[krabka_macros::model_type]
pub struct VoteMembership {
    /// The request carries no cluster ID, or this node's.
    pub cluster_matches: bool,
    /// This node is a voter.
    pub local_is_voter: bool,
    /// The candidate is a voter.
    pub candidate_is_voter: bool,
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

    #[yare::parameterized(
        zeroes = { 0, 0, 0, 0, VoteWireDecision::Accept },
        negative_voter = { -1, 0, 0, 0, VoteWireDecision::Reject },
        negative_candidate = { 0, -1, 0, 0, VoteWireDecision::Reject },
        negative_epoch = { 0, 0, -1, 0, VoteWireDecision::Reject },
        negative_last_epoch = { 0, 0, 0, -1, VoteWireDecision::Reject },
    )]
    fn wire_fields_reject_negatives_without_rejecting_zero(
        voter: i32,
        candidate: i32,
        epoch: i32,
        last_epoch: i32,
        expected: VoteWireDecision,
    ) {
        check!(vote_wire_decision(voter, candidate, epoch, last_epoch) == expected);
    }

    #[yare::parameterized(
        signed_maximum = {
            i32::MAX as u64,
            i32::MAX as u64,
            i32::MAX as u32,
            i32::MAX as u32,
            VoteEncodeDecision::Accept,
        },
        voter_overflow = { i32::MAX as u64 + 1, 0, 0, 0, VoteEncodeDecision::Reject },
        candidate_overflow = { 0, i32::MAX as u64 + 1, 0, 0, VoteEncodeDecision::Reject },
        epoch_overflow = { 0, 0, i32::MAX as u32 + 1, 0, VoteEncodeDecision::Reject },
        last_epoch_overflow = { 0, 0, 0, i32::MAX as u32 + 1, VoteEncodeDecision::Reject },
    )]
    fn encode_fields_reject_values_above_signed_wire_maximum(
        voter: u64,
        candidate: u64,
        epoch: u32,
        last_epoch: u32,
        expected: VoteEncodeDecision,
    ) {
        check!(vote_encode_decision(voter, candidate, epoch, last_epoch) == expected);
    }

    #[yare::parameterized(
        other_voter = {
            VoteTarget { voter_id: 1, local_id: 2, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::IgnoreWrongTarget,
        },
        zero_other_voter = {
            VoteTarget { voter_id: 0, local_id: 1, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::IgnoreWrongTarget,
        },
        directory_mismatch = {
            VoteTarget { voter_id: 1, local_id: 1, directory_matches: false },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::IgnoreWrongTarget,
        },
        exact_target = {
            VoteTarget { voter_id: 0, local_id: 0, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::Consider,
        },
        cluster_mismatch = {
            VoteTarget { voter_id: 1, local_id: 1, directory_matches: true },
            VoteMembership { cluster_matches: false, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::Deny,
        },
        local_not_voter = {
            VoteTarget { voter_id: 1, local_id: 1, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: false, candidate_is_voter: true },
            VoteAdmissionDecision::Deny,
        },
        candidate_not_voter = {
            VoteTarget { voter_id: 1, local_id: 1, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: false },
            VoteAdmissionDecision::Deny,
        },
        valid_nonzero_target = {
            VoteTarget { voter_id: 1, local_id: 1, directory_matches: true },
            VoteMembership { cluster_matches: true, local_is_voter: true, candidate_is_voter: true },
            VoteAdmissionDecision::Consider,
        },
    )]
    fn admission_requires_the_exact_target_and_both_memberships(
        target: VoteTarget,
        membership: VoteMembership,
        expected: VoteAdmissionDecision,
    ) {
        check!(
            vote_admission_decision(target, membership) == expected,
            "{target:?} {membership:?}"
        );
    }
}
