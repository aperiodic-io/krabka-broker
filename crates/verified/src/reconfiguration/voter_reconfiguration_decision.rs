use super::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationDecision, VoterReconfigurationPlan,
};
#[cfg(creusot)]
use super::{
    admitted_plan, is_admit, may_reconfigure, update_key_matches, voter_reconfiguration_rejection,
    voter_set_removes,
};
#[cfg(creusot)]
use creusot_std::prelude::*;

/// Admit one fresh KIP-853 operation and construct its exact result shape.
///
/// The result is exactly the first rejection of
/// `voter_reconfiguration_rejection`, which follows the precedence of Kafka's
/// handlers, and otherwise an admission with the plan `admitted_plan`
/// describes. Every admitted change satisfies `may_reconfigure`.
#[cfg_attr(creusot, ensures(match voter_reconfiguration_rejection(leadership, voters, request, target) {
    Some(rejection) => result == rejection,
    None => match result {
        VoterReconfigurationDecision::Admit(plan) => admitted_plan(voters, request.kind, plan),
        _ => false,
    },
}))]
#[cfg_attr(creusot, ensures(is_admit(result) ==> may_reconfigure(leadership, voters)))]
#[cfg_attr(creusot, ensures(forall<plan: VoterReconfigurationPlan>
    result == VoterReconfigurationDecision::Admit(plan)
        ==> plan.next_voter_count@ > 0 && plan.next_kraft_version@ <= 1))]
#[must_use]
pub fn voter_reconfiguration_decision(
    leadership: ReconfigurationLeadership,
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
    target: TargetVoter,
) -> VoterReconfigurationDecision {
    if !leadership.is_leader {
        return VoterReconfigurationDecision::NotLeader;
    }
    if !leadership.no_pending_change {
        return VoterReconfigurationDecision::InProgress;
    }
    if !leadership.epoch_committed {
        return VoterReconfigurationDecision::EpochUncommitted;
    }
    if !voters.latest_controls_committed {
        return VoterReconfigurationDecision::InProgress;
    }
    if voters.voter_count == 0 {
        return VoterReconfigurationDecision::EmptyCurrentVoterSet;
    }
    if voters.kraft_version > 1 {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    }
    match request.kind {
        VoterChangeKind::Add => add_decision(voters, target),
        VoterChangeKind::Remove => remove_decision(voters, target),
        VoterChangeKind::Update => update_decision(voters, target),
        VoterChangeKind::FinalizeKraftVersion => finalize_decision(voters, request),
    }
}

// Add and Remove both publish only the successor voter-set record.
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        plan.next_voter_count@ == next_voter_count@
        && plan.next_kraft_version@ == next_kraft_version@
        && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
    _ => false,
}))]
fn admit_voter_set_change(
    next_voter_count: usize,
    next_kraft_version: u16,
) -> VoterReconfigurationDecision {
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count,
        next_kraft_version,
        write_voters: true,
        write_kraft_version: false,
        preflight_only: false,
    })
}

/// `AddVoterHandler`: version, duplicate id, `ApiVersions` range, catch-up.
#[cfg_attr(creusot, requires(decision_voter_set_valid(voters)))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 1
            && target.membership == TargetMembership::Absent
            && target.version_compatible
            && target.caught_up
            && voters.voter_count@ < usize::MAX@
            && admitted_plan(voters, VoterChangeKind::Add, plan),
    VoterReconfigurationDecision::UnsupportedKraftVersion => voters.kraft_version@ != 1,
    VoterReconfigurationDecision::DuplicateVoter => voters.kraft_version@ == 1
        && target.membership != TargetMembership::Absent,
    VoterReconfigurationDecision::IncompatibleVoter => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && !target.version_compatible,
    VoterReconfigurationDecision::VoterNotCaughtUp => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && target.version_compatible
        && !target.caught_up,
    VoterReconfigurationDecision::InvalidVersionTransition => voters.kraft_version@ == 1
        && target.membership == TargetMembership::Absent
        && target.version_compatible
        && target.caught_up
        && voters.voter_count@ == usize::MAX@,
    _ => false,
}))]
fn add_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if voters.kraft_version != 1 {
        return VoterReconfigurationDecision::UnsupportedKraftVersion;
    }
    if !matches!(target.membership, TargetMembership::Absent) {
        return VoterReconfigurationDecision::DuplicateVoter;
    }
    if !target.version_compatible {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    if !target.caught_up {
        return VoterReconfigurationDecision::VoterNotCaughtUp;
    }
    // A voter set of `usize::MAX` members cannot grow; fail closed.
    let Some(next_voter_count) = voters.voter_count.checked_add(1) else {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    };
    admit_voter_set_change(next_voter_count, voters.kraft_version)
}

/// `RemoveVoterHandler`: version, then whether `VoterSet.removeVoter`
/// returns a new set; an absent id, a different stored key, and the last
/// voter are all its `VOTER_NOT_FOUND`.
#[cfg_attr(creusot, requires(decision_voter_set_valid(voters)))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 1
            && voter_set_removes(target.membership, voters)
            && admitted_plan(voters, VoterChangeKind::Remove, plan),
    VoterReconfigurationDecision::UnsupportedKraftVersion => voters.kraft_version@ != 1,
    VoterReconfigurationDecision::VoterNotFound => voters.kraft_version@ == 1
        && !voter_set_removes(target.membership, voters),
    _ => false,
}))]
fn remove_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if voters.kraft_version != 1 {
        return VoterReconfigurationDecision::UnsupportedKraftVersion;
    }
    if !matches!(target.membership, TargetMembership::PresentSameDirectory)
        || voters.voter_count == 1
    {
        return VoterReconfigurationDecision::VoterNotFound;
    }
    admit_voter_set_change(voters.voter_count - 1, voters.kraft_version)
}

/// `UpdateVoterHandler`: `ApiVersions` range, then the voter lookup.
#[cfg_attr(creusot, requires(decision_voter_set_valid(voters)))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        target.version_compatible
            && update_key_matches(target.membership)
            && admitted_plan(voters, VoterChangeKind::Update, plan),
    VoterReconfigurationDecision::IncompatibleVoter => !target.version_compatible,
    VoterReconfigurationDecision::VoterNotFound => target.version_compatible
        && target.membership == TargetMembership::Absent,
    VoterReconfigurationDecision::DirectoryMismatch => target.version_compatible
        && target.membership != TargetMembership::Absent
        && !update_key_matches(target.membership),
    _ => false,
}))]
fn update_decision(voters: CurrentVoterSet, target: TargetVoter) -> VoterReconfigurationDecision {
    if !target.version_compatible {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    match target.membership {
        TargetMembership::Absent => return VoterReconfigurationDecision::VoterNotFound,
        TargetMembership::PresentOtherDirectory => {
            return VoterReconfigurationDecision::DirectoryMismatch;
        }
        TargetMembership::PresentUnknownDirectory | TargetMembership::PresentSameDirectory => {}
    }
    let preflight_only = voters.kraft_version == 0;
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count: voters.voter_count,
        next_kraft_version: voters.kraft_version,
        write_voters: !preflight_only,
        write_kraft_version: false,
        preflight_only,
    })
}

/// `maybeAppendUpgradedKRaftVersion`: only the 0 to 1 upgrade, and only when
/// every voter supports version 1.
#[cfg_attr(creusot, requires(voters.voter_count@ > 0))]
#[cfg_attr(creusot, ensures(match result {
    VoterReconfigurationDecision::Admit(plan) =>
        voters.kraft_version@ == 0
            && request.requested_kraft_version@ == 1
            && voters.all_voters_support_requested
            && admitted_plan(voters, VoterChangeKind::FinalizeKraftVersion, plan),
    VoterReconfigurationDecision::InvalidVersionTransition => voters.kraft_version@ != 0
        || request.requested_kraft_version@ != 1,
    VoterReconfigurationDecision::IncompatibleVoter => voters.kraft_version@ == 0
        && request.requested_kraft_version@ == 1
        && !voters.all_voters_support_requested,
    _ => false,
}))]
fn finalize_decision(
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
) -> VoterReconfigurationDecision {
    if voters.kraft_version != 0 || request.requested_kraft_version != 1 {
        return VoterReconfigurationDecision::InvalidVersionTransition;
    }
    if !voters.all_voters_support_requested {
        return VoterReconfigurationDecision::IncompatibleVoter;
    }
    VoterReconfigurationDecision::Admit(VoterReconfigurationPlan {
        next_voter_count: voters.voter_count,
        next_kraft_version: 1,
        write_voters: true,
        write_kraft_version: true,
        preflight_only: false,
    })
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Decision helpers receive a nonempty set using a supported KRaft version.
fn decision_voter_set_valid(voters: CurrentVoterSet) -> bool {
    pearlite! { voters.voter_count@ > 0 && voters.kraft_version@ <= 1 }
}
