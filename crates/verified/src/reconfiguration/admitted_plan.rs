#[cfg(creusot)]
use super::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationDecision, VoterReconfigurationPlan,
};
#[cfg(creusot)]
use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// KIP-853's one-change-at-a-time rule: only a leader whose epoch is
/// committed, with no pending change and no uncommitted control record, may
/// start a voter change.
pub fn may_reconfigure(leadership: ReconfigurationLeadership, voters: CurrentVoterSet) -> bool {
    pearlite! {
        leadership.is_leader
            && leadership.no_pending_change
            && leadership.epoch_committed
            && voters.latest_controls_committed
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Whether a decision admits the change.
pub fn is_admit(decision: VoterReconfigurationDecision) -> bool {
    pearlite! {
        match decision {
            VoterReconfigurationDecision::Admit(_) => true,
            _ => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Whether Kafka's `VoterSet.removeVoter` returns a new voter set: the
/// stored `ReplicaKey` equals the requested one, and at least one voter
/// remains. `KafkaRaftClient` rejects a request without a directory id, so an
/// unknown stored directory never matches. `RemoveVoterHandler` answers every
/// other case, the last voter included, with `VOTER_NOT_FOUND`.
pub fn voter_set_removes(membership: TargetMembership, voters: CurrentVoterSet) -> bool {
    pearlite! {
        membership == TargetMembership::PresentSameDirectory && voters.voter_count@ > 1
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Whether the request's voter key names a current voter for an update:
/// Kafka's `VoterNode.isVoter`, which accepts an unknown stored directory and
/// an equal one but rejects a different known one.
///
/// Kafka applies this rule at `kraft.version` 1. At version 0 its
/// `updateVoterIgnoringDirectoryId` matches by voter id alone, but only to
/// stage upgrade data; Krabka's version-0 preflight applies the updated set to
/// the live quorum, so a different known directory, which would take that
/// voter's identity away, is refused at every version.
pub fn update_key_matches(membership: TargetMembership) -> bool {
    pearlite! {
        membership == TargetMembership::PresentUnknownDirectory
            || membership == TargetMembership::PresentSameDirectory
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The first rejection for a request, in Kafka's precedence order, or `None`
/// when the request is admitted.
///
/// Leadership comes first (`validateLeaderOnlyRequest`), then the pending
/// change and the leader HWM, then an uncommitted control record. Kafka reads
/// the latest `kraft.version`, so an uncommitted control record there always
/// means version 1 and a `REQUEST_TIMED_OUT`; that is why the check precedes
/// the version checks here, which read the committed version. (Kafka's
/// `UpdateVoterHandler` checks the voter's range before it reads the voter
/// set, so for an update with both faults Kafka reports the range.) A
/// finalization is also held to this single-flight rule, which Kafka's
/// `upgradeKRaftVersion` does not check. An empty voter set and a version
/// above 1 cannot occur in Kafka and fail closed. The per-operation checks
/// then follow each handler's order.
pub fn voter_reconfiguration_rejection(
    leadership: ReconfigurationLeadership,
    voters: CurrentVoterSet,
    request: VoterChangeRequest,
    target: TargetVoter,
) -> Option<VoterReconfigurationDecision> {
    pearlite! {
        if !leadership.is_leader {
            Some(VoterReconfigurationDecision::NotLeader)
        } else if !leadership.no_pending_change {
            Some(VoterReconfigurationDecision::InProgress)
        } else if !leadership.epoch_committed {
            Some(VoterReconfigurationDecision::EpochUncommitted)
        } else if !voters.latest_controls_committed {
            Some(VoterReconfigurationDecision::InProgress)
        } else if voters.voter_count@ == 0 {
            Some(VoterReconfigurationDecision::EmptyCurrentVoterSet)
        } else if voters.kraft_version@ > 1 {
            Some(VoterReconfigurationDecision::InvalidVersionTransition)
        } else {
            match request.kind {
                VoterChangeKind::Add => {
                    if voters.kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::UnsupportedKraftVersion)
                    } else if target.membership != TargetMembership::Absent {
                        Some(VoterReconfigurationDecision::DuplicateVoter)
                    } else if !target.version_compatible {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else if !target.caught_up {
                        Some(VoterReconfigurationDecision::VoterNotCaughtUp)
                    } else if voters.voter_count@ == usize::MAX@ {
                        Some(VoterReconfigurationDecision::InvalidVersionTransition)
                    } else {
                        None
                    }
                }
                VoterChangeKind::Remove => {
                    if voters.kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::UnsupportedKraftVersion)
                    } else if !voter_set_removes(target.membership, voters) {
                        Some(VoterReconfigurationDecision::VoterNotFound)
                    } else {
                        None
                    }
                }
                VoterChangeKind::Update => {
                    if !target.version_compatible {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else if target.membership == TargetMembership::Absent {
                        Some(VoterReconfigurationDecision::VoterNotFound)
                    } else if !update_key_matches(target.membership) {
                        Some(VoterReconfigurationDecision::DirectoryMismatch)
                    } else {
                        None
                    }
                }
                VoterChangeKind::FinalizeKraftVersion => {
                    if voters.kraft_version@ != 0 || request.requested_kraft_version@ != 1 {
                        Some(VoterReconfigurationDecision::InvalidVersionTransition)
                    } else if !voters.all_voters_support_requested {
                        Some(VoterReconfigurationDecision::IncompatibleVoter)
                    } else {
                        None
                    }
                }
            }
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The exact result shape of an admitted change: add and remove write one
/// `VotersRecord` for one more or one fewer voter; an update rewrites the
/// set at version 1 and is in-memory preflight data at version 0; a
/// finalization writes the version-1 record with the unchanged voter set.
pub fn admitted_plan(
    voters: CurrentVoterSet,
    kind: VoterChangeKind,
    plan: VoterReconfigurationPlan,
) -> bool {
    pearlite! {
        match kind {
            VoterChangeKind::Add => plan.next_voter_count@ == voters.voter_count@ + 1
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
            VoterChangeKind::Remove => plan.next_voter_count@ + 1 == voters.voter_count@
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters && !plan.write_kraft_version && !plan.preflight_only,
            VoterChangeKind::Update => plan.next_voter_count == voters.voter_count
                && plan.next_kraft_version == voters.kraft_version
                && plan.write_voters == (voters.kraft_version@ != 0)
                && !plan.write_kraft_version
                && plan.preflight_only == (voters.kraft_version@ == 0),
            VoterChangeKind::FinalizeKraftVersion => plan.next_voter_count == voters.voter_count
                && plan.next_kraft_version@ == 1
                && plan.write_voters && plan.write_kraft_version && !plan.preflight_only,
        }
    }
}
