use super::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationDecision as D, VoterReconfigurationPlan,
    test_support::{ControlRecordWrite, KraftFeatureLevel, VoterCount},
    voter_reconfiguration_decision,
};

#[derive(Clone, Copy)]
struct ReconfigurationPlanSetup {
    count: VoterCount,
    level: KraftFeatureLevel,
    voters: ControlRecordWrite,
    kraft: ControlRecordWrite,
}

impl Default for ReconfigurationPlanSetup {
    fn default() -> Self {
        Self {
            count: VoterCount(3),
            level: KraftFeatureLevel(1),
            voters: ControlRecordWrite::Emit,
            kraft: ControlRecordWrite::Skip,
        }
    }
}

const LEADING: ReconfigurationLeadership = ReconfigurationLeadership {
    is_leader: true,
    no_pending_change: true,
    epoch_committed: true,
};

const fn voters(voter_count: VoterCount, kraft_version: KraftFeatureLevel) -> CurrentVoterSet {
    CurrentVoterSet {
        voter_count: voter_count.0,
        kraft_version: kraft_version.0,
        latest_controls_committed: true,
        all_voters_support_requested: true,
    }
}

const fn request(kind: VoterChangeKind) -> VoterChangeRequest {
    VoterChangeRequest {
        kind,
        requested_kraft_version: 1,
    }
}

const fn target(membership: TargetMembership) -> TargetVoter {
    TargetVoter {
        membership,
        version_compatible: true,
        caught_up: true,
    }
}

const fn plan(setup: ReconfigurationPlanSetup) -> D {
    D::Admit(VoterReconfigurationPlan {
        next_voter_count: setup.count.0,
        next_kraft_version: setup.level.0,
        write_voters: matches!(setup.voters, ControlRecordWrite::Emit),
        write_kraft_version: matches!(setup.kraft, ControlRecordWrite::Emit),
        preflight_only: matches!(setup.voters, ControlRecordWrite::Skip),
    })
}

mod leadership_and_single_flight_gate_every_operation_first;

mod update_voter_matches_the_stored_key_as_voter_node_is_voter_does;
