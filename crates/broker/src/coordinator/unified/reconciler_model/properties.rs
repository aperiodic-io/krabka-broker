//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function that drives the real `step_heartbeat`, the
//! search boundary, and the properties the checker proves.
//!
//! The helpers the transitions call live in the sibling modules. This file only
//! sequences them and states what must hold.

use {
    super::{
        config::ReconModel,
        projection::{assert_epoch_monotonic, project, rebuild_group},
        state::{ReconAction, ReconState, member},
    },
    crate::coordinator::unified::{
        actor::reconciliation_model_support::{
            advertised_disjoint, apply_client_move, apply_member_heartbeat, client_moves,
            exclusive_ownership, overlaps_others, owned_map,
        },
        persistence_next_gen::MemberAssignmentState,
    },
    stateright::{Model, Property},
    std::collections::BTreeMap,
};

impl Model for ReconModel {
    type Action = ReconAction;
    type State = ReconState;

    krabka_macros::reconciliation_next_state! {
        fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State>
        [action = ReconAction, lookup = member, rebuild = rebuild_group, check = assert_epoch_monotonic, project = project]
        { setup {} actions {} project {  } }
    }

    krabka_macros::reconciliation_actions! {
        fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>)
        [action = ReconAction, lookup = member, member = m]
        {}
    }

    fn properties(&self) -> Vec<Property<Self>> {
        krabka_macros::model_properties! { ReconState;
        // HEADLINE: no two members ever simultaneously own the same partition.
        always "no_double_ownership" => |s| {
            exclusive_ownership(&s.client_owned)
        },
        // A member is never advertised a partition another member currently
        // owns — the coordinator-side withholding invariant.
        always "advertised_disjoint_from_others_owned" => |s| {
            advertised_disjoint(&s.advertised, &s.client_owned)
        },
        // Non-vacuity: a handoff state is reachable (a partition is in one
        // member's target while another member currently owns it).
        sometimes "handoff_witness" => |s| {
            overlaps_others(s.members.iter().map(|member| (&member.id, &member.target)), &s.client_owned)
        },
        // Non-vacuity: a fully-converged state is reachable (every member
        // owns exactly its target and is Stable).
        sometimes "converged_witness" => |s| {
            !s.members.is_empty()
            && s.members.iter().all(|m| {
                    let owned: Vec<i32> = s
                        .client_owned
                        .iter()
                        .find(|(k, _)| k == &m.id)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    m.assignment_state == MemberAssignmentState::Stable && owned == m.target
                })
        },

        }
    }

    fn init_states(&self) -> Vec<Self::State> { vec![ReconState::empty()] }

    fn within_boundary(&self, state: &Self::State) -> bool { state.group_epoch <= self.max_epoch }
}
