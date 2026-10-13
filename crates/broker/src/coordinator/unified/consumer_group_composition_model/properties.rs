//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function, the search boundary, and the properties
//! the checker proves.
//!
//! The transitions themselves live in the sibling modules. This file only
//! dispatches to them and states what must hold.

use {
    super::{
        MAX_OFFSET,
        commit::do_commit,
        config::CgcModel,
        projection::{assert_epoch_monotonic, project, rebuild_group},
        state::{CgcAction, CgcState, EpochKind, committed_map, committed_of, member},
    },
    crate::coordinator::unified::actor::reconciliation_model_support::{
        advertised_disjoint, apply_client_move, apply_member_heartbeat, client_moves,
        exclusive_ownership, owned_map,
    },
    stateright::{Model, Property},
    std::collections::BTreeMap,
};

impl Model for CgcModel {
    type Action = CgcAction;
    type State = CgcState;

    krabka_macros::reconciliation_next_state! {
        fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State>
        [action = CgcAction, lookup = member, rebuild = rebuild_group, check = assert_epoch_monotonic, project = project]
        { setup { let committed = committed_map(last); } actions { CgcAction::Commit(id, part, kind) => return do_commit(last, &id, part, kind), } project { &committed } }
    }

    krabka_macros::reconciliation_actions! {
        fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>)
        [action = CgcAction, lookup = member, member = m]
        {
            // Offset commit: offered with EACH epoch kind (current / stale /
            // forward) so the real epoch fence — not a precondition — is what's
            // exercised; the committed counter is bounded by MAX_OFFSET.
            for part in 0..self.partitions {
                if committed_of(state, part) < MAX_OFFSET {
                    for kind in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                        actions.push(CgcAction::Commit(m.id.clone(), part, kind));
                    }
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        krabka_macros::model_properties! { CgcState;
        // HEADLINE: no two members ever simultaneously own the same partition
        // (the real reconciliation's withholding — re-verified in the composed
        // context, with offset traffic interleaved).
        always "exclusive_ownership" => |s| {
            exclusive_ownership(&s.client_owned)
        },
        // A member is never advertised a partition another member currently
        // owns — the coordinator-side withholding invariant.
        always "advertised_disjoint_from_others_owned" => |s| {
            advertised_disjoint(&s.advertised, &s.client_owned)
        },
        // The real OffsetCommit epoch fence agrees with the independent oracle
        // is enforced as a per-transition equality assertion in the Commit arm (a
        // divergence is a real `validate_offset_commit` regression). The
        // value here is the COMPOSITION: the epochs that fence drives are set
        // by the real reconciliation, so a zombie from before a rebalance is
        // rejected — see the `member_epoch_advanced` witness.

        // ----- non-vacuity witnesses -----
        // A current-epoch commit was accepted (the fence's accept path fires).
        sometimes "offset_advanced" => |s| {
            s.committed.iter().any(|row| row.offset > 0)
        },
        // The reconciliation actually advanced a member's epoch past its first
        // generation — so `Stale`/`Forward` commits are genuinely distinct from
        // `Current` and the fence is exercised over non-trivial epochs (a real
        // zombie scenario: a stale commit after a rebalance bumped the epoch).
        sometimes "member_epoch_advanced" => |s| {
            s.members.iter().any(|m| m.member_epoch >= 2)
        },
        // KIP-1251: a member holds a partition from an epoch older than its
        // current one, so a `Stale` commit of that partition is accepted
        // and the per-partition half of the fence is exercised.
        sometimes "stale_epoch_partition_witness" => |s| {
            s.members.iter().any(|m| {
                    m.assignment_epochs
                        .iter()
                        .any(|row| row.epoch < m.member_epoch)
                })
        },
        // A handoff state: a partition is in one member's target while another
        // member currently owns it (the baton is mid-pass).
        sometimes "handoff_witness" => |s| {
            s.handoff_witness()
        },

        }
    }

    fn init_states(&self) -> Vec<Self::State> { vec![CgcState::empty(vec![])] }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch
            && state.committed.iter().all(|row| row.offset <= MAX_OFFSET)
    }
}
