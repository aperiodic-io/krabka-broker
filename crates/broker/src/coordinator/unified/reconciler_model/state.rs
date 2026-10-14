//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member or one advertisement out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use crate::coordinator::unified::{
    INITIAL_GROUP_EPOCH,
    actor::reconciliation_model_support::{
        MemberHeartbeat, MemberPartitions, owned_to_vec, parts_of,
    },
    consumer_state::{GroupState, MemberState},
    persistence_next_gen::MemberAssignmentState,
};

/// Per-member coordinator-side projection, which maps a single topic to a
/// `Vec<i32>`.
#[krabka_macros::reconciliation_fields(member)]
#[derive(Clone, PartialEq, Eq, Hash, Debug, krabka_macros::ReconciliationMember)]
pub(super) struct MemberProj {
    /// The member's sorted target assignment.
    pub(super) target: Vec<i32>,
}

#[krabka_macros::reconciliation_fields(group)]
pub(super) struct ReconState {
    /// Coordinator-side members, sorted by id.
    pub(super) members: Vec<MemberProj>,
}

pub(super) fn member<'a>(state: &'a ReconState, id: &str) -> Option<&'a MemberProj> {
    state.members.iter().find(|member| member.id == id)
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, krabka_macros::ReconciliationAction)]
pub(super) enum ReconAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    /// A heartbeat without owned partitions, as the unchanged Java client sends.
    Keepalive(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
}
