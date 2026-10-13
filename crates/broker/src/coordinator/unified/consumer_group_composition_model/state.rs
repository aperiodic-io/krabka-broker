//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member, one advertisement, or one committed
//! offset out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use {
    crate::coordinator::unified::{
        INITIAL_GROUP_EPOCH,
        actor::reconciliation_model_support::{
            MemberHeartbeat, assert_member_epochs, owned_to_vec, parts_of,
        },
        consumer_state::{GroupState, MemberState},
        persistence_next_gen::MemberAssignmentState,
    },
    krabka_log::Offset,
    std::collections::BTreeMap,
};

#[krabka_macros::reconciliation_fields(member)]
#[derive(Clone, PartialEq, Eq, Hash, Debug, krabka_macros::ReconciliationMember)]
pub(super) struct MemberProj {
    /// `(partition, assignment epoch)` of every held partition, sorted.
    #[reconciliation(extra)]
    pub(super) assignment_epochs: Vec<(i32, i32)>,
    /// The member's sorted target assignment.
    pub(super) target: Vec<i32>,
}

#[krabka_macros::reconciliation_fields(group)]
#[derive(Clone, PartialEq, Eq, Hash, Debug, krabka_macros::ReconciliationGroup)]
pub(super) struct CgcState {
    /// Coordinator-side members, sorted by id.
    pub(super) members: Vec<MemberProj>,
    /// Modeled per-partition committed offsets, sorted.
    #[reconciliation(extra)]
    pub(super) committed: Vec<(i32, Offset)>,
}

pub(super) fn member<'a>(state: &'a CgcState, id: &str) -> Option<&'a MemberProj> {
    state.members.iter().find(|member| member.id == id)
}

/// Which epoch a member presents on an `OffsetCommit`: its current epoch (the
/// legitimate owner), one behind (a zombie from before the last rebalance), or
/// one ahead (an impossible/forward epoch). The real fence must accept only
/// `Current`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum EpochKind {
    Current,
    Stale,
    Forward,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, krabka_macros::ReconciliationAction)]
pub(super) enum CgcAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    /// A heartbeat without owned partitions, as the unchanged Java client sends.
    Keepalive(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
    Commit(String, i32, EpochKind), // (member, partition, presented-epoch) — fenced commit
}

pub(super) fn committed_map(s: &CgcState) -> BTreeMap<i32, Offset> {
    s.committed.iter().copied().collect()
}

pub(super) fn committed_of(s: &CgcState, part: i32) -> Offset {
    s.committed
        .iter()
        .find(|(p, _)| *p == part)
        .map_or(Offset(0), |(_, o)| *o)
}
