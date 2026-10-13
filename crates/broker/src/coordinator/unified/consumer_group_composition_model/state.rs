//! The model state the checker enumerates, the actions it explores, and the
//! small accessors that read one member, one advertisement, or one committed
//! offset out of that state.
//!
//! Every field is a sorted `Vec` rather than a map, because stateright hashes
//! and compares each state, so the representation has to be canonical.

use {
    crate::coordinator::unified::actor::reconciliation_model_support::{
        group_projection, member_projection, model_actions,
    },
    krabka_log::Offset,
    std::collections::BTreeMap,
};

member_projection! { pub(super) struct MemberProj {
    /// `(partition, assignment epoch)` of every held partition, sorted.
    assignment_epochs: Vec<(i32, i32)>,
} }

group_projection! { pub(super) struct CgcState {
    members: Vec<MemberProj>,
    /// Modeled per-partition committed offsets, sorted.
    committed: Vec<(i32, Offset)>,
} }

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

model_actions! { pub(super) enum CgcAction {
    Commit(String, i32, EpochKind), // (member, partition, presented-epoch) — fenced commit
} }

pub(super) fn committed_map(s: &CgcState) -> BTreeMap<i32, Offset> {
    s.committed.iter().copied().collect()
}

pub(super) fn committed_of(s: &CgcState, part: i32) -> Offset {
    s.committed
        .iter()
        .find(|(p, _)| *p == part)
        .map_or(Offset(0), |(_, o)| *o)
}
