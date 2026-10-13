//! The projection between the model state and the real
//! [`GroupState`](crate::coordinator::unified::consumer_state::GroupState).
//!
//! Every transition rebuilds a real group from the enumerated state, drives the
//! real code over it, and projects the result back. Keeping both directions in
//! one file is what makes it easy to see that they are inverses. The
//! epoch-monotonicity check that every transition runs against the rebuilt
//! group lives here too, because it reads the same two representations.

use {
    super::state::{MemberProj, ReconState},
    crate::coordinator::unified::{
        actor::reconciliation_model_support::{assert_member_epochs, model_epoch_assertion},
        consumer_state::GroupState,
    },
    std::collections::{BTreeMap, BTreeSet},
};

crate::coordinator::unified::actor::reconciliation_model_support::rebuild_model_group! {
    /// Rebuilds a real `GroupState` from the projection, so that the next real
    /// call behaves exactly as it does in a live run.
    ///
    /// The fields that the projection does not hold get faithful constants. The
    /// subscription is fixed to the one topic, `last_seen` is constant, and
    /// `previous_member_epoch` affects no decision.
    pub(super) fn rebuild_group(s: &ReconState); |m, ms|
}

/// Projects a real `GroupState`, the client ledger, and the advertised map
/// back into the hashable state.
pub(super) fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
) -> ReconState {
    let mut members: Vec<MemberProj> = g
        .members
        .values()
        .map(|m| MemberProj::from_member(m, g.target.per_member.get(&m.member_id)))
        .collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    ReconState::from_group(g, members, owned, advertised)
}

model_epoch_assertion! {
/// Per-member epoch must never regress across a real step.
    pub(super) fn assert_epoch_monotonic(ReconState, GroupState)
}
