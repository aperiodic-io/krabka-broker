//! The projection between the model state and the real
//! [`GroupState`](crate::coordinator::unified::consumer_state::GroupState),
//! which mirrors `reconciler_model`.
//!
//! Every transition rebuilds a real group from the enumerated state, drives the
//! real code over it, and projects the result back. Keeping both directions in
//! one file is what makes it easy to see that they are inverses.

use {
    super::{
        TOPIC,
        state::{CgcState, MemberProj},
    },
    crate::coordinator::{
        test_support::sorted_epoch_pairs,
        unified::{
            actor::reconciliation_model_support::{assert_member_epochs, model_epoch_assertion},
            consumer_state::GroupState,
        },
    },
    krabka_log::Offset,
    krabka_protocol::primitives::uuid::Uuid,
    std::collections::{BTreeMap, BTreeSet, HashMap},
};

fn epochs_of(map: &HashMap<Uuid, HashMap<i32, i32>>) -> Vec<(i32, i32)> {
    sorted_epoch_pairs(map.get(&TOPIC))
}

fn epochs_to_map(epochs: &[(i32, i32)]) -> HashMap<Uuid, HashMap<i32, i32>> {
    if epochs.is_empty() {
        HashMap::new()
    } else {
        [(TOPIC, epochs.iter().copied().collect())].into()
    }
}

crate::coordinator::unified::actor::reconciliation_model_support::rebuild_model_group! {
    pub(super) fn rebuild_group(s: &CgcState); |m, ms|; mutate(updated) {
        updated.assignment_epochs = epochs_to_map(&m.assignment_epochs);
    }
}

pub(super) fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
    committed: &BTreeMap<i32, Offset>,
) -> CgcState {
    let mut members: Vec<MemberProj> = g
        .members
        .values()
        .map(|m| {
            MemberProj::from_member(
                m,
                g.target.per_member.get(&m.member_id),
                epochs_of(&m.assignment_epochs),
            )
        })
        .collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    CgcState::from_group(
        g,
        members,
        owned,
        advertised,
        committed.iter().map(|(&k, &v)| (k, v)).collect(),
    )
}

model_epoch_assertion! {
    pub(super) fn assert_epoch_monotonic(CgcState, GroupState)
}
