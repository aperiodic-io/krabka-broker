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
            actor::reconciliation_model_support::{insert_modeled_member, modeled_member},
            consumer_state::GroupState,
        },
    },
    krabka_log::Offset,
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        time::Instant,
    },
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

krabka_macros::rebuild_reconciliation_group! {

    pub(super) fn rebuild_group(s: &CgcState) -> GroupState
    [member = m, built = ms]
    { let mut updated = ms;
        updated.assignment_epochs = epochs_to_map(&m.assignment_epochs);

updated }
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

pub(super) fn assert_epoch_monotonic(pre: &CgcState, post: &GroupState) {
    pre.assert_epochs_against(post);
}
