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
        state::{AssignmentEpoch, CgcState, CommittedPartitionOffset, MemberProj},
    },
    crate::coordinator::unified::{
        actor::reconciliation_model_support::{insert_modeled_member, modeled_member},
        consumer_state::GroupState,
    },
    krabka_log::Offset,
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        time::Instant,
    },
};

fn epochs_of(map: &HashMap<Uuid, HashMap<i32, i32>>) -> Vec<AssignmentEpoch> {
    // The runtime map exposes std key/value pairs; model coordinates are named.
    let mut result: Vec<_> = map.get(&TOPIC).map_or_default(|epochs| {
        epochs
            .iter()
            .map(|(&partition, &epoch)| AssignmentEpoch { partition, epoch })
            .collect()
    });
    result.sort_unstable_by_key(|row| row.partition);
    result
}

fn epochs_to_map(epochs: &[AssignmentEpoch]) -> HashMap<Uuid, HashMap<i32, i32>> {
    if epochs.is_empty() {
        return HashMap::new();
    }
    let mut assignments = HashMap::new();
    for row in epochs {
        assignments.insert(row.partition, row.epoch);
    }
    let mut topics = HashMap::new();
    topics.insert(TOPIC, assignments);
    topics
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
        // std map iterator pairs are converted directly into the named ledger.
        committed
            .iter()
            .map(|(&partition, &offset)| CommittedPartitionOffset { partition, offset })
            .collect(),
    )
}

pub(super) fn assert_epoch_monotonic(pre: &CgcState, post: &GroupState) {
    pre.assert_epochs_against(post);
}
