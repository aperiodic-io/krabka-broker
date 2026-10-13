//! Bounded stateright model of the KIP-932 share-group membership machine.
//!
//! DRIVEN production code: [`ShareGroupState`] membership and timeout
//! transitions, the real share assignor through [`reconcile`], the shared
//! member-epoch fence, and the real snapshot/apply replay adapters. MODELED:
//! two member identities, one subscribed topic with one or two partitions, a
//! logical clock through four ticks, and group epochs through five. The search
//! explores join, current/stale/forward heartbeat, leave, timeout, metadata
//! resize, and one crash/replay in every reachable ordering. A stale heartbeat
//! sends the member epoch minus one: Kafka accepts it when it is the previous
//! member epoch or 0 (a rejoin), and fences it otherwise.
//!
//! Share assignments intentionally permit the same partition across members
//! when members outnumber partitions. The ownership property therefore proves
//! uniqueness of each `(member, topic, partition)` coordinate, not consumer-
//! group-style cross-member exclusivity.

use {
    super::{
        assignment::reconcile,
        seed::{apply_seed, snapshot_seed},
    },
    crate::{
        coordinator::unified::{
            actor::{MetadataProvider, reconciliation_model_support::model_properties},
            assignor::TopicIdPartition,
            reconciler::ReconcileInput,
            share::state::{ShareGroupState, ShareMemberState},
        },
        model_check::run_bfs,
    },
    krabka_protocol::primitives::uuid::Uuid,
    stateright::{Checker, Model},
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant},
    },
};

const TOPIC: Uuid = Uuid([42; 16]);
const TOPIC_NAME: &str = "t";
const MAX_CLOCK: u8 = 4;
// A new share group starts at Kafka's group epoch 1, so the bound allows the
// same five epoch bumps as before that start moved from 0.
const MAX_EPOCH: i32 = 6;
const MAX_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 64;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 54_860;
const WITNESS_STALE_FENCED: u8 = 1 << 0;
const WITNESS_FORWARD_FENCED: u8 = 1 << 1;
const WITNESS_TIMEOUT: u8 = 1 << 2;
const WITNESS_METADATA: u8 = 1 << 3;
const WITNESS_REPLAY: u8 = 1 << 4;
const WITNESS_UNKNOWN: u8 = 1 << 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EpochKind {
    Current,
    Stale,
    Forward,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Join(&'static str),
    Heartbeat(&'static str, EpochKind),
    Leave(&'static str),
    TimeoutTick,
    MetadataHeartbeat(&'static str, i32),
    UnknownHeartbeat,
    Replay,
}

#[derive(Clone, Debug)]
struct State {
    group: ShareGroupState,
    origin: Instant,
    clock: u8,
    partitions: i32,
    witnesses: u8,
}

/// What a member persists: its identity, epochs, subscriptions and assigned
/// partitions.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct DurableMemberProjection {
    member_id: String,
    member_epoch: i32,
    previous_member_epoch: i32,
    subscriptions: Vec<String>,
    assigned: Vec<i32>,
}

/// A member, with the time the group last saw it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MemberProjection {
    durable: DurableMemberProjection,
    last_seen: Instant,
}

/// The partitions of the topic that the target assigns to a member.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MemberTarget {
    member_id: String,
    partitions: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupProjection {
    group_epoch: i32,
    metadata_hash: i64,
    target_epoch: i32,
    clock: u8,
    partitions: i32,
    members: Vec<MemberProjection>,
    target: Vec<MemberTarget>,
    witnesses: u8,
}

impl State {
    fn projection(&self) -> GroupProjection {
        let mut members: Vec<MemberProjection> = self
            .group
            .members
            .values()
            .map(|member| MemberProjection {
                durable: durable_member(member),
                last_seen: member.last_seen,
            })
            .collect();
        members.sort();
        let mut target: Vec<MemberTarget> = self
            .group
            .target
            .per_member
            .iter()
            .map(|(member_id, assignment)| {
                let mut partitions = assignment.get(&TOPIC).cloned().unwrap_or_default();
                partitions.sort_unstable();
                MemberTarget {
                    member_id: member_id.clone(),
                    partitions,
                }
            })
            .collect();
        target.sort();
        GroupProjection {
            group_epoch: self.group.group_epoch,
            metadata_hash: self.group.metadata_hash,
            target_epoch: self.group.target.epoch,
            clock: self.clock,
            partitions: self.partitions,
            members,
            target,
            witnesses: self.witnesses,
        }
    }
}

krabka_macros::projection_identity!(State, projection);

#[derive(Clone, Debug)]
struct ShareModel;

#[derive(Debug)]
struct ModelMetadata {
    partitions: i32,
}

impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput {
            topic_id_by_name: [(TOPIC_NAME.to_owned(), TOPIC)].into(),
            partitions_per_topic: [(TOPIC, self.partitions)].into(),
            ..Default::default()
        }
    }
}

/// Marks the share state of exactly the topic's `partitions` initialized, as
/// the lifecycle hook leaves it once the persister answers. The model's
/// assignor only hands out initialized partitions.
fn initialize(group: &mut ShareGroupState, partitions: i32) {
    group.initialized = (0..partitions)
        .map(|partition| TopicIdPartition {
            topic_id: TOPIC,
            partition,
        })
        .collect();
    group.topic_names.insert(TOPIC, TOPIC_NAME.to_owned());
}

fn metadata(partitions: i32) -> ModelMetadata { ModelMetadata { partitions } }

fn at(state: &State) -> Instant {
    state
        .origin
        .checked_add(Duration::from_secs(u64::from(state.clock)))
        .expect("bounded model clock fits Instant")
}

fn assignment_coordinates_unique(group: &ShareGroupState) -> bool {
    let unique = |assignment: &HashMap<Uuid, Vec<i32>>| {
        let mut seen = HashSet::new();
        assignment.iter().all(|(topic, parts)| {
            parts
                .iter()
                .all(|partition| seen.insert((*topic, *partition)))
        })
    };
    group
        .members
        .values()
        .all(|member| unique(&member.assigned_partitions))
        && group.target.per_member.values().all(unique)
}

fn epochs_fenced(group: &ShareGroupState) -> bool {
    group.group_epoch >= 0
        && group.target.epoch >= 0
        && group.target.epoch <= group.group_epoch
        && group
            .members
            .values()
            .all(|member| member.member_epoch >= 0 && member.member_epoch <= group.group_epoch)
}

fn assignments_in_metadata(state: &State) -> bool {
    let valid = |assignment: &HashMap<Uuid, Vec<i32>>| {
        assignment.iter().all(|(topic, parts)| {
            *topic == TOPIC
                && parts
                    .iter()
                    .all(|partition| *partition >= 0 && *partition < state.partitions)
        })
    };
    state.group.members.values().all(|member| {
        member.member_epoch < state.group.target.epoch || valid(&member.assigned_partitions)
    }) && state.group.target.per_member.values().all(valid)
}

#[derive(Debug, PartialEq, Eq)]
struct DurableProjection {
    group_epoch: i32,
    metadata_hash: i64,
    target_epoch: i32,
    members: Vec<DurableMemberProjection>,
    target: Vec<MemberTarget>,
}

fn durable_member(member: &ShareMemberState) -> DurableMemberProjection {
    let mut subscriptions: Vec<String> = member.subscribed_topic_names.iter().cloned().collect();
    subscriptions.sort();
    let mut assigned = member
        .assigned_partitions
        .get(&TOPIC)
        .cloned()
        .unwrap_or_default();
    assigned.sort_unstable();
    DurableMemberProjection {
        member_id: member.member_id.clone(),
        member_epoch: member.member_epoch,
        previous_member_epoch: member.previous_member_epoch,
        subscriptions,
        assigned,
    }
}

fn durable_projection(group: &ShareGroupState) -> DurableProjection {
    let mut members: Vec<DurableMemberProjection> =
        group.members.values().map(durable_member).collect();
    members.sort();
    let mut target: Vec<MemberTarget> = group
        .members
        .keys()
        .filter_map(|member_id| {
            group.target.per_member.get(member_id).map(|assignment| {
                let mut partitions = assignment.get(&TOPIC).cloned().unwrap_or_default();
                partitions.sort_unstable();
                MemberTarget {
                    member_id: member_id.clone(),
                    partitions,
                }
            })
        })
        .collect();
    target.sort();
    DurableProjection {
        group_epoch: group.group_epoch,
        metadata_hash: group.metadata_hash,
        target_epoch: group.target.epoch,
        members,
        target,
    }
}

/// Reconciles the group and then the member against its target, as a share
/// heartbeat does: `true` for a joining member.
macro_rules! reconcile_group_and_member {
    ($state:ident, $member_id:ident, $joining:expr) => {
        let metadata = metadata($state.partitions);
        reconcile(
            &mut $state.group,
            &metadata,
            std::time::Duration::ZERO,
            $joining,
        )
        .ok()?;
        $state
            .group
            .reconcile_member($member_id, $joining, &metadata);
    };
}

impl Model for ShareModel {
    type Action = Action;
    type State = State;

    krabka_macros::model_transition! { last, action, state; {
        match action {
            Action::Join(member_id) => {
                if state.group.members.contains_key(member_id) {
                    return None;
                }
                let mut member = ShareMemberState::joining(
                    member_id,
                    "client",
                    "host",
                    [TOPIC_NAME.to_owned()].into(),
                );
                member.last_seen = at(&state);
                state.group.add_or_update_member(member);
                reconcile_group_and_member! { state, member_id, true }
            }
            Action::Heartbeat(member_id, kind) => {
                let current = state.group.members.get(member_id)?.member_epoch;
                let requested = match kind {
                    EpochKind::Current => current,
                    EpochKind::Stale => current.saturating_sub(1),
                    EpochKind::Forward => current.saturating_add(1),
                };
                match state.group.validate_member_epoch(member_id, requested) {
                    Ok(_) => {
                        state.group.members.get_mut(member_id)?.last_seen = at(&state);
                        reconcile_group_and_member! { state, member_id, false }
                    }
                    Err(error) => match kind {
                        EpochKind::Stale => {
                            assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                            state.witnesses |= WITNESS_STALE_FENCED;
                        }
                        EpochKind::Forward => {
                            assert2::assert!(error == crate::codes::FENCED_MEMBER_EPOCH);
                            state.witnesses |= WITNESS_FORWARD_FENCED;
                        }
                        EpochKind::Current => return None,
                    },
                }
            }
            Action::Leave(member_id) => {
                state.group.remove_member(member_id)?;
                if !state.group.bump_epoch() {
                    return None;
                }
            }
            Action::TimeoutTick => {
                state.clock += 1;
                let expired = state
                    .group
                    .evict_expired(at(&state), Duration::from_secs(1));
                if !expired.is_empty() {
                    state.witnesses |= WITNESS_TIMEOUT;
                    // Kafka fences each expired member on its own, with an
                    // epoch bump each, and computes no target.
                    for _ in &expired {
                        if !state.group.bump_epoch() {
                            return None;
                        }
                    }
                }
            }
            Action::MetadataHeartbeat(member_id, partitions) => {
                state.group.members.get(member_id)?;
                state.partitions = partitions;
                initialize(&mut state.group, partitions);
                let before = state.group.group_epoch;
                reconcile_group_and_member! { state, member_id, false }
                if state.group.group_epoch > before {
                    state.witnesses |= WITNESS_METADATA;
                }
                state.group.members.get_mut(member_id)?.last_seen = at(&state);
            }
            Action::UnknownHeartbeat => {
                let error = state
                    .group
                    .validate_member_epoch("unknown", 1)
                    .expect_err("unknown member is rejected");
                assert2::assert!(error == crate::codes::UNKNOWN_MEMBER_ID);
                state.witnesses |= WITNESS_UNKNOWN;
            }
            Action::Replay => {
                let before = durable_projection(&state.group);
                let seed = snapshot_seed(&state.group);
                let mut restored = ShareGroupState::new("g");
                apply_seed(&mut restored, seed);
                for member in restored.members.values_mut() {
                    member.last_seen = at(&state);
                }
                assert2::assert!(durable_projection(&restored) == before);
                state.group = restored;
                state.witnesses |= WITNESS_REPLAY;
            }
        }
        assert2::assert!(assignment_coordinates_unique(&state.group));
        assert2::assert!(epochs_fenced(&state.group));
        assert2::assert!(assignments_in_metadata(&state));
        Some(state)
    }}

    model_properties! {
        @method State;
        always "ownership_coordinate_uniqueness" => |state| assignment_coordinates_unique(&state.group),
        always "member_epoch_fencing" => |state| epochs_fenced(&state.group),
        always "assignment_within_metadata" => |state| assignments_in_metadata(state),
        sometimes "stale_epoch_rejected" => |state| state.witnesses & WITNESS_STALE_FENCED != 0,
        sometimes "forward_epoch_rejected" => |state| state.witnesses & WITNESS_FORWARD_FENCED != 0,
        sometimes "timeout_evicted_member" => |state| state.witnesses & WITNESS_TIMEOUT != 0,
        sometimes "metadata_changed_assignment" => |state| state.witnesses & WITNESS_METADATA != 0,
        sometimes "state_replayed" => |state| state.witnesses & WITNESS_REPLAY != 0,
        sometimes "unknown_member_rejected" => |state| state.witnesses & WITNESS_UNKNOWN != 0,
        sometimes "two_members_joined" => |state| state.group.members.len() == 2,
    }

    fn init_states(&self) -> Vec<Self::State> {
        let mut group = ShareGroupState::new("g");
        initialize(&mut group, 1);
        vec![State {
            group,
            origin: Instant::now(),
            clock: 0,
            partitions: 1,
            witnesses: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.group.group_epoch < MAX_EPOCH {
            for member_id in ["a", "b"] {
                if !state.group.members.contains_key(member_id) {
                    actions.push(Action::Join(member_id));
                }
            }
        }
        for member_id in ["a", "b"] {
            if state.group.members.contains_key(member_id) {
                for epoch in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                    actions.push(Action::Heartbeat(member_id, epoch));
                }
                if state.group.group_epoch < MAX_EPOCH {
                    actions.push(Action::Leave(member_id));
                    for partitions in [1, 2] {
                        if partitions != state.partitions {
                            actions.push(Action::MetadataHeartbeat(member_id, partitions));
                        }
                    }
                }
            }
        }
        if state.clock < MAX_CLOCK && state.group.group_epoch < MAX_EPOCH {
            actions.push(Action::TimeoutTick);
        }
        if state.witnesses & WITNESS_REPLAY == 0 {
            actions.push(Action::Replay);
        }
        if state.witnesses & WITNESS_UNKNOWN == 0 {
            actions.push(Action::UnknownHeartbeat);
        }
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.clock <= MAX_CLOCK
            && state.group.group_epoch <= MAX_EPOCH
            && state.group.members.len() <= 2
    }
}

#[test]
fn share_group_membership_model() {
    let checker = run_bfs(ShareModel, "share_group_membership", MAX_DEPTH, MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
