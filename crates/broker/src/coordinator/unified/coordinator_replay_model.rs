//! Bounded Stateright model of consumer group record replay.
//!
//! DRIVEN production code: the [`replay_policy`](super::replay_policy)
//! decisions that every replay adapter applies, Kafka 4.3.1's
//! `getOrMaybeCreatePersisted*Group` lookup ([`persisted_group`]) and the
//! checks of its member, target assignment metadata and group tombstones.
//! MODELED: one consumer group with two members and one regular expression,
//! every record type as a value and as a tombstone, and the whole
//! `ConsumerGroup.createGroupTombstoneRecords` sequence as one action, in
//! every reachable log ordering. A failed check fails the
//! load, as Kafka's `CoordinatorLoaderImpl` does, so a failed state has no
//! successor.

use {
    super::replay_policy::{
        DELETED_ASSIGNMENT_EPOCH, ExistingGroup, GroupLookup, LEAVE_GROUP_MEMBER_EPOCH,
        ModernGroupType, group_tombstone, member_tombstone, persisted_group,
        target_metadata_tombstone,
    },
    crate::model_check::run_bfs,
    stateright::{Checker, Model, Property},
};

const MAX_DEPTH: usize = 40;
const MAX_STATES: usize = 2_000_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 6_376;
const WITNESS_CREATED_BY_CHILD: u8 = 1 << 0;
const WITNESS_IGNORED_TOMBSTONE: u8 = 1 << 1;
const WITNESS_FAILED_LOAD: u8 = 1 << 2;
const WITNESS_DELETED_IN_ORDER: u8 = 1 << 3;
const WITNESS_ORPHAN_TARGET_BLOCKED_DELETE: u8 = 1 << 4;

/// The epoch that a current assignment record in the model carries.
const MEMBER_EPOCH: i32 = 3;
/// The epoch that a target assignment metadata record in the model carries.
const ASSIGNMENT_EPOCH: i32 = 5;
/// Kafka's `TargetAssignmentMetadata.INITIAL` epoch of a new group.
const INITIAL_ASSIGNMENT_EPOCH: i32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Record {
    Group,
    Member(u8),
    TargetEpoch,
    Target(u8),
    Current(u8),
    Regex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Value(Record),
    Tombstone(Record),
    /// Kafka's `ConsumerGroup.createGroupTombstoneRecords`.
    DeleteGroup,
}

/// The replayed consumer group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Group {
    /// Each member's epoch, or `None` when the group does not hold it.
    members: [Option<i32>; 2],
    /// The members that have a target assignment, as bits.
    targets: u8,
    assignment_epoch: i32,
    regex: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct State {
    group: Option<Group>,
    failed: bool,
    witnesses: u8,
}

#[derive(Clone, Debug)]
struct ReplayModel;

fn bit(member: u8) -> u8 { 1 << member }

/// Applies one record to `state` with the production decisions, and returns
/// whether the load failed.
fn replay(state: &mut State, action: Action) -> bool {
    let create = matches!(action, Action::Value(_));
    let existing = state
        .group
        .map(|_| ExistingGroup::Modern(ModernGroupType::Consumer));
    let lookup = persisted_group("g", ModernGroupType::Consumer, existing, create)
        .expect("one consumer group");
    let group = match lookup {
        GroupLookup::Absent => {
            state.witnesses |= WITNESS_IGNORED_TOMBSTONE;
            return false;
        }
        GroupLookup::Create => {
            if !matches!(action, Action::Value(Record::Group)) {
                state.witnesses |= WITNESS_CREATED_BY_CHILD;
            }
            state.group.insert(Group {
                assignment_epoch: INITIAL_ASSIGNMENT_EPOCH,
                ..Group::default()
            })
        }
        GroupLookup::Found => state.group.as_mut().expect("found"),
    };
    let outcome = match action {
        Action::Value(Record::Group) | Action::DeleteGroup => Ok(()),
        Action::Value(Record::Member(member)) => {
            let slot = &mut group.members[usize::from(member)];
            *slot = Some(slot.unwrap_or(0));
            Ok(())
        }
        Action::Value(Record::Current(member)) => {
            group.members[usize::from(member)] = Some(MEMBER_EPOCH);
            Ok(())
        }
        Action::Value(Record::TargetEpoch) => {
            group.assignment_epoch = ASSIGNMENT_EPOCH;
            Ok(())
        }
        Action::Value(Record::Target(member)) => {
            group.targets |= bit(member);
            Ok(())
        }
        Action::Value(Record::Regex) => {
            group.regex = true;
            Ok(())
        }
        Action::Tombstone(Record::Group) => group_tombstone(
            ModernGroupType::Consumer,
            "g",
            group.members.iter().flatten().count(),
            usize::try_from(group.targets.count_ones()).unwrap(),
            group.assignment_epoch,
        )
        .map(|()| {
            state.group = None;
        }),
        Action::Tombstone(Record::Member(member)) => member_tombstone(
            ModernGroupType::Consumer,
            "m",
            group.members[usize::from(member)],
            group.targets & bit(member) != 0,
        )
        .map(|remove| {
            if remove {
                group.members[usize::from(member)] = None;
            }
        }),
        Action::Tombstone(Record::Current(member)) => {
            if let Some(epoch) = group.members[usize::from(member)].as_mut() {
                *epoch = LEAVE_GROUP_MEMBER_EPOCH;
            }
            Ok(())
        }
        Action::Tombstone(Record::TargetEpoch) => {
            target_metadata_tombstone("g", usize::try_from(group.targets.count_ones()).unwrap())
                .map(|()| group.assignment_epoch = DELETED_ASSIGNMENT_EPOCH)
        }
        Action::Tombstone(Record::Target(member)) => {
            group.targets &= !bit(member);
            Ok(())
        }
        Action::Tombstone(Record::Regex) => {
            group.regex = false;
            Ok(())
        }
    };
    outcome.is_err()
}

/// The records of `ConsumerGroup.createGroupTombstoneRecords` for `group`.
fn kafka_deletion(group: &Group) -> Vec<Record> {
    let members: Vec<u8> = (0..2)
        .filter(|member| group.members[usize::from(*member)].is_some())
        .collect();
    let mut records: Vec<Record> = members.iter().map(|m| Record::Current(*m)).collect();
    records.extend(members.iter().map(|m| Record::Target(*m)));
    records.push(Record::TargetEpoch);
    records.extend(members.iter().map(|m| Record::Member(*m)));
    if group.regex {
        records.push(Record::Regex);
    }
    records.push(Record::Group);
    records
}

impl Model for ReplayModel {
    type Action = Action;
    type State = State;

    krabka_macros::model_transition! { last, action, state; {
        if action == Action::DeleteGroup {
            let group = state.group?;
            let orphan_targets = group.targets
                & !(0..2)
                    .filter(|m| group.members[usize::from(*m)].is_some())
                    .fold(0, |bits, m| bits | bit(m));
            let mut failed = false;
            for record in kafka_deletion(&group) {
                failed |= replay(&mut state, Action::Tombstone(record));
                if failed {
                    break;
                }
            }
            // Kafka's tombstones remove every group whose targets all belong
            // to members; a target that a record set for a member the group
            // never held is not among them, and blocks the target metadata
            // tombstone.
            assert2::assert!(failed == (orphan_targets != 0));
            if failed {
                state.failed = true;
                state.witnesses |= WITNESS_ORPHAN_TARGET_BLOCKED_DELETE;
            } else {
                assert2::assert!(state.group.is_none());
                state.witnesses |= WITNESS_DELETED_IN_ORDER;
            }
        } else if replay(&mut state, action) {
            assert2::assert!(matches!(action, Action::Tombstone(_)));
            state.failed = true;
            state.witnesses |= WITNESS_FAILED_LOAD;
        }
        (state != *last).then_some(state)
    }}

    fn init_states(&self) -> Vec<Self::State> { vec![State::default()] }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.failed {
            return;
        }
        let mut records = vec![Record::Group, Record::TargetEpoch, Record::Regex];
        for member in 0..2 {
            records.extend([
                Record::Member(member),
                Record::Target(member),
                Record::Current(member),
            ]);
        }
        for record in records {
            actions.push(Action::Value(record));
            actions.push(Action::Tombstone(record));
        }
        actions.push(Action::DeleteGroup);
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "a failed load has no group left to use",
                |_, state: &State| {
                    !state.failed
                        || state.witnesses
                            & (WITNESS_FAILED_LOAD | WITNESS_ORPHAN_TARGET_BLOCKED_DELETE)
                            != 0
                },
            ),
            Property::always("epochs stay in their domain", |_, state: &State| {
                state.group.is_none_or(|group| {
                    [
                        INITIAL_ASSIGNMENT_EPOCH,
                        ASSIGNMENT_EPOCH,
                        DELETED_ASSIGNMENT_EPOCH,
                    ]
                    .contains(&group.assignment_epoch)
                        && group.members.iter().flatten().all(|epoch| {
                            [0, MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH].contains(epoch)
                        })
                })
            }),
            Property::sometimes("a child record created the group", |_, state: &State| {
                state.witnesses & WITNESS_CREATED_BY_CHILD != 0
            }),
            Property::sometimes(
                "a tombstone of an absent group was ignored",
                |_, state: &State| state.witnesses & WITNESS_IGNORED_TOMBSTONE != 0,
            ),
            Property::sometimes(
                "an out-of-order tombstone failed the load",
                |_, state: &State| state.witnesses & WITNESS_FAILED_LOAD != 0,
            ),
            Property::sometimes(
                "Kafka's deletion order removed the group",
                |_, state: &State| state.witnesses & WITNESS_DELETED_IN_ORDER != 0,
            ),
            Property::sometimes(
                "an orphan target blocked the deletion",
                |_, state: &State| state.witnesses & WITNESS_ORPHAN_TARGET_BLOCKED_DELETE != 0,
            ),
        ]
    }
}

#[test]
fn coordinator_replay_log_orders_follow_kafka() {
    let checker = run_bfs(ReplayModel, "coordinator_replay", MAX_DEPTH, MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
