//! Kafka 4.3.1's rules for replaying the records of the consumer, share and
//! streams groups, as pure decisions that the replay adapters apply to the
//! seeds.
//!
//! `GroupMetadataManager.replay` looks a group up with
//! `getOrMaybeCreatePersistedConsumerGroup`, `...ShareGroup` or
//! `...StreamsGroup`. A record with a value creates the group it names, and a
//! tombstone of a group that does not exist is ignored. A group of another
//! type is an `IllegalStateException`, except that a consumer or streams
//! record replaces a simple classic group, the empty classic group that an
//! offset commit creates. A tombstone of a member, of the target assignment
//! metadata, or of the group itself checks that the records before it emptied
//! what it removes, and throws `IllegalStateException` otherwise. Kafka's
//! `CoordinatorLoaderImpl` fails the load on any exception that a replay
//! throws, so each such case is an error here.

use crate::error::BrokerError;

/// Kafka's `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`, which the
/// tombstone of a member's current assignment leaves on the member.
pub(crate) const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// The assignment epoch that the tombstone of the target assignment metadata
/// leaves on a group, and that the group tombstone requires.
pub(crate) const DELETED_ASSIGNMENT_EPOCH: i32 = -1;

/// The type of a group that replays records of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ModernGroupType {
    Consumer,
    Share,
    Streams,
}

/// A group that the replay already holds under the id of a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ExistingGroup {
    /// A classic group. It is simple when it has no protocol type and no
    /// member: Kafka's `ClassicGroup.isSimpleGroup`.
    Classic {
        simple: bool,
    },
    Modern(ModernGroupType),
}

/// The outcome of Kafka's `getOrMaybeCreatePersisted*Group`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GroupLookup {
    /// No group, and the record does not create one: Kafka's
    /// `GroupIdNotFoundException`, which the replay of a tombstone catches.
    Absent,
    /// The group of the wanted type.
    Found,
    /// A new, empty group of the wanted type, which replaces a simple
    /// classic group if one held the id.
    Create,
}

fn illegal_state(message: String) -> BrokerError { BrokerError::Startup(message) }

/// Kafka's `getOrMaybeCreatePersistedConsumerGroup`,
/// `getOrMaybeCreatePersistedShareGroup` and
/// `getOrMaybeCreatePersistedStreamsGroup`.
///
/// # Errors
///
/// Returns Kafka's `IllegalStateException` message when the id belongs to a
/// group of another type that the wanted type does not replace.
pub(crate) fn persisted_group(
    group_id: &str,
    wanted: ModernGroupType,
    existing: Option<ExistingGroup>,
    create_if_not_exists: bool,
) -> Result<GroupLookup, BrokerError> {
    match existing {
        None if create_if_not_exists => Ok(GroupLookup::Create),
        None => Ok(GroupLookup::Absent),
        Some(ExistingGroup::Modern(found)) if found == wanted => Ok(GroupLookup::Found),
        // Kafka replaces a simple classic group even for a tombstone, and its
        // share lookup has no such case.
        Some(ExistingGroup::Classic { simple: true }) if wanted != ModernGroupType::Share => {
            Ok(GroupLookup::Create)
        }
        Some(_) => Err(illegal_state(match wanted {
            ModernGroupType::Consumer => format!("Group {group_id} is not a consumer group"),
            ModernGroupType::Share => format!("Group {group_id} is not a share group."),
            ModernGroupType::Streams => format!("Group {group_id} is not a streams group."),
        })),
    }
}

/// The record whose tombstone removes a member, for the message of a failed
/// check.
fn current_assignment_record(group: ModernGroupType) -> &'static str {
    match group {
        ModernGroupType::Consumer => "ConsumerGroupCurrentMemberAssignmentValue",
        ModernGroupType::Share => "ShareGroupCurrentMemberAssignmentValue",
        ModernGroupType::Streams => "StreamsGroupCurrentMemberAssignmentValue",
    }
}

/// The replay of a member tombstone: Kafka removes a member whose current
/// assignment was tombstoned, which left it at
/// [`LEAVE_GROUP_MEMBER_EPOCH`], and whose target assignment is gone.
/// `member_epoch` is `None` for a member the group does not hold, whose
/// tombstone Kafka ignores; then this returns `Ok(false)`.
///
/// # Errors
///
/// Returns Kafka's `IllegalStateException` message when the member has
/// another epoch or still has a target assignment.
pub(crate) fn member_tombstone(
    group: ModernGroupType,
    member_id: &str,
    member_epoch: Option<i32>,
    has_target_assignment: bool,
) -> Result<bool, BrokerError> {
    let Some(member_epoch) = member_epoch else {
        return Ok(false);
    };
    if member_epoch != LEAVE_GROUP_MEMBER_EPOCH {
        return Err(illegal_state(match group {
            ModernGroupType::Share => format!(
                "Received a tombstone record to delete member {member_id} with invalid leave \
                 group epoch."
            ),
            _ => format!(
                "Received a tombstone record to delete member {member_id} but did not receive {} \
                 tombstone.",
                current_assignment_record(group)
            ),
        }));
    }
    if has_target_assignment {
        return Err(illegal_state(match group {
            ModernGroupType::Share => format!(
                "Received a tombstone record to delete member {member_id} but member exists in \
                 target assignment."
            ),
            ModernGroupType::Consumer => format!(
                "Received a tombstone record to delete member {member_id} but did not receive \
                 ConsumerGroupTargetAssignmentMetadataValue tombstone."
            ),
            ModernGroupType::Streams => format!(
                "Received a tombstone record to delete member {member_id} but did not receive \
                 StreamsGroupTargetAssignmentMetadataValue tombstone."
            ),
        }));
    }
    Ok(true)
}

/// The replay of a target assignment metadata tombstone, which Kafka accepts
/// once every member's target assignment is gone.
///
/// # Errors
///
/// Returns Kafka's `IllegalStateException` message when `targets` members
/// still have one.
pub(crate) fn target_metadata_tombstone(group_id: &str, targets: usize) -> Result<(), BrokerError> {
    if targets == 0 {
        Ok(())
    } else {
        Err(illegal_state(format!(
            "Received a tombstone record to delete target assignment of {group_id} but the \
             assignment still has {targets} members."
        )))
    }
}

/// The replay of a target assignment metadata value: Kafka's
/// `TargetAssignmentMetadata` record refuses an epoch below -1 and a negative
/// timestamp with `IllegalArgumentException`.
///
/// # Errors
///
/// Returns that exception's message.
pub(crate) fn target_metadata(
    assignment_epoch: i32,
    assignment_timestamp_ms: i64,
) -> Result<(), BrokerError> {
    if assignment_epoch < 0 && assignment_epoch != DELETED_ASSIGNMENT_EPOCH {
        return Err(illegal_state(
            "The assignment epoch must be non-negative or -1.".to_owned(),
        ));
    }
    if assignment_timestamp_ms < 0 {
        return Err(illegal_state(
            "The assignment timestamp must be non-negative.".to_owned(),
        ));
    }
    Ok(())
}

/// The replay of a group tombstone: Kafka removes a group whose members, and
/// for a consumer or share group whose target assignment, are gone, and
/// whose target assignment metadata was tombstoned.
///
/// # Errors
///
/// Returns Kafka's `IllegalStateException` message for the first check that
/// fails.
pub(crate) fn group_tombstone(
    group: ModernGroupType,
    group_id: &str,
    members: usize,
    targets: usize,
    assignment_epoch: i32,
) -> Result<(), BrokerError> {
    if members != 0 {
        return Err(illegal_state(format!(
            "Received a tombstone record to delete group {group_id} but the group still has \
             {members} members."
        )));
    }
    if group != ModernGroupType::Streams && targets != 0 {
        return Err(illegal_state(format!(
            "Received a tombstone record to delete group {group_id} but the target assignment \
             still has {targets} members."
        )));
    }
    if assignment_epoch != DELETED_ASSIGNMENT_EPOCH {
        return Err(illegal_state(match group {
            ModernGroupType::Consumer => format!(
                "Received a tombstone record to delete group {group_id} but did not receive \
                 ConsumerGroupTargetAssignmentMetadataValue tombstone."
            ),
            ModernGroupType::Share => format!(
                "Received a tombstone record to delete group {group_id} but target assignment \
                 epoch in invalid."
            ),
            ModernGroupType::Streams => format!(
                "Received a tombstone record to delete group {group_id} but did not receive \
                 StreamsGroupTargetAssignmentMetadataValue tombstone."
            ),
        }));
    }
    Ok(())
}
