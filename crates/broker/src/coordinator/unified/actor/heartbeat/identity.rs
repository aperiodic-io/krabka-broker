//! Who a `ConsumerGroupHeartbeat` speaks for: the member-id, instance-id and
//! member-epoch rules of KIP-848 and KIP-345, as Kafka's
//! `GroupMetadataManager` applies them before it touches the group.
//!
//! Every function here is pure. It reads the group and the request and says
//! which member the heartbeat belongs to, or which error Kafka answers.

use {
    crate::{
        codes,
        coordinator::unified::consumer_state::{GroupState, MemberState},
    },
    krabka_protocol::owned::consumer_group_heartbeat_request::{
        ConsumerGroupHeartbeatRequest, TopicPartitions,
    },
};

/// The member epoch of a `ConsumerGroupHeartbeat` that leaves the group.
pub(super) const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// The member epoch of a static member that leaves the group for a while and
/// keeps its assignment for a member with the same instance id.
pub(super) const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// An error code with the message Kafka puts on the response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HeartbeatError {
    pub(super) code: i16,
    pub(super) message: String,
}

impl HeartbeatError {
    fn new(code: i16, message: String) -> Self { Self { code, message } }
}

/// The member a regular heartbeat (member epoch 0 or more) belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Resolved {
    /// No member has this id: a new member joins.
    New,
    /// The member exists and passed the epoch checks.
    Existing,
    /// A static member rejoins with a new member id. The member with id
    /// `previous` holds the instance id and left with epoch -2 (or speaks the
    /// classic protocol), so the new member takes its place.
    Replaces { previous: String },
}

/// Resolves the member of a regular heartbeat, as Kafka's
/// `getOrMaybeSubscribeDynamicConsumerGroupMember` and
/// `getOrMaybeSubscribeStaticConsumerGroupMember` do.
///
/// `member_id` is the request member id, or the id minted for an empty one.
pub(super) fn resolve_member(
    state: &GroupState,
    req: &ConsumerGroupHeartbeatRequest,
    member_id: &str,
) -> Result<Resolved, HeartbeatError> {
    let joining = req.member_epoch == 0;
    let Some(instance_id) = req.instance_id.as_deref() else {
        return match state.members.get(member_id) {
            Some(member) => {
                validate_epoch(member, req.member_epoch, req.topic_partitions.as_deref())
                    .map(|()| Resolved::Existing)
            }
            None if joining => Ok(Resolved::New),
            None => Err(unknown_member(member_id, &state.group_id)),
        };
    };
    let static_member = state
        .current_member_for_instance(instance_id)
        .and_then(|id| state.members.get(id));
    if joining {
        return match static_member {
            None if state.members.contains_key(member_id) => Ok(Resolved::Existing),
            None => Ok(Resolved::New),
            Some(existing)
                if !existing.is_classic()
                    && existing.member_epoch != LEAVE_GROUP_STATIC_MEMBER_EPOCH =>
            {
                Err(HeartbeatError::new(
                    codes::UNRELEASED_INSTANCE_ID,
                    format!(
                        "Static member {member_id} with instance id {instance_id} cannot join the \
                         group because the instance id is owned by {} member.",
                        existing.member_id
                    ),
                ))
            }
            Some(existing) => Ok(Resolved::Replaces {
                previous: existing.member_id.clone(),
            }),
        };
    }
    let existing = static_member.ok_or_else(|| unknown_instance(instance_id))?;
    if existing.member_id != member_id {
        return Err(fenced_instance(member_id, instance_id, &existing.member_id));
    }
    validate_epoch(existing, req.member_epoch, req.topic_partitions.as_deref())
        .map(|()| Resolved::Existing)
}

/// The member a leave heartbeat (member epoch -1 or -2) removes or releases,
/// as Kafka's `consumerGroupLeave` resolves it.
pub(super) fn resolve_leaving_member<'a>(
    state: &'a GroupState,
    req: &ConsumerGroupHeartbeatRequest,
) -> Result<&'a MemberState, HeartbeatError> {
    let Some(instance_id) = req.instance_id.as_deref() else {
        return state
            .members
            .get(&req.member_id)
            .ok_or_else(|| unknown_member(&req.member_id, &state.group_id));
    };
    let existing = state
        .current_member_for_instance(instance_id)
        .and_then(|id| state.members.get(id))
        .ok_or_else(|| unknown_instance(instance_id))?;
    if existing.member_id != req.member_id {
        return Err(fenced_instance(
            &req.member_id,
            instance_id,
            &existing.member_id,
        ));
    }
    Ok(existing)
}

/// Kafka's `throwIfConsumerGroupMemberEpochIsInvalid`.
///
/// Epoch 0 is a rejoin and always passes. A greater epoch is fenced. A smaller
/// epoch is fenced too, unless it is the previous member epoch and the member
/// owns only partitions of its current assignment: the response that carried
/// the new epoch may have been lost.
pub(super) fn validate_epoch(
    member: &MemberState,
    received: i32,
    owned: Option<&[TopicPartitions]>,
) -> Result<(), HeartbeatError> {
    if received == 0 || received == member.member_epoch {
        return Ok(());
    }
    if received > member.member_epoch {
        return Err(fenced_epoch("greater", received, member.member_epoch));
    }
    if received == member.previous_member_epoch && owns_subset(owned, member) {
        return Ok(());
    }
    Err(fenced_epoch("smaller", received, member.member_epoch))
}

/// Kafka's `isSubset`: `true` when every partition the request reports is in
/// the member's assignment. An absent list is not a subset.
fn owns_subset(owned: Option<&[TopicPartitions]>, member: &MemberState) -> bool {
    owned.is_some_and(|owned| {
        owned.iter().all(|topic| {
            member
                .assigned_partitions
                .get(&topic.topic_id)
                .is_some_and(|assigned| topic.partitions.iter().all(|p| assigned.contains(p)))
        })
    })
}

fn fenced_epoch(direction: &str, received: i32, known: i32) -> HeartbeatError {
    HeartbeatError::new(
        codes::FENCED_MEMBER_EPOCH,
        format!(
            "The consumer group member has a {direction} member epoch ({received}) than the one \
             known by the group coordinator ({known}). The member must abandon all its \
             partitions and rejoin."
        ),
    )
}

fn unknown_member(member_id: &str, group_id: &str) -> HeartbeatError {
    HeartbeatError::new(
        codes::UNKNOWN_MEMBER_ID,
        format!("Member {member_id} is not a member of group {group_id}."),
    )
}

fn unknown_instance(instance_id: &str) -> HeartbeatError {
    HeartbeatError::new(
        codes::UNKNOWN_MEMBER_ID,
        format!("Instance id {instance_id} is unknown."),
    )
}

fn fenced_instance(member_id: &str, instance_id: &str, owner: &str) -> HeartbeatError {
    HeartbeatError::new(
        codes::FENCED_INSTANCE_ID,
        format!(
            "Static member {member_id} with instance id {instance_id} was fenced by member \
             {owner}."
        ),
    )
}
