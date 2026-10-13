//! The classic `LeaveGroup` and `DeleteGroups` paths.
//!
//! Both remove state from a group and both must persist the result before they
//! answer: a leave that empties a classic group bumps and rewrites its
//! generation, a leave against an upgraded group tombstones the departed
//! member's next-gen records, and a delete appends the tombstones of the
//! group's offsets and its group records, which stops the actor.

use {
    super::{
        ActorServices, ErrorCode, ParkedWaiters, chrono_now_ms,
        downgrade::{downgrade_fencing, downgrades_without},
        heartbeat::fence_members,
        persistence::{flush_classic_metadata, flush_pending},
        retention::append_tombstones,
        waiters::settle_removed_classic_waiters,
    },
    crate::{
        codes,
        coordinator::{
            DeleteGroupError,
            unified::{
                classic_ops, consumer_state::GroupState, group::CoordinatorGroup,
                offsets_log::OffsetsLog,
            },
        },
        error::BrokerError,
        metadata_epoch::next_i32,
    },
    krabka_protocol::owned::{
        leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
    },
    std::collections::{BTreeSet, HashSet},
    tokio::sync::oneshot,
};

#[cfg(test)]
mod tests;

pub(super) async fn handle_classic_leave_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: &LeaveGroupRequest,
    version: i16,
) -> Result<Vec<MemberResponse>, ErrorCode> {
    if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let before_members: Vec<String> = state.members.keys().cloned().collect();
        let responses = classic_ops::handle_leave(
            state,
            request,
            version,
            services.config.classic_initial_rebalance_delay,
        );
        let removed: Vec<String> = before_members
            .into_iter()
            .filter(|member_id| !state.members.contains_key(member_id))
            .collect();
        if !removed.is_empty() && state.members.is_empty() {
            let Some(generation_id) = next_i32(state.generation_id) else {
                *state = previous;
                tracing::warn!(group_id = %state.group_id,
                    "classic LeaveGroup refused because the generation is exhausted");
                return Err(codes::INVALID_REQUEST);
            };
            state.generation_id = generation_id;
            if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                *state = previous;
                tracing::warn!(group_id = %state.group_id, %error,
                    "classic LeaveGroup log write failed");
                return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
            }
        }
        settle_removed_classic_waiters(state, previous.state, &removed, parked);
        return Ok(responses);
    }

    let Some(state) = group.as_consumer_mut() else {
        return Err(codes::UNKNOWN_MEMBER_ID);
    };
    let (responses, removed) = resolve_consumer_classic_leave(state, request, version);
    if removed.is_empty() {
        return Ok(responses);
    }
    // Kafka's `consumerGroupFenceMembers`: the members leave in one batch, or
    // the group downgrades in their place when only classic members remain.
    if downgrades_without(state, services.config, &removed) {
        downgrade_fencing(
            group,
            &removed,
            services.config,
            services.metadata,
            services.offsets_log,
            services.coordinator,
        )
        .await
        .map_err(|error| {
            tracing::warn!(group_id = %group.group_id, %error,
                "hosted classic LeaveGroup downgrade log write failed");
            codes::COORDINATOR_LOAD_IN_PROGRESS
        })?;
        return Ok(responses);
    }
    let pending = fence_members(state, services.metadata, &removed);
    flush_pending(
        state,
        pending,
        services.offsets_log,
        services.coordinator,
        chrono_now_ms(),
    )
    .await
    .map_err(|error| {
        tracing::warn!(group_id = %state.group_id, %error,
            "hosted classic LeaveGroup log write failed");
        codes::COORDINATOR_LOAD_IN_PROGRESS
    })?;
    Ok(responses)
}

/// Resolves a classic `LeaveGroup` request against a consumer-kind group.
/// Resolution happens before any removal, matching Kafka's batch semantics:
/// duplicate valid identities all succeed but produce one set of tombstones.
fn resolve_consumer_classic_leave(
    state: &GroupState,
    request: &LeaveGroupRequest,
    version: i16,
) -> (Vec<MemberResponse>, Vec<String>) {
    let identities: Vec<(&str, Option<&str>)> = if version >= 3 {
        request
            .members
            .iter()
            .map(|member| {
                (
                    member.member_id.as_str(),
                    member.group_instance_id.as_deref(),
                )
            })
            .collect()
    } else {
        vec![(request.member_id.as_str(), None)]
    };
    let mut seen = HashSet::new();
    let mut removed = Vec::new();
    let responses = identities
        .into_iter()
        .map(|(member_id, instance_id)| {
            let (resolved, error_code) = match instance_id {
                Some(instance_id) => match state.current_member_for_instance(instance_id) {
                    Some(current) if member_id.is_empty() || current == member_id => {
                        (Some(current), codes::NONE)
                    }
                    Some(_) => (None, codes::FENCED_INSTANCE_ID),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                None if state.members.contains_key(member_id) => (Some(member_id), codes::NONE),
                None => (None, codes::UNKNOWN_MEMBER_ID),
            };
            if let Some(resolved) = resolved
                && seen.insert(resolved.to_string())
            {
                removed.push(resolved.to_string());
            }
            MemberResponse {
                member_id: member_id.to_string(),
                group_instance_id: instance_id.map(str::to_string),
                error_code,
                ..Default::default()
            }
        })
        .collect();
    (responses, removed)
}

/// The `ClassicDelete` mailbox arm: delete an empty group, classic or
/// KIP-848 consumer, and every offset it holds, in one batch.
///
/// Both kinds answer the way Kafka's `validateDeleteGroup` does: a group with
/// members answers `NON_EMPTY_GROUP`, whether it is a classic group
/// (`ClassicGroup.validateDeleteGroup`) or a consumer group
/// (`ConsumerGroup.validateDeleteGroup`).
///
/// The batch follows Kafka's `GroupCoordinatorShard.deleteGroups`. It starts
/// with an `OffsetCommit` tombstone for each committed offset
/// (`OffsetMetadataManager.deleteAllOffsets`), then one for each key that an
/// open transaction wrote and that has no committed offset, and ends with the
/// group tombstones: the k2 `GroupMetadata` tombstone of a classic group, or
/// the `ConsumerGroupMetadata` and `ConsumerGroupTargetAssignmentMetadata`
/// tombstones of a consumer group. An empty consumer group holds no member
/// records, because each member's records are tombstoned when it leaves. The
/// open transaction keys include the ones a `TxnOffsetCommit` has reserved but
/// not marked yet. Without the offset tombstones the commits stay live in
/// `__consumer_offsets`, and a replay seeds the deleted group again from them.
///
/// Returns the actor's keep-running flag: a deleted group stops its actor.
pub(super) async fn handle_classic_delete_message(
    group: &CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    reply: oneshot::Sender<Result<(), DeleteGroupError>>,
) -> bool {
    if group.has_members() {
        let _ = reply.send(Err(DeleteGroupError::NonEmpty));
        return true;
    }
    let group_id = group.group_id.clone();
    match append_group_delete(group, offsets_log, chrono_now_ms()).await {
        Ok(()) => {
            let _ = reply.send(Ok(()));
            false
        }
        Err(error) => {
            tracing::warn!(%group_id, %error, "DeleteGroups tombstone write failed");
            let _ = reply.send(Err(DeleteGroupError::Internal));
            true
        }
    }
}

/// Appends the records that delete `group`: its offset tombstones, then its
/// group tombstone. The offset keys are sorted, so the batch does not depend on
/// map order.
async fn append_group_delete(
    group: &CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    now_ms: i64,
) -> Result<(), BrokerError> {
    let keys: Vec<(String, i32)> = group
        .committed_offsets
        .keys()
        .cloned()
        .chain(group.unresolved_txn_keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    append_tombstones(
        offsets_log,
        &group.group_id,
        &keys,
        Some(&group.kind),
        now_ms,
    )
    .await
}
