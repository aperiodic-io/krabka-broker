//! The actor's message router.
//!
//! [`handle_actor_message`] is the single `match` over [`GroupActorMessage`].
//! It owns no policy of its own: every arm dispatches on the group's LIVE kind
//! and delegates to the module that implements that RPC. The return value is
//! the actor loop's keep-running flag.

use krabka_protocol::{
    owned::{heartbeat_request::HeartbeatRequest, leave_group_request::LeaveGroupRequest},
    records::RecordBatch,
};
use tokio::sync::oneshot;

use super::{
    ActorServices, ErrorCode, GroupActorMessage, ParkedWaiters,
    classic_join::handle_classic_join_message,
    classic_leave::{handle_classic_delete_message, handle_classic_leave_message},
    classic_sync::handle_classic_sync_message,
    commit_validation::validate_commit_message,
    heartbeat::handle_actor_heartbeat,
    messages::{LeaveResult, classic_leave_result},
    metadata_update::on_metadata_update,
    offset_delete::offset_delete_guard,
    retention::handle_reap_message,
    seed::apply_seed,
    topic_deletion::reply_delete_topic_offsets,
    views::{build_classic_view, build_describe, inspect_any},
    waiters::drain_parked_for_unload,
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, classic_ops, classic_state::OffsetEntry, group::CoordinatorGroup,
        migration, offsets_log::OffsetsLog,
    },
};

fn handle_classic_heartbeat_message(
    group: &mut CoordinatorGroup,
    request: &HeartbeatRequest,
) -> ErrorCode {
    if let Some(state) = group.as_classic_mut() {
        classic_ops::handle_heartbeat(state, request)
    } else if let Some(state) = group.as_consumer_mut() {
        migration::serve_classic_heartbeat(state, request)
    } else {
        codes::UNKNOWN_MEMBER_ID
    }
}

/// Answers a classic `LeaveGroup` and returns the actor's keep-running flag,
/// which is false only when a consumer-kind group's leave fails.
async fn reply_classic_leave(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: &LeaveGroupRequest,
    version: i16,
    reply: oneshot::Sender<LeaveResult>,
) -> bool {
    let consumer_kind = group.is_consumer();
    let result = handle_classic_leave_message(group, parked, services, request, version).await;
    let keep_running = result.is_ok() || !consumer_kind;
    let _ = reply.send(classic_leave_result(version, result));
    keep_running
}

/// Appends an `OffsetCommit` batch, and records its offsets in the group
/// only when the append is durable.
async fn commit_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    batch: RecordBatch,
    entries: Vec<((String, i32), OffsetEntry)>,
) -> Result<(), ErrorCode> {
    match offsets_log.append(&group.group_id, batch).await {
        Ok(()) => {
            group.committed_offsets.extend(entries);
            Ok(())
        }
        Err(error) => {
            tracing::error!(group_id = %group.group_id, %error, "OffsetCommit append failed");
            Err(codes::from_broker_error(&error))
        }
    }
}

pub(super) async fn handle_actor_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    message: GroupActorMessage,
) -> bool {
    match message {
        GroupActorMessage::Heartbeat {
            request,
            client_id,
            client_host,
            regex_resolver,
            reply,
        } => {
            handle_actor_heartbeat(
                group,
                services,
                request,
                ClientIdentity {
                    id: &client_id,
                    host: &client_host,
                },
                &*regex_resolver,
                reply,
            )
            .await
        }
        GroupActorMessage::ValidateCommit { commit, reply } => {
            let _ = reply.send(validate_commit_message(group, &commit));
            true
        }
        GroupActorMessage::Describe { reply } => {
            if let Some(state) = group.as_consumer() {
                let _ = reply.send(build_describe(state));
            }
            true
        }
        GroupActorMessage::ClassicJoin {
            req,
            version,
            client_id,
            client_host,
            regex_resolver,
            reply,
        } => {
            handle_classic_join_message(
                group,
                parked,
                services,
                req,
                version,
                &client_id,
                &client_host,
                &*regex_resolver,
                reply,
            )
            .await
        }
        GroupActorMessage::ClassicSync { req, reply } => {
            handle_classic_sync_message(group, parked, services, req, reply).await;
            true
        }
        GroupActorMessage::ClassicHeartbeat { req, reply } => {
            let code = handle_classic_heartbeat_message(group, &req);
            let _ = reply.send(code);
            true
        }
        GroupActorMessage::ClassicLeave {
            req,
            version,
            reply,
        } => reply_classic_leave(group, parked, services, &req, version, reply).await,
        GroupActorMessage::ClassicDelete { reply } => {
            handle_classic_delete_message(group, services.offsets_log, reply).await
        }
        GroupActorMessage::ClassicInspect { reply } => {
            if let Some(state) = group.as_classic() {
                let _ = reply.send(build_classic_view(state));
            }
            true
        }
        GroupActorMessage::OffsetDeleteGuard { reply } => {
            let _ = reply.send(offset_delete_guard(group));
            true
        }
        GroupActorMessage::InspectAny { reply } => {
            if let Some(snapshot) = inspect_any(group, services.metadata) {
                let _ = reply.send(snapshot);
            }
            true
        }
        GroupActorMessage::CommitOffsets {
            batch,
            entries,
            reply,
        } => {
            let result = commit_offsets(group, services.offsets_log, batch, entries).await;
            let _ = reply.send(result);
            true
        }
        GroupActorMessage::UpdateCommitted { entries, reply } => {
            group.committed_offsets.extend(entries);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::FetchOffsets { reply } => {
            let _ = reply.send(group.offsets());
            true
        }
        GroupActorMessage::FetchOffsetsForMember { member, reply } => {
            let _ = reply.send(group.offsets_for_member(&member));
            true
        }
        GroupActorMessage::RemoveCommitted { keys, reply } => {
            for key in keys {
                group.committed_offsets.remove(&key);
            }
            let _ = reply.send(());
            true
        }
        GroupActorMessage::DeleteTopicOffsets { topics, reply } => {
            reply_delete_topic_offsets(group, services.offsets_log, &topics, reply).await
        }
        GroupActorMessage::MetadataUpdate { topics } => {
            on_metadata_update(group, &topics);
            true
        }
        GroupActorMessage::AddPendingTxnOffsets {
            producer_id,
            written_at,
            keys,
            reply,
        } => {
            group.add_pending_txn_offsets(producer_id, written_at, keys);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::TxnOffsetReservation(reservation) => reservation.apply(group),
        GroupActorMessage::ResolveTxnOffsets {
            producer_id,
            resolved_through,
            committed,
            reply,
        } => {
            group.committed_offsets.extend(committed);
            group.resolve_pending_txn_offsets(producer_id, resolved_through);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::ReapExpiredOffsets {
            now_ms,
            retention_ms,
            empty_grace_ms,
            reply,
        } => {
            handle_reap_message(
                group,
                services.offsets_log,
                services.coordinator,
                now_ms,
                retention_ms,
                empty_grace_ms,
                reply,
            )
            .await
        }
        GroupActorMessage::Seed(seed) => {
            if let Some(state) = group.as_consumer_mut() {
                apply_seed(state, seed);
            }
            true
        }
        GroupActorMessage::ClassicSeed(seeded) => {
            *group = *seeded;
            true
        }
        GroupActorMessage::Shutdown(reply) => {
            // Kafka's `GroupMetadataManager.onUnloaded`: a parked `JoinGroup`
            // or `SyncGroup` learns that this broker no longer coordinates the
            // group, so its client looks the coordinator up again instead of
            // retrying here.
            drain_parked_for_unload(&mut parked.joiners, &mut parked.followers);
            let _ = reply.send(());
            false
        }
        #[cfg(test)]
        GroupActorMessage::TestForceConsumerKind => {
            *group = CoordinatorGroup::new_consumer(group.group_id.clone());
            true
        }
        #[cfg(test)]
        GroupActorMessage::TestEmptySinceMs { reply } => {
            let _ = reply.send(group.empty_since_ms);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;

    use super::*;
    use crate::coordinator::unified::actor::test_support::{make_coordinator, rpc};

    /// Kafka's `GroupMetadataManager.onUnloaded` answers every awaiting
    /// `JoinGroup` of a `PreparingRebalance` group, under the member's own id,
    /// and every awaiting `SyncGroup` of a `CompletingRebalance` group with
    /// `NOT_COORDINATOR`, so the client looks the coordinator up again instead
    /// of joining the old broker once more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_answers_parked_joiners_and_followers_not_coordinator() {
        use krabka_protocol::owned::sync_group_request::SyncGroupRequest;

        use crate::coordinator::unified::actor::{JoinResult, SyncResult, test_support::rpc};

        // A member that waits in `JoinGroup`, behind the initial delay.
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");
        coord.mark_classic("g");
        let member_id = rpc::classic_join(&handle, "", "t").await.member_id;
        let join_rx = rpc::begin(&handle, |reply| {
            rpc::classic_join_message(&member_id, "t", reply)
        })
        .await;
        rpc::shutdown(&handle).await;
        assert!(
            join_rx.await.unwrap()
                == JoinResult {
                    error_code: codes::NOT_COORDINATOR,
                    member_id,
                    ..JoinResult::default()
                }
        );

        // A follower that waits in `SyncGroup` for the leader.
        let (coord, _log) = make_coordinator();
        let (handle, generation) =
            crate::coordinator::unified::actor::test_support::seed_completing_classic(
                &coord,
                &["m1", "m2"],
            );
        let sync_rx = rpc::begin_classic_sync(
            &handle,
            SyncGroupRequest {
                group_id: "g".into(),
                generation_id: generation,
                member_id: "m2".into(),
                ..Default::default()
            },
        )
        .await;
        rpc::shutdown(&handle).await;
        assert!(
            sync_rx.await.unwrap()
                == SyncResult {
                    error_code: codes::NOT_COORDINATOR,
                    ..SyncResult::default()
                }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_seed_hydrates_group_and_blocks_delete_when_nonempty() {
        use crate::coordinator::unified::{
            classic_state::{ClassicGroup as ClassicState, OffsetEntry},
            group::{CoordinatorGroup, GroupKind},
        };
        let (coord, _log) = make_coordinator();

        let mut cs = ClassicState::new("g");
        cs.add_member(crate::coordinator::unified::actor::test_support::classic_member("m1"));
        let group = Box::new(CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(cs),
            [(
                ("t".to_string(), 0),
                OffsetEntry {
                    offset: Offset(7),
                    leader_epoch: 0,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                    expire_timestamp_ms: None,
                    topic_id: None,
                },
            )]
            .into(),
        ));
        coord.seed_classic("g", group);

        // Seeded committed offsets and member are visible.
        let handle = coord.find("g").expect("seeded actor");
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::FetchOffsets { reply: tx }).await;
        assert!(
            rx.await
                .unwrap()
                .committed
                .get(&("t".to_string(), 0))
                .unwrap()
                .offset
                == 7
        );
        // Non-empty group cannot be deleted.
        assert!(
            coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty)
        );
    }
}
