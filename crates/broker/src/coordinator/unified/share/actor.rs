//! Per-share-group tokio actor (KIP-932). Owns [`ShareGroupState`] for one
//! share group. Heartbeats arrive as mpsc messages, and responses go back
//! through oneshot channels.
//!
//! It mirrors the consumer next-gen [`crate::coordinator::unified::actor`]
//! without any offset-validation or partition-revocation machinery.
//! Share-group assignment is non-exclusive, so a member's epoch advances
//! straight to the group epoch with no acknowledgement round-trip.
//!
//! This file is the module root. It holds the actor's identity — the mailbox
//! protocol, the handle, and the `tokio::select!` loop — while each request
//! path and each persistence concern lives in its own submodule.

use {
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::sync::Arc,
    tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
    },
};

mod admin_offsets;
mod assignment;
mod delete;
mod describe;
mod heartbeat;
mod records;
mod response;
mod seed;
mod session;
mod share_state;

#[cfg(test)]
mod group_config_tests;
#[cfg(test)]
mod test_support;

#[cfg(test)]
#[path = "actor/share_group_model.rs"]
mod share_group_model;

pub(crate) use self::admin_offsets::{DeleteTopicOutcome, ResetPartition};
pub use self::describe::{ShareDescribeMember, ShareDescribeView};
use {
    self::{
        admin_offsets::{delete_offsets, reset_offsets},
        describe::build_describe,
        heartbeat::handle_heartbeat,
        records::{
            PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from,
        },
        seed::apply_seed,
        session::handle_session_tick,
        share_state::apply_initialized,
    },
    super::{config::ShareGroupConfig, state::ShareGroupState},
    crate::{
        coordinator::{
            DeleteGroupError,
            unified::{actor::MetadataProvider, offsets_log::write_failure_code},
        },
        task_util::spawn_mailbox,
    },
};

#[derive(Debug)]
pub enum ShareGroupActorMessage {
    Heartbeat {
        request: ShareGroupHeartbeatRequest,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<ShareGroupHeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<ShareDescribeView>,
    },
    ResetOffsets {
        requests: Vec<ResetPartition>,
        reply: oneshot::Sender<Result<Vec<i16>, i16>>,
    },
    /// `DeleteShareGroupOffsets` for the named topics. The reply holds each
    /// topic's outcome in Kafka's response-row order.
    DeleteOffsets {
        topic_names: Vec<String>,
        reply: oneshot::Sender<Result<Vec<(String, DeleteTopicOutcome)>, i16>>,
    },
    /// `DeleteGroups`. On success the actor stops, and the coordinator drops
    /// its registry entries.
    Delete {
        reply: oneshot::Sender<Result<(), DeleteGroupError>>,
    },
    Seed(super::super::ShareGroupSeed),
    /// The outcome of the share-state `Initialize` calls that a heartbeat
    /// started: each share partition, and whether the persister initialized
    /// it. The actor sends it to itself, from the task that ran the calls.
    ShareStateInitialized(Vec<((krabka_protocol::primitives::uuid::Uuid, i32), bool)>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct ShareGroupActorHandle {
    pub tx: mpsc::Sender<ShareGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl ShareGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<ShareGroupConfig>,
        metadata: Arc<dyn MetadataProvider>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, task) = spawn_mailbox(config.actor_mailbox_capacity, |rx| {
            actor_loop(group_id, config, metadata, coordinator, rx)
        });
        Self { tx, _task: task }
    }
}

crate::coordinator::unified::config::effective_group_config! {
    /// The settings `group_id` runs with: the `share.*` overrides of its group
    /// config in the current metadata image over the broker's `config`. A
    /// coordinator with no metadata source runs every group with the broker
    /// values.
    fn effective_config(ShareGroupConfig);
}

async fn actor_loop(
    group_id: String,
    config: Arc<ShareGroupConfig>,
    metadata: Arc<dyn MetadataProvider>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<ShareGroupActorMessage>,
) {
    let offsets_log = coordinator.offsets_log.clone();
    let mut state = ShareGroupState::new(group_id);
    let mut tick_period = config.heartbeat_interval;
    let mut tick = tokio::time::interval(tick_period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // The session-expiry tick follows the group's heartbeat interval, so a
        // `share.heartbeat.interval.ms` override takes effect at the next turn.
        let heartbeat_interval =
            effective_config(&config, &coordinator, &state.group_id).heartbeat_interval;
        if heartbeat_interval != tick_period {
            tick_period = heartbeat_interval;
            tick = tokio::time::interval(tick_period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        }
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    ShareGroupActorMessage::Heartbeat { request, client_id, client_host, reply } => {
                        // The settings this group runs with when the heartbeat
                        // arrives, not those of the turn before: a `share.*`
                        // override in its group config takes effect at the
                        // next message or tick.
                        let effective = effective_config(&config, &coordinator, &state.group_id);
                        match handle_heartbeat(
                            &mut state,
                            &effective,
                            &*metadata,
                            &*offsets_log,
                            &coordinator,
                            &request,
                            super::super::ClientIdentity {
                                id: &client_id,
                                host: &client_host,
                            },
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %state.group_id,
                                    error = %e,
                                    "share-group actor exiting after log-write failure",
                                );
                                let _ = reply.send(ShareGroupHeartbeatResponse {
                                    error_code: write_failure_code(&e),
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    ShareGroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&state));
                    }
                    ShareGroupActorMessage::ResetOffsets { requests, reply } => {
                        let result = reset_offsets(&mut state, &coordinator, requests).await;
                        let _ = reply.send(result);
                    }
                    ShareGroupActorMessage::DeleteOffsets { topic_names, reply } => {
                        let result = delete_offsets(&mut state, &coordinator, topic_names).await;
                        let _ = reply.send(result);
                    }
                    ShareGroupActorMessage::Delete { reply } => {
                        let result = delete::delete_group(&mut state, &*offsets_log, &coordinator).await;
                        let deleted = result.is_ok();
                        let _ = reply.send(result);
                        if deleted {
                            break;
                        }
                    }
                    ShareGroupActorMessage::Seed(seed) => {
                        apply_seed(&mut state, seed);
                    }
                    ShareGroupActorMessage::ShareStateInitialized(outcomes) => {
                        apply_initialized(
                            &mut state,
                            &outcomes,
                            &*offsets_log,
                            &coordinator,
                            chrono_now_ms(),
                        )
                        .await;
                    }
                    ShareGroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                let effective = effective_config(&config, &coordinator, &state.group_id);
                if handle_session_tick(&mut state, &effective, &*metadata, &*offsets_log, &coordinator).await.is_err() {
                    break;
                }
            }
        }
    }
}
