//! Per-group tokio actor.
//!
//! The actor owns one unified `Group`: either the classic 5-state machine or
//! the next-gen epoch machine. Next-gen heartbeats are non-parking mpsc
//! messages with `oneshot` replies.
//!
//! Classic `JoinGroup` and `SyncGroup` parking becomes a park/wake message
//! protocol. The actor holds the reply `oneshot::Sender` in a parked registry
//! and resolves it at the rebalance boundary: the rebalance-deadline timer, an
//! all-members-joined early-complete, or the leader's `SyncGroup`.
//!
//! This file is the module root. It holds the actor's identity — the handle,
//! the mailbox loop, and the shared services and constants — while each RPC
//! path lives in its own submodule.

use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

mod classic_join;
mod classic_leave;
mod classic_sync;
mod commit_validation;
mod dispatch;
mod downgrade;
mod heartbeat;
mod member_state;
mod messages;
mod metadata_update;
mod offset_delete;
mod pending_records;
mod persistence;
mod regex_resolution;
mod retention;
mod seed;
mod tick;
mod topic_deletion;
mod views;
mod waiters;

#[cfg(test)]
mod group_config_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::{
    classic_join::append_error_code as join_append_error_code,
    commit_validation::validate_commit,
    pending_records::{GroupTombstone, PendingRecords, group_tombstone_keys},
    persistence::{classic_group_metadata_record, full_pending_records},
};
pub use self::{
    commit_validation::{CommitFence, CommitRequest},
    messages::{
        GroupActorMessage, JoinResult, JoinResultMember, LeaveResult, SyncResult,
        TxnOffsetReservation,
    },
    offset_delete::SubscribedTopics,
    retention::ReapOutcome,
    views::{ClassicMemberView, ClassicView, DescribeMember, DescribeView},
};
use self::{
    dispatch::handle_actor_message,
    tick::{handle_actor_tick, handle_classic_sync_expiry},
    waiters::complete_classic_rebalance,
};
use crate::{
    coordinator::unified::{
        GroupCoordinator,
        config::NextGenConfig,
        group::{ConsumerState, CoordinatorGroup},
        offsets_log::OffsetsLog,
        reconciler::ReconcileInput,
    },
    time_util,
};

/// A Kafka wire `error_code` value, as carried in response `error_code`
/// fields. The values live in [`crate::codes`].
pub type ErrorCode = i16;

/// Fallback session timeout (30 s, in ms) for a persisted or requested classic
/// `session_timeout_ms` that the target type cannot represent.
const FALLBACK_SESSION_TIMEOUT_MS: u64 = 30_000;

/// [`FALLBACK_SESSION_TIMEOUT_MS`] as the persisted/wire `i32` field.
const FALLBACK_SESSION_TIMEOUT_MS_I32: i32 = 30_000;

/// Fallback rebalance timeout (60 s, in ms) for a persisted or requested
/// `rebalance_timeout_ms` that the target type cannot represent.
const FALLBACK_REBALANCE_TIMEOUT_MS: u64 = 60_000;

/// [`FALLBACK_REBALANCE_TIMEOUT_MS`] as the persisted/wire `i32` field.
const FALLBACK_REBALANCE_TIMEOUT_MS_I32: i32 = 60_000;

/// Fallback `heartbeat_interval_ms` of 5 s, the KIP-848 default heartbeat
/// interval. The actor reports it when the configured interval overflows the
/// wire `i32`.
const FALLBACK_HEARTBEAT_INTERVAL_MS: i32 = 5_000;

/// Names this actor's session-expiry cadence in the timer-failure logs that
/// [`time_util::arm`] and [`time_util::fired`] emit, so an operator can tell
/// which loop lost its ticker.
const TICK_TASK: &str = "consumer group actor";

/// Which protocol an actor's `Group` speaks. This value is fixed at spawn. The
/// handle exposes it so that the coordinator can route or reject
/// cross-protocol RPCs, and filter admin views, without a message to the
/// actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupKindTag {
    Classic,
    Consumer,
}

#[derive(Debug)]
pub struct GroupActorHandle {
    pub tx: mpsc::Sender<GroupActorMessage>,
    /// Spawn-time protocol hint, fixed for the actor's lifetime. A KIP-848
    /// live migration can flip the group's kind in place after spawn, which
    /// leaves this field stale. The code therefore reads it ONLY for
    /// spawn-time wiring (the initial `CoordinatorGroup::new_classic` or
    /// `new_consumer`) and for replay assertions. Every routing and validation
    /// decision dispatches on the actor's LIVE `group.kind` inside the actor,
    /// never on this field.
    pub kind: GroupKindTag,
    _task: JoinHandle<()>,
}

impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        kind: GroupKindTag,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        let (tx, task) = crate::task_util::spawn_mailbox(config.actor_mailbox_capacity, |rx| {
            actor_loop(
                group_id,
                kind,
                config,
                metadata_provider,
                offsets_log,
                coordinator,
                rx,
            )
        });
        Self {
            tx,
            kind,
            _task: task,
        }
    }
}

pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> ReconcileInput;

    /// The name of the topic with `topic_id`, or `None` when no such topic
    /// exists: Kafka's `TopicIds.TopicResolver.name`.
    ///
    /// A member's reconciliation asks it once per topic of the member's target
    /// and assignment, so the broker's provider answers from the image
    /// directly. This default builds a whole [`snapshot`](Self::snapshot),
    /// which only the static providers of the tests use.
    fn topic_name(&self, topic_id: &krabka_protocol::primitives::uuid::Uuid) -> Option<String> {
        self.snapshot()
            .topic_id_by_name
            .into_iter()
            .find_map(|(name, id)| (id == *topic_id).then_some(name))
    }
}

/// Parked classic-protocol waiters for one group.
#[derive(Default)]
struct ParkedWaiters {
    /// Parked `JoinGroup` handlers, keyed by `member_id`, holding the reply
    /// sender.
    joiners: HashMap<String, oneshot::Sender<JoinResult>>,
    /// Parked `SyncGroup` followers, keyed by `member_id`, holding the reply
    /// sender.
    followers: HashMap<String, oneshot::Sender<SyncResult>>,
}

#[derive(Clone, Copy)]
struct ActorServices<'a> {
    config: &'a NextGenConfig,
    metadata: &'a dyn MetadataProvider,
    offsets_log: &'a dyn OffsetsLog,
    coordinator: &'a GroupCoordinator,
}

async fn actor_loop(
    group_id: String,
    kind: GroupKindTag,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<GroupCoordinator>,
    rx: mpsc::Receiver<GroupActorMessage>,
) {
    // The `Group` the loop hands back belongs to nobody once the actor is
    // gone, so the spawned task drops it. See `run_actor`.
    run_actor(
        group_id,
        kind,
        config,
        metadata,
        offsets_log,
        coordinator,
        rx,
    )
    .await;
}

/// The mailbox loop, returning the `Group` as it stood when the actor stopped.
///
/// The spawned task drops that value, because nothing outside the actor may
/// touch a `Group` it no longer owns. A test keeps it, so that the
/// offset-retention clock every exit stamps — including the one the loop takes
/// when its session-expiry ticker cannot even be armed — is readable after the
/// loop has gone away.
async fn run_actor(
    group_id: String,
    kind: GroupKindTag,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<GroupCoordinator>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) -> CoordinatorGroup {
    let mut group = match kind {
        GroupKindTag::Classic => CoordinatorGroup::new_classic(group_id),
        GroupKindTag::Consumer => CoordinatorGroup::new_consumer(group_id),
    };
    let mut parked = ParkedWaiters::default();
    // A single configured session-expiry tick, kind-agnostic. The tick arm
    // dispatches on the live `group.kind`, so the cadence must not depend on
    // the spawn-time kind. Expiry is a
    // `last_seen`-vs-`session_timeout` comparison, so its cadence only changes
    // how often we check, never the outcome.
    //
    // Driven through the injected `Timer` (production: real time; tests: a
    // controlled manual timeline). Each deadline is armed to the configured
    // interval only after the tick body runs (`MissedTickBehavior::Delay`
    // semantics — a slow tick never bursts). The future is held across loop
    // iterations so an inbound-message stream never resets the tick schedule
    // (matching the persistent `Interval`). It owns its registration outright
    // instead of borrowing the timer, so nothing has to be cloned out of
    // `config` to keep it alive across the arms.
    //
    // The FIRST deadline is a full interval, not the zero-duration one that
    // would reproduce `tokio::time::interval`'s t=0 tick. A sweep at t=0 can
    // only read `last_seen` values that predate this actor, which is exactly
    // the just-replayed group whose members were restored from the log: it
    // evicts every one of them, empties the group, and reaps the actor before
    // it has served a single request. Deferring the first sweep by one
    // interval costs nothing, because as the paragraph above says the cadence
    // "only changes how often we check, never the outcome" -- a member that is
    // genuinely past its session timeout is still past it one interval later.
    //
    // The previous sleeper hid this. Its `tokio::time::sleep(Duration::ZERO)`
    // still went through the runtime's timer driver, so the "immediate" tick
    // actually landed a driver tick later — enough of a window that a caller
    // reliably got in first. `Timer::after(Duration::ZERO)` returns an
    // already-complete future instead, which turned that window into a coin
    // flip against `tokio::select!`'s randomised branch order.
    let Some(mut tick) = time_util::arm(&*config.timer, config.session_expiry_tick, TICK_TASK)
    else {
        // No ticker, no actor. Take the same exit the loop body takes below,
        // so the offset-retention clock is stamped once before we go away.
        group.observe_membership(chrono_now_ms());
        rx.close();
        while rx.recv().await.is_some() {}
        return group;
    };
    let services = ActorServices {
        config: &config,
        metadata: &*metadata,
        offsets_log: &*offsets_log,
        coordinator: &coordinator,
    };

    loop {
        let deadline = classic_deadline(&group);
        let rebalance_deadline = group
            .as_consumer()
            .and_then(ConsumerState::next_rebalance_deadline);
        let keep_running = tokio::select! {
            msg = rx.recv() => match msg {
                None => false,
                Some(msg) => {
                    let effective = effective_config(&config, &coordinator, &group.group_id);
                    let services = ActorServices { config: &effective, ..services };
                    handle_actor_message(&mut group, &mut parked, services, msg).await
                }
            },
            outcome = &mut tick => {
                // A ticker that failed, or that cannot be armed again, takes
                // this actor's session-expiry sweep with it. Report it as
                // "stop" rather than returning outright: the loop tail still
                // has to stamp `observe_membership` and break cleanly.
                if time_util::fired(outcome, TICK_TASK) {
                    let effective = effective_config(&config, &coordinator, &group.group_id);
                    let services = ActorServices { config: &effective, ..services };
                    let keep_running =
                        handle_actor_tick(&mut group, &mut parked, services).await;
                    match time_util::arm(&*config.timer, config.session_expiry_tick, TICK_TASK) {
                        Some(next) => {
                            tick = next;
                            keep_running
                        }
                        None => false,
                    }
                } else {
                    false
                }
            }
            () = crate::time_util::sleep_until_opt(rebalance_deadline) => {
                // KIP-848: a member's rebalance timeout fired. Run the sweep
                // now instead of at the next session tick, so the partitions it
                // did not revoke reach their new owner on time.
                let effective = effective_config(&config, &coordinator, &group.group_id);
                let services = ActorServices { config: &effective, ..services };
                handle_actor_tick(&mut group, &mut parked, services).await
            }
            () = crate::time_util::sleep_until_opt(classic_sync_deadline(&group)) => {
                // Kafka's pending-sync timer: a member never sent SyncGroup.
                let effective = effective_config(&config, &coordinator, &group.group_id);
                let services = ActorServices { config: &effective, ..services };
                handle_classic_sync_expiry(&mut group, &mut parked, services).await
            }
            () = crate::time_util::sleep_until_opt(deadline) => {
                // Classic rebalance deadline fired: extend Kafka's initial
                // delay, or complete with whoever is here.
                if let Some(state) = group.as_classic_mut()
                    && state.rebalance_deadline_fired(
                        config.classic_initial_rebalance_delay,
                        Instant::now(),
                    )
                    && complete_classic_rebalance(
                        state,
                        &mut parked.joiners,
                        &mut parked.followers,
                    )
                    && let Err(error) =
                        persistence::flush_classic_metadata(state, &*offsets_log).await
                {
                    // Kafka only warns here too: an empty generation that
                    // did not persist leaves the previous one, whose members
                    // expire.
                    tracing::warn!(group_id = %state.group_id, %error,
                        "classic empty-generation log write failed");
                }
                true
            }
        };
        // One place maintains `empty_since_ms`, after whatever the turn did to
        // membership. Every join, leave, eviction, seed, and in-place kind flip
        // therefore keeps the offset-retention clock honest without knowing it
        // exists.
        group.observe_membership(chrono_now_ms());
        if !keep_running {
            break;
        }
    }
    // A sender can enqueue using a permit reserved before closure. Drain those
    // sends too, dropping their replies, so callers cannot wait on a dead actor.
    rx.close();
    while rx.recv().await.is_some() {}
    group
}

crate::coordinator::unified::config::effective_group_config! {
    /// The settings `group_id` runs with: the `consumer.*` overrides of its group
    /// config in the current metadata image over the broker's `config`.
    ///
    /// A classic group and a classic member ignore them: Kafka's classic groups
    /// read only the broker-wide `group.min.session.timeout.ms` and
    /// `group.max.session.timeout.ms`, and a coordinator with no metadata source
    /// runs every group with the broker values.
    fn effective_config(NextGenConfig);
}

/// The classic rebalance-completion deadline, if a rebalance is open.
fn classic_deadline(group: &CoordinatorGroup) -> Option<Instant> {
    group.as_classic().and_then(|s| s.rebalance_deadline)
}

/// The classic pending-sync deadline, if a generation still awaits members'
/// `SyncGroup`.
fn classic_sync_deadline(group: &CoordinatorGroup) -> Option<Instant> {
    group.as_classic().and_then(|s| s.sync_deadline)
}

// `reconciler_model` drives the real heartbeat step, so these are re-exported
// for it alone.
#[cfg(test)]
pub(crate) use self::{
    heartbeat::{HeartbeatStep, step_heartbeat},
    regex_resolution::RegexResolution,
};
/// The wall-clock reading this actor subtree stamps records and deadlines
/// with, in milliseconds since the Unix epoch. It reads `std::time`, not
/// chrono, which the name predates.
///
/// This is deliberately **not** [`crate::time_util::now_ms`], which is
/// otherwise the same function. The two disagree on one arm: a duration that
/// overflows `i64` milliseconds saturates to `i64::MAX` there and to `0` here.
/// Collapsing this into the shared helper would therefore change what the
/// offset-retention clock reads, so it stays separate until someone decides
/// which answer that clock wants.
///
/// That arm needs a system clock set roughly 292 million years ahead to reach,
/// and the two answers fail in opposite directions: `0` dates a group to the
/// epoch, so its offsets expire at once, while `i64::MAX` dates it to now, so
/// they never expire. The share-group actor and transaction handlers share this
/// same overflow-to-zero reading.
use crate::txn::util::now_millis as chrono_now_ms;

#[cfg(test)]
#[path = "reconciliation_model_support.rs"]
pub(crate) mod reconciliation_model_support;

#[cfg(test)]
#[path = "reconciler_model.rs"]
mod reconciler_model;

/// Compositional model: the KIP-848 reconciliation engine, composed with a
/// modeled offset-commit fencing and fetch layer. It covers consumer delivery
/// correctness through rebalances.
#[cfg(test)]
#[path = "consumer_group_composition_model.rs"]
mod consumer_group_composition_model;
