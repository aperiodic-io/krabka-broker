//! The periodic session-expiry tick.
//!
//! One kind-agnostic timer drives both protocols: it expires classic members
//! past their session timeout and completes any rebalance their departure
//! unblocks, and it evicts next-gen members and writes their tombstones.

use std::time::Instant;

use super::{
    ActorServices, ParkedWaiters, chrono_now_ms,
    downgrade::{downgrade_fencing, downgrades_without},
    heartbeat::fence_members,
    persistence::{flush_classic_metadata, flush_pending},
    waiters::settle_removed_classic_waiters,
};
#[cfg(test)]
use crate::coordinator::unified::share::config::ShareGroupConfig;
#[cfg(test)]
use crate::coordinator::unified::streams::config::StreamsGroupConfig;
use crate::{
    coordinator::unified::{classic_state::ClassicGroup as ClassicState, group::CoordinatorGroup},
    error::BrokerError,
    metadata_epoch::next_i32,
};

pub(super) async fn handle_actor_tick(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    let group_id = group.group_id.clone();
    if group.as_consumer().is_some() {
        if let Err(error) = handle_session_tick(group, services).await {
            tracing::warn!(%group_id, %error,
                "next-gen actor exiting after tick log-write failure");
            return false;
        }
    } else if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let now = Instant::now();
        let expired_pending = state.expire_pending_members(now);
        let dropped =
            state.expire_dead_members(now, services.config.classic_initial_rebalance_delay);
        if !dropped.is_empty() || !expired_pending.is_empty() {
            tracing::info!(group = %group_id, ?dropped, ?expired_pending,
                "expired members; waking joiners");
            return settle_classic_removal(state, previous, &dropped, parked, services).await;
        }
    }
    true
}

/// Runs when a classic group's pending-sync timer fires: Kafka's
/// `expirePendingSync`. Every member that still owes a `SyncGroup` is removed
/// and the group prepares a rebalance for the rest. It returns the actor's
/// keep-running flag.
pub(super) async fn handle_classic_sync_expiry(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    let Some(state) = group.as_classic_mut() else {
        return true;
    };
    let previous = state.clone();
    let removed = state.expire_pending_sync(
        services.config.classic_initial_rebalance_delay,
        Instant::now(),
    );
    if removed.is_empty() {
        return true;
    }
    tracing::info!(group = %state.group_id, ?removed,
        "removed members that never sent SyncGroup; preparing a rebalance");
    let keep_running = settle_classic_removal(state, previous, &removed, parked, services).await;
    // A failed write restores the group with its timer already due. Retry on
    // the session-expiry cadence instead of spinning on the log.
    if let Some(state) = group.as_classic_mut()
        && state.sync_deadline.is_some_and(|due| due <= Instant::now())
    {
        state.sync_deadline = Some(Instant::now() + services.config.session_expiry_tick);
    }
    keep_running
}

/// What a classic group owes after its timers removed `removed` members, in
/// the order of Kafka's `removeMemberAndUpdateClassicGroup`. An emptied group
/// persists its next generation first. The removed members' parked calls get
/// `UNKNOWN_MEMBER_ID`, the followers parked in `SyncGroup` of a group that left
/// `CompletingRebalance` get `REBALANCE_IN_PROGRESS` at once, as
/// `prepareRebalance` answers them, and a join phase the survivors have all
/// rejoined completes. `previous` is the group before the removal, which a
/// failed write restores. It returns the actor's keep-running flag.
async fn settle_classic_removal(
    state: &mut ClassicState,
    previous: ClassicState,
    removed: &[String],
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    if state.members.is_empty() && !removed.is_empty() {
        let Some(generation_id) = next_i32(state.generation_id) else {
            *state = previous;
            tracing::warn!(group = %state.group_id,
                "classic expiration stopped because the generation is exhausted");
            return false;
        };
        state.generation_id = generation_id;
        if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
            *state = previous;
            tracing::warn!(group = %state.group_id, %error,
                "classic expiration log write failed; retrying on the next tick");
            return true;
        }
    }
    settle_removed_classic_waiters(state, previous.state, removed, parked);
    true
}

/// Runs on every heartbeat-interval tick. Each member whose session expired,
/// or whose rebalance timeout fired with partitions still to revoke, is
/// fenced on its own, with a batch of its own, as each of Kafka's timers runs
/// `consumerGroupFenceMember` for its member. A fence that leaves only
/// classic members downgrades the group instead, and the downgraded group
/// fences no more. It returns `Err` when a log write fails, and the actor
/// must then exit.
async fn handle_session_tick(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
) -> Result<(), BrokerError> {
    let now = Instant::now();
    let Some(state) = group.as_consumer_mut() else {
        return Ok(());
    };
    let mut fenced = state.expired_members(now, services.config.session_timeout);
    // KIP-848: a member that did not revoke its partitions within its
    // rebalance timeout is fenced like a member whose session expired.
    for member_id in state.rebalance_timeouts_due(now) {
        if !fenced.contains(&member_id) {
            fenced.push(member_id);
        }
    }
    for member_id in fenced {
        let Some(state) = group.as_consumer_mut() else {
            return Ok(());
        };
        if !state.members.contains_key(&member_id) {
            continue;
        }
        let member_ids = [member_id];
        if downgrades_without(state, services.config, &member_ids) {
            return downgrade_fencing(
                group,
                &member_ids,
                services.config,
                services.metadata,
                services.offsets_log,
                services.coordinator,
            )
            .await;
        }
        let pending = fence_members(state, services.metadata, &member_ids);
        flush_pending(
            state,
            pending,
            services.offsets_log,
            services.coordinator,
            chrono_now_ms(),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    use super::*;
    use crate::{
        codes,
        coordinator::unified::{
            ClientIdentity, GroupCoordinator,
            actor::{
                GroupActorMessage, GroupKindTag, PendingRecords, RegexResolution,
                test_support::{
                    ConsumerMemberSetup, actor_services_with, completing_classic_group,
                    empty_metadata, last_classic_metadata, make_coordinator, parked_follower, rpc,
                    subscribed_member,
                },
            },
            classic_state::GroupState as ClassicGroupState,
            config::NextGenConfig,
            consumer_state::GroupState,
            group::GroupKind,
            offsets_log::fake::InMemoryOffsetsLog,
            persistence_next_gen::GroupMetadataValue,
            reconciler::ReconcileInput,
            regex_resolver::no_topic_regex_resolver,
            test_support::{check_two_members_expired, expired_session_last_seen},
        },
    };

    #[tokio::test]
    async fn classic_last_member_expiration_persists_empty_generation() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        state.state = ClassicGroupState::Stable;
        state.members.get_mut("m1").unwrap().session_timeout = Duration::ZERO;
        let prior_generation = state.generation_id;
        let mut parked = ParkedWaiters::default();
        let services = super::super::test_support::actor_services(&coord, log.as_ref());

        check!(handle_actor_tick(&mut group, &mut parked, services).await);
        let state = group.as_classic().unwrap();
        check!(state.state == ClassicGroupState::Empty);
        check!(state.generation_id == prior_generation + 1);
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == prior_generation + 1);
        check!(persisted.members.is_empty());
    }

    /// Kafka runs a session timer per member, and each one that fires runs
    /// `consumerGroupFenceMember` for its member alone. Two members that
    /// expire together are fenced in two batches, each with the member's
    /// tombstones and an epoch bump, and no target is computed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_expired_member_is_fenced_in_a_batch_of_its_own() {
        let (coord, log) = make_coordinator();
        // Tiny session timeout so a member whose `last_seen` is a few ms in
        // the past counts as expired — avoids subtracting a large duration
        // from `Instant::now()`, which `checked_sub` rejects on low-uptime
        // CI runners (e.g. a freshly-booted Windows agent).
        let config = NextGenConfig {
            session_timeout: Duration::from_millis(1),
            ..NextGenConfig::assigning_at_once()
        };
        let metadata = empty_metadata();
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            let mut m = subscribed_member(ConsumerMemberSetup {
                member_id,
                client: ClientIdentity {
                    id: "client-a",
                    host: "h",
                },
                now: Instant::now(),
                ..Default::default()
            });
            m.last_seen = expired_session_last_seen();
            state.add_or_update_member(m);
        }
        state.group_epoch = 2;
        state.target.epoch = 2;
        let mut group = CoordinatorGroup::seeded(
            "g",
            GroupKind::Consumer(state),
            std::collections::HashMap::new(),
        );
        let services = actor_services_with()
            .config(&config)
            .metadata(metadata.as_ref())
            .offsets_log(log.as_ref())
            .coordinator(&coord)
            .call();

        handle_session_tick(&mut group, services)
            .await
            .expect("tick should succeed");

        let fence = |member_id: &str, epoch| {
            use crate::coordinator::unified::test_support::member_tombstones;
            PendingRecords {
                member_metadata: member_tombstones(member_id),
                target_per_member: member_tombstones(member_id),
                current_per_member: member_tombstones(member_id),
                group_metadata: Some(GroupMetadataValue {
                    epoch,
                    metadata_hash: 0,
                }),
                ..PendingRecords::default()
            }
            .to_batch("g", 0)
            .unwrap()
            .records
        };
        let written: Vec<_> = log.record_batches().await;
        let mut expected = vec![fence("m1", 3), fence("m2", 4)];
        if written.first().is_some_and(|batch| batch != &expected[0]) {
            // The members expire together: either may be fenced first.
            expected = vec![fence("m2", 3), fence("m1", 4)];
        }
        check!(written == expected);
        let state = group.as_consumer().unwrap();
        check_two_members_expired((state.members.len(), state.group_epoch, state.target.epoch));
    }

    /// KIP-848: Kafka fences a member that does not revoke its partitions
    /// within its rebalance timeout (`scheduleConsumerGroupRebalanceTimeout`),
    /// even when it keeps heartbeating. The fence frees the partitions for
    /// their new owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_that_misses_its_rebalance_timeout_is_fenced() {
        use std::collections::HashMap;

        use krabka_protocol::{
            owned::consumer_group_heartbeat_request::TopicPartitions, primitives::uuid::Uuid,
        };

        use crate::coordinator::unified::{
            actor::{step_heartbeat, test_support::StaticMetadata},
            reconciler::ReconcileInput,
        };

        struct Row {
            name: &'static str,
            rebalance_timeout_ms: i32,
            /// The second heartbeat of m1 revokes the partition it lost.
            revokes: bool,
            m1_present_after: bool,
            m2_partitions_after: usize,
        }
        let rows = [
            Row {
                name: "heartbeats but never revokes",
                rebalance_timeout_ms: 100,
                revokes: false,
                m1_present_after: false,
                m2_partitions_after: 2,
            },
            Row {
                name: "revokes before the timeout",
                rebalance_timeout_ms: 100,
                revokes: true,
                m1_present_after: true,
                m2_partitions_after: 1,
            },
            Row {
                name: "never revokes, timeout not reached",
                rebalance_timeout_ms: 600_000,
                revokes: false,
                m1_present_after: true,
                m2_partitions_after: 0,
            },
        ];

        let topic = Uuid([42; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: HashMap::from([("t".to_string(), topic)]),
                partitions_per_topic: HashMap::from([(topic, 2)]),
                ..Default::default()
            },
        };
        let client = ClientIdentity { id: "c", host: "h" };
        let owned = |partitions: Vec<i32>| {
            Some(vec![TopicPartitions {
                topic_id: topic,
                partitions,
                ..Default::default()
            }])
        };

        for row in rows {
            let (coord, log) = make_coordinator();
            let config = NextGenConfig::assigning_at_once();
            let mut state = GroupState::new("g");
            // Every heartbeat happened a second ago, so a 100 ms timeout armed
            // by them has fired when the tick runs, and a session has not.
            let earlier = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
            let heartbeat =
                |state: &mut GroupState, member_id: &str, partitions: Option<Vec<i32>>| {
                    let member_epoch = state.members.get(member_id).map_or(0, |m| m.member_epoch);
                    step_heartbeat(
                        state,
                        &config,
                        &metadata,
                        &ConsumerGroupHeartbeatRequest {
                            group_id: "g".into(),
                            member_id: member_id.into(),
                            member_epoch,
                            subscribed_topic_names: Some(vec!["t".into()]),
                            rebalance_timeout_ms: row.rebalance_timeout_ms,
                            topic_partitions: partitions.and_then(owned),
                            ..Default::default()
                        },
                        client,
                        earlier,
                        &RegexResolution::none(),
                    )
                    .response
                };

            heartbeat(&mut state, "m1", Some(vec![]));
            heartbeat(&mut state, "m1", Some(vec![0, 1]));
            heartbeat(&mut state, "m2", Some(vec![]));
            // m1 learns that it must give up one partition, and does not.
            heartbeat(&mut state, "m1", Some(vec![0, 1]));
            let kept: Vec<i32> = state.members["m1"]
                .assigned_partitions
                .get(&topic)
                .cloned()
                .unwrap_or_default();
            check!(kept.len() == 1, "{}", row.name);
            let second = if row.revokes { kept } else { vec![0, 1] };
            heartbeat(&mut state, "m1", Some(second));

            let mut group = CoordinatorGroup::seeded(
                "g",
                GroupKind::Consumer(state),
                std::collections::HashMap::new(),
            );
            let services = super::super::test_support::actor_services(&coord, log.as_ref());
            handle_session_tick(&mut group, services)
                .await
                .expect("tick");
            let GroupKind::Consumer(mut state) =
                std::mem::replace(group.kind_mut(), GroupKind::Consumer(GroupState::new("g")))
            else {
                panic!("the group stays a consumer group");
            };

            check!(
                state.members.contains_key("m1") == row.m1_present_after,
                "{}",
                row.name
            );
            let response = heartbeat(&mut state, "m2", None);
            let m2_partitions: usize = response
                .assignment
                .map(|a| {
                    a.topic_partitions
                        .iter()
                        .map(|tp| tp.partitions.len())
                        .sum()
                })
                .unwrap_or_default();
            check!(m2_partitions == row.m2_partitions_after, "{}", row.name);
        }
    }

    /// The actor wakes at a member's rebalance deadline instead of waiting
    /// for the session tick, and a heartbeat's `rebalance_timeout_ms` replaces
    /// the stored timeout before the deadline is armed. The session tick here
    /// never fires, so only the deadline wake can fence the member.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_actor_fences_at_the_rebalance_deadline_between_session_ticks() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;
        use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

        use crate::coordinator::unified::actor::{GroupActorMessage, test_support::StaticMetadata};

        let topic = krabka_protocol::primitives::uuid::Uuid([42; 16]);
        let clock = ManualMonotonicClock::new_shared();
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                timer: clock.new_timer(),
                session_expiry_tick: Duration::from_hours(1),
                ..NextGenConfig::assigning_at_once()
            },
            ShareGroupConfig::assigning_at_once(),
            Arc::new(StaticMetadata {
                input: ReconcileInput {
                    topic_id_by_name: [("t".to_string(), topic)].into(),
                    partitions_per_topic: [(topic, 2)].into(),
                    ..Default::default()
                },
            }),
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_consumer("g");
        let heartbeat = |member_id: &'static str,
                         member_epoch: i32,
                         rebalance_timeout_ms: i32,
                         owned: Option<Vec<i32>>| {
            let handle = Arc::clone(&handle);
            async move {
                let response = rpc::begin(&handle, |reply| GroupActorMessage::Heartbeat {
                    request: ConsumerGroupHeartbeatRequest {
                        group_id: "g".into(),
                        member_id: member_id.into(),
                        member_epoch,
                        subscribed_topic_names: Some(vec!["t".into()]),
                        rebalance_timeout_ms,
                        topic_partitions: owned.map(|partitions| {
                            vec![TopicPartitions {
                                topic_id: topic,
                                partitions,
                                ..Default::default()
                            }]
                        }),
                        ..Default::default()
                    },
                    client_id: "c".into(),
                    client_host: "h".into(),
                    reply,
                    regex_resolver: no_topic_regex_resolver(),
                })
                .await;
                response.await.unwrap()
            }
        };

        // m1 joins with a long timeout and owns both partitions.
        let joined = heartbeat("m1", 0, 600_000, Some(vec![])).await;
        let owned = heartbeat("m1", joined.member_epoch, -1, Some(vec![0, 1])).await;
        heartbeat("m2", 0, 600_000, Some(vec![])).await;
        // m1 shortens its timeout to 50 ms and keeps both partitions.
        let kept = heartbeat("m1", owned.member_epoch, 50, Some(vec![0, 1])).await;
        check!(kept.error_code == crate::codes::NONE);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let view = rpc::begin(&handle, |reply| GroupActorMessage::Describe { reply }).await;
            let members: Vec<String> = view
                .await
                .unwrap()
                .members
                .into_iter()
                .map(|member| member.member_id)
                .collect();
            if members == vec!["m2".to_string()] {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "m1 was not fenced at its rebalance deadline: {members:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// KIP-848 live migration: the tick must dispatch on the LIVE
    /// `group.kind`, not on the captured spawn-time kind. This test spawns a
    /// classic actor, flips it to a consumer group in place, and fires a tick.
    /// The actor must keep running rather than panic on a kind-mismatched
    /// `expect(...)`.
    ///
    /// An injected manual timer drives the session-expiry tick, so the tick
    /// fires on a controlled timeline instead of a real 1.2 s wall-clock
    /// sleep. The test is therefore deterministic and instant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_tick_does_not_panic_after_in_place_flip() {
        use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

        let clock = ManualMonotonicClock::new_shared();
        let log = Arc::new(InMemoryOffsetsLog::default());
        let tick_interval = Duration::from_millis(37);
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                timer: clock.new_timer(),
                session_expiry_tick: tick_interval,
                ..NextGenConfig::assigning_at_once()
            },
            ShareGroupConfig::assigning_at_once(),
            empty_metadata(),
            log.clone(),
            StreamsGroupConfig::default(),
        ));

        let handle = coord.get_or_create_group("g", GroupKindTag::Classic);

        // Flip the group to consumer in place, then round-trip a synchronous
        // inspect. The mpsc is FIFO with a single consumer, so the inspect reply
        // proves the flip message was already processed before we fire a tick.
        handle
            .tx
            .send(GroupActorMessage::TestForceConsumerKind)
            .await
            .unwrap();
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::InspectAny { reply: tx }).await;
        let _ = rx.await;

        // The actor is now parked on the re-armed session-expiry tick sleep.
        // The manual clock has a single kind of waiter, so a count of one is
        // that tick registration and nothing else. Confirm the waiter is
        // registered (only advance the timeline once parked), fire exactly one
        // tick, then confirm the loop re-parks — which proves the tick body ran
        // to completion on the LIVE consumer kind without panicking.
        // `wait_for_waiters` blocks, so it runs on a blocking thread and never
        // stalls the runtime driving the actor. Its five-second real-time bound
        // turns a lost tick into a failure rather than a hung test.
        let waiting = Arc::clone(&clock);
        let parked = tokio::task::spawn_blocking(move || {
            waiting.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(parked, "actor should park on the session-expiry tick sleep");

        clock
            .advance(tick_interval)
            .expect("manual time moves forward");

        let waiting = Arc::clone(&clock);
        let reparked = tokio::task::spawn_blocking(move || {
            waiting.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(reparked, "actor should re-park after processing the tick");
        assert!(!handle.tx.is_closed());
    }

    fn check_preparing_survivor(state: &ClassicState) {
        check!(state.state == ClassicGroupState::PreparingRebalance);
        check!(state.members.keys().collect::<Vec<_>>() == vec!["m2"]);
    }

    /// Kafka's `removeMemberAndUpdateClassicGroup` reaches `prepareRebalance`,
    /// which answers every member waiting in `SyncGroup` with
    /// `REBALANCE_IN_PROGRESS` at once and gives the survivors the whole group
    /// rebalance timeout to rejoin. The tick used to leave the follower parked
    /// and to wake the survivors after the initial rebalance delay instead.
    #[tokio::test]
    async fn leader_expiry_in_completing_rebalance_releases_followers_and_keeps_survivors() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1", "m2"]);
        let state = group.as_classic_mut().unwrap();
        state.members.get_mut("m1").unwrap().session_timeout = Duration::ZERO;
        let (mut parked, mut rx) = parked_follower("m2");
        let services = super::super::test_support::actor_services(&coord, log.as_ref());
        let before = Instant::now();

        check!(handle_actor_tick(&mut group, &mut parked, services).await);

        let state = group.as_classic().unwrap();
        check!(
            rx.try_recv().expect("the follower is answered by the tick")
                == crate::coordinator::unified::actor::SyncResult {
                    error_code: codes::REBALANCE_IN_PROGRESS,
                    ..Default::default()
                }
        );
        check_preparing_survivor(state);
        // The survivor's 60 s rebalance timeout, not the 3 s initial delay.
        check!(
            state
                .rebalance_deadline
                .is_some_and(|deadline| deadline >= before + Duration::from_mins(1))
        );
    }

    /// Kafka's `removePendingMemberAndUpdateClassicGroup`: a `MEMBER_ID_REQUIRED`
    /// id that times out completes a join phase that it alone was holding up.
    #[tokio::test]
    async fn pending_member_expiry_completes_the_join_phase() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let generation = state.generation_id;
        state.state = ClassicGroupState::PreparingRebalance;
        state.mark_awaiting_join("m1");
        state.rebalance_deadline = Some(Instant::now() + Duration::from_hours(1));
        state.add_pending_member(
            "pending".into(),
            Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
        );
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut parked = ParkedWaiters::default();
        parked.joiners.insert("m1".into(), tx);
        let services = super::super::test_support::actor_services(&coord, log.as_ref());

        check!(handle_actor_tick(&mut group, &mut parked, services).await);

        let joined = rx.try_recv().expect("the join phase completed");
        check!(joined.error_code == codes::NONE);
        check!(joined.generation_id == generation + 1);
        let state = group.as_classic().unwrap();
        check!(state.state == ClassicGroupState::CompletingRebalance);
        check!(state.pending_members.is_empty());
    }

    fn group_with_synced_follower() -> CoordinatorGroup {
        let mut group = completing_classic_group(&["m1", "m2"]);
        let state = group.as_classic_mut().unwrap();
        state.arm_pending_sync(Instant::now());
        state.remove_pending_sync_member("m2");
        group
    }

    /// Kafka's `expirePendingSync`: a leader that never sent `SyncGroup` is
    /// removed when the timer fires, and the followers waiting for it are
    /// answered `REBALANCE_IN_PROGRESS` as `prepareRebalance` does.
    #[tokio::test]
    async fn pending_sync_expiry_removes_the_silent_leader_and_releases_followers() {
        let (coord, log) = make_coordinator();
        // The follower synced and waits for the leader.
        let mut group = group_with_synced_follower();
        let (mut parked, mut rx) = parked_follower("m2");
        let services = super::super::test_support::actor_services(&coord, log.as_ref());

        check!(handle_classic_sync_expiry(&mut group, &mut parked, services).await);

        check!(
            rx.try_recv()
                .expect("the follower is answered at once")
                .error_code
                == codes::REBALANCE_IN_PROGRESS
        );
        let state = group.as_classic().unwrap();
        check_preparing_survivor(state);
        check!(state.sync_deadline.is_none());
    }

    /// The whole actor: a generation whose leader never syncs is torn down by
    /// the pending-sync timer alone, although the leader's session is fine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_removes_a_member_that_never_syncs_after_the_rebalance_timeout() {
        let (coord, _log) = make_coordinator();
        let mut group = group_with_synced_follower();
        let state = group.as_classic_mut().unwrap();
        state.sync_deadline = Some(Instant::now() + Duration::from_millis(50));
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();

        let view = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let view = rpc::classic_inspect(&handle).await;
                if view.state != ClassicGroupState::CompletingRebalance {
                    break view;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the pending-sync timer must fire");

        check!(view.state == ClassicGroupState::PreparingRebalance);
        check!(view.members.len() == 1);
        check!(view.members[0].member_id == "m2");
    }
}
