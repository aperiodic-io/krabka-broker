//! Unit tests for the actor loop itself: what a mailbox failure does to the
//! task, what a dead session-expiry ticker does to it, and the
//! one-actor-per-group guarantee the handle registry gives.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use qubit_clock::Timer;
use tokio::sync::mpsc;

use super::{
    GroupActorMessage, GroupKindTag, chrono_now_ms, run_actor,
    test_support::{empty_metadata, make_coordinator},
};
use crate::{
    codes,
    coordinator::unified::{
        config::NextGenConfig, group::CoordinatorGroup, regex_resolver::no_topic_regex_resolver,
    },
    test_support::{BrokenTimer, TimerFailure},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_exits_on_append_error() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client-a".into(),
            client_host: String::new(),
            regex_resolver: no_topic_regex_resolver(),
            reply: tx,
        })
        .await;
    let resp = rx.await.unwrap();
    assert!(resp.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);

    // Wait for the actor to drain and drop its receiver.
    tokio::time::timeout(Duration::from_secs(1), handle.tx.closed())
        .await
        .expect("actor mpsc closed after exit");
    assert!(
        handle.tx.is_closed(),
        "actor mpsc should be closed after exit"
    );

    // get_or_create should respawn a fresh actor.
    let fresh = coord.get_or_create_consumer("g");
    assert!(!fresh.tx.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_protocol_get_or_create_returns_the_one_actor() {
    // KIP-848 live migration: the registry no longer pins a group to its
    // spawn kind. Both getters return the SAME actor for an id; the per-group
    // kind lock now lives in the actor's message arms, not the registry.
    let (coord, _log) = make_coordinator();
    // Consumer owns "c" → a classic get-or-create returns that same actor.
    let c_consumer = coord.get_or_create_consumer("c");
    let c_classic = coord.get_or_create_classic("c");
    assert!(Arc::ptr_eq(&c_consumer, &c_classic));
    // Classic owns "k" → a consumer get-or-create returns that same actor.
    let k_classic = coord.get_or_create_classic("k");
    let k_consumer = coord.get_or_create_consumer("k");
    assert!(Arc::ptr_eq(&k_classic, &k_consumer));
}

#[tokio::test]
async fn closing_an_actor_drains_messages_from_outstanding_mailbox_permits() {
    use std::{future::Future as _, task::Poll};

    for failed_registration in [false, true] {
        let (coordinator, log) = make_coordinator();
        let mut config = NextGenConfig::assigning_at_once();
        if failed_registration {
            config.timer = BrokenTimer::dead(TimerFailure::Registration).injectable();
        }
        let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
        let permit = tx.reserve().await.expect("reserve before the actor exits");
        let (stop, _stopped) = tokio::sync::oneshot::channel();
        tx.try_send(GroupActorMessage::Shutdown(stop)).unwrap();
        let actor = run_actor(
            "g".to_string(),
            GroupKindTag::Classic,
            Arc::new(config),
            empty_metadata(),
            log,
            coordinator,
            rx,
        );
        tokio::pin!(actor);
        let finished =
            std::future::poll_fn(|cx| Poll::Ready(actor.as_mut().poll(cx).is_ready())).await;
        assert!(tx.is_closed());

        // A send reserved before closure can still enqueue after the actor stops.
        let (reply, response) = tokio::sync::oneshot::channel();
        permit.send(GroupActorMessage::FetchOffsets { reply });
        if !finished {
            tokio::time::timeout(Duration::from_secs(1), actor)
                .await
                .expect("the actor drains the reserved send");
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(1), response)
                .await
                .expect("the abandoned request receives a dropped reply")
                .is_err()
        );
    }
}

/// Runs one next-gen actor on `timer` until it stops on its own, and hands
/// back the group it left behind.
///
/// The mailbox sender is held for the whole run, so a closed mailbox — the
/// actor's other way out — cannot be what ends it. A hang-guard keeps an actor
/// that does not give up from wedging the suite.
async fn run_until_the_ticker_dies(timer: Arc<dyn Timer>) -> CoordinatorGroup {
    let (coordinator, log) = make_coordinator();
    let config = Arc::new(NextGenConfig {
        timer,
        ..NextGenConfig::assigning_at_once()
    });
    let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
    let group = tokio::time::timeout(
        Duration::from_secs(10),
        run_actor(
            "g".to_string(),
            GroupKindTag::Consumer,
            config,
            empty_metadata(),
            log,
            coordinator,
            rx,
        ),
    )
    .await
    .expect("the actor stops once its ticker is gone");
    drop(tx);
    group
}

async fn check_retention_clock_on_failure(timer: &Arc<BrokenTimer>) {
    let before = chrono_now_ms();
    let group = run_until_the_ticker_dies(timer.injectable()).await;
    let after = chrono_now_ms();

    let stamped = group
        .empty_since_ms
        .expect("the retention clock is stamped");
    check!((before..=after).contains(&stamped));
    check!(timer.registrations() == 1);
}

#[tokio::test]
async fn the_actor_stamps_the_retention_clock_when_its_ticker_cannot_be_armed() {
    let timer = BrokenTimer::dead(TimerFailure::Registration);

    // The actor never reaches its mailbox loop, so the start-up exit is the
    // only thing that can stamp the offset-retention clock — and it must,
    // because a memberless group whose stamp stayed `None` would never be
    // measured for offset expiry at all.
    check_retention_clock_on_failure(&timer).await;
}

#[tokio::test]
async fn the_actor_stops_when_its_armed_tick_never_completes() {
    let timer = BrokenTimer::dead(TimerFailure::Completion);

    // The registration is accepted, so the actor reaches its select; the
    // deadline then fails, and the loop tail still stamps the clock on the way
    // out rather than returning from inside the arm.
    check_retention_clock_on_failure(&timer).await;
}

#[tokio::test]
async fn the_actor_stops_when_its_tick_cannot_be_re_armed() {
    let timer = BrokenTimer::dead_after(1, TimerFailure::Registration);

    // The start-up deadline is honoured, so the actor takes one session-expiry
    // sweep and then asks for the next deadline. That one is refused, and the
    // actor stops rather than retrying it: two registrations in all, not a
    // climbing count.
    let group = run_until_the_ticker_dies(timer.injectable()).await;

    check!(group.empty_since_ms.is_some());
    check!(timer.registrations() == 2);
}
