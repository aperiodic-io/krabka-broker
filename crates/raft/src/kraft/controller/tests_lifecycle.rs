//! Tests for engine and controller construction: that each configured policy
//! reaches the engine it was built for, and that a built engine elects,
//! reports its role, and drives its timers as configured.

use std::time::Duration as StdDuration;

use assert2::{assert, check};
use krabka_units::prelude::{millis, secs};

use super::*;
use crate::kraft::{
    controller::{
        engine_loop::sleep_until_opt,
        queries::initial_state_voters,
        test_support::{
            ControllerSetup, EngineSetup, TEST_ELECTION_TIMEOUT, await_leader, build,
            build_engine_only, elect_leader_with_helper, elect_single_voter_engine,
        },
    },
    transport::NullPeerSender,
};

#[test]
fn initial_state_voters_preserves_configured_quorum_ids() {
    let (engine, _dir) = build_engine_only(EngineSetup {
        me: NodeId(2),
        ..Default::default()
    });
    assert2::assert!(initial_state_voters(&engine.core) == vec![NodeId(1), NodeId(2), NodeId(3)]);
    assert2::assert!(
        engine
            .quorum_tx
            .borrow()
            .voters
            .ids()
            .into_iter()
            .collect::<Vec<_>>()
            == vec![NodeId(1), NodeId(2), NodeId(3)]
    );
}

#[test]
fn engine_uses_configured_miss_limit_and_fetch_max() {
    let (engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        controller_fetch_miss_limit: ControllerFetchMissLimit::new(5).expect("positive miss limit"),
        metadata_raft_fetch_max: MetadataRaftFetchMax::try_from(krabka_units::bytes(512))
            .expect("positive fetch maximum"),
        ..Default::default()
    });

    check!(engine.controller_fetch_miss_limit.get() == 5);
    check!(engine.metadata_raft_fetch_max.bytes() == 512);
}

#[tokio::test]
async fn spawned_controller_uses_configured_command_queue_capacity() {
    let (controller, _dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::new(7)
            .expect("positive queue capacity"),
        ..Default::default()
    });

    check!(controller.cmd_tx.capacity() == 7);
}

#[tokio::test]
async fn engine_following_leader_reflects_current_role() {
    let (mut follower, _dir) = build_engine_only(EngineSetup::default());
    assert2::assert!(follower.following_leader().is_none());

    follower.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 1,
    });
    assert2::assert!(follower.following_leader() == Some(NodeId(2)));

    let (mut leader, _leader_dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut leader);
    assert2::assert!(leader.following_leader().is_none());
}

#[tokio::test(start_paused = true)]
async fn sleep_until_opt_waits_for_some_and_never_completes_for_none() {
    assert2::assert!(
        tokio::time::timeout(StdDuration::from_millis(1), sleep_until_opt(None))
            .await
            .is_err()
    );

    let deadline = Instant::now() + Duration::from_millis(50);
    let mut sleep = Box::pin(sleep_until_opt(Some(deadline)));
    assert2::assert!(
        tokio::time::timeout(StdDuration::from_millis(1), &mut sleep)
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_millis(50)).await;
    assert2::assert!(
        tokio::time::timeout(StdDuration::from_millis(1), sleep)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn single_voter_engine_starts_with_no_initial_leader() {
    let (ctrl, _dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let initial = *ctrl.watch_leader().borrow();
    assert2::assert!(initial.is_none());
    ctrl.shutdown().await;
}

#[tokio::test]
async fn node_id_reports_configured_node() {
    let (ctrl, _dir) = build(ControllerSetup {
        me: NodeId(7),
        ids: &[NodeId(7)],
        ..Default::default()
    });
    assert2::assert!(ctrl.node_id() == 7);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn injected_election_makes_single_voter_leader() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;
    ctrl.shutdown().await;
}

#[tokio::test]
async fn injected_vote_sequence_makes_multi_voter_leader_before_timer() {
    let (ctrl, _dir) = build(ControllerSetup {
        election_timeout: secs(60),
        ..Default::default()
    });
    elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
    ctrl.shutdown().await;
}

#[tokio::test]
async fn injected_election_timer_makes_single_voter_leader() {
    let (ctrl, _dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    ctrl.cmd_tx
        .send(Command::Timer(TimerTick::Election))
        .await
        .unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;
    ctrl.shutdown().await;
}

/// A single-voter engine started with the REAL clock auto-elects after the
/// election timeout — no injected event.
#[tokio::test]
async fn single_voter_auto_elects_on_election_timeout() {
    let (ctrl, _dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        election_timeout: millis(80),
        ..Default::default()
    });
    // The election timer is armed at construction; wait for it to fire.
    tokio::time::timeout(
        StdDuration::from_secs(5),
        await_leader(&ctrl, Some(NodeId(1))),
    )
    .await
    .expect("auto-elected within timeout");
    ctrl.shutdown().await;
}

/// A follower with a live leader (heartbeats keep arriving) does not
/// spuriously elect: the leader stays node 2 across several fetch cycles.
#[tokio::test]
async fn follower_with_live_leader_does_not_elect() {
    // Node 1 is a follower in a 3-voter cluster; the NullPeerSender means
    // its fetches fail, but a steady stream of BeginQuorumEpoch heartbeats
    // (which we inject) must keep it attached without electing.
    let (ctrl, _dir) = build(ControllerSetup {
        election_timeout: millis(120),
        ..Default::default()
    });
    // Attach to leader 2.
    ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 1,
    })
    .await
    .unwrap();
    await_leader(&ctrl, Some(NodeId(2))).await;

    // Keep re-announcing leader 2 faster than the fetch watchdog would
    // accumulate the configured number of misses; the leader must remain 2.
    for _ in 0..6 {
        tokio::time::sleep(StdDuration::from_millis(40)).await;
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 1,
        })
        .await
        .unwrap();
    }
    // `inject_event` only enqueues the heartbeat. Query through the same
    // command queue so the final heartbeat is processed before asserting.
    let state = ctrl.quorum_state().await.unwrap();
    assert2::assert!(state.leader_id == Some(NodeId(2)));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn controller_handle_reports_node_id_probe_and_shuts_down() {
    let (ctrl, _dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    check!(ctrl.node_id() == NodeId(1));
    check!(matches!(
        ctrl.probe_kraft_version("127.0.0.1:9093", 1).await,
        Err(RaftError::ChangeRejected(_))
    ));
    ctrl.shutdown().await;
    check!(matches!(
        ctrl.inject_event(Event::ElectionTimeout).await,
        Err(RaftError::Shutdown)
    ));
}
