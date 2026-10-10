//! Unit tests for the timer derivation in [`super::timing`]: the staggered
//! startup election deadline, the heartbeat period, and the role predicates
//! that decide which timer runs and when leadership is lost.

use assert2::{assert, check};
use krabka_units::prelude::{TimeExt as _, millis, secs};

use super::*;
use crate::kraft::controller::{
    test_support::{EngineSetup, voter_set},
    timing::{
        election_timeout_ms, election_timer_starts_election, following_leader_for_role,
        heartbeat_period, initial_election_at, instant_from_clock_base,
        should_fail_waiters_on_leadership_change,
    },
};

#[test]
fn initial_election_deadline_matches_startup_role() {
    /// Base election timeout the staggered startup deadline is derived from.
    const TIMEOUT: Time = millis(400);
    /// The same extent in the integer milliseconds the core's jitter uses.
    const TIMEOUT_MS: u64 = 400;

    let base = Instant::now();
    let single = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(&[NodeId(1)])),
        TIMEOUT,
    );
    assert2::assert!(initial_election_at(&single, None, base, NodeId(1), 0, TIMEOUT) == Some(base));

    let known_leader = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    assert2::assert!(
        initial_election_at(&known_leader, Some(NodeId(2)), base, NodeId(1), 0, TIMEOUT).is_none()
    );

    let non_voter = QuorumStateMachine::new(
        NodeId(4),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    assert2::assert!(initial_election_at(&non_voter, None, base, NodeId(4), 0, TIMEOUT).is_none());

    let multi = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(
            uuid::Uuid::nil(),
            voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        ),
        TIMEOUT,
    );
    // The jitter is integer milliseconds and the deadline is the integer
    // sum, so the quantity must not shift the deadline by even a nanosecond.
    let jitter = crate::kraft::core::election_jitter_ms(NodeId(1), 0, TIMEOUT_MS);
    let at =
        initial_election_at(&multi, None, base, NodeId(1), 0, TIMEOUT).expect("multi voter timer");
    assert2::assert!(at.duration_since(base) == Duration::from_millis(TIMEOUT_MS + jitter));
}

#[test]
fn election_timeout_converts_to_whole_milliseconds() {
    for (_case, timeout, want_ms) in [
        ("whole second", secs(1), 1_000u64),
        ("sub-second", millis(250), 250),
        ("zero", secs(0), 0),
        ("negative clamps to zero", Time::from_millis(-4), 0),
    ] {
        check!(election_timeout_ms(timeout) == want_ms);
    }
}

#[test]
fn heartbeat_period_is_one_third_of_election_timeout_with_floor() {
    for (_case, timeout_ms, want_ms) in [
        ("ordinary timeout", 1000, 333),
        ("short timeout", 120, 40),
        ("floor below three milliseconds", 2, 1),
        ("zero timeout floor", 0, 1),
    ] {
        assert2::assert!(heartbeat_period(millis(timeout_ms), None) == millis(want_ms));
    }
}

#[test]
fn configured_heartbeat_overrides_derived_period() {
    assert2::assert!(heartbeat_period(secs(5), Some(millis(500))) == millis(500));
}

#[test]
fn election_timer_only_starts_non_leader_voters() {
    for (_case, is_voter, is_leader, want) in [
        ("non-leader voter", true, false, true),
        ("leader voter", true, true, false),
        ("non-voter follower", false, false, false),
        ("non-voter leader", false, true, false),
    ] {
        assert2::assert!(election_timer_starts_election(is_voter, is_leader) == want);
    }
}

#[test]
fn following_leader_for_role_reports_followed_leader_only() {
    for (role, want) in [
        (
            Role::Follower {
                leader_id: NodeId(7),
                fetch_deadline: SimInstant(10),
                has_fetched_from_leader: false,
            },
            Some(NodeId(7)),
        ),
        (
            Role::Observer {
                leader_id: Some(NodeId(9)),
                fetch_deadline: SimInstant(10),
            },
            Some(NodeId(9)),
        ),
        (
            Role::Observer {
                leader_id: None,
                fetch_deadline: SimInstant(10),
            },
            None,
        ),
        (
            Role::Leader {
                replicas: std::collections::BTreeMap::new(),
                fetched_voters: std::collections::BTreeSet::new(),
                high_watermark: 0,
                epoch_start_offset: 0,
            },
            None,
        ),
    ] {
        assert2::assert!(following_leader_for_role(&role) == want);
    }
}

#[test]
fn leadership_loss_detection_handles_stepdown_and_epoch_bump() {
    for (_case, was_leader, is_leader, held_epoch, current_epoch, want) in [
        ("leader stepped down", true, false, 3, 3, true),
        ("leader epoch advanced", true, true, 3, 4, true),
        ("leadership unchanged", true, true, 3, 3, false),
        ("follower epoch advanced", false, false, 3, 4, false),
    ] {
        assert2::assert!(
            should_fail_waiters_on_leadership_change(
                was_leader,
                is_leader,
                held_epoch,
                current_epoch
            ) == want
        );
    }
}

#[test]
fn deadline_instant_offsets_from_engine_clock_base() {
    let base = Instant::now();
    let at = instant_from_clock_base(base, SimInstant(250));
    assert2::assert!(at.checked_duration_since(base) == Some(Duration::from_millis(250)));
}

#[tokio::test]
async fn discovery_peer_distinguishes_voter_and_observer() {
    use crate::kraft::controller::test_support::build_engine_only;

    let (voter, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    check!(voter.discovery_peer().is_none());

    let (observer, _dir) = build_engine_only(EngineSetup {
        me: NodeId(3),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    check!(observer.discovery_peer() == Some(NodeId(1)));

    let (mut attached_observer, _dir) = build_engine_only(EngineSetup {
        me: NodeId(3),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    attached_observer.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(1),
        leader_epoch: 1,
    });
    check!(attached_observer.following_leader() == Some(NodeId(1)));
    check!(attached_observer.discovery_peer().is_none());
}

/// A leaderless observer asks each peer in turn, so a peer that is down, or
/// one that knows no leader, does not hold its discovery for good.
#[tokio::test]
async fn discovery_takes_the_peers_in_turn() {
    use crate::kraft::controller::test_support::build_engine_only;

    let (mut observer, _dir) = build_engine_only(EngineSetup {
        me: NodeId(3),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let picked: Vec<Option<NodeId>> = (0..3)
        .map(|attempt| {
            observer.discovery_attempts = attempt;
            observer.discovery_peer()
        })
        .collect();
    check!(picked == vec![Some(NodeId(1)), Some(NodeId(2)), Some(NodeId(1))]);
}

#[tokio::test]
async fn fetch_misses_increment_and_trigger_timeout_at_limit() {
    use crate::kraft::{controller::test_support::build_engine_only, transport::TimerTick};

    let (mut follower, _dir) = build_engine_only(EngineSetup {
        me: NodeId(2),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    follower.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(1),
        leader_epoch: 1,
    });
    check!(follower.following_leader() == Some(NodeId(1)));
    check!(follower.fetch_misses == 0);

    // Miss 1
    follower.on_timer(TimerTick::Fetch);
    check!(follower.fetch_misses == 1);
    check!(follower.fetch_at.is_some());
    check!(follower.fetch_at.unwrap() > Instant::now());
    check!(follower.following_leader() == Some(NodeId(1)));

    // Miss 2
    follower.on_timer(TimerTick::Fetch);
    check!(follower.fetch_misses == 2);
    check!(follower.following_leader() == Some(NodeId(1)));

    // Miss 3 (limit is 3 by default)
    follower.on_timer(TimerTick::Fetch);
    check!(follower.fetch_misses == 0);
    check!(follower.following_leader().is_none());
}

#[test]
fn response_to_event_maps_vote_response() {
    use crate::kraft::{
        controller::engine_loop::response_to_event,
        transport::{api_key, wire::PeerResponse},
    };

    let vote = PeerResponse::Vote {
        epoch: 5,
        granted: true,
    };
    let encoded = vote.encode();
    let event = response_to_event(NodeId(3), api_key::VOTE, &encoded);
    check!(
        event
            == Some(Event::ReceiveVoteResponse {
                from: NodeId(3),
                epoch: 5,
                vote_granted: true,
            })
    );

    check!(response_to_event(NodeId(3), 999, &encoded).is_none());
    check!(response_to_event(NodeId(3), api_key::VOTE, b"invalid").is_none());
}

#[tokio::test]
async fn sleep_until_opt_completes_for_past_deadline() {
    use crate::kraft::controller::engine_loop::sleep_until_opt;

    sleep_until_opt(Some(Instant::now())).await;
}

/// Kafka keys a replica that is not a voter by `(id, directory id)`, so the same
/// id fetching under two directories is two observers, and one that names no
/// directory is a third.
#[test]
fn inbound_fetch_keys_observers_by_directory_id() {
    use crate::kraft::{controller::test_support::build_engine_only, transport::wire::PeerRequest};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    // A fetch at the engine's very first millisecond would read as "never".
    engine.clock_base = Instant::now() - Duration::from_millis(50);
    let current_leader_epoch = i32::try_from(engine.core.quorum_state().leader_epoch).unwrap();
    let dir_id = uuid::Uuid::from_u128(999);
    let other_dir_id = uuid::Uuid::from_u128(1_000);
    for directory in [dir_id, other_dir_id, uuid::Uuid::nil(), dir_id] {
        let req = PeerRequest::Fetch {
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
            from: NodeId(2),
            current_leader_epoch,
            fetch_epoch: 0,
            fetch_offset: 0,
            replica_directory_id: directory,
        };
        let (reply, _rx) = oneshot::channel();
        engine.on_inbound(Inbound::Fetch {
            version: crate::kraft::transport::wire::FETCH_VERSION,
            req: req.encode(),
            reply,
        });
    }

    let observers: Vec<(NodeId, uuid::Uuid)> = engine
        .quorum_state_snapshot()
        .observers
        .iter()
        .map(|observer| (observer.id, observer.directory_id))
        .collect();
    assert2::assert!(
        observers
            == vec![
                (NodeId(2), uuid::Uuid::nil()),
                (NodeId(2), dir_id),
                (NodeId(2), other_dir_id),
            ]
    );
}
