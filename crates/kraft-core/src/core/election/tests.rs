use assert2::check;

use super::*;
use crate::{
    core::test_support::{FakeLog, machine, three_voter_machine, voters},
    event::{Event, LogEnd},
};

/// An election at time 2000 with a fresh, up-to-date epoch-1 log.
fn start_election(ids: &[NodeId]) -> (QuorumStateMachine, FakeLog) {
    let mut m = machine(NodeId(1), ids);
    let log = FakeLog::new(5, 1);
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    (m, log)
}

fn five_voter_with_one_grant() -> (QuorumStateMachine, FakeLog) {
    let (mut m, log) = start_election(&[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]);
    vote_response(&mut m, &log, VoteResponseSetup::default());
    (m, log)
}

fn prospective_three_voter() -> (QuorumStateMachine, FakeLog) {
    let (machine, log) = start_election(&[NodeId(1), NodeId(2), NodeId(3)]);
    assert2::assert!(matches!(machine.role(), Role::Prospective { .. }));
    (machine, log)
}

#[derive(Clone, Copy)]
struct VoteEpoch(Epoch);

#[derive(Clone, Copy, Default)]
enum VoteDecision {
    #[default]
    Granted,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum VoteRetention {
    Kept,
    Cleared,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct VoteResponseSetup {
    #[default(NodeId(2))]
    from: NodeId,
    #[default(VoteEpoch(0))]
    epoch: VoteEpoch,
    decision: VoteDecision,
    #[default(SimInstant(2001))]
    now: SimInstant,
}

fn vote_response(
    m: &mut QuorumStateMachine,
    log: &dyn LogView,
    setup: VoteResponseSetup,
) -> Vec<Action> {
    m.on_event(
        Event::ReceiveVoteResponse {
            from: setup.from,
            epoch: setup.epoch.0,
            vote_granted: matches!(setup.decision, VoteDecision::Granted),
        },
        log,
        setup.now,
    )
}

/// Only a rejection from a *higher* epoch fences us.
///
/// Each piece of `!granted && epoch > ours` matters: a grant must never
/// fence, a rejection at our own epoch must not either -- that is the
/// ordinary "you lost the vote" reply -- and only a rejection carrying a
/// newer epoch means the cluster has moved past us. Stepping down clears
/// the vote we are holding, so whether the vote survives is the tell.
#[test]
fn only_a_rejection_from_a_higher_epoch_steps_us_down() {
    let log = FakeLog::new(5, 1);
    // (what it is, granted, epoch offered, do we keep the vote we hold?)
    let cases = [
        (
            "a rejection at our own epoch",
            VoteDecision::Rejected,
            VoteEpoch(3),
            VoteRetention::Kept,
        ),
        (
            "a grant from a higher epoch",
            VoteDecision::Granted,
            VoteEpoch(9),
            VoteRetention::Kept,
        ),
        (
            "a rejection from a higher epoch",
            VoteDecision::Rejected,
            VoteEpoch(9),
            VoteRetention::Cleared,
        ),
    ];
    for (what, decision, epoch, keeps_vote) in cases {
        let mut m = three_voter_machine();
        // Cast a binding vote at epoch 3, so a step-down has something to
        // clear and "nothing happened" is distinguishable.
        m.on_event(
            Event::ReceiveVoteRequest {
                from: NodeId(2),
                cluster_id: None,
                voter_id: NodeId(1),
                voter_directory_id: uuid::Uuid::nil(),
                candidate_epoch: 3,
                candidate: NodeId(2),
                candidate_directory_id: uuid::Uuid::nil(),
                candidate_log_end: LogEnd {
                    last_epoch: 1,
                    last_offset: 5,
                },
                pre_vote: false,
            },
            &log,
            SimInstant(0),
        );
        check!(
            m.quorum_state().voted_key.is_some(),
            "{what}: setup should vote"
        );

        vote_response(
            &mut m,
            &log,
            VoteResponseSetup {
                epoch,
                decision,
                now: SimInstant(0),
                ..Default::default()
            },
        );
        let kept = if m.quorum_state().voted_key.is_some() {
            VoteRetention::Kept
        } else {
            VoteRetention::Cleared
        };
        check!(kept == keeps_vote, "{what}: vote retention = {kept:?}");
    }
}

#[test]
fn election_timeout_starts_prevote_prospective() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(5, 1);
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    crate::core::test_support::check_prevote_started(&m, &actions);
    check!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
}

#[test]
fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
    let (mut m, log) = start_election(&[NodeId(1), NodeId(2), NodeId(3)]); // Prospective
    // 1 (self) + grant from 2 = majority of 3
    let actions = vote_response(&mut m, &log, VoteResponseSetup::default());
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.quorum_state().leader_epoch,
            m.quorum_state().voted_key.map(|k| k.id),
        ) == (true, 1, Some(NodeId(1)))
    );
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            pre_vote: false,
            epoch: 1
        }
    )));
}

#[test]
fn real_majority_promotes_to_leader_and_appends_leader_change() {
    let (mut m, log) = start_election(&[NodeId(1), NodeId(2), NodeId(3)]);
    vote_response(&mut m, &log, VoteResponseSetup::default());
    let actions = vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            epoch: VoteEpoch(1),
            now: SimInstant(2002),
            ..Default::default()
        },
    );
    check!(
        (
            m.role().is_leader(),
            m.quorum_state().leader_id,
            actions
                .iter()
                .any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 })),
            actions
                .iter()
                .any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 })),
        ) == (true, Some(NodeId(1)), true, true)
    );
}

#[test]
fn observer_never_starts_election() {
    let mut m = machine(NodeId(99), &[NodeId(1), NodeId(2), NodeId(3)]); // 99 is not a voter
    let log = FakeLog::new(5, 1);
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert2::assert!(matches!(m.role(), Role::Observer { .. }));
    assert2::assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::SendVoteRequest { .. }))
    );
}

#[test]
fn prospective_counts_grant_with_no_wire_prevote_signal() {
    // A JVM voter's `VoteResponse` carries no pre-vote flag. The candidate
    // must still count the grant as a PRE-VOTE because it is Prospective —
    // this is the KIP-996 interop fix (was dropped by the old echo-tag path).
    let (mut m, log) = prospective_three_voter(); // Prospective, epoch 0
    let actions = vote_response(&mut m, &log, VoteResponseSetup::default());
    // Pre-vote majority (self + 2) → promote to Candidate and bump the epoch.
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
    check!(m.quorum_state().leader_epoch == 1);
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            pre_vote: false,
            epoch: 1
        }
    )));
}

#[test]
fn stale_prevote_grant_ignored_after_promotion() {
    // A late pre-vote grant at the old epoch must not be miscounted toward
    // the real election once we have promoted to Candidate at epoch+1.
    let (mut m, log) = start_election(&[NodeId(1), NodeId(2), NodeId(3)]);
    vote_response(&mut m, &log, VoteResponseSetup::default()); // → Candidate @ epoch 1
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
    // A duplicate/late pre-vote grant still tagged epoch 0 arrives.
    let actions = vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            from: NodeId(3),
            now: SimInstant(2002),
            ..Default::default()
        },
    );
    // Epoch guard (0 != 1) drops it: we stay Candidate, do NOT become leader.
    check!(
        (
            matches!(m.role(), Role::Candidate { .. }),
            m.role().is_leader(),
            actions.is_empty()
        ) == (true, false, true)
    );
    // The ignored stale grant must not have entered the real-vote tally:
    // after promotion the Candidate's grant set holds only our self-vote.
    if let Role::Candidate { granted, .. } = m.role() {
        assert2::assert!((granted.len(), granted.contains(&NodeId(3))) == (1, false));
    } else {
        panic!("expected Candidate");
    }
}

#[test]
fn late_grant_from_removed_voter_does_not_count() {
    let (mut m, log) = five_voter_with_one_grant();
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));

    m.apply_voter_set(voters(&[NodeId(1), NodeId(4), NodeId(5)]), SimInstant(2002));
    let actions = vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            now: SimInstant(2003),
            ..Default::default()
        },
    );
    check!(
        (
            matches!(m.role(), Role::Prospective { .. }),
            actions.is_empty()
        ) == (true, true)
    );

    vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            from: NodeId(4),
            now: SimInstant(2004),
            ..Default::default()
        },
    );
    assert2::assert!(matches!(m.role(), Role::Candidate { .. }));
}

#[test]
fn removed_voter_response_retallies_retained_grants() {
    let (mut m, log) = five_voter_with_one_grant();

    m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(4)]), SimInstant(2002));
    let actions = vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            from: NodeId(3),
            now: SimInstant(2003),
            ..Default::default()
        },
    );

    check!(
        matches!(m.role(), Role::Candidate { .. }),
        "retained grants form a majority after the voter-set shrink"
    );
    assert2::assert!(actions.iter().any(|action| matches!(
        action,
        Action::SendVoteRequest {
            epoch: 1,
            pre_vote: false
        }
    )));
}

#[test]
fn prospective_ignores_grant_from_different_epoch() {
    let (mut m, log) = prospective_three_voter();
    let actions = vote_response(
        &mut m,
        &log,
        VoteResponseSetup {
            epoch: VoteEpoch(5),
            ..Default::default()
        },
    );
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    assert2::assert!(actions.is_empty());
}
