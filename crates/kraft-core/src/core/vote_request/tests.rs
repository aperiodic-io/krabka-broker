use {
    super::*,
    crate::{
        core::test_support::{
            FakeLog, TEST_ELECTION_TIMEOUT, machine, three_voter_machine, voters,
        },
        event::{Event, SuccessorRank},
        types::{NodeId, QuorumState},
    },
    assert2::check,
};

/// A dynamic-quorum machine for voter 1 with the ids it was built from.
struct DynamicMachine {
    machine: QuorumStateMachine,
    cluster_id: uuid::Uuid,
    voter_directory_id: uuid::Uuid,
    candidate_directory_id: uuid::Uuid,
}

fn dynamic_machine() -> DynamicMachine {
    let cluster_id = uuid::Uuid::from_u128(1);
    let voter_directory_id = uuid::Uuid::from_u128(11);
    let candidate_directory_id = uuid::Uuid::from_u128(22);
    let voters = krabka_voters::VoterSet::from_voters([
        krabka_voters::Voter {
            id: NodeId(1),
            directory_id: voter_directory_id,
            endpoints: vec![],
            kraft_version: krabka_voters::KRaftVersionRange::default(),
        },
        krabka_voters::Voter {
            id: NodeId(2),
            directory_id: candidate_directory_id,
            endpoints: vec![],
            kraft_version: krabka_voters::KRaftVersionRange::default(),
        },
    ]);
    let mut state = QuorumState::bootstrap(cluster_id, voters);
    state.kraft_version = 1;
    DynamicMachine {
        machine: QuorumStateMachine::new(NodeId(1), state, TEST_ELECTION_TIMEOUT),
        cluster_id,
        voter_directory_id,
        candidate_directory_id,
    }
}

/// A replica that is not a voter denies every vote request, whoever the
/// candidate is.
///
/// Being a voter and the candidate being one are separate requirements,
/// and an observer satisfies neither -- joining them so that both must
/// fail before denying would let an observer cast a vote.
#[test]
fn an_observer_denies_a_vote_request() {
    // Not in its own voter set: an observer.
    let mut m = machine(NodeId(9), &[NodeId(1), NodeId(2), NodeId(3)]);
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            voter_id: NodeId(9),
            ..VoteRequest::default()
        }
        .event(),
    );
    check!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
    );
    check!(
        m.quorum_state().voted_key.is_none(),
        "an observer must not record a vote"
    );
}

/// A candidate at our own epoch is not fenced.
///
/// Fencing is for a candidate *behind* us. Treating "equal" as behind
/// would deny every first-round vote, because a candidate that bumps to
/// epoch E asks replicas still at E.
#[test]
fn a_candidate_at_our_own_epoch_is_not_fenced() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(5, 0);
    // Both sides at epoch 0, the bootstrap epoch.
    check!(m.quorum_state().leader_epoch == 0);
    let actions = m.on_event(vote_event(NodeId(2), 0, false, 0, 5), &log, SimInstant(0));
    check!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: true, .. }))
    );
}

/// A vote or a pre-vote from a higher epoch moves us to that epoch before the
/// grant is decided, as `KafkaRaftClient.handleVoteRequest` does for both.
#[test]
fn a_vote_or_pre_vote_from_a_higher_epoch_advances_our_epoch() {
    for (pre_vote, want_epoch) in [(false, 7), (true, 7)] {
        let mut m = three_voter_machine();
        receive_vote(&mut m, vote_event(NodeId(2), 7, pre_vote, 1, 5));
        check!(
            m.quorum_state().leader_epoch == want_epoch,
            "pre_vote={pre_vote}: epoch {}",
            m.quorum_state().leader_epoch
        );
    }
}

#[test]
fn grants_standard_vote_when_log_up_to_date_and_not_voted() {
    let mut m = three_voter_machine();
    let actions = receive_vote(&mut m, vote_event(NodeId(2), 1, false, 1, 5));
    assert_binding_vote(&m, &actions, NodeId(2)); // binding
}

#[test]
fn denies_standard_vote_when_candidate_log_behind() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(10, 2);
    let actions = m.on_event(vote_event(NodeId(2), 2, false, 1, 3), &log, SimInstant(0));
    assert_vote_decision(&actions, false);
}

#[test]
fn pre_vote_grant_is_non_binding() {
    let mut m = three_voter_machine();
    receive_vote(&mut m, vote_event(NodeId(2), 1, true, 1, 5));
    // The epoch is adopted, but the pre-vote is not a vote: none is recorded.
    assert2::assert!((m.quorum_state().voted_key, m.quorum_state().leader_epoch) == (None, 1));
}

/// Deliver a vote against voter 1's canonical log at time zero.
fn receive_vote(m: &mut QuorumStateMachine, event: Event) -> Vec<Action> {
    m.on_event(event, &OUR_LOG, SimInstant(0))
}

fn assert_binding_vote(machine: &QuorumStateMachine, actions: &[Action], candidate: NodeId) {
    assert2::assert!(has_vote_reply(actions, candidate, true));
    assert2::assert!(machine.quorum_state().voted_key.map(|key| key.id) == Some(candidate));
}

fn assert_vote_decision(actions: &[Action], granted: bool) {
    assert2::assert!(actions.iter().any(|action| matches!(
        action, Action::ReplyVote { granted: reply, .. } if *reply == granted
    )));
}

fn assert_initial_vote_state(machine: &QuorumStateMachine) {
    assert2::assert!(
        (
            machine.quorum_state().leader_epoch,
            machine.quorum_state().voted_key
        ) == (0, None)
    );
}

fn assert_vote_ignored(machine: &QuorumStateMachine, actions: &[Action]) {
    assert2::assert!((actions.is_empty(), machine.quorum_state().voted_key) == (true, None));
}

fn has_vote_reply(actions: &[Action], to: NodeId, granted: bool) -> bool {
    actions.iter().any(|action| {
        matches!(
            action,
            Action::ReplyVote { to: reply_to, granted: reply_granted, .. }
                if *reply_to == to && *reply_granted == granted
        )
    })
}

struct VoteRequest {
    cluster_id: Option<uuid::Uuid>,
    voter_id: NodeId,
    voter_directory_id: uuid::Uuid,
    candidate: NodeId,
    candidate_directory_id: uuid::Uuid,
    epoch: u32,
    log_end: LogEnd,
    pre_vote: bool,
}

impl Default for VoteRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            voter_id: NodeId(1),
            voter_directory_id: uuid::Uuid::nil(),
            candidate: NodeId(2),
            candidate_directory_id: uuid::Uuid::nil(),
            epoch: 1,
            log_end: LogEnd {
                last_epoch: 1,
                last_offset: 5,
            },
            pre_vote: false,
        }
    }
}

impl VoteRequest {
    fn event(self) -> Event {
        Event::ReceiveVoteRequest {
            from: self.candidate,
            cluster_id: self.cluster_id,
            voter_id: self.voter_id,
            voter_directory_id: self.voter_directory_id,
            candidate_epoch: self.epoch,
            candidate: self.candidate,
            candidate_directory_id: self.candidate_directory_id,
            candidate_log_end: self.log_end,
            pre_vote: self.pre_vote,
        }
    }
}

/// One inbound Vote or pre-vote from a candidate with the given log end.
fn vote_event(
    candidate: NodeId,
    epoch: u32,
    pre_vote: bool,
    last_epoch: u32,
    last_offset: i64,
) -> Event {
    VoteRequest {
        candidate,
        epoch,
        pre_vote,
        log_end: LogEnd {
            last_epoch,
            last_offset,
        },
        ..VoteRequest::default()
    }
    .event()
}

/// Voter 1's log ends at offset 5 in epoch 1: node 2's log is up to date at
/// `(1, 5)` and behind at `(1, 4)`.
const OUR_LOG: FakeLog = FakeLog::new(5, 1);

/// A machine's reply to a Vote or pre-vote request.
#[derive(Debug, PartialEq, Eq)]
struct VoteAnswer {
    granted: bool,
    /// The epoch in the reply.
    epoch: u32,
}

/// What `m` answers a Vote or pre-vote of `epoch` from node 2.
fn vote_answer(
    m: &mut QuorumStateMachine,
    epoch: u32,
    pre_vote: bool,
    up_to_date: bool,
) -> VoteAnswer {
    let last_offset = if up_to_date { 5 } else { 4 };
    let actions = receive_vote(m, vote_event(NodeId(2), epoch, pre_vote, 1, last_offset));
    actions
        .iter()
        .find_map(|action| match action {
            Action::ReplyVote { epoch, granted, .. } => Some(VoteAnswer {
                granted: *granted,
                epoch: *epoch,
            }),
            _ => None,
        })
        .expect("a vote request is answered")
}

/// A test state machine, built fresh for each row of a table.
type Build = Box<dyn Fn() -> QuorumStateMachine>;

/// A machine for voter 1 in a three-voter quorum that follows node 3 at epoch
/// 4, and has fetched from it when `fetched`.
fn follower_of_three(fetched: bool) -> QuorumStateMachine {
    let mut m = three_voter_machine();
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(3),
            leader_epoch: 4,
        },
        &OUR_LOG,
        SimInstant(10),
    );
    if fetched {
        m.on_event(
            Event::ReceiveFetchResponse {
                leader_id: NodeId(3),
                leader_epoch: 4,
                diverging: None,
            },
            &OUR_LOG,
            SimInstant(20),
        );
    }
    assert2::assert!(matches!(m.role(), Role::Follower { .. }));
    m
}

/// A machine for voter 1 in a three-voter quorum that leads epoch 1.
fn leader_of_three() -> QuorumStateMachine {
    let mut m = three_voter_machine();
    m.on_event(Event::ElectionTimeout, &OUR_LOG, SimInstant(0));
    for epoch in [0, 1] {
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch,
                vote_granted: true,
            },
            &OUR_LOG,
            SimInstant(1),
        );
    }
    assert2::assert!(m.role().is_leader());
    m
}

/// Each state's `canGrantVote` for a request at the state's own epoch: a
/// follower grants a pre-vote only until it has fetched from its leader, a
/// leader grants nothing, a candidate or a resigned leader grants a pre-vote on
/// log recency alone, and nothing but an unattached, prospective or voted
/// replica grants a binding vote.
#[test]
fn each_state_grants_pre_votes_and_votes_by_its_own_rule() {
    struct Case {
        label: &'static str,
        build: Build,
        /// The machine's epoch.
        epoch: u32,
        /// Whether a pre-vote is granted when the candidate is up to date.
        pre_vote_granted: bool,
        /// Whether a vote is granted when the candidate is up to date.
        vote_granted: bool,
    }

    let unattached = three_voter_machine;
    let candidate = || {
        let mut m = leader_of_three();
        m.on_event(Event::CheckQuorumTimeout, &OUR_LOG, SimInstant(2));
        // A resigned leader's election timer starts the next pre-vote round.
        m.on_event(Event::ElectionTimeout, &OUR_LOG, SimInstant(3));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(3),
                epoch: m.quorum_state().leader_epoch,
                vote_granted: true,
            },
            &OUR_LOG,
            SimInstant(4),
        );
        assert2::assert!(matches!(m.role(), Role::Candidate { .. }), "{:?}", m.role());
        m
    };
    let resigned = || {
        let mut m = leader_of_three();
        m.on_event(Event::CheckQuorumTimeout, &OUR_LOG, SimInstant(2));
        assert2::assert!(matches!(m.role(), Role::Resigned));
        m
    };
    let cases = vec![
        Case {
            label: "leader",
            build: Box::new(leader_of_three),
            epoch: 1,
            pre_vote_granted: false,
            vote_granted: false,
        },
        Case {
            label: "follower not yet fetched",
            build: Box::new(|| follower_of_three(false)),
            epoch: 4,
            pre_vote_granted: true,
            vote_granted: false,
        },
        Case {
            label: "follower that has fetched",
            build: Box::new(|| follower_of_three(true)),
            epoch: 4,
            pre_vote_granted: false,
            vote_granted: false,
        },
        Case {
            label: "candidate",
            build: Box::new(candidate),
            epoch: 2,
            pre_vote_granted: true,
            vote_granted: false,
        },
        Case {
            label: "resigned leader",
            build: Box::new(resigned),
            epoch: 1,
            pre_vote_granted: true,
            vote_granted: false,
        },
        Case {
            label: "unattached",
            build: Box::new(unattached),
            epoch: 0,
            pre_vote_granted: true,
            vote_granted: true,
        },
    ];
    for Case {
        label,
        build,
        epoch,
        pre_vote_granted,
        vote_granted,
    } in cases
    {
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, true, true)
                == VoteAnswer {
                    granted: pre_vote_granted,
                    epoch,
                },
            "{label}: pre-vote"
        );
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, false, true)
                == VoteAnswer {
                    granted: vote_granted,
                    epoch,
                },
            "{label}: vote"
        );
        let mut m = build();
        check!(
            vote_answer(&mut m, epoch, true, false)
                == VoteAnswer {
                    granted: false,
                    epoch,
                },
            "{label}: pre-vote with a log that is behind"
        );
    }
}

/// A stale leader or follower that sees a pre-vote from a higher epoch steps
/// down to that epoch and answers from there, with the new epoch, so the
/// election proceeds without waiting for a check-quorum or fetch timeout.
#[test]
fn a_pre_vote_from_a_higher_epoch_steps_down_a_stale_leader_or_follower() {
    struct Case {
        label: &'static str,
        build: Build,
    }
    let cases = vec![
        Case {
            label: "leader",
            build: Box::new(leader_of_three),
        },
        Case {
            label: "follower that has fetched",
            build: Box::new(|| follower_of_three(true)),
        },
    ];
    for Case { label, build } in cases {
        let mut m = build();
        let epoch = m.quorum_state().leader_epoch + 3;
        check!(
            vote_answer(&mut m, epoch, true, true)
                == VoteAnswer {
                    granted: true,
                    epoch,
                },
            "{label}: grants at the new epoch"
        );
        check!(
            (
                matches!(m.role(), Role::Unattached { .. }),
                m.quorum_state().leader_epoch,
                m.quorum_state().leader_id,
                m.quorum_state().voted_key,
            ) == (true, epoch, None, None),
            "{label}: is unattached at the new epoch"
        );
    }
}

/// Only the candidate a replica already voted for gets its vote again, however
/// the candidate's log compares, as `unattachedOrProspectiveCanGrantVote` says.
#[test]
fn a_vote_is_granted_again_only_to_the_candidate_it_went_to() {
    let mut m = three_voter_machine();
    check!(
        vote_answer(&mut m, 1, false, true)
            == VoteAnswer {
                granted: true,
                epoch: 1,
            }
    );
    check!(
        vote_answer(&mut m, 1, false, false)
            == VoteAnswer {
                granted: true,
                epoch: 1,
            },
        "same candidate"
    );
    let actions = receive_vote(&mut m, vote_event(NodeId(3), 1, false, 1, 5));
    check!(
        actions
            .iter()
            .any(|action| matches!(action, Action::ReplyVote { granted: false, .. })),
        "another candidate"
    );
}

/// Whether `m` grants a binding vote from `candidate` at `epoch`, with the
/// candidate's log level with ours.
fn grants_vote_to(m: &mut QuorumStateMachine, candidate: NodeId, epoch: u32) -> bool {
    receive_vote(m, vote_event(candidate, epoch, false, 1, 5))
        .iter()
        .find_map(|action| match action {
            Action::ReplyVote { granted, .. } => Some(*granted),
            _ => None,
        })
        .expect("a vote request is answered")
}

/// A vote belongs to its epoch. A replica that votes for node 2, and then hears
/// that node 3 won the same epoch, keeps its vote as Kafka's
/// `QuorumState.transitionToFollower` does, so it grants no other candidate a
/// binding vote at that epoch however it later loses the leader. Without it,
/// voter 1 would count once for node 2 and once for node 3 in epoch 4, and two
/// leaders could hold that epoch.
#[test]
fn a_vote_survives_following_the_leader_of_its_epoch() {
    struct Case {
        label: &'static str,
        event: Event,
    }

    let end_epoch = SuccessorRank {
        position: 1,
        successors: 1,
    };
    let cases = vec![
        Case {
            label: "the leader resigns",
            event: Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(3),
                leader_epoch: 4,
                successor_rank: end_epoch,
            },
        },
        Case {
            label: "the fetch times out",
            event: Event::FetchTimeout,
        },
    ];
    for Case { label, event } in cases {
        let mut m = three_voter_machine();
        check!(
            vote_answer(&mut m, 4, false, true)
                == VoteAnswer {
                    granted: true,
                    epoch: 4,
                },
            "{label}"
        );
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(3),
                leader_epoch: 4,
            },
            &OUR_LOG,
            SimInstant(10),
        );
        check!(
            (
                matches!(m.role(), Role::Follower { .. }),
                m.quorum_state().voted_key.map(|key| key.id),
            ) == (true, Some(NodeId(2))),
            "{label}: following node 3 at epoch 4"
        );

        m.on_event(event, &OUR_LOG, SimInstant(20));

        check!(
            m.quorum_state().voted_key.map(|key| key.id) == Some(NodeId(2)),
            "{label}: still voted for node 2"
        );
        check!(!grants_vote_to(&mut m, NodeId(3), 4), "{label}: node 3");
        check!(
            grants_vote_to(&mut m, NodeId(2), 4),
            "{label}: node 2 again"
        );
    }
}

/// A newer epoch is a new ballot: the replica that followed node 3 at epoch 4
/// has no vote left at epoch 5.
#[test]
fn a_vote_does_not_outlive_its_epoch() {
    let mut m = three_voter_machine();
    check!(
        vote_answer(&mut m, 4, false, true)
            == VoteAnswer {
                granted: true,
                epoch: 4,
            }
    );
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(3),
            leader_epoch: 5,
        },
        &OUR_LOG,
        SimInstant(10),
    );

    check!(m.quorum_state().voted_key.is_none());
}

/// A replica that followed the leader of an epoch, and then stopped, knows a
/// leader for that epoch, so it grants no binding vote in it: Kafka's
/// `ProspectiveState` keeps the `leaderId` of the follower it came from, and
/// `unattachedOrProspectiveCanGrantVote` refuses when one is known. It still
/// grants a pre-vote, or a cluster that lost its leader could not elect a new
/// one, and it still attaches to a leader announced for its epoch, because
/// `leader_id` in the quorum state is `None` while it prospects.
#[test]
fn a_prospective_replica_that_followed_a_leader_grants_no_binding_vote_in_that_epoch() {
    let mut m = follower_of_three(false);

    m.on_event(Event::FetchTimeout, &OUR_LOG, SimInstant(30));

    check!(
        (
            matches!(m.role(), Role::Prospective {
                abandoned_leader: Some(NodeId(3)),
                ..
            }),
            m.quorum_state().leader_id,
            m.quorum_state().voted_key,
        ) == (true, None, None)
    );
    check!(!grants_vote_to(&mut m, NodeId(2), 4), "binding vote");
    check!(
        vote_answer(&mut m, 4, true, true)
            == VoteAnswer {
                granted: true,
                epoch: 4,
            },
        "pre-vote"
    );
}

/// The belief is not lost when the election timer starts the round again.
#[test]
fn a_second_pre_vote_round_still_remembers_the_abandoned_leader() {
    let mut m = follower_of_three(false);
    m.on_event(Event::FetchTimeout, &OUR_LOG, SimInstant(30));

    m.on_event(Event::ElectionTimeout, &OUR_LOG, SimInstant(60));

    check!(
        matches!(m.role(), Role::Prospective {
            abandoned_leader: Some(NodeId(3)),
            ..
        }),
        "{:?}",
        m.role()
    );
    check!(!grants_vote_to(&mut m, NodeId(2), 4));
}

#[test]
fn denies_standard_vote_when_already_voted_for_other() {
    let mut m = three_voter_machine();
    // vote for 2 first
    receive_vote(&mut m, vote_event(NodeId(2), 1, false, 1, 5));
    // now 3 asks in the same epoch
    let actions = receive_vote(&mut m, vote_event(NodeId(3), 1, false, 1, 5));
    assert2::assert!(has_vote_reply(&actions, NodeId(3), false));
}

#[test]
fn fenced_when_candidate_epoch_below_current() {
    let mut m = three_voter_machine();
    m.force_epoch(5); // test helper
    let log = FakeLog::new(5, 5);
    let actions = m.on_event(vote_event(NodeId(2), 3, false, 5, 5), &log, SimInstant(0));
    assert_vote_decision(&actions, false);
}

#[test]
fn vote_from_adjacent_voter_view_is_granted_when_up_to_date() {
    // KIP-853 permits an up-to-date candidate from an adjacent voter view;
    // only the local latest set determines whether this replica may vote.
    let mut m = three_voter_machine();
    let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
    let actions = receive_vote(&mut m, vote_event(NodeId(99), 1, false, 1, 5));
    assert2::assert!(
        m.quorum_state()
            .voted_key
            .is_some_and(|key| key.id == NodeId(99))
    );
    assert_vote_decision(&actions, true);
}

#[test]
fn vote_addressed_to_other_voter_rejected() {
    // C-2: a Vote addressed (voter_id) to a different node than us is
    // ignored, even if the candidate is a legitimate voter.
    let mut m = three_voter_machine();
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            voter_id: NodeId(3),
            ..VoteRequest::default()
        }
        .event(),
    );
    assert_vote_ignored(&m, &actions);
}

#[test]
fn vote_from_voter_addressed_to_us_still_granted() {
    // C-2 must not break the legitimate path: a voter candidate addressing
    // us is still granted.
    let mut m = three_voter_machine();
    let actions = receive_vote(&mut m, vote_event(NodeId(3), 1, false, 1, 5));
    assert_binding_vote(&m, &actions, NodeId(3));
}

#[test]
fn zero_target_is_not_a_wildcard_for_a_nonzero_voter() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            voter_id: NodeId(0),
            ..VoteRequest::default()
        }
        .event(),
    );
    assert_vote_ignored(&m, &actions);
}

#[test]
fn zero_target_is_valid_for_voter_zero() {
    let mut m = machine(NodeId(0), &[NodeId(0), NodeId(2)]);
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            voter_id: NodeId(0),
            ..VoteRequest::default()
        }
        .event(),
    );
    assert_vote_decision(&actions, true);
    assert2::assert!(m.quorum_state().voted_key.map(|key| key.id) == Some(NodeId(2)));
}

#[test]
fn stale_target_directory_is_ignored_before_epoch_mutation() {
    let DynamicMachine {
        machine: mut m,
        cluster_id,
        candidate_directory_id,
        ..
    } = dynamic_machine();
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            cluster_id: Some(cluster_id),
            voter_directory_id: uuid::Uuid::from_u128(99),
            candidate_directory_id,
            epoch: 7,
            ..VoteRequest::default()
        }
        .event(),
    );
    assert2::assert!(actions.is_empty());
    assert_initial_vote_state(&m);
}

#[test]
fn stale_candidate_directory_is_denied_before_epoch_mutation() {
    let DynamicMachine {
        machine: mut m,
        cluster_id,
        voter_directory_id,
        ..
    } = dynamic_machine();
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            cluster_id: Some(cluster_id),
            voter_directory_id,
            candidate_directory_id: uuid::Uuid::from_u128(99),
            epoch: 7,
            ..VoteRequest::default()
        }
        .event(),
    );
    assert_vote_decision(&actions, false);
    assert_initial_vote_state(&m);
}

#[test]
fn foreign_cluster_is_denied_before_epoch_mutation() {
    let DynamicMachine {
        machine: mut m,
        voter_directory_id,
        candidate_directory_id,
        ..
    } = dynamic_machine();
    let actions = receive_vote(
        &mut m,
        VoteRequest {
            cluster_id: Some(uuid::Uuid::from_u128(99)),
            voter_directory_id,
            candidate_directory_id,
            epoch: 7,
            ..VoteRequest::default()
        }
        .event(),
    );
    assert_vote_decision(&actions, false);
    assert_initial_vote_state(&m);
}

#[test]
fn prevote_rejected_when_candidate_log_is_not_up_to_date() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(10, 2);
    let actions = m.on_event(vote_event(NodeId(2), 2, true, 1, 5), &log, SimInstant(0));
    assert_vote_decision(&actions, false);
}
