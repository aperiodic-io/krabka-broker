use assert2::check;

use super::*;
use crate::{
    core::test_support::{CellLog, FakeLog, RunsLog, machine, three_voter_machine, win_election},
    event::Event,
};

fn three_voter_leader(log: &dyn LogView) -> QuorumStateMachine {
    let mut machine = three_voter_machine();
    win_election(&mut machine, log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    machine
}

fn expected_quorum_loss(transition: &'static str, timer: Action) -> Vec<Action> {
    vec![
        Action::SendEndQuorumEpoch {
            epoch: 1,
            preferred_successors: vec![NodeId(2), NodeId(3)],
        },
        Action::PersistQuorumState,
        Action::TransitionedTo(transition),
        timer,
    ]
}

/// Capture the promotion log end before replication tests grow the log.
/// The election and the two grants retain their separate event times.
fn promote_with_second_voter(m: &mut QuorumStateMachine, log: &dyn LogView) {
    m.on_event(Event::ElectionTimeout, log, SimInstant(2000));
    for (epoch, now) in [(0, SimInstant(2001)), (1, SimInstant(2002))] {
        m.on_event(
            Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch,
                vote_granted: true,
            },
            log,
            now,
        );
    }
}

#[test]
fn leader_advances_hwm_at_majority_fetch_offset() {
    let mut m = three_voter_machine();
    // The log end is 0 at promotion so the leader's `epoch_start_offset` is
    // 0; the leader-completeness gate then permits advancing the HWM to any
    // majority offset > 0. After promotion the log grows to end 10, which is
    // what followers replicate against.
    let log = CellLog::new(0, 1);
    // drive to leader (epoch_start_offset captured as end_offset() == 0)
    promote_with_second_voter(&mut m, &log);
    assert2::assert!(matches!(
        m.role(),
        Role::Leader {
            epoch_start_offset: 0,
            ..
        }
    ));
    // Leader's log now ends at 10. follower 2 fetches at 8, follower 3 at 4.
    log.end.set(10);
    let a2 = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    // majority of {self=10, 2=8} = 8, and 8 > epoch_start_offset 0 → advances
    assert2::assert!(
        a2.iter()
            .any(|a| matches!(a, Action::AdvanceHighWatermark(8)))
    );
    let _ = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(3),
            fetch_epoch: 1,
            fetch_offset: 4,
        },
        &log,
        SimInstant(2101),
    );
    // sorted match offsets {10,8,4}; majority (2nd highest) = 8 → no regress
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!()
    }
}

#[test]
fn leader_hwm_does_not_regress_on_reordered_stale_fetch() {
    // A follower's recorded `fetch_offset` can fall (a reordered/stale fetch,
    // or a legitimate post-truncation re-fetch), dropping the `majority()`-th
    // match offset below the current HWM. `recompute_high_watermark` must
    // clamp to the existing HWM (never regress) rather than return a lower
    // value — historically a lower return tripped a debug-only assertion.
    let mut m = three_voter_machine();
    let log = CellLog::new(0, 1);
    promote_with_second_voter(&mut m, &log);
    // Leader log ends at 10; follower 2 fetches at 8 → HWM advances to 8.
    log.end.set(10);
    m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!("expected leader")
    }
    // Now follower 2 sends a STALE lower fetch (offset 2 < its prior 8).
    // match offsets {self=10, 2=2, 3=0} → majority (2nd highest) = 2, which is
    // below the current HWM 8. The HWM must hold at 8, and no spurious
    // AdvanceHighWatermark may be emitted. (Pre-clamp this panicked.)
    let a = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 2,
        },
        &log,
        SimInstant(2101),
    );
    assert2::assert!(
        !a.iter()
            .any(|x| matches!(x, Action::AdvanceHighWatermark(_)))
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 8);
    } else {
        panic!("expected leader")
    }
}

#[test]
fn leader_holds_hwm_for_prior_epoch_entries_until_current_epoch_committed() {
    // Leader-completeness (Raft Fig.8): a leader promoted at log end 10
    // (epoch_start_offset = 10) must NOT advance the HWM to a majority
    // offset that only covers prior-epoch entries (8 < 10).
    let mut m = three_voter_machine();
    let log = FakeLog::new(10, 1);
    promote_with_second_voter(&mut m, &log);
    assert2::assert!(matches!(
        m.role(),
        Role::Leader {
            epoch_start_offset: 10,
            ..
        }
    ));
    // follower 2 fetches at 8: majority of {10, 8} = 8, but 8 <= 10 → hold.
    let a2 = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 8,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        !a2.iter()
            .any(|a| matches!(a, Action::AdvanceHighWatermark(_)))
    );
    if let Role::Leader { high_watermark, .. } = m.role() {
        assert2::assert!(*high_watermark == 0);
    } else {
        panic!()
    }
}

/// krabka-io/krabka-broker#785: the leader validates a follower's fetch
/// position as Kafka's `RaftLog.validateOffsetAndEpoch` does.
///
/// The leader's log holds epoch 4 over offsets 0 to 120 and epoch 6 from 120
/// to 200. Epoch 5 is the classic lost-election epoch: an old leader appended
/// it, and no voter that elected epoch 6 has it. A fetch is valid only when
/// the leader holds the fetch epoch itself out to the fetch offset. Anything
/// else is answered with the leader's `(epoch, end offset)` for that epoch,
/// and records no follower progress.
#[test]
fn a_fetch_is_valid_only_at_an_epoch_the_leader_holds_to_that_offset() {
    struct Case {
        what: &'static str,
        fetch_epoch: Epoch,
        fetch_offset: i64,
        diverging: Option<LogOffsetMetadata>,
    }
    let diverging = |epoch, offset| Some(LogOffsetMetadata { offset, epoch });
    let cases = [
        Case {
            what: "inside an epoch the leader holds",
            fetch_epoch: 4,
            fetch_offset: 100,
            diverging: None,
        },
        Case {
            what: "past the end of that epoch",
            fetch_epoch: 4,
            fetch_offset: 130,
            diverging: diverging(4, 120),
        },
        Case {
            what: "an epoch between two the leader holds",
            fetch_epoch: 5,
            fetch_offset: 100,
            diverging: diverging(4, 120),
        },
        Case {
            what: "that epoch beyond the leader's log end",
            fetch_epoch: 5,
            fetch_offset: 300,
            diverging: diverging(4, 120),
        },
        Case {
            what: "inside the current epoch",
            fetch_epoch: 6,
            fetch_offset: 150,
            diverging: None,
        },
        Case {
            what: "an epoch newer than the leader's log",
            fetch_epoch: 7,
            fetch_offset: 150,
            diverging: diverging(6, 200),
        },
    ];
    let log = RunsLog::new(&[(4, 120), (6, 80)]);
    for case in cases {
        let mut m = three_voter_leader(&log);
        let actions = m.on_event(
            Event::ReceiveFetch {
                from: NodeId(2),
                fetch_epoch: case.fetch_epoch,
                fetch_offset: case.fetch_offset,
            },
            &log,
            SimInstant(2100),
        );
        let answered = actions.iter().find_map(|action| match action {
            Action::ReplyDivergingEpoch(point) => Some(*point),
            _ => None,
        });
        assert2::check!(answered == case.diverging, "{}", case.what);
        let Role::Leader { replicas, .. } = m.role() else {
            panic!("{}: expected leader", case.what);
        };
        let recorded = replicas[&NodeId(2)].fetch_offset;
        let expected = if case.diverging.is_none() {
            case.fetch_offset
        } else {
            0
        };
        assert2::check!(recorded == expected, "{}", case.what);
    }
}

/// krabka-io/krabka-broker#785: a follower at an epoch the leader does not
/// hold must not commit anything.
///
/// The leader is promoted at offset 120 and appends epoch 6 out to 200.
/// Follower 2 kept a divergent epoch 5 tail out to offset 190 and follower 3
/// has not fetched yet. If the leader counted follower 2, the majority
/// `{200, 190}` would commit offset 190, although no second voter holds the
/// leader's records there.
#[test]
fn a_fetch_at_an_epoch_the_leader_lacks_does_not_raise_the_high_watermark() {
    let mut m = three_voter_machine();
    let at_promotion = RunsLog::new(&[(4, 120)]);
    win_election(
        &mut m,
        &at_promotion,
        &[NodeId(2), NodeId(3)],
        SimInstant(2000),
    );
    let log = RunsLog::new(&[(4, 120), (6, 80)]);
    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 5,
            fetch_offset: 190,
        },
        &log,
        SimInstant(2100),
    );
    assert2::check!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::AdvanceHighWatermark(_)))
    );
    let Role::Leader { high_watermark, .. } = m.role() else {
        panic!("expected leader");
    };
    assert2::assert!(*high_watermark == 0);
}

/// krabka-io/krabka-broker#785: a follower truncates as Kafka's
/// `RaftLog.truncateToEndOffset` does, to the lower of the leader's end for
/// the epoch and its own end for it.
///
/// The follower holds epoch 4 over 0 to 110 and a divergent epoch 5 from 110
/// to 130. The leader answers `(4, 120)`. Truncating to 120 keeps ten epoch 5
/// records, and the next fetch at epoch 5 diverges at the same point for
/// ever. Its own epoch 4 ends at 110, so that is where it truncates.
///
/// Then it fetches again at once, as Kafka's follower does after it truncates
/// on a diverging epoch: before, it sent nothing more and sat idle until its
/// fetch timer started an election.
#[test]
fn a_follower_truncates_to_where_both_logs_still_hold_the_epoch() {
    struct Case {
        what: &'static str,
        hint: LogOffsetMetadata,
        truncate_to: i64,
    }
    let point = |epoch, offset| LogOffsetMetadata { offset, epoch };
    let cases = [
        Case {
            what: "the follower's own copy of the epoch ends first",
            hint: point(4, 120),
            truncate_to: 110,
        },
        Case {
            what: "the leader's copy of the epoch ends first",
            hint: point(4, 100),
            truncate_to: 100,
        },
        Case {
            what: "an epoch the follower does not hold",
            hint: point(3, 100),
            truncate_to: 0,
        },
        Case {
            what: "epoch 0 truncates to the lower end offset",
            hint: point(0, 500),
            truncate_to: 130,
        },
    ];
    let log = RunsLog::new(&[(4, 110), (5, 20)]);
    for case in cases {
        let mut m = three_voter_machine();
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 6,
            },
            &log,
            SimInstant(10),
        );
        let actions = m.on_event(
            Event::ReceiveFetchResponse {
                leader_id: NodeId(2),
                leader_epoch: 6,
                diverging: Some(case.hint),
            },
            &log,
            SimInstant(11),
        );
        // Truncated, it fetches again at once from the new log end and
        // re-arms its fetch timer: a diverging answer is a successful fetch.
        assert2::check!(
            actions
                == vec![
                    Action::TruncateTo(LogOffsetMetadata {
                        offset: case.truncate_to,
                        epoch: case.hint.epoch,
                    }),
                    Action::SendFetch {
                        leader_id: NodeId(2)
                    },
                    Action::ResetTimer {
                        kind: TimerKind::Fetch,
                        deadline: SimInstant(11).saturating_add_ms(m.election_timeout_ms),
                    },
                ],
            "{}",
            case.what
        );
    }
}

fn armed_check_quorum(actions: &[Action]) -> Option<SimInstant> {
    actions.iter().find_map(|a| match a {
        Action::ResetTimer {
            kind: TimerKind::CheckQuorum,
            deadline,
        } => Some(*deadline),
        _ => None,
    })
}

fn elect_empty_log(voter_ids: &[NodeId]) -> (QuorumStateMachine, FakeLog, Vec<Action>) {
    let mut m = machine(NodeId(1), voter_ids);
    let log = FakeLog::new(0, 0);
    let peers: Vec<NodeId> = voter_ids
        .iter()
        .copied()
        .filter(|&id| id != NodeId(1))
        .collect();
    let actions = win_election(&mut m, &log, &peers, SimInstant(2000));
    (m, log, actions)
}

/// A new leader arms the check-quorum window at 1.5x the fetch timeout, which
/// is Kafka's `CHECK_QUORUM_TIMEOUT_FACTOR` over the same configured extent the
/// follower fetch deadline uses. A lone voter has nobody to hear from, so it
/// arms nothing at all -- Kafka's `timeUntilCheckQuorumExpires` reports
/// `Long.MAX_VALUE` for a single-voter quorum.
#[test]
fn promotion_arms_the_check_quorum_window_unless_the_leader_is_alone() {
    // `TEST_ELECTION_TIMEOUT` is one second, so the window is 1500ms.
    for (name, voter_ids, want) in [
        (
            "three voters",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            Some(SimInstant(3500)),
        ),
        (
            "two voters",
            &[NodeId(1), NodeId(2)][..],
            Some(SimInstant(3500)),
        ),
        ("sole voter", &[NodeId(1)][..], None),
    ] {
        let (m, _log, actions) = elect_empty_log(voter_ids);
        assert2::assert!(m.role().is_leader(), "case {name}");
        assert2::check!(armed_check_quorum(&actions) == want, "case {name}");
    }
}

/// A fetch re-arms the window only once the leader has heard from the majority
/// it needs *besides itself*: one follower of three voters, two of five. Every
/// fetch below that count leaves the window running, so a leader that only ever
/// hears from a minority still reaches its deadline and resigns.
#[test]
fn only_a_majority_of_followers_re_arms_the_check_quorum_window() {
    for (name, voter_ids, fetchers, want) in [
        (
            "three voters, one follower is the majority",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            &[NodeId(2)][..],
            vec![Some(SimInstant(3600))],
        ),
        (
            "five voters, one follower is short",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2)][..],
            vec![None],
        ),
        (
            "five voters, two followers reach it",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2), NodeId(3)][..],
            vec![None, Some(SimInstant(3600))],
        ),
        (
            "a repeat fetch from one follower is not two voters",
            &[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)][..],
            &[NodeId(2), NodeId(2)][..],
            vec![None, None],
        ),
        (
            "a non-voter observer never counts",
            &[NodeId(1), NodeId(2), NodeId(3)][..],
            &[NodeId(9)][..],
            vec![None],
        ),
    ] {
        let (mut m, log, _actions) = elect_empty_log(voter_ids);
        let got: Vec<Option<SimInstant>> = fetchers
            .iter()
            .map(|&from| {
                armed_check_quorum(&m.on_event(
                    Event::ReceiveFetch {
                        from,
                        fetch_epoch: 1,
                        fetch_offset: 0,
                    },
                    &log,
                    SimInstant(2100),
                ))
            })
            .collect();
        assert2::check!(got == want, "case {name}");
    }
}

/// A `FetchSnapshot` is contact from that voter even though it moves no
/// replication progress, so Kafka scores it for check-quorum exactly as it
/// scores a Fetch. Without this a leader whose only reachable follower is
/// mid-snapshot resigns under a perfectly healthy quorum.
#[test]
fn a_snapshot_fetch_is_check_quorum_contact() {
    let log = FakeLog::new(0, 0);
    let mut m = three_voter_leader(&log);
    let actions = m.on_event(
        Event::ReceiveFetchSnapshot { from: NodeId(2) },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        actions
            == vec![Action::ResetTimer {
                kind: TimerKind::CheckQuorum,
                deadline: SimInstant(3600),
            }]
    );
}

/// A fetch that the leader answers with a truncation hint is still proof the
/// follower is talking to us, so it re-arms the window as well. Resigning the
/// quorum over a truncation round would cost a whole election for nothing.
#[test]
fn a_diverging_fetch_still_counts_as_contact() {
    let log = RunsLog::new(&[(1, 5)]);
    let mut m = three_voter_leader(&log);
    // Follower 2 claims epoch 1 out to offset 9; our epoch 1 ends at 5.
    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 9,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        actions
            == vec![
                Action::ResetTimer {
                    kind: TimerKind::CheckQuorum,
                    deadline: SimInstant(3600),
                },
                Action::ReplyDivergingEpoch(LogOffsetMetadata {
                    offset: 5,
                    epoch: 1,
                }),
            ]
    );
}

/// The window expiring is the leader losing the quorum: it drops the
/// leadership, tells the voters to elect, persists the change, and arms the
/// election timer that carries it into its next pre-vote round.
///
/// Clearing `leader_id` is the point of the whole mechanism. An isolated old
/// leader that keeps naming itself answers `DescribeQuorum`, Metadata and
/// `BrokerHeartbeat` as the controller leader for an epoch the rest of the
/// cluster has already replaced.
#[test]
fn check_quorum_expiry_resigns_the_leader() {
    let log = FakeLog::new(0, 0);
    let mut m = three_voter_leader(&log);
    assert2::assert!(m.quorum_state().leader_id == Some(NodeId(1)));

    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(3500));
    assert2::assert!(
        actions
            == expected_quorum_loss(
                "Resigned",
                Action::ResetTimer {
                    kind: TimerKind::Election,
                    deadline: SimInstant(
                        3500 + 1000 + crate::core::election_jitter_ms(NodeId(1), 1, 1000)
                    ),
                }
            )
    );
    assert2::assert!(
        (
            m.role().clone(),
            m.quorum_state().leader_id,
            m.quorum_state().leader_epoch
        ) == (Role::Resigned, None, 1)
    );
}

/// A resigned replica is not stuck: its election timer starts the ordinary
/// KIP-996 pre-vote round, which is how the surviving side of a healed
/// partition can hand it the leadership back.
#[test]
fn a_resigned_replica_elects_on_its_election_timer() {
    let log = FakeLog::new(0, 0);
    let mut m = three_voter_leader(&log);
    m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(3500));
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(5000));
    assert2::assert!(matches!(m.role(), Role::Prospective { .. }));
    assert2::assert!(actions.iter().any(|a| matches!(
        a,
        Action::SendVoteRequest {
            epoch: 1,
            pre_vote: true
        }
    )));
}

/// A sole voter has no quorum to lose, so the expiry is inert for it. Nothing
/// arms the timer there, but a stray tick must not depose the only replica that
/// can serve the cluster.
#[test]
fn a_sole_voter_never_resigns_on_check_quorum() {
    let mut m = machine(NodeId(1), &[NodeId(1)]);
    let log = FakeLog::new(0, 0);
    win_election(&mut m, &log, &[], SimInstant(2000));
    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(9000));
    assert2::assert!((actions, m.role().is_leader()) == (Vec::new(), true));
}

/// Only a leader owns a check-quorum window. A follower that somehow sees the
/// tick keeps following its leader rather than resigning a leadership it does
/// not hold.
#[test]
fn a_follower_ignores_a_check_quorum_tick() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(0, 0);
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 4,
        },
        &log,
        SimInstant(10),
    );
    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(9000));
    assert2::assert!((actions, m.quorum_state().leader_id) == (Vec::new(), Some(NodeId(2))));
}

/// A leader that was alone arms nothing at promotion, so the voter record that
/// grows the cluster past one voter is what must start its first window.
/// Without that, a leader promoted as the sole voter would never run
/// check-quorum again for the rest of its epoch.
#[test]
fn growing_past_one_voter_starts_the_leader_a_window() {
    let mut m = machine(NodeId(1), &[NodeId(1)]);
    let log = FakeLog::new(0, 0);
    let promotion = win_election(&mut m, &log, &[], SimInstant(2000));
    assert2::assert!(armed_check_quorum(&promotion) == None);

    let actions = m.apply_voter_set(
        crate::core::test_support::voters(&[NodeId(1), NodeId(2)]),
        SimInstant(4000),
    );
    assert2::assert!(
        actions
            == vec![Action::ResetTimer {
                kind: TimerKind::CheckQuorum,
                deadline: SimInstant(5500),
            }]
    );
}

/// A leader that an uncommitted `VotersRecord` has already removed keeps
/// serving Fetch until that record commits, and it is exactly then that it
/// needs the watchdog most: the one voter that can commit the record is the one
/// voter that can keep it alive. The single-voter exemption is about a leader
/// that IS the only voter, not about a one-voter set it has been dropped from.
#[test]
fn a_removed_leader_still_runs_check_quorum_over_the_remaining_voter() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
    let log = FakeLog::new(0, 0);
    win_election(&mut m, &log, &[NodeId(2)], SimInstant(2000));
    // The record that drops us leaves one voter, and we keep leading until it
    // commits.
    let applied = m.apply_voter_set(
        crate::core::test_support::voters(&[NodeId(2)]),
        SimInstant(3000),
    );
    assert2::assert!(m.role().is_leader());
    assert2::check!(armed_check_quorum(&applied) == Some(SimInstant(4500)));

    // That remaining voter is now the whole majority, so its fetch re-arms.
    let fetched = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 0,
        },
        &log,
        SimInstant(3100),
    );
    assert2::check!(armed_check_quorum(&fetched) == Some(SimInstant(4600)));

    // And silence from it still deposes us.
    m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(5000));
    assert2::assert!(!m.role().is_leader());
}

/// A membership change starts a whole new window, so the contacts tallied in
/// the old one must not be carried into it. Otherwise one stale fetch plus one
/// fresh fetch near the new deadline re-arms the timer even though no majority
/// reached the leader inside the window that is actually running.
#[test]
fn a_membership_change_starts_the_contact_tally_over() {
    let five = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
    let mut m = machine(NodeId(1), &five);
    let log = FakeLog::new(0, 0);
    win_election(&mut m, &log, &five[1..], SimInstant(2000));
    // One of the two followers this configuration needs.
    let first = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 0,
        },
        &log,
        SimInstant(2100),
    );
    assert2::check!(armed_check_quorum(&first) == None);

    // Swap voter 5 for voter 6: still five voters, still a threshold of two,
    // but a fresh window.
    m.apply_voter_set(
        crate::core::test_support::voters(&[NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(6)]),
        SimInstant(2200),
    );
    // A single fetch inside the new window is one voter, not two.
    let second = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(3),
            fetch_epoch: 1,
            fetch_offset: 0,
        },
        &log,
        SimInstant(2300),
    );
    assert2::assert!(armed_check_quorum(&second) == None);
}

/// A leader already removed from the voter set has no vote to elect with, and
/// no future leader will announce itself to a node outside the voter set. It
/// must therefore step down into observer discovery, the way
/// `finish_local_leader_removal` does, rather than into `Resigned` — where its
/// election timer is ignored, its fetch timer is cleared, and nothing would ever
/// reach it again.
#[test]
fn a_removed_leader_resigns_into_observer_discovery() {
    let log = FakeLog::new(0, 0);
    let mut m = three_voter_leader(&log);
    m.apply_voter_set(
        crate::core::test_support::voters(&[NodeId(2), NodeId(3)]),
        SimInstant(3000),
    );
    let actions = m.on_event(Event::CheckQuorumTimeout, &log, SimInstant(5000));
    assert2::assert!(
        actions
            == expected_quorum_loss(
                "Observer",
                Action::ResetTimer {
                    kind: TimerKind::Fetch,
                    deadline: SimInstant(6000),
                }
            )
    );
    assert2::assert!(
        (m.role().clone(), m.quorum_state().leader_id)
            == (
                Role::Observer {
                    leader_id: None,
                    fetch_deadline: SimInstant(6000),
                },
                None
            )
    );
}

/// An observer whose leader stops answering looks for the current leader.
///
/// Node 4 observes a three-voter quorum and attached to node 1 at epoch 1.
/// When its fetch timer fires, it forgets node 1 and arms the fetch timer
/// again, so its next Fetch discovers whichever voter leads now.
#[test]
fn an_observer_whose_fetches_time_out_looks_for_the_current_leader() {
    let mut m = machine(NodeId(4), &[NodeId(1), NodeId(2), NodeId(3)]);
    let log = FakeLog::new(0, 0);
    m.on_event(
        Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        },
        &log,
        SimInstant(1_000),
    );

    let actions = m.on_event(Event::FetchTimeout, &log, SimInstant(2_000));

    let deadline = SimInstant(2_000).saturating_add_ms(1_000);
    assert2::assert!(
        actions
            == vec![
                Action::PersistQuorumState,
                Action::TransitionedTo("Observer"),
                Action::ResetTimer {
                    kind: TimerKind::Fetch,
                    deadline,
                },
            ]
    );
    assert2::assert!(
        *m.role()
            == Role::Observer {
                leader_id: None,
                fetch_deadline: deadline,
            }
    );
    assert2::assert!((m.quorum_state().leader_id, m.quorum_state().leader_epoch) == (None, 1));
}

/// A resigning leader ranks the other voters by the fetch offsets it
/// validated. A divergent fetch moves no progress, so it cannot promote a
/// lagging voter.
#[test]
fn a_resigning_leader_ranks_successors_by_validated_progress() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);
    let log = FakeLog::new(10, 1);
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));
    assert2::assert!(m.role().is_leader());
    for (from, fetch_epoch, fetch_offset) in [(3, 1, 10), (2, 1, 4), (4, 7, 20)] {
        m.on_event(
            Event::ReceiveFetch {
                from: NodeId(from),
                fetch_epoch,
                fetch_offset,
            },
            &log,
            SimInstant(2100),
        );
    }

    assert2::assert!(m.preferred_successors() == vec![NodeId(3), NodeId(2), NodeId(4)]);
}

#[test]
fn fetch_at_offset_zero_with_unknown_epoch_does_not_diverge() {
    let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
    let log = FakeLog::new(10, 1);
    win_election(&mut m, &log, &[NodeId(2)], SimInstant(2000));
    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 99,
            fetch_offset: 0,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::ReplyDivergingEpoch(_)))
    );
}

#[test]
fn preferred_successors_ranks_offset_zero_above_unfetched_voter() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(10, 1);
    win_election(&mut m, &log, &[NodeId(2)], SimInstant(2000));
    m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 0,
        },
        &log,
        SimInstant(2100),
    );
    assert2::assert!(m.preferred_successors() == vec![NodeId(2), NodeId(3)]);
}

#[test]
fn fetch_caught_up_updates_last_caught_up_and_observer_does_not_reset_check_quorum() {
    let mut m = three_voter_machine();
    let log = FakeLog::new(10, 1);
    win_election(&mut m, &log, &[NodeId(2), NodeId(3)], SimInstant(2000));

    m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 5,
        },
        &log,
        SimInstant(2100),
    );
    if let Role::Leader { replicas, .. } = m.role() {
        check!(replicas[&NodeId(2)].last_caught_up == SimInstant(0));
    }

    m.on_event(
        Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 10,
        },
        &log,
        SimInstant(2200),
    );
    if let Role::Leader { replicas, .. } = m.role() {
        check!(replicas[&NodeId(2)].last_caught_up == SimInstant(2200));
    }

    let actions = m.on_event(
        Event::ReceiveFetch {
            from: NodeId(99),
            fetch_epoch: 1,
            fetch_offset: 10,
        },
        &log,
        SimInstant(2300),
    );
    check!(!actions.iter().any(|a| matches!(
        a,
        Action::ResetTimer {
            kind: TimerKind::CheckQuorum,
            ..
        }
    )));
}
