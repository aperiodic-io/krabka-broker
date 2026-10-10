//! Tests for the KIP-853 control records: the control state's
//! apply/commit/truncate behaviour, the `LeaderChange` batch the leader
//! appends, and the reasons a reconfiguration is refused before it is proposed.

use assert2::{assert, check};

use super::*;
use crate::kraft::controller::{
    control_state::{voter_set_from_wire, voter_set_to_wire},
    records::leader_change_batch,
    test_support::{
        EngineSetup, build_engine_only, elect_single_voter_engine, one_offset_batch, topic_record,
        voter_set,
    },
};

fn wire_voter(id: i32, directory_byte: u8) -> krabka_protocol::owned::voters_record::Voter {
    use krabka_protocol::owned::voters_record::{Endpoint, KRaftVersionFeature, Voter};

    Voter {
        voter_id: id,
        voter_directory_id: krabka_protocol::primitives::uuid::Uuid([directory_byte; 16]),
        endpoints: vec![Endpoint {
            name: "CONTROLLER".into(),
            host: "controller.example".into(),
            port: 9_093,
            ..Default::default()
        }],
        k_raft_version_feature: KRaftVersionFeature {
            min_supported_version: 0,
            max_supported_version: 1,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn wire_voter_record(
    voters: Vec<krabka_protocol::owned::voters_record::Voter>,
) -> krabka_protocol::owned::voters_record::VotersRecord {
    krabka_protocol::owned::voters_record::VotersRecord {
        version: 0,
        voters,
        ..Default::default()
    }
}

#[test]
fn control_state_applies_before_commit_and_restores_on_truncation() {
    let initial = voter_set(&[NodeId(1)]);
    let two_voters = voter_set(&[NodeId(1), NodeId(2)]);
    let replacement = voter_set(&[NodeId(1), NodeId(3)]);
    let mut controls = KraftControlState::new(initial.clone(), 0);

    controls
        .apply(5, &ControlRecord::Voters(voter_set_to_wire(&two_voters)))
        .unwrap();
    assert!(controls.latest_voters() == &two_voters);
    assert!(controls.committed_voters == initial);
    assert!(controls.commit_to(6));
    assert!(controls.committed_voters == two_voters);

    controls
        .apply(7, &ControlRecord::Voters(voter_set_to_wire(&replacement)))
        .unwrap();
    assert!(controls.latest_voters() == &replacement);
    controls.truncate_to(7);
    assert!(controls.latest_voters() == &two_voters);
    assert!(!controls.commit_to(8));
    assert!(controls.committed_voters == two_voters);
}

#[test]
fn voter_set_wire_admission_preserves_boundary_identities_exactly() {
    let mut zero = wire_voter(0, 0);
    zero.k_raft_version_feature.max_supported_version = i16::MAX;
    let maximum = wire_voter(i32::MAX, 0xff);

    let voters = voter_set_from_wire(&wire_voter_record(vec![zero, maximum]))
        .expect("valid boundary voter set");
    let zero = voters.get(NodeId(0)).expect("zero voter");
    let maximum = voters.get(NodeId(i32::MAX as u64)).expect("maximum voter");

    check!(zero.directory_id == uuid::Uuid::nil());
    check!(zero.kraft_version.max == i16::MAX as u16);
    check!(zero.endpoints[0].name == "CONTROLLER");
    check!(maximum.directory_id.as_bytes() == &[0xff; 16]);
    check!(maximum.id == NodeId(i32::MAX as u64));
}

#[test]
fn voter_set_wire_admission_rejects_every_malformed_shape() {
    let base = wire_voter(1, 1);
    let mut cases = Vec::new();

    let mut unsupported = wire_voter_record(vec![base.clone()]);
    unsupported.version = 1;
    cases.push(("unsupported record version", unsupported));
    cases.push(("empty voter set", wire_voter_record(vec![])));
    let mut negative_id = base.clone();
    negative_id.voter_id = -1;
    cases.push(("negative voter id", wire_voter_record(vec![negative_id])));
    cases.push((
        "duplicate voter id",
        wire_voter_record(vec![base.clone(), wire_voter(1, 2)]),
    ));
    let mut no_endpoints = base.clone();
    no_endpoints.endpoints.clear();
    cases.push(("empty endpoint set", wire_voter_record(vec![no_endpoints])));
    let mut nameless = base.clone();
    nameless.endpoints[0].name.clear();
    cases.push(("nameless endpoint", wire_voter_record(vec![nameless])));
    let mut hostless = base.clone();
    hostless.endpoints[0].host.clear();
    cases.push(("hostless endpoint", wire_voter_record(vec![hostless])));
    let mut zero_port = base.clone();
    zero_port.endpoints[0].port = 0;
    cases.push(("zero endpoint port", wire_voter_record(vec![zero_port])));
    let mut duplicate_endpoint = base.clone();
    duplicate_endpoint
        .endpoints
        .push(duplicate_endpoint.endpoints[0].clone());
    cases.push((
        "duplicate endpoint name",
        wire_voter_record(vec![duplicate_endpoint]),
    ));
    let mut negative_min = base.clone();
    negative_min.k_raft_version_feature.min_supported_version = -1;
    cases.push((
        "negative minimum version",
        wire_voter_record(vec![negative_min]),
    ));
    let mut negative_max = base.clone();
    negative_max.k_raft_version_feature.max_supported_version = -1;
    cases.push((
        "negative maximum version",
        wire_voter_record(vec![negative_max]),
    ));
    let mut inverted = base;
    inverted.k_raft_version_feature.min_supported_version = 1;
    inverted.k_raft_version_feature.max_supported_version = 0;
    cases.push(("inverted version range", wire_voter_record(vec![inverted])));

    for (what, record) in cases {
        check!(
            matches!(
                voter_set_from_wire(&record),
                Err(RaftError::InvalidVoterUpdate(_))
            ),
            "{what}"
        );
    }
}

#[test]
fn failed_voter_record_does_not_replace_state_and_can_be_retried() {
    let initial = voter_set(&[NodeId(1)]);
    let mut controls = KraftControlState::new(initial.clone(), 1);
    let duplicate = wire_voter_record(vec![wire_voter(2, 2), wire_voter(2, 3)]);

    check!(
        controls
            .apply(5, &ControlRecord::Voters(duplicate))
            .is_err()
    );
    check!(controls.latest_voters() == &initial);
    check!(controls.voter_history.len() == 1);

    controls
        .apply(
            5,
            &ControlRecord::Voters(wire_voter_record(vec![wire_voter(2, 2)])),
        )
        .expect("corrected record retries at the same offset");
    check!(controls.latest_voters().contains(NodeId(2)));
    check!(!controls.latest_voters().contains(NodeId(1)));
}

#[test]
fn control_history_frontiers_handle_empty_exact_repeated_and_moving_states() {
    let initial = voter_set(&[NodeId(1)]);
    let mut controls = KraftControlState::new(initial.clone(), 0);
    controls.voter_history.clear();
    controls.version_history.clear();

    check!(controls.voters_at(Offset(10)) == initial);
    check!(controls.version_at(Offset(10)) == 0);

    controls
        .version_history
        .extend([(2, 0), (4, 1), (6, 1), (8, 0)]);
    check!(controls.version_at(Offset(2)) == 0);
    check!(controls.version_at(Offset(4)) == 0);
    check!(controls.version_at(Offset(5)) == 1);
    check!(controls.version_at(Offset(8)) == 1);
    check!(controls.version_at(Offset(9)) == 0);

    check!(!controls.commit_to(4));
    check!(controls.commit_to(5));
    check!(!controls.commit_to(7));
    controls.truncate_to(6);
    check!(controls.version_history.keys().copied().collect::<Vec<_>>() == vec![2, 4]);
    check!(controls.version_at(Offset(i64::MAX)) == 1);
    check!(!controls.commit_to(i64::MAX));
}

#[test]
fn execute_local_only_appends_leader_change_batch_to_log() {
    let (mut engine, _dir) = build_engine_only(EngineSetup::default());
    let start = engine.log.log_end_offset();

    engine.execute_local_only(vec![Action::AppendLeaderChange { epoch: 4 }]);

    assert2::assert!(engine.log.log_end_offset() == start + 1);
    let batches = engine
        .log
        .read_decoded(start, DEFAULT_METADATA_RAFT_FETCH_MAX)
        .expect("read appended leader-change");
    assert2::assert!(batches.len() == 1);
    let batch = &batches[0];
    check!(
        (
            batch.base_offset,
            batch.partition_leader_epoch,
            batch.attributes.is_control_batch(),
            batch.records.len(),
        ) == (start.0, 4, true, 1)
    );
}

#[test]
fn leader_change_batch_encodes_control_record_payload() {
    use krabka_protocol::{
        Decode,
        owned::leader_change_message::LeaderChangeMessage,
        records::metadata::control::{ControlRecordType, control_record_key},
    };

    let voters = voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
    let batch = leader_change_batch(7, NodeId(2), &voters);

    check!(
        (
            batch.partition_leader_epoch,
            batch.attributes.is_control_batch(),
            batch.last_offset_delta,
            batch.records.len(),
        ) == (7, true, 0, 1)
    );
    let record = &batch.records[0];
    check!(record.offset_delta == 0);
    check!(record.key.as_ref() == Some(&control_record_key(ControlRecordType::LeaderChange)));
    let value = record.value.as_ref().expect("leader change value");
    let mut cur: &[u8] = value;
    let decoded = LeaderChangeMessage::decode(&mut cur, 0).expect("decode leader change");
    check!(cur.is_empty());
    check!((decoded.version, decoded.leader_id) == (0, 2));
    let voters: Vec<i32> = decoded.voters.iter().map(|v| v.voter_id).collect();
    let granting_voters: Vec<i32> = decoded.granting_voters.iter().map(|v| v.voter_id).collect();
    assert2::assert!(voters == vec![1, 2, 3]);
    assert2::assert!(granting_voters == vec![1, 2, 3]);
}

/// A reconfiguration is refused before it is proposed when this node is
/// not the leader, or when the quorum cannot support the change.
///
/// Each refusal names a different cause, and the caller acts on which one:
/// `NotLeader` says where to go instead, while the rest say the request
/// itself will not do. Collapsing them loses the redirect.
#[test]
fn a_reconfiguration_is_refused_with_the_reason_it_was_refused_for() {
    use crate::reconfig::{AddVoter, ReconfigOutcome, VoterChange};

    fn add_of(id: u64) -> VoterChange {
        VoterChange::Add(AddVoter {
            voter: krabka_metadata::Voter {
                id: NodeId(id),
                directory_id: uuid::Uuid::nil(),
                endpoints: vec![],
                kraft_version: krabka_metadata::KRaftVersionRange::default(),
            },
            ack_when_committed: true,
        })
    }

    // A follower redirects rather than refusing outright: it knows the
    // request is legitimate, just not addressed to it.
    let (mut follower, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let (reply, mut rx) = oneshot::channel();
    follower.on_reconfigure(add_of(3), reply);
    check!(
        matches!(rx.try_recv(), Ok(Ok(ReconfigOutcome::NotLeader { .. }))),
        "a non-leader redirects"
    );

    // A leader whose quorum is still at kraft.version 0 has no mechanism to
    // add a voter with: dynamic membership is what version 1 introduces.
    let (mut leader, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut leader);
    check!(
        leader.controls.committed_version == 0,
        "a fresh quorum starts at version 0"
    );
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(2), reply);
    check!(
        matches!(
            rx.try_recv(),
            Ok(Err(RaftError::UnsupportedKraftVersion(0)))
        ),
        "adding a voter at version 0 is refused as unsupported"
    );
}

#[test]
fn update_voter_preflight_at_level_0_updates_voter_history() {
    use crate::reconfig::{ReconfigOutcome, UpdateVoter, VoterChange};

    let (mut leader, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut leader);

    let update = VoterChange::Update(UpdateVoter {
        voter: krabka_metadata::Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(99),
            endpoints: vec![],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        },
    });
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(update, reply);
    check!(matches!(rx.try_recv(), Ok(Ok(ReconfigOutcome::Committed))));
    check!(leader.controls.voter_history.contains_key(&-1));
    check!(
        leader
            .controls
            .voters_at(Offset(0))
            .get(NodeId(1))
            .unwrap()
            .directory_id
            == uuid::Uuid::from_u128(99)
    );
}

#[tokio::test]
async fn reconfiguration_refuses_when_epoch_not_committed_and_admits_when_committed() {
    use crate::reconfig::{ReconfigOutcome, RemoveVoter, UpdateVoter, VoterChange};

    fn add_of(id: u64) -> VoterChange {
        VoterChange::Add(add_request(id, 0))
    }

    let (mut leader, _dir) = build_engine_only(EngineSetup::default());
    leader.on_event(Event::ElectionTimeout);
    leader.on_event(Event::ReceiveVoteResponse {
        from: NodeId(2),
        epoch: 0,
        vote_granted: true,
    });
    leader.on_event(Event::ReceiveVoteResponse {
        from: NodeId(2),
        epoch: 1,
        vote_granted: true,
    });
    assert2::assert!(leader.core.role().is_leader());

    // Update committed version to 1 and ensure all existing voters support version 1
    leader.controls.version_history.insert(-1, 1);
    leader.controls.committed_version = 1;
    leader.core.set_kraft_version(1);
    let mut v = leader.controls.committed_voters.clone();
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let mut voter = v.get(id).unwrap().clone();
        voter.kraft_version = krabka_metadata::KRaftVersionRange { min: 0, max: 1 };
        v = v.with_voter(voter);
    }
    leader.controls.committed_voters = v.clone();
    leader.controls.voter_history.insert(-1, v);

    // Epoch not committed (hwm == 0, epoch_start_offset == 0)
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(4), reply);
    let r1 = rx.try_recv();
    check!(
        matches!(r1, Ok(Err(RaftError::ReconfigInProgress))),
        "reconfigure refused when leader has not committed in this epoch"
    );

    // Advance HWM so epoch is committed
    leader.log.advance_hwm(leader.log.log_end_offset() + 10);

    // Target not caught up
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(4), reply);
    let r2 = rx.try_recv();
    check!(
        matches!(r2, Ok(Err(RaftError::VoterNotCaughtUp { .. }))),
        "observer with fetch offset 0 is not caught up"
    );

    // Observer caught up: a valid fetch at the leader's log end, which is what
    // `LeaderState.isReplicaCaughtUp` reads.
    leader.clock_base = Instant::now() - Duration::from_millis(50);
    leader.record_observer_fetch(
        ReplicaKey {
            id: NodeId(4),
            directory_id: uuid::Uuid::nil(),
        },
        leader.log.log_end_offset().0,
    );
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(4), reply);
    leader.advance_and_apply(leader.log.log_end_offset());
    let res = rx.try_recv();
    check!(
        matches!(res, Ok(Ok(ReconfigOutcome::Committed))),
        "caught up observer is admitted"
    );

    // Single flight clear
    leader.pending_reconfig = Some(crate::kraft::controller::PendingReconfig {
        need_offset: Offset(100),
        reply: None,
        removed_local_leader: false,
    });
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(5), reply);
    check!(
        matches!(rx.try_recv(), Ok(Err(RaftError::ReconfigInProgress))),
        "reconfig in progress is refused"
    );
    leader.pending_reconfig = None;

    // Update with nil directory
    let update_matching = VoterChange::Update(UpdateVoter {
        voter: krabka_metadata::Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(99),
            endpoints: vec![krabka_metadata::voters::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9_093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
        },
    });
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(update_matching, reply);
    leader.advance_and_apply(leader.log.log_end_offset());
    let r4 = rx.try_recv();
    check!(
        matches!(r4, Ok(Ok(ReconfigOutcome::Committed))),
        "updating voter with nil directory is admitted"
    );

    // Remove local leader
    let remove_self = VoterChange::Remove(RemoveVoter {
        id: NodeId(1),
        directory_id: uuid::Uuid::from_u128(99),
    });
    let (reply, _rx) = oneshot::channel();
    leader.on_reconfigure(remove_self, reply);
    check!(
        leader
            .pending_reconfig
            .as_ref()
            .unwrap()
            .removed_local_leader,
        "removing self sets removed_local_leader"
    );
}

#[test]
fn apply_and_restore_control_records_updates_core_voters() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let two_voters = voter_set(&[NodeId(1), NodeId(2)]);
    let batch = crate::kraft::controller::records::typed_control_batch(
        1,
        &[ControlRecord::Voters(voter_set_to_wire(&two_voters))],
    )
    .unwrap();
    engine.apply_control_batch(&batch).unwrap();
    check!(engine.core.quorum_state().voters.contains(NodeId(2)));

    engine.restore_control_state_after_truncation(batch.base_offset);
    check!(!engine.core.quorum_state().voters.contains(NodeId(2)));

    let (reply, mut rx) = oneshot::channel();
    engine.pending_reconfig = Some(crate::kraft::controller::PendingReconfig {
        need_offset: Offset(10),
        reply: Some(reply),
        removed_local_leader: false,
    });
    engine.restore_control_state_after_truncation(11);
    check!(engine.pending_reconfig.is_some());
    check!(rx.try_recv().is_err());

    engine.restore_control_state_after_truncation(10);
    check!(engine.pending_reconfig.is_some());
    check!(rx.try_recv().is_err());

    engine.restore_control_state_after_truncation(9);
    check!(engine.pending_reconfig.is_none());
    check!(matches!(
        rx.try_recv(),
        Ok(Err(RaftError::NotLeader { .. }))
    ));

    engine.apply_control_batch(&batch).unwrap();
    engine.commit_control_state(Offset(batch.base_offset + 1));
    check!(engine.controls.committed_voters.contains(NodeId(2)));
}

/// A leader of voters 1, 2 and 3 at `kraft.version` 1 that has committed its
/// epoch, ready to answer `AddRaftVoter`, with a clock that has been running for
/// 50 ms so a fetch is stamped with a nonzero time.
fn kraft_version_one_leader() -> (Engine, tempfile::TempDir) {
    let (mut leader, dir) = build_engine_only(EngineSetup::default());
    leader.on_event(Event::ElectionTimeout);
    for epoch in [0, 1] {
        leader.on_event(Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch,
            vote_granted: true,
        });
    }
    assert2::assert!(leader.core.role().is_leader());
    leader.controls.version_history.insert(-1, 1);
    leader.controls.committed_version = 1;
    leader.core.set_kraft_version(1);
    let mut voters = leader.controls.committed_voters.clone();
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let mut voter = voters.get(id).unwrap().clone();
        voter.kraft_version = krabka_metadata::KRaftVersionRange { min: 0, max: 1 };
        voters = voters.with_voter(voter);
    }
    leader.controls.committed_voters = voters.clone();
    leader.controls.voter_history.insert(-1, voters);
    leader.log.advance_hwm(leader.log.log_end_offset() + 10);
    leader.clock_base = Instant::now() - Duration::from_millis(50);
    (leader, dir)
}

/// The `AddRaftVoter` request for node `id` under `directory`.
fn commit_add_voter(
    leader: &mut Engine,
    id: u64,
    directory: u128,
) -> Result<crate::reconfig::ReconfigOutcome, RaftError> {
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(
        crate::reconfig::VoterChange::Add(add_request(id, directory)),
        reply,
    );
    let appended = leader.log.log_end_offset();
    leader.advance_and_apply(appended);
    rx.try_recv().expect("the add was answered")
}

fn add_request(id: u64, directory: u128) -> crate::reconfig::AddVoter {
    crate::reconfig::AddVoter {
        voter: krabka_metadata::Voter {
            id: NodeId(id),
            directory_id: uuid::Uuid::from_u128(directory),
            endpoints: vec![krabka_metadata::voters::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9_093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
        },
        ack_when_committed: true,
    }
}

/// `LeaderState.isReplicaCaughtUp` reads the state of the exact `(id, directory
/// id)` key that fetched, and a replica counts as caught up once a fetch has
/// reached the leader's log end, or the log end the previous fetch saw. A
/// candidate that is only a record or two behind under continuous appends is
/// admitted.
#[tokio::test]
async fn add_voter_admits_a_candidate_by_kafkas_caught_up_rule() {
    use crate::reconfig::{ReconfigOutcome, VoterChange};

    /// One fetch by the candidate: its directory, the offset it asks for
    /// (`None` is the leader's log end at that moment, `Some(-1)` the log end
    /// before this row's append) and whether the leader appends a record first.
    type Fetch = (u128, Option<i64>, bool);
    let candidate_directory = 7;
    let cases: [(&str, Vec<Fetch>, bool); 5] = [
        ("it never fetched", vec![], false),
        (
            "it fetched short of the end and never reached it",
            vec![(candidate_directory, Some(0), false)],
            false,
        ),
        (
            "it fetched at the log end",
            vec![(candidate_directory, None, false)],
            true,
        ),
        (
            "it kept pace with the appends between two fetches",
            vec![
                (candidate_directory, Some(0), false),
                (candidate_directory, Some(-1), true),
            ],
            true,
        ),
        (
            "the fetches were made under another directory id",
            vec![(candidate_directory + 1, None, false)],
            false,
        ),
    ];
    for (label, fetches, admitted) in cases {
        let (mut leader, _dir) = kraft_version_one_leader();
        for (directory, offset, append_first) in fetches {
            let end_before_append = leader.log.log_end_offset().0;
            if append_first {
                leader.test_append_and_commit(&topic_record("t"));
            }
            leader.clock_base -= Duration::from_millis(10);
            let offset = match offset {
                Some(-1) => end_before_append,
                Some(offset) => offset,
                None => leader.log.log_end_offset().0,
            };
            leader.record_observer_fetch(
                ReplicaKey {
                    id: NodeId(4),
                    directory_id: uuid::Uuid::from_u128(directory),
                },
                offset,
            );
        }

        let (reply, mut rx) = oneshot::channel();
        leader.on_reconfigure(VoterChange::Add(add_request(4, candidate_directory)), reply);
        let end = leader.log.log_end_offset();
        leader.advance_and_apply(end);
        let outcome = rx.try_recv().expect("the add was answered");
        if admitted {
            check!(matches!(outcome, Ok(ReconfigOutcome::Committed)), "{label}");
        } else {
            check!(
                matches!(outcome, Err(RaftError::VoterNotCaughtUp { .. })),
                "{label}"
            );
        }
    }
}

/// Kafka drops an observer that has been silent for five minutes in
/// `observerStates()`, which only `DescribeQuorum` calls, while
/// `isReplicaCaughtUp` reads the same map with a window of an hour. A fetch by
/// another observer must not forget a candidate that fetched to the log end six
/// minutes ago: `DescribeQuorum` no longer lists it, and `AddRaftVoter` still
/// admits it.
#[tokio::test]
async fn an_observer_that_dropped_out_of_describe_quorum_can_still_be_added() {
    use crate::reconfig::{ReconfigOutcome, VoterChange};

    let (mut leader, _dir) = kraft_version_one_leader();
    let candidate = ReplicaKey {
        id: NodeId(4),
        directory_id: uuid::Uuid::from_u128(7),
    };
    let bystander = ReplicaKey {
        id: NodeId(9),
        directory_id: uuid::Uuid::from_u128(9),
    };
    let end = leader.log.log_end_offset().0;
    leader.clock_base -= Duration::from_millis(10);
    leader.record_observer_fetch(candidate, end);
    leader.clock_base -= Duration::from_secs(360);
    leader.record_observer_fetch(bystander, end);

    let listed: Vec<NodeId> = leader
        .quorum_state_snapshot()
        .observers
        .iter()
        .map(|observer| observer.id)
        .collect();
    check!(listed == vec![NodeId(9)], "the five minutes hide it");
    let outcome = commit_add_voter(&mut leader, 4, 7);
    check!(
        matches!(outcome, Ok(ReconfigOutcome::Committed)),
        "the hour keeps it caught up"
    );
}

/// `AddVoterHandler` answers from the leader's own state before it contacts the
/// candidate: a candidate the leader has never seen fetch still gets the
/// pending-change, duplicate-id and `kraft.version` answers, and a request that
/// passes them is admitted without appending anything.
#[tokio::test]
async fn a_check_only_add_runs_the_local_admission_checks_in_kafkas_order() {
    use crate::reconfig::{ReconfigOutcome, VoterChange};

    let check_add = |leader: &mut Engine, request: crate::reconfig::AddVoter| {
        let (reply, mut rx) = oneshot::channel();
        leader.on_reconfigure(VoterChange::CheckAdd(request), reply);
        rx.try_recv().expect("the check was answered")
    };

    // No pending change, HWM past the epoch start, kraft.version 1: a stranger
    // passes, and nothing is appended or left pending.
    let (mut leader, _dir) = kraft_version_one_leader();
    let end = leader.log.log_end_offset();
    check!(
        matches!(
            check_add(&mut leader, add_request(4, 7)),
            Ok(ReconfigOutcome::Committed)
        ),
        "a stranger passes the local checks"
    );
    check!(leader.log.log_end_offset() == end);
    check!(leader.pending_reconfig.is_none());
    check!(!leader.core.quorum_state().voters.contains(NodeId(4)));

    // The id is already a voter, under this directory or another: DUPLICATE_VOTER
    // before any probe or catch-up check.
    for directory in [0, 7] {
        check!(
            matches!(
                check_add(&mut leader, add_request(2, directory)),
                Err(RaftError::DuplicateVoter(NodeId(2)))
            ),
            "voter 2 as directory {directory}"
        );
    }

    // A pending change is REQUEST_TIMED_OUT, whichever candidate asks.
    leader.pending_reconfig = Some(crate::kraft::controller::PendingReconfig {
        need_offset: Offset(100),
        reply: None,
        removed_local_leader: false,
    });
    for id in [2, 4] {
        check!(
            matches!(
                check_add(&mut leader, add_request(id, 7)),
                Err(RaftError::ReconfigInProgress)
            ),
            "node {id} during a pending change"
        );
    }
    leader.pending_reconfig = None;

    // Below kraft.version 1 there is nothing to add to.
    let (mut leader, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut leader);
    check!(
        matches!(
            check_add(&mut leader, add_request(4, 7)),
            Err(RaftError::UnsupportedKraftVersion(0))
        ),
        "kraft.version 0"
    );
}

/// `LeaderState.updateVoterAndObserverStates` keeps a replica's `ReplicaState`
/// as it crosses between the observer and voter maps: a candidate that becomes a
/// voter keeps its fetch and caught-up times, and a removed voter is listed as
/// an observer with the times it had as a voter.
#[tokio::test]
async fn a_replica_keeps_its_progress_across_joining_and_leaving_the_voter_set() {
    use crate::reconfig::{ReconfigOutcome, RemoveVoter, VoterChange};

    let (mut leader, _dir) = kraft_version_one_leader();
    let key = ReplicaKey {
        id: NodeId(4),
        directory_id: uuid::Uuid::from_u128(7),
    };
    let end = leader.log.log_end_offset().0;
    leader.record_observer_fetch(key, end);
    let as_observer = leader.observers[&key].clone();
    assert2::assert!(as_observer.last_caught_up.0 > 0);

    let outcome = commit_add_voter(&mut leader, 4, 7);
    check!(matches!(outcome, Ok(ReconfigOutcome::Committed)));
    check!(leader.observers.is_empty());
    let Role::Leader { replicas, .. } = leader.core.role() else {
        panic!("still the leader");
    };
    check!(replicas[&NodeId(4)] == as_observer);

    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(
        VoterChange::Remove(RemoveVoter {
            id: NodeId(4),
            directory_id: uuid::Uuid::from_u128(7),
        }),
        reply,
    );
    let appended = leader.log.log_end_offset();
    leader.advance_and_apply(appended);
    check!(matches!(rx.try_recv(), Ok(Ok(ReconfigOutcome::Committed))));
    check!(leader.observers[&key] == as_observer);
    check!(!leader.core.quorum_state().voters.contains(NodeId(4)));
}

/// The `validate_only` half of a `kraft.version` upgrade runs every check the
/// real upgrade runs and writes nothing, as Kafka's
/// `LeaderState.maybeAppendUpgradedKRaftVersion` skips only the append.
///
/// Each case runs both requests on identically built engines: a refusal must
/// be the same one for both, and an upgrade the checks admit must leave the
/// log as it was when validated and grow it when finalized.
#[test]
fn validating_a_kraft_version_upgrade_runs_its_checks_and_appends_nothing() {
    use crate::reconfig::{ReconfigOutcome, VoterChange};

    /// What the request left behind: the reply if it was immediate, and
    /// whether the log grew.
    fn run(elect: bool, change: VoterChange) -> (Option<Result<ReconfigOutcome, RaftError>>, bool) {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            ids: &[NodeId(1)],
            ..Default::default()
        });
        if elect {
            elect_single_voter_engine(&mut engine);
        }
        let before = engine.log.log_end_offset();
        let (reply, mut rx) = oneshot::channel();
        engine.on_reconfigure(change, reply);
        (rx.try_recv().ok(), engine.log.log_end_offset() != before)
    }

    // A follower redirects, whichever request it gets.
    let (validated, grew) = run(false, VoterChange::ValidateKraftVersion(1));
    let (finalized, _) = run(false, VoterChange::FinalizeKraftVersion(1));
    check!(matches!(
        (&validated, &finalized),
        (
            Some(Ok(ReconfigOutcome::NotLeader { .. })),
            Some(Ok(ReconfigOutcome::NotLeader { .. }))
        )
    ));
    check!(!grew);

    // A version nobody supports is refused the same way by both.
    let (validated, grew) = run(true, VoterChange::ValidateKraftVersion(9));
    let (finalized, _) = run(true, VoterChange::FinalizeKraftVersion(9));
    check!(matches!(validated, Some(Err(_))), "{validated:?}");
    check!(
        format!("{validated:?}") == format!("{finalized:?}"),
        "{validated:?} against {finalized:?}"
    );
    check!(!grew);

    // An upgrade the checks admit: validating answers at once and leaves the
    // log alone, while finalizing appends the version and the voter set.
    let (validated, grew) = run(true, VoterChange::ValidateKraftVersion(1));
    check!(matches!(validated, Some(Ok(ReconfigOutcome::Committed))));
    check!(!grew, "a validated upgrade must append nothing");
    let (_, grew) = run(true, VoterChange::FinalizeKraftVersion(1));
    check!(grew, "the real upgrade appends");
}

#[tokio::test]
async fn version_finalization_waits_for_the_unchanged_voters_record() {
    use crate::reconfig::{ReconfigOutcome, VoterChange};

    let (mut engine, _dir) = build_engine_only(EngineSetup::default());
    engine.on_event(Event::ElectionTimeout);
    for epoch in [0, 1] {
        engine.on_event(Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch,
            vote_granted: true,
        });
    }
    assert!(engine.core.role().is_leader());
    engine.advance_and_apply(engine.log.log_end_offset());
    let base = engine.log.log_end_offset();
    let before = engine.controls.committed_voters.clone();
    let (reply, mut rx) = oneshot::channel();
    engine.on_reconfigure(VoterChange::FinalizeKraftVersion(1), reply);
    assert!(engine.log.log_end_offset() == Offset(base.0 + 2));
    assert!(engine.pending_reconfig.as_ref().unwrap().need_offset == Offset(base.0 + 2));

    engine.advance_and_apply(Offset(base.0 + 1));
    assert!(engine.controls.committed_version == 1);
    assert!(engine.controls.committed_voters == before);
    assert!(engine.controls.latest_voters() == &engine.controls.committed_voters);
    assert!(engine.controls.voter_history.last_key_value().unwrap().0 == &(base.0 + 1));
    assert!(engine.pending_reconfig.is_some());
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    engine.advance_and_apply(Offset(base.0 + 2));
    assert!(engine.pending_reconfig.is_none());
    assert!(matches!(rx.try_recv(), Ok(Ok(ReconfigOutcome::Committed))));
}

/// The first leader of a dynamic quorum writes the voters of the bootstrap
/// checkpoint into the log, after its `LeaderChange` marker, as Kafka's
/// `LeaderState.appendStartOfEpochControlRecords` does. A replica that never
/// read the checkpoint learns the voters from that batch. A static quorum's
/// leader, and a leader whose epoch starts past offset 0, write the marker
/// alone.
#[test]
fn the_first_leader_of_a_dynamic_quorum_writes_the_bootstrap_voters() {
    use krabka_metadata::{KRaftVersionRecord, MetadataRecord, VotersRecord};

    use crate::{
        config::DEFAULT_METADATA_RAFT_FETCH_MAX, kraft::controller::control_batch_image_records,
    };

    /// The control batch at `at`: its record count, whether its `LeaderChange`
    /// marker reads whole at version 0, the only version Kafka reads it at, and
    /// the image records it carries.
    fn batch_at(engine: &Engine, at: Offset) -> (usize, bool, Vec<MetadataRecord>) {
        use krabka_protocol::{Decode, owned::leader_change_message::LeaderChangeMessage};

        let batches = engine
            .log
            .read_decoded(at, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .expect("read the leader's batch");
        let batch = batches.first().expect("a batch at the epoch start");
        let mut marker: &[u8] = batch.records[0].value.as_ref().expect("a marker value");
        let kafka_readable =
            LeaderChangeMessage::decode(&mut marker, 0).is_ok() && marker.is_empty();
        (
            batch.records.len(),
            kafka_readable,
            control_batch_image_records(batch).expect("decode the control batch"),
        )
    }

    let voters = voter_set(&[NodeId(1)]);
    let dynamic = vec![
        MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
        MetadataRecord::V1Voters(VotersRecord {
            voters: voters.clone(),
        }),
    ];
    // (kraft.version, first epoch's batch, a later epoch's batch)
    let cases = [
        (0_u16, (1, true, vec![]), (1, true, vec![])),
        (1, (3, true, dynamic), (1, true, vec![])),
    ];
    let mut written = Vec::new();
    for &(kraft_version, ..) in &cases {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            ids: &[NodeId(1)],
            ..Default::default()
        });
        engine.controls.version_history.insert(-1, kraft_version);
        engine.controls.committed_version = kraft_version;
        engine.core.set_kraft_version(kraft_version);
        elect_single_voter_engine(&mut engine);
        let first = batch_at(&engine, Offset(0));
        let later_start = engine.log.log_end_offset();
        engine
            .append_leader_change(engine.core.quorum_state().leader_epoch)
            .expect("append a later epoch's marker");
        written.push((kraft_version, first, batch_at(&engine, later_start)));
        // The leader's own voter set is the one it wrote, so it is unchanged.
        check!(engine.controls.latest_voters() == &voters);
    }
    assert!(written == cases);
}

/// A leader that has made no commit since its election still describes the
/// observers that fetch from it: a new observer publishes the quorum snapshot
/// that `DescribeQuorum` reads.
#[test]
fn a_new_observer_is_published_without_a_commit() {
    let (mut leader, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut leader);
    leader.clock_base = Instant::now() - Duration::from_millis(50);
    let published = leader.quorum_tx.subscribe();
    let key = ReplicaKey {
        id: NodeId(7),
        directory_id: uuid::Uuid::from_u128(7),
    };

    leader.record_observer_fetch(key, leader.log.log_end_offset().0);

    let observers: Vec<(NodeId, uuid::Uuid)> = published
        .borrow()
        .observers
        .iter()
        .map(|observer| (observer.id, observer.directory_id))
        .collect();
    assert!(observers == vec![(key.id, key.directory_id)]);
}
