//! Tests for restart recovery: replay of the committed metadata and control
//! prefixes, recovery of an image from a checkpoint plus log, and the
//! round-trip of the durable quorum-state file at both schema versions.

use std::time::Duration as StdDuration;

use assert2::{assert, check};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

use super::*;
use crate::{
    error::PersistedFormatError,
    kraft::controller::{
        control_state::voter_set_to_wire,
        quorum_state_file::{load_quorum_state, save_quorum_state},
        records::typed_control_batch,
        recovery::{control_state_at, replay_committed, replay_control_records},
        test_support::{
            await_leader, open_test_controller, submit_change_with_timeout, test_kraft_config,
            topic_record, voter_set,
        },
    },
};

fn recovery_voters() -> (VoterSet, VoterSet, VoterSet) {
    (
        voter_set(&[NodeId(1)]),
        voter_set(&[NodeId(1), NodeId(2)]),
        voter_set(&[NodeId(1), NodeId(3)]),
    )
}

fn reopened_control_log(write: impl FnOnce(&mut KraftLog)) -> (tempfile::TempDir, KraftLog) {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mut log =
            KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("open log");
        write(&mut log);
    }
    let log = KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("reopen log");
    (dir, log)
}

fn replayed_control_state(log: &KraftLog, initial: VoterSet) -> QuorumState {
    let mut state = QuorumState::bootstrap(uuid::Uuid::nil(), initial);
    replay_control_records(log, &mut state, MetadataRaftFetchMax::default()).expect("replay");
    state
}

#[test]
fn restart_replays_control_records_only_through_persisted_high_watermark() {
    let (initial, committed, uncommitted) = recovery_voters();
    let (_dir, log) = reopened_control_log(|log| {
        let mut batch =
            typed_control_batch(1, &[ControlRecord::Voters(voter_set_to_wire(&committed))])
                .expect("committed voter batch");
        log.append(&mut batch, 0)
            .expect("append committed voter batch");
        log.advance_hwm(log.log_end_offset());
        let mut batch =
            typed_control_batch(1, &[ControlRecord::Voters(voter_set_to_wire(&uncommitted))])
                .expect("uncommitted voter batch");
        log.append(&mut batch, 0)
            .expect("append uncommitted voter batch");
    });
    assert2::assert!(log.hwm() < log.log_end_offset());
    let state = replayed_control_state(&log, initial);
    assert2::assert!(state.voters == committed);
}

#[test]
fn control_replay_stops_inside_a_partially_committed_batch() {
    let (initial, committed, uncommitted) = recovery_voters();
    let (_dir, log) = reopened_control_log(|log| {
        let mut batch = typed_control_batch(
            1,
            &[
                ControlRecord::Voters(voter_set_to_wire(&committed)),
                ControlRecord::Voters(voter_set_to_wire(&uncommitted)),
            ],
        )
        .expect("mixed-commit voter batch");
        log.append(&mut batch, 0).expect("append voter batch");
        log.advance_hwm(Offset(1));
    });
    let state = replayed_control_state(&log, initial);

    assert2::assert!(state.voters == committed);
}

#[test]
fn replay_committed_rebuilds_image_from_log_records() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record("replayed"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    replay_committed(
        &engine.log,
        &mut recovered,
        Offset(0),
        MetadataRaftFetchMax::default(),
    )
    .expect("replay");

    assert2::assert!(recovered.topic("replayed").is_some());
}

#[test]
fn replay_committed_captures_metadata_downgrade_snapshot() {
    use krabka_metadata::FeatureLevelRecord;

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(
        &[MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: 25,
        })],
        reply,
    );
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let (reply, mut rx) = oneshot::channel();
    engine.downgrade_snapshot_failures_remaining = 1;
    engine.on_submit_change(
        &[MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: 16,
        })],
        reply,
    );
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    let pending = replay_committed(
        &engine.log,
        &mut recovered,
        Offset(0),
        MetadataRaftFetchMax::default(),
    )
    .expect("replay")
    .expect("captured downgrade snapshot");

    assert2::assert!(pending.end_offset == engine.log.log_end_offset());
    assert2::assert!(recovered.finalized_metadata_version() == Some(16));
}

#[test]
fn downgrade_snapshot_failures_remaining_decrements_and_eventually_succeeds() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let (reply, _rx) = oneshot::channel();
    engine.on_submit_change(
        &[MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: 25,
            },
        )],
        reply,
    );
    let (reply, _rx) = oneshot::channel();
    engine.downgrade_snapshot_failures_remaining = 3;
    engine.on_submit_change(
        &[MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: 16,
            },
        )],
        reply,
    );
    assert2::assert!(engine.write_downgrade_snapshot_and_prune().is_err());
    assert2::assert!(engine.downgrade_snapshot_failures_remaining == 1);
    assert2::assert!(engine.write_downgrade_snapshot_and_prune().is_err());
    assert2::assert!(engine.downgrade_snapshot_failures_remaining == 0);
    assert2::assert!(engine.write_downgrade_snapshot_and_prune().is_ok());
}

#[test]
fn control_state_at_replays_version_and_voters_at_boundary_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let initial = voter_set(&[NodeId(1)]);
    let committed = voter_set(&[NodeId(1), NodeId(2)]);
    let bootstrap = QuorumState::bootstrap(uuid::Uuid::nil(), initial.clone());
    let max = MetadataRaftFetchMax::default();

    {
        let mut log =
            KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("open log");
        let mut batch0 = typed_control_batch(
            1,
            &[ControlRecord::KRaftVersion(
                krabka_protocol::owned::k_raft_version_record::KRaftVersionRecord {
                    version: 0,
                    k_raft_version: 1,
                    ..Default::default()
                },
            )],
        )
        .expect("batch 0");
        log.append(&mut batch0, 0).expect("append 0");
        log.advance_hwm(log.log_end_offset());

        let mut batch1 =
            typed_control_batch(1, &[ControlRecord::Voters(voter_set_to_wire(&committed))])
                .expect("batch 1");
        log.append(&mut batch1, 0).expect("append 1");
        log.advance_hwm(log.log_end_offset());

        let mut batch2 = typed_control_batch(
            1,
            &[ControlRecord::KRaftVersion(
                krabka_protocol::owned::k_raft_version_record::KRaftVersionRecord {
                    version: 0,
                    k_raft_version: -1,
                    ..Default::default()
                },
            )],
        )
        .expect("batch 2");
        log.append(&mut batch2, 0).expect("append 2");
        log.advance_hwm(log.log_end_offset());
    }

    let log = KraftLog::open(dir.path(), &crate::MetadataLogConfig::default()).expect("reopen log");

    // end_offset 0: no records applied
    let s0 = control_state_at(&log, &bootstrap, Offset(0), max).expect("offset 0");
    check!(s0.kraft_version == 0);
    check!(s0.voters == initial);

    // end_offset 1: only KRaftVersion applied
    let s1 = control_state_at(&log, &bootstrap, Offset(1), max).expect("offset 1");
    check!(s1.kraft_version == 1);
    check!(s1.voters == initial);

    // end_offset 2: KRaftVersion and Voters applied
    let s2 = control_state_at(&log, &bootstrap, Offset(2), max).expect("offset 2");
    check!(s2.kraft_version == 1);
    check!(s2.voters == committed);

    // end_offset 3: negative kraft version errors
    let s3 = control_state_at(&log, &bootstrap, Offset(3), max);
    let Err(RaftError::MetadataReplay(fault)) = s3 else {
        panic!("a negative kraft.version is a replay fault: {s3:?}");
    };
    check!(
        fault
            == crate::error::MetadataReplayError::InvalidControlRecord {
                offset: 2,
                reason: "negative kraft.version -1".into(),
            }
    );
}

#[tokio::test]
async fn snapshot_then_restart_recovers_image() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let cluster_id = uuid::Uuid::from_u128(7);
    let voters = voter_set(&[NodeId(1)]);

    {
        let log =
            KraftLog::open(&data_dir, &crate::MetadataLogConfig::default()).expect("open log");
        let ctrl = KraftController::spawn(
            test_kraft_config(
                NodeId(1),
                cluster_id,
                QuorumState::bootstrap(cluster_id, voters.clone()),
            ),
            log,
            data_dir.clone(),
        );
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;
        submit_change_with_timeout(&ctrl, topic_record("recovered"), "recovery seed")
            .await
            .unwrap();
        assert2::assert!(ctrl.current_image().topic("recovered").is_some());
        ctrl.trigger_snapshot().await.unwrap();
        ctrl.shutdown().await;
        // Give the loop a moment to fully drain.
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    // Reopen over the same dir: the image is rebuilt from checkpoint+log.
    let ctrl2 = open_test_controller(data_dir.clone(), cluster_id, voters).expect("reopen");
    assert2::assert!(ctrl2.current_image().topic("recovered").is_some());
    ctrl2.shutdown().await;
}

#[tokio::test]
async fn quorum_state_file_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cid = uuid::Uuid::from_u128(9);
    let mut state = QuorumState::bootstrap(cid, voter_set(&[NodeId(1), NodeId(2), NodeId(3)]));
    state.leader_epoch = 5;
    state.leader_id = Some(NodeId(2));
    state.voted_key = Some(ReplicaKey {
        id: NodeId(3),
        directory_id: uuid::Uuid::from_u128(3),
    });
    save_quorum_state(dir.path(), &state).unwrap();

    // The JVM tools read this file, so the schema-v0 field set and its
    // Kafka field order are part of the contract, not an implementation
    // detail of whatever serializer writes it.
    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    check!(
        json == format!(
            "{{\"clusterId\":\"{cid}\",\"leaderId\":2,\"leaderEpoch\":5,\"votedId\":3,\
             \"appliedOffset\":0,\"currentVoters\":[{{\"voterId\":1}},{{\"voterId\":2}},\
             {{\"voterId\":3}}],\"data_version\":0}}"
        )
    );

    let loaded = load_quorum_state(
        dir.path(),
        cid,
        &voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
    )
    .unwrap()
    .expect("present");
    // Leadership is volatile (Raft persists only currentTerm + votedFor):
    // `leader_id` is deliberately cleared on load so a restarted ex-leader
    // re-discovers the current leader instead of trusting stale state.
    check!(
        (
            loaded.leader_epoch,
            loaded.leader_id,
            loaded.voted_key.map(|k| k.id),
            loaded.cluster_id,
        ) == (5, None, Some(NodeId(3)), cid)
    );
    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    assert2::assert!(json.contains("\"currentVoters\""));
    assert2::assert!(json.contains("\"data_version\":0"));
    assert2::assert!(!json.contains("votedDirectoryId"));
}

fn level_one_state() -> (krabka_metadata::voters::VoterSet, QuorumState) {
    let voters = voter_set(&[NodeId(1), NodeId(2)]);
    let mut state = QuorumState::bootstrap(uuid::Uuid::from_u128(9), voters.clone());
    state.kraft_version = 1;
    (voters, state)
}

#[test]
fn quorum_state_level_one_uses_voted_directory_and_omits_static_voters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (voters, mut state) = level_one_state();
    state.voted_key = Some(ReplicaKey {
        id: NodeId(2),
        directory_id: uuid::Uuid::from_u128(0x0102_0304),
    });
    save_quorum_state(dir.path(), &state).unwrap();

    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    assert2::assert!(json.contains("\"data_version\":1"));
    assert2::assert!(json.contains("\"votedDirectoryId\""));
    assert2::assert!(!json.contains("currentVoters"));
    let loaded = load_quorum_state(dir.path(), state.cluster_id, &voters)
        .unwrap()
        .unwrap();
    assert2::assert!(loaded.kraft_version == 1);
    assert2::assert!(loaded.voted_key == state.voted_key);
}

#[test]
fn quorum_state_level_one_round_trips_the_no_vote_sentinel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let voters = voter_set(&[NodeId(1)]);
    let mut state = QuorumState::bootstrap(uuid::Uuid::from_u128(9), voters.clone());
    state.kraft_version = 1;
    state.leader_epoch = i32::MAX as u32;

    save_quorum_state(dir.path(), &state).expect("save no-vote state");
    let loaded = load_quorum_state(dir.path(), state.cluster_id, &voters)
        .expect("load no-vote state")
        .expect("present");

    check!(
        (loaded.leader_epoch, loaded.voted_key, loaded.leader_id) == (i32::MAX as u32, None, None)
    );
}

#[test]
fn load_quorum_state_refuses_malformed_or_version_mismatched_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(QUORUM_STATE_FILE);
    let cluster_id = uuid::Uuid::from_u128(9);
    let voters = voter_set(&[NodeId(1), NodeId(2)]);
    let nil_directory = URL_SAFE_NO_PAD.encode([0_u8; 16]);
    let non_nil_directory = URL_SAFE_NO_PAD.encode([1_u8; 16]);
    let v0 = |epoch: i32, vote: i32, cluster: uuid::Uuid, voter_rows: &str, extra: &str| {
        format!(
            "{{\"clusterId\":\"{cluster}\",\"leaderId\":7,\"leaderEpoch\":{epoch},\
             \"votedId\":{vote},\"appliedOffset\":0,\"currentVoters\":{voter_rows},\
             \"data_version\":0{extra}}}"
        )
    };
    let v1 = |epoch: i32, vote: i32, directory: &str, extra_fields: &str| {
        format!(
            "{{\"leaderId\":7,\"leaderEpoch\":{epoch},\"votedId\":{vote},\
             \"votedDirectoryId\":\"{directory}\"{extra_fields},\"data_version\":1}}"
        )
    };
    let cases = [
        v0(-1, -1, cluster_id, "[{\"voterId\":1},{\"voterId\":2}]", ""),
        v0(0, -2, cluster_id, "[{\"voterId\":1},{\"voterId\":2}]", ""),
        v0(
            0,
            -1,
            uuid::Uuid::from_u128(10),
            "[{\"voterId\":1},{\"voterId\":2}]",
            "",
        ),
        v0(0, -1, cluster_id, "[{\"voterId\":2},{\"voterId\":1}]", ""),
        v0(
            0,
            -1,
            cluster_id,
            "[{\"voterId\":1,\"extra\":0},{\"voterId\":2}]",
            "",
        ),
        v0(
            0,
            -1,
            cluster_id,
            "[{\"voterId\":1},{\"voterId\":2}]",
            ",\"extra\":0",
        ),
        v1(0, 1, "not-base64", ""),
        v1(0, -1, &non_nil_directory, ""),
        v1(0, 1, &nil_directory, ",\"clusterId\":\"unexpected\""),
    ];

    for json in cases {
        std::fs::write(&path, &json).expect("write malformed state");
        check!(
            matches!(
                load_quorum_state(dir.path(), cluster_id, &voters),
                Err(RaftError::PersistedFormat {
                    artifact: QUORUM_STATE_FILE,
                    problem: PersistedFormatError::Malformed(_),
                    ..
                })
            ),
            "{json}"
        );
    }
}

#[test]
fn save_quorum_state_rejects_overflow_without_clamping() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster_id = uuid::Uuid::from_u128(9);
    let overflow = u64::from(i32::MAX as u32) + 1;

    let mut states = Vec::new();
    let mut epoch = QuorumState::bootstrap(cluster_id, voter_set(&[NodeId(1)]));
    epoch.leader_epoch = i32::MAX as u32 + 1;
    states.push(epoch);
    let mut leader = QuorumState::bootstrap(cluster_id, voter_set(&[NodeId(1)]));
    leader.leader_id = Some(NodeId(overflow));
    states.push(leader);
    let mut vote = QuorumState::bootstrap(cluster_id, voter_set(&[NodeId(1)]));
    vote.voted_key = Some(ReplicaKey {
        id: NodeId(overflow),
        directory_id: uuid::Uuid::nil(),
    });
    states.push(vote);
    states.push(QuorumState::bootstrap(
        cluster_id,
        voter_set(&[NodeId(overflow)]),
    ));
    let mut version = QuorumState::bootstrap(cluster_id, voter_set(&[NodeId(1)]));
    version.kraft_version = 2;
    states.push(version);

    for state in states {
        check!(matches!(
            save_quorum_state(dir.path(), &state),
            Err(RaftError::Storage(krabka_log::LogError::InvalidArgument(_)))
        ));
        check!(!dir.path().join(QUORUM_STATE_FILE).exists());
    }
}

#[test]
fn save_quorum_state_can_retry_after_an_atomic_rename_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(QUORUM_STATE_FILE);
    std::fs::create_dir(&path).expect("block final rename with a directory");
    let voters = voter_set(&[NodeId(1)]);
    let mut state = QuorumState::bootstrap(uuid::Uuid::from_u128(9), voters.clone());
    state.leader_epoch = 17;
    state.voted_key = Some(ReplicaKey {
        id: NodeId(1),
        directory_id: uuid::Uuid::nil(),
    });

    check!(matches!(
        save_quorum_state(dir.path(), &state),
        Err(RaftError::Storage(krabka_log::LogError::Io(_)))
    ));
    std::fs::remove_dir(&path).expect("remove rename blocker");
    save_quorum_state(dir.path(), &state).expect("retry save");
    let loaded = load_quorum_state(dir.path(), state.cluster_id, &voters)
        .expect("load retried state")
        .expect("present");
    check!((loaded.leader_epoch, loaded.voted_key.map(|key| key.id)) == (17, Some(NodeId(1))));
}

#[test]
fn load_quorum_state_reports_unreadable_non_missing_file_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join(QUORUM_STATE_FILE)).expect("mkdir quorum-state path");

    let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]));

    assert2::assert!(matches!(loaded, Err(RaftError::Storage(_))));
}

/// The exact bytes of a `data_version` 1 quorum-state file: epoch 5, a vote
/// for node 2 in directory `00000000-0000-0000-0000-000000000003`, no leader.
/// Kafka's `QuorumStateDataJsonConverter` writes these fields in this order,
/// and a change to them is a change to the 1.x on-disk contract.
const QUORUM_STATE_V1_FIXTURE: &str = "{\"leaderId\":-1,\"leaderEpoch\":5,\"votedId\":2,\
     \"votedDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAw\",\"data_version\":1}";

#[test]
fn quorum_state_v1_matches_the_fixture_and_decodes_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (voters, mut state) = level_one_state();
    state.leader_epoch = 5;
    state.voted_key = Some(ReplicaKey {
        id: NodeId(2),
        directory_id: uuid::Uuid::from_u128(3),
    });

    save_quorum_state(dir.path(), &state).expect("save");
    let written = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).expect("read");
    check!(written == QUORUM_STATE_V1_FIXTURE);

    let loaded = load_quorum_state(dir.path(), state.cluster_id, &voters)
        .expect("load the fixture")
        .expect("present");
    check!(loaded == state);
}

/// A `data_version` outside 0 and 1, or none at all, stops the load, as
/// Kafka's `FileQuorumStateStore.readStateFromFile` throws on both.
#[test]
fn load_quorum_state_refuses_an_unknown_or_missing_data_version() {
    let unsupported = |found| PersistedFormatError::UnsupportedVersion {
        found,
        min: 0,
        max: 1,
    };
    let without_version = "{\"leaderId\":-1,\"leaderEpoch\":5,\"votedId\":2,\
         \"votedDirectoryId\":\"AAAAAAAAAAAAAAAAAAAAAw\"}";
    let cases = [
        (
            "future version",
            QUORUM_STATE_V1_FIXTURE.replace("\"data_version\":1", "\"data_version\":2"),
            unsupported(2),
        ),
        (
            "negative version",
            QUORUM_STATE_V1_FIXTURE.replace("\"data_version\":1", "\"data_version\":-1"),
            unsupported(-1),
        ),
        (
            "no data_version",
            without_version.to_owned(),
            PersistedFormatError::Malformed("it has no data_version field".into()),
        ),
        (
            "data_version not an integer",
            QUORUM_STATE_V1_FIXTURE.replace("\"data_version\":1", "\"data_version\":\"1\""),
            PersistedFormatError::Malformed("its data_version is not an integer".into()),
        ),
        (
            "not an object",
            "[1]".to_owned(),
            PersistedFormatError::Malformed("[1] is not a JSON object".into()),
        ),
    ];
    for (case, json, want) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(QUORUM_STATE_FILE);
        std::fs::write(&path, json).expect("write quorum-state");

        let loaded = load_quorum_state(
            dir.path(),
            uuid::Uuid::from_u128(9),
            &voter_set(&[NodeId(1)]),
        );

        match loaded {
            Err(RaftError::PersistedFormat {
                artifact,
                path: reported,
                problem,
            }) => {
                check!(
                    (artifact, reported, problem) == (QUORUM_STATE_FILE, path, want),
                    "{case}"
                );
            }
            other => panic!("{case}: expected a persisted-format error, got {other:?}"),
        }
    }
}

/// A file that is not JSON at all, such as the binary quorum state a build
/// before 1.0 once wrote, or a zero-filled file, is refused rather than read
/// as "no vote".
#[test]
fn load_quorum_state_refuses_a_file_that_is_not_json() {
    for contents in [&[0u8; 53][..], &[0u8; 54][..], b"", b"{\"leaderId\""] {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(QUORUM_STATE_FILE), contents).expect("write state");

        let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]));

        check!(
            matches!(
                loaded,
                Err(RaftError::PersistedFormat {
                    artifact: QUORUM_STATE_FILE,
                    problem: PersistedFormatError::Malformed(_),
                    ..
                })
            ),
            "{contents:?}"
        );
    }
}
