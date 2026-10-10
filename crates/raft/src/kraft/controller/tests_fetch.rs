//! Tests for the Fetch and `FetchSnapshot` paths: the predicates that classify
//! a response, the replies a follower and a leaderless node serve, and the
//! requests the engine emits while replicating or transferring a snapshot.

use std::time::Duration as StdDuration;

use assert2::assert;

use super::*;
use crate::kraft::{
    controller::{
        offsets::metadata_fetch_offset_below_log_start,
        records::{decode_batches, encode_batches},
        replication::{
            FetchBatchDisposition, classify_fetch_batch, fetch_epoch_for_request,
            should_serve_fetch_records, should_start_snapshot_fetch,
            snapshot_fetch_response_invalid,
        },
        test_support::{
            EngineSetup, build_engine_only, elect_single_voter_engine, one_offset_batch,
            record_peer_sends, recv_peer_send, recv_peer_send_with_api, submit_change_with_timeout,
            topic_record,
        },
    },
    types::LogOffsetMetadata,
};

#[test]
fn snapshot_install_admission_rejects_malformed_stale_and_pending_cases() {
    use krabka_verified::{SnapshotInstallDecision, snapshot_install_decision};

    for (pending, end, epoch, log_end, expected) in [
        (false, 11, 3, 10, SnapshotInstallDecision::Install),
        (false, 10, 3, 10, SnapshotInstallDecision::Stale),
        (false, 9, 3, 10, SnapshotInstallDecision::Stale),
        (true, 11, 3, 10, SnapshotInstallDecision::Reject),
        (false, -1, 3, 10, SnapshotInstallDecision::Reject),
        (false, 11, -1, 10, SnapshotInstallDecision::Reject),
    ] {
        assert2::assert!(snapshot_install_decision(pending, end, epoch, log_end) == expected);
    }
}

fn become_follower(engine: &mut Engine, leader_id: NodeId, leader_epoch: Epoch) {
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id,
        leader_epoch,
    });
    assert2::assert!(matches!(
        engine.core.role(),
        Role::Follower { leader_id: active, .. } if *active == leader_id
    ));
}

/// A successful Fetch answer from `leader_id` in `leader_epoch`.
fn leader_answer(leader_id: NodeId, leader_epoch: Epoch) -> wire::FetchAnswer {
    wire::FetchAnswer {
        error_code: 0,
        leader: wire::QuorumLeader {
            leader_id: Some(leader_id),
            epoch: leader_epoch,
            endpoint: None,
        },
        diverging: None,
        snapshot_id: None,
        hwm: 0,
        log_start_offset: 0,
        records: bytes::Bytes::new(),
    }
}

/// The endpoint every test voter announces (`test_support::voter_set`).
fn test_endpoint() -> (String, u16) {
    ("127.0.0.1".to_string(), 9_093)
}

/// A Fetch request from `from` in `current_leader_epoch`, at offset 0.
fn fetch_request(from: NodeId, current_leader_epoch: i32) -> bytes::Bytes {
    wire::PeerRequest::Fetch {
        cluster_id: None,
        max_wait_ms: 0,
        high_watermark: -1,
        from,
        current_leader_epoch,
        fetch_epoch: 0,
        fetch_offset: 0,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .encode()
}

/// A transport that reaches no one and records the leader endpoints the
/// engine learns from responses.
#[derive(Default)]
struct EndpointRecorder {
    endpoints: std::sync::Mutex<Vec<(NodeId, String)>>,
}

#[async_trait::async_trait]
impl crate::kraft::transport::PeerSender for EndpointRecorder {
    async fn send(
        &self,
        peer: NodeId,
        _api_key: i16,
        _body: bytes::Bytes,
    ) -> Result<bytes::Bytes, crate::error::RaftError> {
        Err(crate::error::RaftError::NotLeader {
            current_leader: Some(peer),
        })
    }

    fn remember_leader_endpoint(&self, leader: NodeId, address: String) {
        self.endpoints
            .lock()
            .expect("endpoint lock")
            .push((leader, address));
    }
}

#[test]
fn fetch_records_are_served_only_by_clean_leader_fetches() {
    for (_case, has_snapshot, has_divergence, is_leader, want) in [
        ("clean leader", false, false, true, true),
        ("snapshot response", true, false, true, false),
        ("divergence response", false, true, true, false),
        ("clean follower", false, false, false, false),
    ] {
        assert2::assert!(
            should_serve_fetch_records(has_snapshot, has_divergence, is_leader) == want
        );
    }
}

#[test]
fn fetch_epoch_uses_installed_snapshot_epoch_only_at_empty_boundary() {
    for (_case, installed, log_start, log_end, last_epoch, want) in [
        ("empty log with installed snapshot", Some(7), 10, 10, 3, 7),
        ("non-empty log with snapshot", Some(7), 10, 11, 3, 3),
        ("empty log without snapshot", None, 10, 10, 3, 3),
    ] {
        assert2::assert!(
            fetch_epoch_for_request(installed, Offset(log_start), Offset(log_end), last_epoch)
                == want
        );
    }
}

#[test]
fn fetch_batch_classifier_separates_duplicate_append_and_gap() {
    for (_case, base_offset, log_end, want) in [
        (
            "duplicate batch",
            4,
            5,
            FetchBatchDisposition::AlreadyPresent,
        ),
        ("contiguous append", 5, 5, FetchBatchDisposition::Append),
        ("offset gap", 6, 5, FetchBatchDisposition::Gap),
    ] {
        assert2::assert!(classify_fetch_batch(Offset(base_offset), Offset(log_end)) == want);
    }
}

#[test]
fn snapshot_fetch_hint_starts_only_for_future_non_duplicate_snapshots() {
    for (_case, snapshot_id, log_end, in_flight, want) in [
        ("future snapshot", (11, 2), 10, None, true),
        ("snapshot at log end", (10, 2), 10, None, false),
        (
            "duplicate in-flight snapshot",
            (11, 2),
            10,
            Some((11, 2)),
            false,
        ),
        ("newer in-flight snapshot", (12, 2), 10, Some((11, 2)), true),
    ] {
        assert2::assert!(
            should_start_snapshot_fetch(snapshot_id, Offset(log_end), in_flight) == want
        );
    }
}

#[test]
fn snapshot_fetch_response_is_invalid_unless_success_from_active_leader() {
    for (_case, error_code, response_epoch, current_epoch, want) in [
        ("successful active leader", 0, 2, 2, false),
        ("error from active leader", 1, 2, 2, true),
        ("success from wrong epoch", 0, 3, 2, true),
        ("error from wrong epoch", 1, 3, 2, true),
    ] {
        assert2::assert!(
            snapshot_fetch_response_invalid(
                error_code,
                NodeId(response_epoch),
                NodeId(current_epoch)
            ) == want
        );
    }
}

fn elect_with_peer(engine: &mut super::Engine, peer: NodeId) {
    engine.on_event(Event::ElectionTimeout);
    for epoch in [0, 1] {
        engine.on_event(Event::ReceiveVoteResponse {
            from: peer,
            epoch,
            vote_granted: true,
        });
    }
    assert!(engine.core.role().is_leader());
}

fn fetch_at_tip(
    engine: &mut Engine,
    peer: NodeId,
    fetch_epoch: u32,
) -> oneshot::Receiver<bytes::Bytes> {
    let offset = engine.log.log_end_offset().0;
    inbound_fetch(engine, peer, fetch_epoch, offset, uuid::Uuid::nil())
}

fn inbound_fetch(
    engine: &mut Engine,
    peer: NodeId,
    fetch_epoch: u32,
    fetch_offset: i64,
    directory: uuid::Uuid,
) -> oneshot::Receiver<bytes::Bytes> {
    let (reply, receiver) = oneshot::channel();
    engine.on_inbound(Inbound::Fetch {
        version: crate::kraft::transport::wire::FETCH_VERSION,
        req: wire::PeerRequest::Fetch {
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
            from: peer,
            current_leader_epoch: i32::try_from(engine.core.quorum_state().leader_epoch).unwrap(),
            fetch_epoch,
            fetch_offset,
            replica_directory_id: directory,
        }
        .encode(),
        reply,
    });
    receiver
}

/// A Fetch response from a newer epoch moves a follower or an observer to the
/// leader it names, as `KafkaRaftClient.maybeHandleCommonResponse` does.
///
/// Node 1 was the leader at epoch 1 and lost the epoch. It now answers a Fetch
/// with the new leader, node 3 at epoch 7. The response fence accepts only a
/// response that matches this node's own leader and epoch, so a node that
/// ignored the newer epoch would keep fetching from node 1 and reject every
/// answer. An observer hears of a new leader in no other way.
#[derive(Clone, Copy, Default)]
enum TwoVoterRole {
    #[default]
    Leader,
    Follower,
}

fn two_voter_engine(role: TwoVoterRole) -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    match role {
        TwoVoterRole::Leader => elect_with_peer(&mut engine, NodeId(2)),
        TwoVoterRole::Follower => become_follower(&mut engine, NodeId(2), 3),
    }
    (engine, dir)
}

#[tokio::test]
async fn a_fetch_response_from_a_newer_epoch_moves_the_node_to_that_leader() {
    for (case, me) in [
        ("an observer that joins later", NodeId(4)),
        ("a voter that follows", NodeId(2)),
    ] {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            me,
            ..Default::default()
        });
        engine.on_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        });

        engine.on_fetch_response(
            NodeId(1),
            &wire::PeerResponse::Fetch(wire::FetchAnswer {
                diverging: None,
                snapshot_id: None,
                hwm: 0,
                records: bytes::Bytes::new(),
                ..leader_answer(NodeId(3), 7)
            })
            .encode(),
        );

        let state = engine.core.quorum_state();
        assert!(
            (state.leader_id, state.leader_epoch) == (Some(NodeId(3)), 7),
            "{case}"
        );
        assert!(
            matches!(
                engine.core.role(),
                Role::Follower {
                    leader_id: NodeId(3),
                    ..
                } | Role::Observer {
                    leader_id: Some(NodeId(3)),
                    ..
                }
            ),
            "{case}"
        );
    }
}

/// A replica answers a Fetch as Kafka's `tryCompleteFetchRequest` does:
/// `validateLeaderOnlyRequest` refuses another epoch, and a replica that is
/// not the leader, with `buildEmptyFetchResponse`, which names the leader
/// this replica knows, its epoch, and the leader's endpoint.
#[tokio::test]
async fn a_replica_that_cannot_serve_a_fetch_names_the_leader_it_knows() {
    const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    const FENCED_LEADER_EPOCH: i16 = 74;
    const UNKNOWN_LEADER_EPOCH: i16 = 75;
    let refusal = |error_code, leader_id: Option<NodeId>, epoch| wire::FetchAnswer {
        error_code,
        leader: wire::QuorumLeader {
            leader_id,
            epoch,
            endpoint: leader_id.map(|_| test_endpoint()),
        },
        hwm: -1,
        ..leader_answer(NodeId(0), epoch)
    };
    for (case, leader, request_epoch, expected) in [
        // KafkaRaftClient: "non-leaders do not expect to receive requests
        // matching their own epoch, but it is possible when observers are
        // using the Fetch API to find the result of an election."
        (
            "follower of leader 2",
            Some(NodeId(2)),
            1,
            refusal(NOT_LEADER_OR_FOLLOWER, Some(NodeId(2)), 1),
        ),
        (
            "fetcher in an older epoch",
            Some(NodeId(2)),
            0,
            refusal(FENCED_LEADER_EPOCH, Some(NodeId(2)), 1),
        ),
        (
            "fetcher in a newer epoch",
            Some(NodeId(2)),
            2,
            refusal(UNKNOWN_LEADER_EPOCH, Some(NodeId(2)), 1),
        ),
        (
            "no known leader",
            None,
            0,
            refusal(NOT_LEADER_OR_FOLLOWER, None, 0),
        ),
    ] {
        let (mut replica, _dir) = build_engine_only(EngineSetup::default());
        if let Some(leader_id) = leader {
            become_follower(&mut replica, leader_id, 1);
        }
        let (reply, mut response) = oneshot::channel();

        replica.on_inbound(Inbound::Fetch {
            version: crate::kraft::transport::wire::FETCH_VERSION,
            req: fetch_request(NodeId(3), request_epoch),
            reply,
        });

        let body = response.try_recv().expect("the replica answered the Fetch");
        assert!(
            wire::PeerResponse::decode_fetch(&body) == Some(wire::PeerResponse::Fetch(expected)),
            "{case}"
        );
    }
}

/// The leader refuses a fetcher from another epoch too, naming itself, so
/// the fetcher moves to its epoch (`maybeHandleCommonResponse`).
#[tokio::test]
async fn a_leader_refuses_a_fetch_from_another_epoch_and_names_itself() {
    let (mut leader, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    leader.on_event(Event::ElectionTimeout);
    for epoch in [0, 1] {
        leader.on_event(Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch,
            vote_granted: true,
        });
    }
    assert!(leader.core.role().is_leader());
    let epoch = leader.core.quorum_state().leader_epoch;
    let (reply, mut response) = oneshot::channel();

    leader.on_inbound(Inbound::Fetch {
        version: crate::kraft::transport::wire::FETCH_VERSION,
        req: fetch_request(NodeId(2), i32::try_from(epoch).unwrap() - 1),
        reply,
    });

    let body = response.try_recv().expect("the leader answered the Fetch");
    assert!(
        wire::PeerResponse::decode_fetch(&body)
            == Some(wire::PeerResponse::Fetch(wire::FetchAnswer {
                error_code: 74,
                leader: wire::QuorumLeader {
                    leader_id: Some(NodeId(1)),
                    epoch,
                    endpoint: Some(test_endpoint()),
                },
                hwm: -1,
                log_start_offset: leader.log.log_start_offset().0,
                ..leader_answer(NodeId(1), epoch)
            }))
    );
    // A refused fetch is no replica progress.
    assert!(leader.observers.is_empty());
    assert!(matches!(
        leader.core.role(),
        Role::Leader { replicas, .. } if replicas[&NodeId(2)] == ReplicaProgress::default()
    ));
}

/// Kafka's `buildEndQuorumEpochRequest`: every other voter gets the cluster
/// id and the successors in the core's order.
#[tokio::test]
async fn broadcast_end_quorum_epoch_sends_to_every_other_voter() {
    let (engine, _dir, mut sends) = broadcast_fixture();

    engine.broadcast_end_quorum_epoch(4, &[NodeId(3), NodeId(2)]);

    let mut peers = Vec::new();
    for _ in 0..2 {
        let send = recv_peer_send(&mut sends).await;
        assert2::assert!(send.api_key == api_key::END_QUORUM_EPOCH);
        assert2::assert!(
            wire::decode_end(&send.body)
                == Some(wire::PeerRequest::EndQuorumEpoch {
                    cluster_id: Some(uuid::Uuid::nil()),
                    leader_id: NodeId(1),
                    leader_epoch: 4,
                    preferred_candidates: vec![
                        (NodeId(3), uuid::Uuid::nil()),
                        (NodeId(2), uuid::Uuid::nil())
                    ],
                })
        );
        peers.push(send.peer);
    }
    peers.sort_unstable();
    assert2::assert!(peers == vec![NodeId(2), NodeId(3)]);
}

fn broadcast_fixture() -> (
    Engine,
    tempfile::TempDir,
    tokio::sync::mpsc::UnboundedReceiver<crate::kraft::controller::test_support::CapturedPeerSend>,
) {
    let (mut engine, dir) = build_engine_only(EngineSetup::default());
    let sends = record_peer_sends(&mut engine, wire::PeerResponse::Ack { epoch: 4 }.encode());
    (engine, dir, sends)
}

/// Kafka's `buildBeginQuorumEpochRequest`: each other voter gets the cluster
/// id, its own voter key, and the leader's listeners in `LeaderEndpoints`.
#[tokio::test]
async fn broadcast_begin_quorum_epoch_names_each_recipient_and_the_leader_endpoints() {
    let (engine, _dir, mut sends) = broadcast_fixture();

    engine.broadcast_begin_quorum_epoch(4);

    let mut requests = Vec::new();
    for _ in 0..2 {
        let send = recv_peer_send(&mut sends).await;
        assert2::assert!(send.api_key == api_key::BEGIN_QUORUM_EPOCH);
        requests.push((send.peer, wire::decode_begin(&send.body)));
    }
    requests.sort_unstable_by_key(|(peer, _)| *peer);
    let expected = |voter: NodeId| {
        Some(wire::PeerRequest::BeginQuorumEpoch {
            cluster_id: Some(uuid::Uuid::nil()),
            voter_id: voter,
            voter_directory_id: uuid::Uuid::nil(),
            leader_id: NodeId(1),
            leader_epoch: 4,
            leader_endpoints: vec![("CONTROLLER".into(), "127.0.0.1".into(), 9_093)],
        })
    };
    assert2::assert!(
        requests
            == vec![
                (NodeId(2), expected(NodeId(2))),
                (NodeId(3), expected(NodeId(3)))
            ]
    );
}

/// Kafka's `handleBeginQuorumEpochRequest` and `handleEndQuorumEpochRequest`
/// take the leader's address from `LeaderEndpoints` when the request carries
/// them: a replica whose voter set does not name the leader, as during an
/// uncommitted KIP-853 voter change, still learns where to fetch from.
#[tokio::test]
async fn quorum_epoch_requests_teach_the_leader_endpoints() {
    use krabka_protocol::{
        Encode,
        owned::{
            begin_quorum_epoch_request::{self as bqe, BeginQuorumEpochRequest},
            end_quorum_epoch_request::{self as eqe, EndQuorumEpochRequest},
        },
    };

    let endpoints = || {
        vec![
            ("REPLICATION", "r4.example", 9_094),
            ("CONTROLLER", "c4.example", 9_093),
        ]
    };
    let mut begin = BeginQuorumEpochRequest {
        voter_id: 1,
        topics: vec![bqe::TopicData {
            topic_name: wire::METADATA_TOPIC.into(),
            partitions: vec![bqe::PartitionData {
                leader_id: 4,
                leader_epoch: 3,
                ..Default::default()
            }],
            ..Default::default()
        }],
        leader_endpoints: endpoints()
            .into_iter()
            .map(|(name, host, port)| bqe::LeaderEndpoint {
                name: name.into(),
                host: host.into(),
                port,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let end = EndQuorumEpochRequest {
        topics: vec![eqe::TopicData {
            topic_name: wire::METADATA_TOPIC.into(),
            partitions: vec![eqe::PartitionData {
                leader_id: 4,
                leader_epoch: 3,
                ..Default::default()
            }],
            ..Default::default()
        }],
        leader_endpoints: endpoints()
            .into_iter()
            .map(|(name, host, port)| eqe::LeaderEndpoint {
                name: name.into(),
                host: host.into(),
                port,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let encode = |message: &dyn Fn(&mut bytes::BytesMut)| {
        let mut body = bytes::BytesMut::new();
        message(&mut body);
        body.freeze()
    };
    let begin_body = encode(&|body| begin.encode(body, 1).expect("encode"));
    let end_body = encode(&|body| end.encode(body, 1).expect("encode"));
    begin.leader_endpoints.clear();
    let bare_begin_body = encode(&|body| begin.encode(body, 1).expect("encode"));

    let learned = vec![(NodeId(4), "c4.example:9093".to_string())];
    for (name, api, body, remembered) in [
        (
            "BeginQuorumEpoch",
            api_key::BEGIN_QUORUM_EPOCH,
            begin_body,
            learned.clone(),
        ),
        (
            "EndQuorumEpoch",
            api_key::END_QUORUM_EPOCH,
            end_body,
            learned,
        ),
        (
            "BeginQuorumEpoch without endpoints",
            api_key::BEGIN_QUORUM_EPOCH,
            bare_begin_body,
            Vec::new(),
        ),
    ] {
        let (mut replica, _dir) = build_engine_only(EngineSetup::default());
        let recorder = std::sync::Arc::new(EndpointRecorder::default());
        replica.peers = recorder.clone();
        let (reply, mut answer) = oneshot::channel();
        replica.on_inbound(if api == api_key::BEGIN_QUORUM_EPOCH {
            Inbound::BeginQuorumEpoch {
                req: body,
                version: 1,
                reply,
            }
        } else {
            Inbound::EndQuorumEpoch {
                req: body,
                version: 1,
                reply,
            }
        });
        assert2::assert!(answer.try_recv().is_ok(), "{name}");
        assert2::assert!(
            *recorder.endpoints.lock().expect("endpoint lock") == remembered,
            "{name}"
        );
    }
}

async fn check_sent_fetch(
    engine: &mut Engine,
    sends: &mut mpsc::UnboundedReceiver<super::test_support::CapturedPeerSend>,
    expected: (u32, i64),
) {
    engine.send_fetch(NodeId(2));
    let send = recv_peer_send(sends).await;
    match wire::decode_fetch(&send.body) {
        Some(wire::PeerRequest::Fetch {
            fetch_epoch,
            fetch_offset,
            ..
        }) => {
            assert2::assert!((fetch_epoch, fetch_offset) == expected);
        }
        other => panic!("unexpected fetch request: {other:?}"),
    }
}

#[tokio::test]
async fn send_fetch_uses_snapshot_epoch_only_until_log_extends_past_boundary() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    engine
        .log
        .install_snapshot(Offset(10))
        .expect("install snapshot");
    engine.installed_snapshot_epoch = Some(7);
    let fetch_response = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: None,
        hwm: 10,
        records: bytes::Bytes::new(),
        ..leader_answer(NodeId(2), 7)
    })
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_response.clone());

    check_sent_fetch(&mut engine, &mut sends, (7, 10)).await;

    let mut batch = one_offset_batch(10, 9, b"after-snapshot");
    engine
        .log
        .append_at(&mut batch, Offset(10))
        .expect("append after snapshot");
    check_sent_fetch(&mut engine, &mut sends, (9, 11)).await;
}

#[test]
fn serve_fetch_records_returns_batches_only_for_offsets_inside_log() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        metadata_raft_fetch_max: MetadataRaftFetchMax::try_from(krabka_units::bytes(1))
            .expect("one byte still serves the first batch"),
        ..Default::default()
    });
    let mut batch = one_offset_batch(0, 1, b"a");
    engine.log.append(&mut batch, 0).expect("append");
    let mut batch = one_offset_batch(1, 1, b"b");
    engine.log.append(&mut batch, 0).expect("append");

    assert2::assert!(engine.serve_fetch_records(Offset(-1)).is_empty());
    assert2::assert!(engine.serve_fetch_records(Offset(2)).is_empty());
    let records = engine.serve_fetch_records(Offset(0));
    let decoded = decode_batches(&records).expect("decode served records");
    assert2::assert!(
        decoded
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
            == vec![0]
    );
}

#[tokio::test]
async fn fetch_response_snapshot_hint_starts_once_and_ignores_stale_hint() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        metadata_raft_fetch_max: MetadataRaftFetchMax::try_from(krabka_units::bytes(512))
            .expect("positive fetch maximum"),
        ..Default::default()
    });
    let fetch_snapshot_response = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (11, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 0,
    }
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_snapshot_response);
    become_follower(&mut engine, NodeId(2), 3);

    let body = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: Some((11, 3)),
        hwm: 11,
        records: bytes::Bytes::new(),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();
    engine.on_fetch_response(NodeId(2), &body);
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH_SNAPSHOT).await;
    match wire::decode_fetch_snapshot(&send.body) {
        Some(wire::PeerRequest::FetchSnapshot {
            snapshot_id,
            position,
            max_bytes,
            ..
        }) => {
            assert2::assert!(snapshot_id == (11, 3));
            assert2::assert!(position == 0);
            assert2::assert!(max_bytes == 512);
        }
        other => panic!("unexpected fetch snapshot request: {other:?}"),
    }
    assert2::assert!(
        engine
            .snapshot_fetch
            .as_ref()
            .is_some_and(|s| s.snapshot_id == (11, 3))
    );

    engine.on_fetch_response(NodeId(2), &body);
    assert2::assert!(
        tokio::time::timeout(StdDuration::from_millis(20), async {
            loop {
                let send = recv_peer_send(&mut sends).await;
                if send.api_key == api_key::FETCH_SNAPSHOT {
                    return send;
                }
            }
        })
        .await
        .is_err()
    );

    engine
        .log
        .install_snapshot(Offset(11))
        .expect("install snapshot");
    engine.snapshot_fetch = None;
    engine.on_fetch_response(NodeId(2), &body);
    assert2::assert!(engine.snapshot_fetch.is_none());
}

/// A leaderless observer that asks a follower is redirected: the follower
/// answers `NOT_LEADER_OR_FOLLOWER` naming leader 2 and its `NodeEndpoints`
/// entry. The observer attaches to node 2 at that endpoint, as Kafka's
/// `maybeHandleCommonResponse` transitions to follower with the response's
/// leader endpoints, and applies content only once node 2 itself answers.
#[tokio::test]
async fn leaderless_observer_discovers_the_leader_through_a_follower_redirect() {
    let (mut observer, _dir) = build_engine_only(EngineSetup {
        me: NodeId(3),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let transport = Arc::new(EndpointRecorder::default());
    observer.peers = transport.clone();
    assert2::assert!(matches!(
        observer.core.role(),
        Role::Observer {
            leader_id: None,
            ..
        }
    ));
    let epoch = observer.core.quorum_state().leader_epoch;
    let leader_endpoint = wire::QuorumLeader {
        leader_id: Some(NodeId(2)),
        epoch,
        endpoint: Some(("controller-2".to_string(), 9_093)),
    };

    let redirect = wire::PeerResponse::Fetch(wire::FetchAnswer {
        error_code: 6,
        leader: leader_endpoint.clone(),
        hwm: -1,
        ..leader_answer(NodeId(2), epoch)
    })
    .encode();
    observer.on_fetch_response(NodeId(1), &redirect);
    assert2::assert!(matches!(
        observer.core.role(),
        Role::Observer {
            leader_id: Some(NodeId(2)),
            ..
        }
    ));
    assert2::assert!(
        *transport.endpoints.lock().expect("endpoint lock")
            == vec![(NodeId(2), "controller-2:9093".to_string())]
    );

    let served = wire::PeerResponse::Fetch(wire::FetchAnswer {
        leader: leader_endpoint,
        hwm: 1,
        records: encode_batches(&[one_offset_batch(
            0,
            i32::try_from(epoch).unwrap(),
            b"replicated",
        )]),
        ..leader_answer(NodeId(2), epoch)
    })
    .encode();
    // The follower's answer carried no content, and one from it would not be
    // applied: only the attached leader's answers pass the fence.
    observer.on_fetch_response(NodeId(1), &served);
    assert2::assert!(observer.log.log_end_offset() == Offset(0));
    observer.on_fetch_response(NodeId(2), &served);
    assert2::assert!(observer.log.log_end_offset() == Offset(1));
    assert2::assert!(observer.log.hwm() == Offset(1));
}

#[tokio::test]
async fn rejected_fetch_responses_leave_log_watermark_and_snapshot_unchanged() {
    let body = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: Some((11, 3)),
        hwm: 11,
        records: encode_batches(&[one_offset_batch(0, 3, b"foreign")]),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();

    for (case, setup, from) in [
        ("stale epoch", 0u8, NodeId(2)),
        ("wrong sender", 1, NodeId(3)),
        ("changed role", 2, NodeId(2)),
    ] {
        let (mut engine, _dir) = build_engine_only(EngineSetup::default());
        become_follower(&mut engine, NodeId(2), if setup == 0 { 4 } else { 3 });
        if setup == 2 {
            engine.on_event(Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 3,
                successor_rank: crate::kraft::event::SuccessorRank::default(),
            });
            assert2::assert!(matches!(engine.core.role(), Role::Prospective { .. }));
        }

        engine.on_fetch_response(from, &body);

        assert2::assert!(engine.log.log_end_offset() == Offset(0), "{case}");
        assert2::assert!(engine.log.hwm() == Offset(0), "{case}");
        assert2::assert!(engine.snapshot_fetch.is_none(), "{case}");
    }
}

#[tokio::test]
async fn admitted_fetch_selects_truncate_append_or_high_watermark_path() {
    // Truncation does not also append or advance the HWM.
    let (mut truncating, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    for offset in 0..2 {
        let mut batch = one_offset_batch(offset, 2, b"local");
        truncating
            .log
            .append(&mut batch, 0)
            .expect("append local batch");
    }
    become_follower(&mut truncating, NodeId(2), 3);
    let truncate = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: Some(LogOffsetMetadata {
            offset: 1,
            epoch: 2,
        }),
        snapshot_id: None,
        hwm: 2,
        records: encode_batches(&[one_offset_batch(2, 3, b"must-not-append")]),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();
    truncating.on_fetch_response(NodeId(2), &truncate);
    assert2::assert!(truncating.log.log_end_offset() == Offset(1));
    assert2::assert!(truncating.log.hwm() == Offset(0));

    // Append advances the HWM only after the carried batch reaches the log.
    let (mut appending, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    become_follower(&mut appending, NodeId(2), 3);
    let append = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: None,
        hwm: 1,
        records: encode_batches(&[one_offset_batch(0, 3, b"replicated")]),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();
    appending.on_fetch_response(NodeId(2), &append);
    assert2::assert!(appending.log.log_end_offset() == Offset(1));
    assert2::assert!(appending.log.hwm() == Offset(1));

    // An empty response can advance only the watermark over existing data.
    let (mut advancing, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let mut local = one_offset_batch(0, 2, b"already-replicated");
    advancing
        .log
        .append(&mut local, 0)
        .expect("append local batch");
    become_follower(&mut advancing, NodeId(2), 3);
    let watermark = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: None,
        hwm: 1,
        records: bytes::Bytes::new(),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();
    advancing.on_fetch_response(NodeId(2), &watermark);
    assert2::assert!(advancing.log.log_end_offset() == Offset(1));
    assert2::assert!(advancing.log.hwm() == Offset(1));
}

#[tokio::test]
async fn fetch_snapshot_response_error_or_wrong_leader_aborts_transfer() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let fetch_response = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: None,
        hwm: 0,
        records: bytes::Bytes::new(),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();
    let mut sends = record_peer_sends(&mut engine, fetch_response);

    engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
    let error_body = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (12, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 99,
    }
    .encode();
    engine.on_fetch_snapshot_response(NodeId(2), &error_body);
    assert2::assert!(engine.snapshot_fetch.is_none());
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
    assert2::assert!(send.peer == 2);

    engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
    let ok_body = wire::PeerResponse::FetchSnapshot {
        snapshot_id: (12, 3),
        size: 0,
        position: 0,
        bytes: bytes::Bytes::new(),
        error_code: 0,
    }
    .encode();
    engine.on_fetch_snapshot_response(NodeId(3), &ok_body);
    assert2::assert!(engine.snapshot_fetch.is_none());
    let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
    assert2::assert!(send.peer == 3);
}

/// A follower clamps its own high watermark to its log end, so that value
/// alone cannot say how far behind the quorum this node is. The snapshot
/// therefore carries the leader's watermark separately, and it never goes
/// backwards when a later response reports a lower one.
#[tokio::test]
async fn quorum_high_watermark_keeps_the_leader_s_watermark_past_the_local_clamp() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });

    // A node that has heard from nobody reports its own watermark.
    assert!(engine.quorum_state_snapshot().quorum_high_watermark == 0);

    // Only a response that clears the leader/epoch fence is admitted, so put
    // the node behind node 2 at the epoch the responses below carry.
    become_follower(&mut engine, NodeId(2), 3);

    let response = |hwm: i64| {
        wire::PeerResponse::Fetch(wire::FetchAnswer {
            diverging: None,
            snapshot_id: None,
            hwm,
            records: bytes::Bytes::new(),
            ..leader_answer(NodeId(2), 3)
        })
        .encode()
    };

    // The leader has committed 10 000 records and this node has none of them.
    engine.on_fetch_response(NodeId(2), &response(10_000));
    let snapshot = engine.quorum_state_snapshot();
    assert!(snapshot.high_watermark == 0);
    assert!(snapshot.quorum_high_watermark == 10_000);

    // A stale response cannot walk the quorum's committed offset back: every
    // watermark a leader reports is committed, so the highest one seen is a
    // lower bound on what the quorum has.
    engine.on_fetch_response(NodeId(2), &response(9_000));
    assert!(engine.quorum_state_snapshot().quorum_high_watermark == 10_000);
}

/// A node whose fetch offset is below the leader's pruned log start is told to
/// take a snapshot instead, and that response carries the leader's watermark
/// like any other. It is also the node furthest behind the quorum, so dropping
/// the watermark on that path would report the worst laggard as caught up.
#[tokio::test]
async fn quorum_high_watermark_is_recorded_from_a_snapshot_redirect_too() {
    let (mut engine, _dir) = two_voter_engine(TwoVoterRole::Follower);

    let redirect = wire::PeerResponse::Fetch(wire::FetchAnswer {
        diverging: None,
        snapshot_id: Some((20_000, 3)),
        hwm: 20_000,
        records: bytes::Bytes::new(),
        ..leader_answer(NodeId(2), 3)
    })
    .encode();

    engine.on_fetch_response(NodeId(2), &redirect);
    let snapshot = engine.quorum_state_snapshot();
    assert!(snapshot.high_watermark == 0);
    assert!(snapshot.quorum_high_watermark == 20_000);
}

/// Every controller serves an observer's metadata fetch, not only the leader,
/// so the slice says both how far this node has committed -- which bounds the
/// records it can hand over -- and how far the quorum has. A follower that is
/// itself catching up would otherwise report its own clamped watermark as the
/// quorum's, and an observer that drew level with it would call itself ready.
#[tokio::test]
async fn a_lagging_follower_serves_the_quorums_committed_offset() {
    let (mut engine, _dir) = two_voter_engine(TwoVoterRole::Follower);

    engine.on_fetch_response(
        NodeId(2),
        &wire::PeerResponse::Fetch(wire::FetchAnswer {
            diverging: None,
            snapshot_id: None,
            hwm: 10_000,
            records: bytes::Bytes::new(),
            ..leader_answer(NodeId(2), 3)
        })
        .encode(),
    );

    let slice = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(slice.high_watermark == 0);
    assert!(slice.quorum_high_watermark == 10_000);
}

#[test]
fn a_metadata_fetch_needs_a_snapshot_only_below_the_retained_log() {
    // Half-open: a fetch *at* the log start still reads from the log, and a
    // node that has never pruned (log start 0) never redirects. Widening this
    // to `<=` would answer a caught-up observer with a snapshot on every poll.
    for (_case, fetch_offset, log_start, want) in [
        ("pruned away", 0, 4_096, true),
        ("one below the start", 4_095, 4_096, true),
        ("at the start", 4_096, 4_096, false),
        ("past the start", 4_097, 4_096, false),
        ("never pruned", 0, 0, false),
        ("negative offset", -1, 4_096, false),
    ] {
        assert!(
            metadata_fetch_offset_below_log_start(Offset(fetch_offset), Offset(log_start)) == want,
            "fetch {fetch_offset} against log start {log_start}"
        );
    }
}

/// An observer that asks for an offset the controller has already pruned is
/// pointed at the latest checkpoint instead of being served an empty slice.
///
/// This is the restart case: a broker-only node comes back at offset 0, the
/// controller has snapshotted and pruned past it, and without the snapshot id
/// the observer re-asks for the same gone offset on every poll, never builds
/// an image, and never registers.
#[tokio::test]
async fn a_metadata_fetch_below_the_pruned_log_start_returns_the_snapshot_id() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    for name in ["a", "b", "c"] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record(name));
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");
    let log_start = engine.log.log_start_offset();
    assert!(log_start > 0, "the log must have been pruned");
    let snapshot_id = engine.latest_snapshot_id().expect("a checkpoint exists");

    let pruned = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(pruned.snapshot_id == Some(snapshot_id));
    assert!(pruned.records.is_empty());
    assert!(pruned.log_start_offset == log_start.0);

    // At the retained boundary the log still answers, so the observer keeps
    // fetching records rather than re-installing a snapshot it already has.
    let retained = engine.metadata_fetch_slice(log_start.0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    assert!(retained.snapshot_id == None);
}

/// krabka-io/krabka-broker#912: the diverging epoch a leader answers a Fetch
/// with is a hint for the follower. The leader must not apply it to its own
/// log.
///
/// Node 1 holds epoch 1 over offsets 0 to 5, and it wins epoch 3, which
/// appends its leader-change record at offset 5. Node 2 fetches at epoch 1
/// and offset 8: it kept an epoch 1 tail that this leader never had, which is
/// what a crashed leader brings back. The leader answers that epoch 1 ends at
/// offset 5. Applied to its own log, the same hint would delete the epoch 3
/// records, the leader-change record included, and move the high watermark
/// back.
#[tokio::test]
async fn a_leader_answers_a_diverging_fetch_without_truncating_its_own_log() {
    let (mut engine, _dir) = build_engine_only(EngineSetup::default());
    for offset in 0..5 {
        engine
            .log
            .append(&mut one_offset_batch(offset, 1, b"epoch-1"), 0)
            .expect("append an epoch 1 record");
    }
    become_follower(&mut engine, NodeId(2), 2);
    engine.on_event(Event::ElectionTimeout);
    for epoch in [2, 3] {
        for from in [NodeId(2), NodeId(3)] {
            engine.on_event(Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted: true,
            });
        }
    }
    assert!(engine.core.role().is_leader());
    let leader_epoch = engine.core.quorum_state().leader_epoch;
    let log_end = engine.log.log_end_offset();
    assert!(log_end > Offset(5), "the leader appended in its own epoch");

    let mut response = inbound_fetch(&mut engine, NodeId(2), 1, 8, uuid::Uuid::nil());

    let body = response.try_recv().expect("the leader answered the Fetch");
    let diverging = match wire::PeerResponse::decode_fetch(&body) {
        Some(wire::PeerResponse::Fetch(wire::FetchAnswer { diverging, .. })) => diverging,
        other => panic!("expected a Fetch response, got {other:?}"),
    };
    assert2::check!(
        diverging
            == Some(LogOffsetMetadata {
                offset: 5,
                epoch: 1
            })
    );
    assert2::check!(engine.log.log_end_offset() == log_end);
    assert2::check!(engine.core.role().is_leader());
    assert2::check!(engine.core.quorum_state().leader_epoch == leader_epoch);
}

#[tokio::test]
async fn quorum_state_snapshot_tracks_fetch_timestamps_and_observers() {
    let (mut engine, _dir) = two_voter_engine(TwoVoterRole::default());

    // 1. Before Node 2 fetches, fetch_ms and caught_up_ms are -1
    let snap1 = engine.quorum_state_snapshot();
    assert2::check!(snap1.per_replica_last_fetch_ms.get(&NodeId(2)) == Some(&-1));
    assert2::check!(snap1.per_replica_last_caught_up_ms.get(&NodeId(2)) == Some(&-1));
    // Leader itself has a real timestamp > 1_700_000_000_000
    let leader_ms = snap1
        .per_replica_last_fetch_ms
        .get(&NodeId(1))
        .copied()
        .unwrap_or(0);
    let leader_caught_ms = snap1
        .per_replica_last_caught_up_ms
        .get(&NodeId(1))
        .copied()
        .unwrap_or(0);
    assert2::check!(leader_ms > 1_700_000_000_000);
    assert2::check!(leader_caught_ms > 1_700_000_000_000);

    // 2. After Node 2 fetches at the current log end offset:
    tokio::time::sleep(StdDuration::from_millis(10)).await;
    let _rx = fetch_at_tip(&mut engine, NodeId(2), 1);

    let snap2 = engine.quorum_state_snapshot();
    let peer_fetch_ms = snap2
        .per_replica_last_fetch_ms
        .get(&NodeId(2))
        .copied()
        .unwrap_or(0);
    let peer_caught_ms = snap2
        .per_replica_last_caught_up_ms
        .get(&NodeId(2))
        .copied()
        .unwrap_or(0);
    assert2::check!(peer_fetch_ms > 1_700_000_000_000);
    assert2::check!(peer_caught_ms > 1_700_000_000_000);

    // 3. Observer (Node 99) fetches via inbound
    let _rx_obs = inbound_fetch(&mut engine, NodeId(99), 1, 0, uuid::Uuid::from_u128(99));

    // The leader's log holds its `LeaderChange`, so a fetch at offset 0 is a
    // valid fetch that has not reached the log end: the observer is listed with
    // a real fetch time and no caught-up time yet.
    assert2::assert!(engine.log.log_end_offset().0 > 0);
    let snap3 = engine.quorum_state_snapshot();
    let [observer] = snap3.observers.as_slice() else {
        panic!("one observer, got {:?}", snap3.observers);
    };
    assert2::check!(observer.id == NodeId(99));
    assert2::check!(observer.directory_id == uuid::Uuid::from_u128(99));
    assert2::check!(observer.log_end_offset == 0);
    assert2::check!(observer.last_fetch_ms > 1_700_000_000_000);
    assert2::check!(observer.last_caught_up_ms == -1);
}

/// One `Fetch` of `fetch_offset` from `from`, which names `directory_id`,
/// under the leader's own epoch and `fetch_epoch`.
fn fetch_at(
    engine: &mut Engine,
    from: NodeId,
    directory_id: uuid::Uuid,
    fetch_epoch: u32,
    fetch_offset: i64,
) {
    let (reply, _rx) = oneshot::channel();
    engine.on_inbound(Inbound::Fetch {
        version: crate::kraft::transport::wire::FETCH_VERSION,
        req: wire::PeerRequest::Fetch {
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
            from,
            current_leader_epoch: i32::try_from(engine.core.quorum_state().leader_epoch).unwrap(),
            fetch_epoch,
            fetch_offset,
            replica_directory_id: directory_id,
        }
        .encode(),
        reply,
    });
}

/// A single-voter leader whose clock started 50 ms ago, so a fetch is stamped
/// with a nonzero time.
fn leader_with_a_running_clock() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = super::test_support::single_voter_leader_engine();
    engine.clock_base = Instant::now() - StdDuration::from_millis(50);
    (engine, dir)
}

/// Kafka's `LeaderState.observerStates` tracks a non-voter that fetches like a
/// voter: the offset and time of each valid fetch, and the time it was last
/// caught up, by the two-branch rule of `ReplicaState.updateFollowerState`.
#[test]
fn an_observer_is_tracked_with_kafkas_caught_up_rule() {
    let (mut engine, _dir) = leader_with_a_running_clock();
    let observer = NodeId(9);
    let directory = uuid::Uuid::from_u128(9);
    let epoch = engine.core.quorum_state().leader_epoch;
    let first_end = engine.log.log_end_offset().0;
    assert!(first_end > 0);
    // The observer's row as (fetch offset, last fetch, last caught up).
    let row = |engine: &Engine| {
        let snapshot = engine.quorum_state_snapshot();
        let [row] = snapshot.observers.as_slice() else {
            panic!("one observer, got {:?}", snapshot.observers);
        };
        (row.log_end_offset, row.last_fetch_ms, row.last_caught_up_ms)
    };

    // Short of the log end on its first fetch: fetched, never caught up.
    fetch_at(&mut engine, observer, directory, 0, 0);
    let (offset, first_fetch, caught_up) = row(&engine);
    assert!(offset == 0);
    assert!(first_fetch > 0);
    assert!(caught_up == -1);

    // The log grows. A fetch that reaches the log end the previous fetch saw is
    // caught up as of that previous fetch, though it is short of the log end
    // now: a follower keeping pace under continuous appends is not stale.
    engine.test_append_and_commit(&topic_record("grows"));
    assert!(engine.log.log_end_offset().0 > first_end);
    engine.clock_base -= StdDuration::from_millis(20);
    fetch_at(&mut engine, observer, directory, epoch, first_end);
    let (offset, second_fetch, caught_up) = row(&engine);
    assert!(offset == first_end);
    assert!(second_fetch > first_fetch);
    assert!(caught_up == first_fetch);

    // A fetch at the log end is caught up now.
    engine.clock_base -= StdDuration::from_millis(20);
    let end = engine.log.log_end_offset().0;
    fetch_at(&mut engine, observer, directory, epoch, end);
    let (_, third_fetch, caught_up) = row(&engine);
    assert!(third_fetch > second_fetch);
    assert!(caught_up == third_fetch);
}

/// `tryCompleteFetchRequest` moves replica state only for a fetch that
/// validates against the log: one at an epoch the leader does not hold, or past
/// its log end, is answered with a divergence and records nothing.
#[test]
fn a_diverging_observer_fetch_is_not_progress() {
    let (mut engine, _dir) = leader_with_a_running_clock();
    let end = engine.log.log_end_offset().0;
    let epoch = engine.core.quorum_state().leader_epoch;

    // Epoch 0 ends where the leader's log starts, short of the fetch offset.
    fetch_at(&mut engine, NodeId(9), uuid::Uuid::from_u128(9), 0, end);
    // The leader's own epoch ends at its log end, short of the fetch offset.
    fetch_at(
        &mut engine,
        NodeId(8),
        uuid::Uuid::from_u128(8),
        epoch,
        end + 5,
    );

    assert!(engine.quorum_state_snapshot().observers.is_empty());
    assert!(engine.observers.is_empty());
}

/// Kafka's `clearInactiveObservers` drops an observer whose last fetch is five
/// minutes old, and leadership starts over with none.
#[test]
fn an_observer_silent_for_five_minutes_is_dropped() {
    let (mut engine, _dir) = leader_with_a_running_clock();
    let epoch = engine.core.quorum_state().leader_epoch;
    fetch_at(&mut engine, NodeId(9), uuid::Uuid::from_u128(9), epoch, 0);
    assert!(engine.quorum_state_snapshot().observers.len() == 1);

    engine.clock_base -= StdDuration::from_secs(299);
    assert!(engine.quorum_state_snapshot().observers.len() == 1);
    engine.clock_base -= StdDuration::from_secs(2);
    assert!(engine.quorum_state_snapshot().observers.is_empty());
}

#[tokio::test]
async fn kraft_controller_metadata_fetch_returns_slice() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;
    submit_change_with_timeout(&ctrl, topic_record("fetch-test"), "fetch seed")
        .await
        .unwrap();

    let slice = ctrl
        .metadata_fetch(0, krabka_units::prelude::bytes(1024), None)
        .await
        .unwrap();
    assert2::check!(!slice.records.is_empty());
    assert2::check!(slice.high_watermark > 0);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn quorum_state_snapshot_negative_timestamp_fallback() {
    let (mut engine, _dir) = two_voter_engine(TwoVoterRole::default());

    // When wall_clock_base is before UNIX_EPOCH, duration_since returns Err,
    // so map_or fallback -1 must be returned for all timestamps.
    engine.wall_clock_base = std::time::UNIX_EPOCH - StdDuration::from_secs(100_000);
    // Shift clock_base back so engine.now() > 0 and progress.last_fetch / progress.last_caught_up > 0
    engine.clock_base = Instant::now() - StdDuration::from_millis(50);
    drop(fetch_at_tip(&mut engine, NodeId(2), 1));

    let snap = engine.quorum_state_snapshot();
    assert2::assert!(snap.per_replica_last_fetch_ms.get(&NodeId(1)) == Some(&-1));
    assert2::assert!(snap.per_replica_last_caught_up_ms.get(&NodeId(1)) == Some(&-1));
    assert2::assert!(snap.per_replica_last_fetch_ms.get(&NodeId(2)) == Some(&-1));
    assert2::assert!(snap.per_replica_last_caught_up_ms.get(&NodeId(2)) == Some(&-1));
}
