//! Kafka's request checks on an inbound `Vote`, `BeginQuorumEpoch`,
//! `EndQuorumEpoch` and `FetchSnapshot`: one table per API, each row a request and the whole
//! decoded response it must get.

use {
    super::*,
    crate::{
        kraft::{
            controller::test_support::{EngineInDir, build_engine_only, voter_set},
            transport::wire::{FETCH_SNAPSHOT_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION},
            types::{QuorumState, SimInstant},
        },
        test_support::encode,
    },
    assert2::check,
    krabka_protocol::{
        Decode,
        owned::{
            begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
            begin_quorum_epoch_response::{self as bqe_resp, BeginQuorumEpochResponse},
            end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
            end_quorum_epoch_response::{self as eqe_resp, EndQuorumEpochResponse},
            fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
            fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse},
            vote_request::{self as vote_req, VoteRequest},
            vote_response::{self as vote_resp, VoteResponse},
        },
        primitives::uuid::Uuid as WireUuid,
    },
};

const METADATA_TOPIC: &str = "__cluster_metadata";

/// The Kafka base64 form of a cluster id that is not the engines' nil id.
fn foreign_cluster_id() -> String { URL_SAFE_NO_PAD.encode(Uuid::from_u128(7).as_bytes()) }

/// Delivers one inbound request to `engine` and returns its answer, or `None`
/// when the engine dropped the reply, which closes the connection.
fn deliver(
    engine: &mut Engine,
    inbound: impl FnOnce(bytes::Bytes, oneshot::Sender<bytes::Bytes>) -> Inbound,
    body: bytes::Bytes,
) -> Option<bytes::Bytes> {
    let (reply, mut answer) = oneshot::channel();
    engine.on_inbound(inbound(body, reply));
    answer.try_recv().ok()
}

fn vote(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::Vote {
        req,
        version: VOTE_VERSION,
        reply,
    }
}

fn begin(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::BeginQuorumEpoch {
        req,
        version: QUORUM_EPOCH_VERSION,
        reply,
    }
}

fn end(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::EndQuorumEpoch {
        req,
        version: QUORUM_EPOCH_VERSION,
        reply,
    }
}

fn fetch_snapshot(req: bytes::Bytes, reply: oneshot::Sender<bytes::Bytes>) -> Inbound {
    Inbound::FetchSnapshot {
        req,
        version: FETCH_SNAPSHOT_VERSION,
        reply,
    }
}

/// Node 1 of voters 1, 2 and 3, following node 2 in epoch 5.
fn follower_of_2_in_epoch_5() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().call();
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });
    EngineInDir { engine, dir }
}

fn vote_request(edit: impl FnOnce(&mut VoteRequest)) -> VoteRequest {
    let mut request = VoteRequest {
        voter_id: 1,
        topics: vec![vote_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![vote_req::PartitionData {
                partition_index: 0,
                replica_epoch: 6,
                replica_id: 3,
                last_offset_epoch: 0,
                last_offset: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn vote_partition(request: &mut VoteRequest) -> &mut vote_req::PartitionData {
    &mut request.topics[0].partitions[0]
}

/// A response that names the metadata partition and node 2 at epoch 5.
fn vote_answer(error_code: i16) -> VoteResponse {
    vote_answer_with_leader(error_code, 2, 5, vec![vote_answer_node()])
}

fn vote_answer_node() -> vote_resp::NodeEndpoint {
    vote_resp::NodeEndpoint {
        node_id: 2,
        host: "127.0.0.1".into(),
        port: 9_093,
        ..Default::default()
    }
}

fn vote_answer_with_leader(
    error_code: i16,
    leader_id: i32,
    leader_epoch: i32,
    node_endpoints: Vec<vote_resp::NodeEndpoint>,
) -> VoteResponse {
    VoteResponse {
        topics: vec![vote_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![vote_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id,
                leader_epoch,
                vote_granted: false,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints,
        ..Default::default()
    }
}

/// `vote_answer`, but for a vote the follower of node 2 granted.
fn vote_answer_granted() -> VoteResponse {
    let mut answer = vote_answer(0);
    answer.topics[0].partitions[0].vote_granted = true;
    answer
}

/// The refusal of a request from epoch 6: the follower has stepped down to that
/// epoch before it looked at the request's voter key, so it names no leader.
fn vote_answer_after_stepping_down_to_epoch_6(error_code: i16) -> VoteResponse {
    vote_answer_with_leader(error_code, -1, 6, Vec::new())
}

fn engine_with_local_directory() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().call();
    let mut voters = voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
    let mut local = voters.get(NodeId(1)).expect("local voter").clone();
    local.directory_id = Uuid::from_u128(11);
    voters = VoterSet::from_voters(
        voters
            .ids()
            .into_iter()
            .filter(|id| *id != NodeId(1))
            .filter_map(|id| voters.get(id).cloned())
            .chain([local]),
    );
    engine.core = QuorumStateMachine::new(
        NodeId(1),
        QuorumState::bootstrap(Uuid::nil(), voters),
        engine.election_timeout,
    );
    EngineInDir { engine, dir }
}

#[tokio::test]
async fn vote_runs_kafka_request_checks() {
    struct Row {
        label: &'static str,
        request: VoteRequest,
        expected: VoteResponse,
    }

    let top_level = |error_code| VoteResponse {
        error_code,
        ..Default::default()
    };
    let rows = vec![
        Row {
            label: "foreign cluster id",
            request: vote_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            expected: top_level(104),
        },
        Row {
            label: "cluster id that does not parse",
            request: vote_request(|r| r.cluster_id = Some("not-a-cluster".into())),
            expected: top_level(104),
        },
        Row {
            label: "another topic",
            request: vote_request(|r| r.topics[0].topic_name = "other".into()),
            expected: top_level(42),
        },
        Row {
            label: "partition 1",
            request: vote_request(|r| vote_partition(r).partition_index = 1),
            expected: top_level(42),
        },
        Row {
            label: "two partitions",
            request: vote_request(|r| {
                let second = r.topics[0].partitions[0].clone();
                r.topics[0].partitions.push(second);
            }),
            expected: top_level(42),
        },
        Row {
            label: "two topics",
            request: vote_request(|r| {
                let second = r.topics[0].clone();
                r.topics.push(second);
            }),
            expected: top_level(42),
        },
        Row {
            label: "negative last offset",
            request: vote_request(|r| vote_partition(r).last_offset = -1),
            expected: vote_answer(42),
        },
        Row {
            label: "negative last epoch",
            request: vote_request(|r| vote_partition(r).last_offset_epoch = -1),
            expected: vote_answer(42),
        },
        Row {
            label: "standard vote whose last epoch is the replica epoch",
            request: vote_request(|r| vote_partition(r).last_offset_epoch = 6),
            expected: vote_answer(42),
        },
        Row {
            label: "pre-vote whose last epoch is above the replica epoch",
            request: vote_request(|r| {
                vote_partition(r).pre_vote = true;
                vote_partition(r).last_offset_epoch = 7;
            }),
            expected: vote_answer(42),
        },
        Row {
            label: "replica epoch below the local epoch",
            request: vote_request(|r| {
                vote_partition(r).replica_epoch = 4;
                vote_partition(r).pre_vote = true;
            }),
            expected: vote_answer(74),
        },
        Row {
            label: "negative replica id",
            request: vote_request(|r| vote_partition(r).replica_id = -1),
            expected: vote_answer(42),
        },
        Row {
            label: "voter key names another replica, from a higher epoch",
            request: vote_request(|r| r.voter_id = 2),
            expected: vote_answer_after_stepping_down_to_epoch_6(125),
        },
        Row {
            label: "voter key names replica 0 which is not local, from a higher epoch",
            request: vote_request(|r| r.voter_id = 0),
            expected: vote_answer_after_stepping_down_to_epoch_6(125),
        },
        Row {
            label: "voter key names another replica, at the local epoch",
            request: vote_request(|r| {
                r.voter_id = 2;
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            expected: vote_answer(125),
        },
        Row {
            label: "pre-vote from node 0 is valid",
            request: vote_request(|r| {
                vote_partition(r).replica_id = 0;
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            expected: vote_answer(0),
        },
        Row {
            label: "pre-vote to a follower that has not fetched from its leader",
            request: vote_request(|r| {
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            expected: vote_answer_granted(),
        },
        Row {
            label: "pre-vote with no voter key",
            request: vote_request(|r| {
                r.voter_id = -1;
                vote_partition(r).pre_vote = true;
                vote_partition(r).replica_epoch = 5;
            }),
            expected: vote_answer_granted(),
        },
        Row {
            label: "standard vote at the local epoch while a leader is known",
            request: vote_request(|r| vote_partition(r).replica_epoch = 5),
            expected: vote_answer(0),
        },
    ];
    for Row {
        label,
        request,
        expected,
    } in rows
    {
        let EngineInDir {
            mut engine,
            dir: _dir,
        } = follower_of_2_in_epoch_5();
        let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");
        let response = VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode");
        check!(response == expected, "{label}");
    }
}

/// A granted standard vote moves the voter to the candidate's epoch and names
/// no leader.
#[tokio::test]
async fn vote_grant_names_the_new_epoch_and_no_leader() {
    let (mut engine, _dir) = build_engine_only().call();
    let request = vote_request(|r| vote_partition(r).replica_epoch = 1);

    let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");

    check!(
        VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode")
            == VoteResponse {
                topics: vec![vote_resp::TopicData {
                    topic_name: METADATA_TOPIC.into(),
                    partitions: vec![vote_resp::PartitionData {
                        partition_index: 0,
                        leader_id: -1,
                        leader_epoch: 1,
                        vote_granted: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}

/// Kafka's `isValidVoterKey` compares the directory id when both sides have
/// one.
#[tokio::test]
async fn vote_refuses_a_voter_key_with_another_directory_id() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = engine_with_local_directory();

    for (label, directory_id, want_error) in [
        ("another directory", Uuid::from_u128(12), 125),
        ("the local directory", Uuid::from_u128(11), 0),
        ("no directory", Uuid::nil(), 0),
    ] {
        let request = vote_request(|r| {
            vote_partition(r).voter_directory_id = WireUuid(*directory_id.as_bytes());
            vote_partition(r).pre_vote = true;
            vote_partition(r).replica_epoch = 0;
        });
        let body = deliver(&mut engine, vote, encode(&request, VOTE_VERSION)).expect("an answer");
        let response = VoteResponse::decode(&mut &body[..], VOTE_VERSION).expect("decode");
        check!(
            response.topics[0].partitions[0].error_code == want_error,
            "{label}"
        );
    }
}

#[tokio::test]
async fn a_vote_body_that_does_not_decode_gets_no_answer() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
    let body = encode(&vote_request(|_| {}), VOTE_VERSION);
    check!(deliver(&mut engine, vote, body.slice(..body.len() - 1)).is_none());
}

fn begin_request(edit: impl FnOnce(&mut BeginQuorumEpochRequest)) -> BeginQuorumEpochRequest {
    let mut request = BeginQuorumEpochRequest {
        voter_id: -1,
        topics: vec![bqe_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![bqe_req::PartitionData {
                partition_index: 0,
                leader_id: 3,
                leader_epoch: 6,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn begin_answer(error_code: i16, leader_id: i32, leader_epoch: i32) -> BeginQuorumEpochResponse {
    BeginQuorumEpochResponse {
        topics: vec![bqe_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![bqe_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id,
                leader_epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: vec![bqe_resp::NodeEndpoint {
            node_id: leader_id,
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn begin_quorum_epoch_runs_kafka_request_checks() {
    struct Row {
        label: &'static str,
        request: BeginQuorumEpochRequest,
        expected: BeginQuorumEpochResponse,
    }

    let top_level = |error_code| BeginQuorumEpochResponse {
        error_code,
        ..Default::default()
    };
    let rows = vec![
        Row {
            label: "foreign cluster id",
            request: begin_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            expected: top_level(104),
        },
        Row {
            label: "another topic",
            request: begin_request(|r| r.topics[0].topic_name = "other".into()),
            expected: top_level(42),
        },
        Row {
            label: "two partitions",
            request: begin_request(|r| {
                let dup_part = r.topics[0].partitions[0].clone();
                r.topics[0].partitions.push(dup_part);
            }),
            expected: top_level(42),
        },
        Row {
            label: "leader epoch below the local epoch",
            request: begin_request(|r| r.topics[0].partitions[0].leader_epoch = 4),
            expected: begin_answer(74, 2, 5),
        },
        Row {
            label: "negative leader id",
            request: begin_request(|r| r.topics[0].partitions[0].leader_id = -1),
            expected: begin_answer(42, 2, 5),
        },
        Row {
            label: "voter key names another replica, after the transition",
            request: begin_request(|r| r.voter_id = 2),
            expected: begin_answer(125, 3, 6),
        },
        Row {
            label: "a new leader",
            request: begin_request(|_| {}),
            expected: begin_answer(0, 3, 6),
        },
    ];
    for Row {
        label,
        request,
        expected,
    } in rows
    {
        let EngineInDir {
            mut engine,
            dir: _dir,
        } = follower_of_2_in_epoch_5();
        let body =
            deliver(&mut engine, begin, encode(&request, QUORUM_EPOCH_VERSION)).expect("an answer");
        let response =
            BeginQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
        check!(response == expected, "{label}");
    }
}

#[tokio::test]
async fn a_begin_quorum_epoch_body_that_does_not_decode_gets_no_answer() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
    check!(deliver(&mut engine, begin, bytes::Bytes::from_static(&[0xff])).is_none());
}

fn end_request(edit: impl FnOnce(&mut EndQuorumEpochRequest)) -> EndQuorumEpochRequest {
    let mut request = EndQuorumEpochRequest {
        topics: vec![eqe_req::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![eqe_req::PartitionData {
                partition_index: 0,
                leader_id: 2,
                leader_epoch: 5,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    edit(&mut request);
    request
}

fn candidates(ids: &[i32]) -> Vec<eqe_req::ReplicaInfo> {
    ids.iter()
        .map(|&candidate_id| eqe_req::ReplicaInfo {
            candidate_id,
            ..Default::default()
        })
        .collect()
}

#[tokio::test]
async fn end_quorum_epoch_runs_kafka_request_checks() {
    struct Row {
        label: &'static str,
        request: EndQuorumEpochRequest,
        expected: EndQuorumEpochResponse,
        role: &'static str,
    }

    let top_level = |error_code| EndQuorumEpochResponse {
        error_code,
        ..Default::default()
    };
    let answer = |error_code, leader_id: i32| EndQuorumEpochResponse {
        topics: vec![eqe_resp::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![eqe_resp::PartitionData {
                partition_index: 0,
                error_code,
                leader_id,
                leader_epoch: 5,
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: (leader_id >= 0)
            .then(|| eqe_resp::NodeEndpoint {
                node_id: leader_id,
                host: "127.0.0.1".into(),
                port: 9_093,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    };
    // (label, request, response, the role name after the request)
    let rows = vec![
        Row {
            label: "foreign cluster id",
            request: end_request(|r| r.cluster_id = Some(foreign_cluster_id())),
            expected: top_level(104),
            role: "Follower",
        },
        Row {
            label: "partition 1",
            request: end_request(|r| r.topics[0].partitions[0].partition_index = 1),
            expected: top_level(42),
            role: "Follower",
        },
        Row {
            label: "leader epoch below the local epoch",
            request: end_request(|r| r.topics[0].partitions[0].leader_epoch = 4),
            expected: answer(74, 2),
            role: "Follower",
        },
        Row {
            label: "negative leader id",
            request: end_request(|r| r.topics[0].partitions[0].leader_id = -1),
            expected: answer(42, 2),
            role: "Follower",
        },
        Row {
            label: "first preferred candidate elects at once",
            request: end_request(|r| {
                r.topics[0].partitions[0].preferred_candidates = candidates(&[1, 3]);
            }),
            expected: answer(0, -1),
            role: "Prospective",
        },
        Row {
            label: "no preferred candidates elects at once",
            request: end_request(|_| {}),
            expected: answer(0, -1),
            role: "Prospective",
        },
        Row {
            label: "second preferred candidate waits",
            request: end_request(|r| {
                r.topics[0].partitions[0].preferred_candidates = candidates(&[3, 1]);
            }),
            expected: answer(0, -1),
            role: "Unattached",
        },
        Row {
            label: "a replica the list does not name waits",
            request: end_request(|r| {
                r.topics[0].partitions[0].preferred_candidates = candidates(&[3]);
            }),
            expected: answer(0, -1),
            role: "Unattached",
        },
    ];
    for Row {
        label,
        request,
        expected,
        role,
    } in rows
    {
        let EngineInDir {
            mut engine,
            dir: _dir,
        } = follower_of_2_in_epoch_5();
        let body =
            deliver(&mut engine, end, encode(&request, QUORUM_EPOCH_VERSION)).expect("an answer");
        let response =
            EndQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
        check!(response == expected, "{label}");
        check!(engine.core.role().name() == role, "{label}");
    }
}

fn fetch_snapshot_request(
    edit: impl FnOnce(&mut FetchSnapshotRequest, &mut fs_req::PartitionSnapshot),
) -> FetchSnapshotRequest {
    let mut partition = fs_req::PartitionSnapshot {
        partition: 0,
        current_leader_epoch: 1,
        snapshot_id: fs_req::SnapshotId {
            end_offset: 10,
            epoch: 1,
            ..Default::default()
        },
        position: 0,
        ..Default::default()
    };
    let mut request = FetchSnapshotRequest {
        replica_id: 2,
        max_bytes: 4,
        ..Default::default()
    };
    edit(&mut request, &mut partition);
    if request.topics.is_empty() {
        request.topics = vec![fs_req::TopicSnapshot {
            name: METADATA_TOPIC.into(),
            partitions: vec![partition],
            ..Default::default()
        }];
    }
    request
}

/// The topic and partition index that an answer names.
#[derive(Clone, Copy)]
struct AnsweredPartition<'a> {
    topic: &'a str,
    index: i32,
}

/// The leader id and epoch that an answer names.
#[derive(Clone, Copy)]
struct AnsweredLeader {
    id: i32,
    epoch: i32,
}

/// The chunk that an answer carries: the snapshot's size, the chunk's
/// position and its bytes.
#[derive(Clone, Copy)]
struct AnsweredChunk {
    size: i64,
    position: i64,
    bytes: &'static [u8],
}

/// An answer that names `partition`, with the leader view of `leader` when it
/// is `Some`, and node 1's endpoint.
fn fetch_snapshot_answer(
    AnsweredPartition { topic, index }: AnsweredPartition<'_>,
    error_code: i16,
    leader: Option<AnsweredLeader>,
    chunk: Option<AnsweredChunk>,
) -> FetchSnapshotResponse {
    let mut partition = fs_resp::PartitionSnapshot {
        index,
        error_code,
        ..Default::default()
    };
    if let Some(AnsweredLeader {
        id: leader_id,
        epoch: leader_epoch,
    }) = leader
    {
        partition.current_leader = fs_resp::LeaderIdAndEpoch {
            leader_id,
            leader_epoch,
            ..Default::default()
        };
    }
    if let Some(AnsweredChunk {
        size,
        position,
        bytes,
    }) = chunk
    {
        partition.snapshot_id = fs_resp::SnapshotId {
            end_offset: 10,
            epoch: 1,
            ..Default::default()
        };
        partition.size = size;
        partition.position = position;
        partition.unaligned_records =
            krabka_protocol::records::RecordsPayload::Raw(bytes::Bytes::from_static(bytes));
    }
    FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: topic.into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        node_endpoints: vec![fs_resp::NodeEndpoint {
            node_id: leader.map_or(1, |leader| leader.id),
            host: "127.0.0.1".into(),
            port: 9_093,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Node 1, the only voter, leading epoch 1, with checkpoints (10, 1) and the
/// bootstrap id (0, 0) on disk.
fn single_voter_leader_with_checkpoints() -> EngineInDir {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(1)]).call();
    engine.on_event(Event::ElectionTimeout);
    assert2::assert!(
        (
            engine.core.role().is_leader(),
            engine.core.quorum_state().leader_epoch
        ) == (true, 1)
    );
    let checkpoints = engine.data_dir.clone();
    checkpoint::write_checkpoint(&checkpoints, 10, 1, b"0123456789").expect("checkpoint 10/1");
    checkpoint::write_checkpoint(&checkpoints, 0, 0, b"bootstrap").expect("checkpoint 0/0");
    EngineInDir { engine, dir }
}

#[tokio::test]
async fn fetch_snapshot_runs_kafka_request_checks() {
    struct Row {
        label: &'static str,
        request: FetchSnapshotRequest,
        expected: FetchSnapshotResponse,
    }

    let top_level = |error_code| FetchSnapshotResponse {
        error_code,
        ..Default::default()
    };
    let metadata = AnsweredPartition {
        topic: METADATA_TOPIC,
        index: 0,
    };
    let leader = Some(AnsweredLeader { id: 1, epoch: 1 });
    let rows = vec![
        Row {
            label: "foreign cluster id",
            request: fetch_snapshot_request(|r, _| r.cluster_id = Some(foreign_cluster_id())),
            expected: top_level(104),
        },
        Row {
            label: "two partitions",
            request: fetch_snapshot_request(|r, p| {
                r.topics = vec![fs_req::TopicSnapshot {
                    name: METADATA_TOPIC.into(),
                    partitions: vec![p.clone(), p.clone()],
                    ..Default::default()
                }];
            }),
            expected: top_level(42),
        },
        Row {
            label: "another topic",
            request: fetch_snapshot_request(|r, p| {
                r.topics = vec![fs_req::TopicSnapshot {
                    name: "other".into(),
                    partitions: vec![p.clone()],
                    ..Default::default()
                }];
            }),
            expected: fetch_snapshot_answer(
                AnsweredPartition {
                    topic: "other",
                    index: 0,
                },
                3,
                None,
                None,
            ),
        },
        Row {
            label: "partition 1",
            request: fetch_snapshot_request(|_, p| p.partition = 1),
            expected: fetch_snapshot_answer(
                AnsweredPartition {
                    topic: METADATA_TOPIC,
                    index: 1,
                },
                3,
                None,
                None,
            ),
        },
        Row {
            label: "leader epoch below the local epoch",
            request: fetch_snapshot_request(|_, p| p.current_leader_epoch = 0),
            expected: fetch_snapshot_answer(metadata, 74, leader, None),
        },
        Row {
            label: "leader epoch above the local epoch",
            request: fetch_snapshot_request(|_, p| p.current_leader_epoch = 2),
            expected: fetch_snapshot_answer(metadata, 75, leader, None),
        },
        Row {
            label: "unknown snapshot id",
            request: fetch_snapshot_request(|_, p| p.snapshot_id.end_offset = 11),
            expected: fetch_snapshot_answer(metadata, 98, leader, None),
        },
        Row {
            label: "bootstrap snapshot id",
            request: fetch_snapshot_request(|_, p| {
                p.snapshot_id.end_offset = 0;
                p.snapshot_id.epoch = 0;
            }),
            expected: fetch_snapshot_answer(metadata, 98, leader, None),
        },
        Row {
            label: "negative position",
            request: fetch_snapshot_request(|_, p| p.position = -1),
            expected: fetch_snapshot_answer(metadata, 99, leader, None),
        },
        Row {
            label: "position at the size",
            request: fetch_snapshot_request(|_, p| p.position = 10),
            expected: fetch_snapshot_answer(metadata, 99, leader, None),
        },
        Row {
            label: "last byte",
            request: fetch_snapshot_request(|_, p| p.position = 9),
            expected: fetch_snapshot_answer(
                metadata,
                0,
                leader,
                Some(AnsweredChunk {
                    size: 10,
                    position: 9,
                    bytes: b"9",
                }),
            ),
        },
        Row {
            label: "first chunk",
            request: fetch_snapshot_request(|_, _| {}),
            expected: fetch_snapshot_answer(
                metadata,
                0,
                leader,
                Some(AnsweredChunk {
                    size: 10,
                    position: 0,
                    bytes: b"0123",
                }),
            ),
        },
    ];
    for Row {
        label,
        request,
        expected,
    } in rows
    {
        let EngineInDir {
            mut engine,
            dir: _dir,
        } = single_voter_leader_with_checkpoints();
        let body = deliver(
            &mut engine,
            fetch_snapshot,
            encode(&request, FETCH_SNAPSHOT_VERSION),
        )
        .expect("an answer");
        check!(body == encode(&expected, FETCH_SNAPSHOT_VERSION), "{label}");
    }
}

/// A follower refuses `FetchSnapshot` at its own epoch and names the leader.
#[tokio::test]
async fn fetch_snapshot_to_a_follower_names_the_leader() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = follower_of_2_in_epoch_5();
    let request = fetch_snapshot_request(|_, p| p.current_leader_epoch = 5);

    let body = deliver(
        &mut engine,
        fetch_snapshot,
        encode(&request, FETCH_SNAPSHOT_VERSION),
    )
    .expect("an answer");

    check!(
        body == encode(
            &fetch_snapshot_answer(
                AnsweredPartition {
                    topic: METADATA_TOPIC,
                    index: 0,
                },
                6,
                Some(AnsweredLeader { id: 2, epoch: 5 }),
                None,
            ),
            FETCH_SNAPSHOT_VERSION
        )
    );
}

/// A leader that only the adjacent voter set of an uncommitted KIP-853 change
/// names is followed, and its endpoint still goes into `NodeEndpoints`.
#[tokio::test]
async fn a_leader_from_the_adjacent_voter_set_keeps_its_endpoint() {
    let (mut engine, _dir) = build_engine_only().call();
    let _ = engine
        .core
        .apply_voter_set(voter_set(&[NodeId(1), NodeId(2)]), SimInstant(0));

    let body = deliver(
        &mut engine,
        begin,
        encode(&begin_request(|_| {}), QUORUM_EPOCH_VERSION),
    )
    .expect("an answer");

    check!(
        BeginQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode")
            == begin_answer(0, 3, 6)
    );
}

#[tokio::test]
async fn end_quorum_epoch_matches_candidate_directory_id() {
    let EngineInDir {
        mut engine,
        dir: _dir,
    } = engine_with_local_directory();
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });

    // Candidate directory matches local directory -> ranks 0, elects at once (kills `!=`)
    let req = end_request(|r| {
        r.topics[0].partitions[0].preferred_candidates = vec![eqe_req::ReplicaInfo {
            candidate_id: 1,
            candidate_directory_id: WireUuid(*Uuid::from_u128(11).as_bytes()),
            ..Default::default()
        }];
    });
    let body = deliver(&mut engine, end, encode(&req, QUORUM_EPOCH_VERSION)).expect("an answer");
    let response =
        EndQuorumEpochResponse::decode(&mut &body[..], QUORUM_EPOCH_VERSION).expect("decode");
    check!(response.error_code == 0);
    check!(engine.core.role().name() == "Prospective");

    // Reset back to follower
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    });

    // Candidate directory is nil WireUuid -> ranks 0, elects at once (kills `&&`)
    let req2 = end_request(|r| {
        r.topics[0].partitions[0].preferred_candidates = vec![eqe_req::ReplicaInfo {
            candidate_id: 1,
            candidate_directory_id: WireUuid(*Uuid::nil().as_bytes()),
            ..Default::default()
        }];
    });
    let body2 = deliver(&mut engine, end, encode(&req2, QUORUM_EPOCH_VERSION)).expect("an answer");
    let response2 =
        EndQuorumEpochResponse::decode(&mut &body2[..], QUORUM_EPOCH_VERSION).expect("decode");
    check!(response2.error_code == 0);
    check!(engine.core.role().name() == "Prospective");
}
