use {
    super::*,
    assert2::{assert, check},
};

/// Kafka's `NOT_LEADER_OR_FOLLOWER`.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

#[test]
fn vote_response_round_trips() {
    let resp = PeerResponse::Vote {
        epoch: 3,
        granted: true,
    };
    assert2::assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
}

#[test]
fn encoded_vote_response_carries_success_error_codes() {
    use krabka_protocol::Decode;

    let resp = PeerResponse::Vote {
        epoch: 3,
        granted: true,
    };
    let mut cur = &resp.encode()[..];
    let raw = VoteResponse::decode(&mut cur, VOTE_VERSION).expect("decode vote response");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.error_code,
            partition.partition_index,
            partition.error_code,
            partition.leader_epoch,
            partition.vote_granted,
        ) == (0, METADATA_PARTITION, 0, 3, true)
    );
}

#[test]
fn decodes_jvm_style_response_without_echo_tag() {
    // A real JVM `VoteResponse` is byte-faithful Kafka v2 with no Krabka
    // echo tag. Build one straight from the generated protocol type
    // (bypassing `PeerResponse::Vote::encode`) and confirm `decode_vote`
    // tolerates it — the regression guard for the removed
    // `PRE_VOTE_ECHO_TAG`.
    let resp = VoteResponse {
        error_code: 0,
        topics: vec![vote_resp::TopicData {
            topic_name: METADATA_TOPIC.to_string(),
            partitions: vec![vote_resp::PartitionData {
                partition_index: METADATA_PARTITION,
                error_code: 0,
                leader_id: -1,
                leader_epoch: epoch_to_wire(7),
                vote_granted: true,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let bytes = encode_body(&resp, VOTE_VERSION);
    let decoded = PeerResponse::decode_vote(&bytes).unwrap();
    assert2::assert!(
        decoded
            == PeerResponse::Vote {
                epoch: 7,
                granted: true
            }
    );
}

#[test]
fn ack_round_trips() {
    let resp = PeerResponse::Ack { epoch: 8 };
    assert2::assert!(PeerResponse::decode_ack(&resp.encode()) == Some(resp));
}

#[test]
fn encoded_ack_response_carries_success_error_codes() {
    use krabka_protocol::Decode;

    let resp = PeerResponse::Ack { epoch: 8 };
    let mut cur = &resp.encode()[..];
    let raw = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).expect("decode ack");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.error_code,
            partition.partition_index,
            partition.error_code,
            partition.leader_id,
            partition.leader_epoch,
        ) == (0, METADATA_PARTITION, 0, -1, 8)
    );
}

#[test]
fn fetch_snapshot_response_round_trips() {
    let resp = PeerResponse::FetchSnapshot {
        snapshot_id: (42, 3),
        size: 9,
        position: 0,
        bytes: Bytes::from_static(b"snapshotX"),
        error_code: 0,
    };
    assert2::assert!(PeerResponse::decode_fetch_snapshot(&resp.encode()) == Some(resp));
}

#[test]
fn fetch_snapshot_response_round_trips_error_code() {
    let resp = PeerResponse::FetchSnapshot {
        snapshot_id: (42, 3),
        size: 0,
        position: 0,
        bytes: Bytes::new(),
        error_code: 42,
    };
    assert2::assert!(PeerResponse::decode_fetch_snapshot(&resp.encode()) == Some(resp));
}

fn leader(leader_id: Option<u64>, epoch: Epoch, endpoint: Option<(&str, u16)>) -> QuorumLeader {
    QuorumLeader {
        leader_id: leader_id.map(NodeId),
        epoch,
        endpoint: endpoint.map(|(host, port)| (host.to_string(), port)),
    }
}

fn fetch_answer(error_code: i16, leader: QuorumLeader) -> FetchAnswer {
    FetchAnswer {
        error_code,
        leader,
        diverging: None,
        snapshot_id: None,
        hwm: 7,
        log_start_offset: 3,
        records: Bytes::new(),
    }
}

#[test]
fn fetch_response_round_trips() {
    let served = fetch_answer(0, leader(Some(2), 5, Some(("controller-2", 9093))));
    for (case, answer) in [
        ("served", served.clone()),
        ("records", FetchAnswer {
            records: Bytes::from_static(b"\x01\x02\x03"),
            ..served.clone()
        }),
        ("diverged", FetchAnswer {
            diverging: Some(LogOffsetMetadata {
                offset: 5,
                epoch: 1,
            }),
            ..served.clone()
        }),
        ("snapshot", FetchAnswer {
            snapshot_id: Some((42, 3)),
            ..served.clone()
        }),
        (
            "leader without an endpoint",
            fetch_answer(0, leader(Some(2), 5, None)),
        ),
        (
            "follower redirect to leader 0",
            fetch_answer(
                NOT_LEADER_OR_FOLLOWER,
                leader(Some(0), 5, Some(("controller-0", 9093))),
            ),
        ),
        (
            "unknown leader",
            fetch_answer(NOT_LEADER_OR_FOLLOWER, leader(None, 5, None)),
        ),
    ] {
        let response = PeerResponse::Fetch(answer);
        check!(
            PeerResponse::decode_fetch(&response.encode()) == Some(response),
            "{case}"
        );
    }
}

/// The bytes are those of Kafka's `KafkaRaftClient.buildEmptyFetchResponse`
/// on a follower of leader 2: the error, the leader and epoch, no high
/// watermark, the log start, empty non-null records and aborted
/// transactions, and the leader's `NodeEndpoints` entry.
#[test]
fn a_refused_fetch_is_kafkas_empty_fetch_response() {
    let kafka = |leader_id: i32, node_endpoints: Vec<fetch_resp::NodeEndpoint>| FetchResponse {
        responses: vec![fetch_resp::FetchableTopicResponse {
            topic: METADATA_TOPIC.to_string(),
            topic_id: METADATA_TOPIC_ID,
            partitions: vec![fetch_resp::PartitionData {
                partition_index: METADATA_PARTITION,
                error_code: NOT_LEADER_OR_FOLLOWER,
                high_watermark: -1,
                log_start_offset: 3,
                aborted_transactions: Some(Vec::new()),
                records: Some(RecordsPayload::Raw(Bytes::new())),
                current_leader: fetch_resp::LeaderIdAndEpoch {
                    leader_id,
                    leader_epoch: 5,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints,
        ..Default::default()
    };
    let refused = |leader| {
        PeerResponse::Fetch(FetchAnswer {
            hwm: -1,
            ..fetch_answer(NOT_LEADER_OR_FOLLOWER, leader)
        })
    };
    for (case, answer, expected) in [
        (
            "known leader",
            refused(leader(Some(2), 5, Some(("controller-2", 9093)))),
            kafka(2, vec![fetch_resp::NodeEndpoint {
                node_id: 2,
                host: "controller-2".into(),
                port: 9093,
                ..Default::default()
            }]),
        ),
        // `singletonFetchResponse` adds no entry without a leader id.
        (
            "unknown leader",
            refused(leader(None, 5, Some(("controller-2", 9093)))),
            kafka(-1, Vec::new()),
        ),
    ] {
        check!(
            answer.encode() == encode_body(&expected, FETCH_VERSION),
            "{case}"
        );
    }
}

/// Kafka's `Endpoints.fromFetchResponse` keeps only the entry for
/// `CurrentLeader.LeaderId`.
#[test]
fn a_node_endpoint_for_another_node_is_not_the_leaders() {
    let response = FetchResponse {
        responses: vec![fetch_resp::FetchableTopicResponse {
            topic: METADATA_TOPIC.to_string(),
            topic_id: METADATA_TOPIC_ID,
            partitions: vec![fetch_resp::PartitionData {
                error_code: NOT_LEADER_OR_FOLLOWER,
                current_leader: fetch_resp::LeaderIdAndEpoch {
                    leader_id: 2,
                    leader_epoch: 5,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        }],
        node_endpoints: vec![fetch_resp::NodeEndpoint {
            node_id: 9,
            host: "controller-9".into(),
            port: 9093,
            ..Default::default()
        }],
        ..Default::default()
    };
    check!(
        PeerResponse::decode_fetch(&encode_body(&response, FETCH_VERSION))
            == Some(PeerResponse::Fetch(FetchAnswer {
                error_code: NOT_LEADER_OR_FOLLOWER,
                leader: leader(Some(2), 5, None),
                diverging: None,
                snapshot_id: None,
                hwm: 0,
                log_start_offset: -1,
                records: Bytes::new(),
            }))
    );
}

#[test]
fn fetch_snapshot_answer_with_partition_preserves_top_level_error() {
    use krabka_protocol::{Decode, owned::fetch_snapshot_response::FetchSnapshotResponse};

    let partition = FetchSnapshotPartition {
        topic: METADATA_TOPIC.to_string(),
        index: METADATA_PARTITION,
        error_code: 0,
        current_leader: false,
        chunk: None,
    };
    let leader = QuorumLeader {
        leader_id: None,
        epoch: 0,
        endpoint: None,
    };
    let bytes = encode_fetch_snapshot_answer(7, Some(partition), &leader, FETCH_SNAPSHOT_VERSION);
    let mut cur = &bytes[..];
    let raw = FetchSnapshotResponse::decode(&mut cur, FETCH_SNAPSHOT_VERSION)
        .expect("decode FetchSnapshot");
    assert2::check!(raw.error_code == 7);
}
