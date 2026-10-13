use {
    super::*,
    crate::{kraft::transport::api_key::END_QUORUM_EPOCH, network::negotiation::convert_request},
    assert2::{assert, check},
    base64::Engine as _,
};

fn raw_vote_request() -> VoteRequest {
    VoteRequest {
        voter_id: 1,
        topics: vec![vote_req::TopicData {
            topic_name: METADATA_TOPIC.to_string(),
            partitions: vec![vote_req::PartitionData {
                partition_index: METADATA_PARTITION,
                replica_epoch: 3,
                replica_id: 2,
                last_offset_epoch: 2,
                last_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn raw_vote_body(request: &VoteRequest) -> Bytes { encode_body(request, VOTE_VERSION) }

fn candidate_vote(
    cluster_id: Option<uuid::Uuid>,
    directories: (uuid::Uuid, uuid::Uuid),
) -> PeerRequest {
    PeerRequest::Vote {
        cluster_id,
        voter_id: NodeId(9),
        voter_directory_id: directories.0,
        candidate_epoch: 3,
        candidate: NodeId(7),
        candidate_directory_id: directories.1,
        last_epoch: 2,
        last_offset: 42,
        pre_vote: true,
    }
}

fn begin_request() -> PeerRequest {
    PeerRequest::BeginQuorumEpoch {
        cluster_id: Some(uuid::Uuid::from_u128(9)),
        voter_id: NodeId(2),
        voter_directory_id: uuid::Uuid::from_u128(2),
        leader_id: NodeId(5),
        leader_epoch: 9,
        leader_endpoints: vec![("CONTROLLER".into(), "c5".into(), 9093)],
    }
}

#[test]
fn vote_request_round_trips() {
    let cluster_id = uuid::Uuid::from_u128(1);
    let req = candidate_vote(
        Some(cluster_id),
        (uuid::Uuid::from_u128(2), uuid::Uuid::from_u128(3)),
    );
    let encoded = req.encode();
    assert2::assert!(decode_vote(&encoded) == Some(req));

    let mut cur = &encoded[..];
    let raw = VoteRequest::decode(&mut cur, VOTE_VERSION).expect("decode vote request");
    assert2::assert!(
        raw.cluster_id
            == Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()))
    );
}

#[test]
fn vote_request_preserves_legitimate_node_zero() {
    let req = PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(0),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch: 0,
        candidate: NodeId(0),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch: 0,
        last_offset: 0,
        pre_vote: false,
    };
    assert2::assert!(decode_vote(&req.encode()) == Some(req));
}

#[test]
fn vote_decode_accepts_kafka_base64_cluster_id() {
    let cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    let mut request = raw_vote_request();
    request.cluster_id =
        Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()));

    let Some(PeerRequest::Vote {
        cluster_id: decoded,
        ..
    }) = decode_vote(&raw_vote_body(&request))
    else {
        panic!("valid Kafka cluster id must decode");
    };
    assert2::assert!(decoded == Some(cluster_id));
}

#[test]
fn vote_encode_rejects_values_above_signed_wire_maximum() {
    let max_id = i32::MAX as u64;
    let max_epoch = i32::MAX as u32;
    let request = |voter_id, candidate, candidate_epoch, last_epoch| PeerRequest::Vote {
        cluster_id: None,
        voter_id: NodeId(voter_id),
        voter_directory_id: uuid::Uuid::nil(),
        candidate_epoch,
        candidate: NodeId(candidate),
        candidate_directory_id: uuid::Uuid::nil(),
        last_epoch,
        last_offset: 0,
        pre_vote: false,
    };

    assert2::assert!(
        request(max_id, max_id, max_epoch, max_epoch)
            .try_encode()
            .is_some()
    );
    assert2::assert!(request(max_id + 1, 0, 0, 0).try_encode().is_none());
    assert2::assert!(request(0, max_id + 1, 0, 0).try_encode().is_none());
    assert2::assert!(request(0, 0, max_epoch + 1, 0).try_encode().is_none());
    assert2::assert!(request(0, 0, 0, max_epoch + 1).try_encode().is_none());
}

#[test]
fn vote_decode_rejects_negative_ids() {
    let mut request = raw_vote_request();
    request.voter_id = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].replica_id = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_negative_epochs() {
    let mut request = raw_vote_request();
    request.topics[0].partitions[0].replica_epoch = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].last_offset_epoch = -1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_wrong_topic_or_partition() {
    let mut request = raw_vote_request();
    request.topics[0].topic_name = "other".to_string();
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());

    let mut request = raw_vote_request();
    request.topics[0].partitions[0].partition_index = 1;
    assert2::assert!(decode_vote(&raw_vote_body(&request)).is_none());
}

#[test]
fn vote_decode_rejects_trailing_bytes() {
    let mut body = raw_vote_body(&raw_vote_request()).to_vec();
    body.push(0);
    assert2::assert!(decode_vote(&body).is_none());
}

#[test]
fn generic_request_decode_accepts_vote_request() {
    let req = candidate_vote(None, (uuid::Uuid::nil(), uuid::Uuid::nil()));
    assert2::assert!(PeerRequest::decode(&req.encode()) == Some(req));
}

#[test]
fn encoded_vote_request_carries_target_voter_and_empty_cluster_id() {
    use krabka_protocol::Decode;

    let req = candidate_vote(None, (uuid::Uuid::nil(), uuid::Uuid::nil()));
    let mut cur = &req.encode()[..];
    let raw = VoteRequest::decode(&mut cur, VOTE_VERSION).expect("decode vote request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.cluster_id.as_ref(),
            raw.voter_id,
            partition.replica_epoch,
            partition.replica_id,
            partition.last_offset_epoch,
            partition.last_offset,
            partition.pre_vote,
        ) == (None, 9, 3, 7, 2, 42, true)
    );
}

#[test]
fn begin_end_round_trip() {
    let begin = begin_request();
    assert2::assert!(decode_begin(&begin.encode()) == Some(begin));
    let end = PeerRequest::EndQuorumEpoch {
        cluster_id: Some(uuid::Uuid::from_u128(9)),
        leader_id: NodeId(1),
        leader_epoch: 4,
        preferred_candidates: vec![
            (NodeId(3), uuid::Uuid::from_u128(3)),
            (NodeId(2), uuid::Uuid::from_u128(2)),
        ],
    };
    assert2::assert!(decode_end(&end.encode()) == Some(end));
}

/// The requests are Kafka's `RaftUtil.singletonBeginQuorumEpochRequest` and
/// `singletonEndQuorumEpochRequest`: the cluster id, the recipient voter key
/// and the leader's endpoints for `BeginQuorumEpoch`, and the successors in
/// both of `EndQuorumEpoch`'s lists.
#[test]
fn encoded_begin_and_end_requests_are_kafkas_singleton_requests() {
    use krabka_protocol::{
        Decode,
        owned::{begin_quorum_epoch_request as bqe, end_quorum_epoch_request as eqe},
        primitives::uuid::Uuid as WireUuid,
    };

    let begin = begin_request();
    let raw_begin = BeginQuorumEpochRequest::decode(&mut &begin.encode()[..], QUORUM_EPOCH_VERSION)
        .expect("decode begin request");
    let expected_begin = BeginQuorumEpochRequest {
        cluster_id: Some("AAAAAAAAAAAAAAAAAAAACQ".into()),
        voter_id: 2,
        topics: vec![bqe::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![bqe::PartitionData {
                partition_index: 0,
                voter_directory_id: WireUuid(*uuid::Uuid::from_u128(2).as_bytes()),
                leader_id: 5,
                leader_epoch: 9,
                ..Default::default()
            }],
            ..Default::default()
        }],
        leader_endpoints: vec![bqe::LeaderEndpoint {
            name: "CONTROLLER".into(),
            host: "c5".into(),
            port: 9093,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert2::check!(raw_begin == expected_begin);

    let end = PeerRequest::EndQuorumEpoch {
        cluster_id: Some(uuid::Uuid::from_u128(9)),
        leader_id: NodeId(1),
        leader_epoch: 4,
        preferred_candidates: vec![(NodeId(2), uuid::Uuid::from_u128(2))],
    };
    let expected_end = |version: i16| EndQuorumEpochRequest {
        cluster_id: Some("AAAAAAAAAAAAAAAAAAAACQ".into()),
        topics: vec![eqe::TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![eqe::PartitionData {
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 4,
                preferred_successors: if version == 0 { vec![2] } else { Vec::new() },
                preferred_candidates: if version == 0 {
                    Vec::new()
                } else {
                    vec![eqe::ReplicaInfo {
                        candidate_id: 2,
                        candidate_directory_id: WireUuid(*uuid::Uuid::from_u128(2).as_bytes()),
                        ..Default::default()
                    }]
                },
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let raw_end = EndQuorumEpochRequest::decode(&mut &end.encode()[..], QUORUM_EPOCH_VERSION)
        .expect("decode end request");
    assert2::check!(raw_end == expected_end(QUORUM_EPOCH_VERSION));
    // At v0 the successors travel in `PreferredSuccessors`.
    let v0 = convert_request(END_QUORUM_EPOCH, &end.encode(), QUORUM_EPOCH_VERSION, 0)
        .expect("convert to v0");
    let raw_v0 = EndQuorumEpochRequest::decode(&mut &v0[..], 0).expect("decode v0");
    assert2::check!(raw_v0 == expected_end(0));
}

#[test]
fn fetch_request_round_trips() {
    let req = PeerRequest::Fetch {
        cluster_id: None,
        max_wait_ms: 0,
        high_watermark: -1,
        from: NodeId(2),
        current_leader_epoch: 3,
        fetch_epoch: 1,
        fetch_offset: 11,
        replica_directory_id: uuid::Uuid::from_u128(42),
    };
    assert2::assert!(decode_fetch(&req.encode()) == Some(req));
}

#[test]
fn encoded_fetch_request_carries_replica_state_epoch_sentinel() {
    use krabka_protocol::{Decode, owned::fetch_request::FetchRequest};

    let dir_id = uuid::Uuid::from_u128(42);
    let req = PeerRequest::Fetch {
        cluster_id: None,
        max_wait_ms: 0,
        high_watermark: -1,
        from: NodeId(2),
        current_leader_epoch: 4,
        fetch_epoch: 1,
        fetch_offset: 11,
        replica_directory_id: dir_id,
    };
    let mut cur = &req.encode()[..];
    let raw = FetchRequest::decode(&mut cur, FETCH_VERSION).expect("decode fetch request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.replica_state.replica_id,
            raw.replica_state.replica_epoch,
            partition.current_leader_epoch,
            partition.last_fetched_epoch,
            partition.fetch_offset,
            partition.replica_directory_id.0,
        ) == (2, -1, 4, 1, 11, *dir_id.as_bytes())
    );
}

fn sample_fetch_snapshot_request() -> PeerRequest {
    PeerRequest::FetchSnapshot {
        cluster_id: Some(uuid::Uuid::from_u128(9)),
        from: NodeId(2),
        current_leader_epoch: 7,
        snapshot_id: (42, 3),
        position: 128,
        max_bytes: 4096,
    }
}

#[test]
fn fetch_snapshot_request_round_trips() {
    let req = sample_fetch_snapshot_request();
    assert2::assert!(decode_fetch_snapshot(&req.encode()) == Some(req));
}

#[test]
fn encoded_fetch_snapshot_request_carries_cluster_id_and_current_leader_epoch() {
    use krabka_protocol::Decode;

    let req = sample_fetch_snapshot_request();
    let mut cur = &req.encode()[..];
    let raw = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION)
        .expect("decode fetch snapshot request");
    let partition = &raw.topics[0].partitions[0];
    check!(
        (
            raw.cluster_id.as_deref(),
            raw.replica_id,
            raw.max_bytes,
            partition.current_leader_epoch,
            partition.snapshot_id.end_offset,
            partition.snapshot_id.epoch,
            partition.position,
        ) == (Some("AAAAAAAAAAAAAAAAAAAACQ"), 2, 4096, 7, 42, 3, 128)
    );
}

/// The Fetch a replica sends is Kafka's `KafkaRaftClient.buildFetchRequest`:
/// the cluster id, `MaxWaitMs`, the replica id in `ReplicaState`, the quorum
/// epoch, the last fetched epoch, the fetch offset, the directory id and the
/// local high watermark.
#[test]
fn fetch_request_wire_encoding_fields() {
    use krabka_protocol::{
        Decode,
        owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        primitives::uuid::Uuid as WireUuid,
    };

    let req = PeerRequest::Fetch {
        cluster_id: Some(uuid::Uuid::from_u128(9)),
        max_wait_ms: 500,
        high_watermark: 8,
        from: NodeId(1),
        current_leader_epoch: 2,
        fetch_epoch: 2,
        fetch_offset: 10,
        replica_directory_id: uuid::Uuid::from_u128(123),
    };
    let bytes = req.try_encode().expect("encodes fetch request");
    let mut cur = &bytes[..];
    let decoded = FetchRequest::decode(&mut cur, FETCH_VERSION).expect("decodes fetch request");
    let expected = FetchRequest {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        cluster_id: Some("AAAAAAAAAAAAAAAAAAAACQ".into()),
        replica_state: ReplicaState {
            replica_id: 1,
            ..Default::default()
        },
        topics: vec![FetchTopic {
            topic_id: METADATA_TOPIC_ID,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 2,
                fetch_offset: 10,
                last_fetched_epoch: 2,
                replica_directory_id: WireUuid(*uuid::Uuid::from_u128(123).as_bytes()),
                high_watermark: 8,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    check!(cur.is_empty());
    check!(decoded == expected);
    check!(decode_fetch(&bytes) == Some(req));
}
