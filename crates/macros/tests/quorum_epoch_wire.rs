//! Real Kafka schemas exercise shared quorum accessors and both encoder versions.

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request::{self as add_request, AddRaftVoterRequest},
            begin_quorum_epoch_request::{self as begin_request, BeginQuorumEpochRequest},
            begin_quorum_epoch_response::{self as begin_response, BeginQuorumEpochResponse},
            end_quorum_epoch_request::{self as end_request, EndQuorumEpochRequest},
            end_quorum_epoch_response::{self as end_response, EndQuorumEpochResponse},
            update_raft_voter_request::{self as update_request, UpdateRaftVoterRequest},
        },
    },
    std::cell::Cell,
};

const METADATA_TOPIC: &str = "__cluster_metadata";
const METADATA_PARTITION: i32 = 0;
struct RequestedTopic<'a> {
    name: &'a str,
    partitions: Vec<i32>,
}
struct RequestedLeader {
    id: i32,
    epoch: i32,
}
struct ListenerAddress<'a> {
    name: &'a str,
    host: &'a str,
    port: u16,
}
krabka_macros::quorum_epoch_requests! {
    trait QuorumEpochRequest;
    BeginQuorumEpochRequest, EndQuorumEpochRequest
}

#[derive(Clone)]
struct NodeEndpointEntry {
    node_id: i32,
    host: String,
    port: u16,
}
struct QuorumLeader {
    id: Option<i32>,
    epoch: u32,
    endpoint: Option<NodeEndpointEntry>,
}
impl QuorumLeader {
    fn leader_id_to_wire(&self) -> i32 { self.id.unwrap_or(-1) }

    fn node_endpoint(&self) -> Option<NodeEndpointEntry> {
        self.id.and_then(|_| self.endpoint.clone())
    }
}
fn epoch_to_wire(epoch: u32) -> i32 { i32::try_from(epoch).unwrap() }
fn encode_body(message: &impl Encode, version: i16) -> Bytes {
    let mut output = BytesMut::with_capacity(128);
    message.encode(&mut output, version).unwrap();
    output.into()
}

krabka_macros::quorum_epoch_encoder! {
    fn begin(top_level_error: i16, partition_error: i16, leader: &QuorumLeader, version: i16) -> Bytes
    [response = BeginQuorumEpochResponse, wire = begin_response]
    {}
}
krabka_macros::quorum_epoch_encoder! {
    fn end(top_level_error: i16, partition_error: i16, leader: &QuorumLeader, version: i16) -> Bytes
    [response = EndQuorumEpochResponse, wire = end_response]
    {}
}

#[test]
fn request_accessors_preserve_raw_values_borrowed_endpoints_and_topic_order() {
    let begin = BeginQuorumEpochRequest {
        cluster_id: Some("cluster".into()),
        topics: vec![
            begin_request::TopicData {
                topic_name: "first".into(),
                partitions: vec![
                    begin_request::PartitionData {
                        partition_index: -1,
                        leader_id: -3,
                        leader_epoch: -5,
                        ..Default::default()
                    },
                    begin_request::PartitionData {
                        partition_index: 4,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            begin_request::TopicData {
                topic_name: "second".into(),
                ..Default::default()
            },
        ],
        leader_endpoints: vec![begin_request::LeaderEndpoint {
            name: "CONTROLLER".into(),
            host: "host".into(),
            port: 9093,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(begin.cluster_id() == Some("cluster"));
    let mut topics = begin.topics();
    assert!(topics.len() == 2);
    let first = topics.next().unwrap();
    assert!(first.name == "first" && first.partitions == [-1, 4]);
    assert!(topics.next().unwrap().name == "second");
    let leader = begin.leader().unwrap();
    assert!(leader.id == -3 && leader.epoch == -5);
    let endpoint = begin.endpoints().next().unwrap();
    assert!(endpoint.name == "CONTROLLER" && endpoint.host == "host" && endpoint.port == 9093);

    let end = EndQuorumEpochRequest {
        topics: vec![end_request::TopicData {
            topic_name: "empty".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(end.cluster_id().is_none() && end.leader().is_none());
    assert!(end.topics().len() == 1 && end.endpoints().next().is_none());
    assert!(EndQuorumEpochRequest::default().leader().is_none());
}

#[yare::parameterized(v0 = { 0 }, v1 = { 1 })]
fn response_encoders_keep_partition_leader_layouts(version: i16) {
    let leader = QuorumLeader {
        id: Some(7),
        epoch: 9,
        endpoint: Some(NodeEndpointEntry {
            node_id: 7,
            host: "host".into(),
            port: 9093,
        }),
    };
    {
        let begin_bytes = begin(0, 42, &leader, version);
        let decoded = BeginQuorumEpochResponse::decode(&mut &begin_bytes[..], version).unwrap();
        let topic = &decoded.topics[0];
        let partition = &topic.partitions[0];
        assert2::assert!(decoded.error_code == 0 && topic.topic_name == METADATA_TOPIC);
        assert2::assert!(
            partition.partition_index == 0
                && partition.error_code == 42
                && partition.leader_id == 7
                && partition.leader_epoch == 9
        );
        assert2::assert!(decoded.node_endpoints.len() == usize::from(version == 1));
        let end_bytes = end(0, 42, &leader, version);
        let decoded = EndQuorumEpochResponse::decode(&mut &end_bytes[..], version).unwrap();
        assert2::assert!(decoded.topics[0].partitions[0].leader_id == 7);
        assert2::assert!(decoded.node_endpoints.len() == usize::from(version == 1));
        let error_bytes = begin(31, 42, &leader, version);
        let error = BeginQuorumEpochResponse::decode(&mut &error_bytes[..], version).unwrap();
        assert2::assert!(
            error.error_code == 31 && error.topics.is_empty() && error.node_endpoints.is_empty()
        );
    }
}

#[test]
fn response_encoders_keep_bare_errors() {
    let leader = QuorumLeader {
        id: Some(7),
        epoch: 9,
        endpoint: Some(NodeEndpointEntry {
            node_id: 7,
            host: "host".into(),
            port: 9093,
        }),
    };
    let bare = b"\x00\x1f\x00\x00\x00\x00";
    assert!(begin(31, 42, &leader, 0).as_ref() == bare);
    assert!(end(31, 42, &leader, 0).as_ref() == bare);
}

#[yare::parameterized(
    missing_id = { true },
    missing_endpoint = { false },
)]
fn response_encoders_omit_incomplete_leader_endpoints(missing_id: bool) {
    let endpoint = NodeEndpointEntry {
        node_id: 7,
        host: "host".into(),
        port: 9093,
    };
    let absent = if missing_id {
        QuorumLeader {
            id: None,
            epoch: 9,
            endpoint: Some(endpoint),
        }
    } else {
        QuorumLeader {
            id: Some(7),
            epoch: 9,
            endpoint: None,
        }
    };
    let bytes = begin(0, 0, &absent, 1);
    let decoded = BeginQuorumEpochResponse::decode(&mut &bytes[..], 1).unwrap();
    assert2::assert!(decoded.node_endpoints.is_empty());
    assert2::assert!(decoded.topics[0].partitions[0].leader_id == absent.id.unwrap_or(-1));
}

#[derive(Debug, PartialEq, Eq)]
struct VoterEndpoint {
    name: String,
    host: String,
    port: u16,
}

#[test]
fn voter_listener_projection_evaluates_native_source_once_and_keeps_wire_order() {
    let evaluations = Cell::new(0);
    let request = AddRaftVoterRequest {
        listeners: ["CONTROLLER", "BACKUP"]
            .into_iter()
            .map(|name| add_request::Listener {
                name: name.into(),
                host: "controller.local".into(),
                port: 9093,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut projected = krabka_macros::voter_endpoints!({
        evaluations.set(evaluations.get() + 1);
        let listeners = request.listeners;
        assert!(listeners.len() == 2);
        listeners
    });
    assert!(evaluations.get() == 1 && projected.len() == 2);
    let first = projected.next().unwrap();
    assert!(first.name == "CONTROLLER" && first.host == "controller.local" && first.port == 9093);
    assert!(projected.next().unwrap().name == "BACKUP");
    assert!(projected.next().is_none() && evaluations.get() == 1);

    let request = UpdateRaftVoterRequest {
        listeners: vec![update_request::Listener {
            port: 19093,
            host: "replacement.local".into(),
            name: "CONTROLLER".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut projected = krabka_macros::voter_endpoints!({ request.listeners });
    assert!(
        projected.next()
            == Some(VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "replacement.local".into(),
                port: 19093,
            })
    );
    assert!(projected.next().is_none());
}
