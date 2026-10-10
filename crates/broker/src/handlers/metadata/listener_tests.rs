//! Handler tests for what `Metadata` and `DescribeTopicPartitions` say about
//! brokers that are fenced, unregistered or without the connection listener.
//!
//! Kafka's `KRaftMetadataCache.getAliveBrokerNodes` lists the brokers that are
//! not fenced and that have an endpoint on the listener the request arrived
//! on, and `partitionMetadata` answers a partition whose leader has no such
//! endpoint with leader `-1` beside `LEADER_NOT_AVAILABLE` or
//! `LISTENER_NOT_FOUND`. The tests compare the responses as a client decodes
//! them.

use assert2::assert;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};
use krabka_protocol::owned::{
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    metadata_response::{MetadataResponse, MetadataResponseBroker},
};

use crate::{
    broker::BrokerHandle,
    codes,
    handlers::RequestContext,
    test_support::{fence_remote_broker, peer, principal, start_broker_no_audit},
};

const TOPIC: &str = "led";

/// A remote registration with an endpoint `(name, host, port)` for each of
/// `listeners`, and `rack` as its rack.
fn registration(
    node_id: u64,
    rack: Option<&str>,
    listeners: &[(&str, &str, u16)],
) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        broker_epoch: -1,
        host: "legacy-host".into(),
        port: 1,
        rack: rack.map(str::to_owned),
        endpoints: listeners
            .iter()
            .map(|(name, host, port)| BrokerEndpoint {
                name: (*name).to_owned(),
                host: (*host).to_owned(),
                port: *port,
                protocol: krabka_security::ListenerProtocol::Plaintext,
            })
            .collect(),
        ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
    })
}

/// One partition of `TOPIC` with `replicas`, all of them in sync, led by the
/// first.
fn partition(index: i32, replicas: &[u64]) -> MetadataRecord {
    let replicas: Vec<NodeId> = replicas.iter().copied().map(NodeId).collect();
    MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.into(),
        partition: index,
        leader: replicas[0],
        isr: replicas.clone(),
        replicas,
        leader_epoch: LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

/// A broker next to the remote registrations:
///
/// - 2 has `PLAINTEXT` and `EXTERNAL` endpoints;
/// - 3 has a `PLAINTEXT` endpoint and is fenced;
/// - 4 has only an `EXTERNAL` endpoint;
/// - 5 has a `PLAINTEXT` endpoint and a rack;
/// - 6 has no endpoint, only the legacy host and port.
///
/// Topic `led` has four partitions, led by 2, 9 (no registration), 4 and 2,
/// where the last one also holds the fenced broker 3.
async fn cluster() -> (BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_broker_no_audit().await;
    let broker = handle.broker_arc_for_test();
    broker
        .controller
        .submit_change(vec![
            registration(
                2,
                None,
                &[
                    ("PLAINTEXT", "b2-plain", 9092),
                    ("EXTERNAL", "b2-ext", 19092),
                ],
            ),
            registration(3, None, &[("PLAINTEXT", "b3-plain", 9092)]),
            registration(4, None, &[("EXTERNAL", "b4-ext", 19092)]),
            registration(5, Some("rack-5"), &[("PLAINTEXT", "b5-plain", 9092)]),
            registration(6, None, &[]),
            MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.into(),
                topic_id: uuid::Uuid::from_u128(0x1ed),
                partitions: 4,
                replication_factor: 2,
            }),
            partition(0, &[2, 1]),
            partition(1, &[9, 2]),
            partition(2, &[4, 2]),
            partition(3, &[2, 3]),
        ])
        .await
        .expect("seed the cluster");
    fence_remote_broker(&handle, 3).await;
    (handle, dir)
}

/// The response to `request`, which arrived on `listener`.
async fn metadata(
    handle: &BrokerHandle,
    version: i16,
    listener: &str,
    request: &MetadataRequest,
) -> MetadataResponse {
    let broker = handle.broker_arc_for_test();
    let (user, address) = (principal("describer"), peer());
    let ctx = RequestContext::new(&user, &address, "metadata-client", "conn", false, listener);
    crate::test_support::dispatch_wire(
        &broker,
        krabka_protocol::api_key::ApiKey::Metadata as i16,
        version,
        request,
        &ctx,
    )
    .await
}

fn topic_request() -> MetadataRequest {
    MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(TOPIC.into()),
            ..Default::default()
        }]),
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

/// The brokers other than the local broker 1, whose address depends on the
/// port the test broker bound.
fn remote(brokers: Vec<MetadataResponseBroker>) -> Vec<MetadataResponseBroker> {
    let mut brokers: Vec<_> = brokers.into_iter().filter(|row| row.node_id != 1).collect();
    brokers.sort_by_key(|row| row.node_id);
    brokers
}

fn row(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> MetadataResponseBroker {
    MetadataResponseBroker {
        node_id,
        host: host.into(),
        port,
        rack: rack.map(str::to_owned),
        ..Default::default()
    }
}

/// Kafka's `getAliveBrokerNodes`: the fenced broker 3, the broker 6 without
/// an endpoint, and a broker without the connection listener are not listed,
/// and each listed broker is at the address of the connection listener.
#[tokio::test]
async fn brokers_are_the_unfenced_ones_with_an_endpoint_on_the_connection_listener() {
    let (handle, _dir) = cluster().await;

    let plaintext = metadata(&handle, 12, "PLAINTEXT", &MetadataRequest::default()).await;
    let external = metadata(&handle, 12, "EXTERNAL", &MetadataRequest::default()).await;

    assert!(
        remote(plaintext.brokers)
            == vec![
                row(2, "b2-plain", 9092, None),
                row(5, "b5-plain", 9092, Some("rack-5")),
            ]
    );
    // The local broker 1 has no `EXTERNAL` endpoint either, so `remote` drops
    // nothing here.
    assert!(external.brokers.iter().all(|row| row.node_id != 1));
    assert!(
        remote(external.brokers)
            == vec![row(2, "b2-ext", 19092, None), row(4, "b4-ext", 19092, None)]
    );
    handle.shutdown().await;
}

/// The columns of one partition row that the tests compare, as `(error code,
/// leader, replicas, ISR, offline replicas)`.
type Columns = (i16, i32, Vec<i32>, Vec<i32>, Vec<i32>);

fn columns(response: &MetadataResponse, version: i16) -> Vec<Columns> {
    response.topics[0]
        .partitions
        .iter()
        .map(|partition| {
            (
                partition.error_code,
                partition.leader_id,
                partition.replica_nodes.clone(),
                partition.isr_nodes.clone(),
                // `offline_replicas` is on the wire from version 5.
                if version >= 5 {
                    partition.offline_replicas.clone()
                } else {
                    vec![]
                },
            )
        })
        .collect()
}

fn expected_plaintext_columns(listener_error: i16) -> Vec<Columns> {
    vec![
        (codes::NONE, 2, vec![2, 1], vec![2, 1], vec![]),
        (
            codes::LEADER_NOT_AVAILABLE,
            -1,
            vec![9, 2],
            vec![9, 2],
            vec![9],
        ),
        (listener_error, -1, vec![4, 2], vec![4, 2], vec![4]),
        (codes::NONE, 2, vec![2, 3], vec![2, 3], vec![3]),
    ]
}

/// `KRaftMetadataCache.partitionMetadata`, per version and listener. A leader
/// that has no registration answers `LEADER_NOT_AVAILABLE`, and one that is
/// registered without the listener answers `LISTENER_NOT_FOUND` from version
/// 6 and `LEADER_NOT_AVAILABLE` before it. Version 0 also drops the replicas
/// that are fenced, unregistered or without the listener, and answers
/// `REPLICA_NOT_AVAILABLE` beside a leader that lost one.
#[tokio::test]
async fn a_leader_without_an_endpoint_on_the_listener_answers_no_leader_and_an_error() {
    let (handle, _dir) = cluster().await;
    let none = codes::NONE;
    let no_leader = codes::LEADER_NOT_AVAILABLE;
    let cases: [(&str, i16, &str, Vec<Columns>); 4] = [
        (
            "the current version",
            12,
            "PLAINTEXT",
            expected_plaintext_columns(codes::LISTENER_NOT_FOUND),
        ),
        (
            "before LISTENER_NOT_FOUND",
            5,
            "PLAINTEXT",
            expected_plaintext_columns(no_leader),
        ),
        (
            "version 0 drops the replicas that are not alive",
            0,
            "PLAINTEXT",
            vec![
                (none, 2, vec![2, 1], vec![2, 1], vec![]),
                (no_leader, -1, vec![2], vec![2], vec![]),
                (no_leader, -1, vec![2], vec![2], vec![]),
                (codes::REPLICA_NOT_AVAILABLE, 2, vec![2], vec![2], vec![]),
            ],
        ),
        (
            "the other listener",
            12,
            "EXTERNAL",
            vec![
                // The local broker 1 has no `EXTERNAL` endpoint, so it is offline.
                (none, 2, vec![2, 1], vec![2, 1], vec![1]),
                (no_leader, -1, vec![9, 2], vec![9, 2], vec![9]),
                (none, 4, vec![4, 2], vec![4, 2], vec![]),
                (none, 2, vec![2, 3], vec![2, 3], vec![3]),
            ],
        ),
    ];

    for (label, version, listener, expected) in cases {
        let response = metadata(&handle, version, listener, &topic_request()).await;

        assert!(columns(&response, version) == expected, "{label}");
    }
    handle.shutdown().await;
}

/// `DescribeTopicPartitions` takes the same leader and the same offline list,
/// and carries no error code beside a `-1`.
#[tokio::test]
async fn describe_topic_partitions_answers_no_leader_for_a_leader_without_the_listener() {
    let (handle, _dir) = cluster().await;
    let broker = handle.broker_arc_for_test();
    let (user, address) = (principal("describer"), peer());
    let ctx = RequestContext::new(
        &user,
        &address,
        "describe-client",
        "conn",
        false,
        "PLAINTEXT",
    );
    let request = crate::test_support::topic_partitions_request(TOPIC);

    let response = crate::handlers::describe_topic_partitions::handle(&broker, request, 0, &ctx)
        .await
        .expect("handle describe topic partitions");

    let rows: Vec<_> = response.topics[0]
        .partitions
        .iter()
        .map(|partition| {
            (
                partition.error_code,
                partition.leader_id,
                partition.offline_replicas.clone(),
            )
        })
        .collect();
    assert!(
        rows == vec![
            (codes::NONE, 2, vec![]),
            (codes::NONE, -1, vec![9]),
            (codes::NONE, -1, vec![4]),
            (codes::NONE, 2, vec![3]),
        ]
    );
    handle.shutdown().await;
}
