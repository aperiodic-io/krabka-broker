//! Handler tests for the KIP-951 `NodeEndpoints` of `ShareFetch` and
//! `ShareAcknowledge`.
//!
//! Kafka's `KafkaApis.processShareFetchResponse` and
//! `processShareAcknowledgeResponse` set `CurrentLeader` on every row with
//! `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH`, and add the leader's
//! node on the listener of the request to `NodeEndpoints`, once per node. A
//! leader that no live broker registration backs keeps its id in the hint and
//! adds no endpoint.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            StartedBroker, configured_topic_request, replicated_partition, share_fetch_wire,
        },
        share_partition::session::FetchPartitions,
        test_support::{
            decode_response, encode_request, initialize_share_state, peer, principal,
            request_context, start_broker_no_audit_with, try_dispatch_context,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_ids::LeaderEpoch,
    krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId, PartitionRecord,
        TopicRecord,
    },
    krabka_protocol::{
        owned::{
            share_acknowledge_request::{
                AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeBatch,
                ShareAcknowledgeRequest,
            },
            share_acknowledge_response::{self, ShareAcknowledgeResponse},
            share_fetch_request::{FetchPartition, FetchTopic, ShareFetchRequest},
            share_fetch_response::{
                self, LeaderIdAndEpoch, NodeEndpoint, PartitionData, ShareFetchResponse,
                ShareFetchableTopicResponse,
            },
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordsPayload,
    },
    std::sync::Arc,
};

const GROUP: &str = "endpoints-group";

/// The remote broker that leads both partitions of [`REMOTE`].
const REMOTE_NODE: i32 = 2;
/// A leader that no broker registration backs.
const UNREGISTERED_NODE: i32 = 7;

const REMOTE: WireUuid = WireUuid([0x22; 16]);
const ORPHAN: WireUuid = WireUuid([0x77; 16]);

async fn start() -> StartedBroker {
    let (broker, dir) =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(AllowAllAuthorizer)).await;
    StartedBroker { broker, dir }
}

async fn create_local_topic(broker: &BrokerHandle) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-endpoints-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(configured_topic_request().topic("local").call())
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present("local", 0).await;
    let image = broker.controller_image_for_test();
    WireUuid(
        image
            .topic("local")
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

fn partition(topic: &str, partition: i32, leader: i32, epoch: i32) -> MetadataRecord {
    let leader = NodeId(u64::try_from(leader).expect("a node id"));
    MetadataRecord::V1Partition(PartitionRecord {
        leader_epoch: LeaderEpoch(epoch),
        ..replicated_partition()
            .topic(topic)
            .partition(partition)
            .leader(leader)
            .replicas(&[leader])
            .call()
    })
}

/// Registers [`REMOTE_NODE`] with an endpoint on the test listener, and adds
/// a two-partition topic that it leads and a topic led by
/// [`UNREGISTERED_NODE`].
async fn seed_remote_leaders(broker: &BrokerHandle) {
    let listener = request_context(&principal("x"), &peer(), "x")
        .connection_listener_name
        .to_string();
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                broker_epoch: -1,
                host: "legacy-2".into(),
                port: 1000,
                rack: Some("rack-2".into()),
                endpoints: vec![BrokerEndpoint {
                    name: listener,
                    host: "remote-2".into(),
                    port: 9192,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                }],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "remote".into(),
                topic_id: uuid::Uuid::from_bytes(REMOTE.0),
                partitions: 2,
                replication_factor: 1,
            }),
            partition("remote", 0, REMOTE_NODE, 4),
            partition("remote", 1, REMOTE_NODE, 4),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orphan".into(),
                topic_id: uuid::Uuid::from_bytes(ORPHAN.0),
                partitions: 1,
                replication_factor: 1,
            }),
            partition("orphan", 0, UNREGISTERED_NODE, 1),
        ])
        .await
        .expect("seed the remote leaders");
}

/// One row of the request: a topic and one of its partitions.
struct RequestRow {
    topic_id: WireUuid,
    partition_index: i32,
}

/// The rows of the request.
fn rows(local: WireUuid) -> Vec<RequestRow> {
    [(local, 0), (REMOTE, 0), (REMOTE, 1), (ORPHAN, 0)]
        .into_iter()
        .map(|(topic_id, partition_index)| RequestRow {
            topic_id,
            partition_index,
        })
        .collect()
}

/// A leader id and the epoch of its leadership.
#[derive(Clone, Copy)]
struct Leader {
    id: i32,
    epoch: i32,
}

fn fetch_row(partition_index: i32, error_code: i16, leader: Leader) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        current_leader: LeaderIdAndEpoch {
            leader_id: leader.id,
            leader_epoch: leader.epoch,
            ..Default::default()
        },
        records: Some(RecordsPayload::Legacy(Bytes::new())),
        ..Default::default()
    }
}

#[tokio::test]
async fn share_fetch_sends_the_endpoint_of_each_remote_leader_once() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let local = create_local_topic(&broker).await;
    initialize_share_state(&broker, GROUP, uuid::Uuid::from_bytes(local.0), 0).await;
    seed_remote_leaders(&broker).await;
    let request = ShareFetchRequest {
        group_id: Some(GROUP.into()),
        member_id: Some("member".into()),
        share_session_epoch: 0,
        max_bytes: 1 << 20,
        max_records: 10,
        batch_size: 10,
        topics: [local, REMOTE, ORPHAN]
            .into_iter()
            .map(|topic_id| FetchTopic {
                topic_id,
                partitions: rows(local)
                    .into_iter()
                    .filter(|row| row.topic_id == topic_id)
                    .map(|row| FetchPartition {
                        partition_index: row.partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let version = share_fetch_response::MAX_VERSION;
    let response = share_fetch_wire()
        .broker(&broker)
        .request(&request)
        .version(version)
        .call()
        .await;

    let expected = ShareFetchResponse {
        acquisition_lock_timeout_ms: 30_000,
        responses: vec![
            ShareFetchableTopicResponse {
                topic_id: local,
                partitions: vec![fetch_row(0, codes::NONE, Leader { id: 0, epoch: 0 })],
                ..Default::default()
            },
            ShareFetchableTopicResponse {
                topic_id: REMOTE,
                partitions: vec![
                    fetch_row(0, codes::NOT_LEADER_OR_FOLLOWER, Leader {
                        id: REMOTE_NODE,
                        epoch: 4,
                    }),
                    fetch_row(1, codes::NOT_LEADER_OR_FOLLOWER, Leader {
                        id: REMOTE_NODE,
                        epoch: 4,
                    }),
                ],
                ..Default::default()
            },
            ShareFetchableTopicResponse {
                topic_id: ORPHAN,
                partitions: vec![fetch_row(0, codes::NOT_LEADER_OR_FOLLOWER, Leader {
                    id: UNREGISTERED_NODE,
                    epoch: 1,
                })],
                ..Default::default()
            },
        ],
        node_endpoints: vec![NodeEndpoint {
            node_id: REMOTE_NODE,
            host: "remote-2".into(),
            port: 9192,
            rack: Some("rack-2".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(response == expected);
    broker.shutdown().await;
}

#[tokio::test]
async fn share_acknowledge_on_a_remote_leader_answers_unknown_partition_without_a_hint() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let local = create_local_topic(&broker).await;
    seed_remote_leaders(&broker).await;
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("share-consumer"),
        client_id = "share-client"
    );
    shared
        .share_partition_leaders
        .update_fetch_session(
            (GROUP, "member"),
            ctx.connection_id,
            0,
            FetchPartitions {
                requested: &[],
                forgotten: &std::collections::HashSet::new(),
            },
            false,
        )
        .expect("open the share session");
    let request = ShareAcknowledgeRequest {
        group_id: Some(GROUP.into()),
        member_id: Some("member".into()),
        share_session_epoch: 1,
        topics: [REMOTE, ORPHAN]
            .into_iter()
            .map(|topic_id| AcknowledgeTopic {
                topic_id,
                partitions: rows(local)
                    .into_iter()
                    .filter(|row| row.topic_id == topic_id)
                    .map(|row| AcknowledgePartition {
                        partition_index: row.partition_index,
                        acknowledgement_batches: vec![AcknowledgeBatch {
                            first_offset: 0,
                            last_offset: 0,
                            acknowledge_types: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let version = share_acknowledge_response::MAX_VERSION;
    let response = try_dispatch_context(
        &shared,
        krabka_protocol::owned::share_acknowledge_request::API_KEY,
        version,
        &encode_request(&request, version),
        &ctx,
    )
    .await
    .expect("handle share acknowledge");
    let response: ShareAcknowledgeResponse = decode_response(&response, version);

    // Kafka's `SharePartitionManager.acknowledge` has no leadership check: a
    // broker that does not lead the partition holds no share partition for
    // it, so the row answers UNKNOWN_TOPIC_OR_PARTITION with no leader hint,
    // and there is no endpoint to send.
    let row = |partition_index| share_acknowledge_response::PartitionData {
        partition_index,
        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
        ..Default::default()
    };
    let expected = ShareAcknowledgeResponse {
        acquisition_lock_timeout_ms: 30_000,
        responses: vec![
            share_acknowledge_response::ShareAcknowledgeTopicResponse {
                topic_id: REMOTE,
                partitions: vec![row(0), row(1)],
                ..Default::default()
            },
            share_acknowledge_response::ShareAcknowledgeTopicResponse {
                topic_id: ORPHAN,
                partitions: vec![row(0)],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert!(response == expected);
    broker.shutdown().await;
}
