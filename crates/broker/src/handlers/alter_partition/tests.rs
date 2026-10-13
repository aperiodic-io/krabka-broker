//! End-to-end tests for the `AlterPartition` handler entry point.
//!
//! They drive a live broker so that the authorization preamble, the
//! openraft-leader check, and the `submit_change` of an accepted ISR proposal
//! are exercised together, which is why they are kept out of the module root.

use {
    super::{
        test_support::{request_with_topics, seed_partition, wire_topic_id},
        *,
    },
    crate::{
        broker::Broker,
        handlers::RequestContext,
        test_support::{
            DenyAll, KafkaErrorCode, dispatch_wire, start_broker_with_authorizer as start_broker,
            test_ctx,
        },
    },
    assert2::assert,
    krabka_protocol::owned::{
        alter_partition_request::{PartitionData as ReqPartitionData, TopicData as ReqTopicData},
        alter_partition_response,
    },
    std::sync::Arc,
};

crate::test_support::context_helper!(client_id = "broker-client");

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct EmptyAlterPartitionSetup {
    #[default(krabka_ids::ApiVersion(alter_partition_response::MAX_VERSION))]
    version: krabka_ids::ApiVersion,
    expected: KafkaErrorCode,
}

async fn check_empty_alter_partition(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    setup: EmptyAlterPartitionSetup,
) {
    let request = request_with_topics(broker, Vec::new());
    let response = super::handle(broker, request, setup.version.0, ctx)
        .await
        .expect("handle");
    let expected = unthrottled_wire!(AlterPartitionResponse {
        error_code: setup.expected.0,
        topics: Vec::new(),
    });
    assert!(response == expected);
}

#[tokio::test]
async fn handle_denies_cluster_action_for_whole_request() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        deny_all,
        context(ctx, "replica")
    );
    check_empty_alter_partition(&broker, &ctx, EmptyAlterPartitionSetup {
        expected: KafkaErrorCode(codes::CLUSTER_AUTHORIZATION_FAILED),
        ..Default::default()
    })
    .await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn leader_accepts_empty_alter_partition_request() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "replica"),
        controller_leader
    );
    check_empty_alter_partition(&broker, &ctx, EmptyAlterPartitionSetup::default()).await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_topic_partition_response_and_commits_isr_change() {
    let version = 2;
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_partition(&broker).await;
    test_ctx!(ctx, "replica");
    let req = single_partition_request(&broker);
    let resp = super::handle(&broker, req, version, &ctx)
        .await
        .expect("handle");

    let expected = accepted_isr_response();
    assert!(resp == expected);

    let image = broker.controller.current_image();
    let committed = image.partition("t", 0).expect("partition committed");
    assert!(committed.partition_epoch == 1);
    broker_handle.shutdown().await;
}

/// Both requests name the same partition epoch. Exactly one may advance it;
/// the other must rebuild admission after the first commit and be fenced.
#[tokio::test]
async fn concurrent_isr_updates_revalidate_the_partition_epoch_at_append() {
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_partition(&broker).await;
    test_ctx!(ctx, "replica");
    let request = single_partition_request(&broker);
    let (first, second) = tokio::join!(
        super::handle(&broker, request.clone(), 2, &ctx),
        super::handle(&broker, request, 2, &ctx),
    );
    let mut responses = [
        first.expect("first response"),
        second.expect("second response"),
    ];
    responses.sort_unstable_by_key(|response| response.topics[0].partitions[0].error_code);
    let expected = [
        accepted_isr_response(),
        unthrottled_wire!(AlterPartitionResponse {
            error_code: codes::NONE,
            topics: vec![tagged_wire!(RespTopicData {
                topic_id: wire_topic_id(),
                partitions: vec![RespPartitionData {
                    partition_index: 0,
                    error_code: codes::INVALID_UPDATE_VERSION,
                    ..Default::default()
                }],
            })],
        }),
    ];
    assert!(responses == expected);
    assert!(
        broker
            .controller
            .current_image()
            .partition("t", 0)
            .expect("partition")
            .partition_epoch
            == 1
    );
    broker_handle.shutdown().await;
}

/// The topic id that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicIdKind {
    /// The id of the seeded topic "t".
    Known,
    /// A non-zero id that no topic has.
    Unknown,
    /// The zero id.
    Zero,
}

/// Kafka's `ReplicationControlManager.alterPartition` answers
/// `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id is zero or
/// names no topic. For a known topic, a partition that does not exist answers
/// `UNKNOWN_TOPIC_OR_PARTITION` and the other rows go through validation.
///
/// Each row sends partitions 0 and 1. The seeded topic has partition 0 only,
/// and every accepted ISR change raises its partition epoch by one.
#[tokio::test]
async fn topic_row_error_follows_version_and_topic_id() {
    let cases = [
        (2, TopicIdKind::Known),
        (2, TopicIdKind::Unknown),
        (2, TopicIdKind::Zero),
        (3, TopicIdKind::Known),
        (3, TopicIdKind::Unknown),
        (3, TopicIdKind::Zero),
    ];
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_partition(&broker).await;
    test_ctx!(ctx, "replica");

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    let mut partition_epoch = 0;
    for (version, kind) in cases {
        let topic_id = match kind {
            TopicIdKind::Known => wire_topic_id(),
            TopicIdKind::Unknown => krabka_protocol::primitives::uuid::Uuid([0x0b; 16]),
            TopicIdKind::Zero => krabka_protocol::primitives::uuid::Uuid::ZERO,
        };
        // v2 carries `new_isr`, and v3 carries `new_isr_with_epochs`. An epoch
        // of -1 skips the KIP-903 broker epoch check.
        let row = |partition_index| ReqPartitionData {
            partition_index,
            leader_epoch: 5,
            new_isr: vec![1],
            new_isr_with_epochs: vec![super::test_support::bs(
                crate::test_support::WireBrokerId(1),
                crate::test_support::BrokerEpoch(-1),
            )],
            partition_epoch,
            ..Default::default()
        };
        let req = request_with_topics(&broker, vec![ReqTopicData {
            topic_id,
            partitions: vec![row(0), row(1)],
            ..Default::default()
        }]);
        let resp = dispatch_wire(
            &broker,
            krabka_protocol::owned::alter_partition_request::API_KEY,
            version,
            &req,
            &ctx,
        )
        .await;
        actual.push((version, kind, resp));

        let refused = |partition_index, error_code| RespPartitionData {
            partition_index,
            error_code,
            ..Default::default()
        };
        let partitions = if kind == TopicIdKind::Known {
            partition_epoch += 1;
            vec![
                tagged_wire!(RespPartitionData {
                    partition_index: 0,
                    error_code: codes::NONE,
                    leader_id: 1,
                    leader_epoch: 5,
                    isr: vec![1],
                    leader_recovery_state: 0,
                    partition_epoch,
                }),
                refused(1, codes::UNKNOWN_TOPIC_OR_PARTITION),
            ]
        } else {
            vec![
                refused(0, codes::UNKNOWN_TOPIC_ID),
                refused(1, codes::UNKNOWN_TOPIC_ID),
            ]
        };
        expected.push((
            version,
            kind,
            unthrottled_wire!(AlterPartitionResponse {
                error_code: codes::NONE,
                topics: vec![tagged_wire!(RespTopicData {
                    topic_id,
                    partitions,
                })],
            }),
        ));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `ReplicationControlManager.alterPartition` starts with
/// `ClusterControlManager.checkBrokerEpoch`: a sender whose broker epoch is
/// not its registration's, or that is not registered, gets a top-level
/// `STALE_BROKER_EPOCH` and no rows, and the ISR does not move.
#[tokio::test]
async fn a_stale_sender_broker_epoch_refuses_the_whole_request() {
    let version = alter_partition_response::MAX_VERSION;
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_partition(&broker).await;
    test_ctx!(ctx, "replica");
    let current = request_with_topics(&broker, Vec::new()).broker_epoch;

    for (broker_id, broker_epoch) in [(1, current + 1), (1, -1), (99, current)] {
        let req = AlterPartitionRequest {
            broker_id,
            broker_epoch,
            ..request_with_topics(&broker, vec![ReqTopicData {
                topic_id: wire_topic_id(),
                partitions: vec![ReqPartitionData {
                    partition_index: 0,
                    leader_epoch: 5,
                    new_isr_with_epochs: vec![super::test_support::bs(
                        crate::test_support::WireBrokerId(1),
                        crate::test_support::BrokerEpoch(-1),
                    )],
                    ..Default::default()
                }],
                ..Default::default()
            }])
        };
        let resp = super::handle(&broker, req, version, &ctx)
            .await
            .expect("handle");
        let expected = unthrottled_wire!(AlterPartitionResponse {
            error_code: codes::STALE_BROKER_EPOCH,
            topics: Vec::new(),
        });
        assert!(
            resp == expected,
            "broker {broker_id} at epoch {broker_epoch}"
        );
    }
    let image = broker.controller.current_image();
    assert!(image.partition("t", 0).expect("seeded").partition_epoch == 0);
    broker_handle.shutdown().await;
}

fn single_partition_request(broker: &Broker) -> AlterPartitionRequest {
    request_with_topics(broker, vec![ReqTopicData {
        topic_id: wire_topic_id(),
        partitions: vec![ReqPartitionData {
            partition_index: 0,
            leader_epoch: 5,
            new_isr: vec![1],
            partition_epoch: 0,
            ..Default::default()
        }],
        ..Default::default()
    }])
}

fn accepted_isr_response() -> AlterPartitionResponse {
    unthrottled_wire!(AlterPartitionResponse {
        error_code: codes::NONE,
        topics: vec![tagged_wire!(RespTopicData {
            topic_id: wire_topic_id(),
            partitions: vec![tagged_wire!(RespPartitionData {
                partition_index: 0,
                error_code: codes::NONE,
                leader_id: 1,
                leader_epoch: 5,
                isr: vec![1],
                leader_recovery_state: 0,
                partition_epoch: 1,
            })],
        })],
    })
}
