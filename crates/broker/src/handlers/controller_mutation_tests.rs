//! Controller mutations through a broker whose observer stopped before newer commits.

use {
    super::test_support::replicated_partition,
    crate::{
        broker::BrokerHandle,
        codes,
        config::NodeRole,
        test_support::{
            dispatch_wire, peer, principal, request_context, single_partition_topic,
            start_broker_no_audit, start_broker_no_audit_with, string_pairs,
        },
    },
    assert2::assert,
    krabka_metadata::{BrokerConfigRecord, DeleteTopicRecord, MetadataRecord, TopicConfigRecord},
    krabka_protocol::{
        owned::{
            delete_topics_request::{self, DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::DeleteTopicsResponse,
            incremental_alter_configs_request::{
                self, AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
            },
            incremental_alter_configs_response::IncrementalAlterConfigsResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    krabka_raft::{BootstrapMode, NodeId},
    tempfile::TempDir,
    uuid::Uuid,
};

macro_rules! assert_single_response_error {
    ($response:expr, $error_code:expr) => {{
        assert!($response.responses.len() == 1);
        assert!($response.responses[0].error_code == $error_code);
    }};
}

async fn stopped_observer() -> (BrokerHandle, BrokerHandle, TempDir, TempDir) {
    let (controller, controller_dir) = start_broker_no_audit().await;
    controller.wait_until_controller_leader().await;
    let authority = controller.broker_arc_for_test();
    let addr = controller.controller_addr();
    let cluster_id = authority.controller.current_image().cluster_id();
    let (observer, observer_dir) = start_broker_no_audit_with(|config| {
        config.roles = vec![NodeRole::Broker];
        config.node_id = NodeId(2);
        config.broker_id = 2;
        config.bootstrap_mode = BootstrapMode::Join;
        config.cluster_id = Some(cluster_id);
        config.controller_quorum_voters = vec![(NodeId(1), addr.to_string())];
    })
    .await;
    authority
        .controller
        .submit_change(topic_records(1))
        .await
        .expect("seed topic");
    observer.wait_until_partition_present("orders", 0).await;
    observer.broker_arc_for_test().controller.cancel().await;
    (controller, observer, controller_dir, observer_dir)
}

fn topic_records(id: u128) -> Vec<MetadataRecord> {
    vec![
        MetadataRecord::V1Topic(single_partition_topic("orders", Uuid::from_u128(id))),
        MetadataRecord::V1Partition(
            replicated_partition()
                .topic("orders")
                .replicas(&[NodeId(1)])
                .call(),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_observer_cannot_delete_a_recreated_topic_by_old_id() {
    let (controller, observer, _controller_dir, _observer_dir) = stopped_observer().await;
    let authority = controller.broker_arc_for_test();
    authority
        .controller
        .submit_change(
            [
                vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: "orders".into(),
                })],
                topic_records(2),
            ]
            .concat(),
        )
        .await
        .expect("recreate topic");
    let broker = observer.broker_arc_for_test();
    assert!(
        broker
            .controller
            .current_image()
            .topic("orders")
            .expect("cached topic")
            .topic_id
            == Uuid::from_u128(1)
    );
    let principal = principal("alice");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "admin");
    let response: DeleteTopicsResponse = dispatch_wire(
        &broker,
        delete_topics_request::API_KEY,
        6,
        &DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                topic_id: WireUuid(Uuid::from_u128(1).into_bytes()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        },
        &ctx,
    )
    .await;
    assert_single_response_error!(response, codes::UNKNOWN_TOPIC_ID);
    assert!(
        authority
            .controller
            .current_image()
            .topic("orders")
            .expect("replacement remains")
            .topic_id
            == Uuid::from_u128(2)
    );
    observer.shutdown().await;
    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incremental_update_through_a_stale_observer_keeps_unrelated_config() {
    let (controller, observer, _controller_dir, _observer_dir) = stopped_observer().await;
    let authority = controller.broker_arc_for_test();
    authority
        .controller
        .submit_change(vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: string_pairs(&[("cleanup.policy", "compact")]),
        })])
        .await
        .expect("set unrelated config");
    let broker = observer.broker_arc_for_test();
    assert!(
        broker
            .controller
            .current_image()
            .topic_config("orders")
            .is_none()
    );
    let principal = principal("alice");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "admin");
    let response: IncrementalAlterConfigsResponse = dispatch_wire(
        &broker,
        incremental_alter_configs_request::API_KEY,
        1,
        &IncrementalAlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: 2,
                resource_name: "orders".into(),
                configs: vec![AlterableConfig {
                    name: "retention.ms".into(),
                    config_operation: 0,
                    value: Some("30000".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        &ctx,
    )
    .await;
    assert_single_response_error!(response, codes::NONE);
    assert!(
        authority.controller.current_image().topic_config("orders")
            == Some(&string_pairs(&[
                ("cleanup.policy", "compact"),
                ("retention.ms", "30000")
            ]))
    );
    observer.shutdown().await;
    controller.shutdown().await;
}

fn named_broker_config_request(
    key: &str,
    operation: i8,
    value: &str,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: 4,
            resource_name: "2".into(),
            configs: vec![AlterableConfig {
                name: key.into(),
                config_operation: operation,
                value: Some(value.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn named_broker_list_operations_merge_at_authority_and_keep_local_dir_checks() {
    let (controller, observer, _controller_dir, _observer_dir) = stopped_observer().await;
    let authority = controller.broker_arc_for_test();
    authority
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(2),
            config_name: "log.cleanup.policy".into(),
            config_value: Some("compact".into()),
        })])
        .await
        .expect("newer broker config");
    let broker = observer.broker_arc_for_test();
    let principal = principal("alice");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "admin");
    for (operation, value, expected) in [(2, "delete", "compact,delete"), (3, "compact", "delete")]
    {
        let response: IncrementalAlterConfigsResponse = dispatch_wire(
            &broker,
            incremental_alter_configs_request::API_KEY,
            1,
            &named_broker_config_request("log.cleanup.policy", operation, value),
            &ctx,
        )
        .await;
        assert!(
            response.responses[0].error_code == codes::NONE,
            "{response:?}"
        );
        assert!(
            authority
                .controller
                .current_image()
                .broker_config(NodeId(2))
                .and_then(|configs| configs.get("log.cleanup.policy"))
                .map(String::as_str)
                == Some(expected)
        );
    }
    let response: IncrementalAlterConfigsResponse = dispatch_wire(
        &broker,
        incremental_alter_configs_request::API_KEY,
        1,
        &named_broker_config_request("cordoned.log.dirs", 0, "/not/a/configured/log/directory"),
        &ctx,
    )
    .await;
    assert!(
        response.responses[0].error_code == codes::INVALID_REQUEST,
        "{response:?}"
    );
    assert!(
        authority
            .controller
            .current_image()
            .broker_config(NodeId(2))
            .is_none_or(|configs| !configs.contains_key("cordoned.log.dirs"))
    );
    observer.shutdown().await;
    controller.shutdown().await;
}
