//! The client and wire fixtures that more than one `ListOffsets` test module
//! needs, so each of them drives the handler through the same request shape.

use assert2::assert;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsResponse},
};

use super::sentinels::UNKNOWN_EPOCH;
use crate::{
    broker::BrokerHandle, codes, config::RemoteStorageBackend,
    test_support::start_broker_no_audit_with,
};

crate::test_support::wire_helpers!(
    pub(super) ListOffsetsRequest,
    ListOffsetsResponse,
    client_id = "admin-client"
);

pub(super) async fn client_for(broker: &BrokerHandle) -> krabka_client_core::Client {
    krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("list-offsets-test")
        .build()
        .await
        .expect("client build")
}

pub(super) async fn create_topic(
    client: &krabka_client_core::Client,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                configs,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
}

/// A topic whose remote-storage configuration has reached the local log.
/// Keep both directories alive while a test fills or reads its remote tier.
pub(super) async fn remote_topic(
    topic: &str,
    extra_configs: Vec<CreatableTopicConfig>,
) -> (
    BrokerHandle,
    krabka_client_core::Client,
    [tempfile::TempDir; 2],
) {
    let remote_dir = tempfile::tempdir().expect("remote tempdir");
    let remote_path = remote_dir.path().to_path_buf();
    let (broker, directory) = start_broker_no_audit_with(move |config| {
        config.remote_storage_backend = Some(RemoteStorageBackend::Local { dir: remote_path });
    })
    .await;
    let client = client_for(&broker).await;
    let mut configs = vec![CreatableTopicConfig {
        name: "remote.storage.enable".into(),
        value: Some("true".into()),
        ..Default::default()
    }];
    configs.extend(extra_configs);
    create_topic(&client, topic, configs).await;
    broker.wait_until_partition_present(topic, 0).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !broker
            .partition_log_config_for_test(topic, 0)
            .is_some_and(|config| config.remote_storage_enable)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("remote topic config propagated");
    (broker, client, [directory, remote_dir])
}

pub(super) async fn list_one(
    client: &krabka_client_core::Client,
    topic: &str,
    timestamp: i64,
) -> ListOffsetsPartitionResponse {
    list_one_at_epoch(client, topic, timestamp, UNKNOWN_EPOCH).await
}

/// [`list_one`] with the KIP-320 `current_leader_epoch` the client asserts.
/// `UNKNOWN_EPOCH` is the sentinel for "assert nothing", which is what a
/// client sends when it holds no epoch for the partition.
pub(super) async fn list_one_at_epoch(
    client: &krabka_client_core::Client,
    topic: &str,
    timestamp: i64,
    current_leader_epoch: i32,
) -> ListOffsetsPartitionResponse {
    client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    current_leader_epoch,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets")
        .topics
        .remove(0)
        .partitions
        .remove(0)
}

#[derive(Debug)]
pub(super) struct DenyNamed(pub(super) std::collections::HashSet<&'static str>);

test_authorizer!(DenyNamed, (self, _source, req), {
    if self.0.contains(req.resource_name) {
        crate::authorizer::AuthorizationResult::Deny
    } else {
        crate::authorizer::AuthorizationResult::Allow
    }
});
