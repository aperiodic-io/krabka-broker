//! Fixtures shared by the replicator's unit tests: a static `MetadataSource`,
//! metadata-image builders for the leader and follower-throttle cases, a
//! `Config` over a temporary log dir, and `Fetch` response builders.

use std::{collections::BTreeMap, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::{
    MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::NodeId;

use super::Config;
use crate::{
    log_dir_status::LogDirRegistry,
    metrics::BrokerMetrics,
    partition_registry::PartitionRegistry,
    producer_state::ProducerState,
    test_support::FakeMetadataSource,
    throttle::{FOLLOWER_THROTTLED_REPLICAS_KEY, ThrottleState},
};
pub(super) const TOPIC: &str = "orders";
pub(super) const PARTITION: i32 = 0;
pub(super) const NODE_ID: NodeId = NodeId(2);
pub(super) const LEADER_ID: NodeId = NodeId(1);
pub(super) const WIRE_TOPIC_ID: WireUuid = WireUuid([7; 16]);

/// A metadata source over `image` with no controller leader elected, and a
/// loopback controller listener.
fn static_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::static_image(image)
}

pub(super) fn image_with_leader(leader: NodeId) -> MetadataImage {
    image_with_topic_id_and_leader(uuid::Uuid::from_bytes(WIRE_TOPIC_ID.0), leader)
}

pub(super) fn image_with_topic_id_and_leader(
    topic_id: uuid::Uuid,
    leader: NodeId,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.into(),
        topic_id,
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.into(),
        partition: PARTITION,
        leader,
        replicas: vec![LEADER_ID, NODE_ID],
        isr: vec![LEADER_ID, NODE_ID],
        leader_epoch: krabka_metadata::LeaderEpoch(4),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: Vec::new(),
        partition_epoch: 0,
    }));
    image
}

pub(super) fn image_with_follower_throttle(value: &str) -> MetadataImage {
    let mut image = image_with_leader(LEADER_ID);
    let mut overrides = BTreeMap::new();
    overrides.insert(
        FOLLOWER_THROTTLED_REPLICAS_KEY.to_string(),
        value.to_string(),
    );
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.into(),
        overrides,
    }));
    image
}

pub(super) fn test_config(image: MetadataImage) -> (Config, tempfile::TempDir) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = Config {
        node_id: NODE_ID,
        topic: TOPIC.into(),
        topic_id: WIRE_TOPIC_ID,
        partition: PartitionIndex(PARTITION),
        leader_node_id: LEADER_ID,
        leader_epoch: krabka_metadata::LeaderEpoch(4),
        leader_host: "127.0.0.1".into(),
        leader_port: 9,
        partitions: Arc::new(PartitionRegistry::new()),
        log_dirs: vec![log_dir.path().to_path_buf()],
        log_settings: LogConfig::default(),
        client_id: "replica-test".into(),
        connection: super::replication_connection_for_test().call(),
        throttle_state: Arc::new(ThrottleState::new()),
        controller: Arc::new(static_source(image)),
        log_dir_status: LogDirRegistry::default(),
        producer_state: Arc::new(ProducerState::new()),
        metrics: BrokerMetrics::default(),
        lag: super::ReplicaLag::default(),
    };
    (cfg, log_dir)
}

pub(super) fn fetch_response(
    topic: &str,
    topic_id: WireUuid,
    part: PartitionData,
) -> FetchResponse {
    FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic: topic.into(),
            topic_id,
            partitions: vec![part],
            ..FetchableTopicResponse::default()
        }],
        ..FetchResponse::default()
    }
}

pub(super) fn partition_response(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        ..PartitionData::default()
    }
}
