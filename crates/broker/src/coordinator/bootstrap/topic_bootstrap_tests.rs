//! Tests for the local partition directories that [`super::bootstrap`] opens,
//! and for the topic configs of the coordinator topics.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_metadata::{MetadataRecord, TopicRecord};
use krabka_protocol::owned::create_topics_request::CreateTopicsRequest;
use tempfile::tempdir;

use super::{
    OFFSETS_PARTITION, OFFSETS_TOPIC, bootstrap,
    test_support::{controller_with_leader, test_coordinator},
};
use crate::{config::BrokerConfig, log_dir, partition_registry::PartitionRegistry};

krabka_macros::single_replica_partition_fixture!(partition_record);

/// Registers a one-partition `__consumer_offsets` that this node leads, as
/// the first group lookup's auto-creation does.
async fn register_offsets_topic(controller: &Arc<dyn crate::metadata_source::MetadataSource>) {
    let node = krabka_metadata::NodeId(1);
    controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: OFFSETS_TOPIC.to_owned(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(partition_record(
                OFFSETS_TOPIC,
                krabka_ids::PartitionIndex(OFFSETS_PARTITION),
                node,
            )),
        ])
        .await
        .expect("register __consumer_offsets");
}

struct BootstrapFixture {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    config: BrokerConfig,
    _dir: tempfile::TempDir,
}

async fn bootstrap_fixture() -> BootstrapFixture {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let controller = controller_with_leader(dir.path().join("__cluster_metadata_test")).await;
    BootstrapFixture {
        config,
        controller,
        _dir: dir,
    }
}

fn bootstrap_components(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> (
    Arc<PartitionRegistry>,
    Arc<crate::coordinator::GroupCoordinator>,
    crate::log_dir_status::LogDirRegistry,
) {
    let partitions = Arc::new(PartitionRegistry::new());
    let coordinator = test_coordinator(controller, &partitions);
    let status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());
    (partitions, coordinator, status)
}

/// The startup bootstrap creates no topic. Kafka creates `__consumer_offsets`
/// on the first `FindCoordinator(GROUP)`, with its configured replication
/// factor, and a broker that started alone must not create it with fewer
/// replicas.
#[tokio::test]
async fn bootstrap_creates_no_offsets_topic() {
    let BootstrapFixture {
        config,
        controller,
        _dir,
    } = bootstrap_fixture().await;
    let (partitions, coordinator, log_dir_status) = bootstrap_components(&config, &controller);
    bootstrap(
        &config,
        &controller,
        &partitions,
        &coordinator,
        &log_dir_status,
        &Arc::new(crate::producer_state::ProducerState::new()),
    )
    .await
    .unwrap();
    check!(controller.current_image().topic(OFFSETS_TOPIC).is_none());
    check!(!partitions.contains(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION)));
}

/// On a restart the topic is in the image already. The bootstrap opens the
/// local partition, and a second bootstrap keeps the one it opened.
#[tokio::test]
async fn bootstrap_opens_the_local_partitions_of_an_existing_offsets_topic() {
    let BootstrapFixture {
        config,
        controller,
        _dir,
    } = bootstrap_fixture().await;
    register_offsets_topic(&controller).await;
    let (partitions, coordinator, log_dir_status) = bootstrap_components(&config, &controller);
    for _boot in 0..2 {
        bootstrap(
            &config,
            &controller,
            &partitions,
            &coordinator,
            &log_dir_status,
            &Arc::new(crate::producer_state::ProducerState::new()),
        )
        .await
        .unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        check!(topic_dir.exists());
        check!(partitions.contains(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION)));
    }
    let image = controller.current_image();
    check!(image.topics().filter(|t| t.name == OFFSETS_TOPIC).count() == 1);
}

/// Kafka creates each coordinator topic with an explicit config map:
/// `GroupCoordinatorService.groupMetadataTopicConfigs`,
/// `TransactionCoordinator.transactionStateTopicConfigs` and
/// `ShareCoordinatorService.shareGroupStateTopicConfigs`. The values below are
/// Kafka's defaults for `offsets.topic.segment.bytes`,
/// `transaction.state.log.segment.bytes`, `transaction.state.log.min.isr`,
/// `share.coordinator.state.topic.segment.bytes` and
/// `share.coordinator.state.topic.min.isr`.
#[tokio::test]
async fn internal_topics_are_created_with_kafkas_topic_configs() {
    let dir = tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    // Kafka's default minimum ISR. The single-node fixture lowers it to 1.
    config.transaction_state_min_isr = 2;
    config.share_coordinator.state_topic_min_isr = 2;
    let handle = crate::broker::Broker::start(config)
        .await
        .expect("start broker");
    let broker = handle.broker_arc_for_test();
    let request = CreateTopicsRequest {
        topics: [
            OFFSETS_TOPIC,
            crate::txn::bootstrap::TOPIC,
            crate::share_coordinator::bootstrap::TOPIC,
        ]
        .into_iter()
        .map(|topic| crate::auto_topic_creation::creatable_topic(&broker.config, topic))
        .collect(),
        ..Default::default()
    };
    let response = crate::topic_creator::TopicCreator::new(&broker)
        .create_topic_without_principal(request)
        .await
        .expect("the controller answers CreateTopics");
    for row in &response.topics {
        assert!(row.error_code == crate::codes::NONE, "create: {row:?}");
    }

    let image = handle.controller_image_for_test();
    for (topic, expected) in [
        (
            OFFSETS_TOPIC,
            vec![
                ("cleanup.policy", "compact"),
                ("compression.type", "producer"),
                ("segment.bytes", "104857600"),
            ],
        ),
        (
            crate::txn::bootstrap::TOPIC,
            vec![
                ("cleanup.policy", "compact"),
                ("compression.type", "uncompressed"),
                ("min.insync.replicas", "2"),
                ("segment.bytes", "104857600"),
                ("unclean.leader.election.enable", "false"),
            ],
        ),
        (
            crate::share_coordinator::bootstrap::TOPIC,
            vec![
                ("cleanup.policy", "delete"),
                ("compression.type", "producer"),
                ("min.insync.replicas", "2"),
                ("retention.ms", "-1"),
                ("segment.bytes", "104857600"),
            ],
        ),
    ] {
        let expected: std::collections::BTreeMap<String, String> = expected
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        check!(image.topic_config(topic) == Some(&expected), "{topic}");
    }
    handle.shutdown().await;
}
