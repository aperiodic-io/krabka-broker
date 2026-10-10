//! Fixtures shared by the `DescribeShareGroupOffsets` test modules.
//!
//! Building a metadata image that knows one topic, and registering a topic
//! on a live broker, are each needed by more than one of the test modules
//! under this handler, so they live in one file instead of once per module.

use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};

pub(super) fn image_with_topic(name: &str, topic_id: uuid::Uuid) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id,
        partitions: 1,
        replication_factor: 1,
    }));
    image
}

/// Registers topic `name` with one partition, led by this broker, in the
/// broker's own metadata.
///
/// The share coordinator refuses to initialize state for a topic-partition its
/// metadata does not hold, as Kafka's `ShareCoordinatorService` does, so a test
/// registers each topic it seeds share state for. It returns once the
/// partition is materialized locally, so a row's lag reads its high watermark.
pub(super) async fn register_topic(
    broker: &crate::broker::Broker,
    name: &str,
    topic_id: uuid::Uuid,
) {
    let leader = krabka_metadata::NodeId(broker.config.node_id.0);
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(crate::handlers::test_support::single_replica_partition(
                name,
                krabka_ids::PartitionIndex(0),
                leader,
            )),
        ])
        .await
        .expect("register topic");
    let materialized = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while broker
            .partitions
            .get(name, krabka_ids::PartitionIndex(0))
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert2::assert!(
        materialized.is_ok(),
        "{name}-0 was not materialized locally"
    );
}
