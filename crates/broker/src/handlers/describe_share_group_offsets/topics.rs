//! The topics a `DescribeShareGroupOffsets` group row covers when the request
//! names none.
//!
//! KIP-932 lets the request omit the topic list entirely, which means "every
//! topic this group has share state for". This is Kafka's
//! `GroupCoordinatorService.describeShareGroupAllOffsets`: the group's
//! initialized partitions from its share-state partition metadata, each topic
//! id turned back into a name through the metadata image, and a topic the
//! image no longer holds left out.

use {
    crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue,
    krabka_protocol::owned::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic,
};

/// The initialized topics of `metadata` that `image` holds, by name, each
/// with its partitions in ascending order.
///
/// Kafka collects them in a `HashMap` of topic id to a `HashSet` of
/// partitions. The partitions of a set iterate in ascending order; the topic
/// order is that map's, which krabka does not reproduce, so the topics go in
/// name order.
pub(super) fn initialized_topics(
    metadata: Option<&ShareGroupStatePartitionMetadataValue>,
    image: &krabka_metadata::MetadataImage,
) -> Vec<DescribeShareGroupOffsetsRequestTopic> {
    let Some(metadata) = metadata else {
        return Vec::new();
    };
    let mut topics: Vec<_> = metadata
        .initialized
        .iter()
        .filter_map(|topic| {
            image.topic_name_by_id(&topic.topic_id).map(|topic_name| {
                let mut partitions = topic.partitions.clone();
                partitions.sort_unstable();
                partitions.dedup();
                DescribeShareGroupOffsetsRequestTopic {
                    topic_name: topic_name.into(),
                    partitions,
                    ..Default::default()
                }
            })
        })
        .collect();
    topics.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
    topics
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::share::persistence::TopicPartitionsInfo,
            handlers::describe_share_group_offsets::test_support::image_with_topic,
        },
        assert2::assert,
        krabka_metadata::{MetadataRecord, TopicRecord},
    };

    fn initialized_topic(
        topic_id: uuid::Uuid,
        topic_name: &str,
        partitions: Vec<i32>,
    ) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id,
            topic_name: topic_name.to_owned(),
            partitions,
        }
    }

    fn topic(name: &str, partitions: Vec<i32>) -> DescribeShareGroupOffsetsRequestTopic {
        DescribeShareGroupOffsetsRequestTopic {
            topic_name: name.into(),
            partitions,
            ..Default::default()
        }
    }

    #[test]
    fn the_initialized_topics_the_image_holds_are_described() {
        let alpha_id = uuid::Uuid::from_u128(1);
        let beta_id = uuid::Uuid::from_u128(2);
        let missing_id = uuid::Uuid::from_u128(3);
        let mut image = image_with_topic("beta", beta_id);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "alpha".into(),
            topic_id: alpha_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let metadata = ShareGroupStatePartitionMetadataValue {
            initializing: Vec::new(),
            initialized: vec![
                initialized_topic(beta_id, "beta", vec![0]),
                initialized_topic(missing_id, "gone", vec![7]),
                initialized_topic(alpha_id, "alpha", vec![1, 0]),
            ],
            deleting: Vec::new(),
        };

        let actual = [
            initialized_topics(Some(&metadata), &image),
            initialized_topics(None, &image),
        ];

        let expected = [
            vec![topic("alpha", vec![0, 1]), topic("beta", vec![0])],
            Vec::new(),
        ];
        assert!(actual == expected);
    }
}
