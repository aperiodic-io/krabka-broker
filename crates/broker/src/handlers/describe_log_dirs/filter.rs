//! The topic and partition filter that a `DescribeLogDirs` request carries.
//!
//! The request's `topics` field is optional. A named topic contributes only
//! the partition indexes it lists, so an empty list contributes none, as
//! Kafka's `KafkaApis.handleDescribeLogDirsRequest` builds its partition set.
//! This module turns that wire shape into a single predicate, so the directory
//! scan in the parent module stays about directories.

use {
    krabka_protocol::owned::describe_log_dirs_request::DescribeLogDirsRequest,
    std::collections::BTreeSet,
};

/// A partition named by topic and index.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TopicPartition {
    topic: String,
    partition: i32,
}

/// Filter that the handler derives from the request `topics` field.
///
/// - `None`  → report every partition. This is the admin-client default
///   (`DescribeLogDirsRequest.isAllTopicPartitions`).
/// - `Some`  → report only the listed `(topic, partition)` pairs, the union
///   over every entry of the request.
pub(super) enum Filter {
    All,
    Partitions(BTreeSet<TopicPartition>),
}

impl Filter {
    pub(super) fn allows(&self, topic: &str, partition: i32) -> bool {
        match self {
            Filter::All => true,
            Filter::Partitions(set) => set.contains(&TopicPartition {
                topic: topic.to_string(),
                partition,
            }),
        }
    }
}

pub(super) fn request_filter(req: DescribeLogDirsRequest) -> Filter {
    req.topics.map_or(Filter::All, |topics| {
        Filter::Partitions(
            topics
                .into_iter()
                .flat_map(|topic| {
                    let name = topic.topic;
                    topic
                        .partitions
                        .into_iter()
                        .map(move |partition| TopicPartition {
                            topic: name.clone(),
                            partition,
                        })
                })
                .collect(),
        )
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::check,
        krabka_protocol::owned::describe_log_dirs_request::DescribableLogDirTopic,
    };

    #[test]
    fn filter_matches_kafka_partition_set() {
        /// A requested topic and the partitions of it that the request lists.
        struct RequestedTopic {
            topic: &'static str,
            partitions: Vec<i32>,
        }

        let request = |topics: Option<Vec<RequestedTopic>>| DescribeLogDirsRequest {
            topics: topics.map(|topics| {
                topics
                    .into_iter()
                    .map(
                        |RequestedTopic { topic, partitions }| DescribableLogDirTopic {
                            topic: topic.into(),
                            partitions,
                            ..Default::default()
                        },
                    )
                    .collect()
            }),
            ..Default::default()
        };
        let cases = [
            ("null topics", None, ("any", 9), true),
            (
                "listed partition",
                Some(vec![RequestedTopic {
                    topic: "t",
                    partitions: vec![0, 2],
                }]),
                ("t", 2),
                true,
            ),
            (
                "unlisted partition",
                Some(vec![RequestedTopic {
                    topic: "t",
                    partitions: vec![0, 2],
                }]),
                ("t", 1),
                false,
            ),
            (
                "unlisted topic",
                Some(vec![RequestedTopic {
                    topic: "t",
                    partitions: vec![0],
                }]),
                ("u", 0),
                false,
            ),
            (
                "empty partition list",
                Some(vec![RequestedTopic {
                    topic: "t",
                    partitions: vec![],
                }]),
                ("t", 0),
                false,
            ),
            (
                "repeated topic entries union",
                Some(vec![
                    RequestedTopic {
                        topic: "t",
                        partitions: vec![0],
                    },
                    RequestedTopic {
                        topic: "t",
                        partitions: vec![1],
                    },
                ]),
                ("t", 0),
                true,
            ),
        ];
        for (name, topics, (topic, partition), expected) in cases {
            let filter = request_filter(request(topics));
            check!(filter.allows(topic, partition) == expected, "case {name}");
        }
    }
}
