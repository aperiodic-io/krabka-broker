//! The `OffsetCommit` response, built the way Kafka's
//! `OffsetCommitResponse.Builder` builds it.
//!
//! Kafka keys the topic rows by `topic_id` at v10 and later (`TopicIdBuilder`)
//! and by name before v10 (`TopicNameBuilder`). The handler first adds the rows
//! that it refuses. Two refused request rows with the same key share one
//! response row. The coordinator's rows then merge in: into the row with the
//! same key when one exists, and at the end when none does. When the handler
//! refused no row, the coordinator's rows are the whole response, unchanged.

use {
    krabka_protocol::{
        owned::{
            offset_commit_request::OffsetCommitRequestTopic,
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::collections::HashMap,
};

/// The key of one response topic row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TopicKey {
    /// v10 and later.
    Id(WireUuid),
    /// Versions before v10.
    Name(String),
}

/// Collects the topic rows of one `OffsetCommit` response.
#[derive(Debug)]
pub struct ResponseBuilder {
    use_topic_ids: bool,
    topics: Vec<OffsetCommitResponseTopic>,
    slots: HashMap<TopicKey, usize>,
}

impl ResponseBuilder {
    /// Starts an empty response. `use_topic_ids` is true at v10 and later.
    pub fn new(use_topic_ids: bool) -> Self {
        Self {
            use_topic_ids,
            topics: Vec::new(),
            slots: HashMap::new(),
        }
    }

    fn key(&self, topic_id: WireUuid, name: &str) -> TopicKey {
        if self.use_topic_ids {
            TopicKey::Id(topic_id)
        } else {
            TopicKey::Name(name.to_string())
        }
    }

    /// The index of the response row for `(topic_id, name)`, which this call
    /// adds when no row has that key yet.
    fn slot(&mut self, topic_id: WireUuid, name: &str) -> usize {
        let key = self.key(topic_id, name);
        *self.slots.entry(key).or_insert_with(|| {
            self.topics.push(OffsetCommitResponseTopic {
                name: name.to_string(),
                topic_id,
                ..Default::default()
            });
            self.topics.len() - 1
        })
    }

    /// Adds a row for every partition of `topic`, each with `error_code`.
    ///
    /// This is Kafka's `Builder.addPartitions`.
    pub fn add_topic(&mut self, topic: &OffsetCommitRequestTopic, error_code: i16) {
        let slot = self.slot(topic.topic_id, &topic.name);
        self.topics[slot]
            .partitions
            .extend(
                topic
                    .partitions
                    .iter()
                    .map(|partition| OffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        error_code,
                        ..Default::default()
                    }),
            );
    }

    /// Adds one partition row with `error_code`.
    ///
    /// This is Kafka's `Builder.addPartition`.
    pub fn add_partition(
        &mut self,
        topic_id: WireUuid,
        name: &str,
        partition_index: i32,
        error_code: i16,
    ) {
        let slot = self.slot(topic_id, name);
        self.topics[slot]
            .partitions
            .push(OffsetCommitResponsePartition {
                partition_index,
                error_code,
                ..Default::default()
            });
    }

    /// Merges the coordinator's topic rows into the response.
    ///
    /// This is Kafka's `Builder.merge`.
    pub fn merge(&mut self, topics: Vec<OffsetCommitResponseTopic>) {
        if self.topics.is_empty() {
            self.topics = topics;
            return;
        }
        for topic in topics {
            let key = self.key(topic.topic_id, &topic.name);
            if let Some(&slot) = self.slots.get(&key) {
                self.topics[slot].partitions.extend(topic.partitions);
            } else {
                self.slots.insert(key, self.topics.len());
                self.topics.push(topic);
            }
        }
    }

    /// Finishes the response.
    pub fn build(self) -> OffsetCommitResponse {
        OffsetCommitResponse {
            topics: self.topics,
            throttle_time_ms: 0,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::codes, assert2::assert,
        krabka_protocol::owned::offset_commit_request::OffsetCommitRequestPartition,
    };

    const ID_A: WireUuid = WireUuid([0x0a; 16]);
    const ID_B: WireUuid = WireUuid([0x0b; 16]);

    fn request_topic(
        name: &str,
        topic_id: WireUuid,
        partitions: &[i32],
    ) -> OffsetCommitRequestTopic {
        OffsetCommitRequestTopic {
            name: name.to_string(),
            topic_id,
            partitions: partitions
                .iter()
                .map(|&partition_index| OffsetCommitRequestPartition {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn response_topic(
        name: &str,
        topic_id: WireUuid,
        partitions: &[(i32, i16)],
    ) -> OffsetCommitResponseTopic {
        OffsetCommitResponseTopic {
            name: name.to_string(),
            topic_id,
            partitions: partitions
                .iter()
                .map(
                    |&(partition_index, error_code)| OffsetCommitResponsePartition {
                        partition_index,
                        error_code,
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        }
    }

    /// One scenario: the key kind, the refused rows, the coordinator rows, and
    /// the topic rows that Kafka's builder gives.
    struct Case {
        label: &'static str,
        use_topic_ids: bool,
        refused: Vec<(OffsetCommitRequestTopic, i16)>,
        committed: Vec<OffsetCommitResponseTopic>,
        expected: Vec<OffsetCommitResponseTopic>,
    }

    #[test]
    fn rows_merge_by_the_key_of_the_request_version() {
        let cases = [
            Case {
                label: "no refused row keeps the coordinator rows unchanged",
                use_topic_ids: true,
                refused: Vec::new(),
                committed: vec![
                    response_topic("", ID_A, &[(0, codes::NONE)]),
                    response_topic("", ID_A, &[(1, codes::NONE)]),
                ],
                expected: vec![
                    response_topic("", ID_A, &[(0, codes::NONE)]),
                    response_topic("", ID_A, &[(1, codes::NONE)]),
                ],
            },
            Case {
                label: "refused rows come first and two zero ids share one row",
                use_topic_ids: true,
                refused: vec![
                    (
                        request_topic("", WireUuid::ZERO, &[0]),
                        codes::UNKNOWN_TOPIC_ID,
                    ),
                    (request_topic("", ID_B, &[0]), codes::UNKNOWN_TOPIC_ID),
                    (
                        request_topic("", WireUuid::ZERO, &[1]),
                        codes::UNKNOWN_TOPIC_ID,
                    ),
                ],
                committed: vec![response_topic("a", ID_A, &[(0, codes::NONE)])],
                expected: vec![
                    response_topic("", WireUuid::ZERO, &[
                        (0, codes::UNKNOWN_TOPIC_ID),
                        (1, codes::UNKNOWN_TOPIC_ID),
                    ]),
                    response_topic("", ID_B, &[(0, codes::UNKNOWN_TOPIC_ID)]),
                    response_topic("a", ID_A, &[(0, codes::NONE)]),
                ],
            },
            Case {
                label: "a coordinator row joins the refused row with the same name",
                use_topic_ids: false,
                refused: vec![(
                    request_topic("a", WireUuid::ZERO, &[0]),
                    codes::TOPIC_AUTHORIZATION_FAILED,
                )],
                committed: vec![
                    response_topic("b", WireUuid::ZERO, &[(0, codes::NONE)]),
                    response_topic("a", WireUuid::ZERO, &[(1, codes::NONE)]),
                ],
                expected: vec![
                    response_topic("a", WireUuid::ZERO, &[
                        (0, codes::TOPIC_AUTHORIZATION_FAILED),
                        (1, codes::NONE),
                    ]),
                    response_topic("b", WireUuid::ZERO, &[(0, codes::NONE)]),
                ],
            },
        ];

        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for case in cases {
            let mut builder = ResponseBuilder::new(case.use_topic_ids);
            for (topic, error_code) in &case.refused {
                builder.add_topic(topic, *error_code);
            }
            builder.merge(case.committed);
            actual.push((case.label, builder.build()));
            expected.push((case.label, OffsetCommitResponse {
                topics: case.expected,
                throttle_time_ms: 0,
                ..Default::default()
            }));
        }
        assert!(actual == expected);
    }
}
