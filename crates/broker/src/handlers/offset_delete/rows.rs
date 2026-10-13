//! The KIP-496 decision table: the requested `(topic, partition)` tuples in,
//! the response rows plus, for each deletion that may proceed, one tombstone
//! record out.
//!
//! This is the whole per-partition outcome of an `OffsetDelete`, and it is
//! pure. The handler feeds it the ACL decisions, the group's subscribed topics
//! and the partition counts it gathered, so every branch of the specification
//! is testable without a broker.

use {
    crate::{
        authorizer::AuthorizationResult,
        codes,
        coordinator::{persistence::OffsetCommitValue, unified::actor::SubscribedTopics},
        error::BrokerError,
    },
    krabka_protocol::{
        owned::{
            offset_delete_request::OffsetDeleteRequestTopic,
            offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
        },
        records::Record,
    },
    std::collections::HashMap,
};

/// The per-partition rows of an `OffsetDelete` response, the tombstones to
/// append to the group's `__consumer_offsets` partition, and the
/// `(topic, partition)` keys to remove from the group's committed offsets
/// once the append succeeds.
pub(super) struct Rows {
    pub topics: Vec<OffsetDeleteResponseTopic>,
    pub tombstones: Vec<Record>,
    pub to_remove: Vec<(String, i32)>,
}

/// A requested topic and the partitions of it that pass the broker's checks.
struct ValidTopic<'a> {
    name: &'a str,
    partitions: Vec<i32>,
}

/// Builds the response rows in Kafka's two steps.
///
/// `KafkaApis.handleOffsetDeleteRequest` answers first, per topic:
///   - a `Read` denial (`topic_decisions[name] == Deny`, or no decision) gives
///     `TOPIC_AUTHORIZATION_FAILED` for every partition in the topic;
///   - a topic absent from `topic_partition_counts` gives
///     `UNKNOWN_TOPIC_OR_PARTITION` for every partition in the topic;
///   - a `partition_index` out of range gives `UNKNOWN_TOPIC_OR_PARTITION`.
///
/// `OffsetMetadataManager.deleteOffsets` then answers the partitions left:
///   - a topic that the group subscribes to gives `GROUP_SUBSCRIBED_TO_TOPIC`
///     for every one of them;
///   - otherwise each queues a tombstone and answers `NONE`.
///
/// `OffsetDeleteResponse.Builder.merge` appends the coordinator's partitions
/// to a topic that the first step already answered, and its topics after the
/// first step's.
pub(super) fn build_response_rows(
    group_id: &str,
    topics: &[OffsetDeleteRequestTopic],
    topic_decisions: &HashMap<&str, AuthorizationResult>,
    subscribed_topics: &SubscribedTopics,
    topic_partition_counts: &HashMap<&str, i32>,
) -> Result<Rows, BrokerError> {
    let mut out: Vec<OffsetDeleteResponseTopic> = Vec::new();
    let mut valid: Vec<ValidTopic<'_>> = Vec::new();

    for topic in topics {
        let denied = topic_decisions
            .get(topic.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Deny;
        let partition_count = topic_partition_counts.get(topic.name.as_str()).copied();
        let mut valid_partitions = Vec::new();
        for part in &topic.partitions {
            let index = part.partition_index;
            let code = match partition_count {
                _ if denied => codes::TOPIC_AUTHORIZATION_FAILED,
                Some(n) if (0..n).contains(&index) => {
                    valid_partitions.push(index);
                    continue;
                }
                _ => codes::UNKNOWN_TOPIC_OR_PARTITION,
            };
            push_row(&mut out, &topic.name, index, code);
        }
        if !valid_partitions.is_empty() {
            valid.push(ValidTopic {
                name: topic.name.as_str(),
                partitions: valid_partitions,
            });
        }
    }

    let mut tombstones: Vec<Record> = Vec::new();
    let mut to_remove: Vec<(String, i32)> = Vec::new();
    let mut delta: i32 = 0;
    for ValidTopic { name, partitions } in valid {
        let subscribed = subscribed_topics.contains(name);
        for index in partitions {
            let code = if subscribed {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                tombstones.push(Record {
                    offset_delta: delta,
                    timestamp_delta: 0,
                    key: Some(OffsetCommitValue::encode_key(group_id, name, index)?),
                    value: None, // null value = tombstone
                    ..Default::default()
                });
                delta += 1;
                to_remove.push((name.to_string(), index));
                codes::NONE
            };
            push_row(&mut out, name, index, code);
        }
    }

    Ok(Rows {
        topics: out,
        tombstones,
        to_remove,
    })
}

/// Adds one partition row under the topic named `name`, creating the topic
/// row at the end when it is not there yet, as Kafka's `getOrCreateTopic`.
fn push_row(out: &mut Vec<OffsetDeleteResponseTopic>, name: &str, index: i32, code: i16) {
    let row = OffsetDeleteResponsePartition {
        partition_index: index,
        error_code: code,
        ..Default::default()
    };
    if let Some(topic) = out.iter_mut().find(|t| t.name == name) {
        topic.partitions.push(row);
    } else {
        out.push(OffsetDeleteResponseTopic {
            name: name.to_string(),
            partitions: vec![row],
            ..Default::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::offset_delete::test_support::{
            RequestTopic, expected_row, expected_topic, req_with_topics,
        },
        assert2::check,
        std::collections::HashSet,
    };

    type Request<'a> = &'a [RequestTopic<'a>];
    type TopicRows<'a> = &'a [ExpectedTopicRows<'a>];

    /// An expected partition row: its index and error code.
    #[derive(Clone, Copy)]
    struct PartitionCode {
        partition_index: i32,
        error_code: i16,
    }

    /// The expected rows of one topic.
    #[derive(Clone, Copy)]
    struct ExpectedTopicRows<'a> {
        name: &'a str,
        partitions: &'a [PartitionCode],
    }

    /// A key that a case deletes from the group's committed offsets.
    #[derive(Debug, PartialEq, Eq)]
    struct RemovedKey<'a> {
        topic: &'a str,
        partition: i32,
    }

    /// A `build_rows` case: name, request, subscription, expected rows, and
    /// the keys it deletes.
    struct Case<'a> {
        name: &'a str,
        request: Request<'a>,
        subscribed: SubscribedTopics,
        want: TopicRows<'a>,
        want_removed: Vec<RemovedKey<'a>>,
    }

    fn expected(rows: TopicRows<'_>) -> Vec<OffsetDeleteResponseTopic> {
        rows.iter()
            .map(|ExpectedTopicRows { name, partitions }| {
                expected_topic(
                    name,
                    partitions
                        .iter()
                        .map(|row| expected_row(row.partition_index, row.error_code))
                        .collect(),
                )
            })
            .collect()
    }

    fn named(topics: &[&str]) -> SubscribedTopics {
        SubscribedTopics::Named(
            topics
                .iter()
                .map(|s| (*s).to_string())
                .collect::<HashSet<_>>(),
        )
    }

    /// The `build_rows` case table. Topics: `t1` (4 partitions), `t2` (8
    /// partitions), `denied` (Read denied, 4 partitions), `ghost` (not in the
    /// metadata).
    fn cases() -> Vec<Case<'static>> {
        let none = named(&[]);
        [
            Case {
                name: "deletes every valid partition",
                request: &[
                    RequestTopic {
                        name: "t1",
                        partitions: &[0, 2],
                    },
                    RequestTopic {
                        name: "t2",
                        partitions: &[7],
                    },
                ],
                subscribed: none.clone(),
                want: &[
                    ExpectedTopicRows {
                        name: "t1",
                        partitions: &[
                            PartitionCode {
                                partition_index: 0,
                                error_code: codes::NONE,
                            },
                            PartitionCode {
                                partition_index: 2,
                                error_code: codes::NONE,
                            },
                        ],
                    },
                    ExpectedTopicRows {
                        name: "t2",
                        partitions: &[PartitionCode {
                            partition_index: 7,
                            error_code: codes::NONE,
                        }],
                    },
                ],
                want_removed: vec![
                    RemovedKey {
                        topic: "t1",
                        partition: 0,
                    },
                    RemovedKey {
                        topic: "t1",
                        partition: 2,
                    },
                    RemovedKey {
                        topic: "t2",
                        partition: 7,
                    },
                ],
            },
            Case {
                name: "denied topic",
                request: &[RequestTopic {
                    name: "denied",
                    partitions: &[0, 1],
                }],
                subscribed: none.clone(),
                want: &[ExpectedTopicRows {
                    name: "denied",
                    partitions: &[
                        PartitionCode {
                            partition_index: 0,
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        },
                        PartitionCode {
                            partition_index: 1,
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        },
                    ],
                }],
                want_removed: vec![],
            },
            Case {
                name: "denied wins over subscribed",
                request: &[RequestTopic {
                    name: "denied",
                    partitions: &[0],
                }],
                subscribed: named(&["denied"]),
                want: &[ExpectedTopicRows {
                    name: "denied",
                    partitions: &[PartitionCode {
                        partition_index: 0,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    }],
                }],
                want_removed: vec![],
            },
        ]
        .into_iter()
        .chain(more_cases())
        .collect()
    }

    /// The remaining `build_rows` cases, see [`cases`].
    fn more_cases() -> [Case<'static>; 5] {
        let none = named(&[]);
        [
            Case {
                name: "#734: existence is checked before the subscription",
                request: &[RequestTopic {
                    name: "ghost",
                    partitions: &[0],
                }],
                subscribed: named(&["ghost"]),
                want: &[ExpectedTopicRows {
                    name: "ghost",
                    partitions: &[PartitionCode {
                        partition_index: 0,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    }],
                }],
                want_removed: vec![],
            },
            Case {
                name: "subscribed topic that exists",
                request: &[RequestTopic {
                    name: "t1",
                    partitions: &[0],
                }],
                subscribed: named(&["t1"]),
                want: &[ExpectedTopicRows {
                    name: "t1",
                    partitions: &[PartitionCode {
                        partition_index: 0,
                        error_code: codes::GROUP_SUBSCRIBED_TO_TOPIC,
                    }],
                }],
                want_removed: vec![],
            },
            Case {
                name: "subscribed to every topic",
                request: &[
                    RequestTopic {
                        name: "t1",
                        partitions: &[0],
                    },
                    RequestTopic {
                        name: "t2",
                        partitions: &[1],
                    },
                ],
                subscribed: SubscribedTopics::All,
                want: &[
                    ExpectedTopicRows {
                        name: "t1",
                        partitions: &[PartitionCode {
                            partition_index: 0,
                            error_code: codes::GROUP_SUBSCRIBED_TO_TOPIC,
                        }],
                    },
                    ExpectedTopicRows {
                        name: "t2",
                        partitions: &[PartitionCode {
                            partition_index: 1,
                            error_code: codes::GROUP_SUBSCRIBED_TO_TOPIC,
                        }],
                    },
                ],
                want_removed: vec![],
            },
            Case {
                name: "out-of-range partitions come first, then the coordinator's",
                request: &[RequestTopic {
                    name: "t1",
                    partitions: &[0, 99, -1, 3],
                }],
                subscribed: named(&["t2"]),
                want: &[ExpectedTopicRows {
                    name: "t1",
                    partitions: &[
                        PartitionCode {
                            partition_index: 99,
                            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        },
                        PartitionCode {
                            partition_index: -1,
                            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        },
                        PartitionCode {
                            partition_index: 0,
                            error_code: codes::NONE,
                        },
                        PartitionCode {
                            partition_index: 3,
                            error_code: codes::NONE,
                        },
                    ],
                }],
                want_removed: vec![
                    RemovedKey {
                        topic: "t1",
                        partition: 0,
                    },
                    RemovedKey {
                        topic: "t1",
                        partition: 3,
                    },
                ],
            },
            Case {
                name: "the coordinator's topics follow the broker's",
                request: &[
                    RequestTopic {
                        name: "t1",
                        partitions: &[0],
                    },
                    RequestTopic {
                        name: "denied",
                        partitions: &[0],
                    },
                ],
                subscribed: none.clone(),
                want: &[
                    ExpectedTopicRows {
                        name: "denied",
                        partitions: &[PartitionCode {
                            partition_index: 0,
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        }],
                    },
                    ExpectedTopicRows {
                        name: "t1",
                        partitions: &[PartitionCode {
                            partition_index: 0,
                            error_code: codes::NONE,
                        }],
                    },
                ],
                want_removed: vec![RemovedKey {
                    topic: "t1",
                    partition: 0,
                }],
            },
        ]
    }

    /// Every branch of the two-step decision, with the whole row list and the
    /// keys it deletes. Topics: `t1` (4 partitions), `t2` (8 partitions),
    /// `denied` (Read denied, 4 partitions), `ghost` (not in the metadata).
    #[test]
    fn build_rows_follows_kafka_order_and_codes() {
        let decisions = HashMap::from([
            ("t1", AuthorizationResult::Allow),
            ("t2", AuthorizationResult::Allow),
            ("ghost", AuthorizationResult::Allow),
            ("denied", AuthorizationResult::Deny),
        ]);
        let counts = HashMap::from([("t1", 4), ("t2", 8), ("denied", 4)]);
        let rows = cases();

        for Case {
            name,
            request,
            subscribed,
            want,
            want_removed,
        } in rows
        {
            let req = req_with_topics(request);

            let got =
                build_response_rows("g", &req.topics, &decisions, &subscribed, &counts).unwrap();

            check!(got.topics == expected(want), "{name}");
            let removed: Vec<RemovedKey<'_>> = got
                .to_remove
                .iter()
                .map(|(topic, partition)| RemovedKey {
                    topic: topic.as_str(),
                    partition: *partition,
                })
                .collect();
            check!(removed == want_removed, "{name}");
            let deltas: Vec<i32> = got.tombstones.iter().map(|t| t.offset_delta).collect();
            let want_deltas: Vec<i32> = (0..).take(want_removed.len()).collect();
            check!(deltas == want_deltas, "{name}");
            check!(got.tombstones.iter().all(|t| t.value.is_none()), "{name}");
        }
    }
}
