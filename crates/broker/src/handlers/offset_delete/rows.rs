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
    let mut valid: Vec<(&str, Vec<i32>)> = Vec::new();

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
            valid.push((topic.name.as_str(), valid_partitions));
        }
    }

    let mut tombstones: Vec<Record> = Vec::new();
    let mut to_remove: Vec<(String, i32)> = Vec::new();
    let mut delta: i32 = 0;
    for (name, partitions) in valid {
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
            expected_row, expected_topic, req_with_topics,
        },
        assert2::check,
        std::collections::HashSet,
    };

    type Request<'a> = &'a [(&'a str, &'a [i32])];
    type TopicRows<'a> = &'a [(&'a str, &'a [(i32, i16)])];
    /// A `build_rows` case: name, request, subscription, expected rows, and
    /// the keys it deletes.
    type Case<'a> = (
        &'a str,
        Request<'a>,
        SubscribedTopics,
        TopicRows<'a>,
        Vec<(&'a str, i32)>,
    );

    fn expected(rows: TopicRows<'_>) -> Vec<OffsetDeleteResponseTopic> {
        rows.iter()
            .map(|(name, partitions)| {
                expected_topic(
                    name,
                    partitions
                        .iter()
                        .map(|(index, code)| expected_row(*index, *code))
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
        let none = named(&[]);
        let rows: [Case<'_>; 8] = [
            (
                "deletes every valid partition",
                &[("t1", &[0, 2]), ("t2", &[7])],
                none.clone(),
                &[
                    ("t1", &[(0, codes::NONE), (2, codes::NONE)]),
                    ("t2", &[(7, codes::NONE)]),
                ],
                vec![("t1", 0), ("t1", 2), ("t2", 7)],
            ),
            (
                "denied topic",
                &[("denied", &[0, 1])],
                none.clone(),
                &[("denied", &[
                    (0, codes::TOPIC_AUTHORIZATION_FAILED),
                    (1, codes::TOPIC_AUTHORIZATION_FAILED),
                ])],
                vec![],
            ),
            (
                "denied wins over subscribed",
                &[("denied", &[0])],
                named(&["denied"]),
                &[("denied", &[(0, codes::TOPIC_AUTHORIZATION_FAILED)])],
                vec![],
            ),
            (
                "#734: existence is checked before the subscription",
                &[("ghost", &[0])],
                named(&["ghost"]),
                &[("ghost", &[(0, codes::UNKNOWN_TOPIC_OR_PARTITION)])],
                vec![],
            ),
            (
                "subscribed topic that exists",
                &[("t1", &[0])],
                named(&["t1"]),
                &[("t1", &[(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)])],
                vec![],
            ),
            (
                "subscribed to every topic",
                &[("t1", &[0]), ("t2", &[1])],
                SubscribedTopics::All,
                &[
                    ("t1", &[(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)]),
                    ("t2", &[(1, codes::GROUP_SUBSCRIBED_TO_TOPIC)]),
                ],
                vec![],
            ),
            (
                "out-of-range partitions come first, then the coordinator's",
                &[("t1", &[0, 99, -1, 3])],
                named(&["t2"]),
                &[("t1", &[
                    (99, codes::UNKNOWN_TOPIC_OR_PARTITION),
                    (-1, codes::UNKNOWN_TOPIC_OR_PARTITION),
                    (0, codes::NONE),
                    (3, codes::NONE),
                ])],
                vec![("t1", 0), ("t1", 3)],
            ),
            (
                "the coordinator's topics follow the broker's",
                &[("t1", &[0]), ("denied", &[0])],
                none.clone(),
                &[
                    ("denied", &[(0, codes::TOPIC_AUTHORIZATION_FAILED)]),
                    ("t1", &[(0, codes::NONE)]),
                ],
                vec![("t1", 0)],
            ),
        ];

        for (name, request, subscribed, want, want_removed) in rows {
            let req = req_with_topics(request);

            let got =
                build_response_rows("g", &req.topics, &decisions, &subscribed, &counts).unwrap();

            check!(got.topics == expected(want), "{name}");
            let removed: Vec<(&str, i32)> = got
                .to_remove
                .iter()
                .map(|(t, p)| (t.as_str(), *p))
                .collect();
            check!(removed == want_removed, "{name}");
            let deltas: Vec<i32> = got.tombstones.iter().map(|t| t.offset_delta).collect();
            let want_deltas: Vec<i32> = (0..).take(want_removed.len()).collect();
            check!(deltas == want_deltas, "{name}");
            check!(got.tombstones.iter().all(|t| t.value.is_none()), "{name}");
        }
    }
}
