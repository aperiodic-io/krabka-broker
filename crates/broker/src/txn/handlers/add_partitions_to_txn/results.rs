//! The per-topic and per-partition result rows of an `AddPartitionsToTxn`
//! response.
//!
//! Every path through the handler answers with the same nested row shape, so
//! the three builders here are the single place that decides which error code
//! lands on which partition row: one shared code for the whole transaction,
//! one shared code with the per-topic refusal in
//! [`topic_refusal`](super::write_freeze::topic_refusal) overriding it, and
//! the KIP-890 verify-only shape whose code is per partition.

use {
    super::write_freeze::topic_refusal,
    crate::{
        handlers::map_partition_rows,
        txn::{
            coordinator::produce_verification::verification_code,
            state::{TopicPartition, TxnEntry},
        },
    },
    krabka_ids::PartitionIndex,
    krabka_protocol::owned::common::{
        add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        add_partitions_to_txn_response::{
            add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
            add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
        },
    },
    std::collections::{HashMap, HashSet},
};

/// Collapses a requested topic list to one row per topic and one row per
/// partition within it, in first-occurrence order.
///
/// The response schema declares `TransactionalId`, the topic `Name`, and
/// `PartitionIndex` as `mapKey: true` fields (KIP: `AddPartitionsToTxnResponse.json`),
/// and Kafka's `TransactionCoordinator` collects its per-partition results
/// into a `HashMap<TopicPartition, Errors>` before encoding. A caller that
/// names the same topic twice, or the same partition twice within a topic,
/// therefore gets exactly one response row for it, not one per request
/// entry (#883).
pub(super) fn dedup_topics(topics: &[AddPartitionsToTxnTopic]) -> Vec<AddPartitionsToTxnTopic> {
    let mut order: Vec<String> = Vec::new();
    let mut partitions_by_topic: HashMap<String, (Vec<i32>, HashSet<i32>)> = HashMap::new();
    for topic in topics {
        let (partitions, seen) = partitions_by_topic
            .entry(topic.name.clone())
            .or_insert_with(|| {
                order.push(topic.name.clone());
                (Vec::new(), HashSet::new())
            });
        for &partition in &topic.partitions {
            if seen.insert(partition) {
                partitions.push(partition);
            }
        }
    }
    order
        .into_iter()
        .map(|name| {
            let (partitions, _) = partitions_by_topic
                .remove(&name)
                .expect("every ordered name was inserted into the map above");
            AddPartitionsToTxnTopic {
                name,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

/// KIP-890 verify-only per-partition decision. See
/// [`verification_code`](crate::txn::coordinator::produce_verification::verification_code).
fn verify_partition_code(
    entry: &TxnEntry,
    requested: (krabka_log::ProducerId, i16),
    tp: &TopicPartition,
) -> i16 {
    verification_code(
        (entry.producer_id, entry.producer_epoch, entry.state),
        entry.partitions.contains(tp),
        requested,
    )
}

/// Builds the verify-only response. It has the same shape as
/// `per_topic_with_refusals` on the add path, but each partition carries its
/// own verify result instead of one shared code. A denied or frozen topic
/// still short-circuits to its refusal on every partition row.
pub(super) fn verify_partitions(
    entry: &TxnEntry,
    requested: (krabka_log::ProducerId, i16),
    topics: &[AddPartitionsToTxnTopic],
    (denied, frozen): (&HashSet<String>, &HashSet<String>),
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let refusal = topic_refusal(&t.name, denied, frozen);
            topic_result(t, |p| {
                refusal.unwrap_or_else(|| {
                    verify_partition_code(entry, requested, &TopicPartition {
                        topic: t.name.clone(),
                        partition: PartitionIndex(p),
                    })
                })
            })
        })
        .collect()
}

/// Builds a per-topic and per-partition result list. A topic named in `denied`
/// gets `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row, and one
/// named in `frozen` gets `POLICY_VIOLATION (44)`. Every other topic gets
/// `code`.
pub(super) fn per_topic_with_refusals(
    topics: &[AddPartitionsToTxnTopic],
    denied: &HashSet<String>,
    frozen: &HashSet<String>,
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let row_code = topic_refusal(&t.name, denied, frozen).unwrap_or(code);
            topic_result(t, |_| row_code)
        })
        .collect()
}

/// Builds a per-topic and per-partition result list in which every partition
/// carries `error_code`. Whole-transaction errors use it, such as the txn-id
/// ACL deny path.
pub(super) fn topic_error(
    topics: &[AddPartitionsToTxnTopic],
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics.iter().map(|t| topic_result(t, |_| code)).collect()
}

fn topic_result(
    topic: &AddPartitionsToTxnTopic,
    code: impl Fn(i32) -> i16,
) -> AddPartitionsToTxnTopicResult {
    AddPartitionsToTxnTopicResult {
        name: topic.name.clone(),
        results_by_partition: map_partition_rows(
            topic.partitions.iter().copied(),
            |partition_index| AddPartitionsToTxnPartitionResult {
                partition_index,
                partition_error_code: code(partition_index),
                ..Default::default()
            },
        ),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            txn::{
                handlers::add_partitions_to_txn::test_support::{topic, topic_result},
                state::TxnEntry,
            },
        },
        assert2::assert,
    };

    #[test]
    fn verify_only_codes_present_vs_absent() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        let present = TopicPartition {
            topic: "a".into(),
            partition: PartitionIndex(0),
        };
        e.partitions.insert(present.clone());
        let absent = TopicPartition {
            topic: "b".into(),
            partition: PartitionIndex(0),
        };
        let identity = (krabka_log::ProducerId(1), 0);
        assert!(verify_partition_code(&e, identity, &present) == codes::NONE);
        assert!(verify_partition_code(&e, identity, &absent) == codes::TRANSACTION_ABORTABLE);
    }

    #[test]
    fn verify_partitions_preserves_topic_and_partition_rows() {
        let mut e = TxnEntry::new_empty("t".into(), krabka_log::ProducerId(1), 0, 30_000, 0);
        e.partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: PartitionIndex(1),
        });
        let topics = vec![topic("alpha", &[1, 2]), topic("denied", &[3])];
        let denied = maplit::hashset! {"denied".to_string()};

        let frozen = maplit::hashset! {"frozen".to_string()};
        let topics = [topics, vec![topic("frozen", &[4])]].concat();

        let rows = verify_partitions(
            &e,
            (krabka_log::ProducerId(1), 0),
            &topics,
            (&denied, &frozen),
        );

        let expected = vec![
            topic_result("alpha", &[
                (1, codes::NONE),
                (2, codes::TRANSACTION_ABORTABLE),
            ]),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(4, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn per_topic_with_refusals_preserves_rows_and_overrides_refused_topics() {
        let topics = vec![
            topic("alpha", &[1, 2]),
            topic("denied", &[5]),
            topic("frozen", &[6]),
        ];
        let denied = maplit::hashset! {"denied".to_string()};
        let frozen = maplit::hashset! {"frozen".to_string()};

        let rows = per_topic_with_refusals(&topics, &denied, &frozen, codes::NOT_COORDINATOR);

        let expected = vec![
            topic_result("alpha", &[
                (1, codes::NOT_COORDINATOR),
                (2, codes::NOT_COORDINATOR),
            ]),
            topic_result("denied", &[(5, codes::TOPIC_AUTHORIZATION_FAILED)]),
            topic_result("frozen", &[(6, codes::POLICY_VIOLATION)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn topic_error_preserves_each_requested_partition() {
        let topics = vec![topic("alpha", &[4, 5])];

        let rows = topic_error(&topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

        let expected = vec![topic_result("alpha", &[
            (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            (5, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        ])];
        assert!(rows == expected);
    }

    /// #883: `TransactionalId`, the topic `Name`, and `PartitionIndex` are
    /// all `mapKey: true` fields of `AddPartitionsToTxnResponse`, so a
    /// duplicated request entry must collapse to one response row.
    #[test]
    fn dedup_topics_collapses_duplicate_topics_and_partitions() {
        type Case<'a> = (
            &'a str,
            Vec<AddPartitionsToTxnTopic>,
            Vec<(&'a str, Vec<i32>)>,
        );
        let cases: Vec<Case> = vec![
            (
                "duplicate partition index within one topic",
                vec![topic("a", &[0, 0])],
                vec![("a", vec![0])],
            ),
            (
                "the same topic named twice",
                vec![topic("a", &[0]), topic("a", &[1])],
                vec![("a", vec![0, 1])],
            ),
            (
                "the same topic and partition named twice",
                vec![topic("a", &[0]), topic("a", &[0])],
                vec![("a", vec![0])],
            ),
            (
                "distinct topics are unaffected, in first-occurrence order",
                vec![topic("b", &[0]), topic("a", &[0])],
                vec![("b", vec![0]), ("a", vec![0])],
            ),
        ];
        for (label, topics, expected) in cases {
            let deduped = dedup_topics(&topics);
            let names_and_partitions: Vec<(&str, Vec<i32>)> = deduped
                .iter()
                .map(|t| (t.name.as_str(), t.partitions.clone()))
                .collect();
            assert!(names_and_partitions == expected, "{label}");
        }
    }
}
