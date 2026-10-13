//! The topic and partition rows of a `DescribeShareGroupOffsets` response.
//!
//! This is Kafka's `GroupCoordinatorService.describeShareGroupOffsets` and
//! `computeShareGroupLagAndBuildResponse`: the start offset, leader epoch and
//! delivery complete count come from the share-state summary, the lag from
//! the partition end offset its leader reports, and a topic the metadata
//! image does not hold is answered as having no data rather than as an
//! error. The `TOPIC_AUTHORIZATION_FAILED` rows of `KafkaApis` are built here
//! as well, so every row shape of the response lives in one place.

use {
    super::end_offsets::{
        EndOffset, ErrorAnswer, LatestOffsets, TopicPartition, UNKNOWN_SERVER_ERROR_MESSAGE,
        lookup_error,
    },
    crate::{
        codes,
        error::BrokerError,
        share_coordinator::{
            coordinator::{ShareStateSummary, UNINITIALIZED_START_OFFSET},
            persister_client::SharePersister,
        },
    },
    futures_util::future::join_all,
    krabka_protocol::{
        owned::{
            describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic,
            describe_share_group_offsets_response::{
                DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    },
};

/// Kafka's message for `TOPIC_AUTHORIZATION_FAILED`
/// (`Errors.TOPIC_AUTHORIZATION_FAILED.message()`).
const TOPIC_AUTHORIZATION_FAILED_MESSAGE: &str = "Topic authorization failed.";

/// Kafka's `PartitionFactory.DEFAULT_LEADER_EPOCH`.
const DEFAULT_LEADER_EPOCH: i32 = 0;
/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;
/// Kafka's `PartitionFactory.UNINITIALIZED_LAG`.
const UNINITIALIZED_LAG: i64 = -1;

/// Kafka's `Persister.readSummary`, one partition at a time.
pub(super) trait ShareSummaries {
    /// The share-state summary of `(group, topic_id, partition)`: `None` for
    /// a key with no state, or the error of the read.
    async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, BrokerError>;
}

impl ShareSummaries for SharePersister {
    async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, BrokerError> {
        SharePersister::read_summary(self, group, topic_id, partition).await
    }
}

/// Topics without a visible id use the same envelope, with caller-specific partition rows.
fn unidentified_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
    row: impl FnMut(i32) -> DescribeShareGroupOffsetsResponsePartition,
) -> DescribeShareGroupOffsetsResponseTopic {
    DescribeShareGroupOffsetsResponseTopic {
        topic_name: topic.topic_name,
        topic_id: Uuid::default(),
        partitions: topic.partitions.into_iter().map(row).collect(),
        ..Default::default()
    }
}

/// The response row of a topic the request named explicitly, but which the
/// caller may not `Describe`, as `KafkaApis.describeShareGroupOffsetsForGroup`
/// builds it.
///
/// Every partition the request named answers `TOPIC_AUTHORIZATION_FAILED`,
/// with start offset and lag `-1`, the leader epoch Kafka leaves at its
/// default of 0, and the all-zero topic id. An empty `partitions` list gives
/// a row with no partitions.
pub(super) fn unauthorized_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    unidentified_topic(topic, |partition_index| {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: -1,
            leader_epoch: DEFAULT_LEADER_EPOCH,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.to_owned()),
            ..Default::default()
        }
    })
}

/// A topic of the request the image holds, with the partitions it named.
struct FoundTopic {
    topic_id: uuid::Uuid,
    name: String,
    partitions: Vec<i32>,
}

/// The share-state summary of one partition, as
/// `DefaultStatePersister.readSummary` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionSummary {
    start_offset: i64,
    leader_epoch: i32,
    delivery_complete_count: i32,
    /// The read of the summary failed.
    failed: bool,
}

impl PartitionSummary {
    fn from_read(read: &Result<Option<ShareStateSummary>, BrokerError>) -> Self {
        match *read {
            Ok(Some((_, leader_epoch, start_offset, delivery_complete_count))) => Self {
                start_offset: start_offset.0,
                leader_epoch,
                delivery_complete_count,
                failed: false,
            },
            // `ShareCoordinatorShard.readStateSummary` for a key with no state.
            Ok(None) => Self {
                start_offset: UNINITIALIZED_START_OFFSET,
                leader_epoch: DEFAULT_LEADER_EPOCH,
                delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                failed: false,
            },
            Err(_) => Self {
                start_offset: UNINITIALIZED_START_OFFSET,
                leader_epoch: DEFAULT_LEADER_EPOCH,
                delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                failed: true,
            },
        }
    }

    /// Kafka's `shouldComputeSharePartitionLag`: the read succeeded and both
    /// the start offset and the delivery complete count are initialized.
    fn has_lag(self) -> bool {
        !self.failed
            && self.start_offset != UNINITIALIZED_START_OFFSET
            && self.delivery_complete_count != UNINITIALIZED_DELIVERY_COMPLETE_COUNT
    }
}

/// The topic rows of one group, as Kafka's
/// `GroupCoordinatorService.describeShareGroupOffsets` builds them.
///
/// A topic the image holds is read from the share state; its lag is the
/// partition end offset less the start offset and the delivery complete
/// count, and a partition whose end offset lookup fails carries that error.
/// A partition whose state is missing or cannot be read has start offset
/// `-1`, leader epoch 0 and lag `-1`, with no error. A topic the image does
/// not hold answers each partition it named with start offset `-1`, leader
/// epoch 0, lag `-1` and no error, and those rows follow the rows of the
/// topics the image holds.
///
/// A topic named twice is one row, and a partition named twice is one row,
/// as the maps Kafka collects them in make them. Kafka orders the rows of the
/// topics the image holds by the iteration order of those maps; they keep
/// the order the request first names them in here.
///
/// # Errors
///
/// Returns `UNKNOWN_SERVER_ERROR` with Kafka's message when
/// `DefaultStatePersister` refuses the read before it starts: an empty group
/// id, a topic the image holds named with no partitions, or a negative
/// partition.
pub(super) async fn describe_topics(
    summaries: &impl ShareSummaries,
    end_offsets: &impl LatestOffsets,
    image: &krabka_metadata::MetadataImage,
    group: &str,
    topics: Vec<DescribeShareGroupOffsetsRequestTopic>,
) -> Result<Vec<DescribeShareGroupOffsetsResponseTopic>, ErrorAnswer> {
    let mut found: Vec<FoundTopic> = Vec::new();
    let mut missing: Vec<DescribeShareGroupOffsetsResponseTopic> = Vec::new();
    for topic in topics {
        let Some(topic_id) = image.topic(&topic.topic_name).map(|t| t.topic_id) else {
            missing.push(missing_topic(topic));
            continue;
        };
        // `DefaultStatePersister.validate` checks each topic entry of the
        // read, before entries naming one topic are merged.
        if topic.partitions.is_empty() || topic.partitions.iter().any(|p| *p < 0) {
            return Err(ErrorAnswer {
                code: codes::UNKNOWN_SERVER_ERROR,
                message: UNKNOWN_SERVER_ERROR_MESSAGE,
            });
        }
        let index = found
            .iter()
            .position(|entry| entry.topic_id == topic_id)
            .unwrap_or_else(|| {
                found.push(FoundTopic {
                    topic_id,
                    name: topic.topic_name,
                    partitions: Vec::new(),
                });
                found.len() - 1
            });
        let entry = &mut found[index];
        for partition in topic.partitions {
            if !entry.partitions.contains(&partition) {
                entry.partitions.push(partition);
            }
        }
    }
    if found.is_empty() {
        return Ok(missing);
    }
    if group.is_empty() {
        return Err(ErrorAnswer {
            code: codes::UNKNOWN_SERVER_ERROR,
            message: UNKNOWN_SERVER_ERROR_MESSAGE,
        });
    }

    let reads = found.iter().flat_map(|topic| {
        topic
            .partitions
            .iter()
            .map(|partition| summaries.read_summary(group, topic.topic_id, *partition))
    });
    let mut read = join_all(reads)
        .await
        .into_iter()
        .map(|read| PartitionSummary::from_read(&read));
    let summaries: Vec<Vec<PartitionSummary>> = found
        .iter()
        .map(|topic| read.by_ref().take(topic.partitions.len()).collect())
        .collect();

    let lagging: Vec<TopicPartition> = found
        .iter()
        .zip(&summaries)
        .flat_map(|(topic, summaries)| {
            topic
                .partitions
                .iter()
                .zip(summaries)
                .filter(|(_, summary)| summary.has_lag())
                .map(|(partition, _)| TopicPartition {
                    topic: topic.name.clone(),
                    partition: *partition,
                })
        })
        .collect();
    let offsets = if lagging.is_empty() {
        std::collections::HashMap::new()
    } else {
        end_offsets.latest_offsets(&lagging).await
    };

    let mut rows: Vec<DescribeShareGroupOffsetsResponseTopic> = found
        .into_iter()
        .zip(summaries)
        .map(|(topic, summaries)| {
            let partitions = topic
                .partitions
                .iter()
                .zip(summaries)
                .map(|(partition, summary)| {
                    let end_offset = summary.has_lag().then(|| {
                        offsets
                            .get(&TopicPartition {
                                topic: topic.name.clone(),
                                partition: *partition,
                            })
                            .copied()
                            .unwrap_or(Err(codes::UNKNOWN_SERVER_ERROR))
                    });
                    partition_row(*partition, summary, end_offset)
                })
                .collect();
            DescribeShareGroupOffsetsResponseTopic {
                topic_name: topic.name,
                topic_id: Uuid(*topic.topic_id.as_bytes()),
                partitions,
                ..Default::default()
            }
        })
        .collect();
    rows.extend(missing);
    Ok(rows)
}

/// The row of a topic the image does not hold: no data for each partition
/// the request named.
fn missing_topic(
    topic: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    unidentified_topic(topic, |partition_index| {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: UNINITIALIZED_START_OFFSET,
            leader_epoch: DEFAULT_LEADER_EPOCH,
            lag: UNINITIALIZED_LAG,
            ..Default::default()
        }
    })
}

/// One partition row of a topic the image holds. `end_offset` is the lookup
/// of the partition end offset, made only for a partition with a lag.
fn partition_row(
    partition_index: i32,
    summary: PartitionSummary,
    end_offset: Option<EndOffset>,
) -> DescribeShareGroupOffsetsResponsePartition {
    match end_offset {
        None => DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: summary.start_offset,
            leader_epoch: summary.leader_epoch,
            lag: UNINITIALIZED_LAG,
            ..Default::default()
        },
        Some(Ok(end_offset)) => DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: summary.start_offset,
            leader_epoch: summary.leader_epoch,
            lag: end_offset - summary.start_offset - i64::from(summary.delivery_complete_count),
            ..Default::default()
        },
        // Kafka sets only the index and the error on this row, so the start
        // offset and the leader epoch keep the schema default of 0.
        Some(Err(code)) => {
            let ErrorAnswer {
                code: error_code,
                message,
            } = lookup_error(code);
            DescribeShareGroupOffsetsResponsePartition {
                partition_index,
                error_code,
                error_message: Some(message.to_owned()),
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_log::Offset,
        krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord},
        std::collections::HashMap,
    };

    const ORDERS: uuid::Uuid = uuid::Uuid::from_u128(0xD5C0);
    const AUDIT: uuid::Uuid = uuid::Uuid::from_u128(0xA0D1);

    /// A share-state key: a topic id and a partition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct SummaryKey {
        topic_id: uuid::Uuid,
        partition: i32,
    }

    /// A stored summary: `None` makes a read of its key fail.
    struct StoredSummary {
        key: SummaryKey,
        summary: Option<ShareStateSummary>,
    }

    /// The stored summary of `orders` partition 0.
    fn orders_summary(summary: Option<ShareStateSummary>) -> StoredSummary {
        StoredSummary {
            key: SummaryKey {
                topic_id: ORDERS,
                partition: 0,
            },
            summary,
        }
    }

    /// The end offset a lookup answers for a partition.
    struct EndAnswer {
        partition: TopicPartition,
        end_offset: EndOffset,
    }

    /// A case name, the group, the request topics, the stored summaries, the
    /// end offsets, and the whole answer.
    struct ShareOffsetsCase {
        name: &'static str,
        group: &'static str,
        topics: Vec<DescribeShareGroupOffsetsRequestTopic>,
        summaries: Vec<StoredSummary>,
        ends: Vec<EndAnswer>,
        want: Result<Vec<DescribeShareGroupOffsetsResponseTopic>, ErrorAnswer>,
    }

    /// Share-state summaries keyed by `(topic id, partition)`: `Ok(None)`
    /// for a key with no entry, and an error for a key mapped to `None`.
    struct Summaries(HashMap<SummaryKey, Option<ShareStateSummary>>);

    impl ShareSummaries for Summaries {
        fn read_summary(
            &self,
            _group: &str,
            topic_id: uuid::Uuid,
            partition: i32,
        ) -> impl Future<Output = Result<Option<ShareStateSummary>, BrokerError>> {
            std::future::ready(
                match self.0.get(&SummaryKey {
                    topic_id,
                    partition,
                }) {
                    None => Ok(None),
                    Some(Some(summary)) => Ok(Some(*summary)),
                    Some(None) => Err(BrokerError::Share("share coordinator down".into())),
                },
            )
        }
    }

    /// End offsets keyed by partition; a partition with no entry fails with
    /// `NETWORK_EXCEPTION`.
    struct Ends(HashMap<TopicPartition, EndOffset>);

    impl LatestOffsets for Ends {
        fn latest_offsets(
            &self,
            partitions: &[TopicPartition],
        ) -> impl Future<Output = HashMap<TopicPartition, EndOffset>> {
            std::future::ready(
                partitions
                    .iter()
                    .map(|partition| {
                        let end = self
                            .0
                            .get(partition)
                            .copied()
                            .unwrap_or(Err(codes::NETWORK_EXCEPTION));
                        (partition.clone(), end)
                    })
                    .collect(),
            )
        }
    }

    fn image() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (name, topic_id) in [("orders", ORDERS), ("audit", AUDIT)] {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 4,
                replication_factor: 1,
            }));
        }
        image
    }

    fn topic(name: &str, partitions: &[i32]) -> DescribeShareGroupOffsetsRequestTopic {
        DescribeShareGroupOffsetsRequestTopic {
            topic_name: name.into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }
    }

    fn row(
        name: &str,
        topic_id: uuid::Uuid,
        partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
    ) -> DescribeShareGroupOffsetsResponseTopic {
        DescribeShareGroupOffsetsResponseTopic {
            topic_name: name.into(),
            topic_id: Uuid(*topic_id.as_bytes()),
            partitions,
            ..Default::default()
        }
    }

    fn part(
        partition_index: i32,
        start_offset: i64,
        leader_epoch: i32,
        lag: i64,
    ) -> DescribeShareGroupOffsetsResponsePartition {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset,
            leader_epoch,
            lag,
            ..Default::default()
        }
    }

    fn no_data(partition_index: i32) -> DescribeShareGroupOffsetsResponsePartition {
        part(partition_index, -1, 0, -1)
    }

    /// Kafka's `describeShareGroupOffsets` and
    /// `computeShareGroupLagAndBuildResponse`, one row per case: the group,
    /// the request topics, the stored summaries, the end offsets, and the
    /// whole answer.
    #[tokio::test]
    async fn rows_follow_kafkas_share_group_offsets_description() {
        let orders = |p: i32| TopicPartition {
            topic: "orders".to_owned(),
            partition: p,
        };
        let unknown_server_error = || {
            Err(ErrorAnswer {
                code: codes::UNKNOWN_SERVER_ERROR,
                message: UNKNOWN_SERVER_ERROR_MESSAGE,
            })
        };
        let cases: Vec<ShareOffsetsCase> = vec![
            ShareOffsetsCase {
                name: "lag is the end offset less the start offset and the delivered count",
                group: "g",
                topics: vec![topic("orders", &[0])],
                summaries: vec![orders_summary(Some((1, 4, Offset(21), 10)))],
                ends: vec![EndAnswer {
                    partition: orders(0),
                    end_offset: Ok(40),
                }],
                want: Ok(vec![row("orders", ORDERS, vec![part(0, 21, 4, 9)])]),
            },
            ShareOffsetsCase {
                name: "a partition with no state has no data and no lag",
                group: "g",
                topics: vec![topic("orders", &[1])],
                summaries: vec![],
                ends: vec![],
                want: Ok(vec![row("orders", ORDERS, vec![no_data(1)])]),
            },
            ShareOffsetsCase {
                name: "an uninitialized delivered count has no lag",
                group: "g",
                topics: vec![topic("orders", &[0])],
                summaries: vec![orders_summary(Some((1, 2, Offset(5), -1)))],
                ends: vec![],
                want: Ok(vec![row("orders", ORDERS, vec![part(0, 5, 2, -1)])]),
            },
            ShareOffsetsCase {
                name: "a failed state read has no data and no error",
                group: "g",
                topics: vec![topic("orders", &[0])],
                summaries: vec![orders_summary(None)],
                ends: vec![],
                want: Ok(vec![row("orders", ORDERS, vec![no_data(0)])]),
            },
            ShareOffsetsCase {
                name: "a failed end offset lookup carries its error",
                group: "g",
                topics: vec![topic("orders", &[0])],
                summaries: vec![orders_summary(Some((1, 4, Offset(21), 10)))],
                ends: vec![],
                want: Ok(vec![row("orders", ORDERS, vec![
                    DescribeShareGroupOffsetsResponsePartition {
                        partition_index: 0,
                        error_code: codes::NETWORK_EXCEPTION,
                        error_message: Some(
                            "The server disconnected before a response was received.".into(),
                        ),
                        ..Default::default()
                    },
                ])]),
            },
            ShareOffsetsCase {
                name: "an unknown topic has no data, after the known topics",
                group: "g",
                topics: vec![topic("gone", &[3, 5]), topic("orders", &[1])],
                summaries: vec![],
                ends: vec![],
                want: Ok(vec![
                    row("orders", ORDERS, vec![no_data(1)]),
                    row("gone", uuid::Uuid::nil(), vec![no_data(3), no_data(5)]),
                ]),
            },
            ShareOffsetsCase {
                name: "an unknown topic with no partitions has a row with none",
                group: "g",
                topics: vec![topic("gone", &[])],
                summaries: vec![],
                ends: vec![],
                want: Ok(vec![row("gone", uuid::Uuid::nil(), vec![])]),
            },
            ShareOffsetsCase {
                name: "an unknown topic needs no group id",
                group: "",
                topics: vec![topic("gone", &[0])],
                summaries: vec![],
                ends: vec![],
                want: Ok(vec![row("gone", uuid::Uuid::nil(), vec![no_data(0)])]),
            },
            ShareOffsetsCase {
                name: "a topic and a partition named twice are one row each",
                group: "g",
                topics: vec![
                    topic("orders", &[1, 0, 1]),
                    topic("audit", &[0]),
                    topic("orders", &[2]),
                ],
                summaries: vec![],
                ends: vec![],
                want: Ok(vec![
                    row("orders", ORDERS, vec![no_data(1), no_data(0), no_data(2)]),
                    row("audit", AUDIT, vec![no_data(0)]),
                ]),
            },
            ShareOffsetsCase {
                name: "a known topic with no partitions fails the group",
                group: "g",
                topics: vec![topic("orders", &[0]), topic("orders", &[])],
                summaries: vec![],
                ends: vec![],
                want: unknown_server_error(),
            },
            ShareOffsetsCase {
                name: "a negative partition fails the group",
                group: "g",
                topics: vec![topic("orders", &[-1])],
                summaries: vec![],
                ends: vec![],
                want: unknown_server_error(),
            },
            ShareOffsetsCase {
                name: "an empty group id fails a known topic",
                group: "",
                topics: vec![topic("orders", &[0])],
                summaries: vec![],
                ends: vec![],
                want: unknown_server_error(),
            },
        ];
        let image = image();
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for ShareOffsetsCase {
            name: case,
            group,
            topics,
            summaries,
            ends,
            want,
        } in cases
        {
            let summaries = Summaries(
                summaries
                    .into_iter()
                    .map(|stored| (stored.key, stored.summary))
                    .collect(),
            );
            let ends = Ends(
                ends.into_iter()
                    .map(|answer| (answer.partition, answer.end_offset))
                    .collect(),
            );
            actual.push((
                case,
                describe_topics(&summaries, &ends, &image, group, topics).await,
            ));
            expected.push((case, want));
        }
        assert!(actual == expected);
    }

    #[test]
    fn an_unauthorized_topic_answers_each_partition_it_named() {
        let denied = |partition_index| DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset: -1,
            leader_epoch: 0,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".into()),
            ..Default::default()
        };
        let actual = [
            unauthorized_topic(topic("secret", &[1, 0])),
            unauthorized_topic(topic("secret", &[])),
        ];
        let expected = [
            row("secret", uuid::Uuid::nil(), vec![denied(1), denied(0)]),
            row("secret", uuid::Uuid::nil(), vec![]),
        ];
        assert!(actual == expected);
    }
}
