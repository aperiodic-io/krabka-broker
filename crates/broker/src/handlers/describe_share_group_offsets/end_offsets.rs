//! The partition end offsets a `DescribeShareGroupOffsets` lag is computed
//! from.
//!
//! Kafka's `GroupCoordinatorService` asks `NetworkPartitionMetadataClient`
//! for them: one `ListOffsets` at `LATEST_TIMESTAMP`, `read_uncommitted`, to
//! the leader of each partition over the inter-broker listener, and a
//! per-partition error code when the leader cannot answer. It does so even
//! when this broker leads the partition, and so does krabka, so the lag is
//! the same number whichever broker the admin client reaches.

use std::{collections::HashMap, time::Duration};

use futures_util::future::join_all;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::owned::{
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
};

use crate::{broker::Broker, codes};

/// A partition by topic name, as `ListOffsets` names it.
pub(super) type TopicPartition = (String, i32);

/// The end offset of a partition, or the error code of the lookup.
pub(super) type EndOffset = Result<i64, i16>;

/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;

/// Kafka's `ListOffsetsRequest.CONSUMER_REPLICA_ID`: the request is answered
/// as a consumer's, at the high watermark.
const CONSUMER_REPLICA_ID: i32 = -1;

/// Kafka's `IsolationLevel.READ_UNCOMMITTED`.
const READ_UNCOMMITTED: i8 = 0;

/// How long one leader may take to answer, Kafka's socket connection setup
/// ceiling, `CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS`,
/// which `NetworkPartitionMetadataClient` sizes its send thread with.
const LEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// Kafka's `PartitionMetadataClient.listLatestOffsets`.
pub(super) trait LatestOffsets {
    /// The end offset of each of `partitions`.
    ///
    /// Every partition has an entry: its offset, or the error code its leader
    /// answered or its lookup failed with.
    async fn latest_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> HashMap<TopicPartition, EndOffset>;
}

/// The `ListOffsets` client over the inter-broker listener. It finds each
/// leader in the broker's current metadata image, as Kafka's client reads
/// its `MetadataCache`.
pub(super) struct NetworkLatestOffsets<'a> {
    pub(super) broker: &'a Broker,
}

impl LatestOffsets for NetworkLatestOffsets<'_> {
    async fn latest_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> HashMap<TopicPartition, EndOffset> {
        let image = self.broker.controller.current_image();
        let (by_leader, mut offsets) = group_by_leader(&image, partitions);
        let answers = join_all(
            by_leader
                .into_iter()
                .map(|(leader, partitions)| self.ask_leader(&image, leader, partitions)),
        )
        .await;
        offsets.extend(answers.into_iter().flatten());
        offsets
    }
}

impl NetworkLatestOffsets<'_> {
    /// Sends one `ListOffsets` for `partitions` to `leader`.
    // cargo-mutants: dials a broker through the shared `InterBrokerClient`;
    // the request it sends and the reading of the reply are tested on their
    // own.
    #[cfg_attr(test, mutants::skip)]
    async fn ask_leader(
        &self,
        image: &MetadataImage,
        leader: NodeId,
        partitions: Vec<TopicPartition>,
    ) -> HashMap<TopicPartition, EndOffset> {
        let config = &self.broker.config;
        let failed = |code| {
            partitions
                .iter()
                .map(|partition| (partition.clone(), Err(code)))
                .collect()
        };
        let Some(registration) = image.broker(leader) else {
            return failed(codes::LEADER_NOT_AVAILABLE);
        };
        let (host, port) = crate::broker::registered_listener_endpoint(
            registration,
            &config.inter_broker_listener_name,
        );
        let host = host.to_owned();
        let protocol = config
            .effective_listeners()
            .iter()
            .find(|listener| listener.name == config.inter_broker_listener_name)
            .map_or(krabka_security::ListenerProtocol::Plaintext, |listener| {
                listener.protocol
            });
        let options = krabka_client_core::ConnectionOptions {
            client_id: format!("krabka-broker-share-lag-{}", config.node_id),
            ..krabka_client_core::ConnectionOptions::default()
        };
        let exchange = async {
            let connection = self
                .broker
                .inter_broker_client
                .connect_as_connection(
                    &host,
                    port,
                    protocol,
                    &config.inter_broker_server_name,
                    options,
                )
                .await
                .ok()?;
            let response = connection.send(latest_offsets_request(&partitions)).await;
            connection.close();
            response.ok()
        };
        match tokio::time::timeout(LEADER_TIMEOUT, exchange).await {
            Ok(Some(response)) => offsets_from(&response, &partitions),
            Ok(None) => failed(codes::NETWORK_EXCEPTION),
            Err(_) => failed(codes::REQUEST_TIMED_OUT),
        }
    }
}

/// Partitions grouped by their leader, in the order each leader is first
/// seen.
type ByLeader = Vec<(NodeId, Vec<TopicPartition>)>;

/// `partitions` grouped by the leader the image names, and
/// `LEADER_NOT_AVAILABLE` for each partition the image has no leader of, as
/// `getPartitionLeaderEndpoint` answers it.
fn group_by_leader(
    image: &MetadataImage,
    partitions: &[TopicPartition],
) -> (ByLeader, HashMap<TopicPartition, EndOffset>) {
    let mut by_leader: ByLeader = Vec::new();
    let mut leaderless = HashMap::new();
    for partition in partitions {
        let Some(record) = image.partition(&partition.0, partition.1) else {
            leaderless.insert(partition.clone(), Err(codes::LEADER_NOT_AVAILABLE));
            continue;
        };
        match by_leader
            .iter_mut()
            .find(|(leader, _)| *leader == record.leader)
        {
            Some((_, led)) => led.push(partition.clone()),
            None => by_leader.push((record.leader, vec![partition.clone()])),
        }
    }
    (by_leader, leaderless)
}

/// The request `NetworkPartitionMetadataClient.createListOffsetsRequest`
/// builds: `ListOffsetsRequest.Builder.forConsumer(true, READ_UNCOMMITTED)`
/// at `LATEST_TIMESTAMP`, with no current leader epoch, the partitions of
/// each topic in one topic entry.
fn latest_offsets_request(partitions: &[TopicPartition]) -> ListOffsetsRequest {
    let mut topics: Vec<ListOffsetsTopic> = Vec::new();
    for (topic, partition) in partitions {
        let row = ListOffsetsPartition {
            partition_index: *partition,
            current_leader_epoch: -1,
            timestamp: LATEST_TIMESTAMP,
            ..ListOffsetsPartition::default()
        };
        match topics.iter_mut().find(|entry| entry.name == *topic) {
            Some(entry) => entry.partitions.push(row),
            None => topics.push(ListOffsetsTopic {
                name: topic.clone(),
                partitions: vec![row],
                ..ListOffsetsTopic::default()
            }),
        }
    }
    ListOffsetsRequest {
        replica_id: CONSUMER_REPLICA_ID,
        isolation_level: READ_UNCOMMITTED,
        topics,
        ..ListOffsetsRequest::default()
    }
}

/// Each of `partitions` read out of `response`, as
/// `NetworkPartitionMetadataClient.handleResponse` reads it: the row's offset
/// or error code, and `UNKNOWN_TOPIC_OR_PARTITION` for a partition the
/// response has no row for.
fn offsets_from(
    response: &ListOffsetsResponse,
    partitions: &[TopicPartition],
) -> HashMap<TopicPartition, EndOffset> {
    partitions
        .iter()
        .map(|(topic, partition)| {
            let row = response
                .topics
                .iter()
                .filter(|entry| entry.name == *topic)
                .flat_map(|entry| &entry.partitions)
                .find(|row| row.partition_index == *partition);
            let offset = match row {
                Some(row) if row.error_code == codes::NONE => Ok(row.offset),
                Some(row) => Err(row.error_code),
                None => Err(codes::UNKNOWN_TOPIC_OR_PARTITION),
            };
            ((topic.clone(), *partition), offset)
        })
        .collect()
}

/// Kafka's `Errors.forCode(code).message()` for the codes a `ListOffsets`
/// lookup of the end offset answers with. Kafka reads a code it does not
/// know as `UNKNOWN_SERVER_ERROR`, and so does this.
pub(super) fn lookup_error(code: i16) -> (i16, &'static str) {
    let message = match code {
        codes::UNKNOWN_TOPIC_OR_PARTITION => "This server does not host this topic-partition.",
        codes::LEADER_NOT_AVAILABLE => {
            "There is no leader for this topic-partition as we are in the middle of a leadership \
             election."
        }
        codes::NOT_LEADER_OR_FOLLOWER => {
            "For requests intended only for the leader, this error indicates that the broker is \
             not the current leader. For requests intended for any replica, this error indicates \
             that the broker is not a replica of the topic partition."
        }
        codes::REQUEST_TIMED_OUT => "The request timed out.",
        codes::NETWORK_EXCEPTION => "The server disconnected before a response was received.",
        codes::TOPIC_AUTHORIZATION_FAILED => "Topic authorization failed.",
        codes::UNSUPPORTED_VERSION => "The version of API is not supported.",
        codes::INVALID_REQUEST => {
            "This most likely occurs because of a request being malformed by the client library \
             or the message was sent to an incompatible broker. See the broker logs for more \
             details."
        }
        codes::KAFKA_STORAGE_ERROR => "Disk error when trying to access log file on the disk.",
        codes::FENCED_LEADER_EPOCH => {
            "The leader epoch in the request is older than the epoch on the broker."
        }
        codes::UNKNOWN_LEADER_EPOCH => {
            "The leader epoch in the request is newer than the epoch on the broker."
        }
        codes::OFFSET_NOT_AVAILABLE => {
            "The leader high watermark has not caught up from a recent leader election so the \
             offsets cannot be guaranteed to be monotonically increasing."
        }
        _ => return (codes::UNKNOWN_SERVER_ERROR, UNKNOWN_SERVER_ERROR_MESSAGE),
    };
    (code, message)
}

/// Kafka's `Errors.UNKNOWN_SERVER_ERROR.message()`.
pub(super) const UNKNOWN_SERVER_ERROR_MESSAGE: &str =
    "The server experienced an unexpected error when processing the request.";

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_protocol::owned::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        (topic.to_owned(), partition)
    }

    /// `NetworkPartitionMetadataClient.createListOffsetsRequest`: a consumer
    /// request at `LATEST_TIMESTAMP`, `read_uncommitted`, one entry per topic.
    #[test]
    fn the_request_asks_for_each_latest_offset_as_a_consumer() {
        let request = latest_offsets_request(&[tp("a", 1), tp("b", 0), tp("a", 0)]);
        let row = |partition_index| ListOffsetsPartition {
            partition_index,
            current_leader_epoch: -1,
            timestamp: -1,
            ..ListOffsetsPartition::default()
        };
        let expected = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![
                ListOffsetsTopic {
                    name: "a".into(),
                    partitions: vec![row(1), row(0)],
                    ..ListOffsetsTopic::default()
                },
                ListOffsetsTopic {
                    name: "b".into(),
                    partitions: vec![row(0)],
                    ..ListOffsetsTopic::default()
                },
            ],
            ..ListOffsetsRequest::default()
        };
        assert!(request == expected);
    }

    /// `NetworkPartitionMetadataClient.handleResponse`: an answered row
    /// gives its offset or its error, and a partition with no row is
    /// `UNKNOWN_TOPIC_OR_PARTITION`.
    #[test]
    fn each_partition_reads_its_row_of_the_response() {
        let row = |partition_index, error_code, offset| ListOffsetsPartitionResponse {
            partition_index,
            error_code,
            offset,
            ..ListOffsetsPartitionResponse::default()
        };
        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: "a".into(),
                partitions: vec![
                    row(0, codes::NONE, 42),
                    row(1, codes::NOT_LEADER_OR_FOLLOWER, -1),
                ],
                ..ListOffsetsTopicResponse::default()
            }],
            ..ListOffsetsResponse::default()
        };
        let offsets = offsets_from(&response, &[tp("a", 0), tp("a", 1), tp("a", 2), tp("b", 0)]);
        let expected = HashMap::from([
            (tp("a", 0), Ok(42)),
            (tp("a", 1), Err(codes::NOT_LEADER_OR_FOLLOWER)),
            (tp("a", 2), Err(codes::UNKNOWN_TOPIC_OR_PARTITION)),
            (tp("b", 0), Err(codes::UNKNOWN_TOPIC_OR_PARTITION)),
        ]);
        assert!(offsets == expected);
    }

    /// Partitions go to the leader the image names, and one the image does
    /// not hold is `LEADER_NOT_AVAILABLE`.
    #[test]
    fn partitions_are_grouped_by_their_leader() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "a".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 3,
            replication_factor: 1,
        }));
        for (partition, leader) in [(0, 1), (1, 2), (2, 1)] {
            image.apply(&MetadataRecord::V1Partition(
                crate::handlers::test_support::single_replica_partition(
                    "a",
                    krabka_ids::PartitionIndex(partition),
                    NodeId(leader),
                ),
            ));
        }
        let grouped = group_by_leader(&image, &[tp("a", 0), tp("a", 1), tp("a", 2), tp("a", 3)]);
        let expected = (
            vec![
                (NodeId(1), vec![tp("a", 0), tp("a", 2)]),
                (NodeId(2), vec![tp("a", 1)]),
            ],
            HashMap::from([(tp("a", 3), Err(codes::LEADER_NOT_AVAILABLE))]),
        );
        assert!(grouped == expected);
    }

    #[test]
    fn an_unknown_code_reads_as_unknown_server_error() {
        let rows = [
            (
                codes::REQUEST_TIMED_OUT,
                (codes::REQUEST_TIMED_OUT, "The request timed out."),
            ),
            (
                codes::NETWORK_EXCEPTION,
                (
                    codes::NETWORK_EXCEPTION,
                    "The server disconnected before a response was received.",
                ),
            ),
            (
                i16::MAX,
                (codes::UNKNOWN_SERVER_ERROR, UNKNOWN_SERVER_ERROR_MESSAGE),
            ),
        ];
        for (code, expected) in rows {
            assert!(lookup_error(code) == expected, "{code}");
        }
    }
}
