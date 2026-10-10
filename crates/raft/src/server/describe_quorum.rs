//! `DescribeQuorum` (api key 55) on the controller listener, as Kafka's
//! `KafkaRaftClient.handleDescribeQuorumRequest` answers it.
//!
//! 1. A request that does not name exactly `__cluster_metadata` partition 0
//!    gets `UNKNOWN_TOPIC_OR_PARTITION` on every partition it names
//!    (`DescribeQuorumRequest.getPartitionLevelErrorResponse`).
//! 2. A node that is not the leader answers `NOT_LEADER_OR_FOLLOWER` for the
//!    metadata partition (`DescribeQuorumResponse.singletonErrorResponse`).
//! 3. The leader answers its epoch, high watermark, voters and observers, each
//!    replica with its log end offset, directory id and the two KIP-595
//!    timestamps, and the voters' listeners from v2
//!    (`RaftUtil.singletonDescribeQuorumResponse`).

use krabka_protocol::{
    owned::{
        common::describe_quorum_response::replica_state::ReplicaState,
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::{
            DescribeQuorumResponse, Listener, Node, PartitionData, TopicData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::kraft::{
    transport::{ObserverReplica, QuorumStateSnapshot},
    types::NodeId,
};

const METADATA_TOPIC: &str = "__cluster_metadata";
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Kafka's `Errors.UNKNOWN_TOPIC_OR_PARTITION.message()`.
const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str = "This server does not host this topic-partition.";
/// Kafka's `Errors.NOT_LEADER_OR_FOLLOWER.message()`.
const NOT_LEADER_OR_FOLLOWER_MESSAGE: &str = "For requests intended only for the leader, this \
     error indicates that the broker is not the current leader. For requests intended for any \
     replica, this error indicates that the broker is not a replica of the topic partition.";

/// The wire form of a node id, or -1 when it does not fit.
fn node_to_wire(id: NodeId) -> i32 {
    i32::try_from(id.0).unwrap_or(-1)
}

/// Kafka's `hasValidTopicPartition` for `DescribeQuorum`.
fn names_only_the_metadata_partition(request: &DescribeQuorumRequest) -> bool {
    matches!(
        request.topics.as_slice(),
        [topic] if topic.topic_name == METADATA_TOPIC
            && matches!(topic.partitions.as_slice(), [partition] if partition.partition_index == 0)
    )
}

/// One observer row: what the leader tracked of a replica that fetches
/// without being a voter, keyed by its id and directory id.
fn observer_state(observer: &ObserverReplica) -> ReplicaState {
    ReplicaState {
        replica_id: node_to_wire(observer.id),
        replica_directory_id: WireUuid(*observer.directory_id.as_bytes()),
        log_end_offset: observer.log_end_offset,
        last_fetch_timestamp: observer.last_fetch_ms,
        last_caught_up_timestamp: observer.last_caught_up_ms,
        ..Default::default()
    }
}

/// One voter row: the log end offset and the timestamps the leader tracks,
/// or -1 where it tracks none.
fn replica_state(
    quorum: &QuorumStateSnapshot,
    id: NodeId,
    directory_id: uuid::Uuid,
) -> ReplicaState {
    let tracked =
        |map: &std::collections::BTreeMap<NodeId, i64>| map.get(&id).copied().unwrap_or(-1);
    ReplicaState {
        replica_id: node_to_wire(id),
        replica_directory_id: WireUuid(*directory_id.as_bytes()),
        log_end_offset: tracked(&quorum.per_replica_fetch_offset),
        last_fetch_timestamp: tracked(&quorum.per_replica_last_fetch_ms),
        last_caught_up_timestamp: tracked(&quorum.per_replica_last_caught_up_ms),
        ..Default::default()
    }
}

/// Answers a decoded `DescribeQuorum` request from `quorum`, this node's
/// consensus snapshot.
///
/// Shared between the controller listener (this module's own caller,
/// `kip853::describe_quorum_response`) and the broker listener, which
/// forwards to the active controller and answers with this same builder once
/// it holds the leader's own snapshot (`crates/broker/src/handlers/
/// describe_quorum.rs`) -- one implementation on both listeners (#814,
/// #1034).
#[must_use]
pub fn describe_quorum(
    request: &DescribeQuorumRequest,
    quorum: &QuorumStateSnapshot,
) -> DescribeQuorumResponse {
    if !names_only_the_metadata_partition(request) {
        return DescribeQuorumResponse {
            topics: request
                .topics
                .iter()
                .map(|topic| TopicData {
                    topic_name: topic.topic_name.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| PartitionData {
                            partition_index: partition.partition_index,
                            error_code: UNKNOWN_TOPIC_OR_PARTITION,
                            error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            // Kafka's `getPartitionLevelErrorResponse` leaves the top-level
            // message at its empty default.
            error_message: Some(String::new()),
            ..Default::default()
        };
    }
    let Some(leader) = quorum.leader_id.filter(|_| quorum.is_leader) else {
        return DescribeQuorumResponse {
            topics: vec![TopicData {
                topic_name: METADATA_TOPIC.into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: NOT_LEADER_OR_FOLLOWER,
                    error_message: Some(NOT_LEADER_OR_FOLLOWER_MESSAGE.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            // Kafka's `DescribeQuorumResponse.singletonErrorResponse` leaves
            // the top-level message at its empty default.
            error_message: Some(String::new()),
            ..Default::default()
        };
    };
    DescribeQuorumResponse {
        topics: vec![TopicData {
            topic_name: METADATA_TOPIC.into(),
            partitions: vec![PartitionData {
                partition_index: 0,
                leader_id: node_to_wire(leader),
                leader_epoch: i32::try_from(quorum.leader_epoch).unwrap_or(i32::MAX),
                high_watermark: quorum.high_watermark,
                current_voters: quorum
                    .voters
                    .iter()
                    .map(|voter| replica_state(quorum, voter.id, voter.directory_id))
                    .collect(),
                observers: quorum.observers.iter().map(observer_state).collect(),
                // Kafka's `RaftUtil.singletonDescribeQuorumResponse` sets
                // neither message, so both stay at their empty default.
                error_message: Some(String::new()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        nodes: quorum
            .voters
            .iter()
            .map(|voter| Node {
                node_id: node_to_wire(voter.id),
                listeners: voter
                    .endpoints
                    .iter()
                    .map(|endpoint| Listener {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        error_message: Some(String::new()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_protocol::owned::describe_quorum_request::{
        PartitionData as RequestPartition, TopicData as RequestTopic,
    };

    use super::*;
    use crate::server::test_support::voter;

    /// Topic names, each with the partition indexes a request names.
    type RequestedTopics<'a> = &'a [(&'a str, &'a [i32])];

    fn request(topics: RequestedTopics<'_>) -> DescribeQuorumRequest {
        DescribeQuorumRequest {
            topics: topics
                .iter()
                .map(|(name, partitions)| RequestTopic {
                    topic_name: (*name).into(),
                    partitions: partitions
                        .iter()
                        .map(|&partition_index| RequestPartition {
                            partition_index,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn endpoint(port: u16) -> krabka_metadata::VoterEndpoint {
        krabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "controller".into(),
            port,
        }
    }

    /// Node 1 leading epoch 7 over voters 1 and 2, with observer 9.
    fn leader_snapshot() -> QuorumStateSnapshot {
        QuorumStateSnapshot {
            leader_id: Some(NodeId(1)),
            leader_epoch: 7,
            high_watermark: 40,
            last_applied_record_timestamp_ms: 700,
            pending_record_processing_time_us_avg: 0,
            pending_record_processing_time_us_max: 0,
            quorum_high_watermark: 40,
            log_end_offset: 42,
            log_start_offset: 0,
            voters: krabka_metadata::VoterSet::from_voters([
                voter(1, vec![endpoint(9091)]),
                voter(2, vec![endpoint(9092)]),
            ]),
            voted_directory_id: None,
            observers: vec![ObserverReplica {
                id: NodeId(9),
                directory_id: uuid::Uuid::from_u128(99),
                log_end_offset: 38,
                last_fetch_ms: 800,
                last_caught_up_ms: 700,
            }],
            per_replica_fetch_offset: BTreeMap::from([(NodeId(1), 42), (NodeId(2), 40)]),
            per_replica_last_fetch_ms: BTreeMap::from([(NodeId(1), 1_000), (NodeId(2), 900)]),
            per_replica_last_caught_up_ms: BTreeMap::from([(NodeId(1), 1_000), (NodeId(2), 850)]),
            is_leader: true,
            current_state: "leader",
        }
    }

    fn replica(
        id: i32,
        directory: u128,
        log_end_offset: i64,
        fetch: i64,
        caught_up: i64,
    ) -> ReplicaState {
        ReplicaState {
            replica_id: id,
            replica_directory_id: WireUuid(*uuid::Uuid::from_u128(directory).as_bytes()),
            log_end_offset,
            last_fetch_timestamp: fetch,
            last_caught_up_timestamp: caught_up,
            ..Default::default()
        }
    }

    #[test]
    fn the_leader_describes_every_replica() {
        let response = describe_quorum(&request(&[(METADATA_TOPIC, &[0])]), &leader_snapshot());

        check!(
            response
                == DescribeQuorumResponse {
                    topics: vec![TopicData {
                        topic_name: METADATA_TOPIC.into(),
                        partitions: vec![PartitionData {
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 7,
                            high_watermark: 40,
                            current_voters: vec![
                                replica(1, 1, 42, 1_000, 1_000),
                                replica(2, 2, 40, 900, 850),
                            ],
                            observers: vec![replica(9, 99, 38, 800, 700)],
                            error_message: Some(String::new()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    nodes: [(1, 9091), (2, 9092)]
                        .into_iter()
                        .map(|(node_id, port)| Node {
                            node_id,
                            listeners: vec![Listener {
                                name: "CONTROLLER".into(),
                                host: "controller".into(),
                                port,
                                ..Default::default()
                            }],
                            ..Default::default()
                        })
                        .collect(),
                    error_message: Some(String::new()),
                    ..Default::default()
                }
        );
    }

    /// Observers are keyed by `(id, directory id)`: one id under two directories
    /// is two rows, each with its own offset and timestamps, and a timestamp the
    /// leader has not recorded stays -1.
    #[test]
    fn one_observer_id_under_two_directories_is_two_rows() {
        let observer = |directory: u128, offset: i64, fetch: i64, caught_up: i64| ObserverReplica {
            id: NodeId(9),
            directory_id: uuid::Uuid::from_u128(directory),
            log_end_offset: offset,
            last_fetch_ms: fetch,
            last_caught_up_ms: caught_up,
        };
        let snapshot = QuorumStateSnapshot {
            observers: vec![observer(91, 30, 500, 400), observer(92, 10, 600, -1)],
            ..leader_snapshot()
        };

        let response = describe_quorum(&request(&[(METADATA_TOPIC, &[0])]), &snapshot);

        check!(
            response.topics[0].partitions[0].observers
                == vec![replica(9, 91, 30, 500, 400), replica(9, 92, 10, 600, -1)]
        );
    }

    #[test]
    fn a_node_that_is_not_the_leader_refuses() {
        let follower = QuorumStateSnapshot {
            leader_id: Some(NodeId(2)),
            is_leader: false,
            current_state: "follower",
            ..leader_snapshot()
        };

        check!(
            describe_quorum(&request(&[(METADATA_TOPIC, &[0])]), &follower)
                == DescribeQuorumResponse {
                    topics: vec![TopicData {
                        topic_name: METADATA_TOPIC.into(),
                        partitions: vec![PartitionData {
                            partition_index: 0,
                            error_code: 6,
                            error_message: Some(NOT_LEADER_OR_FOLLOWER_MESSAGE.into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    error_message: Some(String::new()),
                    ..Default::default()
                }
        );
    }

    /// A voter id too large for Kafka's `i32` node id gets -1 in its replica
    /// row and in `Nodes`, and a voter the leader has not yet heard from gets
    /// -1 log end offset and timestamps rather than a stale or default value.
    #[test]
    fn an_unmappable_or_never_fetched_voter_gets_blank_fields() {
        let huge_id = u64::from(u32::MAX) + 1;
        let snapshot = QuorumStateSnapshot {
            voters: krabka_metadata::VoterSet::from_voters([
                voter(1, vec![endpoint(9091)]),
                voter(huge_id, vec![endpoint(9093)]),
            ]),
            per_replica_fetch_offset: BTreeMap::from([(NodeId(1), 42)]),
            per_replica_last_fetch_ms: BTreeMap::from([(NodeId(1), 1_000)]),
            per_replica_last_caught_up_ms: BTreeMap::from([(NodeId(1), 1_000)]),
            observers: vec![],
            ..leader_snapshot()
        };

        let response = describe_quorum(&request(&[(METADATA_TOPIC, &[0])]), &snapshot);
        let partition = &response.topics[0].partitions[0];
        let unmapped = partition
            .current_voters
            .iter()
            .find(|voter| voter.replica_id == -1)
            .expect("the oversized id maps to -1");
        check!(unmapped.log_end_offset == -1, "never fetched");
        check!(unmapped.last_fetch_timestamp == -1);
        check!(unmapped.last_caught_up_timestamp == -1);
        check!(
            response.nodes.iter().any(|node| node.node_id == -1),
            "the same id maps to -1 in Nodes"
        );
    }

    /// Any request that does not name exactly the metadata partition gets
    /// `UNKNOWN_TOPIC_OR_PARTITION` on every partition it names, whether or
    /// not this node leads.
    #[test]
    fn a_request_for_another_partition_gets_unknown_topic_or_partition() {
        let rows: [(&str, RequestedTopics<'_>); 5] = [
            ("another topic", &[("other", &[0])]),
            ("partition 1", &[(METADATA_TOPIC, &[1])]),
            ("two partitions", &[(METADATA_TOPIC, &[0, 1])]),
            ("two topics", &[(METADATA_TOPIC, &[0]), ("other", &[0])]),
            ("no topic", &[]),
        ];
        for (label, topics) in rows {
            let expected = DescribeQuorumResponse {
                topics: topics
                    .iter()
                    .map(|(name, partitions)| TopicData {
                        topic_name: (*name).into(),
                        partitions: partitions
                            .iter()
                            .map(|&partition_index| PartitionData {
                                partition_index,
                                error_code: 3,
                                error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                error_message: Some(String::new()),
                ..Default::default()
            };
            check!(
                describe_quorum(&request(topics), &leader_snapshot()) == expected,
                "{label}"
            );
        }
    }
}
