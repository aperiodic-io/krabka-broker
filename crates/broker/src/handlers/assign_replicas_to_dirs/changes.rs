//! The pure metadata delta that turns a reported directory into a
//! `PartitionDirAssignment` record, and the error code of each reported
//! partition.
//!
//! Nothing here touches the controller or the network, so the whole
//! request-to-record mapping is unit-testable against a hand-built
//! `MetadataImage`.
//!
//! The error codes follow Kafka's
//! `ReplicationControlManager.handleAssignReplicasToDirs`:
//! `UNKNOWN_TOPIC_ID` on every partition of a topic id that names no topic,
//! `UNKNOWN_TOPIC_OR_PARTITION` for a partition that the topic does not have,
//! and `NOT_LEADER_OR_FOLLOWER` for a partition that the reporting broker does
//! not replicate. A partition with an error contributes no record.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionDirAssignmentRecord};
use krabka_protocol::owned::{
    assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    assign_replicas_to_dirs_response::{
        AssignReplicasToDirsResponse, DirectoryData as RespDirData, PartitionData as RespPartData,
        TopicData as RespTopicData,
    },
};

use crate::codes;

/// What one request does: the records to commit and the response to send
/// after they commit.
#[derive(Debug, PartialEq)]
pub(crate) struct AssignmentPlan {
    pub(crate) changes: Vec<MetadataRecord>,
    pub(crate) response: AssignReplicasToDirsResponse,
}

/// Plans every directory, topic, and partition in `req` for the broker
/// `broker_id`. The response mirrors the request's structure, in request
/// order, with the error code of each partition. The function is pure and
/// does no I/O.
pub(crate) fn plan_assignments(
    image: &MetadataImage,
    broker_id: u64,
    req: &AssignReplicasToDirsRequest,
) -> AssignmentPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let directories = req
        .directories
        .iter()
        .map(|dir| {
            let dir_uuid = uuid::Uuid::from_bytes(dir.id.0);
            let topics = dir
                .topics
                .iter()
                .map(|t| {
                    let topic_name = image.topic_name_by_id(&uuid::Uuid::from_bytes(t.topic_id.0));
                    let partitions = t
                        .partitions
                        .iter()
                        .map(|p| {
                            let outcome = match topic_name {
                                None => Err(codes::UNKNOWN_TOPIC_ID),
                                Some(topic_name) => assignment_change(
                                    image,
                                    broker_id,
                                    topic_name,
                                    p.partition_index,
                                    dir_uuid,
                                ),
                            };
                            let error_code = match outcome {
                                Ok(change) => {
                                    changes.extend(change);
                                    codes::NONE
                                }
                                Err(error_code) => error_code,
                            };
                            RespPartData {
                                partition_index: p.partition_index,
                                error_code,
                                ..Default::default()
                            }
                        })
                        .collect();
                    RespTopicData {
                        topic_id: t.topic_id,
                        partitions,
                        ..Default::default()
                    }
                })
                .collect();
            RespDirData {
                id: dir.id,
                topics,
                ..Default::default()
            }
        })
        .collect();
    AssignmentPlan {
        changes,
        response: AssignReplicasToDirsResponse {
            directories,
            ..Default::default()
        },
    }
}

/// Computes the directory-assignment delta that records the replica of
/// `(topic_name, partition)` on `broker_id` as living on `dir_uuid`. This
/// function is pure.
///
/// It returns `Err(UNKNOWN_TOPIC_OR_PARTITION)` when the topic has no such
/// partition, and `Err(NOT_LEADER_OR_FOLLOWER)` when the broker is not a
/// replica. It returns `Ok(None)` when the slot already holds `dir_uuid`, so
/// the function is idempotent and avoids churn.
///
/// It emits a [`MetadataRecord::V1PartitionDirAssignment`] DELTA instead of a
/// full `V1Partition`. On apply, the delta merges ONLY the one replica's slot
/// in `directories`, and never touches leader, isr, replicas, adding, or
/// removing. A full read-modify-write here, built from a slightly stale image
/// read, would race a concurrent `AlterPartitionReassignments` and revert
/// `adding_replicas`. The delta does not depend on order (KIP-858).
fn assignment_change(
    image: &MetadataImage,
    broker_id: u64,
    topic_name: &str,
    partition: i32,
    dir_uuid: uuid::Uuid,
) -> Result<Option<MetadataRecord>, i16> {
    let Some(pr) = image.partition(topic_name, partition) else {
        return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    let replica_slot = pr.replicas.iter().position(|n| n.0 == broker_id);
    let already_assigned =
        replica_slot.and_then(|slot| pr.directories.get(slot)) == Some(&dir_uuid);
    let slot = match krabka_verified::directory_assignment_decision(replica_slot, already_assigned)
    {
        krabka_verified::DirectoryAssignmentDecision::Ignore => {
            return Err(codes::NOT_LEADER_OR_FOLLOWER);
        }
        krabka_verified::DirectoryAssignmentDecision::NoOp => return Ok(None),
        krabka_verified::DirectoryAssignmentDecision::Assign(slot) => slot,
    };
    let Some(&replica) = pr.replicas.get(slot) else {
        return Err(codes::NOT_LEADER_OR_FOLLOWER);
    };
    Ok(Some(MetadataRecord::V1PartitionDirAssignment(
        PartitionDirAssignmentRecord {
            topic: topic_name.to_string(),
            partition,
            replica,
            directory: dir_uuid,
        },
    )))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::{NodeId, PartitionIndex};
    use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use krabka_protocol::{
        owned::assign_replicas_to_dirs_request::{
            DirectoryData as ReqDirData, PartitionData as ReqPartData, TopicData as ReqTopicData,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    };

    use super::*;
    use crate::test_support::KafkaErrorCode;

    const DIR: uuid::Uuid = uuid::Uuid::from_u128(0xAA);

    const UNASSIGNED_DIRECTORIES: [uuid::Uuid; 2] = [uuid::Uuid::nil(); 2];

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ReplicaImageSetup<'a> {
        #[default("t")]
        name: &'a str,
        #[default(uuid::Uuid::from_u128(7))]
        topic_id: uuid::Uuid,
        #[default(&[NodeId(1), NodeId(2)])]
        replicas: &'a [NodeId],
        #[default(&UNASSIGNED_DIRECTORIES)]
        directories: &'a [uuid::Uuid],
    }

    fn image_with(setup: ReplicaImageSetup<'_>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        apply_topic(&mut image, setup);
        image
    }

    fn apply_topic(image: &mut MetadataImage, setup: ReplicaImageSetup<'_>) {
        let ReplicaImageSetup {
            name,
            topic_id,
            replicas,
            directories,
        } = setup;
        let nodes = replicas.to_vec();
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id,
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("rf fits i16"),
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: name.into(),
            partition: 0,
            leader: nodes[0],
            replicas: nodes.clone(),
            isr: nodes,
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: directories.to_vec(),
            partition_epoch: 0,
        }));
    }

    fn assigned(topic: &str, broker: NodeId) -> MetadataRecord {
        MetadataRecord::V1PartitionDirAssignment(PartitionDirAssignmentRecord {
            topic: topic.into(),
            partition: 0,
            replica: broker,
            directory: DIR,
        })
    }

    #[test]
    fn assignment_change_follows_the_partition_and_its_replica_slot() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let nil = uuid::Uuid::nil();
        let cases = [
            (
                "broker 2 sets its own slot",
                image_with(ReplicaImageSetup {
                    topic_id,
                    ..Default::default()
                }),
                NodeId(2),
                PartitionIndex(0),
                Ok(Some(assigned("t", NodeId(2)))),
            ),
            (
                "a slot that holds the directory is a no-op",
                image_with(ReplicaImageSetup {
                    topic_id,
                    directories: &[nil, DIR],
                    ..Default::default()
                }),
                NodeId(2),
                PartitionIndex(0),
                Ok(None),
            ),
            (
                "a broker that is not a replica",
                image_with(ReplicaImageSetup {
                    topic_id,
                    ..Default::default()
                }),
                NodeId(99),
                PartitionIndex(0),
                Err(KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER)),
            ),
            (
                "a partition that the topic does not have",
                image_with(ReplicaImageSetup {
                    topic_id,
                    ..Default::default()
                }),
                NodeId(2),
                PartitionIndex(99),
                Err(KafkaErrorCode(codes::UNKNOWN_TOPIC_OR_PARTITION)),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, image, broker, partition, want) in cases {
            actual.push((
                label,
                assignment_change(&image, broker.0, "t", partition.0, DIR),
            ));
            expected.push((label, want.map_err(|code| code.0)));
        }
        assert!(actual == expected);
    }

    #[test]
    fn delta_preserves_replica_order_and_only_changes_the_reporting_slot() {
        let topic_id = uuid::Uuid::from_u128(0x42);
        let nil = uuid::Uuid::nil();
        let mut image = image_with(ReplicaImageSetup {
            topic_id,
            ..Default::default()
        });
        let change = assignment_change(&image, 2, "t", 0, DIR)
            .expect("broker 2 is a replica")
            .expect("the slot changes");
        image.apply(&change);

        let partition = image.partition("t", 0).expect("updated partition");
        assert!(partition.replicas == vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)]);
        assert!(partition.directories == vec![nil, DIR]);
        assert!(assignment_change(&image, 2, "t", 0, DIR) == Ok(None));
    }

    /// One request that reports, for broker 2, a partition it replicates, a
    /// partition the topic does not have, a partition of a topic it does not
    /// replicate, a topic id that names no topic, and the zero id. Only the
    /// first partition gives a record, and each row carries Kafka's code.
    #[test]
    fn plan_answers_each_partition_with_kafkas_error_code() {
        let known = uuid::Uuid::from_u128(0x42);
        let elsewhere = uuid::Uuid::from_u128(0x43);
        let unknown = uuid::Uuid::from_u128(0x44);
        let nil = uuid::Uuid::nil();
        let mut image = image_with(ReplicaImageSetup {
            topic_id: known,
            ..Default::default()
        });
        apply_topic(
            &mut image,
            ReplicaImageSetup {
                name: "other",
                topic_id: elsewhere,
                replicas: &[NodeId(1)],
                directories: &[nil],
            },
        );

        let topic = |topic_id: uuid::Uuid, partitions: &[i32]| ReqTopicData {
            topic_id: ProtocolUuid(topic_id.into_bytes()),
            partitions: partitions
                .iter()
                .map(|&partition_index| ReqPartData {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let req = AssignReplicasToDirsRequest {
            broker_id: 2,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(DIR.into_bytes()),
                topics: vec![
                    topic(known, &[0, 99]),
                    topic(elsewhere, &[0]),
                    topic(unknown, &[0, 1]),
                    topic(nil, &[0]),
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let answer = |topic_id: uuid::Uuid, partitions: &[(i32, i16)]| RespTopicData {
            topic_id: ProtocolUuid(topic_id.into_bytes()),
            partitions: partitions
                .iter()
                .map(|&(partition_index, error_code)| RespPartData {
                    partition_index,
                    error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let expected = AssignmentPlan {
            changes: vec![assigned("t", NodeId(2))],
            response: AssignReplicasToDirsResponse {
                directories: vec![RespDirData {
                    id: ProtocolUuid(DIR.into_bytes()),
                    topics: vec![
                        answer(
                            known,
                            &[(0, codes::NONE), (99, codes::UNKNOWN_TOPIC_OR_PARTITION)],
                        ),
                        answer(elsewhere, &[(0, codes::NOT_LEADER_OR_FOLLOWER)]),
                        answer(
                            unknown,
                            &[(0, codes::UNKNOWN_TOPIC_ID), (1, codes::UNKNOWN_TOPIC_ID)],
                        ),
                        answer(nil, &[(0, codes::UNKNOWN_TOPIC_ID)]),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        };
        assert!(plan_assignments(&image, 2, &req) == expected);
    }
}
