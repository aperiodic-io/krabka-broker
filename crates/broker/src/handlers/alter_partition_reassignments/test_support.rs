//! The request builder, the metadata images, and the context helper that the
//! `AlterPartitionReassignments` tests share.
//!
//! The response tests and the end-to-end handler tests build the same
//! single-partition request, and the
//! planning tests and the cancel-approval tests seed the same one-partition
//! image, so the fixtures live in one module rather than once per test file.

use {
    crate::test_support::{
        PartitionEpoch, ReassignmentSetup, broker_registration, reassignment_partition,
    },
    krabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    },
    krabka_protocol::owned::alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    },
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum ReplicationFactorPolicy {
    #[default]
    Maintain,
    PermitChange,
}

/// Signed wire broker ids also permit the malformed ids used in refusal fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub(super) struct ReplicaBrokerId(pub i32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReassignmentTarget {
    Replicas(Vec<ReplicaBrokerId>),
    Cancel,
}

impl Default for ReassignmentTarget {
    fn default() -> Self { Self::Replicas(vec![ReplicaBrokerId(1), ReplicaBrokerId(2)]) }
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ReassignmentRequestSetup<'a> {
    pub replication_factor_policy: ReplicationFactorPolicy,
    #[default("orders")]
    pub topic: &'a str,
    #[default(krabka_ids::PartitionIndex(7))]
    pub partition_index: krabka_ids::PartitionIndex,
    pub target: ReassignmentTarget,
}

#[bon::builder]
pub(super) fn request(
    #[builder(default)] replication_factor_policy: ReplicationFactorPolicy,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default = krabka_ids :: PartitionIndex (7))]
    partition_index: krabka_ids::PartitionIndex,
    #[builder(default)] target: ReassignmentTarget,
) -> AlterPartitionReassignmentsRequest {
    let setup = ReassignmentRequestSetup {
        replication_factor_policy,
        topic,
        partition_index,
        target,
    };
    let ReassignmentRequestSetup {
        replication_factor_policy,
        topic,
        partition_index,
        target,
    } = setup;
    AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: replication_factor_policy
            == ReplicationFactorPolicy::PermitChange,
        topics: vec![ReassignableTopic {
            name: topic.into(),
            partitions: vec![ReassignablePartition {
                partition_index: partition_index.0,
                replicas: match target {
                    ReassignmentTarget::Replicas(ids) => {
                        Some(ids.into_iter().map(|id| id.0).collect())
                    }
                    ReassignmentTarget::Cancel => None,
                },
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

/// A registered six-broker image with one partition in the supplied reassignment state.
#[derive(Clone, krabka_macros::FieldDefaults)]
pub(super) struct ReassignmentImageSetup<'a> {
    pub assignment: ReassignmentSetup<'a>,
    pub partition_epoch: PartitionEpoch,
}

/// The in-progress replacement of broker two by broker three.
pub(super) fn pending_reassignment_image() -> MetadataImage {
    img_with()
        .assignment(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            ..Default::default()
        })
        .call()
}

#[bon::builder]
pub(super) fn img_with(
    #[builder(default)] assignment: ReassignmentSetup<'_>,
    #[builder(into, default)] partition_epoch: PartitionEpoch,
) -> MetadataImage {
    let setup = ReassignmentImageSetup {
        assignment,
        partition_epoch,
    };
    let ReassignmentImageSetup {
        assignment,
        partition_epoch,
    } = setup;
    let replica_count = assignment.replicas.len();
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    // Register brokers 1..=6 so validate_target accepts target lists.
    for n in 1u64..=6 {
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: "localhost".into(),
                ..broker_registration(krabka_raft::NodeId(n))
            },
        ));
    }
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "foo".into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replica_count).expect("replication factor fits i16"),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        partition_epoch: partition_epoch.0,
        ..crate::reassignment_fields!(reassignment_partition(), assignment).call()
    }));
    img
}
