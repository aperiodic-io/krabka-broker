//! Metadata-image fixtures, request builders, and the broker harness that the
//! `AlterPartition` tests share.
//!
//! The per-partition validation tests and the end-to-end handler tests build
//! the same topic, the same partition record, and the same broker
//! registrations, so those fixtures live in one module rather than being
//! duplicated per test file.

use {
    crate::{
        broker::Broker,
        test_support::{BrokerEpoch, PartitionEpoch, WireBrokerId, broker_registration},
    },
    krabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    },
    krabka_protocol::{
        owned::alter_partition_request::{
            AlterPartitionRequest, BrokerState, TopicData as ReqTopicData,
        },
        primitives::uuid::Uuid as ProtoUuid,
    },
};

const TOPIC_ID_BYTES: [u8; 16] = [7; 16];

pub(super) const THREE_REGISTERED_BROKERS: &[(krabka_metadata::NodeId, BrokerEpoch)] = &[
    (krabka_metadata::NodeId(1), BrokerEpoch(10)),
    (krabka_metadata::NodeId(2), BrokerEpoch(20)),
    (krabka_metadata::NodeId(3), BrokerEpoch(30)),
];
pub(super) const TWO_REGISTERED_BROKERS: &[(krabka_metadata::NodeId, BrokerEpoch)] =
    &[THREE_REGISTERED_BROKERS[0], THREE_REGISTERED_BROKERS[1]];
pub(super) const ONE_REGISTERED_BROKER: &[(krabka_metadata::NodeId, BrokerEpoch)] =
    &[THREE_REGISTERED_BROKERS[0]];

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ThreeBrokerEpochsSetup {
    #[default(BrokerEpoch(10))]
    pub first: BrokerEpoch,
    #[default(BrokerEpoch(20))]
    pub second: BrokerEpoch,
    #[default(BrokerEpoch(30))]
    pub third: BrokerEpoch,
}

pub(super) fn three_broker_states(setup: ThreeBrokerEpochsSetup) -> Vec<BrokerState> {
    vec![
        bs(WireBrokerId(1), setup.first),
        bs(WireBrokerId(2), setup.second),
        bs(WireBrokerId(3), setup.third),
    ]
}

fn reg(node_id: krabka_metadata::NodeId, epoch: BrokerEpoch) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        broker_epoch: epoch.0,
        host: "h".into(),
        ..broker_registration(krabka_raft::NodeId(node_id.0))
    })
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct PartitionFixture<'a> {
    pub(super) partition: krabka_ids::PartitionIndex,
    #[default(krabka_metadata::NodeId(1))]
    pub(super) leader: krabka_metadata::NodeId,
    #[default(&[krabka_metadata::NodeId(1), krabka_metadata::NodeId(2), krabka_metadata::NodeId(3)])]
    pub(super) replicas: &'a [krabka_metadata::NodeId],
    #[default(&[krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)])]
    pub(super) isr: &'a [krabka_metadata::NodeId],
    #[default(krabka_metadata::LeaderEpoch(5))]
    pub(super) leader_epoch: krabka_metadata::LeaderEpoch,
    pub(super) partition_epoch: PartitionEpoch,
}

/// An image with the topic "t" and one partition. `epochs` gives the
/// registered brokers.
pub(super) fn image_with_partition(
    fixture: &PartitionFixture<'_>,
    epochs: &[(krabka_metadata::NodeId, BrokerEpoch)],
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: topic_id(),
        partitions: fixture.partition.0 + 1,
        replication_factor: i16::try_from(fixture.replicas.len()).expect("rf fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "t".into(),
        partition: fixture.partition.0,
        leader: fixture.leader,
        replicas: fixture.replicas.to_vec(),
        isr: fixture.isr.to_vec(),
        leader_epoch: fixture.leader_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: fixture.partition_epoch.0,
    }));
    for &(id, ep) in epochs {
        image.apply(&reg(id, ep));
    }
    image
}

/// An image with topic "t" partition 0, replicas [1,2,3], isr [1,2], and
/// leader 1 at `leader_epoch` 5. `epochs` gives the registered brokers.
pub(super) fn image_with(epochs: &[(krabka_metadata::NodeId, BrokerEpoch)]) -> MetadataImage {
    image_with_partition(&PartitionFixture::default(), epochs)
}

fn topic_id() -> uuid::Uuid { uuid::Uuid::from_bytes(TOPIC_ID_BYTES) }

pub(super) fn wire_topic_id() -> ProtoUuid { ProtoUuid(TOPIC_ID_BYTES) }

pub(super) fn bs(broker_id: WireBrokerId, broker_epoch: BrokerEpoch) -> BrokerState {
    BrokerState {
        broker_id: broker_id.0,
        broker_epoch: broker_epoch.0,
        ..Default::default()
    }
}

pub(super) async fn seed_partition(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: topic_id(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                ..crate::handlers::test_support::replicated_partition(
                    crate::handlers::test_support::ReplicatedPartitionSetup {
                        topic: "t",
                        replicas: &[krabka_metadata::NodeId(1)],
                        ..Default::default()
                    },
                )
            }),
        ])
        .await
        .expect("seed topic partition");
}

/// A request from broker 1 at the broker epoch its registration holds, or -1
/// before it registers.
pub(super) fn request_with_topics(
    broker: &Broker,
    topics: Vec<ReqTopicData>,
) -> AlterPartitionRequest {
    AlterPartitionRequest {
        broker_id: 1,
        broker_epoch: broker
            .controller
            .current_image()
            .broker_epoch(krabka_metadata::NodeId(1))
            .unwrap_or(-1),
        topics,
        ..Default::default()
    }
}
