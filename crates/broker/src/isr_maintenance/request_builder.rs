//! Builds the `AlterPartition` request that carries a proposal to the
//! controller. It is its own module because the KIP-903 broker-epoch stamping
//! and the v2/v3 dual ISR encoding are pure functions of the metadata image.

use {
    krabka_protocol::owned::alter_partition_request::{
        AlterPartitionRequest, BrokerState, PartitionData, TopicData,
    },
    krabka_raft::NodeId,
};

/// KIP-903 sentinel for an unknown broker epoch. The broker stamps it when the
/// metadata image has no epoch for a broker. It tells the controller to skip
/// the stale-replica epoch fence for that entry.
const UNKNOWN_BROKER_EPOCH: i64 = -1;

/// One partition's proposed ISR, with the epochs of the committed state it
/// was built from.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct IsrChange<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) new_isr: Vec<NodeId>,
    pub(super) leader_epoch: i32,
    /// The partition epoch of the committed ISR the proposal replaces. Kafka's
    /// controller refuses a proposal with an older one as
    /// `INVALID_UPDATE_VERSION`, so a stale proposal cannot overwrite a newer
    /// ISR.
    pub(super) partition_epoch: i32,
}

pub(super) fn build_alter_partition_request(
    image: &krabka_metadata::MetadataImage,
    broker_id: i32,
    change: &IsrChange<'_>,
) -> AlterPartitionRequest {
    // Look up topic_id from the metadata image and convert to the protocol Uuid type.
    let topic_id = {
        let raw: [u8; 16] = image
            .topic(change.topic)
            .map_or([0u8; 16], |t| *t.topic_id.as_bytes());
        krabka_protocol::primitives::uuid::Uuid(raw)
    };

    // `new_isr` is the v2 field (versions 2 only on the wire).
    // `new_isr_with_epochs` is the v3 field; the client negotiates MAX_VERSION
    // (= 3), so we must populate both so that whichever version is selected
    // carries the correct ISR.  The handler side reads `new_isr_with_epochs`
    // when `new_isr` is empty (i.e. version 3).
    // KIP-903: per-member epochs come from the metadata image; unknown brokers fall back to -1.
    let new_isr_i32: Vec<i32> = change
        .new_isr
        .iter()
        .map(|n| i32::try_from(n.0).unwrap_or(i32::MAX))
        .collect();
    let new_isr_with_epochs: Vec<BrokerState> = new_isr_i32
        .iter()
        .map(|&bid| BrokerState {
            broker_id: bid,
            broker_epoch: image
                .broker_epoch(NodeId(u64::try_from(bid).unwrap_or(0)))
                .unwrap_or(UNKNOWN_BROKER_EPOCH),
            ..Default::default()
        })
        .collect();

    AlterPartitionRequest {
        broker_id,
        // KIP-903: the partition leader stamps its own broker epoch and each
        // ISR member's epoch from the metadata image so the controller can
        // fence stale replicas. Unknown brokers fall back to -1 (skip-check).
        broker_epoch: image
            .broker_epoch(NodeId(u64::try_from(broker_id).unwrap_or(0)))
            .unwrap_or(UNKNOWN_BROKER_EPOCH),
        topics: vec![TopicData {
            topic_id,
            partitions: vec![PartitionData {
                partition_index: change.partition,
                leader_epoch: change.leader_epoch,
                new_isr: new_isr_i32,
                new_isr_with_epochs,
                leader_recovery_state: 0,
                partition_epoch: change.partition_epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::isr_maintenance::test_support::{reg, topic},
        krabka_metadata::MetadataImage,
    };

    #[test]
    fn build_request_preserves_topic_broker_epochs_isr_and_partition_epoch() {
        use krabka_protocol::{
            UnknownTaggedFields,
            owned::alter_partition_request::{BrokerState, PartitionData, TopicData},
        };

        let topic_id = uuid::Uuid::from_u128(0xA11CE);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&topic("orders", topic_id));
        image.apply(&reg(NodeId(1)));
        // The image has moved on to partition epoch 3; the request still
        // carries the epoch of the committed ISR the proposal was built from.
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "orders".into(),
                partition: 6,
                leader: NodeId(4),
                replicas: vec![NodeId(4), NodeId(1), NodeId(9)],
                isr: vec![NodeId(4), NodeId(1), NodeId(9)],
                leader_epoch: krabka_metadata::LeaderEpoch(12),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 3,
            },
        ));

        let req = build_alter_partition_request(&image, 4, &IsrChange {
            topic: "orders",
            partition: 6,
            new_isr: vec![NodeId(1), NodeId(9)],
            leader_epoch: 12,
            partition_epoch: 21,
        });

        let expected = AlterPartitionRequest {
            broker_id: 4,
            broker_epoch: -1,
            topics: vec![TopicData {
                topic_id: krabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes()),
                partitions: vec![PartitionData {
                    partition_index: 6,
                    leader_epoch: 12,
                    new_isr: vec![1, 9],
                    new_isr_with_epochs: vec![
                        BrokerState {
                            broker_id: 1,
                            broker_epoch: 1,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        BrokerState {
                            broker_id: 9,
                            broker_epoch: -1,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    leader_recovery_state: 0,
                    partition_epoch: 21,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert2::assert!((req) == (expected));
    }

    #[test]
    fn build_request_stamps_leaders_own_broker_epoch_from_image() {
        // The sending broker (id 5) is registered with a distinct positive
        // broker epoch (reg() stamps epoch == node id). The top-level
        // `broker_epoch` field must carry that epoch (5), not the default (0)
        // and not the unknown-broker sentinel (-1). Pins the KIP-903 stamp so
        // dropping the field (→ Default 0) is caught.
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&reg(NodeId(5)));

        let req = build_alter_partition_request(&image, 5, &IsrChange {
            topic: "orders",
            partition: 0,
            new_isr: vec![NodeId(5)],
            leader_epoch: 3,
            partition_epoch: 0,
        });

        assert2::assert!((req.broker_epoch) == (5));
    }
}
