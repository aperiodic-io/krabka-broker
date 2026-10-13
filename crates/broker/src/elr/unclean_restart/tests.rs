//! Withdrawing one replica's ELR membership: what moves, what does not, and
//! what the published value looks like afterwards.

use {
    super::withdraw_elr_membership,
    crate::{
        config_keys::MIN_INSYNC_REPLICAS,
        elr::state::{PartitionElr, TopicElr},
        test_support::finalize_elr_version,
    },
    assert2::assert,
    krabka_metadata::{
        LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionElrRecord, PartitionRecord,
        TopicConfigRecord, TopicRecord,
    },
    std::collections::BTreeMap,
};

fn nodes(ids: &[u64]) -> Vec<NodeId> { ids.iter().copied().map(NodeId).collect() }

/// A topic with `partitions` partitions and the ELR value `elr`.
fn topic_records(name: &str, partitions: i32, elr: &str) -> Vec<MetadataRecord> {
    let mut overrides = BTreeMap::new();
    overrides.insert(MIN_INSYNC_REPLICAS.to_string(), "2".to_string());
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions,
        replication_factor: 3,
    })];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: name.into(),
            partition,
            leader: NodeId(1),
            replicas: nodes(&[1, 2, 3]),
            isr: nodes(&[1]),
            leader_epoch: LeaderEpoch(1),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(); 3],
            partition_epoch: 1,
        })
    }));
    records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: name.into(),
        overrides,
    }));
    let state = TopicElr::parse(elr);
    for partition in 0..partitions {
        let partition_elr = state.partition(partition);
        if !partition_elr.eligible_leader_replicas.is_empty()
            || !partition_elr.last_known_elr.is_empty()
        {
            records.push(MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: name.into(),
                partition,
                eligible_leader_replicas: partition_elr
                    .eligible_leader_replicas
                    .into_iter()
                    .map(|id| NodeId(u64::try_from(id).unwrap()))
                    .collect(),
                last_known_elr: partition_elr
                    .last_known_elr
                    .into_iter()
                    .map(|id| NodeId(u64::try_from(id).unwrap()))
                    .collect(),
            }));
        }
    }
    records
}

/// A topic, the number of its partitions, and its ELR config value.
struct TopicElrSetup<'a> {
    name: &'a str,
    partitions: i32,
    elr: &'a str,
}

fn image_of(topics: &[TopicElrSetup<'_>]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    finalize_elr_version(&mut image);
    for TopicElrSetup {
        name,
        partitions,
        elr,
    } in topics
    {
        for record in topic_records(name, *partitions, elr) {
            image.apply(&record);
        }
    }
    image
}

/// Apply `records` to `image` and read one partition's ELR back out.
fn elr_after(image: &MetadataImage, records: &[MetadataRecord], topic: &str) -> TopicElr {
    let mut image = image.clone();
    for record in records {
        image.apply(record);
    }
    TopicElr::of_topic(&image, topic)
}

/// The strike Kafka's `maybePopulateTargetElr` makes for an unclean-shutdown
/// replica: `targetElr = candidates - targetIsr - uncleanShutdownReplicas`, and
/// nothing else. The replica is no longer offered as a safe election, and it
/// does not move into the last-known ELR, which Kafka 4.3.1 keeps for the last
/// leader of a partition that has none
/// (`testEligibleLeaderReplicas_RemoveUncleanShutdownReplicasFromElr` runs
/// with the last-known set empty).
#[test]
fn an_unclean_replica_is_struck_from_the_elr_and_lands_nowhere() {
    let image = image_of(&[TopicElrSetup {
        name: "orders",
        partitions: 1,
        elr: "0:2,3:",
    }]);

    let records = withdraw_elr_membership(&image, NodeId(2));

    assert!(
        elr_after(&image, &records, "orders").partition(0)
            == PartitionElr {
                eligible_leader_replicas: vec![3],
                last_known_elr: vec![],
            }
    );
}

/// Every partition of every topic that names the replica moves, and no other
/// partition does. Withdrawal is cluster-wide, the way Kafka walks
/// `brokersToElrs.partitionsWithBrokerInElr(brokerId)`.
#[test]
fn withdrawal_reaches_every_partition_that_names_the_replica() {
    let image = image_of(&[
        TopicElrSetup {
            name: "orders",
            partitions: 2,
            elr: "0:2,3:;1:3:",
        },
        TopicElrSetup {
            name: "payments",
            partitions: 1,
            elr: "0:2:4",
        },
    ]);

    let records = withdraw_elr_membership(&image, NodeId(2));

    let orders = elr_after(&image, &records, "orders");
    assert!(
        orders.partition(0)
            == PartitionElr {
                eligible_leader_replicas: vec![3],
                last_known_elr: vec![],
            }
    );
    // Partition 1 never named node 2, so it is untouched.
    assert!(
        orders.partition(1)
            == PartitionElr {
                eligible_leader_replicas: vec![3],
                last_known_elr: vec![],
            }
    );
    // The last-known set the partition already carried is not the
    // withdrawal's to touch.
    assert!(
        elr_after(&image, &records, "payments").partition(0)
            == PartitionElr {
                eligible_leader_replicas: vec![],
                last_known_elr: vec![4],
            }
    );
}

/// A replica in no ELR anywhere produces no records at all -- the common case,
/// and the one that must not churn the metadata log on every restart.
#[test]
fn a_replica_in_no_elr_produces_no_records() {
    let image = image_of(&[
        TopicElrSetup {
            name: "orders",
            partitions: 1,
            elr: "0:2,3:",
        },
        TopicElrSetup {
            name: "payments",
            partitions: 1,
            elr: "",
        },
    ]);

    assert!(withdraw_elr_membership(&image, NodeId(9)).is_empty());
}

/// Withdrawing the only eligible replica leaves the topic with no ELR value at
/// all rather than an entry that renders as "no ELR", and the topic's other
/// overrides survive: applying a `V1TopicConfig` replaces the whole map.
#[test]
fn the_last_eligible_replica_leaving_tombstones_the_key_and_keeps_other_overrides() {
    let image = image_of(&[TopicElrSetup {
        name: "orders",
        partitions: 1,
        elr: "0:2:",
    }]);

    let records = withdraw_elr_membership(&image, NodeId(2));

    let mut after = image.clone();
    for record in &records {
        after.apply(record);
    }
    let overrides = after.topic_config("orders").expect("topic keeps overrides");
    assert!(overrides.get(MIN_INSYNC_REPLICAS) == Some(&"2".to_string()));
    assert!(TopicElr::of_topic(&after, "orders").partition(0) == PartitionElr::default());
}
