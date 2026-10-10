//! Parsing and projection of the KIP-966 ELR state.

use assert2::assert;
use krabka_metadata::{
    MetadataImage, MetadataRecord, NodeId, PartitionElrRecord, PartitionRecord, TopicConfigRecord,
};

use super::TopicElr;
use crate::test_support::partition_elr as elr;

fn orders_partition_image() -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "orders".into(),
        partition: 0,
        ..Default::default()
    }));
    image
}

/// The grammar, one row per shape the controller can publish, plus the
/// malformed shapes that must degrade to "no ELR" rather than fail a request.
#[test]
fn parse_projects_each_partition_of_the_config_value() {
    for (value, partition, want) in [
        // Nothing published at all.
        ("", 0, elr(&[], &[])),
        // Both sets on one partition.
        ("0:2,3:4,5", 0, elr(&[2, 3], &[4, 5])),
        // ELR only, and last-known only.
        ("0:2,3:", 0, elr(&[2, 3], &[])),
        ("0::5", 0, elr(&[], &[5])),
        // A partition the value does not name projects as no ELR.
        ("0:2:3", 1, elr(&[], &[])),
        // Several partitions in one value, in either order.
        ("4::5;0:2,3:", 0, elr(&[2, 3], &[])),
        ("4::5;0:2,3:", 4, elr(&[], &[5])),
        // A trailing separator is not an entry.
        ("0:2:;", 0, elr(&[2], &[])),
        // Malformed entries drop, and drop only themselves.
        ("nope:1:2;0:7:", 0, elr(&[7], &[])),
        ("nope:1:2", 0, elr(&[], &[])),
        ("0:1", 0, elr(&[], &[])),
        ("0:1:2:3", 0, elr(&[], &[])),
        ("0:x:2", 0, elr(&[], &[])),
        ("0:1,:2", 0, elr(&[], &[])),
    ] {
        assert!(
            TopicElr::parse(value).partition(partition) == want,
            "value {value:?} partition {partition}"
        );
    }
}

/// The projection reads partition metadata out of the image, so a topic the
/// controller has never published ELR for answers with empty lists.
#[test]
fn of_topic_reads_the_published_config_and_defaults_to_no_elr() {
    let mut image = orders_partition_image();
    image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
        topic: "orders".into(),
        partition: 0,
        eligible_leader_replicas: vec![NodeId(2), NodeId(3)],
        last_known_elr: vec![NodeId(4)],
    }));

    assert!(TopicElr::of_topic(&image, "orders").partition(0) == elr(&[2, 3], &[4]));
    assert!(TopicElr::of_topic(&image, "orders").partition(1) == elr(&[], &[]));
    assert!(TopicElr::of_topic(&image, "payments").partition(0) == elr(&[], &[]));
}

#[test]
fn of_topic_reads_legacy_elr_until_it_is_migrated() {
    let mut image = orders_partition_image();
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: [("krabka.elr".to_owned(), "0:2,3:4".to_owned())]
            .into_iter()
            .collect(),
    }));

    assert!(TopicElr::of_topic(&image, "orders").partition(0) == elr(&[], &[]));
    crate::test_support::finalize_elr_version(&mut image);
    assert!(TopicElr::of_topic(&image, "orders").partition(0) == elr(&[2, 3], &[4]));
}

/// Kafka 4.3.1 keeps one replica in `lastKnownElr`, the last leader, while a
/// partition has no leader (`PartitionChangeBuilder.maybeUpdateLastKnownLeader`),
/// so that one replica, when it is the leader the record still names, is what
/// says the partition has none. `canElectLastKnownLeader` reads the same shape:
/// a last-known ELR of any other length is not a last known leader.
#[test]
fn a_one_member_last_known_elr_naming_the_recorded_leader_marks_the_partition() {
    for (label, state, recorded_leader, want) in [
        ("the last leader alone", elr(&[], &[1]), 1, true),
        ("with an ELR beside it", elr(&[2, 3], &[1]), 1, true),
        ("a different replica", elr(&[], &[2]), 1, false),
        ("two members", elr(&[], &[1, 2]), 1, false),
        ("no last-known ELR", elr(&[2], &[]), 1, false),
        ("no ELR at all", elr(&[], &[]), 1, false),
    ] {
        assert!(
            state.is_leaderless(NodeId(recorded_leader)) == want,
            "{label}"
        );
    }
}

/// The image-level reader answers from what the log published, per partition.
#[test]
fn the_image_marks_only_the_partition_whose_last_known_elr_names_its_leader() {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for partition in 0..3 {
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition,
            leader: NodeId(1),
            ..Default::default()
        }));
    }
    for (partition, last_known) in [(0, vec![NodeId(1)]), (1, vec![NodeId(2)])] {
        image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: "orders".into(),
            partition,
            eligible_leader_replicas: vec![],
            last_known_elr: last_known,
        }));
    }

    let marked: Vec<bool> = image
        .partitions_of("orders")
        .map(|partition| super::is_leaderless(&image, partition))
        .collect();

    assert!(marked == vec![true, false, false]);
}
