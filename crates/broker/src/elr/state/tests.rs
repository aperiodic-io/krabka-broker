//! Parsing and projection of the KIP-966 ELR state.

use {
    super::TopicElr,
    crate::test_support::{
        elr_model_prelude::PartitionElr, finalize_elr_version, partition_elr as elr,
    },
    assert2::assert,
    krabka_metadata::{
        MetadataImage, MetadataRecord, NodeId, PartitionElrRecord, PartitionRecord,
        TopicConfigRecord,
    },
};

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
#[yare::parameterized(
    empty = { "", 0, elr(&[], &[]) },
    both_sets = { "0:2,3:4,5", 0, elr(&[2, 3], &[4, 5]) },
    eligible_only = { "0:2,3:", 0, elr(&[2, 3], &[]) },
    last_known_only = { "0::5", 0, elr(&[], &[5]) },
    unnamed_partition = { "0:2:3", 1, elr(&[], &[]) },
    several_partitions_eligible = { "4::5;0:2,3:", 0, elr(&[2, 3], &[]) },
    several_partitions_last_known = { "4::5;0:2,3:", 4, elr(&[], &[5]) },
    trailing_separator = { "0:2:;", 0, elr(&[2], &[]) },
    malformed_entry_drops_only_itself = { "nope:1:2;0:7:", 0, elr(&[7], &[]) },
    malformed_only_entry = { "nope:1:2", 0, elr(&[], &[]) },
    missing_separator = { "0:1", 0, elr(&[], &[]) },
    too_many_separators = { "0:1:2:3", 0, elr(&[], &[]) },
    invalid_member = { "0:x:2", 0, elr(&[], &[]) },
    empty_member = { "0:1,:2", 0, elr(&[], &[]) },
)]
fn parse_projects_each_partition_of_the_config_value(
    value: &'static str,
    partition: i32,
    want: PartitionElr,
) {
    assert2::assert!(TopicElr::parse(value).partition(partition) == want);
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
    finalize_elr_version(&mut image);
    assert!(TopicElr::of_topic(&image, "orders").partition(0) == elr(&[2, 3], &[4]));
}

/// Kafka 4.3.1 keeps one replica in `lastKnownElr`, the last leader, while a
/// partition has no leader (`PartitionChangeBuilder.maybeUpdateLastKnownLeader`),
/// so that one replica, when it is the leader the record still names, is what
/// says the partition has none. `canElectLastKnownLeader` reads the same shape:
/// a last-known ELR of any other length is not a last known leader.
#[yare::parameterized(
    last_leader_alone = { elr(&[], &[1]), 1, true },
    eligible_beside_leader = { elr(&[2, 3], &[1]), 1, true },
    different_replica = { elr(&[], &[2]), 1, false },
    two_members = { elr(&[], &[1, 2]), 1, false },
    no_last_known = { elr(&[2], &[]), 1, false },
    no_elr = { elr(&[], &[]), 1, false },
)]
fn a_one_member_last_known_elr_naming_the_recorded_leader_marks_the_partition(
    state: PartitionElr,
    recorded_leader: u64,
    want: bool,
) {
    assert2::assert!(state.is_leaderless(NodeId(recorded_leader)) == want);
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
