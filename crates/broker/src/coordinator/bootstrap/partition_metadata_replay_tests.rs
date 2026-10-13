//! Replay of the deprecated `ConsumerGroupPartitionMetadata` record (key v4),
//! as Kafka 4.3.1's loader and `GroupMetadataManager.replay` handle it.
//!
//! A value creates the consumer group when the log has none and marks it as
//! holding the record. A tombstone clears the mark, and is ignored for a group
//! the log does not hold. A value that does not decode, or whose version is
//! not 0, fails the load.

use {
    super::test_support::replay_log,
    crate::coordinator::unified::{
        GroupSeed,
        persistence_next_gen::{
            GroupMetadataValue, NextGenKey, PartitionMetadataValue, SubscribedTopicMetadata,
            encode_key,
        },
    },
    assert2::check,
    bytes::Bytes,
};

/// One replayed record: its key, and its value or `None` for a tombstone.
type LogRecord = (Bytes, Option<Bytes>);

/// What a replay leaves for group `g`.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// The replay failed.
    Failed,
    /// The replay holds no group `g`.
    NoGroup,
    /// The replay holds group `g` with this seed.
    Group(Box<GroupSeed>),
}

fn key(kind: fn(String) -> NextGenKey) -> Bytes { encode_key(&kind("g".into())).unwrap() }

fn partition_metadata(group_id: String) -> NextGenKey { NextGenKey::PartitionMetadata { group_id } }

fn group_metadata(group_id: String) -> NextGenKey { NextGenKey::GroupMetadata { group_id } }

fn value() -> Bytes {
    PartitionMetadataValue {
        topics: vec![SubscribedTopicMetadata {
            topic_id: uuid::Uuid::from_u128(7),
            topic_name: "t".into(),
            num_partitions: 2,
            partition_metadata: vec![],
        }],
    }
    .encode()
}

/// Replays one batch per record and returns what it left for group `g`.
fn replay(records: Vec<LogRecord>) -> Outcome {
    let (coordinator, replayed) = replay_log(records);
    if replayed.is_err() {
        return Outcome::Failed;
    }
    coordinator.seeds.get("g").map_or(Outcome::NoGroup, |seed| {
        Outcome::Group(Box::new(seed.clone()))
    })
}

#[test]
fn partition_metadata_replays_as_kafka_4_3_1_does() {
    let epoch = |epoch: i32| {
        Some(
            GroupMetadataValue {
                epoch,
                metadata_hash: 0,
            }
            .encode(),
        )
    };
    // A group that the replay creates is Kafka's new `ConsumerGroup`, at
    // group and assignment epoch 1.
    let marked = |group_epoch: i32| GroupSeed {
        group_epoch,
        has_subscription_metadata_record: true,
        ..GroupSeed::new_group()
    };
    let mut version_1 = value().to_vec();
    version_1[1] = 1;
    let truncated = value().slice(..5);

    let cases: Vec<(&str, Vec<LogRecord>, Outcome)> = vec![
        (
            "a value creates an empty consumer group that holds the record",
            vec![(key(partition_metadata), Some(value()))],
            Outcome::Group(Box::new(marked(1))),
        ),
        (
            "a value marks an existing group",
            vec![
                (key(group_metadata), epoch(4)),
                (key(partition_metadata), Some(value())),
            ],
            Outcome::Group(Box::new(marked(4))),
        ),
        (
            "a tombstone clears the mark",
            vec![
                (key(group_metadata), epoch(4)),
                (key(partition_metadata), Some(value())),
                (key(partition_metadata), None),
            ],
            Outcome::Group(Box::new(GroupSeed {
                group_epoch: 4,
                ..GroupSeed::new_group()
            })),
        ),
        (
            "a tombstone of a group the log does not hold is ignored",
            vec![(key(partition_metadata), None)],
            Outcome::NoGroup,
        ),
        (
            "a value version Kafka does not know fails the load",
            vec![(key(partition_metadata), Some(Bytes::from(version_1)))],
            Outcome::Failed,
        ),
        (
            "a value that does not decode fails the load",
            vec![(key(partition_metadata), Some(truncated))],
            Outcome::Failed,
        ),
    ];

    for (name, records, expected) in cases {
        check!(replay(records) == expected, "{name}");
    }
}
