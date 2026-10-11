//! Which group a group id holds as the log replays, as Kafka 4.3.1's
//! `GroupMetadataManager` keeps one group per id whatever its type.
//!
//! An offset commit of an id with no group creates a simple classic group. A
//! consumer or streams record replaces a simple classic group, and fails the
//! load over any other classic group or a group of another modern type; a
//! share record fails over any classic group. A classic `GroupMetadata` value
//! puts a classic group in place of whatever held the id, and its tombstone
//! removes the group of any type.

use assert2::check;
use bytes::Bytes;

use super::test_support::{classic_group_record, replay_log};
use crate::{
    coordinator::{
        persistence::{GroupMetadataValue as ClassicGroupMetadataValue, OffsetCommitValue},
        unified::{
            persistence_next_gen::{GroupMetadataValue, NextGenKey, encode_key},
            share::persistence::{ShareGroupKey, ShareGroupMetadataValue, encode_share_key},
        },
    },
    error::BrokerError,
};

/// One replayed record: its key, and its value or `None` for a tombstone.
type LogRecord = (Bytes, Option<Bytes>);

/// What the replay holds under group id `g`.
#[derive(Debug, PartialEq, Eq)]
enum Held {
    Failed(String),
    Nothing,
    Classic,
    Consumer,
    Share,
}

fn consumer_epoch() -> LogRecord {
    (
        encode_key(&NextGenKey::GroupMetadata {
            group_id: "g".into(),
        })
        .unwrap(),
        Some(
            GroupMetadataValue {
                epoch: 2,
                metadata_hash: 0,
            }
            .encode(),
        ),
    )
}

fn share_epoch() -> LogRecord {
    (
        encode_share_key(&ShareGroupKey::GroupMetadata {
            group_id: "g".into(),
        })
        .unwrap(),
        Some(
            ShareGroupMetadataValue {
                epoch: 2,
                metadata_hash: 0,
            }
            .encode(),
        ),
    )
}

fn offset_commit() -> LogRecord {
    (
        OffsetCommitValue::encode_key("g", "t", 0).unwrap(),
        Some(
            OffsetCommitValue {
                offset: krabka_log::Offset(5),
                leader_epoch: -1,
                metadata: String::new(),
                commit_timestamp_ms: 1,
                expire_timestamp_ms: None,
                topic_id: None,
            }
            .encode_value(),
        ),
    )
}

fn classic_group() -> LogRecord {
    let (key, value) = classic_group_record("g", "m1");
    (key, Some(value))
}

fn classic_tombstone() -> LogRecord {
    (ClassicGroupMetadataValue::encode_key("g").unwrap(), None)
}

fn replay(records: Vec<LogRecord>) -> Held {
    let (coordinator, replayed) = replay_log(records);
    let replayed = match replayed {
        Ok(replayed) => replayed,
        Err(BrokerError::Startup(message)) => return Held::Failed(message),
        Err(other) => return Held::Failed(other.to_string()),
    };
    if coordinator.seeds.contains_key("g") {
        Held::Consumer
    } else if coordinator.share_seeds.contains_key("g") {
        Held::Share
    } else if replayed.classic.contains_key("g") || replayed.simple.contains("g") {
        Held::Classic
    } else {
        Held::Nothing
    }
}

#[test]
fn one_group_per_id_as_kafka() {
    let rows = [
        (
            "an offset commit creates a simple classic group",
            vec![offset_commit()],
            Held::Classic,
        ),
        (
            "a consumer record replaces the simple classic group",
            vec![offset_commit(), consumer_epoch()],
            Held::Consumer,
        ),
        (
            "a consumer record does not replace a classic group with members",
            vec![classic_group(), consumer_epoch()],
            Held::Failed("Group g is not a consumer group".into()),
        ),
        (
            "an upgrade tombstones the classic group first",
            vec![classic_group(), classic_tombstone(), consumer_epoch()],
            Held::Consumer,
        ),
        (
            "a share record does not replace even a simple classic group",
            vec![offset_commit(), share_epoch()],
            Held::Failed("Group g is not a share group.".into()),
        ),
        (
            "a share record does not replace a consumer group",
            vec![consumer_epoch(), share_epoch()],
            Held::Failed("Group g is not a share group.".into()),
        ),
        (
            "a classic value replaces a consumer group",
            vec![consumer_epoch(), classic_group()],
            Held::Classic,
        ),
        (
            "a classic tombstone removes a consumer group",
            vec![consumer_epoch(), classic_tombstone()],
            Held::Nothing,
        ),
    ];
    for (case, records, expected) in rows {
        check!(replay(records) == expected, "{case}");
    }
}
