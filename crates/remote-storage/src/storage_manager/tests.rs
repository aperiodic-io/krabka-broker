//! Tests for the Kafka-compatible remote-tier naming: the index suffixes, the
//! partition-directory and segment-file names that [`super::partition_dir_name`]
//! and [`super::segment_file_name`] render, and the parsers that read them
//! back, including the property tests that round-trip both names.
//!
//! They sit in their own file because the pinned Kafka names, the malformed
//! inputs each parser must reject, and the two round-trip properties run to
//! nearly as many lines as the module they check.

use assert2::check;
use krabka_ids::{LeaderEpoch, Offset, PartitionIndex};
use proptest::prelude::*;

use super::*;
use crate::metadata::{
    RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition,
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct StorageMetadataSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(PartitionIndex(0))]
    partition: PartitionIndex,
    #[default(Uuid::from_u128(1))]
    topic_id: Uuid,
    #[default(Uuid::from_u128(30))]
    segment_id: Uuid,
    #[default(Offset(0))]
    base_offset: Offset,
}

fn metadata(setup: StorageMetadataSetup<'_>) -> RemoteLogSegmentMetadata {
    let StorageMetadataSetup {
        topic,
        partition,
        topic_id,
        segment_id,
        base_offset,
    } = setup;
    let base_offset = base_offset.0;
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, topic, partition.0),
            segment_id,
        ),
        base_offset,
        base_offset,
        0,
        1,
        0,
        RemoteLogSegmentDetails::new(
            1,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => base_offset},
        ),
    )
    .unwrap()
}

fn sample() -> RemoteLogSegmentMetadata {
    metadata(StorageMetadataSetup {
        partition: PartitionIndex(7),
        segment_id: Uuid::from_u128(0xfe),
        base_offset: Offset(11),
        ..Default::default()
    })
}

#[test]
fn index_suffixes_match_kafka() {
    // Filesystem-backed stores key files off these exact suffixes.
    for (index_type, want) in [
        (IndexType::Offset, ".index"),
        (IndexType::Timestamp, ".timeindex"),
        (IndexType::ProducerSnapshot, ".snapshot"),
        (IndexType::LeaderEpoch, ".leader_epoch_checkpoint"),
        (IndexType::Transaction, ".txnindex"),
    ] {
        check!(index_type.suffix() == want, "{index_type:?}");
        check!(IndexType::from_suffix(want) == Some(index_type));
    }
}

#[test]
fn from_suffix_rejects_non_index_suffixes() {
    for suffix in [LOG_FILE_SUFFIX, "", ".INDEX", "index", ".timeindex2"] {
        check!(IndexType::from_suffix(suffix) == None, "{suffix:?}");
    }
}

#[test]
fn local_tiered_storage_names_match_kafka() {
    let metadata = sample();

    check!(partition_dir_name(&metadata) == "orders-7-AAAAAAAAAAAAAAAAAAAAAQ");
    check!(
        segment_file_name(&metadata, IndexType::ProducerSnapshot.suffix())
            == "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot"
    );
}

#[test]
fn object_store_key_components_match_kafka() {
    // The two halves of the S3 key that the object-store backend writes.
    let metadata = metadata(StorageMetadataSetup::default());

    check!(partition_dir_name(&metadata) == "orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
    check!(
        segment_file_name(&metadata, LOG_FILE_SUFFIX)
            == "00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log"
    );
}

#[test]
fn parses_the_pinned_kafka_names() {
    check!(
        parse_partition_dir_name("orders-7-AAAAAAAAAAAAAAAAAAAAAQ")
            == Some(PartitionDirName {
                topic: "orders".to_owned(),
                partition: 7,
                topic_id: Uuid::from_u128(1),
            })
    );
    check!(
        parse_segment_file_name("00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot")
            == Some(SegmentFileName {
                base_offset: 11,
                segment_id: Uuid::from_u128(0xfe),
                suffix: ".snapshot",
            })
    );
    check!(
        parse_segment_file_name("00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log")
            == Some(SegmentFileName {
                base_offset: 0,
                segment_id: Uuid::from_u128(30),
                suffix: ".log",
            })
    );
}

#[test]
fn parse_partition_dir_name_splits_a_hyphenated_topic_from_the_right() {
    // "orders-7" is a topic name, not a topic and a partition index.
    check!(
        parse_partition_dir_name("orders-7-3-AAAAAAAAAAAAAAAAAAAAAQ")
            == Some(PartitionDirName {
                topic: "orders-7".to_owned(),
                partition: 3,
                topic_id: Uuid::from_u128(1),
            })
    );
    // A partition index is never negative, so the leading `-` of "-1"
    // belongs to the topic name and the index is 1.
    check!(
        parse_partition_dir_name("orders--1-AAAAAAAAAAAAAAAAAAAAAQ")
            == Some(PartitionDirName {
                topic: "orders-".to_owned(),
                partition: 1,
                topic_id: Uuid::from_u128(1),
            })
    );
}

#[test]
fn parse_partition_dir_name_rejects_malformed_names() {
    for name in [
        // Topic id one character short of the fixed Base64 width.
        "orders-7-AAAAAAAAAAAAAAAAAAAAA",
        // Topic id that decodes to more than 16 bytes.
        "orders-7-AAAAAAAAAAAAAAAAAAAAAQAA",
        // Non-numeric partition index.
        "orders-x-AAAAAAAAAAAAAAAAAAAAAQ",
        // Empty topic.
        "-7-AAAAAAAAAAAAAAAAAAAAAQ",
        // A key separator cannot appear inside one directory name.
        "or/ders-7-AAAAAAAAAAAAAAAAAAAAAQ",
        // No partition index at all.
        "orders-AAAAAAAAAAAAAAAAAAAAAQ",
        "",
    ] {
        check!(parse_partition_dir_name(name) == None, "{name:?}");
    }
}

#[test]
fn parse_segment_file_name_rejects_malformed_names() {
    for name in [
        // Base offset one digit short of the fixed width.
        "0000000000000000001-AAAAAAAAAAAAAAAAAAAA_g.snapshot",
        // Non-numeric base offset.
        "0000000000000000001x-AAAAAAAAAAAAAAAAAAAA_g.snapshot",
        // Segment id one character short of the fixed Base64 width.
        "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_",
        // Missing the separator between base offset and segment id.
        "00000000000000000011xAAAAAAAAAAAAAAAAAAAA_g.snapshot",
        // Segment id that is not Base64 at all.
        "00000000000000000011-**********************.snapshot",
        "",
    ] {
        check!(parse_segment_file_name(name) == None, "{name:?}");
    }
}

#[test]
fn decode_kafka_uuid_rejects_wrong_length_base64() {
    for encoded in ["", "AAAA", "AAAAAAAAAAAAAAAAAAAAAQAA", "A"] {
        check!(decode_kafka_uuid(encoded) == None, "{encoded:?}");
    }
}

proptest! {
    #[test]
    fn partition_dir_name_round_trips(
        topic in "[a-z0-9]{1,6}(-[a-z0-9]{1,6}){0,3}",
        partition in 0_i32..=i32::MAX,
        topic_id in any::<u128>(),
    ) {
        let topic_id = Uuid::from_u128(topic_id);
        let metadata = metadata(StorageMetadataSetup { topic: &topic, partition: PartitionIndex(partition), topic_id, segment_id: Uuid::nil(), ..Default::default() });
        let name = partition_dir_name(&metadata);
        prop_assert!(
            parse_partition_dir_name(&name)
                == Some(PartitionDirName { topic, partition, topic_id }),
            "{name}"
        );
    }

    #[test]
    fn segment_file_name_round_trips(
        base_offset in 0_i64..=i64::MAX,
        segment_id in any::<u128>(),
        index_type in prop_oneof![
            Just(None),
            Just(Some(IndexType::Offset)),
            Just(Some(IndexType::Timestamp)),
            Just(Some(IndexType::ProducerSnapshot)),
            Just(Some(IndexType::LeaderEpoch)),
            Just(Some(IndexType::Transaction)),
        ],
    ) {
        let segment_id = Uuid::from_u128(segment_id);
        let suffix = index_type.map_or(LOG_FILE_SUFFIX, IndexType::suffix);
        let metadata = metadata(StorageMetadataSetup { topic_id: Uuid::nil(), segment_id, base_offset: Offset(base_offset), ..Default::default() });
        let name = segment_file_name(&metadata, suffix);
        let parsed = parse_segment_file_name(&name);
        prop_assert!(
            parsed == Some(SegmentFileName { base_offset, segment_id, suffix }),
            "{name}"
        );
        prop_assert!(
            parsed.and_then(|p| IndexType::from_suffix(p.suffix)) == index_type,
            "{name}"
        );
    }
}
