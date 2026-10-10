//! Unit tests for the archive scan: how a clean archive groups and sorts, what
//! `--topic` and `--archive-prefix` change about the selection, where a key the
//! codec cannot attribute ends up, and which archives are empty enough to be an
//! error.

use assert2::check;
use krabka_ids::{Offset, PartitionIndex};
use krabka_remote_storage::kafka_uuid;

use super::{
    test_support::{
        FULL_SEGMENT_SUFFIXES, PartitionKey, args_from, expected_full_segment,
        single_segment_archive, write_artifact, write_full_segment,
    },
    *,
};
use crate::{
    backend::open_archive,
    discover::test_support::{ArtifactSetup, SegmentSetup},
};

async fn scan_archive(root: &std::path::Path) -> ArchiveInventory {
    let args = args_from(root, &[]);
    let store = open_archive(&args).expect("store");
    inventory(&store, &args).await.expect("inventory")
}

fn torn_archive(prefix: Option<&str>) -> tempfile::TempDir {
    let archive = tempfile::tempdir().expect("temp dir");
    let key = PartitionKey {
        topic: "orders",
        partition: PartitionIndex(0),
        topic_id: Uuid::from_u128(1),
    };
    let segment = Uuid::from_u128(10);
    for suffix in [".log", ".index"] {
        write_artifact(
            archive.path(),
            ArtifactSetup {
                prefix,
                segment: SegmentSetup {
                    partition: key,
                    segment_id: segment,
                    ..Default::default()
                },
                suffix,
            },
        );
    }
    archive
}

#[tokio::test]
async fn a_clean_archive_groups_and_sorts_by_topic_partition_and_base_offset() {
    let archive = tempfile::tempdir().expect("temp dir");
    let orders_id = Uuid::from_u128(1);
    let alerts_id = Uuid::from_u128(2);
    let seg_a = Uuid::from_u128(10);
    let seg_b = Uuid::from_u128(11);
    let seg_c = Uuid::from_u128(12);
    let seg_d = Uuid::from_u128(20);
    let seg_e = Uuid::from_u128(30);

    // orders-0: three segments, written out of base-offset order.
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic_id: orders_id,
                ..Default::default()
            },
            base_offset: Offset(100),
            segment_id: seg_b,
        },
    );
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic_id: orders_id,
                ..Default::default()
            },
            segment_id: seg_a,
            ..Default::default()
        },
    );
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic_id: orders_id,
                ..Default::default()
            },
            base_offset: Offset(250),
            segment_id: seg_c,
        },
    );
    // orders-1: one segment.
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                partition: PartitionIndex(1),
                topic_id: orders_id,
                ..Default::default()
            },
            segment_id: seg_d,
            ..Default::default()
        },
    );
    // alerts-0: one segment. "alerts" sorts before "orders".
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic: "alerts",
                topic_id: alerts_id,
                ..Default::default()
            },
            segment_id: seg_e,
            ..Default::default()
        },
    );

    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    let expected = ArchiveInventory {
        partitions: vec![
            PartitionInventory {
                partition: TopicIdPartition::new(alerts_id, "alerts", 0),
                segments: vec![expected_full_segment(
                    &store,
                    SegmentSetup {
                        partition: PartitionKey {
                            topic: "alerts",
                            topic_id: alerts_id,
                            ..Default::default()
                        },
                        segment_id: seg_e,
                        ..Default::default()
                    },
                )],
            },
            PartitionInventory {
                partition: TopicIdPartition::new(orders_id, "orders", 0),
                segments: vec![
                    expected_full_segment(
                        &store,
                        SegmentSetup {
                            partition: PartitionKey {
                                topic_id: orders_id,
                                ..Default::default()
                            },
                            segment_id: seg_a,
                            ..Default::default()
                        },
                    ),
                    expected_full_segment(
                        &store,
                        SegmentSetup {
                            partition: PartitionKey {
                                topic_id: orders_id,
                                ..Default::default()
                            },
                            base_offset: Offset(100),
                            segment_id: seg_b,
                        },
                    ),
                    expected_full_segment(
                        &store,
                        SegmentSetup {
                            partition: PartitionKey {
                                topic_id: orders_id,
                                ..Default::default()
                            },
                            base_offset: Offset(250),
                            segment_id: seg_c,
                        },
                    ),
                ],
            },
            PartitionInventory {
                partition: TopicIdPartition::new(orders_id, "orders", 1),
                segments: vec![expected_full_segment(
                    &store,
                    SegmentSetup {
                        partition: PartitionKey {
                            partition: PartitionIndex(1),
                            topic_id: orders_id,
                            ..Default::default()
                        },
                        segment_id: seg_d,
                        ..Default::default()
                    },
                )],
            },
        ],
        unrecognized: UnrecognizedKeys::default(),
    };
    check!(result == expected);

    // `TopicIdPartition` equality ignores the topic name, so pin the
    // names too: a bug that mixed up which name goes with which id would
    // not otherwise be caught by the struct comparison above.
    let names: Vec<(&str, i32)> = result
        .partitions
        .iter()
        .map(|p| (p.partition.topic.as_str(), p.partition.partition))
        .collect();
    check!(names == vec![("alerts", 0), ("orders", 0), ("orders", 1)]);
}

#[tokio::test]
async fn a_topic_filter_narrows_the_selection_and_the_rest_lands_in_unrecognized() {
    let archive = tempfile::tempdir().expect("temp dir");
    let orders_id = Uuid::from_u128(1);
    let payments_id = Uuid::from_u128(2);
    let orders_seg = Uuid::from_u128(10);
    let payments_seg = Uuid::from_u128(20);
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic_id: orders_id,
                ..Default::default()
            },
            segment_id: orders_seg,
            ..Default::default()
        },
    );
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic: "payments",
                topic_id: payments_id,
                ..Default::default()
            },
            segment_id: payments_seg,
            ..Default::default()
        },
    );

    let args = args_from(archive.path(), &["--topic", "orders"]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    check!(result.partitions[0].partition.topic == "orders");
    // Every key of the topic `--topic` excluded is kept, not dropped.
    check!(
        result.unrecognized.total()
            == u64::try_from(FULL_SEGMENT_SUFFIXES.len()).expect("a handful of suffixes")
    );
    check!(result.unrecognized.omitted == 0);
    let payments_dir = format!("payments-0-{}", kafka_uuid(payments_id));
    check!(
        result
            .unrecognized
            .sample
            .iter()
            .all(|key| key.to_string().contains(&payments_dir))
    );
}

#[tokio::test]
async fn malformed_keys_land_in_unrecognized_not_dropped_and_not_an_error() {
    let (archive, _topic_id, _) = single_segment_archive(Offset(0));
    // Wrong number of path components: a key directly under the root.
    std::fs::write(archive.path().join("not-a-valid-key.log"), b"junk").expect("write");
    // Two components, but the directory name does not decode.
    std::fs::create_dir_all(archive.path().join("weird-dir")).expect("mkdir");
    std::fs::write(
        archive
            .path()
            .join("weird-dir")
            .join("00000000000000000005-not-base64.log"),
        b"junk",
    )
    .expect("write");

    let result = scan_archive(archive.path()).await;

    check!(result.partitions.len() == 1);
    check!(result.unrecognized.total() == 2);
    check!(
        result
            .unrecognized
            .sample
            .iter()
            .any(|key| key.to_string().contains("not-a-valid-key.log"))
    );
    check!(
        result
            .unrecognized
            .sample
            .iter()
            .any(|key| key.to_string().contains("weird-dir"))
    );
}

/// The sample is capped and the rest counted, so a `--archive-prefix` that
/// points at the wrong tree costs a bounded amount of memory however many
/// keys the bucket holds. The total is still exact.
#[test]
fn unrecognized_keys_keep_a_bounded_sample_and_count_the_rest() {
    for seen in [
        0,
        1,
        UNRECOGNIZED_SAMPLE_LIMIT - 1,
        UNRECOGNIZED_SAMPLE_LIMIT,
        UNRECOGNIZED_SAMPLE_LIMIT + 1,
        UNRECOGNIZED_SAMPLE_LIMIT * 10,
    ] {
        let mut keys = UnrecognizedKeys::default();
        for index in 0..seen {
            keys.record(Path::from(format!("junk/{index}")));
        }

        let kept = seen.min(UNRECOGNIZED_SAMPLE_LIMIT);
        check!(
            keys == UnrecognizedKeys {
                sample: (0..kept).map(|i| Path::from(format!("junk/{i}"))).collect(),
                omitted: u64::try_from(seen - kept).expect("the fixture counts are small"),
            },
            "after {seen} keys",
        );
        check!(
            keys.total() == u64::try_from(seen).expect("the fixture counts are small"),
            "after {seen} keys",
        );
        check!(keys.is_empty() == (seen == 0), "after {seen} keys");
    }
}

#[tokio::test]
async fn a_torn_copy_still_appears_with_the_missing_artifact_as_none() {
    let archive = torn_archive(None);

    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.partitions.len() == 1);
    let segment = &result.partitions[0].segments[0];
    check!(segment.log.is_some());
    check!(segment.offset_index.is_some());
    check!(segment.time_index.is_none());
    check!(segment.producer_snapshot.is_none());
    check!(segment.leader_epoch.is_none());
    check!(segment.transaction_index.is_none());
}

#[tokio::test]
async fn a_key_prefix_does_not_confuse_the_relative_path_split() {
    let archive = torn_archive(Some("tier"));

    let args = args_from(archive.path(), &["--archive-prefix", "tier"]);
    let store = open_archive(&args).expect("store");
    let result = inventory(&store, &args).await.expect("inventory");

    check!(result.unrecognized.is_empty());
    check!(result.partitions.len() == 1);
    check!(result.partitions[0].segments.len() == 1);
}

/// A scan of an archive whose keys are mostly unattributable keeps the
/// bounded sample and the exact total, and still reports the segments it did
/// recognize. This is the `--archive-prefix` pointed at the wrong tree, which
/// is the case the bound exists for.
#[tokio::test]
async fn a_scan_past_the_sample_limit_keeps_the_sample_and_the_exact_total() {
    let (archive, _topic_id, _) = single_segment_archive(Offset(0));

    let junk = UNRECOGNIZED_SAMPLE_LIMIT + 6;
    let dir = archive.path().join("not-a-partition-dir");
    std::fs::create_dir_all(&dir).expect("create the junk dir");
    for index in 0..junk {
        std::fs::write(dir.join(format!("{index:04}.log")), b"junk").expect("write a junk key");
    }

    let result = scan_archive(archive.path()).await;

    check!(result.partitions.len() == 1);
    check!(result.unrecognized.sample.len() == UNRECOGNIZED_SAMPLE_LIMIT);
    check!(result.unrecognized.omitted == 6);
    check!(result.unrecognized.total() == u64::try_from(junk).expect("the fixture count is small"));
    check!(
        result
            .unrecognized
            .sample
            .iter()
            .all(|key| key.to_string().contains("not-a-partition-dir"))
    );
}

#[tokio::test]
async fn an_archive_with_nothing_in_it_is_an_empty_archive_error() {
    let archive = tempfile::tempdir().expect("temp dir");
    let args = args_from(archive.path(), &[]);
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(err, RestoreError::EmptyArchive { prefix } if prefix.is_empty()));
}

#[tokio::test]
async fn a_topic_filter_that_selects_nothing_is_also_an_empty_archive_error() {
    let (archive, _topic_id, _) = single_segment_archive(Offset(0));

    let args = args_from(archive.path(), &["--topic", "bogus"]);
    let store = open_archive(&args).expect("store");
    let err = inventory(&store, &args).await.unwrap_err();
    check!(matches!(err, RestoreError::EmptyArchive { .. }));
}
