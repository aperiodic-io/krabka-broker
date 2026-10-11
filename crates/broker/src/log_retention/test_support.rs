//! Fixtures the local-retention tests share: a partition whose log holds
//! several sealed segments that `retention.ms` has already expired, the
//! segment files on disk behind it, and the block that makes their deletion
//! fail with a real `io::Error`.

use std::sync::Arc;

use bytes::Bytes;
use krabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;

use crate::{
    log_dir::partition_dir,
    partition::Partition,
    test_support::{CommittedPartitionSetup, block_log_artifact_paths, committed_partition},
};

/// One record at `base`, timestamped at the epoch so every retention setting
/// this module uses has already expired it.
fn epoch_batch(base: i64, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset: base,
        base_timestamp: 0,
        max_timestamp: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A partition hosting a log with sealed, expired segments, registered in
/// `log_dir_status` so a deletion failure has somewhere to report itself.
///
/// `segment_size` is small enough that each append seals the previous segment,
/// and `retention` is a millisecond, so every sealed segment is past its
/// expiry the moment the sweep looks at it.
pub(super) async fn expired_partition(
    root: &TempDir,
    setup: CommittedPartitionSetup<'_>,
) -> Arc<Partition> {
    let part_dir = partition_dir(root.path(), setup.topic, setup.partition.0);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let cfg = krabka_log::LogConfig {
        segment_size: krabka_units::bytes(64),
        retention: Some(krabka_units::millis(1)),
        ..Default::default()
    };
    let mut log = krabka_log::Log::open(&part_dir, cfg).expect("open retainable log");
    for idx in 0..8 {
        let mut batch = epoch_batch(idx, format!("value-{idx}").as_bytes());
        log.append(&mut batch).expect("append expired batch");
    }
    committed_partition(root.path(), log, setup).await
}

/// The base offsets of the segment files currently on disk for `topic`.
pub(super) fn segment_files(root: &TempDir, topic: &str) -> Vec<String> {
    let part_dir = partition_dir(root.path(), topic, 0);
    let mut names: Vec<String> = std::fs::read_dir(&part_dir)
        .expect("read partition dir")
        .map(|entry| entry.expect("partition dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .map(|path| {
            path.file_name()
                .expect("segment file name")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// The log's own view of how many bytes it holds.
pub(super) fn log_size(partition: &Partition) -> u64 {
    use krabka_units::prelude::ByteSizeExt as _;
    partition
        .log
        .lock()
        .expect("partition log lock")
        .size()
        .bytes_u64()
}

/// With `file_delete_delay` set to zero, make every segment deletion fail with a real `io::Error`,
/// by putting a directory where the eviction must rename the segment to its
/// `.deleted` tombstone. Renaming a file onto a directory fails with `EISDIR`
/// for every user including root, so this is a storage failure the filesystem
/// raises rather than one a test hook fabricates.
///
/// Returns the paths it blocked so a test can unblock them and watch the sweep
/// recover.
pub(super) fn block_segment_deletion(root: &TempDir, topic: &str) -> Vec<std::path::PathBuf> {
    block_log_artifact_paths(
        root.path(),
        topic,
        "log.deleted",
        "block the tombstone path",
    )
}
