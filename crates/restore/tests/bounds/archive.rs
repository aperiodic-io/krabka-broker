//! The KIP-405 archive every scenario restores from: real batches appended to
//! a real `krabka_log::Log`, sealed one segment per batch, and copied into a
//! `LocalTieredStorage` tree.
//!
//! Building the archive is the half of each fixture that has nothing to do
//! with any one bound flag, which is why it sits apart from the flags under
//! test.

use assert2::assert;
use bytes::Bytes;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_remote_storage::{LocalTieredStorage, TopicIdPartition};
use tempfile::TempDir;
use uuid::Uuid;

use crate::archive_fixture::archive_segment;

/// A `LogConfig` that rolls the active segment on every append past the
/// first, so [`build_archive`] can seal each fixture batch into its own
/// segment.
///
/// `krabka-restore` does not depend on `krabka-units`, so this scales the
/// log crate's own default `segment_size` -- Kafka's documented 1 GiB
/// `segment.bytes` -- down by its own byte count rather than naming
/// `krabka_units::ByteSize` directly. The result is far smaller than any
/// batch this file builds (every one is at least several dozen bytes) even
/// if `krabka_log`'s default ever changes, because the division only ever
/// shrinks it further.
fn roll_after_every_batch() -> LogConfig {
    LogConfig {
        segment_size: LogConfig::default().segment_size / 1_073_741_824.0,
        ..LogConfig::default()
    }
}

/// One throwaway batch, appended after every real fixture batch so the log
/// rolls and seals the batch before it. Its own bytes never reach the
/// archive: [`build_archive`] only copies `Log::tierable_segments`'s sealed
/// segments, and this trigger always ends up in the still-active one.
fn roll_trigger() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"roll-trigger")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// Append every batch in `batches` to a fresh local log, in order, each
/// sealed into its own segment via [`roll_after_every_batch`], then archive
/// every sealed segment into a fresh KIP-405 local tiered-storage tree for
/// `topic`-`partition`.
///
/// Mutates each batch in place with the `base_offset` `Log::append` assigns
/// it, so the caller's own copies become the exact values a restore's Keep
/// path round-trips verbatim -- ready to reuse as expected values.
///
/// Returns the archive root, to pass as `--archive-local`.
pub(crate) fn build_archive(topic: &str, partition: i32, batches: &mut [RecordBatch]) -> TempDir {
    assert!(!batches.is_empty(), "a fixture needs at least one batch");
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), roll_after_every_batch()).expect("open local log");
    for batch in batches.iter_mut() {
        log.append(batch).expect("append fixture batch");
    }
    // Seals the last real batch above; `tierable_segments` below excludes
    // whatever segment this trigger itself lands in.
    log.append(&mut roll_trigger())
        .expect("append roll trigger");

    log.sync().unwrap();
    let sealed = log.tierable_segments();
    assert!(
        sealed.len() == batches.len(),
        "expected one sealed segment per fixture batch, got {} for {}",
        sealed.len(),
        batches.len(),
    );

    let topic_id = Uuid::new_v4();
    let archive_root = tempfile::tempdir().expect("archive root tempdir");
    let storage = LocalTieredStorage::new(archive_root.path());
    for export in sealed {
        archive_segment(
            &storage,
            TopicIdPartition::new(topic_id, topic, partition),
            Uuid::new_v4(),
            &export,
        );
    }
    archive_root
}
