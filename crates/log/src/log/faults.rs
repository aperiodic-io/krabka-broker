//! What each durable write the log makes costs when the disk fills under it.
//!
//! Every case here drives one [`IoTarget`] through [`Log::test_set_io`], with
//! the two failure shapes a full disk actually produces -- an outright
//! `StorageFull`, and a short write that lands part of a buffer and then fails
//! -- and then reopens the directory to state what survived. The reopen is the
//! assertion that matters: a write that fails silently costs nothing until the
//! next restart, which is where its cost is finally visible.

use {
    super::Log,
    crate::{
        config::LogConfig,
        error::LogError,
        io::{IoTarget, LogIo, file_io},
        log::test_support::{
            append_keyed_samples, compacting_segments, compaction_ctx, sample_batch,
            sample_batch_with_epoch, synced_sample_log, tiny_segments, write_with_budget,
        },
        name,
    },
    assert2::check,
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes},
    std::{
        fs::File,
        io::Write as _,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::SystemTime,
    },
    tempfile::tempdir,
};

/// A disk that fills under one file class: the first `prefix` bytes written to
/// `target` land, and every byte after them is [`std::io::ErrorKind::StorageFull`].
///
/// A `prefix` of zero is the outright failure; a nonzero one is the short
/// write that tears a file at exactly that boundary. Every other target keeps
/// working, so each case fails one thing and no more.
#[derive(Debug)]
struct DiskFull {
    target: IoTarget,
    budget: Mutex<usize>,
}

impl DiskFull {
    fn new(target: IoTarget, prefix: usize) -> Arc<Self> {
        Arc::new(Self {
            target,
            budget: Mutex::new(prefix),
        })
    }
}

impl LogIo for DiskFull {
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        if target != self.target {
            return (&*file).write(buf);
        }
        let mut budget = self.budget.lock().unwrap();
        write_with_budget(file, buf, &mut budget)
    }
}

/// A directory whose `fsync` fails. Nothing else does.
#[derive(Debug)]
struct DirSyncFull;

impl LogIo for DirSyncFull {
    fn sync_dir(&self, _dir: &Path) -> std::io::Result<()> {
        Err(std::io::ErrorKind::StorageFull.into())
    }
}

/// A read-only remount seen from one file: the first attempt to rename or
/// unlink `file_name` fails, and the attempt after it succeeds.
#[derive(Debug)]
struct FailSegmentDeletionOnce {
    /// The live name whose rename to a tombstone fails, when `on_rename`.
    file_name: String,
    /// `true` fails the rename to the `.deleted` tombstone; `false` lets the
    /// rename through and fails the unlink of the tombstone instead.
    on_rename: bool,
    armed: AtomicBool,
}

impl FailSegmentDeletionOnce {
    fn new(file_name: &str, on_rename: bool) -> Arc<Self> {
        Arc::new(Self {
            file_name: file_name.to_owned(),
            on_rename,
            armed: AtomicBool::new(true),
        })
    }

    /// Fire once for `path`, if it is the file this fault is aimed at.
    fn fires_for(&self, path: &Path, wanted: &str) -> bool {
        path.file_name().is_some_and(|name| name == wanted)
            && self.armed.swap(false, Ordering::Relaxed)
    }
}

impl LogIo for FailSegmentDeletionOnce {
    fn rename(&self, target: IoTarget, from: &Path, to: &Path) -> std::io::Result<()> {
        if target == IoTarget::SegmentDeletion
            && self.on_rename
            && self.fires_for(from, &self.file_name)
        {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        std::fs::rename(from, to)
    }

    fn remove_file(&self, target: IoTarget, path: &Path) -> std::io::Result<()> {
        let tombstone = format!("{}.deleted", self.file_name);
        if target == IoTarget::SegmentDeletion
            && !self.on_rename
            && self.fires_for(path, &tombstone)
        {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        std::fs::remove_file(path)
    }
}

/// The extension of `path`, if it has one.
fn extension_of(path: &Path) -> Option<&str> { path.extension().and_then(std::ffi::OsStr::to_str) }

/// `true` for a segment `.log`, whether it is still under its live name or has
/// already been renamed to its `.deleted` tombstone. Both occupy the disk the
/// operator is watching.
fn holds_segment_bytes(path: &Path) -> bool {
    match extension_of(path) {
        Some("log") => true,
        Some("deleted") => path
            .file_stem()
            .is_some_and(|stem| extension_of(Path::new(stem)) == Some("log")),
        _ => false,
    }
}

/// The bytes the directory really holds for segment data.
fn on_disk_log_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| holds_segment_bytes(&entry.path()))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

/// Assert `error` is the injected disk-full write.
fn is_storage_full(error: &LogError) -> bool {
    matches!(error, LogError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull)
}

/// The two failure shapes every seam case is run against: an outright
/// `StorageFull`, and a short write that lands `prefix` bytes first.
const PREFIXES: [(&str, usize); 2] = [("outright StorageFull", 0), ("after a short write", 6)];

fn durable_sample_log(config: &LogConfig) -> (tempfile::TempDir, Log, Offset) {
    let dir = tempdir().unwrap();
    let log = synced_sample_log(dir.path(), config.clone(), 1, 2);
    let durable = log.log_end_offset();
    (dir, log, durable)
}

/// A sparse-index write that fails is reported, and the batch it indexed is
/// rolled back rather than left half-indexed for the next open to find.
#[test]
fn a_disk_full_sparse_index_write_is_reported_and_rolled_back() {
    for target in [IoTarget::OffsetIndex, IoTarget::TimeIndex] {
        for (label, prefix) in PREFIXES {
            // One index entry per batch, so the very next append writes one.
            let config = LogConfig {
                index_interval: bytes(1),
                ..LogConfig::default()
            };
            let (dir, mut log, durable) = durable_sample_log(&config);

            log.test_set_io(DiskFull::new(target, prefix));
            // A newer timestamp, because the time index only takes an entry
            // that is newer than its last one.
            let mut newer = sample_batch(2);
            newer.max_timestamp = 1;
            let error = log
                .append(&mut newer)
                .expect_err("the index write must fail the append");
            check!(is_storage_full(&error), "{target:?}, {label}: {error:?}");
            drop(log);

            let reopened = Log::open(dir.path(), config).unwrap();
            check!(
                reopened.log_end_offset() == durable,
                "{target:?}, {label}: the failed append must not survive the reopen"
            );
        }
    }
}

/// A producer snapshot that cannot be written fails the roll that publishes
/// it, and leaves nothing a reopen mistakes for a snapshot.
#[test]
fn a_disk_full_producer_snapshot_fails_the_roll_and_the_log_reopens_without_it() {
    for (label, prefix) in PREFIXES {
        // Every append after the first rolls the active segment, and a roll is
        // what publishes the boundary snapshot.
        let config = LogConfig {
            flush_on_append: true,
            ..tiny_segments()
        };
        let (dir, mut log, durable) = durable_sample_log(&config);

        log.test_set_io(DiskFull::new(IoTarget::ProducerSnapshot, prefix));
        let error = log
            .append(&mut sample_batch(2))
            .expect_err("the snapshot write must fail the roll");
        check!(is_storage_full(&error), "{label}: {error:?}");
        drop(log);
        check!(
            !name::producer_snapshot_path(dir.path(), durable.0).exists(),
            "{label}: a torn snapshot must not be published"
        );

        // Kafka's `loadProducerState` snapshots the rebuilt state at the log
        // end, so the reopen publishes a whole snapshot where the torn one
        // would have been.
        let reopened = Log::open(dir.path(), config).unwrap();
        check!(
            reopened.log_end_offset() == durable,
            "{label}: the rolled-away batch must not survive the reopen"
        );
    }
}

/// A leader-epoch checkpoint that cannot be written fails the append that
/// opened the epoch, and the reopened log still reports the last epoch that
/// did reach disk.
#[test]
fn a_disk_full_leader_epoch_checkpoint_leaves_the_previous_checkpoint_standing() {
    for (label, prefix) in PREFIXES {
        let dir = tempdir().unwrap();
        let config = LogConfig::default();
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        log.append(&mut sample_batch_with_epoch(2, 3)).unwrap();
        log.sync().unwrap();
        let durable_entries = log.epoch_checkpoint().entries().to_vec();

        log.test_set_io(DiskFull::new(IoTarget::LeaderEpochCheckpoint, prefix));
        let error = log
            .append(&mut sample_batch_with_epoch(2, 9))
            .expect_err("the checkpoint write must fail the append");
        check!(is_storage_full(&error), "{label}: {error:?}");
        drop(log);

        let reopened = Log::open(dir.path(), config).unwrap();
        check!(
            reopened.epoch_checkpoint().entries() == durable_entries,
            "{label}: the torn `.tmp` must never replace the checkpoint"
        );
    }
}

/// Build a compactable log of twelve one-record segments under one key each,
/// so a pass has real work and produces a real swap.
fn compactable_log(dir: &Path) -> (LogConfig, Log) {
    let config = compacting_segments();
    let mut log = Log::open(dir, config.clone()).unwrap();
    append_keyed_samples(&mut log, 12, |i| format!("k{}", i % 3));
    log.sync().unwrap();
    (config, log)
}

/// A `.swap` file that cannot be written fails the pass, and `Log::open`
/// discards the orphan the way it discards a torn one: the pre-compaction
/// segments are still authoritative and every record is still there.
#[test]
fn a_disk_full_compaction_swap_leaves_the_pre_compaction_log_intact() {
    for (label, prefix) in PREFIXES {
        let dir = tempdir().unwrap();
        let (config, mut log) = compactable_log(dir.path());
        let before = log.log_end_offset();

        log.test_set_io(DiskFull::new(IoTarget::CompactionSwap, prefix));
        let error = log
            .compact(&compaction_ctx())
            .expect_err("the swap write must fail the pass");
        check!(is_storage_full(&error), "{label}: {error:?}");
        drop(log);

        let reopened = Log::open(dir.path(), config).unwrap();
        check!(
            reopened.log_end_offset() == before,
            "{label}: an unwritten swap must not move the log end"
        );
        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| extension_of(path) == Some("swap"))
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        check!(
            leftovers.is_empty(),
            "{label}: orphan swaps left: {leftovers:?}"
        );
    }
}

/// The compaction swap's directory `fsync` is reported rather than dropped.
///
/// Until it returns, the renames that promoted the swap live only in the
/// directory's page cache. A crash there restores the pre-compaction names,
/// which on a compacted topic is a tombstoned key coming back to life, so the
/// caller has to hear about it. `Log::open` then heals whichever set of names
/// survived, exactly as it does for a swap torn mid-rename.
#[test]
fn a_failed_compaction_directory_fsync_is_reported_and_the_reopened_log_is_whole() {
    let dir = tempdir().unwrap();
    let (config, mut log) = compactable_log(dir.path());
    let before = log.log_end_offset();

    log.test_set_io(Arc::new(DirSyncFull));
    let error = log
        .compact(&compaction_ctx())
        .expect_err("a dropped directory fsync is what this test exists to forbid");
    check!(is_storage_full(&error), "{error:?}");
    drop(log);

    let reopened = Log::open(dir.path(), config).unwrap();
    check!(reopened.log_end_offset() == before);
    let records = reopened
        .read(Offset(0), krabka_units::mebibytes(4))
        .unwrap();
    check!(
        !records.batches.is_empty(),
        "the promoted segment must still be readable"
    );
}

/// Retention that cannot unlink a segment keeps it in the log's own
/// accounting, and the next tick retries it.
///
/// Dropping the segment from `self.segments` on a failed unlink is what makes
/// the leak invisible: the in-memory size and the partition's disk gauge both
/// fall while the bytes stay on the filesystem, and nothing is left to retry.
/// Both halves of the deletion are driven -- the rename to the tombstone and
/// the unlink of it -- because a partial failure of either has to leave the
/// same consistent picture.
#[test]
fn a_failed_segment_deletion_keeps_the_bytes_accounted_for_and_the_next_tick_retries() {
    for on_rename in [true, false] {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention_size: Some(ByteSize::ZERO),
            file_delete_delay: krabka_units::millis(0),
            ..tiny_segments()
        };
        let mut log = synced_sample_log(dir.path(), config, 4, 2);

        let fault = FailSegmentDeletionOnce::new(
            &format!("{}.log", name::format_base_offset(0)),
            on_rename,
        );
        log.test_set_io(fault);
        let error = log
            .tick(SystemTime::now(), krabka_ids::Offset(i64::MAX))
            .expect_err("a failed unlink must be reported, not discarded");
        check!(
            matches!(&error, LogError::Io(io) if io.kind() == std::io::ErrorKind::PermissionDenied),
            "on_rename={on_rename}: {error:?}"
        );
        check!(
            log.size().bytes_u64() == on_disk_log_bytes(dir.path()),
            "on_rename={on_rename}: the log must still report the bytes that are really there"
        );

        // The fault fires once, so the retry is what a healthy disk gives.
        log.test_set_io(file_io());
        log.tick(SystemTime::now(), krabka_ids::Offset(i64::MAX))
            .expect("the retry finishes the deletion");
        check!(
            log.size().bytes_u64() == on_disk_log_bytes(dir.path()),
            "on_rename={on_rename}: the retry leaves the accounting consistent"
        );
        check!(
            !name::log_path(dir.path(), 0).exists(),
            "on_rename={on_rename}: the segment is gone"
        );
        check!(
            !dir.path()
                .join(format!("{}.log.deleted", name::format_base_offset(0)))
                .exists(),
            "on_rename={on_rename}: no tombstone is left behind"
        );
    }
}

/// A deletion interrupted between the rename and the unlink leaves tombstones
/// and stranded sidecars, and `Log::open` reclaims both.
///
/// `Log::open` finds segments by `.log` filename, so a sidecar whose `.log` is
/// already gone is invisible to every later pass and would never be reclaimed
/// without this sweep.
#[test]
fn log_open_reclaims_what_an_interrupted_segment_deletion_left_behind() {
    let dir = tempdir().unwrap();
    let config = tiny_segments();
    let log = synced_sample_log(dir.path(), config.clone(), 4, 2);
    let end = log.log_end_offset();
    drop(log);

    // A `.log` renamed to its tombstone but never unlinked, and the sidecars
    // of a segment whose `.log` did get unlinked.
    let tombstone = dir
        .path()
        .join(format!("{}.log.deleted", name::format_base_offset(0)));
    std::fs::rename(name::log_path(dir.path(), 0), &tombstone).unwrap();
    let stranded = [
        name::index_path(dir.path(), 0),
        name::timeindex_path(dir.path(), 0),
    ];

    let reopened = Log::open(dir.path(), config).unwrap();

    check!(!tombstone.exists(), "the tombstone is reclaimed");
    for path in &stranded {
        check!(
            !path.exists(),
            "the sidecar of a vanished segment is reclaimed: {}",
            path.display()
        );
    }
    check!(
        reopened.log_end_offset() == end,
        "the surviving segments still describe the log"
    );
}
