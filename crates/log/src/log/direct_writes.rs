//! What the `O_DIRECT` write path of `preallocate` writes, and what a reader
//! gets back.
//!
//! Most cases run under a [`LogIo`] that hands out an ordinary file in place of
//! an `O_DIRECT` handle and checks every write against the alignment it named,
//! so they hold on any filesystem, tmpfs included. The last case goes through
//! the real `O_DIRECT` where the kernel offers it, and through the page cache
//! where it does not, and expects the same log either way.

use {
    super::{
        Log,
        test_support::{
            NO_LIMIT, append_samples, assert_log_end_offset, configured_test_log, sample_batch,
            verbatim_from,
        },
    },
    crate::{
        DeliveryPolicy, LeaderEpoch, VerbatimBatch,
        config::{LogConfig, SegmentAllocation},
        io::{DirectFile, IoTarget, LogIo},
        name,
    },
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes, mebibytes},
    std::{
        fs::{File, OpenOptions},
        io::Write as _,
        os::unix::fs::FileExt as _,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    },
};

const BLOCK: usize = 512;

/// A disk whose "`O_DIRECT`" handle is an ordinary file that refuses, by
/// panicking, any write `O_DIRECT` would refuse, and records the rest.
#[derive(Debug, Default)]
struct AlignedDisk {
    /// Fail the next direct write with a full disk.
    fail_next: AtomicBool,
    partial_next: AtomicBool,
    fail_target: Mutex<Option<IoTarget>>,
    /// `(offset, len)` of every direct write.
    writes: Mutex<Vec<(u64, usize)>>,
}

impl LogIo for AlignedDisk {
    fn open_direct(&self, path: &Path) -> std::io::Result<DirectFile> {
        Ok(DirectFile {
            file: OpenOptions::new().read(true).write(true).open(path)?,
            mem_align: BLOCK,
            block: BLOCK,
        })
    }

    fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        assert2::assert!(offset % BLOCK as u64 == 0);
        assert2::assert!(buf.len() % BLOCK == 0);
        assert2::assert!(buf.as_ptr().align_offset(BLOCK) == 0);
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(std::io::ErrorKind::StorageFull.into());
        }
        self.writes.lock().unwrap().push((offset, buf.len()));
        if self.partial_next.swap(false, Ordering::SeqCst) {
            self.fail_next.store(true, Ordering::SeqCst);
            return file.write_at(&buf[..BLOCK], offset);
        }
        file.write_at(buf, offset)
    }

    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        let mut fail = self.fail_target.lock().unwrap();
        if *fail == Some(target) {
            *fail = None;
            return Err(std::io::ErrorKind::StorageFull.into());
        }
        (&*file).write(buf)
    }
}

fn direct(tail_cache_size: ByteSize) -> LogConfig {
    LogConfig {
        segment_size: mebibytes(1),
        segment_allocation: SegmentAllocation::Preallocate,
        tail_cache_size,
        ..LogConfig::default()
    }
}

/// A log under `disk` that writes directly, as the broker opens a partition
/// and then applies the topic's `preallocate`.
fn direct_log(disk: &Arc<AlignedDisk>, tail_cache_size: ByteSize) -> (tempfile::TempDir, Log) {
    let (dir, mut log) = configured_test_log(LogConfig::default());
    log.test_set_io(disk.clone());
    log.set_config(direct(tail_cache_size));
    (dir, log)
}

/// `(base_offset, last_offset_delta)` of every batch a full read returns.
fn batches(log: &Log) -> Vec<(i64, i32)> {
    log.read(Offset(0), NO_LIMIT)
        .unwrap()
        .batches
        .iter()
        .map(|batch| (batch.base_offset, batch.last_offset_delta))
        .collect()
}

fn log_len(dir: &Path, base: i64) -> u64 {
    std::fs::metadata(name::log_path(dir, base)).unwrap().len()
}

/// Batches of any size go out as whole blocks, the last one zero-padded and
/// written again by the next append, and read back as written. The padding
/// lasts only while the segment is active: the seal leaves the file exactly
/// as long as its batches.
#[test]
fn direct_writes_cover_whole_blocks_and_read_back_as_written() {
    let disk = Arc::new(AlignedDisk::default());
    let (dir, mut log) = direct_log(&disk, mebibytes(1));
    for records in [1, 7, 3, 40] {
        append_samples(&mut log, 1, records);
    }

    assert2::assert!(batches(&log) == [(0, 0), (1, 6), (8, 2), (11, 39)]);
    let written = log.size().bytes_u64();
    let padded = log_len(dir.path(), 0);
    assert2::assert!(padded % BLOCK as u64 == 0);
    assert2::assert!((written..written + BLOCK as u64).contains(&padded));
    // Each append rewrites the block its predecessor ended in.
    let writes = disk.writes.lock().unwrap().clone();
    assert2::assert!(writes.len() == 4);
    assert2::assert!(writes[0].0 == 0);

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(batches(&log) == [(0, 0), (1, 6), (8, 2), (11, 39)]);
}

/// Three batches under a cache that keeps only the newest, which starts at
/// offset 6. Returns the directory, the log, and the byte range of that
/// batch.
fn newest_cached(disk: &Arc<AlignedDisk>) -> (tempfile::TempDir, Log, u64, u64) {
    let (dir, mut log) = direct_log(disk, bytes(1));
    append_samples(&mut log, 2, 3);
    let newest_at = log.size().bytes_u64();
    append_samples(&mut log, 1, 3);
    let newest_len = log.size().bytes_u64() - newest_at;
    (dir, log, newest_at, newest_len)
}

/// The page cache does not hold what an `O_DIRECT` write wrote, so the
/// newest bytes come from memory. The disk under the newest batch is
/// scribbled over here, and a read still returns the batch: it never looked.
#[test]
fn the_newest_bytes_are_read_from_memory() {
    let disk = Arc::new(AlignedDisk::default());
    let (dir, log, newest_at, newest_len) = newest_cached(&disk);

    OpenOptions::new()
        .write(true)
        .open(name::log_path(dir.path(), 0))
        .unwrap()
        .write_all_at(&vec![0; usize::try_from(newest_len).unwrap()], newest_at)
        .unwrap();

    let newest = log.read(Offset(6), NO_LIMIT).unwrap();
    assert2::assert!(newest.batches.len() == 1);
    assert2::assert!(newest.batches[0].base_offset == 6);
    assert2::assert!(newest.batches[0].records.len() == 3);
}

crate::sendfile_cfg! {
    /// `sendfile` reads through the page cache, which does not hold the
    /// newest bytes, so they are not offered to it and the fetch reads them
    /// from memory instead. Older ones, outside the cache, still are.
    #[test]
    fn the_newest_bytes_are_not_offered_to_sendfile() {
        let disk = Arc::new(AlignedDisk::default());
        let (_dir, log, _, _) = newest_cached(&disk);

        let cached = log.read_raw_desc(Offset(6), Offset(9), NO_LIMIT).unwrap();
        assert2::assert!(cached.regions.is_empty());
        let older = log.read_raw_desc(Offset(0), Offset(3), NO_LIMIT).unwrap();
        assert2::assert!(older.regions.len() == 1);
    }
}

/// A truncate puts the writer back on the block the new end falls in and
/// forgets the cached bytes past it, so the appends after it land where a
/// buffered log's would and read back from memory and from disk alike.
#[test]
fn a_truncate_rewinds_the_direct_writer_and_its_cache() {
    let disk = Arc::new(AlignedDisk::default());
    let (dir, mut log) = direct_log(&disk, mebibytes(1));
    append_samples(&mut log, 3, 3);

    log.truncate_to(Offset(3)).unwrap();
    append_samples(&mut log, 2, 5);

    assert2::assert!(batches(&log) == [(0, 2), (3, 4), (8, 4)]);
    let written = log.size().bytes_u64();
    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(batches(&log) == [(0, 2), (3, 4), (8, 4)]);
}

/// Turning `preallocate` off puts the active segment back on the page cache
/// before its next append, which cuts the padding: from then on its file is
/// exactly as long as its batches again.
#[test]
fn turning_preallocate_off_goes_back_to_the_page_cache() {
    let disk = Arc::new(AlignedDisk::default());
    let (dir, mut log) = direct_log(&disk, mebibytes(1));
    append_samples(&mut log, 1, 3);
    let direct_writes = disk.writes.lock().unwrap().len();

    log.set_config(LogConfig::default());
    append_samples(&mut log, 1, 3);

    assert2::assert!(disk.writes.lock().unwrap().len() == direct_writes);
    assert2::assert!(log_len(dir.path(), 0) == log.size().bytes_u64());
    assert2::assert!(batches(&log) == [(0, 2), (3, 2)]);
}

/// A direct write that fails leaves the log as it was: the append is
/// refused, and the next one lands where the failed one would have.
#[test]
fn a_failed_direct_write_leaves_the_log_as_it_was() {
    let disk = Arc::new(AlignedDisk::default());
    let (_dir, mut log) = direct_log(&disk, mebibytes(1));
    append_samples(&mut log, 1, 3);

    disk.fail_next.store(true, Ordering::SeqCst);
    let mut refused = super::test_support::sample_batch(3);
    assert2::assert!(log.append(&mut refused).is_err());
    append_samples(&mut log, 1, 3);

    assert2::assert!(batches(&log) == [(0, 2), (3, 2)]);
}

/// A crash under `O_DIRECT` leaves the active segment ending in up to a
/// block of zeros, as Kafka's own `preallocate` leaves its tail. Recovery
/// cuts them like any bytes that do not decode as a batch, so the log
/// reopens at its last batch and the next append lands right after it. This
/// is the reader every 1.x release has, which is what lets the shape change.
#[test]
fn recovery_cuts_the_padding_a_crash_leaves() {
    let (dir, mut log) = configured_test_log(LogConfig::default());
    append_samples(&mut log, 2, 3);
    let written = log.size().bytes_u64();
    drop(log);
    OpenOptions::new()
        .append(true)
        .open(name::log_path(dir.path(), 0))
        .unwrap()
        .write_all(&[0; 4095])
        .unwrap();

    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();

    assert_log_end_offset(&log, Offset(6));
    assert2::assert!(log_len(dir.path(), 0) == written);
    append_samples(&mut log, 1, 3);
    assert2::assert!(batches(&log) == [(0, 2), (3, 2), (6, 2)]);
}

/// The real thing where the kernel offers it, and the page cache where it
/// does not: either way a `preallocate` log reads back what it was given,
/// before and after a roll and across a restart.
#[test]
fn a_real_direct_log_round_trips_across_a_roll_and_a_restart() {
    let (dir, mut log) = configured_test_log(direct(mebibytes(1)));
    for records in [1, 300, 2, 17, 900, 5] {
        append_samples(&mut log, 1, records);
    }
    let expected = [(0, 0), (1, 299), (301, 1), (303, 16), (320, 899), (1220, 4)];
    assert2::assert!(batches(&log) == expected);

    assert2::assert!(log.roll().unwrap());
    append_samples(&mut log, 1, 3);
    drop(log);

    let log = Log::open(dir.path(), direct(mebibytes(1))).unwrap();
    let mut after = expected.to_vec();
    after.push((1225, 2));
    assert2::assert!(batches(&log) == after);
}

fn producer_batches() -> Vec<VerbatimBatch> {
    let mut sequence = 0;
    (0..64)
        .map(|index| {
            let records = [1, 7, 3, 40][index % 4];
            let mut batch = sample_batch(records);
            batch.producer_id = 42;
            batch.producer_epoch = 0;
            batch.base_sequence = sequence;
            batch.max_timestamp = i64::from(sequence);
            sequence += records;
            verbatim_from(&batch, LeaderEpoch(1)).1
        })
        .collect()
}

#[test]
fn a_direct_group_matches_individual_appends_with_one_write() {
    for cache in [bytes(1), mebibytes(1)] {
        let grouped_disk = Arc::new(AlignedDisk::default());
        let individual_disk = Arc::new(AlignedDisk::default());
        let (dir, mut grouped) = direct_log(&grouped_disk, cache);
        let (individual_dir, mut individual) = direct_log(&individual_disk, cache);
        append_samples(&mut grouped, 1, 7);
        append_samples(&mut individual, 1, 7);
        let batches = producer_batches();
        let assigned = grouped
            .try_append_verbatim_group(&batches.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let expected: Vec<_> = batches
            .iter()
            .map(|batch| individual.append_verbatim(batch).unwrap().0)
            .collect();
        assert2::assert!(assigned == expected);
        assert2::assert!(grouped_disk.writes.lock().unwrap().len() == 2);
        assert2::assert!(individual_disk.writes.lock().unwrap().len() == 65);
        assert2::assert!(
            grouped.read(Offset(0), NO_LIMIT).unwrap().batches
                == individual.read(Offset(0), NO_LIMIT).unwrap().batches
        );
        assert2::assert!(grouped.producer_state_snapshot() == individual.producer_state_snapshot());
        assert2::assert!(
            grouped.epoch_checkpoint().entries() == individual.epoch_checkpoint().entries()
        );
        assert2::assert!(
            std::fs::read(name::log_path(dir.path(), 0)).unwrap()
                == std::fs::read(name::log_path(individual_dir.path(), 0)).unwrap()
        );
        grouped.sync().unwrap();
        grouped.roll().unwrap();
        drop(grouped);
        let reopened = Log::open(dir.path(), direct(cache)).unwrap();
        assert2::assert!(
            reopened.read(Offset(0), NO_LIMIT).unwrap().batches
                == individual.read(Offset(0), NO_LIMIT).unwrap().batches
        );
        assert2::assert!(
            reopened.producer_state_snapshot() == individual.producer_state_snapshot()
        );
    }
}

#[test]
fn failed_direct_groups_restore_bytes_indexes_and_producer_state() {
    for target in [
        None,
        Some(IoTarget::OffsetIndex),
        Some(IoTarget::TimeIndex),
        Some(IoTarget::LeaderEpochCheckpoint),
    ] {
        let disk = Arc::new(AlignedDisk::default());
        let (dir, mut log) = direct_log(&disk, mebibytes(1));
        let mut config = direct(mebibytes(1));
        config.index_interval = bytes(0);
        log.set_config(config.clone());
        let batches = producer_batches();
        log.append_verbatim(&batches[0]).unwrap();
        let before = log.read(Offset(0), NO_LIMIT).unwrap().batches;
        let producers = log.producer_state_snapshot();
        let epochs = log.epoch_checkpoint().entries().to_vec();
        let mut group = batches[1..4].to_vec();
        // A new epoch forces checkpoint I/O after producer state has advanced.
        group[0].leader_epoch = LeaderEpoch(2);
        *disk.fail_target.lock().unwrap() = target;
        disk.partial_next.store(target.is_none(), Ordering::SeqCst);
        let error = log
            .try_append_verbatim_group(&group.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap_err();
        assert2::assert!(
            matches!(error, crate::LogError::Io(ref error) if error.kind() == std::io::ErrorKind::StorageFull)
        );
        assert2::assert!(log.read(Offset(0), NO_LIMIT).unwrap().batches == before);
        assert2::assert!(log.producer_state_snapshot() == producers);
        assert2::assert!(log.epoch_checkpoint().entries() == epochs);
        assert2::assert!(log_len(dir.path(), 0) == log.size().bytes_u64());
        log.try_append_verbatim_group(&group.iter().collect::<Vec<_>>())
            .unwrap()
            .unwrap();
        let expected = log.read(Offset(0), NO_LIMIT).unwrap().batches;
        log.sync().unwrap();
        drop(log);
        let reopened = Log::open(dir.path(), config).unwrap();
        assert2::assert!(reopened.read(Offset(0), NO_LIMIT).unwrap().batches == expected);
    }
}

#[test]
fn direct_groups_leave_special_policies_and_rolls_to_individual_appends() {
    let configs = [
        LogConfig::default(),
        LogConfig {
            segment_size: bytes(1),
            ..direct(mebibytes(1))
        },
        LogConfig {
            segment_index_size: bytes(12),
            ..direct(mebibytes(1))
        },
        LogConfig {
            segment_roll_interval: krabka_units::prelude::millis(1),
            ..direct(mebibytes(1))
        },
        LogConfig {
            delivery_policy: DeliveryPolicy::Scheduled,
            ..direct(mebibytes(1))
        },
        LogConfig {
            message_timestamp_type: krabka_protocol::records::TimestampType::LogAppendTime,
            ..direct(mebibytes(1))
        },
        LogConfig {
            flush_on_append: true,
            ..direct(mebibytes(1))
        },
        LogConfig {
            flush_messages: Some(100),
            ..direct(mebibytes(1))
        },
        LogConfig {
            flush_interval: Some(krabka_units::prelude::millis(100)),
            ..direct(mebibytes(1))
        },
    ];
    for config in configs {
        let disk = Arc::new(AlignedDisk::default());
        let (_dir, mut log) = direct_log(&disk, mebibytes(1));
        append_samples(&mut log, 1, 7);
        let before = log.size();
        let writes = disk.writes.lock().unwrap().len();
        log.set_config(config);
        let batches = producer_batches();
        assert2::assert!(
            log.try_append_verbatim_group(&batches.iter().collect::<Vec<_>>())
                .is_none()
        );
        assert2::assert!(log.size() == before);
        assert2::assert!(disk.writes.lock().unwrap().len() == writes);
        for batch in &batches {
            log.append_verbatim(batch).unwrap();
        }
        assert2::assert!(log.read(Offset(0), NO_LIMIT).unwrap().batches.len() == 65);
    }
}

#[test]
fn direct_groups_refuse_invalid_transactional_and_oversized_batches_without_writing() {
    let disk = Arc::new(AlignedDisk::default());
    let (_dir, mut log) = direct_log(&disk, mebibytes(1));
    let valid = producer_batches();
    assert2::assert!(
        log.try_append_verbatim_group(&[&valid[0], &valid[1]])
            .is_none()
    );
    append_samples(&mut log, 1, 7);
    let mut short = valid[0].clone();
    short.bytes = bytes::Bytes::from_static(b"short");
    let mut invalid = valid[0].clone();
    invalid.last_offset_delta = -1;
    let mut transactional = sample_batch(3);
    transactional.attributes = transactional.attributes.with_transactional(true);
    let mut transactional = verbatim_from(&transactional, LeaderEpoch(1)).1;
    // The wire attribute also excludes a transaction if the supplied flag disagrees.
    transactional.is_transactional = false;
    let mut large = valid[0].clone();
    large.bytes = bytes::Bytes::from(vec![0; mebibytes(1).bytes_usize()]);
    for batches in [
        vec![],
        vec![&valid[0]],
        vec![&valid[0], &short],
        vec![&valid[0], &invalid],
        vec![&valid[0], &transactional],
        vec![&valid[0], &large],
    ] {
        assert2::assert!(log.try_append_verbatim_group(&batches).is_none());
        assert2::assert!(log.log_end_offset() == Offset(7));
        assert2::assert!(disk.writes.lock().unwrap().len() == 1);
    }
}
