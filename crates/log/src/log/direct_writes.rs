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
            NO_LIMIT, TempLog, append_samples, assert_log_end_offset, configured_test_log,
            sample_batch, verbatim_from,
        },
    },
    crate::{
        DeliveryPolicy, LeaderEpoch, VerbatimBatch,
        config::{LogConfig, SegmentAllocation},
        io::{DirectFile, IoTarget, LogIo},
        name,
        test_support::{BatchSpan, span},
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
    fail_target: Mutex<Option<IoTarget>>,
    /// Every direct write.
    writes: Mutex<Vec<DirectWrite>>,
}

/// One direct write: where in the file, and how many bytes.
#[derive(Debug, Clone, Copy)]
struct DirectWrite {
    offset: u64,
    len: usize,
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
        self.writes.lock().unwrap().push(DirectWrite {
            offset,
            len: buf.len(),
        });
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
fn direct_log(disk: &Arc<AlignedDisk>, tail_cache_size: ByteSize) -> TempLog {
    let TempLog { dir, mut log } = configured_test_log(LogConfig::default());
    log.test_set_io(disk.clone());
    log.set_config(direct(tail_cache_size));
    TempLog { dir, log }
}

/// The span of every batch a full read returns.
fn batches(log: &Log) -> Vec<BatchSpan> {
    log.read(Offset(0), NO_LIMIT)
        .unwrap()
        .batches
        .iter()
        .map(|batch| span(batch.base_offset, batch.last_offset_delta))
        .collect()
}

fn log_len(dir: &Path, base: i64) -> u64 {
    std::fs::metadata(name::log_path(dir, base)).unwrap().len()
}

/// Batches of any size go out as whole blocks, the last one zero-padded and
/// written again once the next append completes it, and read back as
/// written. The padding
/// lasts only while the segment is active: the seal leaves the file exactly
/// as long as its batches.
#[test]
fn direct_writes_cover_whole_blocks_and_read_back_as_written() {
    let disk = Arc::new(AlignedDisk::default());
    let TempLog { dir, mut log } = direct_log(&disk, mebibytes(1));
    for records in [1, 7, 3, 40] {
        append_samples(&mut log, 1, records);
    }
    log.sync().unwrap();

    assert2::assert!(batches(&log) == [span(0, 0), span(1, 6), span(8, 2), span(11, 39)]);
    let written = log.size().bytes_u64();
    let padded = log_len(dir.path(), 0);
    assert2::assert!(padded % BLOCK as u64 == 0);
    assert2::assert!((written..written + BLOCK as u64).contains(&padded));
    let writes = disk.writes.lock().unwrap().clone();
    assert2::assert!(writes[0].offset == 0);
    assert2::assert!(writes.iter().all(|write| write.len % BLOCK == 0));

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(batches(&log) == [span(0, 0), span(1, 6), span(8, 2), span(11, 39)]);
}

/// The log of [`newest_cached`], the directory that holds it, and the byte
/// range of its newest batch.
struct NewestCached {
    dir: tempfile::TempDir,
    log: Log,
    newest_at: u64,
    newest_len: u64,
}

/// Three batches under a cache that keeps only the newest, which starts at
/// offset 6. Returns the directory, the log, and the byte range of that
/// batch.
fn newest_cached(disk: &Arc<AlignedDisk>) -> NewestCached {
    let TempLog { dir, mut log } = direct_log(disk, bytes(1));
    append_samples(&mut log, 2, 3);
    let newest_at = log.size().bytes_u64();
    append_samples(&mut log, 1, 3);
    let newest_len = log.size().bytes_u64() - newest_at;
    NewestCached {
        dir,
        log,
        newest_at,
        newest_len,
    }
}

/// The page cache does not hold what an `O_DIRECT` write wrote, so the
/// newest bytes come from memory. The disk under the newest batch is
/// scribbled over here, and a read still returns the batch: it never looked.
#[test]
fn the_newest_bytes_are_read_from_memory() {
    let disk = Arc::new(AlignedDisk::default());
    let NewestCached {
        dir,
        log,
        newest_at,
        newest_len,
    } = newest_cached(&disk);

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

    // The raw path a fetch takes serves them from memory too.
    let raw = log
        .read_raw(Offset(6), log.log_end_offset(), NO_LIMIT)
        .unwrap();
    assert2::assert!(raw.start_offset == 6);
    assert2::assert!(raw.last_offset == Some(Offset(8)));
    assert2::assert!(raw.bytes.len() == usize::try_from(newest_len).unwrap());
    assert2::assert!(raw.bytes.iter().any(|&byte| byte != 0));
}

#[krabka_macros::sendfile_cfg]
/// None of a segment that wrote through `O_DIRECT` is in the page cache, so
/// none of it is offered to `sendfile`, which would read it from the disk one
/// header at a time and again for the run. The fetch takes the byte-copy path
/// instead: the newest bytes from memory, older ones in one large read. That
/// holds after the segment is sealed too.
#[test]
fn a_direct_segment_is_never_offered_to_sendfile() {
    let disk = Arc::new(AlignedDisk::default());
    let NewestCached {
        dir: _dir, mut log, ..
    } = newest_cached(&disk);

    let cached = log.read_raw_desc(Offset(6), Offset(9), NO_LIMIT).unwrap();
    assert2::assert!(cached.regions.is_empty());
    let older = log.read_raw_desc(Offset(0), Offset(3), NO_LIMIT).unwrap();
    assert2::assert!(older.regions.is_empty());
    let raw = log.read_raw(Offset(0), Offset(3), NO_LIMIT).unwrap();
    assert2::assert!(raw.start_offset == 0);
    assert2::assert!(raw.last_offset == Some(Offset(2)));

    log.sync().unwrap();
    assert2::assert!(log.roll().unwrap());
    let sealed = log.read_raw_desc(Offset(0), Offset(9), NO_LIMIT).unwrap();
    assert2::assert!(sealed.regions.is_empty());
}

#[krabka_macros::sendfile_cfg]
/// The descriptor walk stops at a segment that wrote through `O_DIRECT`, so
/// a fetch that starts there falls back to the byte-copy path rather than
/// sending a later, buffered segment's runs and skipping the direct one's
/// offsets.
#[test]
fn a_descriptor_walk_never_passes_over_a_direct_segment() {
    let disk = Arc::new(AlignedDisk::default());
    let TempLog { dir: _dir, mut log } = one_synced_batch(&disk);
    assert2::assert!(log.roll().unwrap());
    log.set_config(LogConfig::default());
    append_samples(&mut log, 1, 3);
    let end = log.log_end_offset();

    let from_direct = log.read_raw_desc(Offset(0), end, NO_LIMIT).unwrap();
    assert2::assert!(from_direct.regions.is_empty());
    let from_buffered = log.read_raw_desc(Offset(3), end, NO_LIMIT).unwrap();
    assert2::assert!(from_buffered.start_offset == 3);
    assert2::assert!(from_buffered.regions.len() == 1);
}

/// A truncate puts the writer back on the block the new end falls in and
/// forgets the cached bytes past it, so the appends after it land where a
/// buffered log's would and read back from memory and from disk alike.
#[test]
fn a_truncate_rewinds_the_direct_writer_and_its_cache() {
    let disk = Arc::new(AlignedDisk::default());
    let TempLog { dir, mut log } = direct_log(&disk, mebibytes(1));
    append_samples(&mut log, 3, 3);

    log.truncate_to(Offset(3)).unwrap();
    append_samples(&mut log, 2, 5);

    assert2::assert!(batches(&log) == [span(0, 2), span(3, 4), span(8, 4)]);
    let written = log.size().bytes_u64();
    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(batches(&log) == [span(0, 2), span(3, 4), span(8, 4)]);
}

/// A direct log holding one batch of three records, synced to the disk.
fn one_synced_batch(disk: &Arc<AlignedDisk>) -> TempLog {
    let TempLog { dir, mut log } = direct_log(disk, mebibytes(1));
    append_samples(&mut log, 1, 3);
    log.sync().unwrap();
    TempLog { dir, log }
}

/// Turning `preallocate` off puts the active segment back on the page cache
/// before its next append, which cuts the padding: from then on its file is
/// exactly as long as its batches again.
#[test]
fn turning_preallocate_off_goes_back_to_the_page_cache() {
    let disk = Arc::new(AlignedDisk::default());
    let TempLog { dir, mut log } = one_synced_batch(&disk);
    let direct_writes = disk.writes.lock().unwrap().len();

    log.set_config(LogConfig::default());
    append_samples(&mut log, 1, 3);

    assert2::assert!(disk.writes.lock().unwrap().len() == direct_writes);
    assert2::assert!(log_len(dir.path(), 0) == log.size().bytes_u64());
    assert2::assert!(batches(&log) == [span(0, 2), span(3, 2)]);
}

/// An append does not wait for its direct write, so a write that fails
/// fails the log from then on, as a page-cache writeback error would: the
/// batch was already acknowledged. Reads still find it in memory, the next
/// sync and append report the error, and a reopen finds only the batches
/// that reached the disk.
#[test]
fn a_failed_direct_write_fails_the_log_from_then_on() {
    let disk = Arc::new(AlignedDisk::default());
    let TempLog { dir, mut log } = one_synced_batch(&disk);

    disk.fail_next.store(true, Ordering::SeqCst);
    append_samples(&mut log, 1, 3);
    assert2::assert!(batches(&log) == [span(0, 2), span(3, 2)]);
    assert2::assert!(log.sync().is_err());
    let mut refused = super::test_support::sample_batch(3);
    assert2::assert!(log.append(&mut refused).is_err());

    drop(log);
    let reopened = Log::open(dir.path(), direct(mebibytes(1))).unwrap();
    assert2::assert!(batches(&reopened) == [span(0, 2)]);
}

/// A crash under `O_DIRECT` leaves the active segment ending in up to a
/// block of zeros, as Kafka's own `preallocate` leaves its tail. Recovery
/// cuts them like any bytes that do not decode as a batch, so the log
/// reopens at its last batch and the next append lands right after it. This
/// is the reader every 1.x release has, which is what lets the shape change.
#[test]
fn recovery_cuts_the_padding_a_crash_leaves() {
    let TempLog { dir, mut log } = configured_test_log(LogConfig::default());
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
    assert2::assert!(batches(&log) == [span(0, 2), span(3, 2), span(6, 2)]);
}

/// The real thing where the kernel offers it, and the page cache where it
/// does not: either way a `preallocate` log reads back what it was given,
/// before and after a roll and across a restart.
#[test]
fn a_real_direct_log_round_trips_across_a_roll_and_a_restart() {
    let TempLog { dir, mut log } = configured_test_log(direct(mebibytes(1)));
    for records in [1, 300, 2, 17, 900, 5] {
        append_samples(&mut log, 1, records);
    }
    let expected = [
        span(0, 0),
        span(1, 299),
        span(301, 1),
        span(303, 16),
        span(320, 899),
        span(1220, 4),
    ];
    assert2::assert!(batches(&log) == expected);

    assert2::assert!(log.roll().unwrap());
    append_samples(&mut log, 1, 3);
    drop(log);

    let log = Log::open(dir.path(), direct(mebibytes(1))).unwrap();
    let mut after = expected.to_vec();
    after.push(span(1225, 2));
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
            verbatim_from(&batch, LeaderEpoch(1)).batch
        })
        .collect()
}

#[test]
fn a_direct_group_matches_individual_appends() {
    for cache in [bytes(1), mebibytes(1)] {
        let grouped_disk = Arc::new(AlignedDisk::default());
        let individual_disk = Arc::new(AlignedDisk::default());
        let TempLog {
            dir,
            log: mut grouped,
        } = direct_log(&grouped_disk, cache);
        let TempLog {
            dir: individual_dir,
            log: mut individual,
        } = direct_log(&individual_disk, cache);
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
        grouped.sync().unwrap();
        individual.sync().unwrap();
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
        IoTarget::OffsetIndex,
        IoTarget::TimeIndex,
        IoTarget::LeaderEpochCheckpoint,
    ] {
        let disk = Arc::new(AlignedDisk::default());
        let TempLog { dir, mut log } = direct_log(&disk, mebibytes(1));
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
        *disk.fail_target.lock().unwrap() = Some(target);
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
        let TempLog { dir: _dir, mut log } = direct_log(&disk, mebibytes(1));
        append_samples(&mut log, 1, 7);
        log.sync().unwrap();
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
    let TempLog { dir: _dir, mut log } = direct_log(&disk, mebibytes(1));
    let valid = producer_batches();
    assert2::assert!(
        log.try_append_verbatim_group(&[&valid[0], &valid[1]])
            .is_none()
    );
    append_samples(&mut log, 1, 7);
    log.sync().unwrap();
    let writes = disk.writes.lock().unwrap().len();
    let mut short = valid[0].clone();
    short.bytes = bytes::Bytes::from_static(b"short");
    let mut invalid = valid[0].clone();
    invalid.last_offset_delta = -1;
    let mut transactional = sample_batch(3);
    transactional.attributes = transactional.attributes.with_transactional(true);
    let mut transactional = verbatim_from(&transactional, LeaderEpoch(1)).batch;
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
        log.sync().unwrap();
        assert2::assert!(disk.writes.lock().unwrap().len() == writes);
    }
}
