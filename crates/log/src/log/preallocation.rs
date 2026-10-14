//! What `preallocate` asks of the disk, and what it leaves on it.
//!
//! [`SegmentAllocation::Preallocate`] reserves a segment's blocks before its
//! first append and gives back the rest when it is sealed. A
//! reservation is invisible to a reader, so the cases here watch it two ways:
//! through a [`LogIo`] that records each request, which says exactly what the
//! log asked for, and on Linux through the blocks a real file holds, which
//! says the kernel did it.

#[cfg(target_os = "linux")]
use super::{
    Log,
    test_support::{assert_log_end_offset, sample_batch},
};
use {
    super::test_support::{NO_LIMIT, TempLog, append_samples, configured_test_log},
    crate::{
        config::{LogConfig, SegmentAllocation},
        io::{DirectFile, LogIo},
        name,
        test_support::{BatchSpan, span},
    },
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, mebibytes},
    std::{
        fs::File,
        path::Path,
        sync::{Arc, Mutex},
    },
};

/// One reservation or release a [`Recorder`] saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Request {
    Reserve { offset: u64, len: u64 },
    Release { len: u64 },
}

/// A disk that records every reservation and release, and refuses every
/// reservation when `refuse` is set, as a filesystem without `fallocate`
/// does.
#[derive(Debug, Default)]
struct Recorder {
    refuse: bool,
    requests: Mutex<Vec<Request>>,
}

impl LogIo for Recorder {
    fn reserve(&self, _file: &File, offset: u64, len: u64) -> std::io::Result<()> {
        self.requests
            .lock()
            .unwrap()
            .push(Request::Reserve { offset, len });
        if self.refuse {
            return Err(std::io::ErrorKind::Unsupported.into());
        }
        Ok(())
    }

    fn release(&self, _file: &File, len: u64) -> std::io::Result<()> {
        self.requests.lock().unwrap().push(Request::Release { len });
        Ok(())
    }

    /// No `O_DIRECT` here: these cases are about the reservation alone, and
    /// `direct_writes` covers the writes.
    fn open_direct(&self, _path: &Path) -> std::io::Result<DirectFile> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}

const SEGMENT: ByteSize = mebibytes(1);

fn preallocating() -> LogConfig {
    LogConfig {
        segment_size: SEGMENT,
        segment_allocation: SegmentAllocation::Preallocate,
        ..LogConfig::default()
    }
}

fn log_len(dir: &Path, base: i64) -> u64 {
    std::fs::metadata(name::log_path(dir, base)).unwrap().len()
}

/// A log opened without `preallocate` that turns it on afterwards, under
/// `io`, as the broker opens a partition with its own config and only then
/// applies the topic's.
fn switched_on(allocation: SegmentAllocation, io: &Arc<Recorder>) -> TempLog {
    let TempLog { dir, mut log } = configured_test_log(LogConfig::default());
    log.test_set_io(io.clone());
    log.set_config(LogConfig {
        segment_allocation: allocation,
        ..preallocating()
    });
    TempLog { dir, log }
}

/// What [`roll_twice`] observed: the requests the disk saw and the lengths of
/// the two segments.
struct RollTwice {
    requests: Vec<Request>,
    lengths: [u64; 2],
}

/// Append two batches and roll, twice: into the segment at 0, which was
/// created before the config changed, and into the one at 6.
///
/// Returns the requests `io` saw and the lengths of the two segments.
fn roll_twice(allocation: SegmentAllocation, io: &Arc<Recorder>) -> RollTwice {
    let TempLog { dir, mut log } = switched_on(allocation, io);
    for _ in 0..2 {
        append_samples(&mut log, 2, 3);
        assert2::assert!(log.roll().unwrap());
    }
    // Whatever the disk said, the log holds what was written.
    let read = log.read(Offset(0), NO_LIMIT).unwrap();
    let batches: Vec<BatchSpan> = read
        .batches
        .iter()
        .map(|batch| span(batch.base_offset, batch.last_offset_delta))
        .collect();
    assert2::assert!(batches == [span(0, 2), span(3, 2), span(6, 2), span(9, 2)]);
    let requests = io.requests.lock().unwrap().clone();
    RollTwice {
        requests,
        lengths: [log_len(dir.path(), 0), log_len(dir.path(), 6)],
    }
}

/// A reservation of a whole segment from its start.
fn whole() -> Request {
    Request::Reserve {
        offset: 0,
        len: SEGMENT.bytes_u64(),
    }
}

/// A segment reserves its whole `segment.bytes` before its first append,
/// once, and gives back everything past the bytes it holds when it is
/// sealed, by truncating to the length it already has. The config change
/// reaches the segment that was already active, not only the next one.
#[test]
fn every_segment_reserves_before_its_first_append_and_releases_at_the_seal() {
    let io = Arc::new(Recorder::default());
    let RollTwice {
        requests,
        lengths: [first, second],
    } = roll_twice(SegmentAllocation::Preallocate, &io);

    assert2::assert!(
        requests
            == [
                whole(),
                Request::Release { len: first },
                whole(),
                Request::Release { len: second },
            ]
    );
}

/// A filesystem that cannot reserve leaves a segment that allocates as it
/// grows: the appends and rolls go on, each segment asks once and not on
/// every append, and there is nothing to give back.
#[test]
fn a_refused_reservation_is_asked_once_and_leaves_a_working_log() {
    let io = Arc::new(Recorder {
        refuse: true,
        ..Recorder::default()
    });
    let RollTwice { requests, .. } = roll_twice(SegmentAllocation::Preallocate, &io);

    assert2::assert!(requests == [whole(), whole()]);
}

/// Kafka's default asks the disk for nothing.
#[test]
fn allocating_on_write_never_reserves_or_releases() {
    let io = Arc::new(Recorder::default());
    let RollTwice { requests, .. } = roll_twice(SegmentAllocation::OnWrite, &io);

    assert2::assert!(requests.is_empty());
}

/// A truncate frees the blocks past the new end, the reservation's with
/// them, as a follower's truncation to the leader does. The segment takes
/// the reservation again from where it now ends, and the appends after it
/// ask for nothing more.
#[test]
fn a_truncated_segment_takes_its_reservation_again() {
    let io = Arc::new(Recorder::default());
    let TempLog { dir, mut log } = switched_on(SegmentAllocation::Preallocate, &io);
    append_samples(&mut log, 2, 3);

    log.truncate_to(Offset(3)).unwrap();
    let kept = log_len(dir.path(), 0);
    append_samples(&mut log, 1, 3);

    let requests = io.requests.lock().unwrap().clone();
    assert2::assert!(
        requests
            == [whole(), Request::Reserve {
                offset: kept,
                len: SEGMENT.bytes_u64() - kept,
            },]
    );
}

/// The kernel's side, on a real file: what a segment occupies on disk.
#[cfg(target_os = "linux")]
fn allocated(dir: &Path, base: i64) -> u64 {
    let metadata = std::fs::metadata(name::log_path(dir, base)).unwrap();
    std::os::unix::fs::MetadataExt::blocks(&metadata) * 512
}

/// On a real file the reservation takes the blocks without lengthening the
/// file, and the seal hands them back and leaves the file exactly as long as
/// its batches. While the segment is active its file may also carry the
/// block of padding an `O_DIRECT` write leaves, which the seal cuts. A
/// filesystem that cannot reserve fails the first assertion; every one the
/// broker runs on can.
#[cfg(target_os = "linux")]
#[test]
fn a_real_reservation_holds_blocks_past_the_end_until_the_seal() {
    let TempLog { dir, mut log } = configured_test_log(preallocating());

    log.append(&mut sample_batch(3)).unwrap();
    let written = log.size().bytes_u64();
    assert2::assert!(log_len(dir.path(), 0) < SEGMENT.bytes_u64());
    assert2::assert!(allocated(dir.path(), 0) >= SEGMENT.bytes_u64());

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(allocated(dir.path(), 0) < SEGMENT.bytes_u64() / 2);

    log.append(&mut sample_batch(3)).unwrap();
    assert2::assert!(allocated(dir.path(), 3) >= SEGMENT.bytes_u64());
}

/// A reservation outlives a process that crashed holding it: nothing on
/// disk records it but the blocks. A clean close cuts the active segment's
/// `O_DIRECT` padding, which frees them, so the leftover is made here the way
/// a crash leaves it. A log reopened without `preallocate` still finds the
/// blocks, and the seal gives them back rather than leaving them for as long
/// as retention keeps the segment.
#[cfg(target_os = "linux")]
#[test]
fn a_reopened_segment_gives_back_a_crashed_runs_reservation() {
    let TempLog { dir, mut log } = configured_test_log(LogConfig::default());
    append_samples(&mut log, 1, 3);
    drop(log);
    let file = File::options()
        .write(true)
        .open(name::log_path(dir.path(), 0))
        .unwrap();
    rustix::fs::fallocate(
        &file,
        rustix::fs::FallocateFlags::KEEP_SIZE,
        0,
        SEGMENT.bytes_u64(),
    )
    .unwrap();

    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert_log_end_offset(&log, Offset(3));
    assert2::assert!(allocated(dir.path(), 0) >= SEGMENT.bytes_u64());

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(allocated(dir.path(), 0) < SEGMENT.bytes_u64() / 2);
}
