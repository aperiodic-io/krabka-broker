//! Positioned reads and writes over a segment's `.log` file.
//!
//! A reader shares the writer's `File` handle through `&self`, so every read
//! here takes an explicit file offset and leaves the file cursor where it was.
//! The hot fetch path runs through these functions for every read, which is
//! why they live together in one small module.

use std::{
    fs::File,
    io::{IoSlice, Seek, SeekFrom},
};

use krabka_units::prelude::{ByteSize, ByteSizeExt};

use super::Segment;
use crate::{error::LogError, io::LogIo};

/// Positioned read: fill `buf` from `offset` in `file` without a move of the
/// file's cursor.
///
/// This function loops over short reads until `buf` is full or it reaches EOF,
/// then returns the number of bytes read. Readers can therefore share the
/// writer's `File` handle through `&self`, with no `dup(2)` or `lseek(2)` per
/// call. The hot fetch path runs this function for every read.
pub(super) fn read_full_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    read_full_with(|at, into| read_at(file, at, into), offset, buf)
}

/// The loop itself, over any positional read.
///
/// Split out because the two things it exists to handle -- a read that returns
/// fewer bytes than asked for, and one interrupted by a signal -- are not
/// things a regular file can be persuaded to do on demand, so a test cannot
/// reach them through [`read_full_at`]. Generic rather than `dyn`, so the hot
/// path monomorphises to what it was.
fn read_full_with(
    read: impl Fn(u64, &mut [u8]) -> std::io::Result<usize>,
    mut offset: u64,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match read(offset, &mut buf[total..]) {
            Ok(0) => break, // EOF
            Ok(n) => {
                total += n;
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

pub(super) fn seek_to_log_size(file: &File, log_size: u64) -> std::io::Result<()> {
    (&*file).seek(SeekFrom::Start(log_size))?;
    Ok(())
}

pub(super) fn write_all(io: &dyn LogIo, file: &File, buf: &[u8]) -> std::io::Result<()> {
    crate::io::write_all_with(buf, |remaining| io.write(file, remaining))
}

pub(super) fn write_all_vectored(
    io: &dyn LogIo,
    file: &File,
    mut bufs: &mut [IoSlice<'_>],
) -> std::io::Result<()> {
    while !bufs.is_empty() {
        let written = crate::io::write_progress(|| io.write_vectored(file, bufs))?;
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

// `std::os::wasi::fs::FileExt` is unstable (`wasi_ext`), so the positional
// read goes through the safe `pread` wrapper of rustix on this target.
#[cfg(target_os = "wasi")]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    rustix::io::pread(file, buf, offset).map_err(Into::into)
}

/// The byte range of `.log` a read from `start_pos` with a `window`-byte
/// budget asks the kernel to read ahead, as `(offset, len)`.
///
/// A consumer that is behind fetches the window after this one next, so the
/// range is the window itself, then up to `read_ahead_max` more of what the
/// next sequential read takes, clamped at `log_size`. The cap bounds the page
/// cache one read can claim when a fetch asks for a very large budget, and `0`
/// leaves only the window. `None` when that leaves nothing.
pub(super) fn read_ahead_span(
    start_pos: u64,
    window: u64,
    log_size: u64,
    read_ahead_max: u64,
) -> Option<(u64, u64)> {
    let end = start_pos
        .saturating_add(window)
        .saturating_add(window.min(read_ahead_max))
        .min(log_size);
    (end > start_pos).then(|| (start_pos, end - start_pos))
}

impl Segment {
    /// Ask the kernel for the window a read from `start_pos` is about to
    /// take, and for up to `read_ahead_max` of the window after it.
    ///
    /// The verbatim read finds its batch boundaries with one small `pread`
    /// per batch header, each up to a window ahead of the last, and only then
    /// sends the run. On a cold segment each of those `pread`s is a disk read
    /// of its own, made one after another, and the scattered pages they leave
    /// behind keep the kernel's sequential readahead from growing. One
    /// `WILLNEED` over the whole window turns them into a few large reads in
    /// flight at once, and the part past the window makes the next fetch of a
    /// consumer that is behind a page-cache hit. Kafka's fetch needs no hint:
    /// it reads no batch header past the one it starts at, and sends a slice
    /// that may end in a partial batch.
    pub(super) fn advise_read(&self, start_pos: u64, window: u64, read_ahead_max: ByteSize) {
        if let Some((offset, len)) =
            read_ahead_span(start_pos, window, self.log_size, read_ahead_max.bytes_u64())
        {
            self.io.advise_will_need(&self.log_file, offset, len);
        }
    }

    pub(super) fn read_log_range(
        &self,
        start_pos: u64,
        buf: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<(), LogError> {
        let available = self.log_size.saturating_sub(start_pos);
        let to_read = available.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let to_read = usize::try_from(to_read).unwrap_or(usize::MAX);
        let base = buf.len();
        buf.resize(base + to_read, 0);
        let n = self.read_at(start_pos, &mut buf[base..])?;
        buf.truncate(base + n);
        Ok(())
    }

    /// Fill `out` from `start_pos`, taking what the segment holds in memory
    /// from memory and the rest from the file.
    ///
    /// An `O_DIRECT` write drops the page cache's copy of what it wrote, so
    /// the newest bytes of a segment that writes directly are read from its
    /// tail cache: a consumer reading right behind the producer would
    /// otherwise wait on the disk for every fetch. The cache runs to
    /// `log_size`, so a range that starts inside it ends inside it too.
    fn read_at(&self, start_pos: u64, out: &mut [u8]) -> std::io::Result<usize> {
        let Some(mode) = self.direct.as_ref() else {
            return read_full_at(&self.log_file, start_pos, out);
        };
        let cached_from = mode.cache.start();
        let from_file = usize::try_from(cached_from.saturating_sub(start_pos))
            .unwrap_or(usize::MAX)
            .min(out.len());
        let (file_part, cache_part) = out.split_at_mut(from_file);
        let read = read_full_at(&self.log_file, start_pos, file_part)?;
        if read < file_part.len() {
            return Ok(read);
        }
        let cache_pos = start_pos.max(cached_from);
        if mode.cache.read(cache_pos, cache_part) {
            return Ok(out.len());
        }
        // Not where the cache says it is: the file has every byte below
        // `log_size`, so read it there.
        Ok(from_file + read_full_at(&self.log_file, cache_pos, cache_part)?)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The read loop fills the buffer across short reads, retries an
    /// interrupted read, and stops at end of file.
    ///
    /// A regular file returns everything asked for or stops at its end, so
    /// none of this is reachable through a real one -- the loop is driven by a
    /// reader that can be told what to do.
    #[test]
    fn the_read_loop_handles_short_reads_interruptions_and_eof() {
        use std::{
            cell::RefCell,
            io::{Error, ErrorKind},
        };

        /// The offset of every call the scripted reader received, in order.
        type SeenOffsets = std::rc::Rc<RefCell<Vec<u64>>>;

        /// A reader that replays a script of results and records the offset of
        /// each call.
        fn scripted(
            script: Vec<std::io::Result<usize>>,
        ) -> (
            impl Fn(u64, &mut [u8]) -> std::io::Result<usize>,
            SeenOffsets,
        ) {
            let offsets = std::rc::Rc::new(RefCell::new(Vec::new()));
            let seen = std::rc::Rc::clone(&offsets);
            let script = RefCell::new(script.into_iter());
            let read = move |offset: u64, into: &mut [u8]| {
                seen.borrow_mut().push(offset);
                match script.borrow_mut().next() {
                    Some(Ok(n)) => {
                        into[..n].fill(b'x');
                        Ok(n)
                    }
                    Some(Err(e)) => Err(e),
                    None => Ok(0),
                }
            };
            (read, offsets)
        }

        // Three short reads fill an eight-byte buffer, each resuming where the
        // last stopped.
        let (read, offsets) = scripted(vec![Ok(3), Ok(4), Ok(1)]);
        let mut buf = [0u8; 8];
        check!(read_full_with(read, 100, &mut buf).unwrap() == 8);
        check!(
            *offsets.borrow() == vec![100, 103, 107],
            "each read resumes where the last ended, got {:?}",
            offsets.borrow()
        );

        // An interrupted read is retried at the same offset, not skipped past.
        let (read, offsets) =
            scripted(vec![Ok(2), Err(Error::from(ErrorKind::Interrupted)), Ok(2)]);
        let mut buf = [0u8; 4];
        check!(read_full_with(read, 0, &mut buf).unwrap() == 4);
        check!(
            *offsets.borrow() == vec![0, 2, 2],
            "the retry repeats the offset, got {:?}",
            offsets.borrow()
        );

        // End of file stops the loop with whatever was read so far.
        let (read, _) = scripted(vec![Ok(2), Ok(0), Ok(9)]);
        let mut buf = [0u8; 8];
        check!(
            read_full_with(read, 0, &mut buf).unwrap() == 2,
            "stops at EOF"
        );

        // Any other error is returned rather than retried.
        let (read, _) = scripted(vec![Ok(1), Err(Error::from(ErrorKind::PermissionDenied))]);
        let mut buf = [0u8; 4];
        check!(
            read_full_with(read, 0, &mut buf).is_err(),
            "a real error propagates"
        );

        // A buffer that is already full asks for nothing at all.
        let (read, offsets) = scripted(vec![Ok(1)]);
        let mut empty: [u8; 0] = [];
        check!(read_full_with(read, 0, &mut empty).unwrap() == 0);
        check!(
            offsets.borrow().is_empty(),
            "no read is issued for no bytes"
        );
    }

    #[test]
    fn write_all_and_vectored_handle_interrupted() {
        use std::io::ErrorKind;

        #[derive(Debug)]
        struct MockIo {
            calls: std::sync::atomic::AtomicUsize,
            error_kind: ErrorKind,
        }

        impl MockIo {
            fn new(error_kind: ErrorKind) -> Self {
                Self {
                    calls: std::sync::atomic::AtomicUsize::new(0),
                    error_kind,
                }
            }

            fn offered(&self, len: usize) -> std::io::Result<usize> {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Err(std::io::Error::from(self.error_kind))
                } else {
                    Ok(len)
                }
            }
        }

        impl LogIo for MockIo {
            fn write(&self, _file: &File, buf: &[u8]) -> std::io::Result<usize> {
                self.offered(buf.len())
            }

            fn write_vectored(&self, _file: &File, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
                self.offered(bufs.iter().map(|b| b.len()).sum())
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let file = File::create(temp.path().join("test")).unwrap();

        for (error, succeeds) in [
            (ErrorKind::Interrupted, true),       // Interrupted is retried.
            (ErrorKind::PermissionDenied, false), // Other errors propagate.
        ] {
            let io = MockIo::new(error);
            assert2::check!(write_all(&io, &file, b"hello").is_ok() == succeeds);

            let io = MockIo::new(error);
            let slice = *b"hi";
            let mut slices = [IoSlice::new(&slice)];
            assert2::check!(write_all_vectored(&io, &file, &mut slices).is_ok() == succeeds);
        }
    }

    /// The readahead span is the read's window and up to one more window
    /// after it, capped at the configured maximum, never past the end of the
    /// file.
    #[test]
    fn the_read_ahead_span_covers_the_window_and_the_next_one() {
        let max = 4 * 1024 * 1024;
        let cases = [
            // (name, start_pos, window, log_size, read_ahead_max, expected)
            (
                "window and the next",
                100,
                1_000,
                10_000,
                max,
                Some((100, 2_000)),
            ),
            (
                "the next window clamped",
                100,
                1_000,
                1_500,
                max,
                Some((100, 1_400)),
            ),
            (
                "the window itself clamped",
                100,
                1_000,
                600,
                max,
                Some((100, 500)),
            ),
            (
                "the lookahead capped",
                0,
                3 * max,
                10 * max,
                max,
                Some((0, 4 * max)),
            ),
            ("a small cap", 100, 1_000, 10_000, 300, Some((100, 1_300))),
            (
                "a zero cap hints only the window",
                100,
                1_000,
                10_000,
                0,
                Some((100, 1_000)),
            ),
            ("nothing left", 600, 1_000, 600, max, None),
            ("past the end", 700, 1_000, 600, max, None),
            ("an empty window", 100, 0, 1_000, max, None),
            (
                "no overflow",
                u64::MAX - 1,
                10,
                u64::MAX,
                max,
                Some((u64::MAX - 1, 1)),
            ),
        ];
        let actual: Vec<_> = cases
            .iter()
            .map(|&(name, start, window, size, cap, _)| {
                (name, read_ahead_span(start, window, size, cap))
            })
            .collect();
        let expected: Vec<_> = cases
            .iter()
            .map(|&(name, .., expected)| (name, expected))
            .collect();
        assert2::assert!(actual == expected);
    }

    /// A verbatim read asks the kernel for its whole window and the window
    /// after it, from the batch it starts at, in one hint.
    #[test]
    fn a_verbatim_read_advises_its_window_and_the_next_in_one_hint() {
        use krabka_ids::Offset;
        use krabka_units::prelude::bytes;

        use crate::segment::test_support::{
            DENSE_INDEX, recording_advice, test_batch_at, test_segment,
        };

        let (_dir, mut seg) = test_segment();
        let mut positions = Vec::new();
        for off in 0..20i64 {
            positions.push(seg.log_size);
            seg.append(&test_batch_at(crate::Offset(off)), DENSE_INDEX)
                .unwrap();
        }
        let advice = recording_advice(&mut seg);
        let budget = 100u64;

        let mid = seg
            .read_raw(Offset(2), Offset(20), bytes(u32::try_from(budget).unwrap()))
            .unwrap();
        check!(mid.start_offset == Offset(2));
        check!(advice.take() == vec![(positions[2], 2 * budget)]);

        // Near the end the hint stops at the end of the file.
        seg.read_raw(
            Offset(19),
            Offset(20),
            bytes(u32::try_from(budget).unwrap()),
        )
        .unwrap();
        check!(advice.take() == vec![(positions[19], seg.log_size - positions[19])]);

        // A read that finds nothing to serve asks for nothing.
        seg.read_raw(Offset(20), Offset(30), bytes(100)).unwrap();
        check!(advice.take() == Vec::<(u64, u64)>::new());
    }
}
