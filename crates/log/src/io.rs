//! Injectable file I/O for every durable write the log makes.
//!
//! The active `.log` file keeps the three handle-shaped methods the append
//! hot path uses. Every other durable write in the crate -- the sparse
//! indexes, the `.stampindex`, the producer snapshots, the leader-epoch
//! checkpoint, the compaction swap and the segment deletions retention
//! performs -- goes through the path-and-target methods below, so a test can
//! fail exactly one class of file and watch what recovery makes of it. The
//! readahead hint of the fetch path goes through it too, so a test can see
//! which ranges a read asks the kernel for, and so do the block reservation
//! and release of `preallocate` and the `O_DIRECT` handle its active segment
//! writes through.

use std::{
    fmt::Debug,
    fs::File,
    io::{IoSlice, Write},
    path::Path,
};

/// Which on-disk file class an [`LogIo`] operation touches.
///
/// A fault injector matches on this to fail one durable write without
/// disturbing the rest of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoTarget {
    /// A segment's sparse `.index`.
    OffsetIndex,
    /// A segment's sparse `.timeindex`.
    TimeIndex,
    /// A segment's `.stampindex` sidecar.
    StampIndex,
    /// A `<offset>.snapshot` producer-state snapshot, or its `.tmp` staging
    /// file.
    ProducerSnapshot,
    /// The partition's `leader-epoch-checkpoint`, or its `.tmp` staging file.
    LeaderEpochCheckpoint,
    /// The partition's `log-start-offset-checkpoint`, or its `.tmp` staging
    /// file.
    LogStartOffsetCheckpoint,
    /// A compaction `.swap` file, or a segment file the swap replaces.
    CompactionSwap,
    /// A segment file being renamed to its `.deleted` tombstone or unlinked
    /// by retention.
    SegmentDeletion,
    /// A file outside any partition log that another crate replaces through
    /// [`write_file_atomic`]. Only the real-file I/O ever writes one.
    External,
}

/// The operating-system I/O boundary every durable log write crosses.
///
/// The default methods perform real file I/O. Tests can override only the
/// operation they need to fail, while reads continue to use the segment's
/// shared `Arc<File>` directly.
pub trait LogIo: Debug + Send + Sync {
    /// Write bytes at the active `.log` file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write(&self, file: &File, buf: &[u8]) -> std::io::Result<usize> { (&*file).write(buf) }

    /// Write byte slices at the active `.log` file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying vectored-write error.
    fn write_vectored(&self, file: &File, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        (&*file).write_vectored(bufs)
    }

    /// Flush the active `.log` file's data to stable storage.
    ///
    /// # Errors
    /// Returns the underlying data-sync error.
    fn sync_data(&self, file: &File) -> std::io::Result<()> { file.sync_data() }

    /// Write bytes at `file`'s current cursor on behalf of `target`.
    ///
    /// Like [`std::io::Write::write`], this may write fewer bytes than asked
    /// for; [`write_all`] is the loop that finishes the buffer.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        let _ = target;
        (&*file).write(buf)
    }

    /// Flush `target`'s file data to stable storage.
    ///
    /// # Errors
    /// Returns the underlying data-sync error.
    fn sync_file(&self, target: IoTarget, file: &File) -> std::io::Result<()> {
        let _ = target;
        file.sync_data()
    }

    /// `fsync` a directory so the names created or renamed inside it are
    /// durable.
    ///
    /// # Errors
    /// Returns the underlying open or sync error.
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        // Rust's standard directory-open path is supported on Unix, where
        // syncing the parent is what makes a rename or a fresh name durable.
        // Windows offers no equivalent through `std` (`File::open` on a
        // directory fails with `EACCES`), so the call is a no-op there and on
        // every other non-Unix target, `wasm32-wasip1` included. The file
        // contents are still synced before every rename.
        #[cfg(unix)]
        {
            File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }

    /// Rename a file on behalf of `target`.
    ///
    /// # Errors
    /// Returns the underlying rename error.
    fn rename(&self, target: IoTarget, from: &Path, to: &Path) -> std::io::Result<()> {
        let _ = target;
        std::fs::rename(from, to)
    }

    /// Unlink a file on behalf of `target`.
    ///
    /// # Errors
    /// Returns the underlying unlink error.
    fn remove_file(&self, target: IoTarget, path: &Path) -> std::io::Result<()> {
        let _ = target;
        std::fs::remove_file(path)
    }

    /// Tell the kernel that a fetch reads `len` bytes of a `.log` file from
    /// `offset` next.
    ///
    /// This is the one read-side method, and it is a hint, not a read: it
    /// starts `posix_fadvise(POSIX_FADV_WILLNEED)` readahead of the range and
    /// returns without waiting for it. A failure changes nothing a reader can
    /// observe, so it is dropped. The targets without `posix_fadvise` do
    /// nothing.
    fn advise_will_need(&self, file: &File, offset: u64, len: u64) {
        advise_will_need(file, offset, len);
    }

    /// Whether the page cache holds the byte at `offset` of `file`, asked
    /// without waiting for the disk. The fetch path asks it before it walks a
    /// run's batch headers one small read at a time: on bytes that are not in
    /// the page cache, each of those reads waits for the disk under the log
    /// lock, and one large read is faster. A host that cannot ask answers
    /// true, which keeps the walk.
    #[krabka_macros::sendfile_cfg]
    fn is_cached(&self, file: &File, offset: u64) -> bool { is_cached(file, offset) }

    /// Reserve disk blocks for the `len` bytes of a `.log` file from `offset`
    /// without changing its length, so the appends that later fill the range
    /// do not allocate.
    ///
    /// This is `fallocate(FALLOC_FL_KEEP_SIZE)`. A reservation changes no
    /// byte a reader can see, so a caller treats a failure as a segment that
    /// grows as it is written. The targets without it answer `Unsupported`.
    ///
    /// # Errors
    /// Returns the underlying `fallocate` error, or `Unsupported` off Linux.
    fn reserve(&self, file: &File, offset: u64, len: u64) -> std::io::Result<()> {
        reserve(file, offset, len)
    }

    /// Give back the blocks a [`LogIo::reserve`] left unused past a `.log`
    /// file's end, which is `len`.
    ///
    /// This truncates the file to the length it already has. ext4 and tmpfs
    /// free every block past the end on a truncate, even one that moves
    /// nothing, and that is the only way to reach blocks reserved past the
    /// end: ext4 ignores `FALLOC_FL_PUNCH_HOLE` at or past it. A filesystem
    /// that keeps them keeps them until retention deletes the segment.
    ///
    /// # Errors
    /// Returns the underlying truncate error.
    fn release(&self, file: &File, len: u64) -> std::io::Result<()> { file.set_len(len) }

    /// Open a second handle on a `.log` file that bypasses the page cache
    /// (`O_DIRECT`), with the alignment every write through it needs.
    ///
    /// The alignment comes from `statx(STATX_DIOALIGN)` (Linux 6.1) and from
    /// nowhere else: a guessed one that is wrong fails the append that first
    /// uses it, and a failed append takes the log directory offline. A
    /// kernel or filesystem that does not report one answers `Unsupported`,
    /// and the segment stays buffered.
    ///
    /// # Errors
    /// Returns `Unsupported` where `O_DIRECT` is unavailable or its alignment
    /// unknown, or the underlying `statx` or `open` error.
    fn open_direct(&self, path: &Path) -> std::io::Result<DirectFile> { open_direct(path) }

    /// Write `buf` at `offset` through a handle [`LogIo::open_direct`]
    /// opened. `buf`, `offset` and the length are aligned as that handle
    /// requires. Like [`std::io::Write::write`], this may write fewer bytes
    /// than asked for.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        write_direct(file, buf, offset)
    }
}

/// A `.log` handle that bypasses the page cache, and what it requires of a
/// write: a buffer address aligned to `mem_align`, and an offset and a length
/// that are multiples of `block`.
#[derive(Debug)]
pub struct DirectFile {
    /// The `O_DIRECT` handle.
    pub file: File,
    /// Required alignment of a write's buffer address, in bytes.
    pub mem_align: usize,
    /// Required alignment of a write's offset and length, in bytes.
    pub block: usize,
}

#[cfg(target_os = "linux")]
fn open_direct(path: &Path) -> std::io::Result<DirectFile> {
    use rustix::fs::{AtFlags, CWD, Mode, OFlags, StatxFlags};

    let stat = rustix::fs::statx(CWD, path, AtFlags::empty(), StatxFlags::DIOALIGN)?;
    let reported = stat.stx_mask & StatxFlags::DIOALIGN.bits() != 0;
    // A zero offset alignment is the kernel saying this file cannot be
    // opened `O_DIRECT` at all.
    if !reported || stat.stx_dio_offset_align == 0 || stat.stx_dio_mem_align == 0 {
        return Err(std::io::ErrorKind::Unsupported.into());
    }
    let to_usize = |align: u32| {
        usize::try_from(align).map_err(|_| std::io::Error::from(std::io::ErrorKind::Unsupported))
    };
    let fd = rustix::fs::open(
        path,
        OFlags::RDWR | OFlags::DIRECT | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(DirectFile {
        file: File::from(fd),
        mem_align: to_usize(stat.stx_dio_mem_align)?,
        block: to_usize(stat.stx_dio_offset_align)?,
    })
}

#[cfg(not(target_os = "linux"))]
fn open_direct(path: &Path) -> std::io::Result<DirectFile> {
    let _ = path;
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(unix)]
fn write_direct(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

/// No target without `O_DIRECT` ever opens a handle to write through.
#[cfg(not(unix))]
fn write_direct(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    let _ = (file, buf, offset);
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(target_os = "linux")]
fn reserve(file: &File, offset: u64, len: u64) -> std::io::Result<()> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::KEEP_SIZE, offset, len)
        .map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
fn reserve(file: &File, offset: u64, len: u64) -> std::io::Result<()> {
    let _ = (file, offset, len);
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(target_os = "linux")]
fn advise_will_need(file: &File, offset: u64, len: u64) {
    if let Some(len) = std::num::NonZeroU64::new(len) {
        // A refused hint leaves the read exactly as it was without one.
        let _ = rustix::fs::fadvise(file, offset, Some(len), rustix::fs::Advice::WillNeed);
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_will_need(file: &File, offset: u64, len: u64) { let _ = (file, offset, len); }

/// `preadv2(RWF_NOWAIT)` of one byte: it fails with `EAGAIN` instead of
/// reading from the disk when the page cache does not hold the byte. Linux
/// 5.9 and 5.10 can return zero bytes instead (a bug `preadv2(2)` documents);
/// callers only ask about bytes below the end of the file, so zero bytes also
/// means not cached.
#[cfg(target_os = "linux")]
fn is_cached(file: &File, offset: u64) -> bool {
    let mut byte = [0u8; 1];
    !matches!(
        rustix::io::preadv2(
            file,
            &mut [std::io::IoSliceMut::new(&mut byte)],
            offset,
            rustix::io::ReadWriteFlags::NOWAIT,
        ),
        Err(rustix::io::Errno::AGAIN) | Ok(0)
    )
}

#[cfg(not(target_os = "linux"))]
#[krabka_macros::sendfile_cfg]
fn is_cached(file: &File, offset: u64) -> bool {
    let _ = (file, offset);
    true
}

/// Write the whole of `buf` to `file` through `io`, looping over short writes.
///
/// A `LogIo` that writes part of the buffer and then fails leaves the file
/// torn at exactly that boundary, which is the shape a disk-full write has on
/// a real filesystem.
///
/// # Errors
/// Returns the first error the underlying writes produce, or `WriteZero` when
/// a write makes no progress.
pub(crate) fn write_all(
    io: &dyn LogIo,
    target: IoTarget,
    file: &File,
    buf: &[u8],
) -> std::io::Result<()> {
    write_all_with(buf, |remaining| io.write_at(target, file, remaining))
}

/// Complete a scalar write, retrying interrupts and rejecting zero progress.
pub(crate) fn write_all_with(
    mut buf: &[u8],
    mut write: impl FnMut(&[u8]) -> std::io::Result<usize>,
) -> std::io::Result<()> {
    while !buf.is_empty() {
        buf = &buf[write_progress(|| write(buf))?..];
    }
    Ok(())
}

/// Retry an interrupted write and require it to make progress.
pub(crate) fn write_progress(
    mut write: impl FnMut() -> std::io::Result<usize>,
) -> std::io::Result<usize> {
    loop {
        match write() {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

/// Replace `path` with `bytes` atomically and durably through real file I/O:
/// write `tmp`, sync it, rename it over `path`, then sync the parent
/// directory. This is the entry point for files outside a partition log, such
/// as the controller's `quorum-state` file and its metadata checkpoints, which
/// have no fault-injection seam of their own.
///
/// # Errors
/// Returns the first create, write, sync or rename error.
pub fn write_file_atomic(tmp: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    write_atomic(&FileIo, IoTarget::External, tmp, path, bytes, true)
}

/// Replace `path` with `bytes` atomically: write them to `tmp`, flush `tmp` to
/// stable storage, rename it over `path`, and, when `sync_dir` is set, `fsync`
/// `path`'s parent directory so the rename itself is durable.
///
/// Every step goes through `io` on behalf of `target`, so a fault injector can
/// fail the write, the flush, the rename or the directory sync. A failure
/// before the rename leaves `path` as it was; `tmp` may be left behind.
///
/// # Errors
/// Returns the first create, write, sync or rename error.
pub(crate) fn write_atomic(
    io: &dyn LogIo,
    target: IoTarget,
    tmp: &Path,
    path: &Path,
    bytes: &[u8],
    sync_dir: bool,
) -> std::io::Result<()> {
    {
        let file = File::create(tmp)?;
        write_all(io, target, &file, bytes)?;
        io.sync_file(target, &file)?;
    }
    io.rename(target, tmp, path)?;
    if sync_dir {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        io.sync_dir(parent)?;
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct FileIo;

impl LogIo for FileIo {}

/// The shared handle every structure defaults to before a test installs its
/// own.
pub(crate) fn file_io() -> std::sync::Arc<dyn LogIo> { std::sync::Arc::new(FileIo) }

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        std::{path::PathBuf, sync::Mutex},
    };

    /// A writer that replays a script of results, recording what each call was
    /// offered. A regular file cannot be persuaded to write short or to make
    /// no progress on demand, so the loop's two obligations -- resume where the
    /// last write stopped, and refuse to spin on a writer that writes nothing
    /// -- are only reachable through one of these.
    #[derive(Debug)]
    struct Scripted {
        script: Mutex<std::vec::IntoIter<std::io::Result<usize>>>,
        seen: Mutex<Vec<Vec<u8>>>,
    }

    impl Scripted {
        fn new(script: Vec<std::io::Result<usize>>) -> Self {
            Self {
                script: Mutex::new(script.into_iter()),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl LogIo for Scripted {
        fn write_at(&self, _target: IoTarget, _file: &File, buf: &[u8]) -> std::io::Result<usize> {
            self.seen.lock().unwrap().push(buf.to_vec());
            self.script
                .lock()
                .unwrap()
                .next()
                .unwrap_or(Ok(buf.len()))
                .map(|written| written.min(buf.len()))
        }
    }

    /// A file in a temporary directory.
    struct ScratchFile {
        dir: tempfile::TempDir,
        file: File,
    }

    fn scratch_file() -> ScratchFile {
        let dir = tempfile::tempdir().unwrap();
        let file = File::create(dir.path().join("scratch")).unwrap();
        ScratchFile { dir, file }
    }

    /// A state file with contents, and the staging path beside it, in a
    /// temporary directory.
    struct ExistingState {
        dir: tempfile::TempDir,
        path: PathBuf,
        tmp: PathBuf,
    }

    fn existing_state(contents: &[u8]) -> ExistingState {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        let tmp = dir.path().join("state.tmp");
        std::fs::write(&path, contents).unwrap();
        ExistingState { dir, path, tmp }
    }

    #[test]
    fn write_atomic_replaces_the_target_and_consumes_the_staging_file() {
        let ExistingState {
            dir: _dir,
            path,
            tmp,
        } = existing_state(b"old contents");

        for contents in [b"new".as_slice(), b"newer"] {
            write_file_atomic(&tmp, &path, contents).unwrap();
            check!(std::fs::read(&path).unwrap() == contents.to_vec());
            check!(!tmp.exists());
        }
    }

    #[test]
    fn write_atomic_leaves_the_target_alone_when_the_staged_write_fails() {
        let ExistingState {
            dir: _dir,
            path,
            tmp,
        } = existing_state(b"old");

        let io = Scripted::new(vec![Err(std::io::Error::from(
            std::io::ErrorKind::StorageFull,
        ))]);
        let error = write_atomic(&io, IoTarget::External, &tmp, &path, b"new", true).unwrap_err();
        check!(error.kind() == std::io::ErrorKind::StorageFull);
        check!(std::fs::read(&path).unwrap() == b"old".to_vec());
    }

    #[test]
    fn write_all_resumes_short_writes_retries_interruptions_and_stops_on_no_progress() {
        let ScratchFile { dir: _dir, file } = scratch_file();

        for (script, expected) in [
            // Three short writes finish a six-byte buffer, each offered only what
            // the last one left.
            (vec![Ok(2), Ok(3), Ok(1)], vec![
                b"abcdef".to_vec(),
                b"cdef".to_vec(),
                b"f".to_vec(),
            ]),
            // An interrupted write is retried with the same bytes, not skipped past.
            (
                vec![
                    Ok(2),
                    Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
                    Ok(4),
                ],
                vec![b"abcdef".to_vec(), b"cdef".to_vec(), b"cdef".to_vec()],
            ),
        ] {
            let io = Scripted::new(script);
            write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap();
            check!(*io.seen.lock().unwrap() == expected);
        }
        for (script, expected) in [
            // A writer that makes no progress is a `WriteZero`, not a spin.
            (vec![Ok(0)], std::io::ErrorKind::WriteZero),
            // Any other error is returned as it stands.
            (
                vec![
                    Ok(1),
                    Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                ],
                std::io::ErrorKind::StorageFull,
            ),
        ] {
            let io = Scripted::new(script);
            let error = write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap_err();
            check!(error.kind() == expected);
        }
    }
}
