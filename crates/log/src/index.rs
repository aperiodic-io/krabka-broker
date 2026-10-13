//! The two sparse per-segment indexes: the `.index` file, which maps a
//! relative offset to a byte position in the segment's log, and the
//! `.timeindex` file, which maps a timestamp to a relative offset.
//!
//! One submodule per on-disk artifact. `offset` holds the 8-byte `.index`
//! entry layout, its loader and its floor lookup; `time` holds the 12-byte
//! `.timeindex` entry layout, its loader and its timestamp lookup. Both
//! layouts are byte-compatible with Kafka's, so each entry's encode, decode
//! and binary-search paths stay beside the layout they read.

use {
    crate::{
        error::LogError,
        io::{IoTarget, LogIo, write_all},
    },
    krabka_verified::log_index::IndexEntry,
    std::{
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom},
        path::Path,
    },
    zerocopy::{
        BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::U32,
    },
};

/// A fixed-width big-endian key and its unsigned coordinate, in Kafka field order.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct IndexEntryRaw<Key> {
    key: Key,
    coordinate: U32<BigEndian>,
}

/// An explicit sparse-index entry a test writes: a key and its coordinate.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexSeed<Key> {
    pub(crate) key: Key,
    pub(crate) coordinate: u32,
}

mod prelude {
    pub(super) use {
        crate::{
            error::LogError,
            io::{IoTarget, LogIo},
        },
        krabka_verified::log_index::IndexEntry,
        std::{fs::File, path::Path},
        tracing::instrument,
        zerocopy::{
            BigEndian, IntoBytes,
            byteorder::{I64, U32},
        },
    };
}

mod offset;
mod time;

#[cfg(test)]
pub(crate) use self::time::TimeIndexEntry;
pub(crate) use self::{offset::OFFSET_ENTRY_SIZE, time::TIME_ENTRY_SIZE};
pub use self::{
    offset::{OffsetIndex, OffsetIndexEntry},
    time::TimeIndex,
};

/// An index file opened for read and write, with the bytes it holds.
struct OpenedFile {
    file: File,
    bytes: Vec<u8>,
}

fn open_index_file(path: &Path) -> Result<OpenedFile, LogError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(OpenedFile { file, bytes })
}

type IndexEntries<Key> = Vec<IndexEntry<Key, u32>>;

/// An index file and the entries decoded from it.
struct OpenedIndex<Key> {
    file: File,
    entries: IndexEntries<Key>,
}

fn load_index<Raw, Key>(
    path: &Path,
    invariant: &str,
    decode: impl Fn(&Raw) -> IndexEntry<Key, u32>,
) -> Result<OpenedIndex<Key>, LogError>
where
    Raw: FromBytes + KnownLayout + Immutable + Unaligned,
{
    let OpenedFile { file, bytes: buf } = open_index_file(path)?;
    let entry_size = std::mem::size_of::<Raw>();
    let truncated_len = (buf.len() / entry_size) * entry_size;
    let raws = <[Raw]>::ref_from_bytes(&buf[..truncated_len]).expect(invariant);
    let entries = increasing_index_entries(raws, decode);
    tracing::Span::current().record("entries", entries.len());
    Ok(OpenedIndex { file, entries })
}

fn append_index<Key>(
    file: &mut File,
    io: &dyn LogIo,
    target: IoTarget,
    bytes: &[u8],
    entries: &mut Vec<IndexEntry<Key, u32>>,
    key: Key,
    coordinate: u32,
) -> Result<(), LogError> {
    file.seek(SeekFrom::End(0))?;
    write_all(io, target, file, bytes)?;
    entries.push(IndexEntry { key, coordinate });
    Ok(())
}

/// Decode the real prefix, stopping when the position or offset column stops increasing.
fn increasing_index_entries<Raw, Key>(
    raws: &[Raw],
    decode: impl Fn(&Raw) -> IndexEntry<Key, u32>,
) -> Vec<IndexEntry<Key, u32>> {
    let mut entries: Vec<IndexEntry<Key, u32>> = Vec::with_capacity(raws.len());
    for raw in raws {
        let entry = decode(raw);
        if entries
            .last()
            .is_some_and(|previous| entry.coordinate <= previous.coordinate)
        {
            break;
        }
        entries.push(entry);
    }
    entries
}

fn truncate_index<Key>(
    file: &mut File,
    entries: &mut Vec<IndexEntry<Key, u32>>,
    entry_size: usize,
    max_exclusive: u32,
) -> Result<(), LogError> {
    let kept = entries
        .iter()
        .take_while(|entry| entry.coordinate < max_exclusive)
        .count();
    entries.truncate(kept);
    file.set_len((kept * entry_size) as u64)?;
    file.seek(SeekFrom::End(0))?;
    tracing::Span::current().record("entries", kept);
    Ok(())
}

/// Rewrite retained sidecar entries only when truncation changed the set.
/// The caller publishes the new entries only after the rewrite succeeds.
pub(crate) fn rewrite_retained_entries<T: Copy>(
    entries: &[T],
    keep: impl Fn(&T) -> bool,
    rewrite: impl FnOnce(&[T]) -> Result<(), LogError>,
) -> Result<Option<Vec<T>>, LogError> {
    let retained: Vec<_> = entries.iter().copied().filter(keep).collect();
    if retained.len() == entries.len() {
        return Ok(None);
    }
    rewrite(&retained)?;
    Ok(Some(retained))
}

/// Read a fixed-width sidecar, treating only a missing file as an empty index.
pub(crate) fn read_sidecar<Raw, Entries>(
    path: &Path,
    name: &str,
    invariant: &str,
    decode: impl FnOnce(&[Raw]) -> Result<Entries, LogError>,
) -> Result<Entries, LogError>
where
    Raw: FromBytes + KnownLayout + Immutable + Unaligned,
{
    let entry_size = std::mem::size_of::<Raw>();
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    if !bytes.len().is_multiple_of(entry_size) {
        return Err(LogError::Corrupt(format!(
            "{name} {} has length {} not divisible by {entry_size}",
            path.display(),
            bytes.len(),
        )));
    }
    decode(<[Raw]>::ref_from_bytes(&bytes).expect(invariant))
}

/// A sparse index, the path of its file, and the temporary directory that
/// holds it.
#[cfg(test)]
pub(crate) struct IndexFixture<Index> {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) path: std::path::PathBuf,
    pub(crate) index: Index,
}

/// Open a sparse-index fixture while keeping its directory alive beside its handle.
#[cfg(test)]
pub(crate) fn index_fixture<Index>(
    name: &str,
    open: impl FnOnce(&Path) -> Result<Index, LogError>,
) -> IndexFixture<Index> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let index = open(&path).unwrap();
    IndexFixture { dir, path, index }
}

/// Open a sidecar for a complete rewrite, retaining the established create/truncate flags.
pub(crate) fn rewrite_sidecar_file(path: &Path) -> Result<File, LogError> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(LogError::Io)
}

#[cfg(test)]
mod derived_operations_tests {
    use {
        super::{OffsetIndex, TimeIndex},
        crate::{
            error::LogError,
            io::{IoTarget, LogIo},
        },
        assert2::assert,
        std::{
            fs::File,
            sync::{
                Arc, Mutex,
                atomic::{AtomicBool, Ordering},
            },
        },
    };

    #[derive(Debug, Default)]
    struct SyncProbe {
        calls: Mutex<Vec<IoTarget>>,
        fail: AtomicBool,
    }

    impl LogIo for SyncProbe {
        fn sync_file(&self, target: IoTarget, _: &File) -> std::io::Result<()> {
            self.calls.lock().unwrap().push(target);
            if self.fail.load(Ordering::Relaxed) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "index sync refused",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn derived_flush_uses_the_installed_io_target_and_preserves_sync_errors() {
        let directory = tempfile::tempdir().unwrap();
        let mut offset = OffsetIndex::open(&directory.path().join("probe.index")).unwrap();
        let mut time = TimeIndex::open(&directory.path().join("probe.timeindex")).unwrap();
        let probe = Arc::new(SyncProbe::default());
        offset.set_io(probe.clone());
        time.set_io(probe.clone());
        for fail in [false, true] {
            probe.fail.store(fail, Ordering::Relaxed);
            for result in [offset.flush(), time.flush()] {
                if fail {
                    assert!(matches!(result, Err(LogError::Io(error))
                        if error.kind() == std::io::ErrorKind::PermissionDenied
                            && error.to_string() == "index sync refused"));
                } else {
                    assert!(result.is_ok());
                }
            }
        }
        assert!(
            *probe.calls.lock().unwrap()
                == [
                    IoTarget::OffsetIndex,
                    IoTarget::TimeIndex,
                    IoTarget::OffsetIndex,
                    IoTarget::TimeIndex,
                ]
        );
        assert!(offset.entry_count() == 0 && time.entry_count() == 0);
    }
}
