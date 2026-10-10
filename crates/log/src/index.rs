//! The two sparse per-segment indexes: the `.index` file, which maps a
//! relative offset to a byte position in the segment's log, and the
//! `.timeindex` file, which maps a timestamp to a relative offset.
//!
//! One submodule per on-disk artifact. `offset` holds the 8-byte `.index`
//! entry layout, its loader and its floor lookup; `time` holds the 12-byte
//! `.timeindex` entry layout, its loader and its timestamp lookup. Both
//! layouts are byte-compatible with Kafka's, so each entry's encode, decode
//! and binary-search paths stay beside the layout they read.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::U32,
};

use crate::{
    error::LogError,
    io::{IoTarget, LogIo},
};

// Keep the open span consistent for every on-disk index and checkpoint.
macro_rules! open_index {
    ($(#[$doc:meta])* $visibility:vis fn open($path:ident: $path_type:ty) -> $result:ty $body:block) => {
        $(#[$doc])*
        #[tracing::instrument(
            level = "debug",
            skip_all,
            fields(path = %$path.display(), entries = tracing::field::Empty),
            err,
        )]
        $visibility fn open($path: $path_type) -> $result $body
    };
}

pub(crate) use open_index;

/// A fixed-width big-endian key and its unsigned coordinate, in Kafka field order.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct IndexEntryRaw<Key> {
    key: Key,
    coordinate: U32<BigEndian>,
}

macro_rules! index_constructor {
    ($raw:ty, $invariant:literal, $decode:expr; $(#[$doc:meta])*) => {
        open_index! {
            $(#[$doc])*
            pub fn open(path: &Path) -> Result<Self, LogError> {
                let (file, entries) = crate::index::load_index::<$raw, _>(path, $invariant, $decode)?;
                Ok(Self { file, io: crate::io::file_io(), entries })
            }
        }
    };
}

// Both index files expose the same memory count and file operations.
macro_rules! index_methods {
    ($target:expr) => {
        #[cfg(not(target_os = "wasi"))]
        pub(crate) fn flush_handle(&self) -> std::io::Result<File> {
            self.file.try_clone()
        }

        /// The number of entries the index holds, which decides when the
        /// segment is full under `segment.index.bytes`.
        #[must_use]
        pub fn entry_count(&self) -> usize {
            self.entries.len()
        }

        #[tracing::instrument(level = "debug", skip_all, err)]
        pub fn flush(&mut self) -> Result<(), LogError> {
            self.io.sync_file($target, &self.file).map_err(LogError::Io)
        }

        /// Route this index's writes and syncs through `io`.
        pub(crate) fn set_io(&mut self, io: std::sync::Arc<dyn crate::io::LogIo>) {
            self.io = io;
        }
    };
}

/// Write and flush explicit sparse-index entries, then close the writer before reopening.
#[cfg(test)]
macro_rules! seed_index_fixture {
    ($index:ident, $key:ty) => {
        fn append_entries(index: &mut $index, entries: &[($key, u32)]) {
            for &(key, coordinate) in entries {
                index.append(key, coordinate).unwrap();
            }
        }

        fn write_entries(path: &std::path::Path, entries: &[($key, u32)]) {
            let mut index = $index::open(path).unwrap();
            append_entries(&mut index, entries);
            index.flush().unwrap();
        }

        fn written_index(
            name: &str,
            entries: &[($key, u32)],
        ) -> (tempfile::TempDir, std::path::PathBuf) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(name);
            write_entries(&path, entries);
            (dir, path)
        }

        fn populated_index(
            name: &str,
            entries: &[($key, u32)],
        ) -> (tempfile::TempDir, std::path::PathBuf, $index) {
            let (dir, path, mut index) = crate::index::index_fixture(name, $index::open);
            append_entries(&mut index, entries);
            (dir, path, index)
        }
    };
}

mod offset;
mod time;

pub(crate) use self::{offset::OFFSET_ENTRY_SIZE, time::TIME_ENTRY_SIZE};
pub use self::{offset::OffsetIndex, time::TimeIndex};

fn open_index_file(path: &Path) -> Result<(File, Vec<u8>), LogError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok((file, bytes))
}

type IndexEntries<Key> = Vec<(Key, u32)>;
type OpenedIndex<Key> = (File, IndexEntries<Key>);

fn load_index<Raw, Key>(
    path: &Path,
    invariant: &str,
    decode: impl Fn(&Raw) -> (Key, u32),
) -> Result<OpenedIndex<Key>, LogError>
where
    Raw: FromBytes + KnownLayout + Immutable + Unaligned,
{
    let (file, buf) = open_index_file(path)?;
    let entry_size = std::mem::size_of::<Raw>();
    let truncated_len = (buf.len() / entry_size) * entry_size;
    let raws = <[Raw]>::ref_from_bytes(&buf[..truncated_len]).expect(invariant);
    let entries = increasing_index_entries(raws, decode);
    tracing::Span::current().record("entries", entries.len());
    Ok((file, entries))
}

fn append_index<Key>(
    file: &mut File,
    io: &dyn LogIo,
    target: IoTarget,
    bytes: &[u8],
    entries: &mut Vec<(Key, u32)>,
    entry: (Key, u32),
) -> Result<(), LogError> {
    file.seek(SeekFrom::End(0))?;
    crate::io::write_all(io, target, file, bytes)?;
    entries.push(entry);
    Ok(())
}

/// Decode the real prefix, stopping when the position or offset column stops increasing.
fn increasing_index_entries<Raw, Key>(
    raws: &[Raw],
    decode: impl Fn(&Raw) -> (Key, u32),
) -> Vec<(Key, u32)> {
    let mut entries: Vec<(Key, u32)> = Vec::with_capacity(raws.len());
    for raw in raws {
        let entry = decode(raw);
        if entries.last().is_some_and(|previous| entry.1 <= previous.1) {
            break;
        }
        entries.push(entry);
    }
    entries
}

fn truncate_index<Key>(
    file: &mut File,
    entries: &mut Vec<(Key, u32)>,
    entry_size: usize,
    max_exclusive: u32,
) -> Result<(), LogError> {
    let kept = entries
        .iter()
        .take_while(|(_, coordinate)| *coordinate < max_exclusive)
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

/// Open a sparse-index fixture while keeping its directory alive beside its handle.
#[cfg(test)]
pub(crate) fn index_fixture<Index>(
    name: &str,
    open: impl FnOnce(&Path) -> Result<Index, LogError>,
) -> (tempfile::TempDir, std::path::PathBuf, Index) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let index = open(&path).unwrap();
    (dir, path, index)
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
