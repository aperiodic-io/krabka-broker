//! Per-partition `log-start-offset-checkpoint` file: the durable record of a
//! log start that segment names alone cannot express.
//!
//! A trim that lands on a segment boundary survives a reopen on its own,
//! because the segments below it are gone and the first surviving base offset
//! *is* the log start. A trim that lands inside a segment has no such witness:
//! the records below it are still in the file, and without this checkpoint a
//! reopened log serves them again. Apache Kafka keeps the same value in a
//! `log-start-offset-checkpoint` per log dir, keyed by partition, written by
//! `LogManager.checkpointLogStartOffsets` and read back by `UnifiedLog`.
//! krabka writes one file per partition directory instead, which is the layout
//! `leader-epoch-checkpoint` already uses here and needs no topic/partition key
//! inside the file.
//!
//! The format is the checkpoint-file family's version header followed by the
//! offset:
//!
//! ```text
//!   0          <-- header version
//!   <offset>   <-- log start offset
//! ```
//!
//! Kafka's row count has no counterpart: a per-partition file holds exactly one
//! value.

use std::{fs, path::Path};

use krabka_ids::Offset;

use crate::{
    error::LogError,
    io::{IoTarget, LogIo, write_atomic},
    name,
};

/// Read the checkpointed log start offset. A missing file gives `None`, which
/// means "no trim has moved the start past what the segment names say".
///
/// # Errors
///
/// Returns [`LogError::Corrupt`] when the file is present but does not parse,
/// and [`LogError::Io`] when the read itself fails. A trim that a reopen
/// silently dropped serves records an operator deleted, so a checkpoint that
/// cannot be read is an error rather than a fall back to the derived start.
pub(crate) fn read(dir: &Path) -> Result<Option<Offset>, LogError> {
    let path = name::log_start_offset_checkpoint_path(dir);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(LogError::Io(e)),
    };
    parse(&contents).map(Some)
}

fn parse(contents: &str) -> Result<Offset, LogError> {
    let corrupt = || LogError::Corrupt(format!("bad log start offset checkpoint: {contents:?}"));
    let mut lines = contents.lines();
    let version = lines.next().ok_or_else(corrupt)?.trim();
    if version != "0" {
        return Err(corrupt());
    }
    let offset: i64 = lines
        .next()
        .and_then(|line| line.trim().parse().ok())
        .ok_or_else(corrupt)?;
    if offset < 0 {
        return Err(corrupt());
    }
    Ok(Offset(offset))
}

/// Rewrite the checkpoint atomically and durably. The temporary file is synced
/// before the rename, and the parent directory after it, so the checkpoint is
/// on stable storage by the time this returns.
///
/// The directory sync is not deferred to [`crate::Log::sync`] the way a new
/// segment name is. A `DeleteRecords` is acknowledged to the client as soon as
/// the trim returns, and a trimmed partition is often idle afterwards, so there
/// may be no later `sync` to pay the debt: a crash would drop the rename and
/// serve the deleted records again. A trim is an administrative operation, so
/// the extra fsync costs nothing that matters.
pub(crate) fn write(io: &dyn LogIo, dir: &Path, log_start_offset: Offset) -> Result<(), LogError> {
    let path = name::log_start_offset_checkpoint_path(dir);
    let tmp = path.with_extension("tmp");
    write_atomic(
        io,
        IoTarget::LogStartOffsetCheckpoint,
        &tmp,
        &path,
        format!("0\n{}\n", log_start_offset.0).as_bytes(),
        true,
    )
    .map_err(LogError::Io)
}

/// Drop the checkpoint. A log that was reset holds no records at all, so the
/// base offset of its fresh active segment is the whole truth about its start.
pub(crate) fn remove(dir: &Path) -> Result<(), LogError> {
    match fs::remove_file(name::log_start_offset_checkpoint_path(dir)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(LogError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::io::FileIo;

    #[test]
    fn round_trips_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read(dir.path()).unwrap() == None);

        write(&FileIo, dir.path(), Offset(42)).unwrap();
        assert!(read(dir.path()).unwrap() == Some(Offset(42)));

        write(&FileIo, dir.path(), Offset(100)).unwrap();
        assert!(read(dir.path()).unwrap() == Some(Offset(100)));

        remove(dir.path()).unwrap();
        assert!(read(dir.path()).unwrap() == None);
    }

    #[test]
    fn writes_the_versioned_two_line_format() {
        let dir = tempfile::tempdir().unwrap();
        write(&FileIo, dir.path(), Offset(7)).unwrap();
        let contents =
            fs::read_to_string(name::log_start_offset_checkpoint_path(dir.path())).unwrap();
        assert!(contents == "0\n7\n");
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        remove(dir.path()).unwrap();
        remove(dir.path()).unwrap();
    }

    #[test]
    fn rejects_a_checkpoint_it_cannot_trust() {
        for contents in ["", "0\n", "1\n7\n", "0\nnot-a-number\n", "0\n-1\n", "7\n"] {
            let dir = tempfile::tempdir().unwrap();
            fs::write(name::log_start_offset_checkpoint_path(dir.path()), contents).unwrap();
            let error = read(dir.path()).unwrap_err();
            assert!(matches!(error, LogError::Corrupt(_)), "{contents:?}");
        }
    }

    #[test]
    fn surfaces_non_missing_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(name::log_start_offset_checkpoint_path(dir.path())).unwrap();
        assert!(matches!(read(dir.path()), Err(LogError::Io(_))));
        assert!(matches!(remove(dir.path()), Err(LogError::Io(_))));
    }
}
