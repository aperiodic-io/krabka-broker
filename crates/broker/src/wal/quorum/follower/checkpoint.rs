//! The durable-offset checkpoint that the WAL follower keeps beside its log.
//! It records the offset range this broker has fsynced, and it is what recovery
//! uses to discard an uncertain suffix after a restart. The write goes through a
//! temporary file and a backup copy, so a crash in the middle of the rename
//! still leaves one readable checkpoint behind.

use std::{io::Write as _, path::Path};

use krabka_ids::Offset;
use krabka_log::Log;

pub(super) const DURABLE_OFFSET_FILE: &str = "wal-durable-offset.checkpoint";
const DURABLE_OFFSET_BACKUP_FILE: &str = "wal-durable-offset.checkpoint.bak";

/// Version of `wal-durable-offset.checkpoint` and its `.bak`, the file's first
/// line, as in Kafka's own checkpoint files. The second line is
/// `<start> <end>`.
///
/// Part of the 1.x on-disk contract: a 1.x broker reads every checkpoint that
/// any earlier 1.x broker wrote.
pub(super) const DURABLE_OFFSET_VERSION: i16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DurableRange {
    pub(super) start: Offset,
    pub(super) end: Offset,
}

/// Why a durable-offset checkpoint could not be read.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum DurableOffsetDecodeError {
    /// The first line holds the two offsets, as a checkpoint written before
    /// 1.0 does, with no version line above them.
    #[error(
        "the checkpoint has no version line: it predates krabka 1.0, whose data a 1.x broker \
         does not read; reformat this node"
    )]
    MissingVersion,
    /// The version line names a version this build does not read.
    #[error(
        "unsupported checkpoint version {found:?}: this build reads version \
         {DURABLE_OFFSET_VERSION}"
    )]
    UnsupportedVersion {
        /// The version line as it was found.
        found: String,
    },
    /// The offsets line is not two offsets.
    #[error("expected two offsets: {0}")]
    Malformed(String),
}

fn decode_durable_offset(text: &str) -> Result<DurableRange, DurableOffsetDecodeError> {
    let mut lines = text.lines();
    let version = lines.next().unwrap_or_default().trim();
    if version.split_ascii_whitespace().count() > 1 {
        return Err(DurableOffsetDecodeError::MissingVersion);
    }
    if version.parse::<i16>() != Ok(DURABLE_OFFSET_VERSION) {
        return Err(DurableOffsetDecodeError::UnsupportedVersion {
            found: version.to_owned(),
        });
    }
    let offsets = lines
        .next()
        .unwrap_or_default()
        .split_ascii_whitespace()
        .map(str::parse::<i64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DurableOffsetDecodeError::Malformed(error.to_string()))?;
    let trailing = lines.any(|line| !line.trim().is_empty());
    match (offsets.as_slice(), trailing) {
        ([start, end], false) => Ok(DurableRange {
            start: Offset(*start),
            end: Offset(*end),
        }),
        _ => Err(DurableOffsetDecodeError::Malformed(format!("{text:?}"))),
    }
}

pub(super) fn recover_durable_offset(log: &mut Log, path: &Path) -> Result<(), crate::BrokerError> {
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let checkpoint = if path.exists() {
        Some(path)
    } else if backup.exists() {
        Some(backup.as_path())
    } else {
        None
    };
    let durable = checkpoint.map_or_else(
        || {
            Ok(DurableRange {
                start: log.log_start_offset(),
                end: log.log_start_offset(),
            })
        },
        |checkpoint| {
            // The backup is read under the same rules as the primary: a backup
            // of a version this build does not read is an error, never a
            // reason to fall back to the log start.
            let value = std::fs::read_to_string(checkpoint)?;
            decode_durable_offset(&value).map_err(|error| {
                crate::BrokerError::Replication(format!(
                    "decode WAL durable offsets {}: {error}",
                    checkpoint.display()
                ))
            })
        },
    )?;
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    let (true, true) = (
        (start..=end).contains(&durable.start),
        (durable.start..=end).contains(&durable.end),
    ) else {
        return Err(crate::BrokerError::Replication(format!(
            "WAL durable range {}..{} is outside recovered range {}..{}",
            durable.start.0, durable.end.0, start.0, end.0
        )));
    };
    let observed_last = if durable.start == durable.end {
        None
    } else if durable.end == end {
        Some(end.0 - 1)
    } else {
        // ponytail: scans one segment's tail; use a header-only boundary reader
        // if large partial-checkpoint recovery scans become costly.
        log.read_raw(Offset(durable.end.0 - 1), durable.end, log.size())?
            .last_offset
            .map(|offset| offset.0)
    };
    if !krabka_verified::wal::wal_checkpoint_range_valid(
        start.0,
        end.0,
        durable.start.0,
        durable.end.0,
        observed_last,
    ) {
        return Err(crate::BrokerError::Replication(format!(
            "WAL durable range {}..{} does not end at a whole-batch boundary",
            durable.start.0, durable.end.0
        )));
    }
    if durable.start == durable.end {
        log.reset_to(durable.start)?;
    } else {
        log.truncate_to(durable.end)?;
        log.trim_to_offset(durable.start)?;
    }
    log.sync()?;
    write_durable_offset(path, durable)?;
    Ok(())
}

pub(super) fn write_durable_offset(
    path: &Path,
    durable: DurableRange,
) -> Result<(), crate::BrokerError> {
    let temporary = path.with_extension("checkpoint.tmp");
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let mut file = std::fs::File::create(&temporary)?;
    write!(file, "{}", encode_durable_offset(durable))?;
    file.sync_all()?;
    drop(file);
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    if path.exists() {
        std::fs::rename(path, &backup)?;
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        restore_durable_offset_backup(path, &backup);
        return Err(error.into());
    }
    if backup.exists() {
        std::fs::remove_file(backup)?;
    }
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn encode_durable_offset(durable: DurableRange) -> String {
    format!(
        "{DURABLE_OFFSET_VERSION}\n{} {}\n",
        durable.start.0, durable.end.0
    )
}

fn restore_durable_offset_backup(path: &Path, backup: &Path) {
    let (Ok(false), Ok(true)) = (path.try_exists(), backup.try_exists()) else {
        return;
    };
    let _ = std::fs::rename(backup, path);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::LogConfig;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;

    /// The exact bytes a 1.x broker writes for the range `3..7`. A change
    /// here is a change to the 1.x on-disk contract.
    const GOLDEN_CHECKPOINT: &str = "0\n3 7\n";

    fn empty_checkpoint_log() -> (tempfile::TempDir, std::path::PathBuf, Log) {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = dir.path().join(DURABLE_OFFSET_FILE);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        (dir, checkpoint, log)
    }

    fn checkpoint_log(
        records: crate::test_support::RecordCount,
    ) -> (tempfile::TempDir, std::path::PathBuf, Log) {
        let (dir, checkpoint, mut log) = empty_checkpoint_log();
        let mut batch = crate::wal::quorum::test_support::batch(records.0);
        log.append(&mut batch).unwrap();
        (dir, checkpoint, log)
    }

    #[test]
    fn durable_offset_checkpoint_matches_the_golden_bytes() {
        let range = DurableRange {
            start: Offset(3),
            end: Offset(7),
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DURABLE_OFFSET_FILE);

        write_durable_offset(&path, range).unwrap();

        assert!(std::fs::read_to_string(&path).unwrap() == GOLDEN_CHECKPOINT);
        assert!(decode_durable_offset(GOLDEN_CHECKPOINT) == Ok(range));
    }

    #[test]
    fn durable_offset_decode_rejects_a_missing_or_unknown_version() {
        for (name, text, expected) in [
            (
                "pre-1.0 checkpoint, offsets on the first line",
                "3 7\n",
                DurableOffsetDecodeError::MissingVersion,
            ),
            (
                "future version",
                "1\n3 7\n",
                DurableOffsetDecodeError::UnsupportedVersion {
                    found: "1".to_owned(),
                },
            ),
            (
                "version that is not a number",
                "v0\n3 7\n",
                DurableOffsetDecodeError::UnsupportedVersion {
                    found: "v0".to_owned(),
                },
            ),
            (
                "empty file",
                "",
                DurableOffsetDecodeError::UnsupportedVersion {
                    found: String::new(),
                },
            ),
        ] {
            assert!(decode_durable_offset(text) == Err(expected), "case {name}");
        }
    }

    #[test]
    fn durable_offset_recovery_refuses_a_primary_or_backup_of_an_unknown_version() {
        for file in [DURABLE_OFFSET_FILE, DURABLE_OFFSET_BACKUP_FILE] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            std::fs::write(dir.path().join(file), "1\n0 0\n").unwrap();

            let error = recover_durable_offset(&mut log, &dir.path().join(DURABLE_OFFSET_FILE))
                .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("unsupported checkpoint version \"1\""),
                "{file}: {error}"
            );
        }
    }

    #[test]
    fn durable_offset_backup_is_restored_only_when_primary_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DURABLE_OFFSET_FILE);
        let backup = dir.path().join(DURABLE_OFFSET_BACKUP_FILE);
        std::fs::write(&backup, "0 4\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(!backup.exists());
        std::fs::write(&backup, "0 3\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(std::fs::read_to_string(&backup).unwrap() == "0 3\n");
    }

    #[test]
    fn follower_recovery_discards_a_suffix_beyond_the_durable_checkpoint() {
        let (dir, checkpoint, mut log) = empty_checkpoint_log();
        let mut durable = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut durable).unwrap();
        log.sync().unwrap();
        write_durable_offset(
            &checkpoint,
            DurableRange {
                start: Offset(0),
                end: Offset(1),
            },
        )
        .unwrap();
        let mut uncertain = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut uncertain).unwrap();
        assert2::assert!((log.log_end_offset()) == (Offset(2)));
        drop(log);

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &checkpoint).unwrap();

        assert2::assert!((reopened.log_end_offset()) == (Offset(1)));
        assert2::assert!((std::fs::read_to_string(checkpoint).unwrap()) == ("0\n0 1\n"));
    }

    #[test]
    fn follower_checkpoint_recovery_respects_whole_batches_and_interior_floors() {
        for (value, accepted, expected_start, expected_end) in [
            (Some("0\n0 1\n"), false, 0, 3),
            (Some("0\n1 2\n"), false, 0, 3),
            (Some("0\n1 3\n"), true, 1, 3),
            (Some("0\n1 1\n"), true, 1, 1),
            (None, true, 1, 1),
        ] {
            let (dir, checkpoint, mut log) = checkpoint_log(crate::test_support::RecordCount(3));
            if let Some(value) = value {
                std::fs::write(&checkpoint, value).unwrap();
            } else {
                log.trim_to_offset(Offset(1)).unwrap();
            }
            log.sync().unwrap();
            let before = log
                .read_raw(log.log_start_offset(), Offset(3), krabka_units::bytes(1))
                .unwrap()
                .bytes;

            assert!(
                recover_durable_offset(&mut log, &checkpoint).is_ok() == accepted,
                "checkpoint {value:?}"
            );
            assert!(log.log_start_offset() == Offset(expected_start));
            assert!(log.log_end_offset() == Offset(expected_end));
            if accepted {
                drop(log);
                let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
                recover_durable_offset(&mut reopened, &checkpoint).unwrap();
                assert!(reopened.log_start_offset() == Offset(expected_start));
                assert!(reopened.log_end_offset() == Offset(expected_end));
            } else {
                assert!(
                    log.read_raw(Offset(0), Offset(3), krabka_units::bytes(1))
                        .unwrap()
                        .bytes
                        == before
                );
                assert!(std::fs::read_to_string(&checkpoint).unwrap() == value.unwrap());
            }
        }
    }

    #[test]
    fn follower_recovery_rejects_incomplete_and_invalid_durable_ranges() {
        for (checkpoint_value, expected_error) in [
            ("0\n1\n", "expected two offsets"),
            ("0\n-1 0\n", "outside recovered range"),
            ("0\n1 0\n", "outside recovered range"),
            ("0\n0 2\n", "outside recovered range"),
            ("0 1\n", "predates krabka 1.0"),
            ("1\n0 1\n", "unsupported checkpoint version \"1\""),
        ] {
            let (_dir, checkpoint, mut log) = checkpoint_log(crate::test_support::RecordCount(1));
            log.sync().unwrap();
            std::fs::write(&checkpoint, checkpoint_value).unwrap();

            let error = recover_durable_offset(&mut log, &checkpoint).unwrap_err();

            assert!(
                error.to_string().contains(expected_error),
                "checkpoint {checkpoint_value:?}: {error}"
            );
            assert!(log.log_start_offset() == Offset(0));
            assert!(log.log_end_offset() == Offset(1));
        }
    }
}
