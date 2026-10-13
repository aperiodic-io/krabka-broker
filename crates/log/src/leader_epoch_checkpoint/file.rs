//! Reading and writing the checkpoint file itself: `open` and its strict
//! `parse`, and the atomic `flush` that every mutation ends with. The parser
//! and the writer live together because they are the two halves of one
//! byte-for-byte Kafka format, and a change to either has to move the other.

use {
    super::{EpochEntry, LeaderEpochCheckpoint, is_strict_successor},
    crate::{
        error::LogError,
        io::{IoTarget, file_io, write_atomic},
    },
    krabka_ids::{LeaderEpoch, Offset},
    std::{
        fmt::Write as _,
        fs,
        path::{Path, PathBuf},
    },
};

/// The `leader-epoch-checkpoint` format version this build writes and reads:
/// the first line of the file, as in Kafka's `CheckpointFile`.
///
/// It is part of the 1.x on-disk contract, and it is Kafka's number too:
/// `LeaderEpochCheckpointFile.CURRENT_VERSION` is 0.
pub(crate) const LEADER_EPOCH_CHECKPOINT_VERSION: i32 = 0;

/// The artifact name the version errors carry, the file's own name.
const ARTIFACT: &str = "leader-epoch-checkpoint";

impl LeaderEpochCheckpoint {
    #[krabka_macros::index_open]
    /// Open or recover the checkpoint at `path`. A missing file gives an
    /// empty checkpoint.
    /// # Errors
    /// Returns [`LogError::UnsupportedFormatVersion`] when the header line is
    /// a version other than `LEADER_EPOCH_CHECKPOINT_VERSION`,
    /// [`LogError::MissingFormatVersion`] when it is not a version at all, and
    /// an error when log I/O fails or a row is corrupt.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let entries = match fs::read_to_string(&path) {
            Ok(s) => Self::parse(&path, &s)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(LogError::Io(e)),
        };
        tracing::Span::current().record("entries", entries.len());
        Ok(Self {
            path,
            io: file_io(),
            entries,
        })
    }

    /// Parse the file's text. As Kafka's `CheckpointFile.read`, an empty
    /// file has no entries, and a header other than the current version is
    /// refused: Kafka throws "Unrecognized version of the ... file".
    fn parse(path: &Path, s: &str) -> Result<Vec<EpochEntry>, LogError> {
        let mut lines = s.lines();
        let Some(header) = lines.next() else {
            return Ok(Vec::new());
        };
        // Kafka's `toInt` takes the whole line, so neither side may carry
        // padding. A first line that is no integer is a file with no header.
        let version: i64 = header.parse().map_err(|_| LogError::MissingFormatVersion {
            artifact: ARTIFACT,
            path: path.to_path_buf(),
        })?;
        if version != i64::from(LEADER_EPOCH_CHECKPOINT_VERSION) {
            return Err(LogError::UnsupportedFormatVersion {
                artifact: ARTIFACT,
                path: path.to_path_buf(),
                found: version,
            });
        }
        let count: usize = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        // Do NOT pre-size from the untrusted `count`: a corrupt or hostile
        // checkpoint (local dir, or bytes restored from tiered storage) could
        // declare a huge count and trigger a multi-GB allocation before the
        // bounded `lines.take(count)` loop ever runs. `count` is used only to
        // bound the number of rows read; the Vec grows as entries are parsed.
        // Matches Kafka's CheckpointFile, which reads entries line-by-line.
        let mut out: Vec<EpochEntry> = Vec::new();
        for line in lines.take(count) {
            let mut parts = line.split_whitespace();
            let epoch = parse_column(&mut parts, line)?;
            let start_offset = parse_column(&mut parts, line)?;
            let entry = EpochEntry {
                epoch: LeaderEpoch(epoch),
                start_offset: Offset(start_offset),
            };
            if out
                .last()
                .is_some_and(|previous| !is_strict_successor(previous, &entry))
            {
                return Err(LogError::Corrupt(format!(
                    "leader epoch checkpoint rows are not strictly increasing: {line:?}"
                )));
            }
            out.push(entry);
        }
        Ok(out)
    }

    /// Rewrite the whole file atomically. `mutation` calls this after every
    /// change that altered the entry list.
    pub(super) fn flush(&self) -> Result<(), LogError> {
        let mut s = String::new();
        let _ = writeln!(s, "{LEADER_EPOCH_CHECKPOINT_VERSION}");
        let _ = writeln!(s, "{}", self.entries.len());
        for e in &self.entries {
            let _ = writeln!(s, "{} {}", e.epoch.0, e.start_offset.0);
        }
        let tmp = self.path.with_extension("tmp");
        write_atomic(
            &*self.io,
            IoTarget::LeaderEpochCheckpoint,
            &tmp,
            &self.path,
            s.as_bytes(),
            false,
        )
        .map_err(LogError::Io)?;
        Ok(())
    }
}

fn parse_column<T: std::str::FromStr>(
    parts: &mut std::str::SplitWhitespace<'_>,
    line: &str,
) -> Result<T, LogError> {
    parts
        .next()
        .and_then(|token| token.parse().ok())
        .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            leader_epoch_checkpoint::test_support::{TempCheckpoint, checkpoint, entry, fresh},
            test_support::TempPath,
        },
    };

    /// The golden file: Kafka's text layout, version line `0` first.
    const GOLDEN: &str = "0\n3\n0 0\n1 50\n2 100\n";

    fn golden_entries() -> Vec<EpochEntry> {
        [(0, 0), (1, 50), (2, 100)]
            .into_iter()
            .map(|(epoch, start_offset)| EpochEntry {
                epoch: LeaderEpoch(epoch),
                start_offset: Offset(start_offset),
            })
            .collect()
    }

    #[test]
    fn round_trip_byte_compat_format() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0), entry(1, 50), entry(2, 100)]);
        let s = std::fs::read_to_string(&c.path).unwrap();
        assert2::assert!(s == GOLDEN);
    }

    #[test]
    fn the_golden_file_decodes_to_its_entries() {
        let TempPath { dir: _d, path } = fresh();
        std::fs::write(&path, GOLDEN).unwrap();
        assert2::assert!(LeaderEpochCheckpoint::open(path).unwrap().entries() == golden_entries());
    }

    /// An empty file and a header with no count line both hold no entries,
    /// as Kafka's reader returns an empty list when a line runs out.
    #[test]
    fn a_file_that_ends_early_has_no_entries() {
        for contents in ["", "0\n"] {
            let TempPath { dir: _d, path } = fresh();
            std::fs::write(&path, contents).unwrap();
            let c = LeaderEpochCheckpoint::open(path).unwrap();
            assert2::assert!(c.entries() == &[][..], "{contents:?}");
        }
    }

    /// A header that is an integer other than 0 is an unrecognized version,
    /// as in Kafka. A first line that is no integer is a file without a
    /// header.
    #[test]
    fn open_refuses_an_unknown_or_missing_version() {
        for (contents, found) in [
            ("1\n3\n0 0\n1 50\n2 100\n", Some(1)),
            ("-1\n0\n", Some(-1)),
            ("99999999999\n0\n", Some(99_999_999_999)),
            ("3\n0 0\n1 50\n2 100\n", Some(3)),
            ("0 0\n1 50\n", None),
            (" 0\n0\n", None),
            ("v0\n0\n", None),
        ] {
            let TempPath { dir: _d, path } = fresh();
            std::fs::write(&path, contents).unwrap();
            let error = LeaderEpochCheckpoint::open(path.clone()).unwrap_err();
            let got = match error {
                LogError::UnsupportedFormatVersion {
                    artifact: ARTIFACT,
                    path: named,
                    found,
                } if named == path => Some(Some(found)),
                LogError::MissingFormatVersion {
                    artifact: ARTIFACT,
                    path: named,
                } if named == path => Some(None),
                _ => None,
            };
            assert2::assert!(got == Some(found), "{contents:?}");
        }
    }

    #[test]
    fn missing_file_yields_empty() {
        let TempPath { dir: _d, path } = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert2::assert!(c.entries() == &[][..]);
        assert2::assert!(c.latest_epoch() == None);
    }

    #[test]
    fn open_rejects_out_of_order_checkpoint_rows() {
        let TempPath { dir: _d, path } = fresh();
        std::fs::write(&path, "0\n4\n0 0\n5 100\n2 50\n4 80\n").unwrap();

        let error = LeaderEpochCheckpoint::open(path).unwrap_err();
        assert2::assert!(matches!(error, LogError::Corrupt(_)));
    }

    #[test]
    fn absurd_declared_count_does_not_over_allocate() {
        // Hostile/corrupt checkpoint: header declares billions of rows but only
        // one actual entry line follows. Parsing must not pre-size a giant Vec;
        // it should grow to fit the real rows and return just those.
        let s = "0\n9999999999999\n3 42\n";
        let entries = LeaderEpochCheckpoint::parse(Path::new("checkpoint"), s).unwrap();
        assert2::assert!(
            entries
                == [EpochEntry {
                    epoch: LeaderEpoch(3),
                    start_offset: Offset(42),
                }]
        );
        // `lines.take(count)` bounds reads to the available lines, so capacity
        // stays at the grown size, not the untrusted billions.
        assert2::assert!(entries.capacity() < 9_999_999_999_999);
    }
}
