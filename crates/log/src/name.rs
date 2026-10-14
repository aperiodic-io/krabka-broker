//! Names inside a Kafka log directory: the per-partition directory, and the segment files in it.
//!
//! A partition's data lives in `<log_dir>/<topic>-<partition>/`. Kafka names each segment in that directory by its 20-digit zero-padded base offset, with the `.log`, `.index`, and `.timeindex` extensions.
//!
//! The module is public because a tool that opens a log directory needs the same paths the broker uses, and it must not have to link the broker to compute them.

use {
    crate::error::LogError,
    std::path::{Path, PathBuf},
};

/// Digit count of a segment filename stem. Kafka zero-pads the base offset to this width.
pub const FILENAME_DIGITS: usize = 20;

/// `0` → `"00000000000000000000"`. `1847` → `"00000000000000001847"`.
#[must_use]
pub fn format_base_offset(base_offset: i64) -> String { format!("{base_offset:020}") }

/// Parse a `.log` filename and return its base offset.
/// `"00000000000000001847.log"` → `Ok(1847)`.
///
/// # Errors
///
/// Returns [`LogError::BadSegmentName`] when the name has no `.log` extension, when the stem is not [`FILENAME_DIGITS`] characters long, or when the stem is not a decimal `i64`.
pub fn parse_log_filename(name: &str) -> Result<i64, LogError> {
    let stem = name
        .strip_suffix(".log")
        .ok_or_else(|| LogError::BadSegmentName(name.into()))?;
    if stem.len() != FILENAME_DIGITS {
        return Err(LogError::BadSegmentName(name.into()));
    }
    stem.parse::<i64>()
        .map_err(|_| LogError::BadSegmentName(name.into()))
}

/// Path to the `.log` file of the segment that starts at `base_offset`. It holds the record batches.
#[must_use]
pub fn log_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{}.log", format_base_offset(base_offset)))
}

/// Path to the sparse offset index, `.index`, of the segment that starts at `base_offset`.
#[must_use]
pub fn index_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{}.index", format_base_offset(base_offset)))
}

/// Path to the sparse timestamp index, `.timeindex`, of the segment that starts at `base_offset`.
#[must_use]
pub fn timeindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{}.timeindex", format_base_offset(base_offset)))
}

/// Path to the aborted-transaction index, `.txnindex`, of the segment that starts at `base_offset`.
#[must_use]
pub fn txnindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{}.txnindex", format_base_offset(base_offset)))
}

/// Path to the `.snapshot` file that holds the producer state as of `offset`.
#[must_use]
pub fn producer_snapshot_path(dir: &Path, offset: i64) -> PathBuf {
    dir.join(format!("{}.snapshot", format_base_offset(offset)))
}

/// Path to the per-segment `.stampindex` sidecar. It holds the additional
/// internal stamp coordinate and is never a client-facing file.
#[must_use]
pub fn stampindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{}.stampindex", format_base_offset(base_offset)))
}

/// Path to the per-partition `.leader-epoch-checkpoint` file.
#[must_use]
pub fn leader_epoch_checkpoint_path(dir: &Path) -> PathBuf { dir.join("leader-epoch-checkpoint") }

/// Path to the per-partition `log-start-offset-checkpoint` file.
///
/// It holds the log start offset that no segment name records: the part of a
/// trim that lands inside a segment rather than on a segment boundary.
#[must_use]
pub fn log_start_offset_checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("log-start-offset-checkpoint")
}

/// Suffix that the broker appends to a future-log partition directory while a KIP-113 intra-broker move runs.
///
/// The directory at `<target_log_dir>/<topic>-<partition><FUTURE_SUFFIX>` collects copied batches. When the future log catches up, the broker renames it in place to `<topic>-<partition>`.
///
/// This is not the name that Apache Kafka gives the directory. Kafka writes `<topic>-<partition>.<32-hex-uuid>-future` (`LocalLog.logFutureDirName`, `LogFileUtils.FUTURE_DIR_SUFFIX`), and a JVM broker cannot parse the name without the unique id. A partition has at most one move, and only the krabka broker reads the name, so the unique id is left out. `docs/format-divergences.md` lists the difference.
pub const FUTURE_SUFFIX: &str = "-future";

/// Builds the directory path for a (topic, partition).
#[must_use]
pub fn partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}"))
}

/// Parses `<topic>-<partition>` from a directory name. It returns `None` when the name does not match the pattern.
#[must_use]
pub fn parse_partition_dir(name: &str) -> Option<(String, i32)> {
    let (topic, part) = name.rsplit_once('-')?;
    if topic.is_empty() || topic.ends_with('-') {
        // Empty topic or trailing `-` in the topic indicates a malformed
        // name like "-0" or "foo--1" (which would otherwise parse the
        // tail as a positive partition number).
        return None;
    }
    let partition = part.parse::<i32>().ok()?;
    if partition < 0 {
        return None;
    }
    Some((topic.to_string(), partition))
}

#[cfg(test)]
mod tests {

    use {super::*, assert2::assert};

    fn check_offset_case(offset: i64, expected_filename: &str) {
        let formatted = format_base_offset(offset);
        assert!(formatted == expected_filename);
        let parsed = parse_log_filename(&format!("{formatted}.log")).unwrap();
        assert!(parsed == offset);
    }

    #[test]
    fn zero() { check_offset_case(0, "00000000000000000000"); }
    #[test]
    fn small() { check_offset_case(1847, "00000000000000001847"); }
    // Plan had "00000000001000000000000" (23 chars) which is wrong:
    // `{:020}` zero-pads to 20 chars, and 1_000_000_000_000 is 13 digits,
    // so we expect 7 leading zeros + "1000000000000" = 20 chars total.
    #[test]
    fn large() { check_offset_case(1_000_000_000_000, "00000001000000000000"); }

    #[test]
    fn rejects_non_log_extension() {
        assert2::assert!(parse_log_filename("00000000000000000000.index").is_err());
    }

    #[yare::parameterized(
        too_few_digits = { "123.log" },
        too_many_digits = { "000000000000000001847.log" },
    )]
    fn rejects_wrong_digit_count(filename: &str) {
        assert2::assert!(parse_log_filename(filename).is_err());
    }

    #[test]
    fn round_trip_partition_dir() {
        let p = partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert!(parse_partition_dir(name) == Some(("foo".to_string(), 7)));
        assert!(parse_partition_dir("foo-0") == Some(("foo".to_string(), 0)));
    }

    #[test]
    fn rejects_negative_partition() {
        assert!(parse_partition_dir("foo--1") == None);
    }

    #[test]
    fn rejects_empty_topic() {
        assert!(parse_partition_dir("-0") == None);
    }

    #[test]
    fn rejects_no_dash() {
        assert!(parse_partition_dir("foo") == None);
    }

    #[test]
    fn handles_topic_with_dashes() {
        // Topic names can themselves contain hyphens; rsplit takes the last.
        assert!(parse_partition_dir("my-cool-topic-3") == Some(("my-cool-topic".to_string(), 3)));
    }
}
