//! Per-partition directory layout: `<log_dir>/<topic>-<partition>/`.
//!
//! This layout mirrors the Apache Kafka convention, so `krabka-log` can open
//! existing Kafka log directories with byte compatibility.

use std::path::{Path, PathBuf};

pub use krabka_log::name::{FUTURE_SUFFIX, parse_partition_dir, partition_dir};

use crate::error::BrokerError;

/// Builds the future-log directory path for a (topic, partition) in `log_dir`.
///
/// `AlterReplicaLogDirs` and KIP-113 use it. The future log collects copied
/// batches, and the broker renames it atomically to the canonical
/// `<topic>-<partition>` on the swap.
#[must_use]
pub fn future_partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}{FUTURE_SUFFIX}"))
}

/// Parses a `<topic>-<partition>-future` directory name back to
/// `(topic, partition)`.
///
/// It strips the [`FUTURE_SUFFIX`], then calls [`parse_partition_dir`], so
/// that the topic-name handling stays in one place. It returns `None` when the
/// name does not carry the suffix, and when the remainder after the strip is
/// not a valid partition directory.
#[must_use]
pub fn parse_future_partition_dir(name: &str) -> Option<(String, i32)> {
    let base = name.strip_suffix(FUTURE_SUFFIX)?;
    parse_partition_dir(base)
}

/// The topic of the metadata partition, `__cluster_metadata-0`. Its
/// directory sits in the metadata log directory, which can be a data
/// directory, but it is never a partition the broker hosts.
const METADATA_TOPIC: &str = "__cluster_metadata";

/// [`parse_partition_dir`] for a directory the broker hosts: a partition
/// directory of any topic but `__cluster_metadata`, which Kafka's
/// `LogManager.loadLogs` skips too.
fn parse_hosted_partition_dir(name: &str) -> Option<(String, i32)> {
    parse_partition_dir(name).filter(|(topic, _)| topic != METADATA_TOPIC)
}

/// Walks `log_dir` and returns every `(topic, partition)` whose directory
/// exists. Broker startup uses it to refill the metadata image and the
/// partition registry from what the last run left on disk. The metadata
/// partition `__cluster_metadata-0` is not one of them.
pub fn scan(log_dir: &Path) -> Result<Vec<(String, i32)>, BrokerError> {
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir)?;
        return Ok(Vec::new());
    }
    scan_existing(log_dir, parse_hosted_partition_dir)
}

/// Walks `log_dir` and returns every `(topic, partition)` that has a
/// future-log directory, `<topic>-<partition>-future`.
///
/// `DescribeLogDirs` uses it to report KIP-113 moves that are still running,
/// with `is_future_key=true`. Broker startup uses it to resume a move that a
/// crash interrupted.
pub fn scan_future(log_dir: &Path) -> Result<Vec<(String, i32)>, BrokerError> {
    if !log_dir.exists() {
        return Ok(Vec::new());
    }
    scan_existing(log_dir, parse_future_partition_dir)
}

fn scan_existing(
    log_dir: &Path,
    parse_dir_name: fn(&str) -> Option<(String, i32)>,
) -> Result<Vec<(String, i32)>, BrokerError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(log_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if let Some((topic, partition)) = parse_dir_name(&name) {
            out.push((topic, partition));
        }
    }
    out.sort();
    Ok(out)
}

/// Counts the partition subdirectories, `<topic>-<partition>/`, directly under
/// `dir`.
///
/// Least-loaded JBOD placement uses this count. A missing directory counts as
/// zero. This function ignores non-partition entries such as the metadata
/// partition `__cluster_metadata-0` and stray files.
#[must_use]
pub fn count_partitions(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| parse_hosted_partition_dir(name).is_some())
        .count()
}

/// Resolves the directory that a `(topic, partition)` should live in across a
/// JBOD set of `log_dirs`, which is KIP-113 placement. It returns the full
/// `<dir>/<topic>-<partition>` path.
///
/// - When the partition directory already exists under one of `log_dirs`, that
///   existing location wins. This makes the function idempotent, and it covers
///   restart, recovery, and concurrent re-materialization.
/// - In every other case the function puts the partition in the least-loaded
///   directory, which is the one with the fewest partition subdirs. It breaks
///   a tie by `log_dirs` order. This mirrors the round-robin-by-count default
///   in Kafka's `LogManager`.
///
/// `log_dirs` must not be empty. The caller guarantees this through
/// [`crate::BrokerConfig::all_log_dirs`], which always includes the primary
/// `log_dir`.
#[must_use]
pub fn place_partition_dir(log_dirs: &[PathBuf], topic: &str, partition: i32) -> PathBuf {
    assert2::assert!(!log_dirs.is_empty(), "log_dirs must be non-empty");
    if let Some(existing) = partition_dir_in(log_dirs, topic, partition) {
        return existing;
    }

    // Least-loaded placement, ties broken by order.
    let chosen = log_dirs
        .iter()
        .min_by_key(|dir| count_partitions(dir))
        .unwrap_or(&log_dirs[0]);
    partition_dir(chosen, topic, partition)
}

/// [`place_partition_dir`] with KIP-1066 cordoned directories: an existing
/// location in any of `log_dirs` still wins, and a new partition goes to the
/// least-loaded directory `cordoned` leaves open, as Kafka's
/// `LogManager.nextLogDirs` filters the cordoned ones out.
#[must_use]
pub(crate) fn place_partition_dir_avoiding(
    log_dirs: &[PathBuf],
    cordoned: &crate::cordoned_log_dirs::CordonedLogDirs,
    topic: &str,
    partition: i32,
) -> PathBuf {
    let existing = partition_dir_in(log_dirs, topic, partition);
    existing.unwrap_or_else(|| {
        place_partition_dir(&cordoned.placement_dirs(log_dirs), topic, partition)
    })
}

/// The `<dir>/<topic>-<partition>` path of the first directory in `log_dirs`
/// that already holds the partition.
fn partition_dir_in(log_dirs: &[PathBuf], topic: &str, partition: i32) -> Option<PathBuf> {
    log_dirs
        .iter()
        .map(|dir| partition_dir(dir, topic, partition))
        .find(|candidate| candidate.exists())
}

/// Scans every directory in `log_dirs` and returns each discovered
/// `(topic, partition, owning_dir)`.
///
/// When the same partition appears in more than one directory, which is a
/// misconfiguration, the first occurrence wins and this function logs a
/// warning. It sorts the results by `(topic, partition)`.
pub fn scan_all(log_dirs: &[PathBuf]) -> Result<Vec<(String, i32, PathBuf)>, BrokerError> {
    use std::collections::HashMap;
    let mut seen: HashMap<(String, i32), PathBuf> = HashMap::new();
    for dir in log_dirs {
        for (topic, partition) in scan(dir)? {
            match seen.entry((topic.clone(), partition)) {
                std::collections::hash_map::Entry::Occupied(existing) => {
                    tracing::warn!(
                        topic = %topic, partition,
                        first = %existing.get().display(),
                        duplicate = %dir.display(),
                        "partition present in multiple log dirs; ignoring duplicate"
                    );
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(dir.clone());
                }
            }
        }
    }
    let mut out: Vec<(String, i32, PathBuf)> =
        seen.into_iter().map(|((t, p), dir)| (t, p, dir)).collect();
    out.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;

    fn partition_dirs(names: &[&str]) -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        for name in names {
            std::fs::create_dir(dir.path().join(name)).unwrap();
        }
        dir
    }

    #[test]
    fn scan_creates_dir_when_missing() {
        let dir = tempdir().expect("tempdir");
        let log_dir = dir.path().join("does-not-exist");
        let out = scan(&log_dir).expect("scan ok");
        assert!(out.is_empty());
        assert!(log_dir.exists());
    }

    #[test]
    fn scan_returns_existing_partitions() {
        let dir = tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("foo-0")).expect("mkdir foo-0");
        std::fs::create_dir(dir.path().join("foo-1")).expect("mkdir foo-1");
        std::fs::create_dir(dir.path().join("bar-0")).expect("mkdir bar-0");
        std::fs::create_dir(dir.path().join("not_a_partition")).expect("mkdir other");
        // The metadata partition of a metadata log directory that is also a
        // data directory.
        std::fs::create_dir(dir.path().join("__cluster_metadata-0")).expect("mkdir metadata");
        let mut out = scan(dir.path()).expect("scan ok");
        out.sort();
        assert!(out == vec![("bar".into(), 0), ("foo".into(), 0), ("foo".into(), 1),]);
    }

    #[test]
    fn count_partitions_ignores_non_partition_entries() {
        let dir = tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("foo-0")).unwrap();
        std::fs::create_dir(dir.path().join("foo-1")).unwrap();
        std::fs::create_dir(dir.path().join("__cluster_metadata-0")).unwrap();
        std::fs::write(dir.path().join("bootstrap.json"), b"{}").unwrap();
        assert!(count_partitions(dir.path()) == 2);
    }

    #[test]
    fn count_partitions_missing_dir_is_zero() {
        let dir = tempdir().expect("tempdir");
        assert!(count_partitions(&dir.path().join("nope")) == 0);
    }

    #[test]
    fn place_reuses_existing_location() {
        let (_a, b, dirs) = crate::test_support::two_log_dirs();
        // Pre-create the partition in the *second* dir.
        std::fs::create_dir(b.path().join("t-0")).unwrap();
        let placed = place_partition_dir(&dirs, "t", 0);
        assert!(placed == b.path().join("t-0"));
    }

    #[test]
    fn place_picks_least_loaded_then_order() {
        let (a, b, dirs) = crate::test_support::two_log_dirs();
        // Empty cluster: tie → first dir.
        assert!(place_partition_dir(&dirs, "t", 0) == a.path().join("t-0"));
        // Load `a` with two partitions; next placement should go to `b`.
        std::fs::create_dir(a.path().join("t-0")).unwrap();
        std::fs::create_dir(a.path().join("t-1")).unwrap();
        assert!(place_partition_dir(&dirs, "t", 2) == b.path().join("t-2"));
    }

    /// A new partition skips a cordoned directory even when it is the least
    /// loaded, and a partition that already lives in one stays there.
    #[test]
    fn placement_avoids_cordoned_dirs_but_keeps_existing_partitions() {
        let (a, b, dirs) = crate::test_support::two_log_dirs();
        std::fs::create_dir(b.path().join("t-0")).unwrap();
        let cordoned = crate::cordoned_log_dirs::CordonedLogDirs::new(
            dirs.clone(),
            Some(a.path().display().to_string()),
        );
        let cases = [
            ("a new partition", 1, b.path().join("t-1")),
            ("an existing partition", 0, b.path().join("t-0")),
        ];
        for (label, partition, want) in cases {
            check!(
                place_partition_dir_avoiding(&dirs, &cordoned, "t", partition) == want,
                "{label}"
            );
        }
        std::fs::create_dir(a.path().join("u-0")).unwrap();
        let everything =
            crate::cordoned_log_dirs::CordonedLogDirs::new(dirs.clone(), Some("*".into()));
        check!(place_partition_dir_avoiding(&dirs, &everything, "u", 0) == a.path().join("u-0"));
        check!(place_partition_dir_avoiding(&dirs, &everything, "t", 1) == a.path().join("t-1"));
    }

    #[test]
    fn scan_all_merges_dirs_and_sorts() {
        let a = partition_dirs(&["foo-0"]);
        let b = partition_dirs(&["bar-1"]);
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let out = scan_all(&dirs).expect("scan_all ok");
        assert!(
            out == vec![
                ("bar".to_string(), 1, b.path().to_path_buf()),
                ("foo".to_string(), 0, a.path().to_path_buf()),
            ]
        );
    }

    #[test]
    fn future_partition_dir_round_trips() {
        let p = future_partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert!(name == "foo-7-future");
        assert!(parse_future_partition_dir(name) == Some(("foo".to_string(), 7)));
    }

    #[test]
    fn parse_future_rejects_non_future_name() {
        // Plain partition dir has no `-future` suffix.
        assert!(parse_future_partition_dir("foo-7") == None);
        // Suffix present but the remainder isn't a partition dir.
        assert!(parse_future_partition_dir("garbage-future") == None);
    }

    #[test]
    fn scan_does_not_pick_up_future_dirs() {
        let dir = partition_dirs(&["foo-0", "foo-1-future"]);
        let out = scan(dir.path()).expect("scan ok");
        assert!(out == vec![("foo".into(), 0)]);
    }

    #[test]
    fn scan_future_returns_only_future_dirs() {
        let dir = partition_dirs(&["foo-0", "foo-1-future"]);
        std::fs::create_dir(dir.path().join("bar-3-future")).unwrap();
        let mut out = scan_future(dir.path()).expect("scan_future ok");
        out.sort();
        assert!(out == vec![("bar".into(), 3), ("foo".into(), 1)]);
    }

    #[test]
    fn scan_future_missing_dir_is_empty() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(scan_future(&missing).expect("ok").is_empty());
    }

    #[test]
    fn count_partitions_ignores_future_dirs() {
        // KIP-113 placement should not see future-log dirs as load —
        // they are transient state belonging to an in-flight move.
        let dir = partition_dirs(&["foo-0", "foo-1-future"]);
        assert!(count_partitions(dir.path()) == 1);
    }

    #[test]
    fn scan_all_first_dir_wins_on_duplicate() {
        let a = partition_dirs(&["foo-0"]);
        let b = partition_dirs(&["foo-0"]);
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let out = scan_all(&dirs).expect("scan_all ok");
        assert!(out == vec![("foo".to_string(), 0, a.path().to_path_buf())]);
    }
}
