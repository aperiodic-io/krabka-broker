//! What the handler reports about a log directory itself: its absolute path,
//! its filesystem capacity, whether it is cordoned, and the result entry an
//! offline directory gets.
//!
//! These answers are about the directory rather than about the partitions
//! inside it, and two of them are platform-specific, so they sit apart from the
//! scan loop.

use {
    crate::{codes, cordoned_log_dirs::CordonedLogDirs},
    krabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResult,
};

/// The directories `DescribeLogDirs` reports with `is_cordoned = true`.
///
/// Kafka's `ReplicaManager.describeLogDirs` reports the cordoned set only when
/// `metadata.version` is at least `4.3-IV0`
/// (`MetadataVersion.isCordonedLogDirsSupported`), and none below it.
pub(super) fn cordoned_log_dirs_reported(
    image: &krabka_metadata::MetadataImage,
    cordoned: &CordonedLogDirs,
) -> Vec<std::path::PathBuf> {
    let supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL
    });
    if supported {
        cordoned.cordoned()
    } else {
        Vec::new()
    }
}

pub(super) fn offline_result(dir: &std::path::Path) -> DescribeLogDirsResult {
    DescribeLogDirsResult {
        error_code: codes::KAFKA_STORAGE_ERROR,
        log_dir: absolute_path(dir),
        topics: Vec::new(),
        total_bytes: -1,
        usable_bytes: -1,
        ..Default::default()
    }
}

/// The absolute path string for a log dir, as Kafka's
/// `Paths.get(logDir).toAbsolutePath` gives it: relative to the working
/// directory, without resolving symbolic links, and with the redundant
/// separators `Paths.get` drops.
pub(super) fn absolute_path(dir: &std::path::Path) -> String {
    std::path::absolute(dir)
        .unwrap_or_else(|_| dir.to_path_buf())
        .components()
        .collect::<std::path::PathBuf>()
        .display()
        .to_string()
}

/// `(total_bytes, usable_bytes)` for the filesystem that hosts `dir`.
///
/// The pair matches the KIP-827 `DescribeLogDirsResult` v4 fields.
/// `total_bytes` is the capacity of the filesystem. `usable_bytes` is the space
/// available to a non-root caller, so it respects the typical 5 % root reserve.
///
/// Returns `(-1, -1)`, the Kafka "unknown" sentinel, when the platform has no
/// `statvfs`, as on Windows, or when the syscall fails. The syscall fails when
/// the path vanishes during a reconfigure, or on a permission error. The JVM
/// admin tools tolerate `-1` and skip the column.
pub(super) fn log_dir_capacity(dir: &std::path::Path) -> (i64, i64) {
    disk_stats(dir).map_or((-1, -1), |stats| (stats.total, stats.usable))
}

/// The capacity of a filesystem, in bytes.
struct DiskStats {
    total: i64,
    usable: i64,
}

#[cfg(unix)]
fn disk_stats(dir: &std::path::Path) -> Option<DiskStats> {
    let stat = rustix::fs::statvfs(dir).ok()?;
    // `f_frsize` is the fragment size in bytes; multiplying by the
    // block counts yields capacity in bytes. Both fields come back as
    // `u64`; clamp to `i64::MAX` rather than overflow on a hypothetical
    // exabyte-scale volume.
    let frsize = i64::try_from(stat.f_frsize).unwrap_or(i64::MAX);
    let total = i64::try_from(stat.f_blocks)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    let usable = i64::try_from(stat.f_bavail)
        .unwrap_or(i64::MAX)
        .saturating_mul(frsize);
    Some(DiskStats { total, usable })
}

#[cfg(not(unix))]
fn disk_stats(_dir: &std::path::Path) -> Option<DiskStats> { None }

// Both tests below are unix-only, so the module itself is gated: on another
// platform there would be no test left to use the `assert2` imports.
#[cfg(all(test, unix))]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    /// On unix, `statvfs` against any tempdir must return sensible positive
    /// numbers.
    ///
    /// The rule is `total_bytes >= usable_bytes > 0`. This test catches a
    /// regression in the multiplication of the fragment size by the block
    /// count. Such a regression reports zeros silently. The Kafka tools then
    /// display "0 B free", and operators chase a problem that does not exist.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_sensible_unix_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let (total, usable) = log_dir_capacity(tmp.path());
        check!(
            total > 0,
            "total_bytes must be positive on unix tempdir, got {total}"
        );
        check!(
            usable > 0,
            "usable_bytes must be positive on unix tempdir, got {usable}"
        );
        check!(
            total >= usable,
            "total_bytes ({total}) must be ≥ usable_bytes ({usable})",
        );
    }

    /// A vanished path gives the Kafka "unknown" sentinel and not the syscall
    /// error.
    ///
    /// Operators see `-1`, and the JVM tool skips the column. The alternative
    /// is a `KafkaStorageException`, much like a 500, which would block the
    /// whole describe.
    #[cfg(unix)]
    #[test]
    fn log_dir_capacity_returns_minus_one_for_missing_path() {
        let phantom = std::path::Path::new("/nonexistent/krabka/test/dir/should/not/exist");
        assert!(log_dir_capacity(phantom) == (-1, -1));
    }

    /// `Paths.get(logDir).toAbsolutePath` keeps a symbolic link and drops a
    /// trailing separator; it does not resolve the link as `canonicalize`
    /// does.
    #[test]
    fn absolute_path_keeps_the_configured_link() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("real");
        std::fs::create_dir(&target).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let with_slash = format!("{}/", link.display());

        for configured in [link.display().to_string(), with_slash] {
            check!(
                absolute_path(std::path::Path::new(&configured)) == link.display().to_string(),
                "{configured}"
            );
        }
    }
}
