//! Pure-logic helper that sums the regular-file sizes inside a partition
//! directory. It returns 0 for a missing directory, which it treats as not yet
//! materialized and not as an error. It propagates IO errors for every other
//! failure mode.

use {
    krabka_log::name::parse_log_filename,
    std::{fs, io, path::Path},
};

/// Sums every regular file in a partition directory: the disk the partition
/// uses.
///
/// # Errors
/// Returns an error when the directory cannot be read.
pub fn sum_partition_dir(path: &Path) -> Result<u64, io::Error> { sum_files(path, |_| true) }

/// Sums the `.log` segment files of a partition directory, and nothing else:
/// Kafka's `UnifiedLog.size`, which `DescribeLogDirs` reports as
/// `PartitionSize`. The indexes, the producer snapshots, the leader-epoch
/// checkpoint and `partition.metadata` do not count, so an empty partition is
/// 0.
///
/// # Errors
/// Returns an error when the directory cannot be read.
pub fn sum_log_segments(path: &Path) -> Result<u64, io::Error> {
    sum_files(path, |name| parse_log_filename(name).is_ok())
}

fn sum_files(path: &Path, keep: impl Fn(&str) -> bool) -> Result<u64, io::Error> {
    let entries = match fs::read_dir(path) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut total: u64 = 0;
    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() && entry.file_name().to_str().is_some_and(&keep) {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::io::Write};

    #[test]
    fn empty_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(sum_partition_dir(tmp.path()).unwrap() == 0);
    }

    #[test]
    fn missing_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        assert!(sum_partition_dir(&missing).unwrap() == 0);
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        // Drop closes the handle so Windows updates the directory metadata
        // before `sum_partition_dir` walks it.
    }

    #[test]
    fn sums_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        write_file(&tmp.path().join("00000000000000000000.log"), &[0u8; 1024]);
        write_file(&tmp.path().join("00000000000000000000.index"), &[0u8; 128]);
        write_file(&tmp.path().join("leader-epoch-checkpoint"), &[0u8; 32]);
        assert!(sum_partition_dir(tmp.path()).unwrap() == 1024 + 128 + 32);
    }

    #[test]
    fn ignores_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("subdir")).unwrap();
        write_file(&tmp.path().join("subdir/inner.log"), &[0u8; 999]);
        write_file(&tmp.path().join("top.log"), &[0u8; 100]);
        assert!(sum_partition_dir(tmp.path()).unwrap() == 100);
    }

    /// `PartitionSize` is the segment bytes, as Kafka's `UnifiedLog.size` counts
    /// them: the `.log` files, including the active segment, and no index,
    /// snapshot, checkpoint, metadata file, or file of another name.
    #[test]
    fn log_segment_sum_counts_only_the_segment_files() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(sum_log_segments(tmp.path()).unwrap() == 0);
        write_file(&tmp.path().join("00000000000000000000.log"), &[0u8; 1024]);
        write_file(&tmp.path().join("00000000000000000100.log"), &[0u8; 512]);
        for (name, len) in [
            ("00000000000000000000.index", 128),
            ("00000000000000000000.timeindex", 96),
            ("00000000000000000000.txnindex", 8),
            ("00000000000000000100.snapshot", 40),
            ("00000000000000000100.log.swap", 77),
            ("00000000000000000100.log.deleted", 66),
            ("leader-epoch-checkpoint", 32),
            ("partition.metadata", 24),
            ("stray.log", 5),
        ] {
            write_file(&tmp.path().join(name), &vec![0u8; len]);
        }
        std::fs::create_dir(tmp.path().join("00000000000000000200.log")).unwrap();

        assert!(sum_log_segments(tmp.path()).unwrap() == 1024 + 512);
        assert!(sum_partition_dir(tmp.path()).unwrap() > 1024 + 512);
        assert!(sum_log_segments(&tmp.path().join("nope")).unwrap() == 0);
    }
}
