//! Kafka's `MetadataLogConfig`: where the metadata log lives, how its
//! segments roll, how long its prefix stays once a snapshot covers it, and how
//! often an idle leader writes to it.
//!
//! The metadata log is the single partition `__cluster_metadata-0`. Kafka keeps
//! it in a directory of that name under `metadata.log.dir`, beside its
//! segments, its KIP-630 snapshots and its `quorum-state` file, and krabka
//! keeps the same layout, so the JVM tools and Kafka's system tests find the
//! files where they look for them.

use {
    krabka_units::{
        fmt::Human as _,
        prelude::{ByteSize, Time, days, gibibytes, mebibytes, millis},
    },
    std::path::{Path, PathBuf},
};

/// The name of the metadata partition's directory under the metadata log
/// directory: Kafka's `UnifiedLog.logDirName` of `__cluster_metadata-0`.
pub const METADATA_PARTITION_DIR: &str = "__cluster_metadata-0";

/// `metadata.log.segment.bytes` default: 1 GiB.
pub const DEFAULT_METADATA_LOG_SEGMENT_SIZE: ByteSize = gibibytes(1);

/// The least `metadata.log.segment.bytes` Kafka accepts: 8 MiB. Kafka's
/// `ConfigDef` refuses a smaller value, and only its internal
/// `internal.metadata.log.segment.bytes` goes below it, for tests.
pub const MIN_METADATA_LOG_SEGMENT_SIZE: ByteSize = mebibytes(8);

/// `metadata.log.segment.ms` default: seven days.
pub const DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL: Time = days(7);

/// `metadata.max.retention.bytes` default: 100 MiB.
pub const DEFAULT_METADATA_MAX_RETENTION_SIZE: ByteSize = mebibytes(100);

/// `metadata.max.retention.ms` default: seven days.
pub const DEFAULT_METADATA_MAX_RETENTION: Time = days(7);

/// `metadata.max.idle.interval.ms` default: 500 ms.
pub const DEFAULT_METADATA_MAX_IDLE_INTERVAL: Time = millis(500);

/// The metadata partition directory under `metadata_log_dir`, Kafka's
/// `metadata.log.dir`. It holds the metadata log segments, the KIP-630
/// `<end_offset>-<epoch>.checkpoint` snapshots and the `quorum-state` file.
#[must_use]
pub fn metadata_partition_dir(metadata_log_dir: &Path) -> PathBuf {
    metadata_log_dir.join(METADATA_PARTITION_DIR)
}

/// How the metadata log rolls its segments and when it gives up the prefix
/// that a snapshot covers. The defaults are Kafka's.
///
/// Quantities render in the operator form (`1GiB`, `7d`), as the rest of
/// `ControllerConfig`'s `Debug` does.
#[derive(Clone, Copy, PartialEq, derive_more::Debug, krabka_macros::FieldDefaults)]
pub struct MetadataLogConfig {
    /// `metadata.log.segment.bytes`: the active segment rolls before an
    /// append that would take it past this size.
    #[debug("{:?}", segment_size.human().to_string())]
    #[default(DEFAULT_METADATA_LOG_SEGMENT_SIZE)]
    pub segment_size: ByteSize,
    /// `metadata.log.segment.ms`: the active segment rolls before an append
    /// that comes this long after the segment's first record.
    #[debug("{:?}", segment_roll_interval.human().to_string())]
    #[default(DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL)]
    pub segment_roll_interval: Time,
    /// `metadata.max.retention.bytes`: once the log and its snapshots together
    /// are larger than this, the oldest snapshot goes and the log start moves
    /// up to the next one. `None` is Kafka's negative value, which keeps every
    /// snapshot whatever the size.
    #[debug("{:?}", max_retention_size.map(|size| size.human().to_string()))]
    #[default(Some(DEFAULT_METADATA_MAX_RETENTION_SIZE))]
    pub max_retention_size: Option<ByteSize>,
    /// `metadata.max.retention.ms`: a snapshot whose last record is older than
    /// this goes, and the log start moves up to the next one. `None` is
    /// Kafka's negative value, which keeps every snapshot whatever its age.
    #[debug("{:?}", max_retention.map(|time| time.human().to_string()))]
    #[default(Some(DEFAULT_METADATA_MAX_RETENTION))]
    pub max_retention: Option<Time>,
    /// `metadata.max.idle.interval.ms` (KIP-835): how often the leader appends
    /// a `NoOpRecord`. Zero appends none.
    #[debug("{:?}", max_idle_interval.human().to_string())]
    #[default(DEFAULT_METADATA_MAX_IDLE_INTERVAL)]
    pub max_idle_interval: Time,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{ByteSizeExt, TimeExt},
    };

    /// The defaults are the numbers Kafka's `MetadataLogConfig` documents.
    #[test]
    fn defaults_are_kafkas() {
        let config = MetadataLogConfig::default();
        check!(
            (
                config.segment_size.bytes_u64(),
                config.segment_roll_interval.millis_i64(),
                config.max_retention_size.map(ByteSizeExt::bytes_u64),
                config.max_retention.map(TimeExt::millis_i64),
                config.max_idle_interval.millis_i64(),
            ) == (
                1024 * 1024 * 1024,
                7 * 24 * 60 * 60 * 1000,
                Some(100 * 1024 * 1024),
                Some(7 * 24 * 60 * 60 * 1000),
                500,
            )
        );
        check!(MIN_METADATA_LOG_SEGMENT_SIZE.bytes_u64() == 8 * 1024 * 1024);
    }

    #[test]
    fn the_partition_directory_is_kafkas() {
        check!(
            metadata_partition_dir(Path::new("/mnt/kafka/kafka-metadata-logs"))
                == PathBuf::from("/mnt/kafka/kafka-metadata-logs/__cluster_metadata-0")
        );
    }
}
