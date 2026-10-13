//! KIP-113 (`AlterReplicaLogDirs`): intra-broker log-dir reassignment.
//!
//! When the `AlterReplicaLogDirs` handler accepts a move
//! `(topic, partition) → target log.dir`, it asks this module to:
//!
//! 1. Open a fresh `krabka_log::Log` at
//!    `<target_log_dir>/<topic>-<partition>-future/`.
//! 2. Spawn a per-move replicator task that reads batches from the
//!    partition's current `Log` and appends them to the future log with
//!    `Log::append_at`, which keeps the leader-assigned offsets.
//! 3. Once `future_log.LEO == current_log.LEO`, ask the partition
//!    writer to swap atomically with `WriterMessage::SwapFutureLog`.
//!
//! The swap itself checks only the log end offsets, as Kafka's
//! `Partition.runCallbackIfFutureReplicaCaughtUp` does. What keeps the future
//! log a prefix of the current one is the truncation module: a truncation of
//! the current log, which a follower makes after a leader change, cuts the
//! future log to the same offset first
//! (`WriterMessage::TakeFutureTruncation`), and a move that starts or resumes
//! compares the two logs by leader epoch and cuts the future log to what they
//! share. So the swap can never install records that the current log dropped.
//!
//! The on-disk `*-future` directory is the only persisted state. A
//! crash mid-move leaves it behind. Broker startup re-discovers it with
//! `log_dir::scan_future` and re-spawns the replicator.

use {
    crate::{error::BrokerError, log_dir::FUTURE_SUFFIX, throttle::TokenBucket},
    krabka_log::Log,
    krabka_units::{ByteSize, Time},
    std::{
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
    tokio::task::JoinHandle,
    tokio_util::sync::CancellationToken,
};

mod catch_up;
mod cleanup;
mod replicator;
mod start;
#[cfg(test)]
mod test_support;
mod truncation;

pub(crate) use self::{
    cleanup::{abort_moves, shutdown_moves},
    start::{resume_move, start_move},
};

/// One in-progress intra-broker log-dir move. Inserted into
/// `Broker.future_logs` keyed by `(topic, partition)`.
///
/// The struct holds these fields to keep ownership of the future log alive
/// and to let `DescribeLogDirs` and future cancellation paths consult the
/// move's state through the registry. The writer task consumes them
/// indirectly through the `SwapFutureLog` message, which Rust's dead-code
/// pass cannot see through.
#[derive(Debug)]
pub struct FutureLogState {
    /// Parent `log.dir` that the move targets. It is one of the broker's
    /// configured `log.dirs`. The handler uses it to make a duplicate
    /// `AlterReplicaLogDirs` for the same `(topic, partition)`
    /// idempotent, or to reject a conflicting target.
    pub target_log_dir: PathBuf,
    /// The future log's `<target>/<topic>-<partition>-future` path.
    pub future_path: PathBuf,
    /// The future log itself. Shared with the replicator task and the
    /// `SwapFutureLog` writer message so all three hold the same
    /// `Arc<Mutex<Log>>`.
    pub future_log: Arc<Mutex<Log>>,
    /// Cancelled by the swap or by a follow-up `AlterReplicaLogDirs` that
    /// redirects an in-progress move.
    pub cancel: CancellationToken,
    /// Retained so cancellation and broker shutdown can abort and await the
    /// replicator task.
    pub task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

/// Why a [`start_move`] or [`resume_move`] call could not be honoured.
/// The handler translates these to the wire error codes
/// [`crate::codes::INVALID_TOPIC_EXCEPTION`],
/// [`crate::codes::LOG_DIR_NOT_FOUND`],
/// [`crate::codes::REPLICA_NOT_AVAILABLE`], and
/// [`crate::codes::KAFKA_STORAGE_ERROR`].
#[derive(Debug, derive_more::From)]
pub enum MoveError {
    /// Kafka's future directory name for the partition,
    /// `<topic>-<partition>.<uniqueId>-future`, would be longer than 255
    /// characters (`ReplicaManager.alterReplicaLogDirs`, KAFKA-4893).
    TopicNameTooLong,
    /// Target path is not one of this broker's configured `log.dirs`.
    LogDirNotFound,
    /// KIP-1066: the target directory is cordoned. Kafka's
    /// `ReplicaManager.alterReplicaLogDirs` answers it with
    /// `INVALID_REPLICA_ASSIGNMENT`, after the offline check and before it
    /// looks at the partition.
    Cordoned,
    /// The named partition is not hosted on this broker.
    ReplicaNotAvailable,
    /// `krabka_log::Log::open` or `mkdir` failed while staging the future log.
    /// The handler logs the inner error, then maps every storage failure to
    /// `KAFKA_STORAGE_ERROR` on the wire.
    #[from(BrokerError, krabka_log::LogError, std::io::Error)]
    Storage(BrokerError),
}

#[derive(Debug, Clone)]
pub(crate) struct MovePolicy {
    pub retry_backoff: Time,
    pub read_chunk: ByteSize,
    pub throttle: Arc<TokenBucket>,
}

/// The longest directory name Kafka lets a future log take.
const MAX_FUTURE_DIR_NAME_LEN: usize = 255;

/// The length of the unique id in Kafka's future directory name: a random
/// UUID in hex with its dashes removed.
const FUTURE_DIR_UNIQUE_ID_LEN: usize = 32;

/// Whether Kafka's future directory name for `(topic, partition)` is longer
/// than 255 characters.
///
/// Kafka names a future log `<topic>-<partition>.<uniqueId>-future`
/// (`LocalLog.logFutureDirName`), and `AlterReplicaLogDirs` refuses a move
/// whose name would be longer with "The topic name is too long." The length
/// is a Java `String.length`, so it counts UTF-16 code units.
fn future_dir_name_too_long(topic: &str, partition: i32) -> bool {
    let len = topic.encode_utf16().count()
        + "-".len()
        + partition.to_string().len()
        + ".".len()
        + FUTURE_DIR_UNIQUE_ID_LEN
        + FUTURE_SUFFIX.len();
    len > MAX_FUTURE_DIR_NAME_LEN
}

/// The configured log directory that an `AlterReplicaLogDirs` destination
/// names, or `None` when it names none.
///
/// Kafka compares the request string with the absolute path of each
/// configured directory by string equality (`LogManager.isLogDirOnline`).
/// A relative path, a trailing slash, or a symbolic link to a configured
/// directory names nothing. The destination may spell a directory as its
/// absolute path, which `DescribeLogDirs` reports, or as its canonical path.
/// The two are the same string unless the configured path runs through a
/// link.
fn configured_log_dir(all_log_dirs: &[PathBuf], requested: &Path) -> Option<PathBuf> {
    let requested = requested.as_os_str();
    all_log_dirs
        .iter()
        .find(|dir| {
            let absolute: PathBuf = std::path::absolute(dir)
                .unwrap_or_else(|_| (*dir).clone())
                .components()
                .collect();
            absolute.as_os_str() == requested || canonicalize_or_self(dir).as_os_str() == requested
        })
        .cloned()
}

/// Canonicalize a path for equality comparisons. It falls back to the
/// lexical path when canonicalisation fails, because the directory may not
/// exist yet. That is correct for log-dir comparisons, because this code also
/// compares against the configured value.
fn canonicalize_or_self(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::throttle::TokenBucket,
        assert2::assert,
        krabka_units::{kibibytes, millis},
    };

    #[test]
    fn move_policy_preserves_nondefault_values() {
        let policy = MovePolicy {
            retry_backoff: millis(7),
            read_chunk: kibibytes(4),
            throttle: Arc::new(TokenBucket::new()),
        };

        assert!(policy.retry_backoff == millis(7));
        assert!(policy.read_chunk == kibibytes(4));
        assert!(policy.throttle.byte_rate() == krabka_units::bytes_per_sec(0));
    }
}
