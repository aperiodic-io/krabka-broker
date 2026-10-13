//! Reading a WAL voter's on-disk state straight off the filesystem.
//!
//! This is the part of the failover case that no in-process handle can stand
//! in for. The claim under test is that an `acks=all` diskless append is on
//! *another broker's* disk before it is acknowledged, so the assertion has to
//! look at that broker's `__diskless_wal_quorum` tree: the durable-offset
//! checkpoint it fsyncs after every append, and the batch bytes behind it.
//!
//! The layout mirrors `wal::quorum::shard_dirs` and
//! `wal::quorum::follower::log`:
//!
//! ```text
//! <log.dir>/__diskless_wal_quorum/<topic>-<topic-id>-<partition>/voter-<node-id>/
//!     00000000000000000000.log         the replicated batches
//!     wal-durable-offset.checkpoint    "0\n<start> <end>\n": the version line, then the
//!                                      range, fsynced after each append
//! ```
//!
//! The log is read through a **copy** of that directory. `Log::open` recovers
//! and rewrites what it opens, so pointing it at a directory a live broker
//! owns would corrupt the thing under test.

use {
    crate::TOPIC,
    bytes::Bytes,
    krabka_broker::NodeId,
    krabka_log::{Log, LogConfig, Offset},
    std::path::{Path, PathBuf},
    uuid::Uuid,
};

/// Name of the follower's durable-offset checkpoint, from
/// `wal::quorum::follower::checkpoint`.
const DURABLE_OFFSET_FILE: &str = "wal-durable-offset.checkpoint";

/// The checkpoint's first line, its version, from
/// `wal::quorum::follower::checkpoint::DURABLE_OFFSET_VERSION`.
const CHECKPOINT_VERSION: &str = "0";

/// The offset range one voter reports as fsynced: `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableRange {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// The directory `node` keeps its WAL replica of `(TOPIC, partition)` in.
///
/// `TOPIC` is `[A-Za-z0-9_-]`-only, so it survives the shard directory's
/// sanitization unchanged and this path can be built literally.
pub(crate) fn voter_dir(log_dir: &Path, topic_id: Uuid, partition: i32, node: NodeId) -> PathBuf {
    log_dir
        .join("__diskless_wal_quorum")
        .join(format!("{TOPIC}-{topic_id}-{partition}"))
        .join(format!("voter-{}", node.0))
}

/// The durable range `node` has checkpointed, or `None` before it has written
/// its first checkpoint (or if it is not a voter for this shard at all).
pub(crate) fn durable_range(dir: &Path) -> Option<DurableRange> {
    let raw = std::fs::read_to_string(dir.join(DURABLE_OFFSET_FILE)).ok()?;
    let offsets = checkpoint_offsets(&raw)?;
    match offsets.as_slice() {
        [start, end] => Some(DurableRange {
            start: *start,
            end: *end,
        }),
        _ => None,
    }
}

/// The offsets on the second line of a durable-offset checkpoint, or `None`
/// unless its first line is the version, `0`. A half-written file reads as
/// `None` too, and the caller polls again.
fn checkpoint_offsets(raw: &str) -> Option<Vec<i64>> {
    let mut lines = raw.lines();
    if lines.next()? != CHECKPOINT_VERSION {
        return None;
    }
    lines
        .next()?
        .split_ascii_whitespace()
        .map(str::parse::<i64>)
        .collect::<Result<_, _>>()
        .ok()
}

/// The verbatim batch bytes this voter holds over `[start, end)`.
///
/// Call it only once the voter is quiescent for that range -- the failover
/// case waits on the checkpoint first -- because the copy is not atomic.
pub(crate) fn durable_bytes(dir: &Path, range: DurableRange) -> Bytes {
    let copy = tempfile::tempdir().expect("voter log copy dir");
    let root = copy.path().join("voter");
    copy_dir(dir, &root);
    let log = Log::open(&root, LogConfig::default()).expect("open the copied voter log");
    log.read_raw(
        Offset(range.start),
        Offset(range.end),
        krabka_units::mebibytes(64),
    )
    .expect("read the copied voter log")
    .bytes
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the voter log copy");
    for entry in std::fs::read_dir(from).expect("read the voter log dir") {
        let entry = entry.expect("voter log dir entry");
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy a voter log file");
        }
    }
}
