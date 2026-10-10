//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! This crate is the append-only storage layer that the Krabka broker uses.
//! It reads and writes Kafka 4.x's on-disk log format byte-for-byte:
//! 20-digit zero-padded segment filenames, sparse `.index` and
//! `.timeindex` files, and append-only `.log` files that hold
//! [`krabka_protocol::records::RecordBatch`] v2 streams.
//!
//! ## What this crate does
//!
//! - Open and recover existing log directories.
//! - Append `RecordBatch`es to the active segment.
//! - Read sequentially from an absolute offset.
//! - Truncate the log to an offset, for replication and leader election.
//! - Time-based and size-based retention.
//! - Deliver-at-time visibility, where a batch becomes readable only once its
//!   activation time has passed.
//!
//! ## Scope and boundaries
//!
//! This crate is the byte-compatible segment and index layer. It exposes
//! append, read, truncation, leader-epoch checkpoint, transaction-index,
//! retention, and compaction primitives over a single log directory.
//! `krabka-broker` applies broker-level policy above this storage layer. That
//! policy covers topic configuration, leader and follower ownership,
//! tiered-storage scheduling, transaction visibility rules, and write
//! serialization.
//!
//! ## Quick start
//!
//! ```no_run
//! use krabka_ids::Offset;
//! use krabka_log::{Log, LogConfig};
//! use krabka_protocol::records::RecordBatch;
//! use krabka_units::prelude::mebibytes;
//!
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default()).unwrap();
//! let mut batch = RecordBatch::default();
//! // ... fill the batch ...
//! let assigned_offset = log.append(&mut batch).unwrap();
//!
//! let out = log.read(Offset(0), mebibytes(1)).unwrap();
//! # let _ = (assigned_offset, out);
//! ```
//! ## Exporting a segment
//!
//! ```no_run
//! use krabka_log::{Log, LogConfig};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default())?;
//! for segment in log.tierable_segments() {
//!     println!(
//!         "segment {} starts at offset {}",
//!         segment.log_path.display(),
//!         segment.base_offset
//!     );
//! }
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/krabka-log/1.0.1")]

/// Emit the wrapped items only on platforms with a usable file-to-socket
/// `sendfile(2)` for the zero-copy fetch path.
///
/// Those platforms are Linux, the Apple targets, and FreeBSD/DragonFly. They
/// are the "SENDFILE alias". Windows is excluded, because there is no safe
/// `TransmitFile` wrapper under `unsafe_code = "forbid"`. The fetch path there
/// uses `pread` and `write_all`, and never needs the file-region descriptors.
///
/// One macro per crate keeps the predicate identical at every sendfile-gated
/// site: the `read_raw_desc` descriptor types, their impls, and the re-exports.
/// The cfg set therefore cannot drift between them.
macro_rules! sendfile_cfg {
    ($($item:item)*) => {
        $(krabka_macros::sendfile_platform! { $item })*
    };
}
pub(crate) use sendfile_cfg;

mod compact;
mod config;
mod delivery;
mod error;
mod filter;
mod index;
mod io;
mod leader_epoch_checkpoint;
mod log;
mod log_start_offset_checkpoint;
/// Shared breadth-first runner for the exhaustive Stateright models.
#[cfg(test)]
mod model_check;
pub mod name;
mod producer_snapshot;
mod record_limit;
mod recovery;
mod retention;
mod segment;
mod stamp_index;
mod stamp_source;
#[cfg(test)]
mod test_support;
pub mod topic_name;
mod txn_index;

pub use config::{
    CleanupPolicy, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_READ_AHEAD_MAX, DEFAULT_TAIL_CACHE_SIZE,
    DeliveryPolicy, LogConfig, RemoteTierFlags, ScheduleOrder, SegmentAllocation,
};
pub use delivery::{DeliveryAdvance, batch_is_deliverable};
pub use error::LogError;
pub use filter::{FilteredBatch, filter_batch};
pub use io::write_file_atomic;
#[cfg(any(test, feature = "test-helpers"))]
pub use io::{DirectFile, IoTarget, LogIo};
pub use krabka_ids::{LeaderEpoch, Offset, ProducerId};
pub use leader_epoch_checkpoint::{
    EpochEntry, LeaderEpochCheckpoint, epoch_and_offset_for_entries,
};
sendfile_cfg! {
    pub use log::RawReadDesc;
}
pub use log::{
    ActiveProducer, ActiveSegmentExport, BARRIER_CONTROL_TYPE, CompactionContext, Log, RawRead,
    ReadOutput, SegmentExport, TransactionAppendRefusal, TransactionalBatch, VerbatimBatch,
    VerificationGuard,
};
pub use producer_snapshot::{
    NUM_BATCHES_TO_RETAIN, ProducerBatchMetadata, ProducerSnapshotEntry, RecoveredProducer,
    SnapshotDecodeError, decode as decode_producer_snapshot,
};
pub use retention::now_ms as epoch_ms;
sendfile_cfg! {
    pub use segment::RawSegmentDesc;
}
// Re-export the zero-copy fetch descriptor so broker code can name
// `krabka_log::FileRegion` without depending on the protocol crate's path.
pub use krabka_protocol::records::FileRegion;
pub use segment::{RawSegmentRead, Segment};
pub use stamp_index::{StampEntry, StampIndex};
#[cfg(any(test, feature = "test-helpers"))]
pub use stamp_source::MonotonicStampSource;
pub use stamp_source::StampSource;
pub use txn_index::{AbortedTxn, TxnIndex};
