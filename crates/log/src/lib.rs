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
//! use {
//!     krabka_ids::Offset,
//!     krabka_log::{Log, LogConfig},
//!     krabka_protocol::records::RecordBatch,
//!     krabka_units::prelude::mebibytes,
//! };
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

#![doc(html_root_url = "https://docs.rs/krabka-log/1.0.5")]

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

#[cfg(any(test, feature = "test-helpers"))]
pub use io::{DirectFile, IoTarget, LogIo};
#[krabka_macros::sendfile_cfg]
pub use log::RawReadDesc;
#[krabka_macros::sendfile_cfg]
pub use segment::RawSegmentDesc;
// Re-export the zero-copy fetch descriptor so broker code can name
// `krabka_log::FileRegion` without depending on the protocol crate's path.
#[cfg(any(test, feature = "test-helpers"))]
pub use stamp_source::MonotonicStampSource;
pub use {
    config::{
        CleanupPolicy, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_READ_AHEAD_MAX, DEFAULT_TAIL_CACHE_SIZE,
        DeliveryPolicy, LogConfig, RemoteTierFlags, ScheduleOrder, SegmentAllocation,
    },
    delivery::{DeliveryAdvance, batch_is_deliverable},
    error::LogError,
    filter::{FilteredBatch, filter_batch},
    io::write_file_atomic,
    krabka_ids::{LeaderEpoch, Offset, ProducerId},
    krabka_protocol::records::FileRegion,
    leader_epoch_checkpoint::{EpochEntry, LeaderEpochCheckpoint, epoch_and_offset_for_entries},
    log::{
        ActiveProducer, ActiveSegmentExport, BARRIER_CONTROL_TYPE, CompactionContext, Log, RawRead,
        ReadOutput, SegmentExport, TransactionAppendRefusal, TransactionalBatch, VerbatimBatch,
        VerificationGuard,
    },
    producer_snapshot::{
        NUM_BATCHES_TO_RETAIN, ProducerBatchMetadata, ProducerSnapshotEntry, RecoveredProducer,
        SnapshotDecodeError, decode as decode_producer_snapshot,
    },
    retention::now_ms as epoch_ms,
    segment::{RawSegmentRead, Segment},
    stamp_index::{StampEntry, StampIndex},
    stamp_source::StampSource,
    txn_index::{AbortedTxn, TxnIndex},
};
