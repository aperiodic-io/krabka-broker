//! `Log`: a sorted collection of `Segment`s with append, read, and truncate.
//!
//! This file holds the `Log` type itself. Every operation over it lives in a
//! submodule named for the concern it covers, and each of those submodules
//! adds its own `impl Log` block: [`self::open`] for opening a directory and
//! rebuilding recovered state, [`self::append`] and [`self::verbatim`] for the
//! two write paths, [`self::read`] for the three read paths, and so on. The
//! private fields below stay in this module so that every one of those
//! submodules, as a descendant, can reach them.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
};

use krabka_ids::{Offset, ProducerId};

use crate::{
    config::LogConfig,
    io::LogIo,
    leader_epoch_checkpoint::LeaderEpochCheckpoint,
    producer_snapshot::{ProducerBatchMetadata, ProducerSnapshotEntry},
    segment::Segment,
    stamp_index::StampIndex,
    stamp_source::StampSource,
    txn_index::TxnIndex,
};

mod active_producers;
mod append;
mod compaction;
pub(crate) mod control;
mod delivery;
#[cfg(all(test, unix))]
mod direct_writes;
#[cfg(test)]
mod faults;
mod maintenance;
mod open;
#[cfg(test)]
mod preallocation;
mod read;
mod rollover_flush;
mod stamp;
mod state;
mod sync;
#[cfg(test)]
mod test_support;
mod tick;
mod tiering;
mod timestamp;
mod transaction;
mod truncate;
mod verbatim;
mod verbatim_group;
mod verification;

pub use self::{
    active_producers::ActiveProducer,
    compaction::CompactionContext,
    control::BARRIER_CONTROL_TYPE,
    read::{RawRead, ReadOutput},
    tiering::{ActiveSegmentExport, SegmentExport},
    verbatim::VerbatimBatch,
    verification::{TransactionAppendRefusal, TransactionalBatch, VerificationGuard},
};

crate::sendfile_cfg! {
    pub use self::read::RawReadDesc;
}

/// A Kafka-format log: a sorted collection of [`Segment`]s plus a single
/// active segment that accepts appends.
///
/// `Log` has one writer and many concurrent readers. Mutation takes
/// `&mut self`; `read`, `log_start_offset`, and the other read methods take
/// `&self`. Construct one with [`Log::open`].
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    config: std::sync::Arc<std::sync::RwLock<LogConfig>>,
    io: std::sync::Arc<dyn LogIo>,
    segments: Vec<Segment>,
    active: Option<Segment>,
    dir_sync_needed: bool,
    rollover_flusher: rollover_flush::Flusher,
    unflushed_messages: u64,
    last_flush: std::time::Instant,
    roll_jitter: Option<(Offset, u64)>,
    pending_deletes: Vec<(std::time::SystemTime, Vec<PathBuf>)>,
    /// The global log start (Kafka's `logStartOffset`): the first offset any
    /// reader may ask for, wherever the records for it live.
    ///
    /// It is a pointer of its own rather than a value derived from the
    /// segments on disk, because KIP-405 gives a tiered partition two floors
    /// and only one of them follows local files. `DeleteRecords`, ordinary
    /// retention, a trim and a reset move this one; dropping a local segment
    /// whose copy is in the remote tier does not, and that is what leaves the
    /// band `[log_start_offset(), local_log_start_offset())` for the remote
    /// read path to serve. [`Log::local_log_start_offset`] is the second
    /// floor.
    ///
    /// The segment names witness only the local half. [`Log::open`] derives
    /// this pointer from them and then restores the durable half from
    /// `log-start-offset-checkpoint`, which [`Log::set_log_start_offset`]
    /// writes.
    start_offset: Offset,

    /// Whether [`Self::start_offset`] is a floor someone deleted up to, rather
    /// than one derived from the segments that happened to be on disk at
    /// [`Log::open`].
    ///
    /// True once this process moves the floor, and true at open when
    /// `log-start-offset-checkpoint` restored one. False on a log whose only
    /// witness is its segment names: on a tiered partition whose local
    /// segments were evicted, that derivation sits *above* everything the
    /// archive holds. Three callers must not act on such a floor: the remote
    /// read would refuse offsets the archive can still serve, the log-start
    /// breach in remote retention would delete the archive, and the
    /// `ListOffsets(earliest)` clamp would report an offset past every record
    /// the tier still holds. [`Log::established_log_start`] is what they ask
    /// instead.
    start_offset_established: bool,

    /// How many of the leading sealed segments are a *clean* prefix, because
    /// a compaction pass produced them and nothing has appended to the log's
    /// dirty region since.
    ///
    /// Kafka keeps the same fact in `cleaner-offset-checkpoint`: the dirty
    /// region is everything above the last cleaned offset, and on a log
    /// nothing has cleaned yet that is the whole log. A pass groups every
    /// cleanable sealed segment into one or more size-bounded output
    /// segments (`segment.bytes`, Kafka's `Cleaner.groupSegmentsBySize`), and
    /// every one of them was rewritten with the full offset map, so all of
    /// them -- not just the first -- are the last pass's output and count as
    /// clean. Tracking a count rather than a single boolean is what keeps
    /// `min.cleanable.dirty.ratio` from treating every output segment past
    /// the first as dirty and asking for a pass that would rewrite an
    /// already-deduplicated segment for nothing.
    ///
    /// It is in memory only. A reopened log reads as never cleaned, which
    /// costs one extra pass and never skips a needed one; Kafka's checkpoint
    /// is durable, and making this one durable is the same change on both
    /// sides of a restart.
    clean_prefix_segments: usize,

    /// First unstable offset: the first offset of the earliest transaction
    /// that is open, or that is complete but whose marker is not yet below the
    /// high watermark. It is `log_end_offset()` when there is no such
    /// transaction. [`Log::last_stable_offset`] caps it at the high watermark.
    lso: Offset,

    /// In-flight transactions: `producer_id` → first offset of this
    /// producer's currently-open txn. The log clears the entry when it
    /// applies a commit or abort marker for that `producer_id`.
    pending: HashMap<ProducerId, Offset>,

    /// KIP-890 verification state per producer, from the start of a
    /// transaction verification to the producer's next transactional append.
    /// See [`VerificationGuard`].
    verification_states: HashMap<ProducerId, verification::VerificationState>,
    /// Complete transactions whose marker the high watermark has not passed:
    /// first offset of the transaction → last offset of its marker. They still
    /// hold the last stable offset, as Kafka's `unreplicatedTxns` does, so a
    /// `read_committed` reader never sees a transaction whose outcome a leader
    /// change can still truncate away. [`Log::last_stable_offset`] releases
    /// an entry once its marker is below the high watermark.
    unreplicated: BTreeMap<Offset, Offset>,

    /// Exact data-batch ranges for each in-flight transaction. A commit
    /// marker stamps only these ranges, so interleaved transactions and
    /// ordinary records never inherit another transaction's commit stamp.
    pending_stamp_ranges: HashMap<ProducerId, Vec<(Offset, Offset)>>,

    /// Last transaction-coordinator epoch observed in a durable commit or
    /// abort marker for each producer. The marker value carries this field,
    /// so log recovery can rebuild it without a separate sidecar.
    coordinator_epochs: HashMap<ProducerId, i32>,

    /// Producer sequence, epoch, and transaction metadata persisted in
    /// Kafka-compatible `.snapshot` files at segment boundaries.
    producer_state: HashMap<ProducerId, ProducerSnapshotEntry>,

    /// Up to four data batches each producer appended before the last one
    /// its `producer_state` entry holds, at that entry's epoch, oldest first.
    /// With the last batch they are Kafka's retained `BatchMetadata`. No
    /// snapshot carries them, so a reopen rebuilds them from the replayed
    /// tail alone.
    earlier_batches: HashMap<ProducerId, VecDeque<ProducerBatchMetadata>>,

    /// Active segment's `TxnIndex`. The log reopens it on segment roll.
    active_txn_index: TxnIndex,

    /// Immutable transaction indexes keyed by sealed segment base offset.
    sealed_txn_indexes: BTreeMap<Offset, TxnIndex>,

    /// Injected source of the additional internal stamp coordinate. `None`
    /// is the default and means this partition stamps nothing. Behavior and
    /// all wire-exact bytes stay exactly as they are without a stamp.
    /// [`Log::set_stamp_source`] sets this field. The broker shares one tenant
    /// source across all hosted partitions when internal stamping is enabled.
    stamp_source: Option<std::sync::Arc<dyn StampSource>>,

    /// Open `.stampindex` sidecars keyed by segment base offset. Transactional
    /// data can commit after its segment rolls, so the log keeps sealed
    /// indexes available instead of retaining only the active one.
    stamp_indexes: BTreeMap<Offset, StampIndex>,

    /// Per-partition leader-epoch checkpoint. All segments share it, and
    /// epoch history accumulates over the log's lifetime.
    epoch_checkpoint: LeaderEpochCheckpoint,

    /// External next-offset authority used by diskless recovery. The broker
    /// sets this after it reads the committed `KRaft` frontier. Caller-supplied
    /// append-at bases must then not be below
    /// `max(log_end_offset, reconciled_frontier)`.
    reconciled_frontier: Offset,

    /// Cached deliver-at-time watermark: the first offset whose activation
    /// time has not passed. It only ever moves forward, and it is derived
    /// from the batch timestamps on disk, so nothing persists it.
    /// [`Log::advance_delivery_watermark`] recomputes it.
    delivery_watermark: Offset,

    /// Activation time of the batch that stopped the last activation walk.
    /// While that instant is still in the future, no walk can get past that
    /// batch, so a repeat advance answers from this field and does no I/O.
    /// `None` means the last walk found nothing waiting, or that a truncation
    /// or a compaction may have removed the batch it named.
    delivery_pending_ms: Option<i64>,
}
