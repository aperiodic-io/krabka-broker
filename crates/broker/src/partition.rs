//! A single partition's runtime handle. Owned by the partition registry
//! inside `Broker`. The handle gives any task:
//!
//! - read access to the partition's [`Log`] through `Arc<Mutex<Log>>`
//! - write access through a `mpsc::Sender<WriterMessage>`. A single writer
//!   task drains the channel; see `partition_writer.rs`.
//! - a [`Notify`] that fires after every successful append. Long-poll Fetch
//!   uses it to wake when new data arrives.
//! - the partition's deliver-at-time state: a lock-free mirror of the delivery
//!   watermark and a second [`Notify`] that fires when a scheduled batch comes
//!   due. See [`crate::delivery`].

// `std::sync::Mutex` is kept for `log` (sync hot-path callers);
// `replica_state` uses `tokio::sync::Mutex` to avoid blocking worker threads.
use {
    crate::{
        config::BrokerConfig,
        diskless::hot_tail::HotTailCache,
        txn::{handlers::write_txn_markers::CommittedOffsets, marker::MarkerType},
        wal::{OffsetSequencer, quorum::registry::WalShardRegistry},
    },
    arc_swap::ArcSwap,
    krabka_ids::PartitionIndex,
    krabka_log::{Log, Offset, ReadOutput},
    krabka_units::ByteSize,
    std::{
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicI32, AtomicU64},
        },
    },
    tokio::{
        sync::{Notify, mpsc},
        task::JoinHandle,
    },
};

/// Per producer, the offset resolution a durable `__consumer_offsets` marker
/// still owes the group actors: the marker type, the marker's offset, and
/// the offsets the transaction wrote.
pub(crate) type OwedMarkerResolutions =
    std::collections::HashMap<krabka_log::ProducerId, (MarkerType, i64, CommittedOffsets)>;
use crate::{delivery::DeliveryHandles, error::BrokerError, replica_state::ReplicaState};

mod commands;
mod commit_wait;
mod leadership;
mod messages;
mod watermark;
mod writer_gate;

pub(crate) use self::writer_gate::WriterGate;

#[cfg(test)]
pub(crate) mod test_support;

pub use self::messages::{
    AppendedBatch, ProduceData, ProduceJob, ProducerAppendCheck, ReplicatedBatch, SwapOutcome,
    WriterMessage,
};
// Only watermark's own tests name crate::partition::HwTimeout.
#[cfg(test)]
pub use self::watermark::HwTimeout;
pub(crate) use self::{
    commands::ProduceBatchError,
    commit_wait::Uncommitted,
    leadership::{ReplicationTarget, initial_replication_target},
};

/// Absolute record offset within a partition's log (base offset, log end
/// offset, high watermark, truncation points, …). This is an alias only. It
/// shows which `i64`s in signatures are offsets and not timestamps or counts.
pub type LogOffset = i64;

/// Writer sizing and diskless services shared by startup and first-touch materialization.
pub(crate) struct PartitionRuntimeConfig {
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub diskless: bool,
    pub hot_tail: Option<Arc<HotTailCache>>,
    pub wal_shards: Option<Arc<WalShardRegistry>>,
    pub sequencer: Option<Arc<dyn OffsetSequencer>>,
}

type PartitionWalServices = (
    Option<Arc<HotTailCache>>,
    Option<Arc<WalShardRegistry>>,
    Option<Arc<dyn OffsetSequencer>>,
);

impl PartitionRuntimeConfig {
    pub(crate) fn new(
        writer_sizes: (usize, usize, usize),
        diskless: bool,
        services: PartitionWalServices,
    ) -> Self {
        let (max_produce_group, partition_writer_queue_depth, diskless_wal_local_replica_count) =
            writer_sizes;
        let (hot_tail, wal_shards, sequencer) = services;
        Self {
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            diskless,
            hot_tail,
            wal_shards,
            sequencer,
        }
    }

    pub(crate) fn from_broker(
        config: &BrokerConfig,
        diskless: bool,
        services: PartitionWalServices,
    ) -> Self {
        Self::new(
            (
                config.max_produce_group,
                config.partition_writer_queue_depth,
                config.diskless_wal_local_replica_count,
            ),
            diskless,
            services,
        )
    }
}

/// Runtime handle for a single partition.
///
/// Cheap to clone. `log`, `writer_tx`, and `append_notify` are all `Arc`-ish,
/// and the writer handle is not cloned because `Arc<JoinHandle<()>>` wraps it.
#[derive(Clone)]
// `partition_id` mirrors Kafka's wire naming and is the conventional term
// used throughout the broker; renaming to `id` would shadow `Partition`'s
// own identity at every call site.
//
// `Debug` deliberately leaves out `log`: formatting a `Mutex<Log>` would block
// on the mutex and dump internal segment state into tracing output.
#[derive(derive_more::Debug)]
#[debug("Partition {{ topic: {topic:?}, partition_id: {index:?}, delivery: {delivery:?}, .. }}")]
pub struct Partition {
    pub topic: String,
    pub index: PartitionIndex,
    /// Parent `log.dir` that currently owns the partition. This is the parent
    /// of `log.lock().dir()`, that is, the configured directory and not the
    /// `<topic>-<partition>` subdirectory. Updated by
    /// [`WriterMessage::SwapFutureLog`] as the last step of a KIP-113
    /// move. It is an `ArcSwap` so that readers, such as `DescribeLogDirs`
    /// and `AlterReplicaLogDirs` validation, see the swap atomically
    /// without the `log` mutex.
    pub log_dir: Arc<ArcSwap<PathBuf>>,
    pub log: Arc<Mutex<Log>>,
    pub writer_tx: mpsc::Sender<WriterMessage>,
    /// Lets a produce append on its own task while the writer is idle; see
    /// [`WriterGate`].
    pub(crate) writer_gate: Arc<WriterGate>,
    /// Serializes transaction-marker admission, append, and any offset
    /// resolution for this partition. Holds, per producer, the resolution a
    /// durable `__consumer_offsets` marker still owes the group actors: the
    /// marker type, its offset, and the offsets the transaction wrote.
    pub(crate) marker_materialization: Arc<tokio::sync::Mutex<OwedMarkerResolutions>>,
    pub append_notify: Arc<Notify>,
    pub(crate) replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
    /// Deliver-at-time state: the lock-free delivery-watermark mirror, the
    /// wake a long poll parks on until a scheduled batch comes due, and the
    /// slot the broker-wide delivery scheduler installs its poke into.
    ///
    /// The writer actor holds a clone of the same handles, so an append
    /// refreshes the mirror without reaching back through this struct.
    pub(crate) delivery: DeliveryHandles,
    /// Current leader's `NodeId` from the metadata image. Atomic for
    /// lock-free reads in the Produce/Fetch hot paths.
    pub current_leader: Arc<AtomicU64>,
    /// Current `leader_epoch` from the metadata image. The broker stamps it on
    /// every appended batch and validates it on every follower Fetch.
    pub current_leader_epoch: Arc<AtomicI32>,
    /// Serializes follower mutations against topic recreation and local
    /// leader/epoch installation. Replication holds a read guard through the
    /// writer acknowledgement; metadata reconciliation takes the write guard.
    pub(crate) replication_target: Arc<tokio::sync::RwLock<ReplicationTarget>>,
    /// True for diskless runtime partitions whose client-visible HW may only
    /// advance through the WAL durable-sync path.
    pub(crate) diskless: bool,
    /// Retained so broker shutdown can abort and await the writer task after
    /// all request handlers have drained.
    pub(crate) writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// New partitions begin with no transaction-marker resolutions owed to group actors.
pub(crate) fn empty_marker_materialization() -> Arc<tokio::sync::Mutex<OwedMarkerResolutions>> {
    Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::default()))
}

impl Partition {
    /// The complete checkpoint history, without changing the test's independent expected rows.
    #[cfg(test)]
    pub(crate) fn epoch_history(&self) -> Vec<(i32, i64)> {
        test_support::epoch_history(&self.log.lock().unwrap())
    }

    /// A disk-backed fixture with the ordinary eight-slot writer channel and zeroed leader state.
    /// The caller opens the log and selects the exact writer entry point; the writer and partition
    /// share the delivery handles whose mirror the writer refreshes.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn writer_fixture(
        topic: &str,
        log: Arc<Mutex<Log>>,
        log_dir: Arc<ArcSwap<PathBuf>>,
        spawn: impl FnOnce(
            (String, PartitionIndex),
            (Arc<Mutex<Log>>, Arc<ArcSwap<PathBuf>>),
            mpsc::Receiver<WriterMessage>,
            (
                Arc<Notify>,
                Arc<tokio::sync::Mutex<ReplicaState>>,
                Arc<Notify>,
                DeliveryHandles,
            ),
        ) -> JoinHandle<()>,
    ) -> Self {
        let (writer_tx, rx) = mpsc::channel::<WriterMessage>(8);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
        let hw_advance_notify = Arc::new(Notify::new());
        let delivery = DeliveryHandles::new();
        let writer = spawn(
            (topic.to_string(), PartitionIndex(0)),
            (log.clone(), log_dir.clone()),
            rx,
            (
                append_notify.clone(),
                replica_state.clone(),
                hw_advance_notify.clone(),
                delivery.clone(),
            ),
        );
        Self {
            topic: topic.to_string(),
            index: PartitionIndex(0),
            log_dir,
            log,
            writer_tx,
            writer_gate: WriterGate::shared(false),
            marker_materialization: empty_marker_materialization(),
            append_notify,
            replica_state,
            hw_advance_notify,
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            delivery,
            replication_target: initial_replication_target(None),
            diskless: false,
            writer_handle: Arc::new(Mutex::new(Some(writer))),
        }
    }

    /// Next offset the underlying [`Log`] will assign. Cheap: takes the
    /// `Arc<Mutex<Log>>` briefly. Replicators call this before each Fetch
    /// to compute `fetch_offset`.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. The caller treats that as no progress, and the
    /// writer-died path later reports a clearer error.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        match self.log.lock() {
            Ok(g) => g.log_end_offset(),
            Err(_) => Offset(0),
        }
    }

    /// The bytes of the log's segments when its directory is `dir`: Kafka's
    /// `UnifiedLog.size`, which `DescribeLogDirs` reports as
    /// `PartitionSize`. Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// The log counts the bytes of its batches, where its files can hold
    /// more: the active segment of a `preallocate` partition writes through
    /// `O_DIRECT` and carries up to a block of padding past its last batch.
    ///
    /// Returns `None` when the log lives in another directory, or if the log
    /// mutex is poisoned.
    #[must_use]
    pub fn log_size_in(&self, dir: &std::path::Path) -> Option<ByteSize> {
        let log = self.log.lock().ok()?;
        (log.dir() == dir).then(|| log.size())
    }

    /// Last Stable Offset at `high_watermark`: Kafka's
    /// `UnifiedLog.lastStableOffset`. A transaction that is open, or complete
    /// with a marker that `high_watermark` has not passed, holds it at the
    /// transaction's first offset. See [`Log::last_stable_offset`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. The caller treats that as no progress, and the
    /// writer-died path later reports a clearer error.
    #[must_use]
    pub fn last_stable_offset(&self, high_watermark: Offset) -> Offset {
        match self.log.lock() {
            Ok(mut g) => g.last_stable_offset(high_watermark),
            Err(_) => Offset(0),
        }
    }

    /// First absolute offset still present in the underlying [`Log`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned, that is, if the writer task
    /// panicked. `TxnCoordinator::recover` uses this to seed the replay scan
    /// offset.
    #[must_use]
    pub(crate) fn log_start_offset(&self) -> Offset {
        match self.log.lock() {
            Ok(g) => g.log_start_offset(),
            Err(_) => Offset(0),
        }
    }

    /// The additional internal stamp coordinate that covers `offset`. Returns
    /// `None` when this partition is unstamped, that is, when no
    /// [`krabka_log::StampSource`] is injected, or when no stamped range
    /// covers `offset`.
    ///
    /// Locks the `Arc<Mutex<Log>>` briefly. This is a server-side query only.
    /// No produce or fetch handler consults it, so the stamp cannot leak into
    /// any client-facing response. Returns `None` if the mutex is poisoned.
    #[cfg(test)]
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        match self.log.lock() {
            Ok(g) => g.stamp_for_offset(offset),
            Err(_) => None,
        }
    }

    /// Fence the log and then remove and return the writer task handle, for a
    /// caller that is about to delete the partition's directory.
    ///
    /// An append can run on a request's own task while the writer is idle
    /// (see [`WriterGate`]), so aborting the writer does not stop every
    /// writer of the log. The fence does: it is taken under the log's mutex,
    /// so an append either finished before it or finds the log fenced and is
    /// answered `NOT_LEADER_OR_FOLLOWER`, as a produce to a gone writer was.
    pub(crate) fn retire(&self) -> Option<JoinHandle<()>> {
        self.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fence();
        self.take_writer_handle()
    }

    /// Remove and return the writer task handle exactly once. Broker shutdown
    /// uses this after request handlers drain, then aborts and awaits the task.
    pub(crate) fn take_writer_handle(&self) -> Option<JoinHandle<()>> {
        self.writer_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Read batches from the underlying [`Log`] that start at `offset`, and
    /// return up to `max_size` of data.
    ///
    /// Locks the `Arc<Mutex<Log>>` for the duration of the read.
    /// `TxnCoordinator::recover` uses this to replay `__transaction_state`
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Log`] if the underlying [`Log::read`] fails,
    /// for example when `offset < log_start_offset()`.
    pub(crate) fn read_log(
        &self,
        offset: Offset,
        max_size: ByteSize,
    ) -> Result<ReadOutput, BrokerError> {
        self.log
            .lock()
            .map_err(|_| BrokerError::Txn("log mutex poisoned".into()))?
            .read(offset, max_size)
            .map_err(BrokerError::from)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            partition::test_support::{append_records, test_partition},
            test_support::{PartitionRecordsSetup, RecordCount},
        },
        assert2::{assert, check},
        tokio::sync::Notify,
    };

    /// `Partition::stamp_for_offset` returns the log's actual stamp for a
    /// covered offset and `None` beyond the stamped range. It is not a
    /// constant. A distinctive stamp (`4242`) pins the delegated value, so
    /// the test catches a mutant that hard-codes `Some(0)`, `Some(1)`, or
    /// `None`.
    #[tokio::test]
    async fn stamp_for_offset_delegates_actual_stamp() {
        #[derive(Debug)]
        struct FixedStamp(u64);
        impl krabka_log::StampSource for FixedStamp {
            fn next_stamp(&self) -> u64 { self.0 }
        }

        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        p.log
            .lock()
            .expect("log mutex")
            .set_stamp_source(Arc::new(FixedStamp(4242)))
            .expect("set stamp source");
        append_records(&p, PartitionRecordsSetup {
            count: RecordCount(3),
            ..Default::default()
        }); // offsets 0..=2, each stamped 4242

        check!(p.stamp_for_offset(Offset(0)) == Some(4242));
        check!(p.stamp_for_offset(Offset(2)) == Some(4242));
        check!(p.stamp_for_offset(Offset(3)) == None); // beyond the stamped range
    }

    /// The size `DescribeLogDirs` reports is the log's own, and only for the
    /// directory the log lives in.
    #[tokio::test]
    async fn log_size_in_reports_the_logs_size_for_its_own_directory_only() {
        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        append_records(&p, PartitionRecordsSetup {
            count: RecordCount(3),
            ..Default::default()
        });
        let (log_dir, size) = {
            let log = p.log.lock().expect("log mutex");
            (log.dir().to_path_buf(), log.size())
        };

        check!(p.log_size_in(&log_dir) == Some(size));
        check!(p.log_size_in(&log_dir.join("elsewhere")) == None);
    }

    #[test]
    fn partition_is_clone_and_send() {
        // Compile-time check.
        fn assert_send<T: Send>() {}
        fn assert_clone<T: Clone>() {}
        assert_send::<Partition>();
        assert_clone::<Partition>();
    }

    #[tokio::test]
    async fn debug_does_not_dump_log() {
        let (p, _dir) = test_partition(Arc::new(Notify::new()));
        let s = format!("{p:?}");
        // topic/partition_id appear; the mutex/log internals must NOT appear
        // in Debug output.
        let cases = [
            ("topic", true),
            ("partition_id", true),
            ("Mutex", false),
            ("segments", false),
        ];
        for (needle, expected) in cases {
            assert!(s.contains(needle) == expected, "needle {needle:?} in {s:?}");
        }
    }
}
