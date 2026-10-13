//! Command messages for a partition's single writer task, with the payload and
//! outcome types they carry. They live apart from the partition handle because
//! a request handler builds one without holding that handle.

use {
    crate::error::BrokerError,
    krabka_compression::CompressionType,
    krabka_log::{Log, Offset, VerbatimBatch},
    krabka_protocol::records::RecordBatch,
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio::sync::oneshot,
};

/// The records to append for a single produce job. This is either the
/// producer's verbatim wire bytes (zero-copy passthrough fast path) or a fully
/// owned, decoded [`RecordBatch`]. The broker takes the fallback path when
/// passthrough is unsafe: recompression, legacy up-conversion, control
/// batches, and similar cases.
///
/// The `Owned` arm is a complete fallback, so the whole verbatim
/// passthrough feature is easy to revert. An "always construct `Owned`" rule
/// restores the previous behavior.
#[derive(Debug)]
pub enum ProduceData {
    /// Append the producer's exact wire bytes, and patch only `base_offset`
    /// and `partition_leader_epoch`. No decode, re-encode, recompress, or CRC.
    Verbatim(VerbatimBatch),
    /// Decode and re-encode the owned batch on append (the original path).
    /// The writer mutates `base_offset` before append.
    Owned(RecordBatch),
    /// An internally built control batch, such as a transaction ABORT marker
    /// or a barrier marker.
    ///
    /// The writer appends it without the compression rewrite that
    /// [`Self::Owned`] gets. Kafka never compresses a control batch that
    /// arrived uncompressed, and a control batch holds one small record, so
    /// the rewrite would both diverge from Kafka and buy nothing.
    OwnedControl(RecordBatch),
    /// An internally built COMMIT marker plus the cross-domain coordinator's
    /// commit stamp. The stamp is written only to `.stampindex`; it never
    /// enters the Kafka batch bytes.
    OwnedCommitMarker {
        batch: RecordBatch,
        commit_stamp: u64,
    },
}

impl ProduceData {
    /// Apply the partition's configured compression to an owned client batch.
    /// Control batches and verbatim producer bytes keep their original shape.
    pub(crate) fn apply_target_compression(&mut self, target: Option<CompressionType>) {
        let Self::Owned(batch) = self else {
            return;
        };
        if let Some(target) = target
            && batch.attributes.compression() != target
        {
            batch.attributes = batch.attributes.with_compression(target);
        }
    }

    /// The producer ID of an internally built control batch, or `None` for a
    /// data batch.
    ///
    /// The writer reads the log's entry for this producer after the append,
    /// because a marker can change the producer epoch and sequence state that
    /// the produce path checks.
    #[must_use]
    pub(crate) fn control_producer_id(&self) -> Option<krabka_log::ProducerId> {
        match self {
            Self::Verbatim(_) | Self::Owned(_) => None,
            Self::OwnedControl(batch) | Self::OwnedCommitMarker { batch, .. } => {
                Some(krabka_log::ProducerId(batch.producer_id))
            }
        }
    }

    #[must_use]
    pub(crate) fn record_count(&self) -> u32 {
        match self {
            Self::Verbatim(batch) => u32::try_from(batch.last_offset_delta + 1)
                .expect("verbatim batch offset count is non-negative"),
            Self::Owned(batch) => u32::try_from(batch.last_offset_delta + 1)
                .expect("owned batch offset count is non-negative"),
            Self::OwnedControl(batch) => u32::try_from(batch.last_offset_delta + 1)
                .expect("control batch offset count is non-negative"),
            Self::OwnedCommitMarker { batch, .. } => u32::try_from(batch.last_offset_delta + 1)
                .expect("commit marker offset count is non-negative"),
        }
    }
}

/// What the writer reports back for one appended batch.
///
/// This is Kafka's `LogAppendInfo` narrowed to the two values a produce
/// response takes from the append itself: where the batch landed and, on a
/// `message.timestamp.type=LogAppendTime` partition, the broker clock the log
/// stamped into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedBatch {
    /// The offset the log assigned to the batch's first record.
    pub base_offset: Offset,
    /// Kafka's `LogAppendInfo.logAppendTime`, which the produce response
    /// carries as `logAppendTimeMs`. `Some(ms)` only on a `LogAppendTime`
    /// partition; `None` under `CreateTime`, which the response reports as
    /// the `-1` Kafka reports.
    pub log_append_time_ms: Option<i64>,
}

/// Produce-path message sent from the Produce handler to the partition's
/// writer task. The writer assigns `base_offset`, overwrites whatever the
/// handler put there, and replies with the assigned value.
#[derive(Debug)]
pub struct ProduceJob {
    /// The records to append (verbatim passthrough or owned fallback).
    pub data: ProduceData,
    /// Oneshot that the writer uses to report a successful append, or failure,
    /// back to the handler.
    pub ack: oneshot::Sender<Result<AppendedBatch, BrokerError>>,
    /// The producer transaction check the log runs just before it appends a
    /// client batch. `None` for a batch the broker built itself, such as a
    /// marker, an offset commit or a tombstone.
    pub producer_check: Option<ProducerAppendCheck>,
}

/// A client batch as the log's transaction check sees it, and the KIP-890
/// verification guard the produce path obtained for it. See
/// [`krabka_log::Log::check_transactional_append`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerAppendCheck {
    pub batch: krabka_log::TransactionalBatch,
    pub guard: krabka_log::VerificationGuard,
}

/// All message kinds the partition's writer task accepts.
///
/// The writer task is single-consumer over a single `mpsc::Sender`. An enum
/// here keeps replication appends ordered with produce appends.
#[derive(Debug)]
pub enum WriterMessage {
    /// Acknowledge after every earlier writer command has completed. This
    /// orders coordinator recovery after an append whose reply was abandoned.
    Barrier { ack: oneshot::Sender<()> },
    /// Append a batch and assign `base_offset` from the log. The `Produce`
    /// handler sends this message.
    Produce(ProduceJob),
    /// Make the log prefix ending at `leo` durable before acknowledging.
    /// Diskless partitions sync their WAL; ordinary partitions fsync the log.
    SyncDurable {
        leo: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Append a batch at the caller-supplied offset, which the partition's
    /// leader already assigned. The per-(topic, partition) replicator on a
    /// follower broker sends this message.
    Replicate {
        batch: RecordBatch,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Append a leader's already-encoded data batch without decoding or
    /// recompressing it. Control batches cannot use this arm because their
    /// marker record is needed for transaction bookkeeping.
    ReplicateVerbatim {
        batch: VerbatimBatch,
        base_offset: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Truncate the log so no records at offset `>= offset` remain. Used
    /// by the replicator's `OFFSET_OUT_OF_RANGE` recovery path.
    Truncate {
        offset: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Drop every segment and recreate the active segment at `new_base`.
    /// The replicator's `OFFSET_OUT_OF_RANGE` recovery path sends this when
    /// the follower has fallen behind the leader's `log_start`. The
    /// follower must move its own `log_start` *forward* past records it
    /// never saw, and `Truncate` cannot do that.
    ResetTo {
        new_base: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Atomically swap the partition's `LogConfig`. The writer task
    /// serializes this with appends so no in-flight `RecordBatch` sees a
    /// half-applied config. Sent by
    /// `ReplicatorSupervisor::reconcile` whenever a `V1TopicConfig`
    /// record changes the topic's overrides.
    SetLogConfig {
        config: krabka_log::LogConfig,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Run one compaction pass. The writer actor serializes this with
    /// appends to preserve the single-writer invariant on `Log`.
    Compact {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Run one local-retention pass: the time-based segment roll, then the
    /// `retention.ms` and `retention.bytes` eviction of sealed segments. The
    /// writer actor serializes this with appends for the same reason
    /// [`WriterMessage::Compact`] is serialized -- both mutate the log.
    Retain {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Trim from the start of the log: drop sealed segments whose last
    /// offset is `< new_start`, advance `log_start_offset` if `new_start`
    /// falls inside the active segment. Returns the resulting
    /// `log_start_offset`. That value can be less than `new_start` when
    /// `new_start` falls between segment boundaries, which is Kafka
    /// semantics. The `DeleteRecords` handler sends this message.
    TrimToOffset {
        new_start: Offset,
        ack: tokio::sync::oneshot::Sender<Result<Offset, BrokerError>>,
    },
    /// Test-only: shift the in-memory `log_start_offset` and do not
    /// physically truncate segments. This simulates retention-driven
    /// truncation for the `out_of_range_truncates_and_recovers`
    /// replication integration test.
    #[cfg(any(test, feature = "test-helpers"))]
    TestSetLogStart {
        new_start: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Atomically swap the partition's `Log` to a future log that has
    /// fully caught up. Sent by the KIP-113 move task in
    /// `future_log.rs` once `future_log.LEO == current_log.LEO`. The
    /// writer re-checks the invariant under its own lock, then:
    /// 1. drops the current `Log`,
    /// 2. `fs::rename`s `future_path` → `target_partition_path`,
    /// 3. removes the source partition directory,
    /// 4. re-opens `Log` at `target_partition_path` and stores it,
    /// 5. updates `Partition.log_dir` to `target_log_dir`.
    ///
    /// If the future log fell behind during the request hop, returns
    /// `Ok(SwapOutcome::NotCaughtUp)` so the caller can loop once more.
    SwapFutureLog {
        target_log_dir: PathBuf,
        future_log: Arc<Mutex<Log>>,
        future_path: PathBuf,
        target_partition_path: PathBuf,
        ack: oneshot::Sender<Result<SwapOutcome, BrokerError>>,
    },
    /// Report and forget the lowest offset that a [`WriterMessage::Truncate`]
    /// or [`WriterMessage::ResetTo`] cut the log to since the last such
    /// report, or `None` when the log was not cut.
    ///
    /// The KIP-113 move task sends it before each catch-up pass and cuts its
    /// future log to the answer, so the moved copy never keeps records that
    /// the current log dropped. It is Kafka's
    /// `ReplicaFetcherThread.truncate`, which calls
    /// `replicaAlterLogDirsManager.markPartitionsForTruncation` after it
    /// truncates the current log. A [`WriterMessage::SwapFutureLog`] answers
    /// `NotCaughtUp` while a cut is unreported, whatever the log ends say.
    TakeFutureTruncation {
        ack: oneshot::Sender<Option<Offset>>,
    },
}

/// Result of a [`WriterMessage::SwapFutureLog`] handling cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapOutcome {
    /// The swap succeeded. The partition now serves from the
    /// target log dir, and the broker removed the source dir.
    Swapped,
    /// The future log was behind the current log when the writer
    /// re-checked. The caller should resume replication and retry.
    NotCaughtUp,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn commit_marker_record_count_uses_batch_offset_span() {
        let data = ProduceData::OwnedCommitMarker {
            batch: krabka_protocol::records::RecordBatch {
                last_offset_delta: 3,
                ..krabka_protocol::records::RecordBatch::default()
            },
            commit_stamp: 99,
        };

        check!(data.record_count() == 4);
    }
}
