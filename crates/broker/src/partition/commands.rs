//! The [`Partition`] methods that drive its single writer task. Each one sends
//! a [`WriterMessage`] and awaits the writer's acknowledgement, and they are
//! grouped here because they share that one request-response shape.

use krabka_log::Offset;
use krabka_protocol::records::RecordBatch;
use tokio::sync::oneshot;

use crate::{
    blocking::run_blocking,
    config_keys::apply_to_log_config,
    error::BrokerError,
    partition::{Partition, ProduceData, ProduceJob, ProducerAppendCheck, WriterMessage},
    task_util::{AskError, ask},
};

/// Whether an internal produce definitely failed or may already be appended.
#[derive(Debug)]
pub(crate) enum ProduceBatchError {
    /// The partition writer reported failure or never accepted the command.
    Rejected(BrokerError),
    /// The command was accepted, but its acknowledgement channel closed.
    Indeterminate(String),
}

impl Partition {
    /// Wait until the writer has completed every command accepted before this
    /// barrier. A dropped acknowledgement or closed writer is an error; the
    /// caller may bound or cancel the wait without changing the log.
    pub(crate) async fn writer_barrier(&self) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::Barrier { ack }).await
    }

    /// [`ask`] the writer task, and report a dead writer or a dropped
    /// acknowledgement as [`BrokerError::Replication`].
    async fn ask_writer<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> WriterMessage,
    ) -> Result<T, BrokerError> {
        self.ask_writer_or(make, BrokerError::Replication, "ack dropped")
            .await
    }

    /// [`ask`] the writer task. A dead writer is reported as
    /// `error("partition writer dead")`, and a dropped acknowledgement as
    /// `error(dropped)`.
    async fn ask_writer_or<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> WriterMessage,
        error: fn(String) -> BrokerError,
        dropped: &str,
    ) -> Result<T, BrokerError> {
        ask(&self.writer_tx, make).await.map_err(|ask_error| {
            error(
                match ask_error {
                    AskError::Closed => "partition writer dead",
                    AskError::Dropped => dropped,
                }
                .into(),
            )
        })
    }

    /// Start a KIP-890 transaction verification on the log. See
    /// [`krabka_log::Log::maybe_start_transaction_verification`].
    ///
    /// The log mutex can be held by an append that waits on the disk, so the
    /// call leaves normal async polling the way the writer's appends do,
    /// through [`crate::blocking::run_blocking`]: `block_in_place` on the
    /// multi-thread runtime, `spawn_blocking` on a current-thread one, and
    /// inline on `wasm32-wasip1`.
    ///
    /// # Errors
    ///
    /// Returns the log's refusal for a stale producer epoch.
    pub(crate) async fn start_transaction_verification(
        &self,
        batch: krabka_log::TransactionalBatch,
        supports_epoch_bump: bool,
        clock: (i64, i64),
    ) -> Result<krabka_log::VerificationGuard, krabka_log::TransactionAppendRefusal> {
        let log = std::sync::Arc::clone(&self.log);
        let start = move || {
            log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .maybe_start_transaction_verification(batch, supports_epoch_bump, clock)
        };
        run_blocking(start).await.unwrap_or(Err(
            krabka_log::TransactionAppendRefusal::InvalidTransactionState,
        ))
    }

    /// Push `overrides` through the writer actor so the partition's `Log`
    /// picks up the new `retention.ms`, `retention.bytes`, and
    /// `segment.bytes` on the next retention or roll tick. The caller has
    /// already validated `overrides`; see `config_keys`. The call is
    /// idempotent, so the same map pushed twice is a cheap noop.
    /// `ReplicatorSupervisor::reconcile` calls this every time the metadata
    /// image changes.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Replication` if the writer is dead or the
    /// ack is dropped.
    pub(crate) async fn apply_log_config_overrides(
        &self,
        overrides: &std::collections::BTreeMap<String, String>,
        base: &krabka_log::LogConfig,
    ) -> Result<(), BrokerError> {
        let merged = apply_to_log_config(overrides, base);
        self.ask_writer(|ack| WriterMessage::SetLogConfig {
            config: merged,
            ack,
        })
        .await?;
        Ok(())
    }

    /// Append a leader-assigned batch to the local log and keep its
    /// `base_offset`. The per-partition replicator on a follower broker
    /// calls this. It sends the batch through the writer task so the batch
    /// stays ordered with produce appends. On a follower the produce handler
    /// rejects those appends anyway, but the channel ordering is still part
    /// of the invariant.
    pub async fn replicate_batch(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::Replicate { batch, ack })
            .await?
    }

    pub async fn replicate_verbatim(
        &self,
        batch: krabka_log::VerbatimBatch,
        base_offset: Offset,
    ) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::ReplicateVerbatim {
            batch,
            base_offset,
            ack,
        })
        .await?
    }

    /// Truncate the log to `offset` and drop all records at offsets
    /// `>= offset`. The replicator's `OFFSET_OUT_OF_RANGE` recovery path
    /// calls this, and so does the KIP-320 in-band `diverging_epoch`
    /// truncation path, which passes the leader's epoch boundary and not 0.
    pub async fn truncate_to(&self, offset: Offset) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::Truncate { offset, ack })
            .await?
    }

    /// The lowest offset the log was cut to by [`Self::truncate_to`] or
    /// [`Self::reset_to`] since the last call, which the KIP-113 move task
    /// applies to its future log. See [`WriterMessage::TakeFutureTruncation`].
    pub(crate) async fn take_future_truncation(&self) -> Result<Option<Offset>, BrokerError> {
        self.ask_writer(|ack| WriterMessage::TakeFutureTruncation { ack })
            .await
    }

    /// Drop every segment and recreate the active segment at `new_base`.
    /// The request goes through the writer task, so it stays ordered with
    /// appends.
    pub async fn reset_to(&self, new_base: Offset) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::ResetTo { new_base, ack })
            .await?
    }

    /// Send a trim request through the writer actor. Returns the resulting
    /// `log_start_offset`. The `DeleteRecords` handler calls this.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` if the writer is dead, the ack is dropped,
    /// or the underlying `Log::trim_to_offset` fails (negative offset).
    pub async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        self.ask_writer(|ack| WriterMessage::TrimToOffset { new_start, ack })
            .await?
    }

    /// Send a `WriterMessage::Compact` to the partition's writer
    /// actor and await the ack. The broker-wide [`Cleaner`] ticker
    /// calls this.
    pub async fn compact_log(&self) -> Result<(), BrokerError> {
        self.ask_writer_or(
            |ack| WriterMessage::Compact { ack },
            BrokerError::Replication,
            "compact ack dropped",
        )
        .await?
    }

    /// Send a `WriterMessage::Retain` to the partition's writer actor and
    /// await the ack. The broker-wide log-retention sweep
    /// ([`crate::log_retention`]) calls this.
    ///
    /// Retention deletes segment files, so it goes through the writer for the
    /// same reason [`Partition::compact_log`] does: the writer task owns the
    /// only `&mut Log`, and a background task taking the log mutex directly
    /// would run a segment deletion concurrently with an append.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Replication` if the writer is dead or the ack is
    /// dropped, and whatever `Log::tick` returned otherwise.
    pub async fn retain_log(&self) -> Result<(), BrokerError> {
        self.ask_writer_or(
            |ack| WriterMessage::Retain { ack },
            BrokerError::Replication,
            "retain ack dropped",
        )
        .await?
    }

    /// Append `batch` to the local log at the next assigned offset. The append
    /// goes through the partition's writer task, so it stays ordered with
    /// all other produce appends. Returns the assigned `base_offset`.
    ///
    /// `TxnCoordinator::put` uses this to persist `__transaction_state`
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the writer task is dead or the ack
    /// channel closes before the writer replies.
    pub(crate) async fn produce_batch(&self, batch: RecordBatch) -> Result<Offset, BrokerError> {
        self.produce_batch_outcome(batch)
            .await
            .map_err(|error| match error {
                ProduceBatchError::Rejected(error) => error,
                ProduceBatchError::Indeterminate(error) => BrokerError::Txn(error),
            })
    }

    pub(crate) async fn produce_batch_outcome(
        &self,
        batch: RecordBatch,
    ) -> Result<Offset, ProduceBatchError> {
        self.produce_batch_checked(batch, None).await
    }

    /// [`Self::produce_batch_outcome`] with the producer transaction check the
    /// log runs under its append lock, just before the append. A coordinator
    /// that writes a producer's transactional records for it, such as
    /// `TxnOffsetCommit`, passes the check its verification produced.
    pub(crate) async fn produce_batch_checked(
        &self,
        batch: RecordBatch,
        producer_check: Option<ProducerAppendCheck>,
    ) -> Result<Offset, ProduceBatchError> {
        let job = |ack| {
            WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(batch),
                ack,
                producer_check,
            })
        };
        ask(&self.writer_tx, job)
            .await
            .map_err(|error| match error {
                AskError::Closed => {
                    ProduceBatchError::Rejected(BrokerError::Txn("partition writer dead".into()))
                }
                AskError::Dropped => {
                    ProduceBatchError::Indeterminate("produce acknowledgement dropped".into())
                }
            })?
            .map(|appended| appended.base_offset)
            .map_err(ProduceBatchError::Rejected)
    }

    /// Append a batch and acknowledge it only after its resulting log prefix
    /// is durable in the partition's configured storage medium.
    pub(crate) async fn produce_batch_durable_outcome(
        &self,
        batch: RecordBatch,
    ) -> Result<Offset, ProduceBatchError> {
        let record_count = i64::from(batch.last_offset_delta) + 1;
        let base_offset = self.produce_batch_outcome(batch).await?;
        let leo = base_offset + record_count;
        ask(&self.writer_tx, |ack| WriterMessage::SyncDurable {
            leo,
            ack,
        })
        .await
        .map_err(|error| {
            ProduceBatchError::Indeterminate(
                match error {
                    AskError::Closed => "durable sync command rejected",
                    AskError::Dropped => "durable sync ack dropped",
                }
                .into(),
            )
        })?
        .map_err(|error| ProduceBatchError::Indeterminate(error.to_string()))?;
        Ok(base_offset)
    }

    /// Append an internally built COMMIT marker with a coordinator-supplied
    /// commit stamp. The partition writer keeps it ordered with all produce
    /// and replication appends.
    ///
    /// # Errors
    /// Returns an error if the writer task is unavailable or the log rejects
    /// the marker/stamp pair.
    pub(crate) async fn produce_commit_marker(
        &self,
        batch: RecordBatch,
        commit_stamp: u64,
    ) -> Result<Offset, BrokerError> {
        let job = |ack| {
            WriterMessage::Produce(ProduceJob {
                data: ProduceData::OwnedCommitMarker {
                    batch,
                    commit_stamp,
                },
                ack,
                producer_check: None,
            })
        };
        Ok(self
            .ask_writer_or(job, BrokerError::Txn, "ack dropped")
            .await??
            .base_offset)
    }

    /// Append an internally built control batch, such as a transaction ABORT
    /// marker or a barrier marker.
    ///
    /// The partition writer keeps it ordered with all produce and replication
    /// appends, and appends it without the compression rewrite that
    /// [`Self::produce_batch`] applies.
    ///
    /// The caller stamps `partition_leader_epoch` before it calls this
    /// function. The writer does not stamp it, and a batch that keeps the
    /// default of zero carries a false leader epoch in its header.
    ///
    /// # Errors
    /// Returns [`BrokerError::Txn`] if the writer task is dead or the ack
    /// channel closes before the writer replies, or the log rejects the batch.
    pub(crate) async fn produce_control_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<Offset, BrokerError> {
        Ok(self
            .ask_writer_or(
                |ack| {
                    WriterMessage::Produce(ProduceJob {
                        data: ProduceData::OwnedControl(batch),
                        ack,
                        producer_check: None,
                    })
                },
                BrokerError::Txn,
                "ack dropped",
            )
            .await??
            .base_offset)
    }

    /// Test-only: shift the partition's in-memory `log_start_offset` to
    /// `new_start`. The request goes through the writer task to keep the
    /// single-writer invariant on the underlying `Log`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn test_set_log_start(&self, new_start: Offset) -> Result<(), BrokerError> {
        self.ask_writer(|ack| WriterMessage::TestSetLogStart { new_start, ack })
            .await?
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::partition::test_support::test_partition_with_writer;

    #[tokio::test]
    async fn test_set_log_start_updates_log_start_through_writer() {
        let (p, _td) = test_partition_with_writer();

        p.test_set_log_start(Offset(5))
            .await
            .expect("set log start");

        assert!(p.log_start_offset() == 5);
    }

    /// Kafka trunk's `UnifiedLog.appendAsFollower` skips `LogValidator`, the one
    /// place an append reads `max.decompressed.message.bytes`, so a follower
    /// takes the compressed record above the limit that its leader admitted.
    /// Both writes of a follower's Fetch loop are pinned: an owned batch, which
    /// is how a control batch arrives, and the verbatim bytes a data batch
    /// arrives as.
    #[tokio::test]
    async fn a_follower_takes_a_compressed_record_above_the_decompressed_limit() {
        use krabka_ids::{LeaderEpoch, ProducerId};
        use krabka_protocol::records::{Attributes, Record};

        let (p, _td) = test_partition_with_writer();
        {
            let log = p.log.lock().expect("partition log lock");
            let config = krabka_log::LogConfig {
                max_decompressed_record: Some(krabka_units::bytes(100)),
                ..log.config_snapshot()
            };
            log.set_config(config);
        }
        let oversized = |base_offset| RecordBatch {
            base_offset,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            attributes: Attributes::default()
                .with_compression(krabka_compression::CompressionType::Gzip),
            records: vec![Record {
                value: Some(bytes::Bytes::from(vec![7_u8; 1_000])),
                ..Default::default()
            }],
            ..Default::default()
        };

        p.replicate_batch(oversized(0))
            .await
            .expect("the owned batch is taken");
        assert!(p.log.lock().expect("partition log lock").log_end_offset() == 1);

        let producer = oversized(1);
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).expect("encode the batch");
        p.replicate_verbatim(
            krabka_log::VerbatimBatch {
                bytes: wire.freeze(),
                last_offset_delta: producer.last_offset_delta,
                max_timestamp: producer.max_timestamp,
                leader_epoch: LeaderEpoch(0),
                producer_id: ProducerId(-1),
                producer_epoch: -1,
                base_sequence: -1,
                is_transactional: false,
            },
            Offset(1),
        )
        .await
        .expect("the verbatim batch is taken");
        assert!(p.log.lock().expect("partition log lock").log_end_offset() == 2);
    }
}
