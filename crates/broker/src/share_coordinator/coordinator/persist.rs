//! The durable append path for `__share_group_state`, the wait for its
//! commit, and the best-effort prune of that partition's log prefix.
//!
//! `persist_record` is the single write path that `initialize`, `write`,
//! `delete`, and the background jobs share. `await_committed` is the wait
//! that every operation makes before it answers. `maybe_prune` is the KIP-932
//! log trim that the prune timer in `jobs` runs. All three talk to the
//! partition log rather than to the delivery state machine, so they live
//! apart from `state_machine`.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::Mutex;
use tracing::warn;

use super::{ShareCoordinator, ShareErrorCode, ShareStateError, Term, message};
use crate::{
    codes,
    error::BrokerError,
    partition::Partition,
    share_coordinator::{
        bootstrap,
        persistence::{ShareStateKey, encode_state_key},
        pruning::redundant_offset,
        state::SharePartitionState,
    },
};

#[cfg(test)]
mod tests;

/// A write to `__share_group_state` that failed or did not commit.
#[derive(Debug, thiserror::Error)]
pub(super) enum AppendError {
    /// The partition log is not open on this broker.
    #[error("__share_group_state-{0} not local")]
    NotLocal(PartitionIndex),
    /// The partition does not lead on this broker at the leader epoch of the
    /// term. The check runs at the append, and again until the high
    /// watermark covers the record.
    #[error("__share_group_state-{0} does not lead on this broker in the term")]
    NotLeader(PartitionIndex),
    /// The record could not be encoded, or the partition log refused the
    /// append.
    #[error(transparent)]
    Failed(BrokerError),
    /// The append, or the wait for its commit, took longer than
    /// `share.coordinator.write.timeout.ms`.
    #[error("__share_group_state write timed out")]
    TimedOut,
}

impl AppendError {
    /// The error of the append as the coordinator answers it.
    ///
    /// The code follows Kafka's
    /// `CoordinatorOperationExceptionHelper.handleOperationException`. A
    /// partition that does not lead in the term answers as the runtime
    /// answers for a shard that it unloaded: `NOT_COORDINATOR`.
    pub(super) fn share_error(&self) -> ShareStateError {
        let append_code = match self {
            Self::NotLocal(_) => codes::UNKNOWN_TOPIC_OR_PARTITION,
            Self::NotLeader(_) => codes::NOT_COORDINATOR,
            // A local append that fails on the log or the writer task is a
            // storage failure in Kafka's terms.
            Self::Failed(BrokerError::Io(_) | BrokerError::Log(_) | BrokerError::Txn(_)) => {
                codes::KAFKA_STORAGE_ERROR
            }
            Self::Failed(error) => codes::from_broker_error(error),
            Self::TimedOut => codes::REQUEST_TIMED_OUT,
        };
        ShareStateError::Operation {
            code: operation_error_code(append_code),
            message: append_message(append_code),
        }
    }
}

/// Kafka's `handleOperationException` mapping from an append error code to
/// the code of the coordinator answer.
fn operation_error_code(append_code: ShareErrorCode) -> ShareErrorCode {
    codes::coordinator_append_error(append_code)
}

/// The message of the append error, before the mapping.
fn append_message(append_code: ShareErrorCode) -> &'static str {
    match append_code {
        codes::UNKNOWN_TOPIC_OR_PARTITION => message::UNKNOWN_TOPIC_OR_PARTITION,
        codes::KAFKA_STORAGE_ERROR => message::KAFKA_STORAGE_ERROR,
        codes::NOT_COORDINATOR => message::NOT_COORDINATOR,
        codes::REQUEST_TIMED_OUT => message::REQUEST_TIMED_OUT,
        _ => message::UNKNOWN_SERVER_ERROR,
    }
}

impl ShareCoordinator {
    /// Appends one `(key, value)` record to the state partition of `term`.
    ///
    /// This method returns the base offset of the record. A `None` value
    /// writes a tombstone. The record is in the local log only: the caller
    /// waits for its commit with [`ShareCoordinator::await_committed`].
    ///
    /// # Errors
    ///
    /// Returns [`AppendError::NotLocal`] if the partition log is not open
    /// locally, [`AppendError::NotLeader`] if the partition does not lead on
    /// this broker at the leader epoch of `term`, [`AppendError::Failed`] if
    /// the key does not encode or `produce_batch` fails, and
    /// [`AppendError::TimedOut`] if the append does not finish within the
    /// configured write timeout.
    pub(super) async fn persist_record(
        &self,
        term: Term,
        key: ShareStateKey,
        value: Option<Bytes>,
    ) -> Result<Offset, AppendError> {
        let part = self
            .partitions
            .get(bootstrap::TOPIC, term.partition)
            .ok_or(AppendError::NotLocal(term.partition))?;

        // Kafka's `share.coordinator.state.topic.compression.codec`. The
        // state topic keeps `compression.type=producer`, so the log stores
        // the batch in this codec.
        let mut batch = RecordBatch::default();
        batch.attributes = batch
            .attributes
            .with_compression(self.config.state_topic_compression_codec);
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(encode_state_key(&key).map_err(AppendError::Failed)?),
            value,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        tokio::time::timeout(
            self.config.write_timeout,
            self.append_as_leader(&part, term, batch),
        )
        .await
        .map_err(|_| AppendError::TimedOut)?
    }

    /// Appends `batch` as the leader of the partition in `term`, as Kafka's
    /// `Partition.appendRecordsToLeader` does.
    ///
    /// The leadership check and the append hold the leadership barrier of
    /// the partition. The metadata reconcile cannot install a new leader or
    /// epoch between them.
    async fn append_as_leader(
        &self,
        part: &Partition,
        term: Term,
        mut batch: RecordBatch,
    ) -> Result<Offset, AppendError> {
        let leadership = part.lock_produce_transition().await;
        if leadership.leader_node_id != self.node_id
            || leadership.leader_epoch.0 != term.leader_epoch
        {
            return Err(AppendError::NotLeader(term.partition));
        }
        // `UnifiedLog.appendAsLeader` stamps the leader epoch on the batch.
        // The partition writer does not stamp an owned batch.
        batch.partition_leader_epoch = term.leader_epoch;
        let base_offset = part
            .produce_batch(batch)
            .await
            .map_err(AppendError::Failed)?;
        drop(leadership);
        Ok(base_offset)
    }

    /// The offset after the last record of the state partition of `term`,
    /// Kafka's `lastWrittenOffset`, or `None` when the partition log is not
    /// open locally.
    ///
    /// Only the coordinator appends to `__share_group_state`, so this is the
    /// log end offset.
    pub(super) fn last_written(&self, term: Term) -> Option<Offset> {
        self.partitions
            .get(bootstrap::TOPIC, term.partition)
            .map(|part| part.log_end_offset())
    }

    /// Whether `part` leads on this broker at the leader epoch of `term`.
    fn leads(&self, part: &Partition, term: Term) -> bool {
        part.current_leader.load(Ordering::Acquire) == self.node_id.0
            && part.current_leader_epoch.load(Ordering::Acquire) == term.leader_epoch
    }

    /// Waits until the high watermark of the state partition of `term`
    /// reaches `end_offset`, while the term holds.
    ///
    /// This is the deferred completion of an operation in Kafka's
    /// `CoordinatorRuntime`. The runtime answers an operation when the high
    /// watermark passes the last record that the shard wrote. When the shard
    /// unloads because the broker lost the partition, the runtime fails the
    /// operation with `NOT_COORDINATOR`. After
    /// `share.coordinator.write.timeout.ms`, it fails the operation with a
    /// timeout.
    ///
    /// The high watermark alone is not enough. A former leader that follows
    /// the new one can see its high watermark pass `end_offset` over records
    /// that replaced the write.
    ///
    /// # Errors
    ///
    /// Returns [`AppendError::NotLeader`] if the partition log is not open
    /// locally, or if the partition leads at another broker or epoch before
    /// the high watermark reaches `end_offset`. Returns
    /// [`AppendError::TimedOut`] if the high watermark does not reach
    /// `end_offset` within the configured write timeout.
    pub(super) async fn await_committed(
        &self,
        term: Term,
        end_offset: Offset,
    ) -> Result<(), AppendError> {
        let Some(part) = self.partitions.get(bootstrap::TOPIC, term.partition) else {
            return Err(AppendError::NotLeader(term.partition));
        };
        let deadline = tokio::time::Instant::now() + self.config.write_timeout;
        loop {
            // Register for the next notice before the reads, so a notice
            // between the reads and the wait is not lost. The partition
            // sends the same notice when it installs a new leader.
            let notice = part.hw_advance_notify.notified();
            tokio::pin!(notice);
            // Read the high watermark before the leadership. This broker
            // installs a new leader before it fetches the records of that
            // leader, so a high watermark that the new leader brought comes
            // with a leadership that fails the check.
            let high_watermark = part.high_watermark().await;
            if !self.leads(&part, term) {
                return Err(AppendError::NotLeader(term.partition));
            }
            if high_watermark >= end_offset {
                return Ok(());
            }
            tokio::select! {
                () = &mut notice => {}
                () = tokio::time::sleep_until(deadline) => return Err(AppendError::TimedOut),
            }
        }
    }

    /// Prunes the log prefix of `state_partition` on a best-effort basis.
    ///
    /// This method computes `redundant_offset`, the smallest
    /// `last_snapshot_offset` across every live key mapped to that state
    /// partition. Kafka's prune is a write operation that writes no record,
    /// so the method then waits until every record of the partition is
    /// committed: a snapshot that the next leader can lose must not make the
    /// snapshot before it redundant. If `redundant_offset` is more than the
    /// current `log_start_offset` of the partition, the method trims the log
    /// up to it. Every retained key keeps its latest snapshot, so the trim is
    /// safe. The method logs each error and then discards it.
    pub(super) async fn maybe_prune(&self, state_partition: PartitionIndex) {
        let (term, redundant, last_written) = {
            let Ok(active) = self.active(state_partition).await else {
                return;
            };
            let Some(last_written) = self.last_written(active.term) else {
                return;
            };

            // Collect this partition's keys' last-snapshot offsets.
            let handles: Vec<Arc<Mutex<SharePartitionState>>> = self
                .state
                .iter()
                .filter(|e| {
                    let (g, t, p) = e.key();
                    self.state_partition_for(g, t, *p) == state_partition
                })
                .map(|e| e.value().clone())
                .collect();

            let mut offsets = Vec::with_capacity(handles.len());
            for h in handles {
                offsets.push(h.lock().await.last_snapshot_offset);
            }

            let Some(redundant) = redundant_offset(&offsets) else {
                return;
            };
            (active.term, redundant, last_written)
        };
        if let Err(error) = self.await_committed(term, last_written).await {
            warn!(
                partition = state_partition.get(),
                %error,
                "share-state log prune skipped: the records are not committed"
            );
            return;
        }
        let Some(part) = self.partitions.get(bootstrap::TOPIC, state_partition) else {
            return;
        };
        if redundant > part.log_start_offset()
            && self.leads(&part, term)
            && let Err(e) = part.trim_to_offset(redundant).await
        {
            warn!(
                partition = state_partition.get(),
                error = %e,
                "share-state log prune failed; continuing"
            );
        }
    }
}
