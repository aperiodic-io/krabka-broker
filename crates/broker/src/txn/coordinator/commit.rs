//! The commit rule of a `__transaction_state` write.
//!
//! Kafka's `TransactionStateManager.appendTransactionToLog` appends a state
//! record with `acks=-1`, under the coordinator epoch it read, and changes its
//! cache only in the callback of a complete append. `ReplicaManager` completes
//! that append once the high watermark covers the record, fails it with
//! `NOT_LEADER_OR_FOLLOWER` when the broker stops leading the partition first,
//! refuses it up front with `NOT_ENOUGH_REPLICAS` when the ISR is below
//! `min.insync.replicas`, and fails it with `REQUEST_TIMED_OUT` after the
//! transaction's timeout. The callback maps each append error to the
//! coordinator error the client gets, see [`coordinator_append_error`], and a
//! coordinator epoch that changed meanwhile is `NOT_COORDINATOR`.
//!
//! A coordinator that answered at the local append, as this one once did,
//! could hand a client a `Prepare*`, a `Complete*`, an epoch bump, or a
//! partition registration that the next leader of the partition never got.
//! `EndTxn` then fans markers out for a transaction the next coordinator still
//! holds as `Ongoing`, and exactly-once breaks on failover.

use std::{sync::Arc, time::Duration};

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::{LeaderEpoch, MetadataImage, NodeId};
use krabka_protocol::records::RecordBatch;
use tokio::sync::watch;

use super::{TxnCoordinator, leadership};
use crate::{
    codes,
    error::BrokerError,
    partition::{Partition, ProduceBatchError},
    txn::bootstrap,
};

#[cfg(test)]
mod tests;

/// How long an expiry tombstone waits to commit: the default of Kafka's
/// `request.timeout.ms`, which `writeTombstonesForExpiredTransactionalIds`
/// passes to `appendRecords`.
pub(super) const TOMBSTONE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a state transition waits to commit: the transaction's own
/// timeout, which `appendTransactionToLog` passes to `appendRecords`.
pub(super) fn transition_timeout(txn_timeout_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(txn_timeout_ms).unwrap_or(0))
}

/// One loaded term of a `__transaction_state` partition on this broker: the
/// coordinator epoch of Kafka's cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StateTerm {
    pub(super) partition: PartitionIndex,
    /// The load of this term. A newer term has a different generation.
    pub(super) generation: u64,
    /// The partition leader epoch this broker leads the partition at.
    pub(super) leader_epoch: LeaderEpoch,
}

/// The coordinator error that Kafka's `appendTransactionToLog` answers for
/// the error of its `__transaction_state` append.
fn coordinator_append_error(code: i16) -> i16 {
    match code {
        // Kafka answers a timed-out append `COORDINATOR_NOT_AVAILABLE`, so
        // that the client retries.
        codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::NOT_ENOUGH_REPLICAS
        | codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND
        | codes::REQUEST_TIMED_OUT => codes::COORDINATOR_NOT_AVAILABLE,
        codes::NOT_LEADER_OR_FOLLOWER | codes::KAFKA_STORAGE_ERROR => codes::NOT_COORDINATOR,
        codes::MESSAGE_TOO_LARGE | codes::RECORD_LIST_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => other,
    }
}

/// A `__transaction_state` write of `partition` that failed with the append
/// error `append_code`, answered with Kafka's coordinator error for it.
fn uncommitted(partition: PartitionIndex, append_code: i16) -> BrokerError {
    BrokerError::TransactionStateWriteUncommitted {
        partition: partition.get(),
        code: coordinator_append_error(append_code),
    }
}

/// Why `image` refuses an `acks=-1` append in `term`, as the append error
/// Kafka's `Partition.appendRecordsToLeader` raises, or `None` when it admits
/// it: this broker must lead the partition at the term's leader epoch, and
/// the ISR must hold `min.insync.replicas` members.
fn append_refusal(image: &MetadataImage, node_id: NodeId, term: StateTerm) -> Option<i16> {
    term_refusal(image, node_id, term, codes::NOT_ENOUGH_REPLICAS)
}

/// Why `image` fails an `acks=-1` append in `term` whose records the high
/// watermark covers, as Kafka's `Partition.checkEnoughReplicasReachOffset`
/// answers it, or `None` when the append succeeded: the ISR may have shrunk
/// below `min.insync.replicas` after the append, and the leadership may have
/// moved.
fn completion_refusal(image: &MetadataImage, node_id: NodeId, term: StateTerm) -> Option<i16> {
    term_refusal(
        image,
        node_id,
        term,
        codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    )
}

/// Check the held leadership before applying the append phase's ISR error.
fn term_refusal(
    image: &MetadataImage,
    node_id: NodeId,
    term: StateTerm,
    isr_error: i16,
) -> Option<i16> {
    if !term_holds_in(image, node_id, term) {
        return Some(codes::NOT_LEADER_OR_FOLLOWER);
    }
    isr_below_min(image, node_id, term).then_some(isr_error)
}

/// Whether the ISR of the term's partition in `image` is smaller than its
/// effective `min.insync.replicas`: the configured value, which
/// `__transaction_state` carries as a topic config, capped at the replica
/// count, as Kafka's `Partition.effectiveMinIsr` reads it.
fn isr_below_min(image: &MetadataImage, node_id: NodeId, term: StateTerm) -> bool {
    let Some(record) = image.partition(bootstrap::TOPIC, term.partition.get()) else {
        return false;
    };
    let min_isr = crate::config_keys::node_min_insync_replicas(image, node_id, bootstrap::TOPIC)
        .map_or(1, |configured| usize::try_from(configured).unwrap_or(1))
        .min(record.replicas.len());
    record.isr.len() < min_isr
}

/// Whether `image` still names this broker the leader of the term's
/// partition, at the term's leader epoch.
///
/// A new epoch fails the term even when it names this broker again: the image
/// watch keeps only the newest image, so a leadership that moved away and
/// came back between two reads could have truncated the write.
fn term_holds_in(image: &MetadataImage, node_id: NodeId, term: StateTerm) -> bool {
    image
        .partition(bootstrap::TOPIC, term.partition.get())
        .is_some_and(|record| record.leader == node_id && record.leader_epoch == term.leader_epoch)
}

impl TxnCoordinator {
    /// The loaded term of `partition`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] with the
    /// coordinator error of the partition when it is not loaded:
    /// `COORDINATOR_LOAD_IN_PROGRESS` while it loads, and `NOT_COORDINATOR`
    /// otherwise, as Kafka's `getTransactionState` answers.
    pub(super) async fn loaded_term(
        &self,
        partition: PartitionIndex,
    ) -> Result<StateTerm, BrokerError> {
        let leaders = self.leader_partitions.read().await;
        let loaded = leaders.get(&partition).and_then(|leadership| {
            leadership
                .term
                .filter(|term| term.status == leadership::LoadStatus::Loaded)
                .map(|term| StateTerm {
                    partition,
                    generation: term.generation,
                    leader_epoch: leadership.leader_epoch,
                })
        });
        loaded.ok_or_else(|| Self::term_error(&leaders, partition))
    }

    /// The error of a write whose term is not, or is no longer, the loaded
    /// term of `partition`: the coordinator error of the partition now, and
    /// `NOT_COORDINATOR` when a newer term has loaded it. Kafka's callback
    /// answers a changed coordinator epoch `NOT_COORDINATOR` too.
    pub(super) fn term_error(
        leaders: &leadership::StatePartitionLeaders,
        partition: PartitionIndex,
    ) -> BrokerError {
        BrokerError::TransactionStateWriteUncommitted {
            partition: partition.get(),
            code: leadership::coordinator_error(leadership::status(leaders, partition))
                .unwrap_or(codes::NOT_COORDINATOR),
        }
    }

    /// Whether `term` is still the loaded term of its partition.
    async fn term_is_current(&self, term: StateTerm) -> bool {
        leadership::loaded_generation(&*self.leader_partitions.read().await, term.partition)
            == Some(term.generation)
    }

    /// Appends `batch` to the partition of `term` as Kafka's
    /// `appendTransactionToLog` does, in `term`, and returns once the high
    /// watermark covers it.
    ///
    /// `term` is the loaded term the caller read before it built the batch.
    /// The batch carries its leader epoch. The caller publishes its state only
    /// after this returns, and only while `term` is still the loaded one.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] with the
    /// coordinator error the client gets.
    pub(super) async fn append_committed(
        &self,
        term: StateTerm,
        mut batch: RecordBatch,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let partition = term.partition;
        // Subscribe before the leadership checks, so the commit wait sees
        // every image after the one the checks read.
        let mut images = self.metadata.as_ref().map(|metadata| {
            let mut images = metadata.watch_image();
            images.borrow_and_update();
            images
        });
        Self::require_generation(
            &*self.leader_partitions.read().await,
            partition,
            term.generation,
        )?;
        if let Some(refusal) = images
            .as_ref()
            .and_then(|images| append_refusal(&images.borrow(), self.node_id, term))
        {
            return Err(uncommitted(partition, refusal));
        }
        let Some(part) = self.partitions.get(bootstrap::TOPIC, partition) else {
            return Err(uncommitted(partition, codes::NOT_LEADER_OR_FOLLOWER));
        };
        // `UnifiedLog.appendAsLeader` stamps the leader epoch on the batch.
        // The partition writer does not stamp an owned batch.
        batch.partition_leader_epoch = term.leader_epoch.get();
        let records = i64::from(batch.last_offset_delta) + 1;
        let base_offset = part
            .produce_batch_outcome(batch)
            .await
            .map_err(|error| match error {
                ProduceBatchError::Rejected(error) => {
                    uncommitted(partition, codes::from_broker_error(&error))
                }
                // The writer took the batch and went away before it answered,
                // as it does when the partition stops on this broker.
                ProduceBatchError::Indeterminate(_) => {
                    uncommitted(partition, codes::NOT_LEADER_OR_FOLLOWER)
                }
            })?;
        self.await_state_committed(
            &part,
            images.as_mut(),
            term,
            Offset(base_offset.get() + records),
            timeout,
        )
        .await
    }

    /// Wait until the high watermark of `part` reaches `end_offset`, the
    /// offset after the appended batch, while `term` holds.
    ///
    /// This is the `DelayedProduce` of an `acks=-1` append. The term holds
    /// while the coordinator's load of the partition is the term's and, when
    /// the coordinator has `images`, while the image names this broker leader
    /// at the term's epoch. The high watermark alone is not enough: a former
    /// leader that follows the new one can see its high watermark pass
    /// `end_offset` over records that replaced the write.
    async fn await_state_committed(
        &self,
        part: &Partition,
        mut images: Option<&mut watch::Receiver<Arc<MetadataImage>>>,
        term: StateTerm,
        end_offset: Offset,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let lost = || uncommitted(term.partition, codes::NOT_LEADER_OR_FOLLOWER);
        let deadline = std::time::Instant::now() + timeout;
        let committed = part.await_hw_at_least(end_offset, deadline);
        tokio::pin!(committed);
        loop {
            // Register for the next term change before reading the term, so a
            // change between the read and the wait still wakes the wait.
            let term_changed = self.leadership_changed.notified();
            tokio::pin!(term_changed);
            term_changed.as_mut().enable();
            if !self.term_is_current(term).await {
                return Err(lost());
            }
            let image_changed =
                crate::metadata_source::wait_for_image_change(images.as_deref_mut());
            tokio::select! {
                reached = &mut committed => {
                    if reached.is_err() {
                        return Err(uncommitted(term.partition, codes::REQUEST_TIMED_OUT));
                    }
                    let refusal = images.as_deref_mut().and_then(|images| {
                        completion_refusal(&images.borrow_and_update(), self.node_id, term)
                    });
                    if let Some(refusal) = refusal {
                        return Err(uncommitted(term.partition, refusal));
                    }
                    if !self.term_is_current(term).await {
                        return Err(lost());
                    }
                    return Ok(());
                }
                () = &mut term_changed => {}
                open = image_changed => {
                    let image_holds = images
                        .as_deref_mut()
                        .is_some_and(|images| {
                            term_holds_in(&images.borrow_and_update(), self.node_id, term)
                        });
                    if !open || !image_holds {
                        return Err(lost());
                    }
                }
            }
        }
    }
}
