//! A transaction marker that the leader of its partition appends with
//! `requiredAcks=-1`, and the wait for the marker to commit.
//!
//! Kafka's `KafkaApis.handleWriteTxnMarkersRequest` appends the markers
//! through `ReplicaManager.appendRecords` with `requiredAcks=-1` and the
//! broker's `request.timeout.ms`. It answers a partition only when its
//! `DelayedProduce` completes, that is when the high watermark covers the
//! marker. The transaction coordinator writes `CompleteCommit` or
//! `CompleteAbort` only after every partition answered `NONE`.
//!
//! A marker that a leader answered at its local append can die with that
//! leader before a follower fetched it. The coordinator then records the
//! transaction complete, and the next leader of the partition holds the
//! transaction open permanently. The last stable offset of the partition
//! stops at the first record of the transaction, and a `read_committed`
//! consumer reads nothing after it.

use {
    super::{MarkerAppend, materialize::append_marker_and_materialize},
    crate::{
        codes,
        coordinator::GroupCoordinator,
        error::BrokerError,
        partition::{Partition, Uncommitted},
    },
    krabka_log::Offset,
    krabka_metadata::NodeId,
    std::{
        sync::{Arc, atomic::Ordering},
        time::{Duration, Instant},
    },
};

#[cfg(test)]
mod tests;

/// How long a marker waits to commit: the default of Kafka's broker
/// `request.timeout.ms`, which `handleWriteTxnMarkersRequest` passes to
/// `appendRecords`.
pub(crate) const MARKER_COMMIT_TIMEOUT: Duration = Duration::from_secs(30);

/// A marker in this broker's log of its partition, whose commit is not
/// confirmed yet.
pub(crate) struct PendingMarker {
    partition: Arc<Partition>,
    /// The leader and the leader epoch that the partition had installed when
    /// it took the marker.
    term: (u64, i32),
    /// The offset that the high watermark has to reach.
    end_offset: Offset,
}

/// Append `marker` to `partition` as its leader, as Kafka's
/// `Partition.appendRecordsToLeader` appends with `requiredAcks=-1`, and
/// return the wait for the marker to commit.
///
/// The transition read guard of the partition spans the leadership check and
/// the append, as it does for a Produce. A leadership change takes the write
/// guard, so it cannot move the partition to a follower between the check
/// and the append. The function releases the guard before the commit wait. A
/// wait that held it would hold the leadership change back, and the wait
/// could not see the change that ends it.
///
/// A diskless partition skips the leader check, as a diskless Produce does,
/// and the ISR check, because its WAL quorum makes a record durable.
///
/// # Errors
///
/// Returns [`BrokerError::MarkerWriteRefused`] with `NOT_LEADER_OR_FOLLOWER`
/// when the partition has another leader installed, and with
/// `NOT_ENOUGH_REPLICAS` when its ISR is smaller than `min.insync.replicas`.
/// The function appends nothing then. Returns the error of
/// `append_marker_and_materialize` when the append itself fails.
pub(crate) async fn append_marker_as_leader(
    partition: &Arc<Partition>,
    node_id: NodeId,
    group_coordinator: Option<&Arc<GroupCoordinator>>,
    topic: &str,
    marker: MarkerAppend,
) -> Result<PendingMarker, BrokerError> {
    let transition = partition.lock_produce_transition().await;
    if transition.leader_node_id != node_id && !partition.diskless {
        tracing::debug!(
            topic,
            partition = partition.index.get(),
            leader = transition.leader_node_id.0,
            "transaction marker: partition not led here"
        );
        return Err(refused(partition, codes::NOT_LEADER_OR_FOLLOWER));
    }
    if !partition.diskless && partition.replica_state.lock().await.under_min_isr() {
        return Err(refused(partition, codes::NOT_ENOUGH_REPLICAS));
    }
    let end_offset =
        append_marker_and_materialize(partition, group_coordinator, topic, marker).await?;
    let term = (transition.leader_node_id.0, transition.leader_epoch.0);
    drop(transition);
    Ok(PendingMarker {
        partition: Arc::clone(partition),
        term,
        end_offset,
    })
}

impl PendingMarker {
    /// Wait until the marker commits, as Kafka's `DelayedProduce` waits: the
    /// high watermark covers the marker while the partition keeps the leader
    /// and the leader epoch that took it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::MarkerWriteRefused`] with the code Kafka answers
    /// for the partition: `NOT_LEADER_OR_FOLLOWER` when the partition installs
    /// another leader or leader epoch first, as `ReplicaManager.makeFollowers`
    /// completes the delayed produce; `REQUEST_TIMED_OUT` when `deadline`
    /// passes first; and `NOT_ENOUGH_REPLICAS_AFTER_APPEND` when the high
    /// watermark covers the marker but the ISR is smaller than
    /// `min.insync.replicas`, as `Partition.checkEnoughReplicasReachOffset`
    /// answers.
    pub(crate) async fn committed(self, deadline: Instant) -> Result<(), BrokerError> {
        let (leader, epoch) = self.term;
        let waited = self
            .partition
            .await_committed_while(self.end_offset, deadline, None, |partition, _| {
                partition.current_leader.load(Ordering::Acquire) == leader
                    && partition.current_leader_epoch.load(Ordering::Acquire) == epoch
            })
            .await;
        let code = match waited {
            Err(Uncommitted::TermEnded) => codes::NOT_LEADER_OR_FOLLOWER,
            Err(Uncommitted::TimedOut) => codes::REQUEST_TIMED_OUT,
            Ok(())
                if !self.partition.diskless
                    && self.partition.replica_state.lock().await.under_min_isr() =>
            {
                codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND
            }
            Ok(()) => return Ok(()),
        };
        Err(refused(&self.partition, code))
    }
}

/// The refusal of a marker on `partition` with the Kafka error `code`.
fn refused(partition: &Partition, code: i16) -> BrokerError {
    BrokerError::MarkerWriteRefused {
        code,
        message: format!(
            "transaction marker for {}-{} answered error code {code}",
            partition.topic,
            partition.index.get()
        ),
    }
}
