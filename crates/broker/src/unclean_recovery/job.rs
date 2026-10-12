//! The recovery job, its outcome, and the handle that enqueues one.
//!
//! This is the manager's inbound API. The failover path and the `ElectLeaders`
//! handler hold an `UncleanRecoveryHandle` and post a `RecoveryJob` on it; the
//! admin path also keeps the reply channel that carries the `RecoveryOutcome`
//! back.

use krabka_raft::NodeId;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;
use uuid::Uuid;

use crate::config_keys::RecoveryStrategy;

/// A request to run unclean recovery for one partition, if it is needed. The
/// failover path and the `ElectLeaders` handler enqueue it, and the URM
/// services it.
pub(crate) struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// Optional reply channel. The admin-triggered `ElectLeaders` path wants
    /// the outcome. The background failover path sends the job and does not
    /// wait for a reply.
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
    /// KFC-9: the break-glass proposal that authorized this recovery, and
    /// `None` when the controller started it on its own.
    ///
    /// The `ElectLeaders` handler spends the approval before it enqueues the
    /// job, so this id is evidence and not an authorization: it says that a
    /// person asked for this recovery, which is what takes the job out of
    /// [`BackgroundRecovery`](super::BackgroundRecovery)'s reach. The two
    /// background sites have no caller to ask, so they send `None`.
    pub proposal: Option<Uuid>,
}

/// Result of attempting unclean recovery for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// The URM elected a new leader and submitted the change. This variant
    /// carries the id.
    Elected(NodeId),
    /// No surviving replica could serve as a leader.
    NoEligibleReplica,
    /// Recovery was unnecessary. The leader is alive, or this node is not the
    /// controller leader, or the partition is gone.
    NotNeeded,
    /// The selection or controller admission changed before commit, so the
    /// URM aborted this attempt. A fresh job must select against current state.
    Stale,
    /// Another recovery for the same `(topic, partition)` is already running.
    InProgress,
    /// KFC-9: `break_glass.background_unclean_recovery` is `require`, and no
    /// proposal approved this recovery. The partition keeps no leader and
    /// stays visibly offline.
    BreakGlassRequired,
    /// Fail-closed audit storage could not durably record the election intent.
    AuditUnavailable,
}

/// Cloneable handle that enqueues [`RecoveryJob`] values onto the URM task.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    pub(super) tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    #[cfg(test)]
    pub(crate) fn for_tests(tx: mpsc::Sender<RecoveryJob>) -> Self {
        Self { tx }
    }

    /// Enqueues a recovery job. It logs a message, and does not panic, if the
    /// manager has shut down.
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_sends_job_to_channel() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = UncleanRecoveryHandle::for_tests(tx);

        let job = RecoveryJob {
            topic: "test-topic".into(),
            partition: 0,
            strategy: RecoveryStrategy::Balanced,
            reply: None,
            proposal: None,
        };

        handle.enqueue(job).await;
        let received = rx.recv().await.expect("job received");
        assert2::check!(received.topic == "test-topic");
        assert2::check!(received.partition == 0);
    }
}
