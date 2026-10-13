//! The two watermarks a partition publishes: the replication high watermark
//! that the ISR advances, and the deliver-at-time watermark that keeps a
//! scheduled batch out of a consumer's view. Both bound what a fetch may
//! return, so they are read, advanced, and waited on together.

use {
    crate::{
        delivery::{DeliveryWaker, PartitionDelivery},
        partition::Partition,
    },
    krabka_log::Offset,
    std::sync::Arc,
};

/// Returned by `await_hw_at_least` when the deadline elapses before
/// the High Watermark reaches the target offset.
#[derive(Debug)]
pub struct HwTimeout;

impl Partition {
    /// Cached High Watermark. Awaits `replica_state` cooperatively, so it
    /// does not block tokio worker threads.
    #[must_use]
    pub async fn high_watermark(&self) -> Offset { self.replica_state.lock().await.hw }

    /// Cached deliver-at-time watermark: the first offset that is not visible
    /// to a consumer yet. Reads a mirror, so it takes no lock and does no I/O.
    ///
    /// The value is as fresh as the last recompute left it. A fetch must not
    /// cap itself on it, because a batch can come due between two recomputes.
    /// A fetch calls [`Self::advance_delivery_watermark`], or recomputes on the
    /// log it already holds. This accessor is for a reader that must not take
    /// the log mutex, such as the metric sweep.
    #[must_use]
    pub(crate) fn delivery_watermark(&self) -> Offset { self.delivery.watermark() }

    /// Recompute the deliver-at-time watermark against `now_ms`, publish it to
    /// the mirror, and wake a long poll parked at the old value.
    ///
    /// Returns `None` on a topic that delivers immediately, which has no
    /// schedule to track. The mirror is refreshed either way, and an ordinary
    /// topic pays one uncontended mutex and no I/O for it.
    pub(crate) fn advance_delivery_watermark(&self, now_ms: i64) -> Option<PartitionDelivery> {
        self.delivery.publish(&self.log, now_ms)
    }

    /// Let an append to this partition re-arm the broker-wide delivery
    /// scheduler. The scheduler calls this on every sweep that sees the
    /// partition, and a repeat install of the same waker stores nothing.
    pub(crate) fn adopt_delivery_waker(&self, waker: &Arc<DeliveryWaker>) {
        self.delivery.adopt(waker);
    }

    /// KIP-392: record the high watermark the leader reported in a follower
    /// Fetch response, so that consumer reads served from this follower are
    /// bounded correctly. It clamps to the local log end, so the broker never
    /// exposes records it has not replicated yet, and it only advances `hw`,
    /// because HW is monotonic. It fires `hw_advance_notify` when the HW
    /// advances, so a consumer parked at the old HW wakes.
    pub async fn set_follower_hw(&self, reported_hw: Offset) {
        let log_end = self.log_end_offset();
        let new_hw = reported_hw.min(log_end);
        let advanced = {
            let mut st = self.replica_state.lock().await;
            if new_hw > st.hw {
                st.hw = new_hw;
                true
            } else {
                false
            }
        };
        if advanced {
            self.hw_advance_notify.notify_waiters();
        }
    }

    /// Install (or reinstall) the ISR membership and seed non-leader
    /// follower entries to 0. The replicator supervisor calls this
    /// when this broker materializes a partition where it is the leader.
    /// The call is idempotent, so a re-install of the same
    /// `(isr, replicas, leader)` keeps existing follower progress.
    ///
    /// `isr` is the committed in-sync set. `replicas` is the full replica
    /// assignment. Follower-progress tracking is keyed on `replicas`, so a
    /// replica that catches up toward ISR re-admission keeps its progress
    /// across reconciles. See
    /// [`crate::replica_state::ReplicaState::install_isr`].
    ///
    /// The method recomputes HW under the new ISR and fires
    /// `hw_advance_notify` if HW advanced. Diskless partitions deliberately
    /// skip this LEO-based HW recompute, because their client-visible HW
    /// advances only after WAL fsync.
    pub async fn install_isr(
        &self,
        isr: &[krabka_raft::NodeId],
        replicas: &[krabka_raft::NodeId],
        leader: krabka_raft::NodeId,
    ) {
        let leader_leo = self.log_end_offset();
        let mut st = self.replica_state.lock().await;
        let prev_hw = st.hw;
        st.install_isr(isr, replicas, leader, std::time::Instant::now());
        let new_hw = if self.diskless {
            st.hw
        } else {
            st.recompute_hw_for_leader_append(leader_leo)
        };
        drop(st);
        if new_hw > prev_hw {
            self.hw_advance_notify.notify_waiters();
        }
    }

    /// Wait until `replica_state.hw >= target_offset` or `deadline`
    /// elapses. The Produce handler calls this for `acks == -1` to gate
    /// the response on full replication.
    ///
    /// # Errors
    ///
    /// Returns `Err(HwTimeout)` if the deadline elapses before the HW
    /// advances. Returns `Ok(())` on the first re-check that satisfies
    /// the target.
    pub async fn await_hw_at_least(
        &self,
        target_offset: Offset,
        deadline: std::time::Instant,
    ) -> Result<(), HwTimeout> {
        loop {
            if self.high_watermark().await >= target_offset {
                return Ok(());
            }
            // Subscribe to the notify BEFORE re-reading HW so we don't
            // miss an advance that happens between read and await.
            let waiter = self.hw_advance_notify.notified();
            tokio::pin!(waiter);
            if self.high_watermark().await >= target_offset {
                return Ok(());
            }
            tokio::select! {
                () = &mut waiter => {},
                () = tokio::time::sleep_until(deadline.into()) => {
                    // Diagnostic: an acks=all produce gave up waiting for the HW
                    // to reach its appended offset. Dump the leader-side replica
                    // state so a failover stall (HW stuck because the ISR can't
                    // be satisfied) is observable — this path was previously
                    // silent. Cheap: only fires on a (rare) produce timeout.
                    let leader_leo = self.log_end_offset();
                    let st = self.replica_state.lock().await;
                    let mut isr: Vec<krabka_raft::NodeId> = st.isr.iter().copied().collect();
                    isr.sort_unstable();
                    let followers: Vec<(krabka_raft::NodeId, Offset)> =
                        st.per_follower.iter().map(|(k, v)| (*k, v.leo)).collect();
                    tracing::warn!(
                        target_offset = target_offset.0,
                        hw = st.hw.0,
                        leader_leo = leader_leo.0,
                        leader_epoch = st.current_leader_epoch.0,
                        ?isr,
                        ?followers,
                        "await_hw_at_least: acks=all produce timed out; HW below target offset"
                    );
                    return Err(HwTimeout);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
