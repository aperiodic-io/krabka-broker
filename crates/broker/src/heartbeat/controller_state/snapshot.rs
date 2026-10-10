//! The queries the controller's maintenance loops ask of the registry.
//!
//! Fencing decides whether a live broker may be elected or receive replicas,
//! and the three snapshots answer "who is eligible", "who is unavailable" and
//! "whose session expired" under one lock acquisition each. The failover,
//! rebalance and metrics loops read the registry only through these.

use std::collections::HashSet;

use krabka_raft::NodeId;

use super::{BrokerLivenessState, ControllerLivenessState, registry::BrokerEntry};

impl ControllerLivenessState {
    /// Return the current liveness state for `broker_id`, or `None` if
    /// the broker has never sent a heartbeat.
    #[cfg(test)]
    pub(super) async fn state(&self, broker_id: u64) -> Option<BrokerLivenessState> {
        let map = self.brokers.lock().await;
        map.get(&broker_id).map(|e| e.state)
    }

    /// Return `true` if `broker_id` is active: `Alive` (it sent a heartbeat
    /// within the timeout window), unfenced, and not in controlled shutdown,
    /// which is Kafka's `ClusterControlManager.isActive`. Returns `false` for
    /// unknown brokers and for brokers whose heartbeat has expired.
    pub(crate) async fn is_alive(&self, broker_id: u64) -> bool {
        self.brokers
            .lock()
            .await
            .get(&broker_id)
            .is_some_and(BrokerEntry::is_active)
    }

    /// Apply the broker's fencing request. A broker can only unfence after it
    /// has caught up through its registration record. Returns the resulting
    /// fenced state.
    #[cfg(test)]
    pub(crate) async fn apply_fencing(
        &self,
        broker_id: u64,
        want_fence: bool,
        is_caught_up: bool,
    ) -> bool {
        let mut map = self.brokers.lock().await;
        let Some(entry) = map.get_mut(&broker_id) else {
            return true;
        };
        if want_fence {
            entry.fenced = true;
        } else if is_caught_up {
            entry.fenced = false;
        }
        entry.fenced
    }

    /// Snapshot the set of active broker ids under a single
    /// lock acquisition. This is equivalent to calling
    /// [`is_alive`](Self::is_alive) for every broker. But the cluster-wide
    /// maintenance loops for failover, rebalance, and metrics take the
    /// `brokers` lock once and then do synchronous set-membership checks.
    /// They do not take one `.await` lock per partition. Unknown brokers
    /// are absent from the set, so membership `false` means not alive.
    /// This matches `is_alive`'s predicate exactly.
    pub(crate) async fn alive_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, entry)| entry.is_active())
            .map(|(&id, _)| id)
            .collect()
    }

    /// [`alive_snapshot`](Self::alive_snapshot) as the [`NodeId`] set the
    /// failover and reassignment scans check membership against.
    pub(crate) async fn alive_node_ids(&self) -> HashSet<NodeId> {
        self.alive_snapshot()
            .await
            .into_iter()
            .map(NodeId)
            .collect()
    }

    /// Snapshot brokers that are known to be unavailable for new replica
    /// assignments. Unknown brokers are deliberately omitted: immediately
    /// after a controller election, registrations may be visible before the
    /// liveness registry has been seeded.
    pub(crate) async fn unavailable_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, entry)| entry.state == BrokerLivenessState::Dead || entry.fenced)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Snapshot the brokers whose heartbeat session has expired. Only the
    /// `Dead` state qualifies. A fenced but alive broker is not in the set,
    /// and neither is an unknown broker. The liveness ticker's sweep uses it
    /// to find dead brokers that still lead a partition or still sit in an
    /// ISR.
    pub(crate) async fn dead_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, entry)| entry.state == BrokerLivenessState::Dead)
            .map(|(&id, _)| id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::heartbeat::controller_state::{LivenessTransition, TestClock};

    #[tokio::test]
    async fn fencing_removes_alive_broker_from_eligible_snapshot() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        liveness.record_heartbeat(3).await;

        assert!(liveness.apply_fencing(3, true, true).await);
        assert!(!liveness.is_alive(3).await);
        assert!(!liveness.alive_snapshot().await.contains(&3));
        assert!(liveness.unavailable_snapshot().await.contains(&3));
    }

    #[tokio::test]
    async fn unavailable_snapshot_includes_dead_but_not_unknown_brokers() {
        let (_clock, liveness, _) = crate::test_support::expired_broker_fixture(2).await;

        let unavailable = liveness.unavailable_snapshot().await;
        assert!(unavailable.contains(&2));
        assert!(!unavailable.contains(&99));
    }

    #[tokio::test]
    async fn dead_snapshot_holds_expired_brokers_only() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(1).await;
        liveness.record_heartbeat(2).await;
        clock.advance(Duration::from_millis(11));
        // Broker 2 heartbeats again inside the new window. Broker 1 does not.
        // Broker 3 is alive but fenced.
        liveness.record_heartbeat(2).await;
        liveness.record_fenced_heartbeat(3).await;
        let transitions = liveness.tick().await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(1)]);

        let dead = liveness.dead_snapshot().await;
        assert!(dead == [1].into_iter().collect());
        // The fenced broker is unavailable but not dead.
        assert!(liveness.unavailable_snapshot().await.contains(&3));
        assert!(!dead.contains(&99));

        // A revival heartbeat empties the set.
        liveness.record_heartbeat(1).await;
        assert!(liveness.dead_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn broker_only_unfences_after_metadata_catch_up() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        liveness.record_fenced_heartbeat(3).await;

        assert!(liveness.apply_fencing(3, false, false).await);
        assert!(!liveness.is_alive(3).await);
        assert!(!liveness.apply_fencing(3, false, true).await);
        assert!(liveness.is_alive(3).await);
    }
}
