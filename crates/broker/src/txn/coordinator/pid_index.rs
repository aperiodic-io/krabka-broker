//! The `producer_id` → `transactional_id` reverse index.
//!
//! The Produce handler reads the index to verify a transactional batch
//! (KIP-1319 v2), so a superseded producer id must not survive in it: a fenced
//! id that outlived its transaction would bypass coordinator validation. The
//! eviction that keeps only the live identities lives here, with the tests that
//! pin it.

use {
    super::TxnCoordinator,
    crate::{error::BrokerError, txn::state::TxnEntry},
    dashmap::DashMap,
    krabka_log::ProducerId,
    krabka_verified::transaction::{
        TransactionPidInstallDecision, transaction_pid_install_decision,
    },
    std::collections::HashMap,
};

#[derive(Default)]
pub(super) struct RecoveredTransactions {
    pub(super) state: HashMap<String, TxnEntry>,
    pub(super) pid_to_tid: HashMap<ProducerId, String>,
}

impl RecoveredTransactions {
    pub(super) fn apply_value(
        &mut self,
        entry: TxnEntry,
        partition_matches: bool,
    ) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.clone();
        let current_owner_matches = self
            .pid_to_tid
            .get(&entry.producer_id)
            .is_none_or(|owner| owner == &tid);
        let next_owner_matches = self
            .pid_to_tid
            .get(&entry.next_producer_id)
            .is_none_or(|owner| owner == &tid);
        validate_install(
            &entry,
            partition_matches,
            current_owner_matches,
            next_owner_matches,
        )?;

        self.pid_to_tid.retain(|_, owner| owner != &tid);
        self.pid_to_tid.insert(entry.producer_id, tid.clone());
        if !entry.next_producer_id.is_none() {
            self.pid_to_tid.insert(entry.next_producer_id, tid.clone());
        }
        self.state.insert(tid, entry);
        Ok(())
    }

    pub(super) fn apply_tombstone(&mut self, tid: &str) {
        self.state.remove(tid);
        self.pid_to_tid.retain(|_, owner| owner != tid);
    }
}

fn validate_install(
    entry: &TxnEntry,
    partition_matches: bool,
    current_owner_matches: bool,
    next_owner_matches: bool,
) -> Result<(), BrokerError> {
    let decision = transaction_pid_install_decision(
        partition_matches,
        entry.producer_id.0,
        entry.producer_epoch,
        entry.next_producer_id.0,
        entry.next_producer_epoch,
        current_owner_matches,
        next_owner_matches,
    );
    match decision {
        TransactionPidInstallDecision::Apply => Ok(()),
        TransactionPidInstallDecision::RejectWrongPartition => Err(BrokerError::Txn(format!(
            "transaction {} is in the wrong state partition",
            entry.transactional_id
        ))),
        TransactionPidInstallDecision::RejectCurrentIdentity => Err(BrokerError::Txn(format!(
            "transaction {} has an invalid current producer identity",
            entry.transactional_id
        ))),
        TransactionPidInstallDecision::RejectStagedIdentity => Err(BrokerError::Txn(format!(
            "transaction {} has an invalid staged producer identity",
            entry.transactional_id
        ))),
        TransactionPidInstallDecision::RejectCollision => Err(BrokerError::Txn(format!(
            "transaction {} reuses a producer ID owned by another transaction",
            entry.transactional_id
        ))),
    }
}

impl TxnCoordinator {
    pub(super) fn validate_pid_install(&self, entry: &TxnEntry) -> Result<(), BrokerError> {
        let tid = &entry.transactional_id;
        let current_owner_matches = self
            .pid_to_tid
            .get(&entry.producer_id)
            .is_none_or(|owner| owner.value() == tid);
        let next_owner_matches = self
            .pid_to_tid
            .get(&entry.next_producer_id)
            .is_none_or(|owner| owner.value() == tid);
        validate_install(entry, true, current_owner_matches, next_owner_matches)
    }

    /// Keep only the current transaction and staged recovery producer IDs for
    /// this transactional ID. Repeated KIP-939 recovery calls can rotate the
    /// staged ID before the transaction completes; retaining the superseded
    /// mapping would let that fenced ID bypass coordinator validation.
    pub(super) fn evict_superseded_pids(
        pid_to_tid: &DashMap<ProducerId, String>,
        entry: &TxnEntry,
    ) {
        pid_to_tid.retain(|pid, tid| {
            tid != &entry.transactional_id
                || *pid == entry.producer_id
                || *pid == entry.next_producer_id
        });
    }

    /// Drops every producer id `entry` owns out of the reverse index.
    ///
    /// Both paths that delete a transactional id call it: the live tombstone
    /// append in [`TxnCoordinator::tombstone`], and the tombstone replay in
    /// [`TxnCoordinator::recover`]. A deleted tid therefore leaves no mapping
    /// behind either way -- a replay that dropped the state entry alone would
    /// rebuild the reverse index for every id ever expired, which is the
    /// unbounded growth the KIP-98 sweep exists to stop.
    ///
    /// A mapping is removed only while it still names this tid. A pid the
    /// coordinator has since registered under another transactional id
    /// belongs to that one and stays.
    pub(super) fn evict_entry_pids(pid_to_tid: &DashMap<ProducerId, String>, entry: &TxnEntry) {
        for pid in [
            entry.producer_id,
            entry.prev_producer_id,
            entry.next_producer_id,
        ] {
            if pid.is_none() {
                continue;
            }
            pid_to_tid.remove_if(&pid, |_, tid| tid == &entry.transactional_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::txn::coordinator::test_support::entry,
        assert2::{assert, check},
    };

    #[derive(Clone, Copy)]
    enum StatePartition {
        Matching,
        Misplaced,
    }

    fn rolled_entry() -> TxnEntry { entry().producer(2000).previous(1000).call() }

    fn pid_map(entries: &[(i64, &str)]) -> DashMap<ProducerId, String> {
        let map = DashMap::new();
        for &(pid, tid) in entries {
            map.insert(ProducerId(pid), tid.into());
        }
        map
    }

    #[test]
    fn evict_rolled_pid_drops_only_the_prior_id_on_a_roll() {
        let map = pid_map(&[(1000, "tid-a")]); // the pre-roll mapping

        // A roll: new pid 2000, prev = 1000. The stale 1000 mapping is evicted;
        // put then inserts 2000 (mirrored here).
        TxnCoordinator::evict_superseded_pids(&map, &rolled_entry());
        map.insert(ProducerId(2000), "tid-a".into());

        assert!(
            map.get(&ProducerId(1000)).is_none(),
            "stale pre-roll pid must be evicted"
        );
        check!(map.get(&ProducerId(2000)).map(|e| e.value().clone()) == Some("tid-a".into()));
    }

    #[test]
    fn evict_rolled_pid_is_noop_without_a_roll() {
        let map = pid_map(&[(1000, "tid-a")]);
        // Never rolled: prev == -1 → nothing evicted.
        TxnCoordinator::evict_superseded_pids(&map, &entry().call());
        assert!(map.get(&ProducerId(1000)).is_some());
        // prev == current (defensive): nothing evicted.
        TxnCoordinator::evict_superseded_pids(&map, &entry().previous(1000).call());
        assert!(map.get(&ProducerId(1000)).is_some());
    }

    #[test]
    fn evict_rolled_pid_is_idempotent_after_the_id_is_gone() {
        let map = pid_map(&[(2000, "tid-a")]);
        // prev=1000 already absent → repeated evictions are harmless no-ops.
        TxnCoordinator::evict_superseded_pids(&map, &rolled_entry());
        TxnCoordinator::evict_superseded_pids(&map, &rolled_entry());
        assert!(map.get(&ProducerId(1000)).is_none());
        assert!(map.get(&ProducerId(2000)).is_some());
    }

    #[test]
    fn evict_superseded_pids_removes_a_rotated_recovery_identity() {
        let map = pid_map(&[(1000, "tid-a"), (2000, "tid-a")]);

        let mut current = entry().call();
        current.next_producer_id = ProducerId(3000);
        current.next_producer_epoch = 0;
        TxnCoordinator::evict_superseded_pids(&map, &current);

        assert!(map.get(&ProducerId(1000)).is_some());
        assert!(map.get(&ProducerId(2000)).is_none());
    }

    #[test]
    fn recovery_replay_evicts_superseded_ids_and_tombstone_dominates() {
        let mut recovered = RecoveredTransactions::default();
        recovered.apply_value(entry().call(), true).unwrap();
        recovered
            .apply_value(entry().producer(2000).next(3000).next_epoch(0).call(), true)
            .unwrap();

        assert!(!recovered.pid_to_tid.contains_key(&ProducerId(1000)));
        assert!(
            recovered
                .pid_to_tid
                .get(&ProducerId(2000))
                .map(String::as_str)
                == Some("tid-a")
        );
        assert!(
            recovered
                .pid_to_tid
                .get(&ProducerId(3000))
                .map(String::as_str)
                == Some("tid-a")
        );

        recovered.apply_tombstone("tid-a");
        recovered.apply_tombstone("tid-a");
        assert!(!recovered.state.contains_key("tid-a"));
        assert!(recovered.pid_to_tid.is_empty());
    }

    #[test]
    fn recovery_retry_is_idempotent_and_pid_collision_is_atomic() {
        let mut recovered = RecoveredTransactions::default();
        let first = entry().call();
        recovered.apply_value(first.clone(), true).unwrap();
        recovered.apply_value(first, true).unwrap();

        let error = recovered
            .apply_value(entry().transactional_id("tid-b").call(), true)
            .unwrap_err();

        assert!(error.to_string().contains("owned by another transaction"));
        assert!(recovered.state.len() == 1);
        assert!(recovered.state.contains_key("tid-a"));
        assert!(
            recovered
                .pid_to_tid
                .get(&ProducerId(1000))
                .map(String::as_str)
                == Some("tid-a")
        );
    }

    #[test]
    fn recovery_accepts_a_staged_epoch_on_the_current_pid() {
        let mut recovered = RecoveredTransactions::default();

        recovered
            .apply_value(entry().next(1000).next_epoch(1).call(), true)
            .unwrap();

        assert!(recovered.state.contains_key("tid-a"));
        assert!(recovered.pid_to_tid.len() == 1);
        assert!(
            recovered
                .pid_to_tid
                .get(&ProducerId(1000))
                .map(String::as_str)
                == Some("tid-a")
        );
    }

    #[test]
    fn recovery_rejects_malformed_or_misplaced_identities_without_mutation() {
        for (entry, partition_matches, message) in [
            (
                entry().producer(-1).call(),
                StatePartition::Matching,
                "invalid current producer identity",
            ),
            (
                entry().producer(1).next(2).call(),
                StatePartition::Matching,
                "invalid staged producer identity",
            ),
            (
                entry().producer(1).next_epoch(0).call(),
                StatePartition::Matching,
                "invalid staged producer identity",
            ),
            (
                entry().producer(1).call(),
                StatePartition::Misplaced,
                "wrong state partition",
            ),
        ] {
            let mut recovered = RecoveredTransactions::default();
            let error = recovered
                .apply_value(entry, matches!(partition_matches, StatePartition::Matching))
                .unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
            assert!(recovered.state.is_empty());
            assert!(recovered.pid_to_tid.is_empty());
        }
    }
}
