use {krabka_ids::PartitionIndex, krabka_log::ProducerId, std::collections::HashSet};

/// Transaction state machine. It mirrors Apache Kafka's classic transaction
/// states (KIP-98), extended for KIP-1319 v2.
///
/// `as_str` is the state's name as Kafka's `TransactionState` spells it on the
/// wire, the string `DescribeTransactions` and `ListTransactions` report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
pub enum TxnState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

impl TxnState {
    /// Can the machine move from `self` to `other`?
    pub fn can_transition_to(self, other: TxnState) -> bool {
        use TxnState::{
            CompleteAbort, CompleteCommit, Dead, Empty, Ongoing, PrepareAbort, PrepareCommit,
        };
        matches!(
            (self, other),
            // re-init: empty → empty, or after a completed txn
            (Empty | CompleteCommit | CompleteAbort, Empty)
            // add partitions: first or subsequent, or starting a new txn after
            // a completed one (CompleteCommit/CompleteAbort → Ongoing is the
            // implicit "re-use without init" path that Apache Kafka supports)
            | (Empty | Ongoing | CompleteCommit | CompleteAbort, Ongoing)
            // begin end-of-txn
            | (Ongoing, PrepareCommit | PrepareAbort)
            // finalise
            | (PrepareCommit, CompleteCommit)
            | (PrepareAbort, CompleteAbort)
            // expire / delete
            | (CompleteCommit | CompleteAbort, Dead)
        )
    }

    /// Kafka `TransactionState` byte id, that is
    /// `TransactionLogValue.TransactionStatus` as an int8.
    ///
    /// It matches `org.apache.kafka.coordinator.transaction.TransactionState`
    /// exactly: Empty=0, Ongoing=1, PrepareCommit=2, PrepareAbort=3,
    /// CompleteCommit=4, CompleteAbort=5, and Dead=6. Kafka also has
    /// `PrepareEpochFence`=7, a transient fencing state that Krabka does not
    /// model.
    #[must_use]
    pub fn to_kafka_status(self) -> i8 {
        match self {
            TxnState::Empty => 0,
            TxnState::Ongoing => 1,
            TxnState::PrepareCommit => 2,
            TxnState::PrepareAbort => 3,
            TxnState::CompleteCommit => 4,
            TxnState::CompleteAbort => 5,
            TxnState::Dead => 6,
        }
    }

    /// The inverse of [`Self::to_kafka_status`]. It returns `None` for an id
    /// that Krabka does not model, such as 7 for `PrepareEpochFence`, and for
    /// an id outside the range.
    #[must_use]
    pub fn from_kafka_status(id: i8) -> Option<TxnState> {
        Some(match id {
            0 => TxnState::Empty,
            1 => TxnState::Ongoing,
            2 => TxnState::PrepareCommit,
            3 => TxnState::PrepareAbort,
            4 => TxnState::CompleteCommit,
            5 => TxnState::CompleteAbort,
            6 => TxnState::Dead,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: PartitionIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnEntry {
    pub transactional_id: String,
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    pub state: TxnState,
    pub txn_timeout_ms: i32,
    pub partitions: HashSet<TopicPartition>,
    /// KIP-890/KIP-939 producer identity bookkeeping. `next_producer_id` and
    /// `next_producer_epoch` hold the fenced identity returned by
    /// `InitProducerId(keepPreparedTxn=true)` until the older ongoing
    /// transaction completes.
    pub prev_producer_id: ProducerId,
    pub next_producer_id: ProducerId,
    pub next_producer_epoch: i16,
    /// KIP-360 fencing state. `last_producer_epoch` is the epoch the entry held
    /// before its last bump or before an epoch fence was prepared for an abort,
    /// which is how a retry that names it is recognised. Kafka keeps it in
    /// memory only, and a reload sees `-1`. Kafka trunk persists it as
    /// `TransactionLogValue`'s `LastProducerEpoch` (tag 4, KAFKA-20357), and so
    /// does this broker under `unstable.api.versions.enable`, so it survives a
    /// coordinator restart or a partition move there (#892).
    /// `has_failed_epoch_fence` says that abort
    /// then failed, so the producer that still holds `last_producer_epoch`
    /// may retry its `InitProducerId`; it stays in-memory only, since a
    /// reload always sees the fence as not yet attempted.
    pub last_producer_epoch: i16,
    pub has_failed_epoch_fence: bool,
    pub last_update_ms: i64,
    pub start_ms: i64,
    /// `TransactionLogValue.ClientTransactionVersion`: the transaction
    /// version of the request that made the latest transition, not the
    /// cluster's live transaction.version level. Kafka derives it from the
    /// request's API version: `2` for `EndTxn` v5 and `AddPartitionsToTxn` v4
    /// and later (completion bumps the epoch under KIP-890's version-2 rule),
    /// `0` for every earlier version and for `AddOffsetsToTxn`. A server-side
    /// abort (the timeout reaper, the `InitProducerId` fence) stamps `2` on a
    /// `transaction.version` 2 cluster and `0` below it, since
    /// `endTransactionWithTV1` stamps `TV_0`. The transition that completes a
    /// transaction keeps the stamp of the one that prepared it, and the
    /// `WriteTxnMarkers` `transaction_version` reads it.
    pub client_transaction_version: i16,
}

impl TxnEntry {
    pub fn has_staged_producer_identity(&self) -> bool { self.next_producer_epoch >= 0 }

    /// Fresh entry for a tid that's never been seen, or the entry after an
    /// epoch bump.
    ///
    /// The transaction start time is `-1` until a partition joins the
    /// transaction, as Kafka's `TransactionMetadata` keeps
    /// `txnStartTimestamp` (`prepareIncrementProducerEpoch` and
    /// `prepareProducerIdRotation` reset it to `-1`).
    pub fn new_empty(
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        txn_timeout_ms: i32,
        now_ms: i64,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            producer_epoch,
            state: TxnState::Empty,
            txn_timeout_ms,
            partitions: HashSet::new(),
            prev_producer_id: ProducerId(-1),
            next_producer_id: ProducerId(-1),
            next_producer_epoch: -1,
            last_producer_epoch: -1,
            has_failed_epoch_fence: false,
            last_update_ms: now_ms,
            start_ms: -1,
            client_transaction_version: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn empty_to_ongoing_allowed() {
        assert!(TxnState::Empty.can_transition_to(TxnState::Ongoing));
    }

    #[test]
    fn empty_to_prepare_commit_disallowed() {
        assert!(!TxnState::Empty.can_transition_to(TxnState::PrepareCommit));
    }

    #[test]
    fn ongoing_to_complete_commit_disallowed_without_prepare() {
        assert!(!TxnState::Ongoing.can_transition_to(TxnState::CompleteCommit));
    }

    #[test]
    fn complete_commit_to_empty_for_reuse() {
        assert!(TxnState::CompleteCommit.can_transition_to(TxnState::Empty));
    }

    #[test]
    fn kafka_status_round_trips_all_states() {
        for s in [
            TxnState::Empty,
            TxnState::Ongoing,
            TxnState::PrepareCommit,
            TxnState::PrepareAbort,
            TxnState::CompleteCommit,
            TxnState::CompleteAbort,
            TxnState::Dead,
        ] {
            assert!(TxnState::from_kafka_status(s.to_kafka_status()) == Some(s));
        }
    }

    #[test]
    fn kafka_status_values_match_kafka() {
        // Empirically pinned: cp-kafka 4.0 Ongoing record had TransactionStatus=1.
        for (state, want) in [
            (TxnState::Ongoing, 1),
            (TxnState::Empty, 0),
            (TxnState::Dead, 6),
        ] {
            assert!(state.to_kafka_status() == want, "{state:?}");
        }
        // PrepareEpochFence (7) and out-of-range are not modeled.
        for status in [7, -1] {
            assert!(TxnState::from_kafka_status(status).is_none(), "{status}");
        }
    }
}
