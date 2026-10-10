//! The `producer.id.expiration.ms` inactivity window: eviction of the
//! producers that have gone quiet.
//!
//! A broker maintenance loop calls the eviction so the per-partition maps do
//! not grow without bound. The log cleaner does not read this tracker: it reads
//! the producer state of the partition log, which a follower also updates
//! (`krabka_log::Log::compact`).
use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;

use super::{PartitionMap, PartitionProducerState, ProducerState};
#[cfg(test)]
use crate::producer_state::test_support::{ProducerCommitSetup, SequenceDelta, commit_fixture};

impl ProducerState {
    /// Evict idempotent-producer entries that are expired at `now_ms`.
    ///
    /// This is Kafka's `ProducerStateManager.removeExpiredProducers` for
    /// `producer.id.expiration.ms`, whose default is `86_400_000` ms = 24h.
    /// Kafka's `isProducerExpired` ages an entry by its `lastTimestamp`, the
    /// max timestamp of its last batch (or of the marker after it), not by
    /// the broker clock at the append, and never expires a producer whose
    /// transaction on the partition is still open.
    ///
    /// This function removes empty partition maps and empty topic maps once
    /// their last entry expires, so stale `(topic, partition)` keys do not
    /// leak. It returns the number of producer-id entries it evicted.
    ///
    /// This function gives the mechanism only. The periodic caller is a
    /// broker maintenance loop, wired separately.
    pub async fn expire_older_than(&self, now_ms: i64, ttl: Time) -> usize {
        let ttl_ms = ttl.millis_i64();
        let mut evicted = 0usize;
        // Snapshot the (topic -> partition-map) refs first so we don't
        // hold a DashMap shard guard across the per-partition `.await`.
        let topics: Vec<(String, Arc<PartitionMap>)> = self
            .by_topic
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (topic, parts) in topics {
            let partition_refs: Vec<(PartitionIndex, Arc<Mutex<PartitionProducerState>>)> = parts
                .iter()
                .map(|e| (*e.key(), e.value().clone()))
                .collect();
            for (partition, handle) in partition_refs {
                let mut state = handle.lock().await;
                let before = state.entries.len();
                state
                    .entries
                    .retain(|_pid, entry| !entry.is_expired(now_ms, ttl_ms));
                evicted += before - state.entries.len();
                let now_empty = state.entries.is_empty();
                drop(state);
                if now_empty {
                    // Only drop the partition slot if it's *still* empty
                    // under the removal guard, so a concurrent commit that
                    // re-populated it isn't lost.
                    parts.remove_if(&partition, |_, h| {
                        h.try_lock().is_ok_and(|s| s.entries.is_empty())
                    });
                }
            }
            // Drop the topic slot if all its partitions are gone.
            self.by_topic.remove_if(&topic, |_, p| p.is_empty());
        }
        evicted
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_log::ProducerId;
    use krabka_units::millis;

    use super::*;
    use crate::producer_state::Decision;

    /// How a row's producer reaches the sweep.
    #[derive(Clone, Copy, Debug)]
    enum History {
        /// One idempotent batch.
        Idempotent,
        /// One transactional batch, with no marker yet.
        OpenTransaction,
        /// One transactional batch, then its commit marker at the batch's
        /// timestamp.
        ClosedTransaction,
    }

    /// #908: Kafka's `ProducerStateManager.isProducerExpired` ages an entry by
    /// the timestamp of its last batch (or marker), never by the broker's
    /// clock at the append, and never expires a producer whose transaction on
    /// the partition is still open. The expiry window is 5 s and every batch
    /// carries timestamp `t0`; the append itself happens now, on the wall
    /// clock, long after `t0`.
    #[tokio::test]
    async fn expiry_ages_by_the_batch_timestamp_and_spares_open_transactions() {
        const T0: i64 = 1_000;
        const EXPIRY_MS: i64 = 5_000;
        let cases = [
            (
                "idle for the whole window",
                History::Idempotent,
                T0 + EXPIRY_MS,
                1,
            ),
            (
                "one millisecond short",
                History::Idempotent,
                T0 + EXPIRY_MS - 1,
                0,
            ),
            (
                "open transaction, ten windows later",
                History::OpenTransaction,
                T0 + 10 * EXPIRY_MS,
                0,
            ),
            (
                "committed transaction, one window later",
                History::ClosedTransaction,
                T0 + EXPIRY_MS,
                1,
            ),
        ];
        for (label, history, now_ms, evicted) in cases {
            let s = ProducerState::new();
            let transactional = !matches!(history, History::Idempotent);
            s.commit(
                "t",
                PartitionIndex(0),
                (7, 0),
                (0, 0),
                (0, T0, transactional),
            )
            .await;
            if matches!(history, History::ClosedTransaction) {
                s.mirror_log_entries(
                    "t",
                    PartitionIndex(0),
                    vec![krabka_log::ProducerSnapshotEntry {
                        producer_id: ProducerId(7),
                        producer_epoch: 0,
                        last_sequence: 0,
                        last_offset: krabka_log::Offset(0),
                        offset_delta: 0,
                        timestamp: T0,
                        coordinator_epoch: 0,
                        current_txn_first_offset: None,
                    }],
                )
                .await;
            }
            let survivors: Vec<i64> = if evicted == 0 { vec![7] } else { vec![] };
            check!(
                (
                    s.expire_older_than(now_ms, Time::from_millis(EXPIRY_MS))
                        .await,
                    s.snapshot("t", PartitionIndex(0))
                        .await
                        .into_iter()
                        .map(|(pid, _)| pid)
                        .collect::<Vec<_>>(),
                ) == (evicted, survivors),
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        commit_fixture(
            &s,
            ProducerCommitSetup {
                producer: krabka_log::ProducerId(1),
                delta: SequenceDelta(0),
                timestamp: crate::test_support::UnixMillis(0),
                ..Default::default()
            },
        )
        .await;
        let evicted = s.expire_older_than(1_000_000, millis(1)).await;
        // The empty partition and topic maps are pruned (the empty topic slot
        // must be removed), and a subsequent produce still works after pruning.
        check!(evicted == 1);
        check!(s.by_topic.get("t").is_none());
        check!(s.check("t", PartitionIndex(0), 1, 0, 0, 0).await == Decision::Append);
    }
}
