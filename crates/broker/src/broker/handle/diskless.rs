//! Test-only [`BrokerHandle`] helpers for the diskless WAL: flusher readiness,
//! the projected flush frontier, the object-PUT failure count, and the shard
//! readiness check that shipping gates poll before they produce.

use {
    crate::broker::{BrokerHandle, DISKLESS_FLUSHER_READY_TIMEOUT},
    krabka_ids::PartitionIndex,
    std::sync::atomic::Ordering,
};

impl BrokerHandle {
    /// Test-only: read the `tiered_storage_rlmm_topic_backed` gauge. `1`
    /// once the bootstrap task has swapped the fail-closed `NotReadyRlmm`
    /// for the topic-backed [`krabka_remote_storage::RemoteLogMetadataManager`],
    /// `0` before the swap completes, or when `remote_log_metadata` is
    /// `RlmmKind::InMemory`.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn rlmm_topic_backed_active_for_test(&self) -> bool {
        self.broker.metrics.tiered_storage_rlmm_topic_backed.get() == 1
    }

    /// Test-only count of object PUT errors observed by the diskless WAL
    /// flusher. Shipping-gate tests sample this before and after fault
    /// injection so a PUT failure that never reaches the real flusher cannot
    /// pass as a no-op.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn diskless_put_failure_count_for_test(&self) -> u64 {
        self.broker.metrics.diskless_wal_flush_failures_total.get()
    }

    /// Test-only: await this broker's own diskless index/flusher bootstrap.
    /// This resolves once the index projection has replayed the index topic
    /// and the flusher's first tick may fire, not merely once the index log
    /// opened.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_diskless_flusher_ready(&self) {
        let ready = self
            .diskless_flusher_ready
            .as_ref()
            .expect("diskless flusher is configured");
        tokio::time::timeout(DISKLESS_FLUSHER_READY_TIMEOUT, async {
            while !ready.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("diskless index/flusher bootstrap did not become ready within 90s");
    }

    /// Test-only snapshot of the local inputs used by the diskless flusher:
    /// `(diskless, runtime leader, log start, log end, high watermark,
    /// projected flush frontier)`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn diskless_flush_state_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<(bool, u64, i64, i64, i64, Option<i64>)> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        let (log_start, log_end) = {
            let log = part
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (log.log_start_offset().0, log.log_end_offset().0)
        };
        let high_watermark = part.high_watermark().await.0;
        let topic_id = self
            .broker
            .controller
            .current_image()
            .topic(topic)?
            .topic_id;
        let frontier = self
            .broker
            .diskless_read
            .as_ref()?
            .index
            .lock()
            .await
            .flushed_frontier(topic_id, partition);
        Some((
            part.diskless,
            part.current_leader.load(Ordering::Acquire),
            log_start,
            log_end,
            high_watermark,
            frontier,
        ))
    }

    /// Test-only readiness check for a distributed diskless WAL shard. This
    /// observes the real runtime registry rather than only the metadata image,
    /// so a shipping gate cannot race Produce against asynchronous placement
    /// reconciliation.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn diskless_wal_ready_for_test(
        &self,
        topic: &str,
        partition: i32,
        expected_leader: krabka_raft::NodeId,
        expected_voters: usize,
    ) -> bool {
        use crate::wal::quorum::registry::ShardId;

        let image = self.broker.controller.current_image();
        let Some(topic_id) = image.topic(topic).map(|record| record.topic_id) else {
            return false;
        };
        let shard = ShardId {
            topic_id,
            partition: PartitionIndex(partition),
        };
        self.broker
            .wal_shards
            .placement(shard)
            .is_some_and(|voters| {
                voters.len() == expected_voters && voters.first() == Some(&expected_leader)
            })
            && self.broker.wal_shards.get(shard).is_some()
            && self.broker.wal_shards.follower_fetcher_count(shard)
                == expected_voters.saturating_sub(1)
    }
}
