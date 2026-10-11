//! Test-only [`BrokerHandle`] helpers that read or mutate one locally hosted
//! partition: its existence, its log config, its metadata record, and the
//! direct append and truncate paths that tests use in place of the Kafka wire
//! protocol.

use std::sync::atomic::Ordering;

use krabka_ids::PartitionIndex;
use krabka_units::convert::TimeExt;

#[cfg(any(test, feature = "test-helpers"))]
use crate::error::BrokerError;
use crate::{broker::BrokerHandle, partition::Partition};

impl BrokerHandle {
    /// This broker's partition `topic-partition`, or a
    /// [`BrokerError::Replication`][crate::error::BrokerError::Replication]
    /// naming it when it is not hosted here.
    fn local_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<std::sync::Arc<Partition>, BrokerError> {
        self.broker
            .partitions
            .get(topic, PartitionIndex(partition))
            .ok_or_else(|| {
                BrokerError::Replication(format!("partition {topic}-{partition} not local"))
            })
    }

    /// Test-only: truncate this broker's local partition log so no
    /// records at offset `>= offset` remain. Simulates "fell behind
    /// past retention" in the out-of-range replication integration
    /// test.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not
    /// hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn test_truncate_local_log(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), BrokerError> {
        let part = self.local_partition(topic, partition)?;
        part.truncate_to(krabka_log::Offset(offset)).await?;
        // Mirror the production truncation path (the replicator): a log
        // truncation also reverts idempotent-producer dedup entries for the
        // dropped offsets, so a retried batch from the truncated tail re-appends
        // instead of deduplicating against a vanished offset.
        self.broker
            .producer_state
            .truncate(topic, PartitionIndex(partition), offset)
            .await;
        Ok(())
    }

    /// Test-only: advance this broker's local partition `log_start_offset`
    /// to `new_start` without physically deleting on-disk segments.
    /// Simulates retention-driven truncation on a leader for the
    /// out-of-range replication integration test.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not
    /// hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn test_advance_log_start(
        &self,
        topic: &str,
        partition: i32,
        new_start: i64,
    ) -> Result<(), BrokerError> {
        let part = self.local_partition(topic, partition)?;
        part.test_set_log_start(krabka_log::Offset(new_start)).await
    }

    /// Test-only: directly set `current_leader_epoch` on a locally-hosted
    /// partition. `tests/leader_epoch.rs` uses this to simulate split-brain
    /// with a forced epoch bump. It does not use the supervisor's
    /// metadata-image-driven path.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_leader_epoch(&self, topic: &str, partition: i32, epoch: i32) {
        if let Some(part) = self.broker.partitions.get(topic, PartitionIndex(partition)) {
            part.test_set_leader_epoch(epoch);
        }
    }

    /// Test-only: return `true` if `(topic, partition)` is present in this
    /// broker's in-process partition registry. Admin-handler integration
    /// tests use this to confirm that `CreatePartitions` materialised a
    /// new partition dir and writer task.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_exists_for_test(&self, topic: &str, partition: i32) -> bool {
        self.broker
            .partitions
            .contains(topic, PartitionIndex(partition))
    }

    // ── partition log helpers ──────────────────────────────────────────────────

    /// Test-only: return the base offset of every batch in the local log of
    /// `(topic, partition)`, in log order. Returns `None` if the partition is
    /// not hosted on this broker, or if the read fails.
    ///
    /// The read budget is larger than any test log, so the read gives the whole
    /// log. A replication test compares the result with the batches of the
    /// leader. A follower that skipped batches still reaches the log end offset
    /// of the leader, so the log end offset alone does not show the gap.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn local_batch_base_offsets_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<Vec<i64>> {
        let batches = self.local_batches_for_test(topic, partition)?;
        Some(batches.iter().map(|batch| batch.base_offset).collect())
    }

    /// Test-only: return every batch in the local log of `(topic,
    /// partition)`, in log order. Returns `None` if the partition is not
    /// hosted on this broker, or if the read fails.
    ///
    /// The read budget is larger than any test log, so the read gives the whole
    /// log. A compaction test compares the batches that each replica keeps.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn local_batches_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<Vec<krabka_protocol::records::RecordBatch>> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        let log = part.log.lock().ok()?;
        let read = log
            .read(log.log_start_offset(), krabka_units::gibibytes(4))
            .ok()?;
        Some(read.batches)
    }

    /// Test-only: run one compaction pass on the local log of `(topic,
    /// partition)` now, through its writer, as the cleaner does on each tick.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not hosted on
    /// this broker, and the error of the pass otherwise.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn compact_local_log_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(), BrokerError> {
        let part = self.local_partition(topic, partition)?;
        part.compact_log().await
    }

    /// Test-only: return the `log_start_offset` of `(topic, partition)` as
    /// reported by its underlying [`krabka_log::Log`]. Returns `None` if the
    /// partition is not hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_log_start_for_test(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        // Unwrap `Offset` -> `i64` at this test-helper boundary.
        Some(part.log_start_offset().0)
    }

    /// Test-only: hold `(topic, partition)`'s high watermark at `offset`,
    /// leaving its log end offset where it is. Returns `false` if the partition
    /// is not hosted on this broker.
    ///
    /// This is the state a leader is in whenever a follower in the ISR has not
    /// acknowledged the tail of the log: records are durable locally and
    /// invisible to every client until the watermark catches up. Producing it
    /// for real needs a second broker that is stopped mid-flight, and the
    /// window before Kafka shrinks the ISR and advances the watermark anyway is
    /// a timeout rather than a state. A test that wants to assert what a client
    /// may see of an unreplicated tail installs the watermark instead, and gets
    /// the same partition without the second broker or the race.
    ///
    /// Nothing here reopens the log or moves the log end offset, so a caller
    /// that has already settled its writes can take a reading immediately.
    ///
    /// The hold is not sticky: a replicator reconcile reinstalls the ISR and
    /// recomputes the watermark from the log end, which raises it again. A
    /// caller that reads under the hold confirms with
    /// [`Self::high_watermark_for_test`] that it was still in place.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn hold_high_watermark_for_test(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> bool {
        let Some(part) = self.broker.partitions.get(topic, PartitionIndex(partition)) else {
            return false;
        };
        part.replica_state.lock().await.hw = krabka_log::Offset(offset);
        true
    }

    /// Test-only: the high watermark of `(topic, partition)` on this broker,
    /// or `None` when the partition is not hosted here.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn high_watermark_for_test(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        Some(part.high_watermark().await.0)
    }

    /// Test-only: return the `retention.ms` override currently active in
    /// `(topic, partition)`'s log config. Returns `None` if the partition is
    /// not hosted on this broker. The inner `Option<Duration>` is `None` when
    /// no retention override has been applied (topic uses broker default).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_retention_ms_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<Option<std::time::Duration>> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        let snap = part.log.lock().ok()?.config_snapshot();
        // `krabka-log` holds this as a `Time` now, but the helper's signature is
        // public under `test-helpers`, so the extent converts back at the seam
        // rather than churning the callers.
        Some(snap.retention.map(TimeExt::to_std))
    }

    /// Test-only: full `LogConfig` snapshot for `(topic, partition)`.
    /// Returns `None` if the partition is not hosted on this broker.
    /// Used by the compaction integration test to wait for
    /// `cleanup.policy=compact` + `segment.bytes` overrides to propagate
    /// from the metadata image through the supervisor's reconcile loop.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_log_config_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<krabka_log::LogConfig> {
        let part = self
            .broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        Some(part.log.lock().ok()?.config_snapshot())
    }

    /// Test-only: append `n` single-record batches to `(topic, partition)`
    /// through the partition's writer task. Used by admin-handler integration
    /// tests that need a non-empty log without going through the Kafka Produce
    /// wire protocol. Returns the `base_offset` of the last appended batch, or
    /// an error if the partition is not hosted on this broker or the writer is
    /// dead.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not local.
    /// Returns [`BrokerError::Txn`] if the writer task is dead.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn produce_records_for_test(
        &self,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Result<i64, BrokerError> {
        let part = self.local_partition(topic, partition)?;
        let leader_epoch = part.current_leader_epoch.load(Ordering::Acquire);
        let mut last_offset = 0i64;
        for i in 0..n {
            let batch = krabka_protocol::records::RecordBatch {
                partition_leader_epoch: leader_epoch,
                records: vec![krabka_protocol::records::Record {
                    offset_delta: 0,
                    value: Some(bytes::Bytes::from(format!("test-record-{i}").into_bytes())),
                    ..Default::default()
                }],
                ..Default::default()
            };
            // Unwrap `Offset` -> `i64` at this test-helper boundary.
            last_offset = part.produce_batch(batch).await?.0;
        }
        Ok(last_offset)
    }

    /// Test-only: return the current leader node-id for `(topic, partition)`
    /// as seen by this broker's metadata image. Returns `None` if the
    /// partition is not yet in the image or the leader field is `0` (no
    /// elected leader).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_leader_for_test(&self, topic: &str, partition: i32) -> Option<u64> {
        let img = self.broker.controller.current_image();
        let p = img.partition(topic, partition)?;
        if p.leader == krabka_raft::NodeId(0) {
            None
        } else {
            Some(p.leader.0)
        }
    }

    /// Test-only: return the current ISR for `(topic, partition)` as seen
    /// by this broker's metadata image. Returns `None` if the partition is
    /// not yet in the image.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_isr_for_test(&self, topic: &str, partition: i32) -> Option<Vec<u64>> {
        let img = self.broker.controller.current_image();
        let p = img.partition(topic, partition)?;
        Some(p.isr.iter().map(|n| n.0).collect())
    }

    /// Test-only: return a clone of the full `PartitionRecord` for
    /// `(topic, partition)` as seen by this broker's metadata image.
    /// Returns `None` if the partition is not yet in the image.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn partition_record_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<krabka_metadata::PartitionRecord> {
        self.broker
            .controller
            .current_image()
            .partition(topic, partition)
            .cloned()
    }
}
