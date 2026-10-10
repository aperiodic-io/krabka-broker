//! Test-only [`BrokerHandle`] awaiters for a local partition's log state: the
//! installed replication target, the log end offset, and the high watermark.
//! Each waits on the partition's own notify rather than sleeping, so they are
//! kept together with the offset accessors they pair with.

use std::sync::atomic::Ordering;

use krabka_ids::PartitionIndex;

use crate::broker::{BrokerHandle, TEST_AWAITER_TIMEOUT};

#[cfg(any(test, feature = "test-helpers"))]
#[derive(Clone, Copy)]
enum LocalFrontier {
    LogEnd,
    HighWatermark,
}

impl BrokerHandle {
    /// Test-only: await until the local partition runtime has installed the
    /// metadata leader, epoch, and ISR used by the Produce readiness gate.
    // cargo-mutants: removing this setup synchronizer only turns its bounded
    // failure into a downstream integration-test timeout.
    #[cfg_attr(test, mutants::skip)]
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_local_partition_leader(
        &self,
        topic: &str,
        partition: i32,
        leader: krabka_raft::NodeId,
    ) {
        let ready = super::await_until(|| async {
            let image = self.broker.controller.current_image();
            let (Some(record), Some(part)) = (
                image.partition(topic, partition),
                self.broker.partitions.get(topic, PartitionIndex(partition)),
            ) else {
                return false;
            };
            if record.leader != leader || part.current_leader.load(Ordering::Acquire) != leader.0 {
                return false;
            }
            let state = part.replica_state.lock().await;
            state.current_leader_epoch.0 == record.leader_epoch.0
                && state.isr.len() == record.isr.len()
                && record.isr.iter().all(|node| state.isr.contains(node))
        })
        .await;
        assert2::assert!(
            ready,
            "local partition {topic}-{partition} did not become produce-ready with leader {leader} within 30s"
        );
    }

    /// Test-only: await until the local partition runtime installs a specific
    /// replication leader and epoch. Unlike the Produce-readiness waiter, this
    /// does not require the local broker to own the leader's ISR state.
    // cargo-mutants: test-only convergence waiter exercised by the ignored JVM
    // KIP-320 acceptance gate; mutating it only changes test orchestration.
    #[cfg_attr(test, mutants::skip)]
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_local_partition_target(
        &self,
        topic: &str,
        partition: i32,
        leader: krabka_raft::NodeId,
        leader_epoch: krabka_metadata::LeaderEpoch,
    ) {
        let installed = super::await_until(|| {
            std::future::ready(
                self.broker
                    .partitions
                    .get(topic, PartitionIndex(partition))
                    .is_some_and(|part| {
                        part.current_leader.load(Ordering::Acquire) == leader.0
                            && part.current_leader_epoch.load(Ordering::Acquire) == leader_epoch.0
                    }),
            )
        })
        .await;
        assert2::assert!(
            installed,
            "local partition {topic}-{partition} did not install leader {leader} at epoch {leader_epoch:?} within 30s"
        );
    }

    /// Test-only: await until the LOCAL log for `topic-partition` reaches
    /// `log_end_offset >= min`. Uses the partition's `append_notify`; if the
    /// partition has not yet materialized locally, awaits a metadata image change
    /// and retries. The `notified()` future is created BEFORE the offset check to
    /// avoid a lost wakeup.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_local_log_end_offset(&self, topic: &str, partition: i32, min: i64) {
        self.wait_for_local_frontier(topic, partition, min, LocalFrontier::LogEnd)
            .await;
    }

    /// Test-only: await until the LOCAL high watermark for `topic-partition`
    /// reaches `min`. Uses the partition's HW notify so tests can wait for the
    /// async HW recompute that happens after the writer acks `acks=1` appends.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_high_watermark(&self, topic: &str, partition: i32, min: i64) {
        self.wait_for_local_frontier(topic, partition, min, LocalFrontier::HighWatermark)
            .await;
    }

    /// Register before inspecting the frontier to avoid losing its notification.
    #[cfg(any(test, feature = "test-helpers"))]
    async fn wait_for_local_frontier(
        &self,
        topic: &str,
        partition: i32,
        min: i64,
        frontier: LocalFrontier,
    ) {
        let result = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(part) = self.broker.partitions.get(topic, PartitionIndex(partition)) {
                    let notify = match frontier {
                        LocalFrontier::LogEnd => &part.append_notify,
                        LocalFrontier::HighWatermark => &part.hw_advance_notify,
                    };
                    let notified = notify.notified();
                    let offset = match frontier {
                        LocalFrontier::LogEnd => part.log_end_offset(),
                        LocalFrontier::HighWatermark => part.high_watermark().await,
                    };
                    if offset >= krabka_log::Offset(min) {
                        return;
                    }
                    notified.await;
                } else {
                    let mut image = self.broker.controller.watch_image();
                    if image.changed().await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        let label = match frontier {
            LocalFrontier::LogEnd => "local log_end_offset",
            LocalFrontier::HighWatermark => "high_watermark",
        };
        assert2::assert!(
            result.is_ok(),
            "{label}({topic}-{partition}) did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the LOCAL log end offset for `topic-partition` is
    /// EXACTLY `target`. `wait_until_local_log_end_offset` waits on a monotonic
    /// `>=`; this method handles non-monotonic convergence. For example, a
    /// follower truncates a divergent suffix and then re-replicates to match the
    /// leader, so the offset can pass through `>= target` with wrong-epoch data
    /// before it settles at `target`. The method wakes on `append_notify` for
    /// re-appends, and a short fallback tick also observes a truncation, which
    /// does not notify. It returns the instant LEO == target. This is a
    /// condition wait on real state, not a fixed-duration sleep, so it cannot
    /// flake on timing. It fails only if the condition never holds within 30s.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_local_log_end_offset_eq(
        &self,
        topic: &str,
        partition: i32,
        target: i64,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(part) = self.broker.partitions.get(topic, PartitionIndex(partition)) {
                    let notified = part.append_notify.notified();
                    if part.log_end_offset() == krabka_log::Offset(target) {
                        return;
                    }
                    // Truncation does not fire append_notify; fall back to a short
                    // re-check tick so a truncate-to-target is still observed.
                    tokio::select! {
                        () = notified => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
                    }
                } else {
                    let mut img = self.broker.controller.watch_image();
                    if img.changed().await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        assert2::assert!(
            res.is_ok(),
            "local log_end_offset({topic}-{partition}) did not settle at {target} within 30s"
        );
    }

    /// Test-only: number of `OffsetForLeaderEpoch` (`api_key` 23) requests this
    /// broker has served since startup. The KIP-320 proactive-validation
    /// integration test reads this before and after a `Consumer::poll` to
    /// prove that the consumer's validate pass issued an OFLE RPC. The count
    /// separates the proactive path from the reactive in-band `diverging_epoch`
    /// and `OFFSET_OUT_OF_RANGE` fetch paths, which issue no OFLE.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn offset_for_leader_epoch_count_for_test(&self) -> u64 {
        self.broker
            .offset_for_leader_epoch_requests
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Test-only: flip a configured log dir offline at runtime. This simulates
    /// a live fsync failure without real EIO injection, which is unreliable
    /// across platforms. It drives the KIP-112 offline path.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn test_mark_log_dir_offline(&self, dir: &std::path::Path) -> bool {
        self.broker
            .log_dir_status
            .mark_offline(dir, "test-injected storage failure")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::{kibibytes, mebibytes, secs};

    use super::*;
    use crate::broker::test_support::local_partition_with_records;

    #[tokio::test]
    async fn single_broker_handle_local_log_helpers_observe_real_state() {
        let (handle, dir) = crate::test_support::start_broker_with(|_| {}).await;
        let broker = handle.broker_arc_for_test();

        let helper_topic = "handle-partition-helper-mutant-topic";
        let helper_part = local_partition_with_records(
            dir.path(),
            crate::broker::test_support::LocalPartitionSetup {
                topic: helper_topic,
                ..Default::default()
            },
        );
        let helper_config = krabka_log::LogConfig {
            retention: Some(secs(123)),
            segment_size: kibibytes(4),
            ..Default::default()
        };
        helper_part
            .log
            .lock()
            .expect("helper partition log lock")
            .set_config(helper_config.clone());
        broker.partitions.insert(
            helper_topic.into(),
            PartitionIndex(0),
            Arc::clone(&helper_part),
        );
        handle
            .test_advance_log_start(helper_topic, 0, 2)
            .await
            .expect("advance helper partition log start");
        assert2::assert!(handle.partition_log_start_for_test(helper_topic, 0) == Some(2));
        assert2::assert!(
            handle.partition_retention_ms_for_test(helper_topic, 0)
                == Some(Some(std::time::Duration::from_secs(123)))
        );
        let observed_config = handle
            .partition_log_config_for_test(helper_topic, 0)
            .expect("helper partition log config");
        assert2::assert!(observed_config.retention == helper_config.retention);
        assert2::assert!(observed_config.segment_size == helper_config.segment_size);
        let last_offset = handle
            .produce_records_for_test(helper_topic, 0, 3)
            .await
            .expect("produce helper partition records");
        let log_end = handle
            .local_log_end_offset(helper_topic, 0)
            .expect("helper partition log end offset");
        assert2::assert!(last_offset >= 2);
        assert2::assert!(last_offset + 1 == log_end);
        let read = helper_part
            .log
            .lock()
            .expect("helper partition log lock")
            .read(krabka_log::Offset(2), mebibytes(1))
            .expect("read helper partition records");
        assert2::assert!(read.start_offset == krabka_log::Offset(2));
        assert2::assert!(!read.batches.is_empty());
        let records: Vec<_> = read
            .batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .collect();
        check!(records.len() == 1);
        check!(records[0].offset_delta == 0);
        check!(
            records[0].value.as_ref().map(bytes::Bytes::as_ref)
                == Some(b"test-record-2".as_slice())
        );
        // Waiting for log_end + 1 must stay pending; waiting for the reached
        // log_end must resolve (both the >= and == variants).
        check!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_local_log_end_offset(helper_topic, 0, log_end + 1),
            )
            .await
            .is_err()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_local_log_end_offset_eq(helper_topic, 0, log_end + 1),
            )
            .await
            .is_err()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_local_log_end_offset(helper_topic, 0, log_end),
            )
            .await
            .is_ok()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_local_log_end_offset_eq(helper_topic, 0, log_end),
            )
            .await
            .is_ok()
        );

        handle.shutdown().await;
    }
}
