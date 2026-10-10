//! Log-maintenance accounting: the sweep counter the cleaner bumps once per
//! clean pass, the per-partition counter of compactions that passed, the
//! per-partition failure counter, and the uncleanable-partition gauge that
//! says how many partitions the cleaner has lost -- plus the same pair of
//! sweep and failure counters for the local-retention loop, which fails a
//! disk in the same way and is alerted on the same way.

use std::sync::Arc;

use super::{BrokerMetrics, CleanerFailureLabel, CleanerFailureReason, PartitionLabel};

impl BrokerMetrics {
    /// Account one clean log-compaction sweep (a full `tick_all` pass that
    /// failed no partition). Called once per cleaner tick, whether or not
    /// any partition was eligible, so a test can observe that a full pass
    /// ran after it sealed a segment.
    ///
    /// A sweep that failed a partition does not reach this counter: it is
    /// accounted by [`Self::record_cleaner_failure`] instead, so the pass
    /// rate never reports success during a compaction outage.
    pub fn record_cleaner_run(&self) {
        self.log_cleaner_runs_total.inc();
    }

    /// Account one successful per-partition compaction pass
    /// (`Partition::compact_log` returned `Ok`).
    pub fn record_compaction(&self, topic: &str, partition: i32) {
        let lbl = PartitionLabel {
            topic: Arc::from(topic),
            partition,
        };
        self.log_compactions_total.get_or_create(&lbl).inc();
        self.track_partition_series(&lbl);
    }

    /// Account one failed per-partition compaction pass
    /// (`Partition::compact_log` returned `Err`), under the reason the
    /// cleaner classified the error as.
    pub fn record_cleaner_failure(
        &self,
        topic: &str,
        partition: i32,
        reason: CleanerFailureReason,
    ) {
        let lbl = CleanerFailureLabel {
            topic: Arc::from(topic),
            partition,
            reason,
        };
        self.log_cleaner_failures.get_or_create(&lbl).inc();
    }

    /// Account one clean local-retention sweep (a full pass that failed no
    /// partition), the counterpart of [`Self::record_cleaner_run`] for the
    /// broker-wide local-retention loop.
    pub fn record_retention_run(&self) {
        self.log_retention_runs_total.inc();
    }

    /// Account one failed local-retention pass
    /// (`Partition::retain_log` returned `Err`), under the reason the sweep
    /// classified the error as.
    ///
    /// The label carries the same three reasons compaction failures do, so an
    /// operator reads and alerts on both series the same way.
    pub fn record_retention_failure(
        &self,
        topic: &str,
        partition: i32,
        reason: CleanerFailureReason,
    ) {
        let lbl = CleanerFailureLabel {
            topic: Arc::from(topic),
            partition,
            reason,
        };
        self.log_retention_failures.get_or_create(&lbl).inc();
    }

    /// Publish the count of partitions whose most recent compaction attempt
    /// failed and which have not compacted since. The cleaner republishes it
    /// at the end of every sweep, so the gauge falls on the pass that
    /// recovers a partition and on the pass after this broker stops leading
    /// it.
    pub fn set_uncleanable_partitions(&self, count: usize) {
        self.log_cleaner_uncleanable_partitions
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

/// Compaction and retention classify failures identically for operator metrics.
pub(crate) fn failure_reason(error: &crate::error::BrokerError) -> CleanerFailureReason {
    use crate::error::BrokerError;
    match error {
        BrokerError::Log(krabka_log::LogError::Io(_)) => CleanerFailureReason::Io,
        BrokerError::Replication(_) => CleanerFailureReason::Writer,
        _ => CleanerFailureReason::Other,
    }
}
