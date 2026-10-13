//! The share-group dead-letter queue meters (KIP-1191): Kafka's
//! `DeadLetterQueueRecordCount`, `DeadLetterQueueTotalProduceRequestsPerSec`
//! and `DeadLetterQueueFailedProduceRequestsPerSec` of `ShareGroupMetrics`.
//!
//! Kafka's are per-group meters, whose count and rate the JMX attributes read.
//! These are the same three as monotonic counters, as the other Kafka meters
//! here are, and a rate is what `rate()` says at scrape time.

use super::{BrokerMetrics, ShareGroupIdLabel};

impl BrokerMetrics {
    /// Records `count` dead-letter records that a produce round wrote for
    /// `group_id`: Kafka's `recordDLQRecordWrite`.
    pub(crate) fn record_share_dlq_records(&self, group_id: &str, count: usize) {
        self.share_group_dlq_records
            .get_or_create(&label(group_id))
            .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
    }

    /// Records one attempt to produce dead-letter records for `group_id`:
    /// Kafka's `recordDLQProduce`.
    pub(crate) fn record_share_dlq_produce(&self, group_id: &str) {
        self.share_group_dlq_produce_requests
            .get_or_create(&label(group_id))
            .inc();
    }

    /// Records a dead-letter write of `group_id` that failed: Kafka's
    /// `recordDLQProduceFailed`.
    pub(crate) fn record_share_dlq_produce_failed(&self, group_id: &str) {
        self.share_group_dlq_failed_produce_requests
            .get_or_create(&label(group_id))
            .inc();
    }

    /// Releases the dead-letter series of `group_id`.
    ///
    /// The coordinator calls it when `DeleteGroups` deletes a share group,
    /// since a deleted group is not a metadata-image event.
    pub(crate) fn evict_share_group_dlq_series(&self, group_id: &str) {
        let label = label(group_id);
        self.share_group_dlq_records.remove(&label);
        self.share_group_dlq_produce_requests.remove(&label);
        self.share_group_dlq_failed_produce_requests.remove(&label);
    }
}

fn label(group_id: &str) -> ShareGroupIdLabel {
    ShareGroupIdLabel {
        group_id: group_id.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    krabka_macros::share_dlq_meters! {
        counts, BrokerMetrics, ShareGroupIdLabel
    }

    /// Each meter counts for its own group, and eviction takes one group's
    /// series and leaves the other's.
    #[test]
    fn the_meters_count_for_each_group_and_release_a_deleted_group() {
        let metrics = BrokerMetrics::new();

        metrics.record_share_dlq_produce("g1");
        metrics.record_share_dlq_produce("g1");
        metrics.record_share_dlq_records("g1", 3);
        metrics.record_share_dlq_produce_failed("g1");
        metrics.record_share_dlq_produce("g2");
        metrics.record_share_dlq_records("g2", 1);
        let before = (counts(&metrics, "g1"), counts(&metrics, "g2"));
        metrics.evict_share_group_dlq_series("g1");
        let after = (counts(&metrics, "g1"), counts(&metrics, "g2"));

        assert!(before == ((3, 2, 1), (1, 1, 0)));
        assert!(after == ((0, 0, 0), (1, 1, 0)));
    }
}
