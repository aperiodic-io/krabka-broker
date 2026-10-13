//! KFC-1 scheduled-delivery accounting: the per-partition watermark and
//! pending-record gauges the delivery scheduler publishes after it recomputes
//! a partition.

use {
    super::{BrokerMetrics, PartitionLabel},
    std::sync::Arc,
};

impl BrokerMetrics {
    /// KFC-1: publish one scheduled partition's delivery watermark and the
    /// count of records that are durable but not visible yet. Called from the
    /// delivery scheduler after it recomputes the partition. A partition whose
    /// topic delivers immediately never reaches this method, so an ordinary
    /// topic creates no series.
    pub fn record_delivery_watermark(
        &self,
        topic: &str,
        partition: i32,
        watermark: i64,
        pending: i64,
    ) {
        let lbl = PartitionLabel {
            topic: Arc::from(topic),
            partition,
        };
        self.delivery_watermark.get_or_create(&lbl).set(watermark);
        self.delivery_pending_records
            .get_or_create(&lbl)
            .set(pending);
        self.track_partition_series(&lbl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_delivery_watermark_and_pending() {
        let metrics = BrokerMetrics::new();
        metrics.record_delivery_watermark("scheduled-orders", 1, 42, 10);

        let lbl = PartitionLabel {
            topic: Arc::from("scheduled-orders"),
            partition: 1,
        };
        assert2::check!(metrics.delivery_watermark.get_or_create(&lbl).get() == 42);
        assert2::check!(metrics.delivery_pending_records.get_or_create(&lbl).get() == 10);
    }
}
