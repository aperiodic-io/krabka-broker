//! The metric seam of the delivery scheduler.
//!
//! The scheduler reports through this trait. The broker binds it to the process
//! metric registry. A unit test binds [`NoDeliveryMetrics`], so no test needs a
//! live registry.

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt as _};

use crate::{delivery::PartitionDelivery, metrics::BrokerMetrics};

/// The gauges, the histogram, and the counter that the delivery scheduler
/// feeds.
pub(crate) trait DeliveryMetrics: Send + Sync {
    /// One scheduled partition's state after a recompute. A partition whose
    /// topic delivers immediately never reaches this method, so it never
    /// creates a series.
    fn watermark_advanced(
        &self,
        topic: &str,
        partition: PartitionIndex,
        delivery: PartitionDelivery,
    );

    /// A batch became visible `lateness` after the deadline the scheduler was
    /// waiting on.
    ///
    /// The deadline is the record timestamp plus the topic's declared clock
    /// bound, so this is the delay *beyond* that bound and not the delay from
    /// the record's own delivery time. It is the number that says whether the
    /// declared bound is honest.
    fn activation_late(&self, lateness: Time);

    /// The scheduler woke, whether a deadline came due, a produce re-armed it,
    /// or its idle bound elapsed.
    fn scheduler_woke(&self);
}

/// The [`DeliveryMetrics`] the running broker uses.
///
/// It holds a [`BrokerMetrics`](crate::metrics::BrokerMetrics), which clones
/// cheaply, so the scheduler can own one behind an `Arc<dyn DeliveryMetrics>`
/// without borrowing from the broker.
#[derive(Clone)]
pub(crate) struct BrokerDeliveryMetrics {
    metrics: BrokerMetrics,
}

impl BrokerDeliveryMetrics {
    pub(crate) const fn new(metrics: BrokerMetrics) -> Self {
        Self { metrics }
    }
}

impl DeliveryMetrics for BrokerDeliveryMetrics {
    fn watermark_advanced(
        &self,
        topic: &str,
        partition: PartitionIndex,
        delivery: PartitionDelivery,
    ) {
        self.metrics.record_delivery_watermark(
            topic,
            partition.0,
            delivery.watermark.0,
            delivery.pending,
        );
    }

    fn activation_late(&self, lateness: Time) {
        self.metrics
            .delivery_activation_lateness_seconds
            .observe(lateness.secs_f64());
    }

    fn scheduler_woke(&self) {
        self.metrics.delivery_scheduler_wakeups_total.inc();
    }
}

/// A [`DeliveryMetrics`] that records nothing.
///
/// It exists so a unit test needs no live metric registry.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoDeliveryMetrics;

#[cfg(test)]
impl DeliveryMetrics for NoDeliveryMetrics {
    fn watermark_advanced(
        &self,
        _topic: &str,
        _partition: PartitionIndex,
        _delivery: PartitionDelivery,
    ) {
    }
    fn activation_late(&self, _lateness: Time) {}
    fn scheduler_woke(&self) {}
}
