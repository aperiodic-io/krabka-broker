//! The metric seam of the barrier coordinator.
//!
//! The coordinator reports each injection through [`BrokerBarrierMetrics`],
//! which feeds the process metric registry.

use krabka_units::{Time, convert::TimeExt as _};

use crate::{
    barrier::persistence::CutStatus,
    metrics::{BarrierGroupLabel, BrokerMetrics},
};

/// What one finished injection reported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct InjectionReport {
    /// The epoch the injection consumed.
    pub(crate) epoch: i64,
    /// Whether every target partition carries the marker.
    pub(crate) status: CutStatus,
    /// How many partitions carry the marker.
    pub(crate) marked: usize,
    /// How many partitions carry no marker.
    pub(crate) missing: usize,
    /// How long the injection took, from the first record to the cut record.
    pub(crate) elapsed: Time,
}

/// The counters and gauges that the barrier coordinator feeds.
///
/// It holds a [`BrokerMetrics`](crate::metrics::BrokerMetrics), which clones
/// cheaply, so the coordinator can own one without borrowing from the broker.
#[derive(Clone)]
pub(crate) struct BrokerBarrierMetrics {
    metrics: BrokerMetrics,
}

impl BrokerBarrierMetrics {
    pub(crate) const fn new(metrics: BrokerMetrics) -> Self {
        Self { metrics }
    }

    fn group(group: &str) -> BarrierGroupLabel {
        BarrierGroupLabel {
            group: group.to_owned(),
        }
    }

    /// The coordinator wrote the injection-start record of `epoch`.
    pub(crate) fn injection_started(&self, group: &str, _epoch: i64) {
        self.metrics
            .barrier_epochs_started_total
            .get_or_create(&Self::group(group))
            .inc();
    }

    /// The coordinator published the cut of one injection.
    pub(crate) fn injection_completed(&self, group: &str, report: InjectionReport) {
        let label = Self::group(group);
        // A partial cut is published, so it counts as an outcome, not as a
        // failure. The two counters separate the alertable case from the
        // healthy one.
        match report.status {
            CutStatus::Complete => self
                .metrics
                .barrier_epochs_committed_total
                .get_or_create(&label)
                .inc(),
            CutStatus::Partial => self
                .metrics
                .barrier_epochs_published_partial_total
                .get_or_create(&label)
                .inc(),
        };
        self.metrics
            .barrier_injection_duration_seconds
            .get_or_create(&label)
            .observe(report.elapsed.secs_f64());
        // The gauge names the newest cut this coordinator PUBLISHED, so it
        // moves here and not when the injection starts. A started epoch that
        // never publishes must not advance it.
        self.metrics
            .barrier_latest_epoch
            .get_or_create(&label)
            .set(report.epoch);
    }

    /// One marker landed in a partition this broker leads, or in one a remote
    /// leader answered for.
    pub(crate) fn marker_written(&self, topic: &str) {
        self.metrics.count_topic(
            &self.metrics.barrier_markers_written_total,
            std::sync::Arc::from(topic),
            1,
        );
    }

    /// How many groups this broker coordinates now.
    pub(crate) fn groups_coordinated(&self, count: usize) {
        self.metrics
            .barrier_groups_coordinated
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}
