//! Construction of a [`BrokerMetrics`] bundle and its registration with the
//! Prometheus registry. It holds the histogram bucket boundaries that the
//! `#[metric(buckets = ...)]` attributes on [`BrokerMetrics`] name, and the
//! entry point that runs the derived constructor and registration.

use crate::metrics::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel};

#[cfg(test)]
mod tests;

/// Latency buckets (seconds) for the per-API `request_duration_seconds`
/// histogram. Spans ~100µs (idempotent `ApiVersions`) to 10s (a slow
/// controller round-trip or a throttled admin RPC), tuned so the common
/// Produce/Fetch band (0.5ms–50ms) lands on distinct buckets.
///
/// The `request_{local,remote,throttle}_duration_seconds` phases and
/// `quota_throttle_duration_seconds` share these boundaries deliberately. A
/// phase is a part of the total, and an operator checks the phases against the
/// total bucket by bucket; two bucket sets would make that comparison an
/// interpolation rather than a subtraction.
pub(super) const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 10.0,
];

/// Latency buckets (seconds) for `barrier_injection_duration_seconds`. One
/// injection appends a marker to every partition of a barrier group, and a
/// partition that another broker leads costs an inter-broker round trip. The
/// span runs from 5ms for a small single-broker group to 30s, which is the
/// default `barrier_injection_timeout`.
pub(super) const BARRIER_INJECTION_DURATION_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Latency buckets (seconds) for `delivery_activation_lateness_seconds`.
/// KFC-1 bounds activation lateness at twice the topic's declared
/// `delivery_clock_uncertainty` plus one scheduler tick, so the value an
/// operator sees is normally a few hundred milliseconds at most: the span opens
/// at 1ms and resolves the sub-second band finely. The tail runs to 30s so a
/// broker with real clock skew, or one whose scheduler is starved of CPU, still
/// lands in a bucket instead of in `+Inf`.
pub(super) const DELIVERY_ACTIVATION_LATENESS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 30.0,
];

impl BrokerMetrics {
    /// Build and register every broker metric.
    ///
    /// # Panics
    ///
    /// Never in practice: the registry is locked once, before any clone of it
    /// exists.
    #[must_use]
    pub fn new() -> Self {
        let metrics = Self::unregistered();
        {
            let mut registry = metrics
                .registry
                .try_lock()
                .expect("fresh metrics registry cannot be locked");
            metrics.register(&mut registry);
        }
        // Create all three series at zero. The drain only ever touches the
        // path it took, and on some targets it can never take two of them, so
        // without this a dashboard panel or an alert that names `sendfile` has
        // no series to read until the first zero-copy fetch — or, on Windows,
        // ever.
        for path in FetchDrainPath::ALL {
            let _ = metrics
                .fetch_response_drain
                .get_or_create(&FetchDrainPathLabel { path });
        }
        metrics
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self { Self::new() }
}
