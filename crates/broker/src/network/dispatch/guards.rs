//! RAII metric guards for the dispatch loop. One counts a request while it is
//! in flight and records its duration when it finishes; the other counts a
//! live client connection for the lifetime of its serve loop.

use crate::metrics::BrokerMetrics;

/// RAII guard for one dispatched request.
///
/// The guard increments `in_flight_requests` on construction. On drop it
/// decrements the counter and records the elapsed wall-clock time on the
/// `request_duration_seconds{api}` histogram. Drop covers every exit path:
/// success, a handler error, and a panic unwind.
///
/// The serve loop keeps the broker alive across the handler `.await`, so
/// the guard borrows its metrics instead of cloning every metric handle.
pub(super) struct InFlightGuard<'a> {
    metrics: &'a BrokerMetrics,
    api_key: i16,
    started: std::time::Instant,
}

impl<'a> InFlightGuard<'a> {
    pub(super) fn new(metrics: &'a BrokerMetrics, api_key: i16) -> Self {
        metrics.in_flight_requests.inc();
        Self {
            metrics,
            api_key,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.metrics.in_flight_requests.dec();
        self.metrics
            .observe_request_duration(self.api_key, self.started.elapsed().as_secs_f64());
    }
}

/// RAII guard for one live client connection. It increments
/// `active_connections` on construction and decrements it on drop, when the
/// per-connection serve loop exits.
pub(super) struct ActiveConnectionGuard<'a> {
    metrics: &'a BrokerMetrics,
}

impl<'a> ActiveConnectionGuard<'a> {
    pub(super) fn new(metrics: &'a BrokerMetrics) -> Self {
        metrics.active_connections.inc();
        Self { metrics }
    }
}

impl Drop for ActiveConnectionGuard<'_> {
    fn drop(&mut self) { self.metrics.active_connections.dec(); }
}

/// RAII guard for one queued request waiting for / holding execution capacity (#412).
pub(super) struct QueuedRequestGuard<'a> {
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// The `queued.max.request.bytes` budget this request spent, given back
    /// when the guard drops. Absent when the knob is off.
    _bytes: Option<tokio::sync::OwnedSemaphorePermit>,
    metrics: &'a BrokerMetrics,
    /// The value this guard added to `queued_request_bytes`, kept as the
    /// gauge's own type so the decrement on drop is exactly the increment
    /// that was made and the gauge cannot drift.
    bytes: i64,
}

impl<'a> QueuedRequestGuard<'a> {
    pub(super) fn new(
        permit: tokio::sync::OwnedSemaphorePermit,
        bytes_permit: Option<tokio::sync::OwnedSemaphorePermit>,
        metrics: &'a BrokerMetrics,
        bytes: usize,
    ) -> Self {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        metrics.queued_requests.inc();
        if bytes > 0 {
            metrics.queued_request_bytes.inc_by(bytes);
        }
        Self {
            _permit: permit,
            _bytes: bytes_permit,
            metrics,
            bytes,
        }
    }
}

impl Drop for QueuedRequestGuard<'_> {
    fn drop(&mut self) {
        self.metrics.queued_requests.dec();
        if self.bytes > 0 {
            self.metrics.queued_request_bytes.dec_by(self.bytes);
        }
    }
}
