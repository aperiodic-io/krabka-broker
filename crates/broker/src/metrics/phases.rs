//! The per-request accumulator that carries a request's phase durations from
//! the place that spends them to the place that observes them.
//!
//! One Produce request appends to many partitions and one Fetch reads many,
//! so neither phase is a single interval that a guard could wrap. The handler
//! creates one [`RequestPhases`], hands a borrow of it to every partition it
//! works on, and observes the totals once when the request is done. That keeps
//! `_count` on the phase families equal to the request count, which is what
//! makes the phases comparable with `request_duration_seconds`.
//!
//! The fields are atomics rather than cells because the accumulator is
//! borrowed across `.await` points inside futures that must stay `Send`.

use {
    super::BrokerMetrics,
    crate::handlers::ApiKeyCode,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
};

/// Nanosecond accumulators for the phases of one in-flight request.
///
/// The phases are disjoint: a request is in at most one of them at a time, and
/// no interval is charged twice. They do not cover the whole request — see
/// [`BrokerMetrics::request_throttle_duration_seconds`] for what the remainder
/// is.
#[derive(Debug, Default)]
pub(crate) struct RequestPhases {
    local_nanos: AtomicU64,
    remote_nanos: AtomicU64,
}

impl RequestPhases {
    /// Charge `elapsed` to the local phase: this broker's own log.
    pub(crate) fn add_local(&self, elapsed: Duration) { Self::add(&self.local_nanos, elapsed); }

    /// Charge `elapsed` to the remote phase: a wait on something other than
    /// this broker's own log — a follower, a consumer's long poll, or the
    /// object store behind a tiered or diskless partition.
    pub(crate) fn add_remote(&self, elapsed: Duration) { Self::add(&self.remote_nanos, elapsed); }

    /// Seconds charged to the local phase so far.
    pub(crate) fn local_seconds(&self) -> f64 { Self::seconds(&self.local_nanos) }

    /// Seconds charged to the remote phase so far.
    pub(crate) fn remote_seconds(&self) -> f64 { Self::seconds(&self.remote_nanos) }

    /// Saturating add, so a clock that jumped cannot wrap a total back to
    /// nearly zero and report a request as instantaneous.
    fn add(slot: &AtomicU64, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        slot.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(nanos))
        })
        .ok();
    }

    /// Round-trips through [`Duration`] rather than dividing the raw count,
    /// which keeps the conversion free of a lossy integer-to-float cast.
    fn seconds(slot: &AtomicU64) -> f64 {
        Duration::from_nanos(slot.load(Ordering::Relaxed)).as_secs_f64()
    }
}

impl BrokerMetrics {
    /// Observe the local and remote phases of one finished request, on the
    /// same `api_key` label the total latency uses.
    ///
    /// Both are observed on every request, including with a zero value: a
    /// Produce whose partitions were all rejected by an ACL did no local work,
    /// and recording that zero is what keeps `_count` equal to the request
    /// count so the phases can be compared with the total.
    pub(crate) fn observe_request_phases(&self, api_key: ApiKeyCode, phases: &RequestPhases) {
        self.observe_request_local_duration(api_key, phases.local_seconds());
        self.observe_request_remote_duration(api_key, phases.remote_seconds());
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn phases_accumulate_each_kind_independently() {
        let phases = RequestPhases::default();
        phases.add_local(Duration::from_millis(3));
        phases.add_local(Duration::from_millis(7));
        phases.add_remote(Duration::from_millis(250));

        assert!((phases.local_seconds() - 0.010).abs() < 1.0e-9);
        assert!((phases.remote_seconds() - 0.250).abs() < 1.0e-9);
    }

    #[test]
    fn phases_start_at_zero_and_saturate_instead_of_wrapping() {
        let phases = RequestPhases::default();
        assert!(phases.local_seconds() < 1.0e-9);
        assert!(phases.remote_seconds() < 1.0e-9);

        // Two maximal durations must not wrap the total back toward zero: a
        // wrapped total would report a stalled request as instantaneous.
        phases.add_remote(Duration::MAX);
        phases.add_remote(Duration::MAX);
        assert!(phases.remote_seconds() > 1.0e9);
    }

    #[tokio::test]
    async fn observe_request_phases_records_one_sample_on_each_family() {
        let metrics = BrokerMetrics::new();
        let phases = RequestPhases::default();
        phases.add_local(Duration::from_millis(2));

        metrics.observe_request_phases(0, &phases);

        // `Histogram::sum`/`count` are behind prometheus-client's `test-util`
        // feature, so the rendered exposition is what a test reads — the same
        // text an operator scrapes.
        krabka_macros::metric_registry_fixture!(metrics, rendered, registry; unwrap());

        for expected in [
            "krabka_broker_request_local_duration_seconds_count{api_key=\"Produce\"} 1",
            "krabka_broker_request_local_duration_seconds_sum{api_key=\"Produce\"} 0.002",
            // The remote phase is observed too, with the zero this request
            // spent there: that is what keeps the two `_count`s equal.
            "krabka_broker_request_remote_duration_seconds_count{api_key=\"Produce\"} 1",
            "krabka_broker_request_remote_duration_seconds_sum{api_key=\"Produce\"} 0.0",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected} in:\n{rendered}"
            );
        }
    }
}
