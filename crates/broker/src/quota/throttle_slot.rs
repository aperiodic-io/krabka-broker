//! The KIP-219 throttle window that the request in flight earned.

use std::sync::atomic::{AtomicU64, Ordering};

use krabka_units::{Time, convert::TimeExt};

use crate::metrics::QuotaCharge;

/// The KIP-219 throttle window a handler computed for the request in flight.
///
/// KIP-219 splits a quota violation in two. The response tells the client how
/// long to back off, and the broker enforces that window by muting the
/// connection *after* the response bytes are on the wire. The window itself is
/// computed deep inside the quota code, so the handler records it here and the
/// per-connection dispatch loop reads it back once the write has completed.
///
/// Sleeping inside the handler instead is the pre-KIP-219 behaviour KIP-219
/// replaced: the client's `request.timeout.ms` fires during the stall, the
/// client retries, and the retry adds load to the very quota that is shedding
/// it.
///
/// A request can trip more than one quota — a produce charges both
/// `producer_byte_rate` and `request_percentage`. Recording therefore keeps the
/// longest window rather than summing, which matches Kafka's single
/// `throttle_time_ms` and single channel mute per request.
#[derive(Debug, Default)]
pub(crate) struct ThrottleSlot {
    /// The window in whole microseconds. `Time` is an `f64` quantity and so
    /// cannot live in an atomic directly; microseconds are finer than the
    /// millisecond resolution of the wire field the same window is reported in.
    micros: AtomicU64,
    /// A quota charge the handler leaves for the dispatch loop, which resolves
    /// it together with the request quota in one metrics call. Kafka's
    /// `sendResponseMaybeThrottleWithControllerQuota` takes the larger of the
    /// controller-mutation and request throttles as one decision.
    deferred: std::sync::OnceLock<QuotaCharge>,
}

impl ThrottleSlot {
    /// Raises the recorded window to `window`, and leaves it alone when the
    /// slot already holds a longer one.
    ///
    /// The relaxed ordering is enough: the dispatch loop reads the slot back
    /// only after awaiting the handler future that wrote it, and that await is
    /// itself the synchronisation edge.
    pub(crate) fn record(&self, window: Time) {
        let micros = u64::try_from(window.micros_i64()).unwrap_or(0);
        self.micros.fetch_max(micros, Ordering::Relaxed);
    }

    /// Leaves `charge` for the dispatch loop to resolve with the request quota,
    /// and records its window. A request defers at most one charge; a second
    /// one is ignored.
    pub(crate) fn defer(&self, charge: QuotaCharge) {
        self.record(charge.delay);
        // The first charge wins. No handler defers two.
        let _ = self.deferred.set(charge);
    }

    /// The charge a handler deferred, if any.
    pub(crate) fn deferred(&self) -> Option<QuotaCharge> {
        self.deferred.get().cloned()
    }

    /// Takes the recorded window and resets the slot to zero, so that a
    /// context reused across requests cannot mute twice for one throttle.
    pub(crate) fn take(&self) -> Time {
        let micros = self.micros.swap(0, Ordering::Relaxed);
        Time::from_micros(i64::try_from(micros).unwrap_or(i64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::millis;

    use super::*;

    #[test]
    fn empty_slot_takes_a_zero_window() {
        let slot = ThrottleSlot::default();
        assert!(slot.take() == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn record_keeps_the_longest_window_and_take_drains_it() {
        let slot = ThrottleSlot::default();
        slot.record(millis(250));
        slot.record(millis(40));
        slot.record(millis(700));
        slot.record(<Time as TimeExt>::ZERO);

        assert!(slot.take() == millis(700));
        assert!(slot.take() == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn record_clamps_a_negative_window_to_zero() {
        let slot = ThrottleSlot::default();
        slot.record(Time::from_secs(-1));
        assert!(slot.take() == <Time as TimeExt>::ZERO);
    }
}
