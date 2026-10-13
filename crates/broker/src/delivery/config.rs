//! Tunables of the delivery scheduler.
//!
//! The struct carries the two injected time sources as well as the two
//! durations, so a test drives the scheduler on a manual timeline instead of
//! wall-clock time. [`Default`] wires the system wall clock and the system
//! timer.

use {
    krabka_units::{Time, millis, secs},
    qubit_clock::{StdWallClock, Timer, WallClock},
    std::sync::Arc,
};

/// How the broker-wide delivery scheduler paces itself, and where it reads
/// time.
///
/// The struct is [`Clone`] but neither [`Debug`] nor [`PartialEq`], because the
/// two injected trait objects are neither.
#[derive(Clone, krabka_macros::FieldDefaults)]
pub(crate) struct DeliveryConfig {
    /// Longest the scheduler sleeps when no partition it leads has a batch
    /// waiting, and the cap on every sleep.
    ///
    /// It bounds how long a newly scheduled partition, or a produce that lands
    /// a deadline further out than the instant the task sleeps on, stays
    /// undiscovered. The [`waker`](crate::delivery::waker) covers the deadlines
    /// that fall *before* that instant, so this value only has to be short
    /// enough to keep discovery honest, not short enough to be prompt.
    #[default(secs(1))]
    pub(crate) idle_sleep: Time,

    /// Shortest sleep the scheduler arms.
    ///
    /// A deadline that is already in the past would otherwise ask for a
    /// zero-length sleep, and a partition whose watermark cannot advance for an
    /// unrelated reason would spin the task. The floor turns that into a slow
    /// retry.
    #[default(millis(1))]
    pub(crate) min_sleep: Time,

    /// Wall clock the scheduler reads to decide which batches are due.
    ///
    /// Production uses [`qubit_clock::StdWallClock`]. A test drives both this
    /// and [`Self::timer`] from one
    /// [`qubit_clock::ManualMonotonicClock`], so the activation boundary is an
    /// assertion and not a race against real time.
    #[default(Arc::new(StdWallClock::new()))]
    pub(crate) clock: Arc<dyn WallClock>,

    /// Timer that drives the scheduler's cadence. Production uses
    /// `time_util::system_timer`. A test takes this from the same
    /// [`qubit_clock::ManualMonotonicClock`] that handed out [`Self::clock`],
    /// which puts the reading and the cadence on one timeline by construction
    /// rather than by agreement.
    #[default(crate::time_util::system_timer())]
    pub(crate) timer: Arc<dyn Timer>,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, krabka_units::convert::TimeExt as _};

    #[test]
    fn the_defaults_are_the_documented_values() {
        let config = DeliveryConfig::default();
        check!(config.idle_sleep == secs(1));
        check!(config.min_sleep == millis(1));
    }

    #[test]
    fn the_sleep_floor_is_below_the_idle_bound() {
        let config = DeliveryConfig::default();
        check!(config.min_sleep.millis_i64() < config.idle_sleep.millis_i64());
    }
}
