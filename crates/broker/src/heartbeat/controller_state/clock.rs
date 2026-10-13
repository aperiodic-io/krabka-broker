//! The monotonic time source the liveness registry reads.
//!
//! The registry never calls `Instant::now` directly. It asks a [`Clock`], so a
//! test replaces the real clock with one it advances by hand and the liveness
//! windows stay deterministic.

#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

/// Monotonic time source for liveness tracking.
///
/// Production uses the real clock. Tests use a controllable clock, so
/// explicit advances drive the liveness windows instead of wall-clock
/// `std::thread::sleep`. Sleeps flake when the CI runner is loaded. The gap
/// between a re-seed and the next `tick` can then exceed a short timeout and
/// mark a broker dead by mistake.
pub(super) enum Clock {
    Real,
    #[cfg(test)]
    Test(std::sync::Arc<TestClockInner>),
}

impl Clock {
    pub(super) fn now(&self) -> Instant {
        match self {
            Clock::Real => Instant::now(),
            #[cfg(test)]
            Clock::Test(inner) => {
                inner.base
                    + Duration::from_nanos(
                        inner
                            .offset_nanos
                            .load(std::sync::atomic::Ordering::Relaxed),
                    )
            }
        }
    }
}

#[cfg(test)]
pub(super) struct TestClockInner {
    base: Instant,
    offset_nanos: std::sync::atomic::AtomicU64,
}

/// Test handle for the controllable [`Clock`]. It shares its inner state with
/// the `Clock::Test` handed to [`ControllerLivenessState::with_clock`], so the
/// liveness state under test observes every `advance`. Tests in other modules
/// reach it through [`ControllerLivenessState::with_test_clock`].
#[cfg(test)]
pub(crate) struct TestClock(std::sync::Arc<TestClockInner>);

#[cfg(test)]
impl TestClock {
    pub(crate) fn new() -> Self {
        Self(std::sync::Arc::new(TestClockInner {
            base: Instant::now(),
            offset_nanos: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    pub(crate) fn advance(&self, by: Duration) {
        self.0.offset_nanos.fetch_add(
            u64::try_from(by.as_nanos()).expect("advance fits u64 nanos"),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(super) fn clock(&self) -> Clock { Clock::Test(self.0.clone()) }
}
