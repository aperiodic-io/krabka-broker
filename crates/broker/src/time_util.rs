//! Shared wall-clock time helpers, and the two guards that every cadence loop
//! puts around its injected [`Timer`].
//!
//! The wall-clock half is the single source of truth for the `SystemTime →
//! UNIX_EPOCH → as_millis() → i64` sequence that the transaction, OAuth, and
//! delegation-token handlers use, and that every reader of an injected
//! [`WallClock`](qubit_clock::WallClock) narrows through. The helpers saturate
//! on overflow and on pre-epoch clock skew, and do not panic.
//!
//! The timer half is [`arm`] and [`fired`]. Registering a deadline is fallible
//! -- a timer backend reports [`TimeError`] when it cannot take a registration
//! or cannot see one through -- and so is the completion the registration
//! yields. A cadence loop has nothing left to do once its ticker cannot be
//! armed, and re-arming it in a loop would spin the task at full speed, so both
//! helpers log the failure against the task's name and report it as "stop".
//!
//! [`system_timer`] is the real-time timer those loops run on outside tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use qubit_clock::{
    MonotonicClock, MonotonicInstant, StdMonotonicClock, StdTimer, TimeError, Timer, TimerFuture,
};

/// Cadence and metadata authority shared by local log-maintenance sweeps.
#[derive(Clone)]
pub(crate) struct MetadataSweepConfig {
    pub interval: krabka_units::Time,
    /// Production uses a real-time timer; tests may inject a manual timeline.
    pub timer: Arc<dyn Timer>,
    /// Re-read once per sweep so freezes and thaws take effect at the next tick.
    /// With no authority, the sweep resolves no freeze and leaves partitions eligible.
    pub metadata: Option<Arc<dyn crate::metadata_source::MetadataSource>>,
}

impl MetadataSweepConfig {
    pub(crate) fn system(interval: krabka_units::Time) -> Self {
        Self {
            interval,
            timer: system_timer(),
            metadata: None,
        }
    }

    pub(crate) fn current_image(&self) -> Option<Arc<krabka_metadata::MetadataImage>> {
        self.metadata.as_ref().map(|source| source.current_image())
    }
}

/// Returns `instant` in milliseconds since the Unix epoch.
///
/// The value saturates to `0` if `instant` falls before the epoch. It
/// saturates to `i64::MAX` if the duration overflows `i64`, which is about
/// 292 million years from now and therefore safe in practice.
#[inline]
pub(crate) fn epoch_millis(instant: SystemTime) -> i64 {
    instant
        .duration_since(UNIX_EPOCH)
        .map_or(0, duration_millis)
}

/// Whole milliseconds of `duration`, saturating at [`i64::MAX`].
#[inline]
pub(crate) fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// This reads the system clock directly. A component that takes an injected
/// [`WallClock`](qubit_clock::WallClock) so a test can drive it reads
/// `epoch_millis(clock.now())` instead.
#[inline]
pub(crate) fn now_ms() -> i64 {
    epoch_millis(SystemTime::now())
}

/// Sleeps until `deadline`, or never resolves when it is `None`.
///
/// A `tokio::select!` arm uses it to disarm a timer that has nothing to wait
/// for, such as a rebalance that is not open or a connection with no session
/// expiry.
pub(crate) async fn sleep_until_opt(deadline: Option<impl Into<tokio::time::Instant>>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

krabka_macros::timer_hooks! {
    /// Registers a deadline `delay` from now on `timer`, for the loop named
    /// `task`.
    ///
    /// `None` means the timer refused the registration, and the caller must stop:
    /// the failure is already logged with `task` naming which cadence went away.
    pub(crate) arm("could not arm the timer; stopping the task");

    /// Reports whether a deadline armed by [`arm`] completed, for the loop named
    /// `task`.
    ///
    /// `false` means the timer gave up on a registration it had accepted, and the
    /// caller must stop for the same reason [`arm`] returning `None` does.
    pub(crate) fired("the armed timer failed; stopping the task");
}

/// The real-time timer a cadence loop runs on outside tests.
///
/// Natively this is [`StdTimer`], whose deadlines one scheduler thread, shared
/// by the process, watches. `wasm32-wasip1` cannot start that thread -- every
/// registration fails there -- so on that target it is a [`RuntimeTimer`].
pub(crate) fn system_timer() -> Arc<dyn Timer> {
    if cfg!(target_os = "wasi") {
        Arc::new(RuntimeTimer::default())
    } else {
        Arc::new(StdTimer::new())
    }
}

/// A timer whose deadlines are sleeps on the tokio runtime that polls them.
///
/// It needs no thread of its own, which is what `wasm32-wasip1` needs. The
/// sleep is made at the first poll, inside the runtime, so the timer and its
/// futures can be made outside one, as the configs that hold a timer are; the
/// deadline is still fixed when it is armed, as for any [`Timer`].
#[derive(Debug, Default)]
pub(crate) struct RuntimeTimer {
    /// The clock the deadlines are read against, and whose domain
    /// [`Timer::at`] holds them to.
    clock: StdMonotonicClock,
}

impl Timer for RuntimeTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        deadline.validate_domain(MonotonicClock::domain(&self.clock))?;
        // A deadline that has passed is due at once, as it is for `StdTimer`.
        let Ok(wait) = deadline.duration_since(MonotonicClock::now(&self.clock)) else {
            return Ok(Box::pin(std::future::ready(Ok(()))));
        };
        let until = tokio::time::Instant::now()
            .checked_add(wait)
            .ok_or(TimeError::InstantOverflow)?;
        Ok(Box::pin(async move {
            tokio::time::sleep_until(until).await;
            Ok(())
        }))
    }
}

/// Share the cadence while keeping each task's concrete sweep future in its context.
macro_rules! run_sweeps {
    ($timer:expr, $cadence:expr, $shutdown:expr, $task:expr, $sweep:block, $on_shutdown:block $(,)?) => {{
        let timer = $timer;
        let cadence = $cadence;
        let shutdown = $shutdown;
        let task = $task;
        let Some(mut tick) = $crate::time_util::arm(timer, cadence.0, task) else { return; };
        loop {
            tokio::select! {
                outcome = &mut tick => {
                    if !$crate::time_util::fired(outcome, task) { return; }
                    $sweep
                    let Some(next) = $crate::time_util::arm(timer, cadence.1, task) else { return; };
                    tick = next;
                }
                () = shutdown.cancelled() => {
                    $on_shutdown
                    return;
                }
            }
        }
    }};
}
pub(crate) use run_sweeps;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use qubit_clock::ManualMonotonicClock;

    use super::*;

    const TASK: &str = "a test loop";

    #[test]
    fn epoch_millis_reads_an_instant_and_saturates_outside_the_range() {
        let cases = [
            (UNIX_EPOCH, 0),
            // A sub-second remainder, so a conversion that truncated to whole
            // seconds would fail here rather than pass.
            (
                UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
                1_700_000_000_123,
            ),
            // Before the epoch: a backwards clock reads as the epoch itself
            // rather than as a negative timestamp.
            (UNIX_EPOCH - Duration::from_millis(1), 0),
        ];

        for (instant, want) in cases {
            check!(epoch_millis(instant) == want, "{instant:?}");
        }

        // Some platforms cannot construct a `SystemTime` far enough from the
        // epoch to exceed `i64::MAX` milliseconds. Exercise the narrowing step
        // directly so the saturation check stays portable.
        check!(duration_millis(Duration::from_secs(1 << 60)) == i64::MAX);
    }

    #[test]
    fn now_ms_reads_the_system_clock() {
        let before = epoch_millis(SystemTime::now());
        let sampled = now_ms();
        let after = epoch_millis(SystemTime::now());

        check!(before <= sampled && sampled <= after);
    }

    #[test]
    fn arm_registers_a_deadline_the_timer_accepts() {
        let clock = ManualMonotonicClock::new_shared();
        let timer = clock.new_timer();

        // The registration lives only as long as the future does: dropping it
        // cancels the deadline. Hold it while asserting the clock saw it.
        let armed = arm(&*timer, Duration::from_secs(1), TASK);

        check!(armed.is_some());
        check!(clock.pending_waiters() == 1);
        drop(armed);
        check!(clock.pending_waiters() == 0);
    }

    /// A clock whose elapsed span has moved off its origin, so that a
    /// `Duration::MAX` delay overflows the deadline arithmetic and the timer
    /// refuses the registration.
    fn clock_past_its_origin() -> Arc<ManualMonotonicClock> {
        let clock = ManualMonotonicClock::new_shared();
        clock
            .advance(Duration::from_nanos(1))
            .expect("manual time moves forward");
        clock
    }

    #[test]
    fn arm_reports_a_deadline_the_timer_refuses() {
        let clock = clock_past_its_origin();
        let timer = clock.new_timer();

        check!(arm(&*timer, Duration::MAX, TASK).is_none());
        check!(clock.pending_waiters() == 0);
    }

    #[test]
    fn fired_separates_a_completed_deadline_from_a_failed_one() {
        let clock = clock_past_its_origin();
        // `expect_err` would need `Debug` on the success type, and a
        // `TimerFuture` is a boxed trait object that has none.
        let Err(refusal) = clock.new_timer().after(Duration::MAX) else {
            panic!("an overflowing delay must be refused");
        };

        check!(fired(Ok(()), TASK));
        check!(!fired(Err(refusal), TASK));
    }

    /// A [`RuntimeTimer`] deadline is a sleep on the runtime's clock: it is
    /// pending until the clock reaches it, and then fires.
    #[tokio::test(start_paused = true)]
    async fn a_runtime_timer_fires_when_the_runtime_clock_reaches_the_deadline() {
        let timer = RuntimeTimer::default();
        let started = tokio::time::Instant::now();
        let mut armed = timer.after(Duration::from_secs(60)).expect("arm");

        check!(futures_util::poll!(&mut armed).is_pending());
        tokio::time::advance(Duration::from_secs(59)).await;
        check!(futures_util::poll!(&mut armed).is_pending());
        let fired = armed.await;

        assert!(let Ok(()) = fired);
        check!(started.elapsed() >= Duration::from_secs(60));
    }

    /// The timer and its futures can be made outside a runtime, because the
    /// sleep is only made at the first poll; a deadline already reached is
    /// ready at once; and a deadline from another clock is refused.
    #[test]
    fn a_runtime_timer_needs_no_runtime_until_it_is_polled() {
        let timer = RuntimeTimer::default();
        let armed = timer.after(Duration::from_millis(5)).expect("arm");
        let due = timer.after(Duration::ZERO).expect("arm a due deadline");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("build a runtime");

        assert!(let Ok(()) = runtime.block_on(armed));
        check!(let Some(Ok(())) = futures_util::FutureExt::now_or_never(due));

        let foreign = StdMonotonicClock::new().now();
        check!(let Err(TimeError::ClockDomainMismatch { .. }) = timer.at(foreign));
    }

    // `start_paused = true` runs these on tokio's virtual clock: with no other
    // work pending, the runtime auto-advances logical time to the next timer, so
    // the `sleep_until`/`timeout` deadlines fire instantly and deterministically
    // instead of burning real wall-clock milliseconds.
    #[tokio::test(start_paused = true)]
    async fn sleep_until_opt_none_remains_pending() {
        // `None` never resolves; the 10ms timeout is the only timer, so virtual
        // time jumps to it and the timeout elapses -> Err.
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            sleep_until_opt(None::<tokio::time::Instant>),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_until_opt_some_waits_until_deadline() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        // The inner sleep (deadline) fires before the outer 1s timeout, so the
        // timeout resolves Ok and virtual time has advanced exactly to `deadline`.
        let result =
            tokio::time::timeout(Duration::from_secs(1), sleep_until_opt(Some(deadline))).await;
        assert!(result.is_ok());
        assert!(tokio::time::Instant::now() >= deadline);
    }
}
