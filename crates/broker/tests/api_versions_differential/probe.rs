//! Deadlines for the readiness polling this suite does against a container.
//!
//! Waiting for a broker to come up means running something that talks to it and
//! asking whether it worked. The something here is a JVM tool, and the failure
//! that matters is the one where the broker never answers: `AdminClient` sits on
//! its own `default.api.timeout.ms` -- a minute in a stock client -- before the
//! tool exits non-zero, and a wedged `docker exec` need not exit at all. A retry
//! loop that blocks on each attempt therefore bounds nothing, and the "give up
//! after a minute" it looks like is really "give up after an hour, or never".
//!
//! So both halves of the loop carry a deadline: [`wait_bounded`] kills a child
//! that outruns the budget it was given, and [`retry_until`] hands each attempt
//! only the budget that is left. The whole wait then ends when the clock says
//! so, whatever the attempt inside it is doing.

use std::{
    process::{Child, ExitStatus},
    time::{Duration, Instant},
};

/// How often [`wait_bounded`] looks at a child that has not exited yet.
///
/// Short enough that killing an overrun probe is prompt, long enough that a
/// minute of waiting is not a minute of spinning.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait for `child` to exit, for at most `budget`.
///
/// Returns its exit status when it exits in time. Otherwise the child is killed
/// and reaped -- so the caller leaks neither a process nor a zombie -- and the
/// result is `None`.
///
/// # Panics
///
/// Panics when the child cannot be waited on at all, which means the process
/// handle is broken rather than the child being slow.
pub(crate) fn wait_bounded(child: &mut Child, budget: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = child.try_wait().expect("wait on child process") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Run `probe` until it reports ready, or until `budget` runs out.
///
/// Each attempt is handed the budget that remains, and is expected to bound
/// itself by it; `gap` is the pause between a failed attempt and the next one.
/// Returns how many attempts it took to become ready, or `Err` with the number
/// that were made before the deadline passed.
pub(crate) fn retry_until(
    budget: Duration,
    gap: Duration,
    mut probe: impl FnMut(Duration) -> bool,
) -> Result<u32, u32> {
    let deadline = Instant::now() + budget;
    let mut attempts = 0;
    loop {
        attempts += 1;
        if probe(deadline.saturating_duration_since(Instant::now())) {
            return Ok(attempts);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(attempts);
        }
        std::thread::sleep(gap.min(remaining));
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{retry_until, wait_bounded},
        assert2::assert,
        std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        },
    };

    /// Spawn a shell that runs `script`, with its output discarded.
    fn spawn(script: &str) -> std::process::Child {
        Command::new("sh")
            .args(["-c", script])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sh")
    }

    #[yare::parameterized(
        success = { "exit 0", true },
        failure = { "exit 3", false },
    )]
    fn a_child_that_exits_in_time_reports_its_status(script: &str, expected_success: bool) {
        let mut child = spawn(script);
        let status = wait_bounded(&mut child, Duration::from_secs(30));
        assert2::assert!(status.map(|status| status.success()) == Some(expected_success));
    }

    #[test]
    fn a_child_that_outruns_its_budget_is_killed_and_reaped() {
        let mut child = spawn("sleep 300");
        let started = Instant::now();
        let status = wait_bounded(&mut child, Duration::from_millis(200));
        let elapsed = started.elapsed();

        assert!(status.is_none());
        assert!(elapsed < Duration::from_secs(30));
        // The child was reaped inside the call, so its status is already known
        // and no process is left behind.
        assert!(child.try_wait().expect("wait on child process").is_some());
    }

    #[test]
    fn a_zero_budget_still_reports_a_child_that_has_already_exited() {
        let mut child = spawn("exit 0");
        child.wait().expect("wait on child process");

        let status = wait_bounded(&mut child, Duration::ZERO);

        assert!(status.map(|status| status.success()) == Some(true));
    }

    #[test]
    fn readiness_stops_at_the_first_ready_probe() {
        let mut seen = 0;
        let attempts = retry_until(Duration::from_secs(30), Duration::ZERO, |_| {
            seen += 1;
            seen == 3
        });

        assert!(attempts == Ok(3));
        assert!(seen == 3);
    }

    #[test]
    fn a_probe_that_blocks_cannot_outlive_the_deadline() {
        let budget = Duration::from_millis(300);
        let started = Instant::now();
        // The probe spends every bit of budget it is handed and never reports
        // ready, which is how an unresponsive broker behaves: without the
        // remaining budget reaching it, one attempt alone would outlast the
        // whole wait.
        let attempts = retry_until(budget, Duration::from_millis(10), |remaining| {
            std::thread::sleep(remaining);
            false
        });
        let elapsed = started.elapsed();

        assert!(attempts.is_err());
        assert!(elapsed < budget * 10);
    }

    #[test]
    fn a_probe_that_never_reports_ready_gives_up_at_the_deadline() {
        let started = Instant::now();
        let attempts = retry_until(
            Duration::from_millis(200),
            Duration::from_millis(20),
            |_| false,
        );
        let elapsed = started.elapsed();

        assert!(attempts.is_err());
        assert!(elapsed >= Duration::from_millis(200));
        assert!(elapsed < Duration::from_secs(30));
    }
}
