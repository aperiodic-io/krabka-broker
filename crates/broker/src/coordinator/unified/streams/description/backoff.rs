//! Kafka's `StreamsGroupTopologyDescriptionBackoff`, for one group.
//!
//! When a heartbeat asks a member for the topology description, the broker
//! arms a window in which no other heartbeat of the group is asked for it at
//! the same topology epoch. Two members that join together therefore push the
//! description once, not twice. A push that succeeds clears the window. A
//! window that ends with no push behind it, because the member never sent one
//! or its push was lost, lets the next heartbeat ask again, and each
//! consecutive window at one epoch doubles, from 30 seconds to an hour, with
//! Kafka's 20% jitter.
//!
//! Kafka keeps the windows in one broker-wide map keyed by group id. The
//! group's streams actor keeps its own here, which forgets it when the group
//! is deleted, as Kafka's `DeleteGroups` does.

use std::time::{Duration, Instant};

/// Kafka's `INITIAL_DELAY_MS`.
const INITIAL_DELAY_MS: u64 = 30_000;
/// Kafka's `MAX_DELAY_MS`.
const MAX_DELAY_MS: u64 = 3_600_000;
/// The first attempt whose window is the maximum: Kafka's `ExponentialBackoff`
/// caps the exponent at `log2(MAX_DELAY_MS / INITIAL_DELAY_MS)`, which is
/// 6.9, and `INITIAL_DELAY_MS * 2^6.9` is `MAX_DELAY_MS`.
const FIRST_MAXIMAL_ATTEMPT: u32 = 7;

/// One draw of Kafka's jitter factor, `random(1 - 0.2, 1 + 0.2)`, in
/// thousandths: a value in `800..1200`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Jitter(u64);

impl Jitter {
    /// A fresh random draw.
    #[must_use]
    pub fn draw() -> Self {
        use std::hash::{BuildHasher as _, Hasher as _};

        // Every `RandomState` is keyed afresh, so an empty hash is a random
        // `u64` without a random-number crate.
        let random = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        Self(800 + random % 400)
    }

    /// A fixed factor of `per_mille` thousandths.
    #[cfg(test)]
    pub(crate) const fn per_mille(per_mille: u64) -> Self { Self(per_mille) }
}

/// The window of Kafka's `ExponentialBackoff(30 s, 2, 1 h, 0.2)` for the
/// `attempts`-th consecutive arm at one epoch.
fn delay(attempts: u32, jitter: Jitter) -> Duration {
    let term = if attempts >= FIRST_MAXIMAL_ATTEMPT {
        MAX_DELAY_MS
    } else {
        INITIAL_DELAY_MS << attempts
    };
    Duration::from_millis((term * jitter.0 / 1_000).min(MAX_DELAY_MS))
}

/// The armed window of one group, if any.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SolicitationBackoff {
    window: Option<Window>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    topology_epoch: i32,
    /// How many windows before this one were armed at the same epoch.
    attempts: u32,
    ends: Instant,
}

impl SolicitationBackoff {
    /// Kafka's `armIfNotActive`: arms a window at `topology_epoch` unless one
    /// is already running at that epoch, and answers whether it armed one.
    /// A window that follows an ended one at the same epoch is twice as long;
    /// one at another epoch starts the chain again.
    pub fn arm_if_not_active(&mut self, topology_epoch: i32, now: Instant, jitter: Jitter) -> bool {
        let attempts = match self.window {
            Some(window) if window.topology_epoch == topology_epoch => {
                if now < window.ends {
                    return false;
                }
                window.attempts.saturating_add(1)
            }
            _ => 0,
        };
        self.window = Some(Window {
            topology_epoch,
            attempts,
            ends: now + delay(attempts, jitter),
        });
        true
    }

    /// Kafka's `clear`: drops the window when it is at `topology_epoch`, after
    /// the description of that epoch is stored. A window that a heartbeat
    /// armed at a newer epoch stays.
    pub fn clear(&mut self, topology_epoch: i32) {
        if self
            .window
            .is_some_and(|window| window.topology_epoch == topology_epoch)
        {
            self.window = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// The windows of the chain at the neutral factor, its end points at
    /// the extreme factors, and the cap that the jitter cannot pass.
    #[test]
    fn the_window_doubles_from_30_seconds_to_an_hour() {
        let rows = [
            (0, 1_000, 30_000),
            (1, 1_000, 60_000),
            (2, 1_000, 120_000),
            (3, 1_000, 240_000),
            (4, 1_000, 480_000),
            (5, 1_000, 960_000),
            (6, 1_000, 1_920_000),
            (7, 1_000, 3_600_000),
            (40, 1_000, 3_600_000),
            (u32::MAX, 1_000, 3_600_000),
            (0, 800, 24_000),
            (0, 1_199, 35_970),
            (6, 1_199, 2_302_080),
            (7, 800, 2_880_000),
            (7, 1_199, 3_600_000),
        ];
        for (attempts, per_mille, millis) in rows {
            assert!(
                delay(attempts, Jitter::per_mille(per_mille)) == Duration::from_millis(millis),
                "attempt {attempts} at {per_mille}"
            );
        }
    }

    #[test]
    fn a_draw_is_kafkas_twenty_percent_jitter() {
        for _ in 0..1_000 {
            let Jitter(per_mille) = Jitter::draw();
            assert!((800..1_200).contains(&per_mille));
        }
    }

    /// One group's backoff through a run of heartbeats and pushes. Each step
    /// is an operation at a time in seconds, what `arm_if_not_active`
    /// answers, and the window afterwards as (epoch, attempts, end second).
    #[test]
    fn a_window_holds_off_a_second_request_until_it_ends_or_clears() {
        enum Op {
            Arm(i32, u64),
            Clear(i32),
        }
        /// The window as (epoch, attempts, end second).
        type Expected = Option<(i32, u32, u64)>;
        /// A label, the second, the operation, its answer, and the window.
        type Step = (&'static str, u64, Op, Option<bool>, Expected);
        use Op::{Arm, Clear};

        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        let rows: [Step; 9] = [
            (
                "the first heartbeat asks",
                0,
                Arm(0, 1_000),
                Some(true),
                Some((0, 0, 30)),
            ),
            (
                "a second member is not asked",
                10,
                Arm(0, 1_000),
                Some(false),
                Some((0, 0, 30)),
            ),
            (
                "still inside the window",
                29,
                Arm(0, 1_000),
                Some(false),
                Some((0, 0, 30)),
            ),
            (
                "an ended window asks again, twice as long",
                30,
                Arm(0, 1_000),
                Some(true),
                Some((0, 1, 90)),
            ),
            (
                "a newer epoch starts a fresh chain",
                40,
                Arm(1, 800),
                Some(true),
                Some((1, 0, 64)),
            ),
            (
                "a late push of the old epoch keeps the window",
                41,
                Clear(0),
                None,
                Some((1, 0, 64)),
            ),
            ("the push of the epoch clears it", 42, Clear(1), None, None),
            (
                "a cleared window asks at once",
                43,
                Arm(1, 1_000),
                Some(true),
                Some((1, 0, 73)),
            ),
            ("clearing again", 44, Clear(1), None, None),
        ];
        let mut backoff = SolicitationBackoff::default();
        for (row, secs, op, answered, window) in rows {
            let answer = match op {
                Arm(epoch, per_mille) => {
                    Some(backoff.arm_if_not_active(epoch, at(secs), Jitter::per_mille(per_mille)))
                }
                Clear(epoch) => {
                    backoff.clear(epoch);
                    None
                }
            };
            let expected = SolicitationBackoff {
                window: window.map(|(topology_epoch, attempts, ends)| Window {
                    topology_epoch,
                    attempts,
                    ends: at(ends),
                }),
            };
            assert!(answer == answered, "{row}");
            assert!(backoff == expected, "{row}");
        }
    }
}
