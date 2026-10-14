//! A seeded `SplitMix64` generator: the one source of randomness of a chaos
//! run, so that a seed names the same fault plan and the same producer
//! choices every time it runs.

use std::time::Duration;

/// The `SplitMix64` increment, the odd integer nearest `2^64 / phi`.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The output function of `SplitMix64`.
fn mix(state: u64) -> u64 {
    let state = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let state = (state ^ (state >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    state ^ (state >> 31)
}

/// A `SplitMix64` stream.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// The stream `lane` of `seed`. Each producer and the fault planner draw
    /// from their own lane, so how many values one of them draws does not
    /// move what another one draws.
    pub fn lane(seed: u64, lane: u64) -> Self { Self(mix(seed ^ mix(lane.wrapping_mul(GAMMA)))) }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(GAMMA);
        mix(self.0)
    }

    /// A value in `0..bound`, or 0 when `bound` is 0. The modulo bias is
    /// far below anything a fault plan can notice.
    pub fn below(&mut self, bound: u64) -> u64 { self.next_u64() % bound.max(1) }

    /// A value in `low..=high`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high.saturating_sub(low) + 1)
    }

    /// True `percent` times out of a hundred.
    pub fn chance(&mut self, percent: u64) -> bool { self.below(100) < percent }

    /// An index into a slice of `len` elements.
    pub fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("a slice length fits u64");
        usize::try_from(self.below(len)).expect("an index below a usize length")
    }

    /// A duration of `low_ms..=high_ms` milliseconds.
    pub fn millis(&mut self, low_ms: u64, high_ms: u64) -> Duration {
        Duration::from_millis(self.between(low_ms, high_ms))
    }

    /// One of `weights.len()` outcomes, each as likely as its weight.
    pub fn weighted(&mut self, weights: &[u64]) -> usize {
        let mut roll = self.below(weights.iter().sum());
        for (outcome, weight) in weights.iter().enumerate() {
            if roll < *weight {
                return outcome;
            }
            roll -= weight;
        }
        weights.len().saturating_sub(1)
    }
}
