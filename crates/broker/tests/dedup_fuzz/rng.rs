//! A small seeded generator, so that a run replays from its seed alone.
//!
//! Every iteration draws from its own generator, derived from the run seed
//! and the iteration number, so one failing iteration replays without the
//! iterations before it.

/// `SplitMix64`: a few lines, fast, and good enough to drive a mutator.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// The generator of iteration `iteration` of the run seeded `seed`.
    pub fn for_iteration(seed: u64, iteration: u64) -> Self {
        let mut mixer = Self { state: seed };
        let base = mixer.next_u64();
        Self {
            state: base ^ iteration.wrapping_mul(0xD1B5_4A32_D192_ED03),
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..bound`, or 0 when `bound` is 0.
    pub fn below(&mut self, bound: usize) -> usize {
        let Ok(wide) = u64::try_from(bound) else {
            return 0;
        };
        if wide == 0 {
            return 0;
        }
        usize::try_from(self.next_u64() % wide).unwrap_or(0)
    }

    /// True with probability `1 / n`.
    pub fn one_in(&mut self, n: usize) -> bool { self.below(n) == 0 }

    /// One of `items`, which must not be empty.
    pub fn pick<T: Copy>(&mut self, items: &[T]) -> T { items[self.below(items.len())] }

    /// A value in `low..=high`.
    pub fn between(&mut self, low: i64, high: i64) -> i64 {
        let span = u64::try_from(i128::from(high) - i128::from(low) + 1).unwrap_or(u64::MAX);
        let offset = if span == 0 {
            self.next_u64()
        } else {
            self.next_u64() % span
        };
        i64::try_from(i128::from(low) + i128::from(offset)).unwrap_or(high)
    }

    /// One random byte.
    pub fn byte(&mut self) -> u8 { self.next_u64().to_le_bytes()[0] }

    /// `len` random bytes.
    pub fn bytes(&mut self, len: usize) -> Vec<u8> { (0..len).map(|_| self.byte()).collect() }

    /// Fewer than `max` random bytes.
    pub fn some_bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max);
        self.bytes(len)
    }

    /// At least `min` and fewer than `min + spread` random bytes.
    pub fn some_bytes_from(&mut self, min: usize, spread: usize) -> Vec<u8> {
        let len = min + self.below(spread);
        self.bytes(len)
    }
}
