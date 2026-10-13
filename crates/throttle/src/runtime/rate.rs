//! The rate and burst side of [`TokenBucket`]: the typed setters, the
//! accessors that read the configuration back, and the reset that publishes
//! the `{rate, burst, available, fraction, last_refill}` group as one unit.
//!
//! The bucket stores micro-tokens, so every dimensioned quantity narrows here.
//! A byte rate and a byte burst keep their fractional part, so a rate of half
//! a byte per second is stored as half a byte per second. The byte pair and
//! the event pair stay separate because a token means a different thing in
//! each.

use {
    super::{BucketState, MICROS_PER_TOKEN, TokenBucket},
    krabka_units::prelude::{
        ByteRate, ByteRateExt as _, ByteSize, ByteSizeExt as _, Frequency, FrequencyExt as _, Time,
        secs,
    },
    num_traits::ToPrimitive as _,
    std::sync::atomic::Ordering::Relaxed,
};

/// The time window that [`TokenBucket::set_byte_rate`] uses for the burst
/// capacity when the caller does not give one. The burst is the throughput of
/// this window.
const DEFAULT_BURST_WINDOW: Time = secs(1);

/// [`MICROS_PER_TOKEN`] as a float, for the fractional conversions.
const MICROS_PER_TOKEN_F64: f64 = 1_000_000.0;

/// A token count, fractional or not, in micro-tokens, rounded to the nearest.
///
/// A negative or `NaN` count is not a quantity, so it becomes `0`, and
/// anything past `u64::MAX` micro-tokens saturates.
fn tokens_to_micros(tokens: f64) -> u64 {
    if tokens.is_nan() || tokens <= 0.0 {
        return 0;
    }
    (tokens * MICROS_PER_TOKEN_F64)
        .round()
        .to_u64()
        .unwrap_or(u64::MAX)
}

/// A rate in tokens per second, fractional or not, as the bucket's stored
/// micro-tokens per second.
///
/// A rate that is not positive is not a throughput, so it becomes `0`, the
/// bucket's "no limit configured" sentinel. A positive rate never does: one
/// under the storage resolution of a micro-token per second is stored as
/// that resolution, the slowest rate the bucket can meter.
fn rate_to_micros(tokens_per_sec: f64) -> u64 {
    if tokens_per_sec.is_nan() || tokens_per_sec <= 0.0 {
        return 0;
    }
    tokens_to_micros(tokens_per_sec).max(1)
}

/// A stored micro-token count as tokens. Exact below 2^53 micro-tokens.
fn micros_to_tokens(micros: u64) -> f64 {
    micros.to_f64().unwrap_or(f64::INFINITY) / MICROS_PER_TOKEN_F64
}

impl TokenBucket {
    /// Updates the rate in raw tokens per second, with a one-second burst at
    /// the new rate. The balance is kept: see
    /// [`Self::set_token_rate_with_burst`].
    ///
    /// This is the primitive. The bucket counts tokens and does not know what a
    /// token means. Callers that meter a dimensioned quantity should use the
    /// typed pair that names the dimension: [`Self::set_byte_rate`] or
    /// [`Self::set_event_rate`].
    pub fn set_token_rate(&self, tokens_per_sec: u64) {
        self.set_token_rate_with_burst(tokens_per_sec, tokens_per_sec);
    }

    /// Updates the rate and the independent burst capacity, both in raw tokens.
    ///
    /// A bucket that already has a rate keeps its balance across the change,
    /// as Kafka's `ClientQuotaManager.updateQuotaMetricConfigs` only swaps a
    /// metric's bound and leaves what it recorded: the time up to now is
    /// refilled at the old rate, the balance is capped at the new burst, and
    /// debt stays debt, so a client in debt is still throttled after a change
    /// of its quota. A bucket that had no rate has recorded nothing, so it
    /// starts at `burst`.
    pub fn set_token_rate_with_burst(&self, new_rate: u64, burst: u64) {
        self.set_micro_rate_with_burst(
            new_rate.saturating_mul(MICROS_PER_TOKEN),
            burst.saturating_mul(MICROS_PER_TOKEN),
        );
    }

    /// Updates the rate and the burst, both in micro-tokens, as
    /// [`Self::set_token_rate_with_burst`] does.
    ///
    /// It stores the whole `{rate, burst, available, debt, fraction, last_refill}` group
    /// in one critical section, so a concurrent [`Self::try_consume`] runs
    /// either wholly before the reset or wholly after it. No consume can
    /// commit a balance it computed under the old configuration.
    fn set_micro_rate_with_burst(&self, micro_rate_per_sec: u64, micro_burst: u64) {
        let mut state = self.lock_state();
        let now = self.now_nanos();
        let (micro_available, micro_debt, micro_refill_fraction) =
            if state.micro_rate_per_sec == 0 || micro_rate_per_sec == 0 {
                (micro_burst, 0, 0)
            } else {
                let refilled = state.refilled(now);
                let available = refilled.available.min(micro_burst);
                let fraction = if available == micro_burst && refilled.debt == 0 {
                    0
                } else {
                    refilled.fraction
                };
                (available, refilled.debt, fraction)
            };
        *state = BucketState {
            micro_rate_per_sec,
            micro_burst,
            micro_available,
            micro_debt,
            last_refill_nanos: now.max(state.last_refill_nanos),
            micro_refill_fraction,
        };
        self.micro_rate_per_sec.store(micro_rate_per_sec, Relaxed);
    }

    /// The configured rate in whole raw tokens per second, rounded down. `0`
    /// means no limit, or a rate under one token per second:
    /// [`Self::byte_rate`] reads a fractional rate back exactly.
    #[must_use]
    pub fn token_rate(&self) -> u64 { self.fast_path_rate() / MICROS_PER_TOKEN }

    /// The configured burst capacity in whole raw tokens, rounded down. This
    /// is the most the bucket holds.
    #[must_use]
    pub fn token_burst(&self) -> u64 { self.lock_state().micro_burst / MICROS_PER_TOKEN }

    /// Updates a byte throughput and bursts one second's worth, keeping the
    /// balance as [`Self::set_token_rate_with_burst`] says.
    ///
    /// The burst is `rate * DEFAULT_BURST_WINDOW`. `uom` type-checks this as a
    /// [`ByteRate`] times a [`Time`], which gives a [`ByteSize`].
    pub fn set_byte_rate(&self, new_rate: ByteRate) {
        self.set_byte_rate_with_burst(new_rate, (new_rate * DEFAULT_BURST_WINDOW).into());
    }

    /// Updates a byte throughput and an independent byte burst capacity.
    ///
    /// Both keep their fractional part to a millionth of a byte, so a rate of
    /// half a byte per second is enforced as half a byte per second. Kafka
    /// holds a byte-rate quota as a double. A positive rate is never stored
    /// as the unlimited rate `0`. The balance is kept, as
    /// [`Self::set_token_rate_with_burst`] says.
    pub fn set_byte_rate_with_burst(&self, new_rate: ByteRate, burst: ByteSize) {
        self.set_micro_rate_with_burst(
            rate_to_micros(new_rate.bytes_per_sec_f64()),
            tokens_to_micros(burst.bytes_f64()),
        );
    }

    /// The configured byte throughput, fractional part included.
    /// [`krabka_units::prelude::ByteRateExt::ZERO`] means no limit.
    #[must_use]
    pub fn byte_rate(&self) -> ByteRate {
        ByteRate::from_bytes_per_sec_f64(micros_to_tokens(self.fast_path_rate()))
    }

    /// Whether the bucket already runs at `rate`, as
    /// [`Self::set_byte_rate_with_burst`] would store it.
    ///
    /// A caller that re-applies a configured rate compares with this rather
    /// than with [`Self::byte_rate`]: a rate finer than a micro-token per
    /// second reads back rounded, so a comparison that never matched would
    /// reset the bucket on every re-apply.
    #[must_use]
    pub fn runs_at_byte_rate(&self, rate: ByteRate) -> bool {
        self.fast_path_rate() == rate_to_micros(rate.bytes_per_sec_f64())
    }

    /// The configured byte burst capacity, fractional part included.
    #[must_use]
    pub fn byte_burst(&self) -> ByteSize {
        ByteSize::from_bytes_f64(micros_to_tokens(self.lock_state().micro_burst))
    }

    /// Updates an event throughput, such as samples, records, or requests, and
    /// bursts one second's worth.
    ///
    /// A token here is one event, not one byte. If you meter events with the
    /// byte pair above, the code compiles but the result is wrong. This is why
    /// the two pairs are separate.
    pub fn set_event_rate(&self, new_rate: Frequency) {
        let per_sec = new_rate.per_sec_u64();
        self.set_token_rate_with_burst(per_sec, per_sec);
    }

    /// Updates an event throughput and an independent burst, in whole events.
    pub fn set_event_rate_with_burst(&self, new_rate: Frequency, burst: u64) {
        self.set_token_rate_with_burst(new_rate.per_sec_u64(), burst);
    }

    /// The configured event throughput in whole events per second, rounded
    /// down. Zero means no limit.
    #[must_use]
    pub fn event_rate(&self) -> Frequency { Frequency::from_per_sec_u64(self.token_rate()) }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{bytes, bytes_per_sec, kibibytes, kibibytes_per_sec, mebibytes},
    };

    // The bucket narrows every rate and burst to raw bytes and bytes-per-second
    // for the verified integer kernel, so the accessors are the only place a
    // scale factor could go missing. Each case pairs a value written in one unit
    // with the byte-denominated magnitude it must read back as: a dropped or
    // doubled 1024 in either direction fails here.
    #[test]
    fn rate_and_burst_round_trip_through_the_accessors() {
        let cases = [
            (bytes_per_sec(0), bytes(0)),
            (bytes_per_sec(1), bytes(1)),
            (kibibytes_per_sec(1), bytes(1024)),
            (bytes_per_sec(1024), kibibytes(1)),
            (bytes_per_sec(3_000_000), mebibytes(2)),
            (kibibytes_per_sec(64), mebibytes(1)),
            (
                ByteRate::from_bytes_per_sec_f64(0.5),
                ByteSize::from_bytes_f64(5.5),
            ),
            (
                ByteRate::from_bytes_per_sec_f64(1.25),
                ByteSize::from_bytes_f64(0.75),
            ),
            (
                ByteRate::from_bytes_per_sec_f64(0.000_001),
                ByteSize::from_bytes_f64(0.000_011),
            ),
        ];

        for (rate, burst) in cases {
            let b = TokenBucket::new();
            b.set_byte_rate_with_burst(rate, burst);
            check!((b.byte_rate(), b.byte_burst()) == (rate, burst));
        }
    }

    /// The event pair exists separately from the byte pair because a token
    /// means a different thing in each -- the API's own warning is that mixing
    /// them "compiles but the result is wrong". Only the byte pair was round
    /// tripped, so nothing checked that an event rate reads back as one, nor
    /// that the raw token primitive underneath both publishes what it is given.
    #[test]
    fn event_and_token_rates_round_trip_through_the_accessors() {
        // (rate per second, independent burst in whole events)
        let cases = [(0u64, 0u64), (1, 1), (10, 100), (1_000, 250)];

        for (per_sec, burst) in cases {
            let rate = Frequency::from_per_sec_u64(per_sec);

            let b = TokenBucket::new();
            b.set_event_rate_with_burst(rate, burst);
            check!((b.event_rate(), b.token_burst()) == (rate, burst));

            // `set_event_rate` bursts one second of the rate, as the byte pair does.
            let b = TokenBucket::new();
            b.set_event_rate(rate);
            check!((b.event_rate(), b.token_burst()) == (rate, per_sec));

            // The untyped primitive both pairs delegate to.
            let b = TokenBucket::new();
            b.set_token_rate(per_sec);
            check!((b.token_rate(), b.token_burst()) == (per_sec, per_sec));
        }
    }

    /// A positive byte rate never reads back as the unlimited rate `0`:
    /// one under the storage resolution is stored as that resolution, and a
    /// rate that is not positive is no limit.
    #[test]
    fn a_positive_byte_rate_is_never_unlimited() {
        let resolution = ByteRate::from_bytes_per_sec_f64(0.000_001);
        let cases = [
            (1.0e-12, resolution),
            (0.000_000_4, resolution),
            (0.0, ByteRate::from_bytes_per_sec(0)),
            (-3.0, ByteRate::from_bytes_per_sec(0)),
            (f64::NAN, ByteRate::from_bytes_per_sec(0)),
        ];
        for (rate, want) in cases {
            let b = TokenBucket::new();
            b.set_byte_rate_with_burst(ByteRate::from_bytes_per_sec_f64(rate), bytes(1));
            check!(b.byte_rate() == want, "{rate}");
        }
    }

    /// A rate finer than the storage resolution still matches the bucket it
    /// configured, and a different rate does not.
    #[test]
    fn runs_at_byte_rate_compares_the_stored_rate() {
        let b = TokenBucket::new();
        b.set_byte_rate_with_burst(ByteRate::from_bytes_per_sec_f64(0.123_456_7), bytes(1));
        let cases = [
            (0.123_456_7, true),
            (0.123_457, true),
            (0.123_456, false),
            (0.5, false),
            (0.0, false),
        ];
        for (rate, want) in cases {
            check!(
                b.runs_at_byte_rate(ByteRate::from_bytes_per_sec_f64(rate)) == want,
                "{rate}"
            );
        }
    }

    // `set_rate` derives the burst from one second's worth of the rate, so the
    // burst it publishes must be the byte count the rate delivers in that
    // second — not the rate's bare number in some other unit.
    #[test]
    fn set_rate_bursts_one_second_of_throughput() {
        let b = TokenBucket::new();
        b.set_byte_rate(kibibytes_per_sec(64));
        check!((b.byte_rate(), b.byte_burst()) == (kibibytes_per_sec(64), kibibytes(64)));
    }
}
