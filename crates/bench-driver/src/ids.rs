//! Newtypes over the bare integers that the driver and report layers thread
//! around. A mix-up between two same-typed values becomes a compile error and
//! not a silently wrong benchmark number. Two such pairs are a message count
//! against a window length, and a wallclock epoch-ms against a ms-into-run
//! offset.
//!
//! Each type is `#[serde(transparent)]`, so the on-disk `RunOutput` JSON is
//! byte-identical to the bare primitive it wraps. The report aggregator and
//! any external tool that reads the artifacts see no change.

use {crate::numeric::saturating_u64_to_i64, krabka_units::prelude::*};

/// A number of Kafka messages (produced, consumed, or dropped).
#[krabka_macros::scalar_id]
#[derive(Default)]
pub struct MessageCount(pub u64);

/// Milliseconds **into a run**. This is a time-series sample offset relative to
/// the start of the measurement window or the wallclock window. It is ordered,
/// so it can key the per-offset averaging maps. Do not confuse it with
/// [`WallclockMs`], which is an absolute unix-epoch timestamp.
///
/// This stays an integer and does not become a [`Time`], because it is a
/// *coordinate* on the fixed sampling grid. The cross-run averaging in
/// [`crate::aggregate`] keys `BTreeMap`s by it, which a `f64`-backed quantity
/// cannot do. [`Self::as_time`] and [`Self::since`] are the seams that turn a
/// pair of coordinates into the extent between them.
#[krabka_macros::scalar_id]
#[derive(Default)]
pub struct TimeOffsetMs(pub u64);

impl TimeOffsetMs {
    /// This offset as the time extent from the window start.
    #[must_use]
    pub fn as_time(self) -> Time { Time::from_millis(saturating_u64_to_i64(self.0)) }

    /// The extent from `earlier` to this offset. This is no time at all when
    /// the two are the wrong way round.
    #[must_use]
    pub fn since(self, earlier: Self) -> Time {
        Time::from_millis(saturating_u64_to_i64(self.0.saturating_sub(earlier.0)))
    }
}

/// An **absolute** wallclock timestamp in unix-epoch milliseconds. It is an
/// `i64` to match `chrono::Utc::now().timestamp_millis()`. It is distinct from
/// the run-relative [`TimeOffsetMs`], so no one can swap the paired start and
/// end fields on `RunOutput`.
#[krabka_macros::scalar_id]
pub struct WallclockMs(pub i64);

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn scalar_ids_keep_their_json_and_primitive_interfaces() {
        let count = MessageCount::from(17_u64);
        check!(serde_json::to_string(&count).unwrap() == "17");
        check!(serde_json::from_str::<MessageCount>("17").unwrap() == count);
        check!(count > 16_u64);
        check!(count.to_string() == "17");
        check!(format!("{count:?}") == "MessageCount(17)");
        check!(MessageCount::default() == 0_u64);

        let offset = TimeOffsetMs::from(u64::MAX);
        let encoded = "18446744073709551615";
        check!(serde_json::to_string(&offset).unwrap() == encoded);
        check!(serde_json::from_str::<TimeOffsetMs>(encoded).unwrap() == offset);
        check!(TimeOffsetMs::default() == 0_u64);
        let raw_offset: u64 = offset.into();
        check!(raw_offset == u64::MAX);

        let wallclock = WallclockMs::from(i64::MIN);
        let encoded = "-9223372036854775808";
        check!(serde_json::to_string(&wallclock).unwrap() == encoded);
        check!(serde_json::from_str::<WallclockMs>(encoded).unwrap() == wallclock);
        let raw_wallclock: i64 = wallclock.into();
        check!(raw_wallclock == i64::MIN);
    }

    #[test]
    fn offsets_convert_to_extents_and_differences() {
        check!(TimeOffsetMs(2_500).as_time() == millis(2500));
        check!(TimeOffsetMs(6_000).since(TimeOffsetMs(4_000)) == secs(2));
        // Out of order: no negative extents.
        check!(TimeOffsetMs(4_000).since(TimeOffsetMs(6_000)) == Time::ZERO);
    }
}
