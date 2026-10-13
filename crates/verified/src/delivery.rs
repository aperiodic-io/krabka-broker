//! Scheduled-delivery visibility kernels.

#[cfg(creusot)]
use creusot_std::prelude::{Int, logic};
use creusot_std::prelude::{ensures, requires};

/// Decide whether a record batch may be exposed at the supplied clock reading.
///
/// A negative uncertainty is invalid and fails closed for scheduled delivery.
/// A deadline that cannot be represented also fails closed instead of becoming
/// visible early through saturating arithmetic.
#[ensures(result == (!scheduled || (uncertainty_ms@ >= 0
    && activation_ms@ + uncertainty_ms@ <= i64::MAX@
    && activation_ms@ + uncertainty_ms@ <= now_ms@)))]
#[must_use]
pub fn scheduled_delivery_visible(
    scheduled: bool,
    uncertainty_ms: i64,
    activation_ms: i64,
    now_ms: i64,
) -> bool {
    if !scheduled {
        return true;
    }
    if uncertainty_ms < 0 {
        return false;
    }
    let Some(deadline_ms) = activation_ms.checked_add(uncertainty_ms) else {
        return false;
    };
    deadline_ms <= now_ms
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn clamp_to(value: Int, low: Int, high: Int) -> Int {
    pearlite! { if value < low { low } else if value > high { high } else { value } }
}

/// Bound one scheduled-delivery watermark step to the live log range.
///
/// The current watermark is first clamped into `[log_start, log_end]` after
/// retention or truncation. The step then moves it to the candidate when the
/// candidate is ahead, capped at the log end:
/// `min(max(candidate, clamped current), log_end)`. It never moves the clamped
/// watermark backwards or past the log end.
#[requires(log_start@ <= log_end@)]
#[ensures(result@ == candidate@.max(clamp_to(current@, log_start@, log_end@)).min(log_end@))]
#[ensures(log_start@ <= result@ && result@ <= log_end@)]
#[ensures(current@ <= candidate@ && log_start@ <= candidate@ && candidate@ <= log_end@
    ==> result@ == candidate@)]
#[must_use]
pub fn delivery_watermark_advance(
    log_start: i64,
    current: i64,
    candidate: i64,
    log_end: i64,
) -> i64 {
    let bounded_current = if current < log_start {
        log_start
    } else if current > log_end {
        log_end
    } else {
        current
    };
    if candidate <= bounded_current {
        bounded_current
    } else if candidate >= log_end {
        log_end
    } else {
        candidate
    }
}

/// Decide whether the next inclusive range touches the preceding range and
/// return the maximum end offset for a merged range.
///
/// The comparison is expressed without overflowing `last_high + 1` at
/// `i64::MAX`.
#[ensures(result.0 == (low@ <= last_high@ + 1))]
#[ensures(result.1@ >= last_high@ && result.1@ >= high@)]
#[ensures(result.1@ == last_high@ || result.1@ == high@)]
#[must_use]
pub fn coalesce_delivery_range(last_high: i64, low: i64, high: i64) -> (bool, i64) {
    let merge = if low <= last_high {
        true
    } else {
        // `low > last_high` proves `last_high < i64::MAX`.
        low == last_high + 1
    };
    let merged_high = if high > last_high { high } else { last_high };
    (merge, merged_high)
}

#[cfg(test)]
mod tests {
    use {
        super::{coalesce_delivery_range, delivery_watermark_advance, scheduled_delivery_visible},
        assert2::assert,
    };

    #[test]
    fn visibility_never_ignores_deadline_or_overflow() {
        assert!(scheduled_delivery_visible(false, 250, i64::MAX, i64::MIN));
        assert!(!scheduled_delivery_visible(true, -1, 10_000, 10_000));
        assert!(scheduled_delivery_visible(true, 0, 10_000, 10_000));
        assert!(!scheduled_delivery_visible(true, 250, 10_000, 10_249));
        assert!(scheduled_delivery_visible(true, 250, 10_000, 10_250));
        assert!(!scheduled_delivery_visible(true, 1, i64::MAX, i64::MAX));
    }

    #[test]
    fn watermark_step_is_monotonic_inside_live_bounds() {
        assert!(delivery_watermark_advance(5, 7, 9, 10) == 9);
        assert!(delivery_watermark_advance(5, 7, 6, 10) == 7);
        assert!(delivery_watermark_advance(5, 7, 12, 10) == 10);
        assert!(delivery_watermark_advance(5, 2, 8, 10) == 8);
        assert!(delivery_watermark_advance(5, 2, 4, 10) == 5);
        assert!(delivery_watermark_advance(5, 12, 12, 10) == 10);
    }

    #[test]
    fn range_step_merges_only_overlap_or_adjacency() {
        assert!(coalesce_delivery_range(4, 5, 9) == (true, 9));
        assert!(coalesce_delivery_range(9, 6, 7) == (true, 9));
        assert!(coalesce_delivery_range(9, 11, 12) == (false, 12));
        assert!(coalesce_delivery_range(i64::MAX, i64::MAX, i64::MAX) == (true, i64::MAX));
    }
}
