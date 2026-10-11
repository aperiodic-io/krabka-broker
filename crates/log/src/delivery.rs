//! Deliver-at-time visibility: when a scheduled batch becomes readable.
//!
//! On a topic set to [`DeliveryPolicy::Scheduled`](crate::DeliveryPolicy), a
//! record batch is not visible the moment it is durable. It becomes visible
//! once its activation time has passed. The activation time is the batch's
//! `max_timestamp`, the v2 header field, so a producer schedules delivery
//! through the timestamp it stamps and no sidecar is needed.
//!
//! The schedule lives entirely in the records, which is why nothing here is
//! persisted. A watermark is derived state: after a restart or a leader
//! change it is recomputed from the segments, and a recomputation can only
//! agree with what the records say.

use krabka_ids::Offset;

use crate::DeliveryPolicy;

/// Result of
/// [`Log::advance_delivery_watermark`](crate::Log::advance_delivery_watermark).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryAdvance {
    /// First offset that is not visible yet. Every offset below it has
    /// reached its activation time, so a fetch may serve it.
    ///
    /// On a topic that does not schedule delivery this is the log end offset:
    /// everything written is visible.
    pub watermark: Offset,

    /// Epoch-millisecond instant at which the first waiting batch above
    /// [`Self::watermark`] becomes visible, or `None` when nothing is waiting.
    ///
    /// The value already includes the configured clock-uncertainty bound, so
    /// a scheduler compares it against its own clock and adds nothing. A
    /// caller that wakes at this instant and advances the watermark again
    /// finds that batch visible.
    pub next_deadline_ms: Option<i64>,
}

/// The instant a batch that activates at `activation_ms` becomes visible.
///
/// The bound is added, never subtracted: if the clock reads `c` while true
/// time lies in `[c - e, c + e]`, then `c >= activation + e` proves true time
/// has reached the activation instant, so delivery is never early.
pub(crate) fn visible_at_ms(activation_ms: i64, uncertainty_ms: i64) -> i64 {
    activation_ms.saturating_add(uncertainty_ms)
}

/// Apply the shared scheduled-delivery visibility rule to one batch.
#[must_use]
pub fn batch_is_deliverable(
    policy: DeliveryPolicy,
    uncertainty_ms: i64,
    activation_ms: i64,
    now_ms: i64,
) -> bool {
    krabka_verified::scheduled_delivery_visible(
        policy == DeliveryPolicy::Scheduled,
        uncertainty_ms,
        activation_ms,
        now_ms,
    )
}

/// Merge inclusive offset ranges that touch or overlap.
///
/// The walk produces them in ascending order, one per batch, so a single pass
/// is enough. Adjacent batches merge because `[0, 4]` and `[5, 9]` describe
/// one uninterrupted run of waiting records.
pub(crate) fn coalesce_ranges(ranges: Vec<(Offset, Offset)>) -> Vec<(Offset, Offset)> {
    let mut out: Vec<(Offset, Offset)> = Vec::with_capacity(ranges.len());
    for (low, high) in ranges {
        match out.last_mut() {
            Some(last) => {
                let (merge, merged_high) =
                    krabka_verified::coalesce_delivery_range(last.1.0, low.0, high.0);
                if merge {
                    last.1 = Offset(merged_high);
                } else {
                    out.push((low, high));
                }
            }
            None => out.push((low, high)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn adjacent_and_overlapping_ranges_merge_but_separated_ones_do_not() {
        let merged = coalesce_ranges(vec![
            (Offset(0), Offset(4)),
            // Touches the previous range.
            (Offset(5), Offset(9)),
            // Contained in the run so far: the end must not move backwards.
            (Offset(6), Offset(7)),
            // One offset of daylight: a separate range.
            (Offset(11), Offset(12)),
        ]);
        check!(merged == vec![(Offset(0), Offset(9)), (Offset(11), Offset(12))]);
        check!(
            coalesce_ranges(vec![
                (Offset(i64::MAX - 1), Offset(i64::MAX)),
                (Offset(i64::MAX), Offset(i64::MAX)),
            ]) == vec![(Offset(i64::MAX - 1), Offset(i64::MAX))]
        );
    }

    #[test]
    fn no_ranges_merge_to_nothing() {
        check!(coalesce_ranges(Vec::new()).is_empty());
    }

    #[test]
    fn the_visible_instant_adds_the_bound_and_saturates() {
        check!(visible_at_ms(1_000, 250) == 1_250);
        check!(visible_at_ms(i64::MAX, 250) == i64::MAX);
    }

    #[test]
    fn batch_visibility_delegates_to_the_proved_deadline_rule() {
        check!(batch_is_deliverable(
            crate::DeliveryPolicy::Immediate,
            250,
            10_000,
            0
        ));
        check!(!batch_is_deliverable(
            crate::DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_249
        ));
        check!(batch_is_deliverable(
            crate::DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_250
        ));
    }
}
