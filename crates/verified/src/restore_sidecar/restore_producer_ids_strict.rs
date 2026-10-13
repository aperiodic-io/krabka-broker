#[cfg(creusot)]
use super::{Int, invariant};
use {
    super::{RestoreAbortedTxn, RestoreSegmentExtent},
    creusot_std::prelude::*,
};

/// Compute the largest relative offset an archived sidecar may reference.
///
/// Kafka's sparse indexes store offsets relative to the segment base as a
/// 32-bit value (`AbstractIndex.relativeOffset`), so a segment whose span does
/// not fit `u32` has no valid index. The frontier is returned as `i64` so the
/// entry validators compare widened `u32` offsets against it and no narrowing
/// conversion is needed.
#[ensures(match result {
    Some(frontier) => segment_base@ >= 0
        && segment_end@ >= segment_base@
        && frontier@ == segment_end@ - segment_base@
        && frontier@ <= u32::MAX@,
    None => segment_base@ < 0
        || segment_end@ < segment_base@
        || segment_end@ - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn restore_index_frontier(segment_base: i64, segment_end: i64) -> Option<i64> {
    if segment_base < 0 || segment_end < segment_base {
        return None;
    }
    let relative = segment_end - segment_base;
    if relative > i64::from(u32::MAX) {
        None
    } else {
        Some(relative)
    }
}

/// Validate one decoded offset-index entry against its predecessor and the
/// verified log extent.
#[ensures(result == (relative_offset@ <= max_relative@
    && position@ < log_bytes@
    && match previous {
        Some((previous_relative, previous_position)) =>
            previous_relative@ < relative_offset@ && previous_position@ < position@,
        None => true,
    }))]
#[must_use]
pub fn restore_offset_index_entry_valid(
    previous: Option<(u32, u32)>,
    relative_offset: u32,
    position: u32,
    max_relative: i64,
    log_bytes: u64,
) -> bool {
    i64::from(relative_offset) <= max_relative
        && u64::from(position) < log_bytes
        && match previous {
            Some((previous_relative, previous_position)) => {
                previous_relative < relative_offset && previous_position < position
            }
            None => true,
        }
}

/// Validate one decoded time-index entry. Relative offsets strictly advance;
/// timestamps may repeat but may not decrease.
///
/// The segment writer emits strictly newer running maxima, at the last offset
/// of the batch that established each maximum. Archive validation also admits
/// repeated maxima: non-decreasing timestamps with advancing offsets suffice
/// for the binary lookup and strict-start kernels. Structural validity cannot
/// establish truthful record prefix bounds; those require faithful construction
/// or a separate semantic check against complete decoded records.
#[ensures(result == (relative_offset@ <= max_relative@
    && match previous {
        Some((previous_timestamp, previous_relative)) =>
            previous_timestamp@ <= timestamp@ && previous_relative@ < relative_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_time_index_entry_valid(
    previous: Option<(i64, u32)>,
    timestamp: i64,
    relative_offset: u32,
    max_relative: i64,
) -> bool {
    i64::from(relative_offset) <= max_relative
        && match previous {
            Some((previous_timestamp, previous_relative)) => {
                previous_timestamp <= timestamp && previous_relative < relative_offset
            }
            None => true,
        }
}

/// Validate one decoded aborted-transaction index entry against the previous
/// entry's `last_offset`.
///
/// An aborted transaction is indexed in the segment that holds its abort
/// marker, in marker order, so `last_offset` (the marker) lies inside the
/// segment and strictly increases across entries (Kafka's
/// `TransactionIndex.append` rejects a non-increasing `lastOffset`, and
/// `TransactionIndex.sanityCheck` requires `lastOffset` at or past the index's
/// start offset). `start_offset` is neither bounded below by the segment base
/// (a transaction may span a segment roll) nor ordered across entries
/// (transactions interleave: one opened later can abort first).
#[ensures(result == (entry.producer_id@ >= 0
    && segment.base_offset@ >= 0
    && entry.start_offset@ >= 0
    && entry.start_offset@ <= entry.last_offset@
    && segment.base_offset@ <= entry.last_offset@
    && entry.last_offset@ <= segment.last_offset@
    && match previous_last {
        Some(previous) => previous@ < entry.last_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_txn_index_entry_valid(
    previous_last: Option<i64>,
    entry: RestoreAbortedTxn,
    segment: RestoreSegmentExtent,
) -> bool {
    entry.producer_id >= 0
        && segment.base_offset >= 0
        && entry.start_offset >= 0
        && entry.start_offset <= entry.last_offset
        && segment.base_offset <= entry.last_offset
        && entry.last_offset <= segment.last_offset
        && match previous_last {
            Some(previous) => previous < entry.last_offset,
            None => true,
        }
}

/// Validate one segment-scoped leader-epoch checkpoint row.
#[ensures(result == (epoch@ >= 0
    && segment_base@ >= 0
    && segment_base@ <= start_offset@
    && start_offset@ <= segment_end@
    && match previous {
        Some((previous_epoch, previous_start)) =>
            previous_epoch@ < epoch@ && previous_start@ < start_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_leader_epoch_entry_valid(
    previous: Option<(i32, i64)>,
    epoch: i32,
    start_offset: i64,
    segment_base: i64,
    segment_end: i64,
) -> bool {
    epoch >= 0
        && segment_base >= 0
        && segment_base <= start_offset
        && start_offset <= segment_end
        && match previous {
            Some((previous_epoch, previous_start)) => {
                previous_epoch < epoch && previous_start < start_offset
            }
            None => true,
        }
}

/// Accept exactly the canonical strictly increasing producer-ID order. Strict
/// order also proves every ID is unique while keeping validation linear.
#[ensures(result == (forall<i: Int> 1 <= i && i < producer_ids@.len()
    ==> producer_ids@[i - 1]@ < producer_ids@[i]@))]
#[must_use]
pub fn restore_producer_ids_strict(producer_ids: &[i64]) -> bool {
    let mut index = 0usize;
    #[invariant(index@ <= producer_ids@.len())]
    #[invariant(forall<i: Int> 1 <= i && i < index@
        ==> producer_ids@[i - 1]@ < producer_ids@[i]@)]
    #[variant(producer_ids@.len() - index@)]
    while index < producer_ids.len() {
        if index > 0 && producer_ids[index - 1] >= producer_ids[index] {
            return false;
        }
        index += 1;
    }
    true
}
