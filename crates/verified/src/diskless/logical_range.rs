use creusot_std::prelude::*;

use super::DisklessRetentionPolicy;

/// The bytes of the oldest `count` ranges.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(count)]
pub fn indexed_bytes(byte_lens: Seq<u64>, count: Int) -> Int {
    pearlite! {
        if count <= 0 {
            0
        } else {
            indexed_bytes(byte_lens, count - 1) + byte_lens[count - 1]@
        }
    }
}

/// Kafka's `diff` when the size walk reaches range `i`: the index's bytes
/// over `retention.bytes`, less the ranges already expired before `i`. It is
/// negative when the index fits the budget.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn size_debt_before(byte_lens: Seq<u64>, budget: u64, i: Int) -> Int {
    pearlite! {
        indexed_bytes(byte_lens, byte_lens.len()) - budget@ - indexed_bytes(byte_lens, i)
    }
}

/// Whether Kafka's `UnifiedLog.deleteOldSegments` lets range `i` expire,
/// given that every older range expires first.
///
/// - `deleteLogStartOffsetBreachedSegments`: the range ends below the
///   `DeleteRecords` floor.
/// - `deleteRetentionSizeBreachedSegments`: `diff - segmentSize >= 0`, so
///   `retention.bytes` never deletes past its own budget.
/// - `deleteRetentionMsBreachedSegments`: `now - largestTimestamp >
///   retention.ms`, with a horizon `now - retention.ms` that `i64` cannot
///   represent expiring nothing.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn expirable(
    max_timestamps: Seq<i64>,
    byte_lens: Seq<u64>,
    last_offsets: Seq<i64>,
    policy: DisklessRetentionPolicy,
    i: Int,
) -> bool {
    pearlite! {
        last_offsets[i]@ < policy.log_start_offset@
            || match policy.retention_bytes {
                Some(budget) => size_debt_before(byte_lens, budget, i) - byte_lens[i]@ >= 0,
                None => false,
            }
            || match policy.retention_ms {
                Some(retention) => i64::MIN@ <= policy.now_ms@ - retention@
                    && policy.now_ms@ - retention@ <= i64::MAX@
                    && max_timestamps[i]@ < policy.now_ms@ - retention@,
                None => false,
            }
    }
}

/// Select the oldest contiguous prefix of one diskless partition's committed
/// WAL index ranges that retention allows to expire.
///
/// Kafka runs three walks oldest first, each stopping at the first segment it
/// must keep: the log-start-offset walk, then the size walk over what is left,
/// then the time walk over what is left after that. The first two predicates
/// only ever hold on a prefix (the size one because sizes are nonnegative, the
/// floor one because the host supplies ranges in offset order), so the three
/// walks together expire exactly the longest prefix on which one of the three
/// holds at every range. That prefix is what this returns, capped one short of
/// the newest range; see `expirable`.
///
/// The ranges arrive oldest first, one entry per index range, and the three
/// slices are parallel.
///
/// The newest range never expires. Kafka keeps the active segment for the same
/// reason, and here it is also what keeps the flusher's `flushed_frontier`
/// pointing past the last flushed offset: an empty index would send the next
/// tick back to the local log start and re-upload a prefix the bucket holds.
#[requires(max_timestamps@.len() == byte_lens@.len())]
#[requires(max_timestamps@.len() == last_offsets@.len())]
#[ensures(result@ <= max_timestamps@.len())]
#[ensures(max_timestamps@.len() > 0 ==> result@ < max_timestamps@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@ ==>
    expirable(max_timestamps@, byte_lens@, last_offsets@, policy, i))]
#[ensures(result@ + 1 < max_timestamps@.len() ==>
    !expirable(max_timestamps@, byte_lens@, last_offsets@, policy, result@))]
#[must_use]
pub fn diskless_retention_prefix(
    max_timestamps: &[i64],
    byte_lens: &[u64],
    last_offsets: &[i64],
    policy: DisklessRetentionPolicy,
) -> usize {
    if matches!(max_timestamps.len(), 0) {
        return 0;
    }
    let max_expire = max_timestamps.len() - 1;
    let horizon = match policy.retention_ms {
        Some(retention) => policy.now_ms.checked_sub(retention),
        None => None,
    };
    // Kafka's `diff`, `None` once it is negative. A `u128` holds the sum of
    // any slice of `u64` lengths without overflow.
    let mut debt: Option<u128> = match policy.retention_bytes {
        Some(budget) => {
            let mut indexed = 0u128;
            let mut scanned = 0usize;
            #[invariant(scanned@ <= byte_lens@.len())]
            #[invariant(indexed@ == indexed_bytes(byte_lens@, scanned@))]
            #[invariant(indexed@ <= scanned@ * u64::MAX@)]
            #[variant(byte_lens@.len() - scanned@)]
            while scanned < byte_lens.len() {
                proof_assert!(indexed_bytes(byte_lens@, scanned@ + 1)
                    == indexed_bytes(byte_lens@, scanned@) + byte_lens@[scanned@]@);
                indexed += u128::from(byte_lens[scanned]);
                scanned += 1;
            }
            indexed.checked_sub(u128::from(budget))
        }
        None => None,
    };

    let mut len = 0usize;
    #[invariant(len@ <= max_expire@)]
    #[invariant(match policy.retention_bytes {
        Some(budget) => match debt {
            Some(debt) => debt@ == size_debt_before(byte_lens@, budget, len@),
            None => size_debt_before(byte_lens@, budget, len@) < 0,
        },
        None => debt == None,
    })]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==>
        expirable(max_timestamps@, byte_lens@, last_offsets@, policy, i))]
    #[variant(max_expire@ - len@)]
    while len < max_expire {
        proof_assert!(indexed_bytes(byte_lens@, len@ + 1)
            == indexed_bytes(byte_lens@, len@) + byte_lens@[len@]@);
        let below_floor = last_offsets[len] < policy.log_start_offset;
        let aged_out = match horizon {
            Some(horizon) => max_timestamps[len] < horizon,
            None => false,
        };
        let size = u128::from(byte_lens[len]);
        let over_budget = match debt {
            Some(debt) => debt >= size,
            None => false,
        };
        if !below_floor && !aged_out && !over_budget {
            break;
        }
        debt = match debt {
            Some(debt) => debt.checked_sub(size),
            None => None,
        };
        len += 1;
    }
    len
}

/// Permit object deletion only after the grace period and with no index
/// reference in the projection protected by the caller's cache lock.
#[ensures(result == (!referenced && grace_elapsed))]
#[must_use]
pub const fn diskless_object_reclaimable(referenced: bool, grace_elapsed: bool) -> bool {
    !referenced && grace_elapsed
}

/// Select the covering logical range, or the first successor after a gap.
#[requires(forall<i: Int> 0 <= i && i < entries@.len()
    ==> entries@[i].0@ <= entries@[i].1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].1@ < entries@[j].0@)]
#[ensures(match result {
    Some(index) => index@ < entries@.len()
        && ((entries@[index@].0@ <= requested@ && requested@ <= entries@[index@].1@)
            || (requested@ < entries@[index@].0@
                && index@ > 0
                && entries@[index@ - 1].1@ < requested@)),
    None => entries@.len() == 0
        || requested@ < entries@[0].0@
        || entries@[entries@.len() - 1].1@ < requested@,
})]
#[must_use]
pub fn diskless_logical_range(entries: &[(i64, i64)], requested: i64) -> Option<usize> {
    let lo = crate::log_index::upper_range_cursor(entries, requested);
    if lo == 0 {
        return None;
    }
    let floor = lo - 1;
    if requested <= entries[floor].1 {
        Some(floor)
    } else if lo < entries.len() {
        Some(lo)
    } else {
        None
    }
}
