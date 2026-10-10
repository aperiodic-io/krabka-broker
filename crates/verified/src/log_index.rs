//! Sparse-index lookup kernels.
//!
//! These kernels come out of the `OffsetIndex` and `TimeIndex` of `krabka-log`,
//! so that Creusot can verify them. They use hand-rolled binary searches, the
//! canonical Creusot loop, and not `binary_search_by_key`. The proofs thus do
//! not depend on a model of the std search.

use creusot_std::prelude::*;

// A sorted first coordinate determines both sparse-index and logical-range cursors.
macro_rules! sorted_first_cursor {
    ($name:ident, $key:ty, $value:ty, $before:tt) => {
        #[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
            ==> entries@[i].0@ <= entries@[j].0@)]
        #[ensures(result@ <= entries@.len())]
        #[ensures(forall<i: Int> 0 <= i && i < result@ ==> entries@[i].0@ $before target@)]
        #[ensures(forall<i: Int> result@ <= i && i < entries@.len()
            ==> !(entries@[i].0@ $before target@))]
        #[must_use]
        #[inline]
        pub(crate) fn $name(entries: &[($key, $value)], target: $key) -> usize {
            let mut lo = 0usize;
            let mut hi = entries.len();
            #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
            #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ $before target@)]
            #[invariant(forall<i: Int> hi@ <= i && i < entries@.len()
                ==> !(entries@[i].0@ $before target@))]
            #[variant(hi - lo)]
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if entries[mid].0 $before target {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        }
    };
}

sorted_first_cursor!(upper_offset_cursor, u32, u32, <=);
sorted_first_cursor!(upper_time_cursor, i64, u32, <=);
sorted_first_cursor!(upper_range_cursor, i64, i64, <=);
sorted_first_cursor!(lower_offset_cursor, u32, u32, <);

macro_rules! floor_lookup {
    ($(#[$doc:meta])* $name:ident, $key:ty, $order:tt, $cursor:ident) => {
                $(#[$doc])*
        #[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
            ==> entries@[i].0@ $order entries@[j].0@)]
        #[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ <= target@)
            ==> exists<i: Int> 0 <= i && i < entries@.len()
                && entries@[i].0@ <= target@ && result@ == entries@[i].1@
                && (forall<j: Int> i < j && j < entries@.len() ==> entries@[j].0@ > target@))]
        #[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ > target@) ==> result@ == 0)]
        #[must_use]
        pub fn $name(entries: &[($key, u32)], target: $key) -> u32 {
            let lo = $cursor(entries, target);
            if lo == 0 { 0 } else { entries[lo - 1].1 }
        }
    };
}

floor_lookup! {
#[doc = "The byte position to start reading at for `target`."]
#[doc = ""]
#[doc = "This is the position field of the largest entry with"]
#[doc = "`relative_offset <= target`, or 0 if no such entry exists. `entries` must be"]
#[doc = "strictly sorted by relative offset, which the construction of `OffsetIndex`"]
#[doc = "guarantees."]
offset_index_lookup, u32, <, upper_offset_cursor
}

floor_lookup! {
#[doc = "The relative offset to start reading at for `target_timestamp`."]
#[doc = ""]
#[doc = "This is the offset field of the last entry with"]
#[doc = "`timestamp <= target_timestamp`, or 0 if no such entry exists. `entries`"]
#[doc = "must be sorted by timestamp; equal timestamps are allowed."]
time_index_lookup, i64, <=, upper_time_cursor
}

/// A safe sparse starting offset for a forward scan seeking `timestamp >= target`.
/// Entries carry running maxima. Starting at an equal maximum could skip its
/// first occurrence, so only a strictly smaller maximum may advance the scan.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ <= entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ < target_timestamp@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ < target_timestamp@
        && result@ == entries@[i].1@
        && (forall<j: Int> i < j && j < entries@.len()
            ==> entries@[j].0@ >= target_timestamp@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ >= target_timestamp@)
    ==> result@ == 0)]
#[must_use]
pub fn time_index_scan_start(entries: &[(i64, u32)], target_timestamp: i64) -> u32 {
    match target_timestamp.checked_sub(1) {
        Some(before) => time_index_lookup(entries, before),
        None => 0,
    }
}

/// The byte position of the first entry at or after `target`.
///
/// Returns `None` when every entry is below `target`. `entries` must be
/// strictly sorted by relative offset.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && target@ <= entries@[i].0@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && target@ <= entries@[i].0@
        && result == Some(entries@[i].1)
        && (forall<j: Int> 0 <= j && j < i ==> entries@[j].0@ < target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ < target@)
    ==> result == None)]
#[must_use]
pub fn offset_index_position_at_or_after(entries: &[(u32, u32)], target: u32) -> Option<u32> {
    let lo = lower_offset_cursor(entries, target);
    if lo == entries.len() {
        None
    } else {
        Some(entries[lo].1)
    }
}

#[cfg(test)]
mod tests {

    use proptest::prelude::*;

    use super::*;

    fn offset_lookup_cases() -> impl Strategy<Value = (std::collections::BTreeSet<u32>, u32)> {
        (
            proptest::collection::btree_set(0u32..10_000, 0..64),
            0u32..10_000,
        )
    }

    fn offset_entries(rels: &std::collections::BTreeSet<u32>) -> Vec<(u32, u32)> {
        rels.iter()
            .enumerate()
            .map(|(i, rel)| {
                (
                    *rel,
                    u32::try_from(i).expect("btree set length is bounded to 64") * 17,
                )
            })
            .collect()
    }

    fn offset_floor_oracle(entries: &[(u32, u32)], target: u32) -> u32 {
        match entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => entries[i].1,
            Err(0) => 0,
            Err(i) => entries[i - 1].1,
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_binary_search_oracle(
            (rels, target) in offset_lookup_cases(),
        ) {
            // btree_set gives strictly-sorted unique keys, matching the
            // OffsetIndex construction invariant.
            let entries = offset_entries(&rels);
            prop_assert_eq!(offset_index_lookup(&entries, target), offset_floor_oracle(&entries, target));
        }

        #[test]
        fn time_lookup_matches_linear_oracle(
            timestamps in proptest::collection::vec(-10_000i64..10_000, 0..64),
            target in -10_000i64..10_000,
        ) {
            let mut timestamps = timestamps;
            timestamps.sort_unstable();
            let entries: Vec<(i64, u32)> = timestamps
                .iter()
                .enumerate()
                .map(|(i, timestamp)| {
                    (
                        *timestamp,
                        u32::try_from(i).expect("timestamp list length is bounded to 64") * 17,
                    )
                })
                .collect();
            let want = entries
                .iter()
                .rfind(|&&(timestamp, _)| timestamp <= target)
                .map_or(0, |&(_, offset)| offset);
            prop_assert_eq!(time_index_lookup(&entries, target), want);
        }

        #[test]
        fn position_at_or_after_matches_linear_oracle(
            (rels, target) in offset_lookup_cases(),
        ) {
            let entries = offset_entries(&rels);
            let want = entries
                .iter()
                .find(|&&(rel, _)| rel >= target)
                .map(|&(_, position)| position);
            prop_assert_eq!(offset_index_position_at_or_after(&entries, target), want);
        }
    }

    #[test]
    fn empty_indexes_return_fallbacks() {
        assert2::assert!(offset_index_lookup(&[], 42) == 0);
        assert2::assert!(time_index_lookup(&[], 42) == 0);
        assert2::assert!(offset_index_position_at_or_after(&[], 42) == None);
    }

    #[test]
    fn position_at_or_after_exact_match() {
        let entries = [(10, 100), (20, 200), (30, 300)];
        assert2::assert!(offset_index_position_at_or_after(&entries, 20) == Some(200));
    }
}
