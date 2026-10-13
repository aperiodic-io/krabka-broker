//! Sparse-index lookup kernels.
//!
//! These kernels come out of the `OffsetIndex` and `TimeIndex` of `krabka-log`,
//! so that Creusot can verify them. They use hand-rolled binary searches, the
//! canonical Creusot loop, and not `binary_search_by_key`. The proofs thus do
//! not depend on a model of the std search.

use creusot_std::prelude::*;

/// A decoded sparse-index key and the coordinate it selects.
#[krabka_macros::model_type(runtime(Clone, Copy, Debug, PartialEq, Eq, Hash))]
pub struct IndexEntry<Key, Coordinate> {
    pub key: Key,
    pub coordinate: Coordinate,
}

impl<Key, Coordinate> IndexEntry<Key, Coordinate> {
    /// Construct a decoded entry without changing its encoded field order.
    #[ensures(result.key == key && result.coordinate == coordinate)]
    #[must_use]
    pub const fn new(key: Key, coordinate: Coordinate) -> Self { Self { key, coordinate } }
}

pub type OffsetEntry = IndexEntry<u32, u32>;
pub type TimeEntry = IndexEntry<i64, u32>;
pub(crate) type RangeEntry = IndexEntry<i64, i64>;

krabka_macros::sorted_index_cursor! {
    pub(crate) fn upper_offset_cursor(entries: &[OffsetEntry], target: u32) -> usize;
    <=;
}
krabka_macros::sorted_index_cursor! {
    pub(crate) fn upper_time_cursor(entries: &[TimeEntry], target: i64) -> usize;
    <=;
}
krabka_macros::sorted_index_cursor! {
    pub(crate) fn upper_range_cursor(entries: &[RangeEntry], target: i64) -> usize;
    <=;
}
krabka_macros::sorted_index_cursor! {
    pub(crate) fn lower_offset_cursor(entries: &[OffsetEntry], target: u32) -> usize;
    <;
}

krabka_macros::index_floor_lookup! {
    /// The position of the last entry at or below `target`, or zero.
    /// Entries must be strictly sorted by relative offset.
    pub fn offset_index_lookup(entries: &[OffsetEntry], target: u32) -> u32;
    <; upper_offset_cursor;
}
krabka_macros::index_floor_lookup! {
    /// The relative offset of the last entry at or below `target`, or zero.
    /// Timestamps must be nondecreasing; equal timestamps are allowed.
    pub fn time_index_lookup(entries: &[TimeEntry], target: i64) -> u32;
    <=; upper_time_cursor;
}

/// A safe sparse starting offset for a forward scan seeking `timestamp >= target`.
/// Entries carry running maxima. Starting at an equal maximum could skip its
/// first occurrence, so only a strictly smaller maximum may advance the scan.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].key@ <= entries@[j].key@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].key@ < target_timestamp@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].key@ < target_timestamp@
        && result@ == entries@[i].coordinate@
        && (forall<j: Int> i < j && j < entries@.len()
            ==> entries@[j].key@ >= target_timestamp@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].key@ >= target_timestamp@)
    ==> result@ == 0)]
#[must_use]
pub fn time_index_scan_start(entries: &[TimeEntry], target_timestamp: i64) -> u32 {
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
    ==> entries@[i].key@ < entries@[j].key@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && target@ <= entries@[i].key@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && target@ <= entries@[i].key@
        && result == Some(entries@[i].coordinate)
        && (forall<j: Int> 0 <= j && j < i ==> entries@[j].key@ < target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].key@ < target@)
    ==> result == None)]
#[must_use]
pub fn offset_index_position_at_or_after(entries: &[OffsetEntry], target: u32) -> Option<u32> {
    let lo = lower_offset_cursor(entries, target);
    if lo == entries.len() {
        None
    } else {
        Some(entries[lo].coordinate)
    }
}

#[cfg(test)]
mod tests {

    use {super::*, proptest::prelude::*};

    fn offset_lookup_cases() -> impl Strategy<Value = (std::collections::BTreeSet<u32>, u32)> {
        (
            proptest::collection::btree_set(0u32..10_000, 0..64),
            0u32..10_000,
        )
    }

    fn offset_entries(rels: &std::collections::BTreeSet<u32>) -> Vec<OffsetEntry> {
        rels.iter()
            .enumerate()
            .map(|(i, rel)| {
                IndexEntry::new(
                    *rel,
                    u32::try_from(i).expect("btree set length is bounded to 64") * 17,
                )
            })
            .collect()
    }

    fn offset_floor_oracle(entries: &[OffsetEntry], target: u32) -> u32 {
        match entries.binary_search_by_key(&target, |entry| entry.key) {
            Ok(i) => entries[i].coordinate,
            Err(0) => 0,
            Err(i) => entries[i - 1].coordinate,
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
            let entries: Vec<TimeEntry> = timestamps
                .iter()
                .enumerate()
                .map(|(i, timestamp)| {
                    IndexEntry::new(*timestamp, u32::try_from(i).expect("timestamp list length is bounded to 64") * 17)
                })
                .collect();
            let want = entries
                .iter()
                .rfind(|entry| entry.key <= target)
                .map_or(0, |entry| entry.coordinate);
            prop_assert_eq!(time_index_lookup(&entries, target), want);
        }

        #[test]
        fn position_at_or_after_matches_linear_oracle(
            (rels, target) in offset_lookup_cases(),
        ) {
            let entries = offset_entries(&rels);
            let want = entries
                .iter()
                .find(|entry| entry.key >= target)
                .map(|entry| entry.coordinate);
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
        let entries = [
            IndexEntry::new(10, 100),
            IndexEntry::new(20, 200),
            IndexEntry::new(30, 300),
        ];
        assert2::assert!(offset_index_position_at_or_after(&entries, 20) == Some(200));
    }
}
