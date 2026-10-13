//! Arithmetic over a role's task map, the `BTreeMap` from `subtopology_id` to
//! a sorted, deduped partition list.
//!
//! The function here is pure and total. The state machine calls it to
//! normalize an assignment.

use std::collections::BTreeMap;

/// Sorts and dedups every subtopology's partition list, then drops the
/// subtopology entries that end up empty. The function is idempotent.
pub(super) fn normalize_task_map(
    mut map: BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<i32>> {
    map.retain(|_, parts| {
        parts.sort_unstable();
        parts.dedup();
        !parts.is_empty()
    });
    map
}

#[cfg(test)]
mod tests {
    use {
        super::{super::test_support::task_map, *},
        assert2::assert,
    };

    #[test]
    fn normalize_sorts_dedups_and_drops_empty() {
        let m = normalize_task_map(task_map(&[("sub0", &[2, 0, 1, 1]), ("sub1", &[])]));
        assert!(m == task_map(&[("sub0", &[0, 1, 2])]));
    }
}
