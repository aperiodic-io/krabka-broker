use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(bases.len() == lasts.len())]
#[requires(0 <= index && index <= bases.len())]
#[variant(bases.len() - index)]
fn exact_wal_batch_suffix(
    bases: Seq<i64>,
    lasts: Seq<i64>,
    index: Int,
    expected: Int,
    target: Int,
) -> bool {
    pearlite! {
        if index == bases.len() {
            expected == target
        } else {
            bases[index]@ == expected
                && bases[index] <= lasts[index]
                && lasts[index] < i64::MAX
                && exact_wal_batch_suffix(
                    bases,
                    lasts,
                    index + 1,
                    lasts[index]@ + 1,
                    target,
                )
        }
    }
}

/// Check that decoded WAL batches cover exactly one contiguous half-open range.
#[cfg_attr(creusot, ensures(result == (bases@.len() == lasts@.len()
    && if start@ == target@ {
        bases@.len() == 0
    } else {
        exact_wal_batch_suffix(bases@, lasts@, 0, start@, target@)
    })))]
#[cfg_attr(creusot, ensures(result == wal_batch_layout(bases@, lasts@, start@, target@)))]
#[cfg_attr(creusot, ensures(result ==> forall<i: Int> 0 <= i && i < bases@.len()
    ==> start@ <= bases@[i]@ && bases@[i]@ <= lasts@[i]@ && lasts@[i]@ < target@))]
#[cfg_attr(creusot, ensures(result ==> forall<i: Int, j: Int>
    0 <= i && i < j && j < bases@.len() ==> lasts@[i]@ < bases@[j]@))]
#[must_use]
pub fn exact_wal_batch_range(bases: &[i64], lasts: &[i64], start: i64, target: i64) -> bool {
    if bases.len() != lasts.len() {
        return false;
    }
    if start == target {
        return matches!(bases.len(), 0);
    }

    let mut expected = start;
    let mut i = 0usize;
    #[cfg_attr(creusot, invariant(i@ <= bases@.len()))]
    #[cfg_attr(creusot, invariant(bases@.len() == lasts@.len()))]
    #[cfg_attr(creusot, invariant(exact_wal_batch_suffix(bases@, lasts@, 0, start@, target@)
        == exact_wal_batch_suffix(bases@, lasts@, i@, expected@, target@)))]
    #[cfg_attr(creusot, invariant(expected@ == if i@ == 0 { start@ } else { lasts@[i@ - 1]@ + 1 }))]
    #[cfg_attr(creusot, invariant(start@ <= expected@))]
    #[cfg_attr(creusot, invariant(forall<j: Int> 0 <= j && j < i@ ==>
        start@ <= bases@[j]@ && bases@[j]@ <= lasts@[j]@ && lasts@[j]@ < expected@
        && lasts@[j] < i64::MAX
        && bases@[j]@ == if j == 0 { start@ } else { lasts@[j - 1]@ + 1 }))]
    #[cfg_attr(creusot, invariant(forall<a: Int, b: Int>
        0 <= a && a < b && b < i@ ==> lasts@[a]@ < bases@[b]@))]
    #[cfg_attr(creusot, variant(bases@.len() - i@))]
    while i < bases.len() {
        if bases[i] != expected || bases[i] > lasts[i] {
            return false;
        }
        let Some(next) = lasts[i].checked_add(1) else {
            return false;
        };
        expected = next;
        i += 1;
    }
    expected == target
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
// cargo-mutants: #[cfg(creusot)] mathematical layout; not compiled at runtime.
pub fn wal_batch_layout(bases: Seq<i64>, lasts: Seq<i64>, start: Int, target: Int) -> bool {
    pearlite! {
        bases.len() == lasts.len() && if start == target {
            bases.len() == 0
        } else {
            (forall<i: Int> 0 <= i && i < bases.len() ==>
                bases[i] <= lasts[i] && lasts[i] < i64::MAX
                && bases[i]@ == if i == 0 { start } else { lasts[i - 1]@ + 1 })
            && target == if bases.len() == 0 { start } else { lasts[bases.len() - 1]@ + 1 }
        }
    }
}

/// Whole batches may cover an interior logical floor. They must still form
/// an exact physical prefix ending at `target`; the first batch contains `start`.
#[cfg_attr(creusot, ensures((result != None) == (
    0 <= start@ && start@ <= target@ && bases@.len() == lasts@.len()
    && if start == target { bases@.len() == 0 } else {
        bases@.len() > 0 && 0 <= bases@[0]@ && bases@[0]@ <= start@ && start@ <= lasts@[0]@
        && wal_batch_layout(bases@, lasts@, bases@[0]@, target@)
    }
)))]
#[cfg_attr(creusot, ensures(match result {
    None => true,
    Some(physical) => physical@ == (if bases@.len() == 0 { start@ } else { bases@[0]@ })
        && 0 <= physical@ && physical@ <= start@ && start@ <= target@
        && wal_batch_layout(bases@, lasts@, physical@, target@)
        && (forall<i: Int> 0 <= i && i < bases@.len() ==>
            physical@ <= bases@[i]@ && bases@[i]@ <= lasts@[i]@ && lasts@[i]@ < target@),
}))]
#[cfg_attr(creusot, ensures(match result {
    None => true,
    Some(_) => forall<i: Int, j: Int> 0 <= i && i < j && j < bases@.len() ==> lasts@[i]@ < bases@[j]@,
}))]
#[must_use]
pub fn wal_covering_batch_range(
    bases: &[i64],
    lasts: &[i64],
    start: i64,
    target: i64,
) -> Option<i64> {
    if start < 0 || start > target || bases.len() != lasts.len() {
        return None;
    }
    if start == target {
        return if matches!(bases.len(), 0) {
            Some(start)
        } else {
            None
        };
    }
    if matches!(bases.len(), 0) || bases[0] < 0 || bases[0] > start || lasts[0] < start {
        return None;
    }
    if !exact_wal_batch_range(bases, lasts, bases[0], target) {
        return None;
    }
    Some(bases[0])
}

/// A nonempty checkpoint must end at the observed whole batch's successor.
/// Its logical floor may lie inside a batch; an empty range is reset at that floor.
#[ensures(result == (0 <= recovered_start@ && recovered_start@ <= start@
    && start@ <= end@ && end@ <= recovered_end@
    && (start == end || (match observed_last { Some(last) => last@ == end@ - 1, None => false }))))]
#[must_use]
pub fn wal_checkpoint_range_valid(
    recovered_start: i64,
    recovered_end: i64,
    start: i64,
    end: i64,
    observed_last: Option<i64>,
) -> bool {
    0 <= recovered_start
        && recovered_start <= start
        && start <= end
        && end <= recovered_end
        && (start == end || observed_last == end.checked_sub(1))
}

/// Compare actual encoded batch bytes and their inclusive offset coordinates.
/// Matching lengths or frontiers alone do not establish the same records.
#[ensures(result == (left.0 == right.0 && left.1 == right.1 && left.2@ == right.2@))]
#[must_use]
pub fn wal_batch_equal(left: (i64, i64, &[u8]), right: (i64, i64, &[u8])) -> bool {
    if left.0 != right.0 || left.1 != right.1 || left.2.len() != right.2.len() {
        return false;
    }
    let mut i = 0usize;
    #[invariant(i@ <= left.2@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> left.2@[j] == right.2@[j])]
    #[variant(left.2@.len() - i@)]
    while i < left.2.len() {
        if left.2[i] != right.2[i] {
            return false;
        }
        i += 1;
    }
    true
}
