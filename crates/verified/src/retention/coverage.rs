use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn remote_ranges_valid(ranges: Seq<(i64, i64)>) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < ranges.len() ==> ranges[i].0@ <= ranges[i].1@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn remote_covers_offset(ranges: Seq<(i64, i64)>, offset: Int) -> bool {
    pearlite! { exists<i: Int> 0 <= i && i < ranges.len() && ranges[i].0@ <= offset && offset <= ranges[i].1@ }
}

/// Find the greatest contiguous inclusive coverage rooted at `local_start`.
/// Sorted ranges may overlap or precede the anchor; obsolete disconnected
/// prefixes cannot suppress a later anchored component. Reject malformed ranges
/// anywhere in the listing. Metadata truth, copy completion and bytes are external.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ranges@.len()
    ==> ranges@[i].0@ <= ranges@[j].0@)]
#[ensures(match result {
    None => !remote_ranges_valid(ranges@)
        || forall<i: Int> 0 <= i && i < ranges@.len()
            ==> local_start@ < ranges@[i].0@ || ranges@[i].1@ < local_start@,
    Some(through) => remote_ranges_valid(ranges@) && local_start@ <= through@
        && (exists<i: Int> 0 <= i && i < ranges@.len() && through == ranges@[i].1)
        && (forall<offset: Int> local_start@ <= offset && offset <= through@
            ==> remote_covers_offset(ranges@, offset))
        && forall<i: Int> 0 <= i && i < ranges@.len() && ranges@[i].0@ <= through@ + 1
            ==> ranges@[i].1@ <= through@,
})]
#[must_use]
pub fn remote_covered_through(ranges: &[(i64, i64)], local_start: i64) -> Option<i64> {
    let mut i = 0usize;
    #[invariant(i@ <= ranges@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> ranges@[j].0@ <= ranges@[j].1@)]
    #[variant(ranges@.len() - i@)]
    while i < ranges.len() {
        if ranges[i].0 > ranges[i].1 {
            return None;
        }
        i += 1;
    }
    let mut covered: Option<i64> = None;
    i = 0;
    #[invariant(i@ <= ranges@.len())]
    #[invariant(match covered {
        None => forall<j: Int> 0 <= j && j < i@ ==> local_start@ < ranges@[j].0@ || ranges@[j].1@ < local_start@,
        Some(through) => local_start@ <= through@
            && (exists<j: Int> 0 <= j && j < i@ && through == ranges@[j].1)
            && (forall<j: Int> 0 <= j && j < i@ ==> ranges@[j].1@ <= through@)
            && forall<offset: Int> local_start@ <= offset && offset <= through@
                ==> remote_covers_offset(ranges@, offset),
    })]
    #[variant(ranges@.len() - i@)]
    while i < ranges.len() {
        let (start, end) = ranges[i];
        match covered {
            None if start <= local_start && local_start <= end => covered = Some(end),
            None => {}
            Some(through) if start <= through.saturating_add(1) => {
                if end > through {
                    covered = Some(end);
                }
            }
            Some(_through) => {
                proof_assert!(forall<j: Int> i@ <= j && j < ranges@.len() ==> ranges@[j].0@ > _through@ + 1);
                return covered;
            }
        }
        i += 1;
    }
    covered
}

#[cfg(test)]
mod tests;
