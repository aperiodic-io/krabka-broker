#[cfg(creusot)]
use crate::retention::{
    local_prefix_bytes, local_prefix_bytes_monotone, local_retention_limit, local_retention_model,
    remote_covers_offset, remote_ranges_valid,
};
use {
    crate::retention::{
        LocalRetentionSegment, local_retention_prefix, remote_covered_through,
        retention_delete_target,
    },
    creusot_std::prelude::*,
};

type CoveredRetention = (Option<i64>, Vec<LocalRetentionSegment>, usize, Option<i64>);

/// Derive whole-segment eligibility from actual remote intervals, consume the
/// local size/time prefix, and convert its inclusive endpoint to a delete target.
/// Every offset selected for eviction belongs to a supplied remote interval,
/// including local gaps; every prefix eligible by size or expiry is selected.
/// Every selected non-expired row fits the initial size debt.
/// Exact expiry/size facts, complete truthful copy metadata valid through
/// application and durable physical deletion remain external.
/// The active segment is protected.
#[requires(local@.len() < usize::MAX@)]
#[requires(forall<i: Int> 0 <= i && i < local@.len() ==> 0 <= local@[i].0@ && local@[i].0@ <= local@[i].1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < local@.len() ==> local@[i].1@ < local@[j].0@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < remote@.len() ==> remote@[i].0@ <= remote@[j].0@)]
#[ensures({
    let anchor = if local@.len() == 0 { 0 } else { local@[0].0@ };
    match result.0 {
        None => !remote_ranges_valid(remote@) || forall<j: Int> 0 <= j && j < remote@.len()
            ==> anchor < remote@[j].0@ || remote@[j].1@ < anchor,
        Some(through) => remote_ranges_valid(remote@) && anchor <= through@
            && (exists<j: Int> 0 <= j && j < remote@.len() && through == remote@[j].1)
            && (forall<offset: Int> anchor <= offset && offset <= through@
                ==> remote_covers_offset(remote@, offset))
            && forall<j: Int> 0 <= j && j < remote@.len() && remote@[j].0@ <= through@ + 1
                ==> remote@[j].1@ <= through@,
    }
})]
#[ensures(result.1@.len() == local@.len() + 1 && result.2@ <= local@.len())]
#[ensures(forall<i: Int> 0 <= i && i < local@.len()
    ==> result.1@[i].size == local@[i].2 && result.1@[i].expired == local@[i].3
        && result.1@[i].blocked == match result.0 { None => true, Some(through) => local@[i].1@ > through@ })]
#[ensures(result.1@[local@.len()].blocked && !result.1@[local@.len()].expired
    && result.1@[local@.len()].size == active_size)]
#[ensures(result.2@ == match size_debt {
    None => local_retention_model(result.1@, local_retention_limit(result.1@), 0, 0, false),
    Some(debt) => local_retention_model(result.1@, local_retention_limit(result.1@), 0, debt@, true),
})]
#[ensures(forall<n: Int> 0 <= n && n <= local@.len()
    && (forall<i: Int> 0 <= i && i < n ==> !result.1@[i].blocked
        && (local@[i].3 || match size_debt {
            None => false, Some(debt) => local_prefix_bytes(result.1@, i + 1) <= debt@,
        }))
    ==> n <= result.2@)]
#[ensures((forall<i: Int> 0 <= i && i < result.2@ ==> local@[i].3
    || match size_debt { None => false, Some(debt) => local_prefix_bytes(result.1@, i + 1) <= debt@ })
    && (forall<n: Int> 0 <= n && n <= local@.len()
        && (forall<i: Int> 0 <= i && i < n ==> !result.1@[i].blocked)
        && match size_debt { None => false, Some(debt) => local_prefix_bytes(result.1@, n) <= debt@ }
        ==> n <= result.2@))]
#[ensures(result.0 == None ==> result.2@ == 0 && result.3 == None)]
#[ensures(match result.3 {
    None => result.2@ == 0 || local@[result.2@ - 1].1@ == i64::MAX@,
    Some(target) => result.2@ > 0 && local@[result.2@ - 1].1@ < i64::MAX@
        && target@ == local@[result.2@ - 1].1@ + 1 && local@[0].0@ < target@
        && (exists<through: i64> result.0 == Some(through) && target@ <= through@ + 1)
        && forall<offset: Int> local@[0].0@ <= offset && offset < target@
            ==> remote_covers_offset(remote@, offset),
})]
#[ensures(forall<i: Int, offset: Int> 0 <= i && i < result.2@
    && local@[i].0@ <= offset && offset <= local@[i].1@
    ==> remote_covers_offset(remote@, offset))]
pub(super) fn remote_coverage_bounds_local_retention(
    remote: &[(i64, i64)],
    local: &[(i64, i64, u64, bool)], // start, inclusive end, bytes, expired
    size_debt: Option<u64>,
    active_size: u64,
) -> CoveredRetention {
    let anchor = match local.len() {
        0 => 0,
        _ => local[0].0,
    };
    proof_assert!(anchor@ == if local@.len() == 0 { 0 } else { local@[0].0@ });
    let covered = remote_covered_through(remote, anchor);
    let mut facts: Vec<LocalRetentionSegment> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= local@.len() && facts@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> facts@[j].size == local@[j].2
        && facts@[j].expired == local@[j].3
        && facts@[j].blocked == match covered { None => true, Some(through) => local@[j].1@ > through@ })]
    #[variant(local@.len() - i@)]
    while i < local.len() {
        facts.push(LocalRetentionSegment {
            blocked: !matches!(covered, Some(through) if local[i].1 <= through),
            expired: local[i].3,
            size: local[i].2,
        });
        i += 1;
    }
    facts.push(LocalRetentionSegment {
        blocked: true,
        expired: false,
        size: active_size,
    });
    proof_assert!(forall<j: Int> 0 <= j && j < local@.len()
    ==> facts@[j].blocked == match covered {
        None => true, Some(through) => local@[j].1@ > through@,
    });
    let count = local_retention_prefix(&facts, size_debt);
    proof_assert!(forall<j: Int> 0 <= j && j < count@ ==> !facts@[j].blocked);
    proof_assert!(count@ <= local@.len());
    proof_assert!(count@ < local@.len() ==> facts@[count@].blocked || !facts@[count@].expired);
    proof_assert!({
        local_prefix_bytes_monotone(facts@, facts@.len());
        forall<n: Int> 0 <= n && n <= local@.len()
            && (forall<j: Int> 0 <= j && j < n ==> !facts@[j].blocked)
            && match size_debt { None => false, Some(debt) => local_prefix_bytes(facts@, n) <= debt@ }
            ==> n <= count@
    });
    proof_assert!(match covered {
        None => count@ == 0,
        Some(through) => forall<j: Int> 0 <= j && j < count@ ==> local@[j].1@ <= through@,
    });
    let last = if count == 0 {
        None
    } else {
        Some(local[count - 1].1)
    };
    let target = retention_delete_target(last);
    (covered, facts, count, target)
}
