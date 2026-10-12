use creusot_std::prelude::*;

#[cfg(creusot)]
use crate::retention::remote_prefix_charge;
use crate::retention::{
    RemoteRetentionSegment, remote_retention_floor_step, remote_retention_prefix,
};

type RemoteDeletion = (Vec<RemoteRetentionSegment>, usize, usize, i64);

open_logic! {
/// Every whole range in the prefix reaches the initial floor or an earlier
/// range. This geometric predicate does not run the floor-update fold.
fn deletion_prefix_connected(rows: Seq<(i64, i64, u64, bool)>, initial: i64, count: Int) -> bool {
    pearlite! { forall<j: Int> 0 <= j && j < count ==> rows[j].1@ < i64::MAX@
    && (rows[j].0@ <= initial@ || exists<k: Int> 0 <= k && k < j && rows[j].0@ <= rows[k].1@ + 1) }
}
}

open_logic! {
/// Membership in the actual completed prefix, used as a quantifier trigger.
fn deleted_prefix_covers(rows: Seq<(i64, i64, u64, bool)>, count: Int, offset: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < count && rows[j].0@ <= offset && offset <= rows[j].1@ }
}
}

/// Appending one range preserves every offset covered by the older prefix.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count < rows.len())]
#[ensures(forall<offset: Int> deleted_prefix_covers(rows, count, offset)
    ==> deleted_prefix_covers(rows, count + 1, offset))]
fn lemma_deleted_prefix_covers_extend(rows: Seq<(i64, i64, u64, bool)>, count: Int) {}

/// Construct retention facts from actual ranges, select by all three policies,
/// then consume completed delete outcomes up to the first failure. Every offset
/// crossed by the resulting floor belongs to a successfully deleted range.
/// Connected representable prefixes make progress even across obsolete/overlapping
/// rows; a gap or unrepresentable successor cannot restart floor advancement.
/// Successful outcomes mean complete delete lifecycles. Exact time flags/debt,
/// truthful metadata, real object deletion and floor publication are external.
#[requires(current@ >= 0)]
#[requires(match deleted_below { None => true, Some(floor) => 0 <= floor@ && floor@ <= current@ })]
#[requires(super::finished_ranges_valid(finished@.len(), |i: Int| (finished@[i].0, finished@[i].1)))]
#[ensures((result.0@.len() == finished@.len() && result.1@ <= finished@.len())
    && (forall<i: Int> 0 <= i && i < finished@.len() ==> result.0@[i].size == finished@[i].2
    && result.0@[i].time_expired == finished@[i].3
    && result.0@[i].log_start_breached == match deleted_below { None => false, Some(floor) => finished@[i].1@ < floor@ }))]
#[ensures((deletion_prefix_requested(result.0@, finished@, size_debt@, result.1@))
    && (deletes_allowed && result.1@ < finished@.len() ==> !result.0@[result.1@].log_start_breached
    && !finished@[result.1@].3 && (size_debt@ == 0 || remote_prefix_charge(result.0@, result.1@) >= size_debt@
        || remote_prefix_charge(result.0@, result.1@ + 1) > size_debt@))
    && (forall<n: Int> deletes_allowed && 0 <= n && n <= finished@.len()
    && (deletion_prefix_requested(result.0@, finished@, size_debt@, n)) ==> n <= result.1@))]
#[ensures((!deletes_allowed ==> result.1@ == 0))]
#[ensures((result.2@ <= result.1@ && result.2@ <= completed@.len())
    && (forall<i: Int> 0 <= i && i < result.2@ ==> completed@[i])
    && (result.2@ < result.1@ && result.2@ < completed@.len() ==> !completed@[result.2@]))]
#[ensures((current@ <= result.3@ && (result.2@ == 0 ==> result.3 == current))
    && (forall<offset: Int> current@ <= offset && offset < result.3@
    ==> deleted_prefix_covers(finished@, result.2@, offset))
    && (forall<n: Int> 0 <= n && n <= result.2@ && deletion_prefix_connected(finished@, current, n)
    ==> forall<i: Int> 0 <= i && i < n ==> finished@[i].1@ < result.3@))]
pub(super) fn completed_remote_retention_bounds_floor(
    current: i64,
    deleted_below: Option<i64>,
    deletes_allowed: bool,
    finished: &[(i64, i64, u64, bool)], // start, inclusive end, bytes, time-expired
    size_debt: u64,
    completed: &[bool],
) -> RemoteDeletion {
    let mut facts: Vec<RemoteRetentionSegment> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= finished@.len() && facts@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> !facts@[j].blocked && facts@[j].size == finished@[j].2
        && facts@[j].time_expired == finished@[j].3 && facts@[j].log_start_breached == match deleted_below {
            None => false, Some(floor) => finished@[j].1@ < floor@,
        })]
    #[variant(finished@.len() - i@)]
    while i < finished.len() {
        facts.push(RemoteRetentionSegment {
            blocked: false,
            log_start_breached: matches!(deleted_below, Some(floor) if finished[i].1 < floor),
            time_expired: finished[i].3,
            size: finished[i].2,
        });
        i += 1;
    }
    let planned = remote_retention_prefix(deletes_allowed, &facts, size_debt);
    let mut floor = current;
    let mut contiguous = true;
    i = 0;
    #[invariant(i@ <= planned@ && planned@ <= finished@.len() && i@ <= completed@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> completed@[j])]
    #[invariant(current@ <= floor@ && (i@ == 0 ==> floor == current))]
    #[invariant(forall<offset: Int> current@ <= offset && offset < floor@
        ==> deleted_prefix_covers(finished@, i@, offset))]
    #[invariant(contiguous ==> forall<j: Int> 0 <= j && j < i@ ==> finished@[j].1@ < floor@)]
    #[invariant(!contiguous ==> !deletion_prefix_connected(finished@, current, i@))]
    #[invariant(forall<n: Int> 0 <= n && n <= i@ && deletion_prefix_connected(finished@, current, n)
        ==> forall<j: Int> 0 <= j && j < n ==> finished@[j].1@ < floor@)]
    #[variant(planned@ - i@)]
    while i < planned && i < completed.len() {
        if !completed[i] {
            break;
        }
        #[cfg(creusot)]
        let previous = floor;
        (floor, contiguous) =
            remote_retention_floor_step(floor, contiguous, finished[i].0, finished[i].1);
        proof_assert!(forall<offset: Int> previous@ <= offset && offset < floor@
            ==> deleted_prefix_covers(finished@, i@ + 1, offset));
        proof_assert!({
            lemma_deleted_prefix_covers_extend(finished@, i@);
            forall<offset: Int> current@ <= offset && offset < floor@
                ==> deleted_prefix_covers(finished@, i@ + 1, offset)
        });
        i += 1;
    }
    (facts, planned, i, floor)
}

open_logic! {
/// Every selected whole range is requested by at least one retention policy.
fn deletion_prefix_requested(
    facts: Seq<RemoteRetentionSegment>,
    finished: Seq<(i64, i64, u64, bool)>,
    debt: Int,
    count: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count ==> facts[i].log_start_breached || finished[i].3
    || (crate::retention::remote_charge_funded(debt, remote_prefix_charge(facts, i), remote_prefix_charge(facts, i + 1))) }
}
}
