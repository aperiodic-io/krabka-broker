#[cfg(creusot)]
use super::{RemoteRetentionSegment, local_prefix_bytes};
use {super::LocalRetentionSegment, creusot_std::prelude::*};

/// Decide whether one held barrier cut falls outside the retained epoch
/// window.
///
/// A nonpositive retention count and an unrepresentable cutoff both fail
/// closed by expiring nothing.
#[ensures(result == (retained_cuts@ > 0
    && published_epoch@ - retained_cuts@ >= i64::MIN@
    && held_epoch@ <= published_epoch@ - retained_cuts@))]
#[must_use]
pub fn barrier_cut_expired(published_epoch: i64, retained_cuts: i32, held_epoch: i64) -> bool {
    if retained_cuts <= 0 {
        return false;
    }
    let retained_cuts = i64::from(retained_cuts);
    if published_epoch < i64::MIN + retained_cuts {
        return false;
    }
    held_epoch <= published_epoch - retained_cuts
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// How many segments the local walk may consider: every one of them, except
/// a newest segment that is empty. That is Kafka's `deletableSegments`, which
/// never returns a last segment of size zero (`isLastSegmentAndEmpty`), so a
/// log never loses the segment it appends to without a record having left
/// with it.
pub fn local_retention_limit(segments: Seq<LocalRetentionSegment>) -> Int {
    pearlite! {
        if segments.len() > 0 && segments[segments.len() - 1].size@ == 0 {
            segments.len() - 1
        } else {
            segments.len()
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The reference rule for local retention, from segment `i` onwards.
///
/// Kafka's `UnifiedLog.deleteOldSegments` runs three passes over the local
/// log in this order: the log-start-offset breach, then `retention.bytes`,
/// then `retention.ms`. Each pass deletes an oldest prefix and stops at the
/// first segment its predicate rejects. The size pass
/// (`deleteRetentionSizeBreachedSegments`) deletes a segment only while
/// `diff - segment.size() >= 0`, and it runs only when the log is at least
/// its budget.
///
/// While `sizing` holds, the fold is in the size pass with `debt` bytes left
/// to reclaim. A segment that does not fit ends the size pass, and the
/// fold continues through the time pass. The time flag also carries the
/// log-start breach. That breach covers an oldest prefix, so handling it
/// inside the time pass deletes the same segments as Kafka's separate first
/// pass. A breached segment that fits the size debt is charged against it,
/// exactly as Kafka's size pass sees the log after the breach pass. A
/// breached segment that does not fit leaves Kafka's recomputed debt
/// negative, and then Kafka runs no size pass either.
///
/// The fold stops at a blocked segment and at `limit`.
#[variant(limit - i)]
pub fn local_retention_model(
    segments: Seq<LocalRetentionSegment>,
    limit: Int,
    i: Int,
    debt: Int,
    sizing: bool,
) -> Int {
    pearlite! {
        if i >= limit || i >= segments.len() || segments[i].blocked {
            i
        } else if sizing && segments[i].size@ <= debt {
            local_retention_model(segments, limit, i + 1, debt - segments[i].size@, true)
        } else if segments[i].expired {
            local_retention_model(segments, limit, i + 1, debt, false)
        } else {
            i
        }
    }
}

/// Count the oldest local segments that Kafka's local retention passes
/// delete.
///
/// `segments` is every local segment, oldest first, and the newest one is
/// the segment the log appends to: Kafka's `deletableSegments` walks the
/// active segment too. A result that covers every segment tells the host to
/// roll first and then delete them all, the way Kafka's `deleteSegments`
/// does (`if (numberOfSegments == numToDelete) roll()`); the walk never
/// takes a newest segment that is empty, so the roll always leaves a fresh
/// empty segment behind and never has to recreate the one it deletes.
///
/// `size_debt` is `None` when Kafka skips the size pass: `retention.bytes`
/// is unset, the policy does not delete, or the log is smaller than the
/// budget. Otherwise it is the log size minus the budget. The walk never
/// passes a blocked segment.
///
/// The result equals `local_retention_model` from the oldest segment,
/// limited by `local_retention_limit`. That fold states the Kafka rule.
/// Before that limit, the first kept segment is blocked or not expired.
/// Every selected non-expired segment fits the initial size debt together with
/// every older segment. Before the limit, an unblocked kept segment exceeds
/// that debt. These independent cost bounds reject a selector/reference pair
/// that omits the size pass or deletes beyond the budget.
#[ensures(result@ == match size_debt {
    None => local_retention_model(
        segments@, local_retention_limit(segments@), 0, 0, false),
    Some(debt) => local_retention_model(
        segments@, local_retention_limit(segments@), 0, debt@, true),
})]
#[ensures(result@ <= segments@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@ ==> !segments@[i].blocked)]
#[ensures(segments@.len() > 0 && segments@[segments@.len() - 1].size@ == 0
    ==> result@ < segments@.len())]
#[ensures(result@ < local_retention_limit(segments@)
    ==> segments@[result@].blocked || !segments@[result@].expired)]
#[ensures(forall<i: Int> 0 <= i && i < result@ ==> segments@[i].expired
    || match size_debt { None => false, Some(debt) => local_prefix_bytes(segments@, i + 1) <= debt@ })]
#[ensures(result@ < local_retention_limit(segments@) && !segments@[result@].blocked
    ==> match size_debt { None => true, Some(debt) => local_prefix_bytes(segments@, result@ + 1) > debt@ })]
#[must_use]
pub fn local_retention_prefix(segments: &[LocalRetentionSegment], size_debt: Option<u64>) -> usize {
    let limit = match segments.len().checked_sub(1) {
        Some(newest) if segments[newest].size == 0 => newest,
        _ => segments.len(),
    };
    let (mut debt, mut sizing) = match size_debt {
        Some(debt) => (debt, true),
        None => (0, false),
    };
    let mut len = 0usize;
    #[invariant(len@ <= limit@)]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==> !segments@[i].blocked)]
    #[invariant(local_prefix_bytes(segments@, len@) >= 0)]
    #[invariant(match size_debt {
        None => !sizing,
        Some(initial) => if sizing {
            local_prefix_bytes(segments@, len@) + debt@ == initial@
        } else { local_prefix_bytes(segments@, len@) > initial@ },
    })]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==> segments@[i].expired
        || match size_debt { None => false, Some(initial) => local_prefix_bytes(segments@, i + 1) <= initial@ })]
    #[invariant(limit@ == local_retention_limit(segments@))]
    #[invariant(local_retention_model(segments@, limit@, len@, debt@, sizing) == match size_debt {
        None => local_retention_model(segments@, limit@, 0, 0, false),
        Some(initial) => local_retention_model(segments@, limit@, 0, initial@, true),
    })]
    #[variant(limit@ - len@)]
    while len < limit {
        let segment = segments[len];
        if segment.blocked {
            break;
        }
        if sizing && segment.size <= debt {
            debt -= segment.size;
        } else if segment.expired {
            sizing = false;
        } else {
            break;
        }
        len += 1;
    }
    len
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The reference rule for remote retention, from segment `i` onwards, with
/// `debt` bytes of `retention.bytes` breach left to reclaim.
///
/// This is Kafka's `RemoteLogManager.cleanupExpiredRemoteLogSegments`, which
/// asks `RemoteLogRetentionHandler` about each segment in turn and stops at
/// the first one it keeps. A log-start breach deletes the segment and leaves
/// the debt alone. A `retention.ms` breach deletes it and lowers the debt by
/// its size, but not below zero. Otherwise the segment goes only when the
/// debt is positive and still covers the whole segment
/// (`isSegmentBreachedByRetentionSize`).
#[variant(segments.len() - i)]
pub fn remote_retention_model(segments: Seq<RemoteRetentionSegment>, i: Int, debt: Int) -> Int {
    pearlite! {
        if i >= segments.len() || segments[i].blocked {
            i
        } else if segments[i].log_start_breached {
            remote_retention_model(segments, i + 1, debt)
        } else if segments[i].time_expired {
            remote_retention_model(
                segments,
                i + 1,
                if segments[i].size@ <= debt { debt - segments[i].size@ } else { 0 },
            )
        } else if debt > 0 && segments[i].size@ <= debt {
            remote_retention_model(segments, i + 1, debt - segments[i].size@)
        } else {
            i
        }
    }
}
