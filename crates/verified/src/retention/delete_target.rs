use creusot_std::prelude::*;

use super::RemoteRetentionSegment;
#[cfg(creusot)]
use super::{remote_prefix_charge, remote_retention_model};

/// Count the oldest finished remote segments that Kafka's remote retention
/// deletes.
///
/// `size_debt` is the total size minus `retention.bytes`, or zero when
/// `retention.bytes` is unset or not exceeded. A tier that accepts no delete
/// (`deletes_allowed` false) selects nothing.
/// A blocked row stops the prefix even when its floor, time or size axis
/// would otherwise select it.
///
/// The result equals `remote_retention_model` from the oldest segment.
/// That fold states the Kafka rule. Independent bounds justify every selected
/// row by a floor/time breach or a funded charged prefix, and require the first
/// kept row to be blocked or fail all three axes. Zero debt cannot delete an
/// unbreached empty row; an expired oversized row exhausts debt without
/// restarting size deletion.
#[ensures(result@ == if deletes_allowed {
    remote_retention_model(segments@, 0, size_debt@)
} else {
    0
})]
#[ensures(result@ <= segments@.len())]
#[ensures(!deletes_allowed ==> result@ == 0)]
#[ensures(retention_prefix_funded(segments@, size_debt@, result@))]
#[ensures(deletes_allowed && result@ < segments@.len()
    ==> segments@[result@].blocked || (!segments@[result@].log_start_breached && !segments@[result@].time_expired
        && (size_debt@ == 0 || remote_prefix_charge(segments@, result@) >= size_debt@
            || remote_prefix_charge(segments@, result@ + 1) > size_debt@)))]
#[must_use]
pub fn remote_retention_prefix(
    deletes_allowed: bool,
    segments: &[RemoteRetentionSegment],
    size_debt: u64,
) -> usize {
    if !deletes_allowed {
        return 0;
    }
    let mut debt = size_debt;
    let mut len = 0usize;
    #[invariant(len@ <= segments@.len())]
    #[invariant(remote_prefix_charge(segments@, len@) >= 0)]
    #[invariant(debt@ == if remote_prefix_charge(segments@, len@) <= size_debt@ {
        size_debt@ - remote_prefix_charge(segments@, len@)
    } else { 0 })]
    #[invariant(retention_prefix_funded(segments@, size_debt@, len@))]
    #[invariant(remote_retention_model(segments@, len@, debt@)
        == remote_retention_model(segments@, 0, size_debt@))]
    #[variant(segments@.len() - len@)]
    while len < segments.len() {
        let segment = segments[len];
        if segment.blocked {
            break;
        }
        if segment.log_start_breached {
            // Deleted below the floor; the size debt is untouched.
        } else if segment.time_expired {
            debt = debt.saturating_sub(segment.size);
        } else if debt > 0 && segment.size <= debt {
            debt -= segment.size;
        } else {
            break;
        }
        len += 1;
    }
    len
}

/// The exclusive delete-through target after an inclusive last offset.
///
/// No selected segment gives no target. An inclusive last offset of
/// `i64::MAX` has no representable successor, so it fails closed with no
/// target too.
#[ensures(match (last_offset, result) {
    (None, result) => result == None,
    (Some(last), None) => last@ == i64::MAX@,
    (Some(last), Some(target)) => last@ < i64::MAX@ && target@ == last@ + 1,
})]
#[must_use]
pub fn retention_delete_target(last_offset: Option<i64>) -> Option<i64> {
    match last_offset {
        Some(last) if last < i64::MAX => Some(last + 1),
        Some(_) | None => None,
    }
}

open_logic! {
/// Each deleted row breaches the floor or time policy, or fits the charged size debt.
fn retention_prefix_funded(segments: Seq<RemoteRetentionSegment>, debt: Int, count: Int) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count ==> !segments[i].blocked && (segments[i].log_start_breached
    || segments[i].time_expired || (super::remote_charge_funded(debt, remote_prefix_charge(segments, i), remote_prefix_charge(segments, i + 1)))) }
}
}
