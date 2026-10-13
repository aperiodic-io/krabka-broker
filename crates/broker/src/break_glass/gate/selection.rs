//! Which stored proposals reach a request, and which one of them the gate
//! spends.
//!
//! Target matching decides the set a request can draw on: an exact target
//! always, and the topic of a partition target for the actions that name a
//! partition. When more than one usable proposal remains, the choice is a
//! function of the records alone, so two brokers reading one image spend the
//! same proposal.

use {crate::break_glass::action_targets_partition, krabka_metadata::BreakGlassAction};

/// Whether a proposal on `proposal_target` covers a request for
/// `request_target`.
pub(super) fn covers(
    proposal_target: &str,
    request_target: &str,
    action: BreakGlassAction,
) -> bool {
    if proposal_target == request_target {
        return true;
    }
    if !action_targets_partition(action) {
        return false;
    }
    topic_of_partition_target(request_target).is_some_and(|topic| proposal_target == topic)
}

/// The topic name in a `"<topic>-<partition>"` target, or `None` when the
/// target does not take that form.
fn topic_of_partition_target(target: &str) -> Option<&str> {
    let (topic, partition) = target.rsplit_once('-')?;
    if topic.is_empty() || partition.is_empty() {
        return None;
    }
    if !partition.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(topic)
}
