//! The gate that every break-glass transition calls.
//!
//! [`authorize`] finds an approved proposal that covers a request and returns
//! the metadata record that spends it. Every gated handler goes through this
//! one function, so one set of rules decides what an approval authorizes.

use {
    self::{denial::nearer_reason, selection::covers, usability::unusable_because},
    crate::break_glass::config::BreakGlassPolicy,
    krabka_metadata::BreakGlassProposalRecord,
    uuid::Uuid,
};
pub(crate) use {
    self::{
        denial::{BreakGlassDenial, DenialReason},
        usability::distinct_approvers,
    },
    crate::config::BreakGlassConfig,
    krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord},
};

mod denial;
mod selection;
mod usability;

#[cfg(test)]
pub(crate) mod tests;

/// Whether this broker gates the privileged transitions at all.
///
/// A caller asks this first. An empty `break_glass.approvers` turns the
/// workflow off, and every gated transition then behaves as it does on a
/// cluster with no `[break_glass]` section. [`authorize`] on such a broker
/// denies with [`DenialReason::NoProposal`], because there is no proposal for
/// it to return, so a caller that skips this test refuses every transition.
pub(crate) fn is_gated(config: &BreakGlassConfig) -> bool {
    BreakGlassPolicy::new(config).is_enabled()
}

/// Place a consumed proposal before the metadata record it authorizes.
///
/// An ungated broker gets the action record alone. A gated broker persists the
/// consume and action together, so the approval cannot be spent twice.
pub(crate) fn records_with_consume(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    action: BreakGlassAction,
    target: &str,
    now_ms: i64,
    record: MetadataRecord,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    if !is_gated(config) {
        return Ok(vec![record]);
    }
    let consumed = authorize(image, config, action, target, now_ms)?;
    Ok(vec![consumed, record])
}

/// Find the approved proposal that authorizes `action` on `target`, and return
/// the record that spends it.
///
/// # The caller must persist the consume with or before the action
///
/// The returned record is the stored proposal with `consumed_at_ms` stamped. A
/// metadata-backed caller prepends it to its action records and submits one
/// raft append. A local action calls
/// [`crate::break_glass::persistence::spend_before_local_action`] and starts
/// only after the consume commits. These are the only safe orders: the action
/// must never become durable or start while the proposal remains reusable.
///
/// # What makes a proposal usable
///
/// The proposal must name `action`, must cover `target`, must not be withdrawn,
/// must not be consumed, must not have expired, and must hold approvals from at
/// least `break_glass.required_approvals` distinct principals. Every approval
/// must also carry a signature when `break_glass.signed_actions` names the
/// action.
///
/// Two concurrent approvals cannot overwrite each other, because
/// [`MetadataImage::validate`] refuses a record whose approval list is not a
/// strict extension of the stored list, and refuses any change to a consumed or
/// a withdrawn proposal. The controller also admits a consume only when its
/// committed image covers the full metadata log. A concurrent consume or an
/// uncommitted tail therefore fails before append and must retry.
///
/// # Target matching
///
/// A proposal covers a request when the two targets are equal. A proposal on a
/// bare topic name also covers `"<topic>-<partition>"`, so one proposal can
/// authorize the same action on every partition of a topic. The wider rule
/// applies only to the actions that name a partition, which
/// [`action_targets_partition`] lists. Without that limit a proposal to delete
/// the topic `logs` would also cover the topic `logs-2024`, whose name reads as
/// partition 2024 of topic `logs`.
///
/// # The approver set is not read here
///
/// The broker checks `break_glass.approvers` when a person approves, and never
/// when it spends the approval. A second check here would make the consume
/// depend on a per-node file value, and two brokers can legitimately disagree
/// about that value during a rolling configuration change. The operator-facing
/// consequence is the right one as well: removing a person stops that person
/// from making new approvals, and it does not silently invalidate an incident
/// response that is already under way. The time to live is the safety bound.
///
/// `break_glass.signed_actions` is read here, because it answers a different
/// question. The approver set answers "may this person approve", which is
/// settled when they approve. `signed_actions` answers "does this action need a
/// signature", which is a property of the transition the broker is about to do,
/// so the broker answers it when it acts.
///
/// # Errors
///
/// Returns [`BreakGlassDenial`] when no proposal covers the request, or when
/// the covering proposal is withdrawn, consumed, expired, short of approvals,
/// or unsigned for an action that needs a signature. The caller picks the wire
/// code: `POLICY_VIOLATION` (44) on a Kafka API, and
/// `BREAK_GLASS_APPROVAL_REQUIRED` (1006) on the private thaw path.
///
/// [`action_targets_partition`]: crate::break_glass::action_targets_partition
pub(crate) fn authorize(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    action: BreakGlassAction,
    target: &str,
    now_ms: i64,
) -> Result<MetadataRecord, BreakGlassDenial> {
    let policy = BreakGlassPolicy::new(config);
    let mut usable: Vec<&BreakGlassProposalRecord> = Vec::new();
    let mut denial: Option<DenialReason> = None;

    for proposal in image.break_glass_proposals() {
        if proposal.action != action || !covers(&proposal.target, target, action) {
            continue;
        }
        match unusable_because(policy, proposal, now_ms) {
            None => usable.push(proposal),
            Some(reason) => denial = Some(nearer_reason(denial, reason)),
        }
    }

    let candidates: Vec<(i64, u64, u64)> = usable
        .iter()
        .map(|proposal| {
            let (high, low) = proposal.proposal_id.as_u64_pair();
            (proposal.expires_at_ms, high, low)
        })
        .collect();
    let selected = krabka_verified::break_glass::select_break_glass_candidate(&candidates)
        .map(|index| usable[index]);
    match selected {
        Some(proposal) => Ok(MetadataRecord::V1BreakGlassProposal(
            BreakGlassProposalRecord {
                // `0` is the unconsumed sentinel, so a clock that reads zero
                // still has to stamp a consumed record.
                consumed_at_ms: now_ms.max(1),
                ..proposal.clone()
            },
        )),
        None => Err(BreakGlassDenial {
            action,
            target: target.to_owned(),
            reason: denial.unwrap_or(DenialReason::NoProposal),
        }),
    }
}

/// Whether a consumed record is valid for the action about to start.
///
/// Local actions cannot share a raft append with their consume. They call this
/// at the persistence boundary so a malformed or misrouted record cannot be
/// committed and then treated as authorization for a different action.
pub(crate) fn consumed_record_matches(
    record: &MetadataRecord,
    action: BreakGlassAction,
    target: &str,
) -> bool {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => {
            proposal.consumed_at_ms > 0
                && proposal.action == action
                && covers(&proposal.target, target, action)
        }
        _ => false,
    }
}

/// The proposal that a consumed record names.
///
/// [`authorize`] only ever answers with a proposal record, so the `None` arm
/// costs one match rather than a panic. Every gated handler reads the id back
/// through this one function, to dedupe a spend and to name the proposal in
/// its audit event.
pub(crate) fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}
