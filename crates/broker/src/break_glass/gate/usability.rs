//! Whether one covering proposal can still authorize a transition.
//!
//! The checks run in a fixed order -- withdrawn, consumed, expired, short of
//! approvals, unsigned -- and the first one that holds is the reason the gate
//! reports for that proposal. The approval count is over distinct principals,
//! because the rule the feature promises is a rule about people.

use {
    super::DenialReason,
    crate::break_glass::config::BreakGlassPolicy,
    krabka_metadata::BreakGlassProposalRecord,
    krabka_verified::break_glass::{
        BreakGlassAdmission, BreakGlassApprovalSigning, BreakGlassProposalFacts,
        BreakGlassSignaturePolicy, break_glass_admission,
    },
};

/// Why `proposal` cannot authorize a transition now, or `None` when it can.
pub(super) fn unusable_because(
    policy: BreakGlassPolicy<'_>,
    proposal: &BreakGlassProposalRecord,
    now_ms: i64,
) -> Option<DenialReason> {
    let proposal_id = proposal.proposal_id;
    let held = distinct_approvers(proposal);
    let required = policy.required_approvals();
    let facts = BreakGlassProposalFacts {
        withdrawn: proposal.withdrawn,
        consumed: proposal.consumed_at_ms != 0,
        expired: now_ms >= proposal.expires_at_ms,
        held_approvals: held,
        required_approvals: required,
        signature_policy: if policy.needs_signature(proposal.action) {
            BreakGlassSignaturePolicy::Required
        } else {
            BreakGlassSignaturePolicy::Optional
        },
        signing: if every_approval_is_signed(proposal) {
            BreakGlassApprovalSigning::AllSigned
        } else {
            BreakGlassApprovalSigning::NotAllSigned
        },
    };
    match break_glass_admission(facts) {
        BreakGlassAdmission::Withdrawn => Some(DenialReason::Withdrawn { proposal_id }),
        BreakGlassAdmission::Consumed => Some(DenialReason::Consumed {
            proposal_id,
            consumed_at_ms: proposal.consumed_at_ms,
        }),
        BreakGlassAdmission::Expired => Some(DenialReason::Expired {
            proposal_id,
            expires_at_ms: proposal.expires_at_ms,
        }),
        BreakGlassAdmission::NotEnoughApprovals => Some(DenialReason::NotEnoughApprovals {
            proposal_id,
            held,
            required,
        }),
        BreakGlassAdmission::Unsigned => Some(DenialReason::Unsigned { proposal_id }),
        BreakGlassAdmission::Usable => None,
    }
}

/// How many distinct principals approved `proposal`.
///
/// The approve handler already refuses a principal that appears in the list, so
/// this count and the list length agree on every record the handler wrote. The
/// count is over distinct principals anyway, because the rule the feature
/// promises is a rule about people and not about rows.
pub(crate) fn distinct_approvers(proposal: &BreakGlassProposalRecord) -> usize {
    let mut seen: Vec<&str> = Vec::with_capacity(proposal.approvals.len());
    for approval in &proposal.approvals {
        if !seen.contains(&approval.principal.as_str()) {
            seen.push(&approval.principal);
        }
    }
    seen.len()
}

/// Whether every approval on `proposal` carries a key id and a signature.
fn every_approval_is_signed(proposal: &BreakGlassProposalRecord) -> bool {
    !proposal.approvals.is_empty()
        && proposal
            .approvals
            .iter()
            .all(|approval| !approval.key_id.is_empty() && !approval.signature.is_empty())
}
