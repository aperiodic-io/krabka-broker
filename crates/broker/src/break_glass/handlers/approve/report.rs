//! The names and counts that one settled request reports back.
//!
//! The response body and the audit event read the same fields, and a refusal
//! reports the stored counts, so an operator sees how far a proposal got even
//! when this request did not move it.

use {
    crate::break_glass::{
        action_name,
        config::BreakGlassPolicy,
        gate::distinct_approvers,
        handlers::{Refusal, UNKNOWN_ACTION},
    },
    krabka_audit::PrivilegedPhase,
    krabka_metadata::BreakGlassProposalRecord,
    uuid::Uuid,
};

/// The phase an audit event records for one settled request.
pub(super) fn phase_of(
    outcome: &Result<BreakGlassProposalRecord, Refusal>,
    withdraw: bool,
) -> PrivilegedPhase {
    match (outcome, withdraw) {
        (Err(_), _) => PrivilegedPhase::Refused,
        // A withdrawal spends the proposal without doing the action, so it is
        // the same end of the lifecycle that a consume reaches.
        (Ok(_), true) => PrivilegedPhase::Consumed,
        (Ok(_), false) => PrivilegedPhase::Approved,
    }
}

/// The fields that the response and the audit event both read.
///
/// A refusal still reports the stored counts, so an operator sees how far the
/// proposal got even when this request did not move it.
pub(super) struct Report<'a> {
    pub(super) action: &'a str,
    pub(super) target: &'a str,
    pub(super) proposal_id: Option<Uuid>,
    pub(super) counterparties: Vec<String>,
    pub(super) held: i32,
    pub(super) required: i32,
}

impl<'a> Report<'a> {
    pub(super) fn of(
        stored: Option<&'a BreakGlassProposalRecord>,
        settled: Option<&BreakGlassProposalRecord>,
        policy: BreakGlassPolicy<'_>,
    ) -> Self {
        let required = count(policy.required_approvals());
        let Some(stored) = stored else {
            return Self {
                action: UNKNOWN_ACTION,
                target: "",
                proposal_id: None,
                counterparties: Vec::new(),
                held: 0,
                required,
            };
        };
        let latest = settled.unwrap_or(stored);
        Self {
            action: action_name(stored.action),
            target: &stored.target,
            proposal_id: Some(stored.proposal_id),
            counterparties: latest
                .approvals
                .iter()
                .map(|approval| approval.principal.clone())
                .collect(),
            held: count(distinct_approvers(latest)),
            required,
        }
    }
}

/// The text an audit event carries for one settled request.
pub(super) fn reason(outcome: &Result<BreakGlassProposalRecord, Refusal>) -> &str {
    match outcome {
        Ok(record) => record.reason.as_str(),
        Err(refusal) => refusal.message.as_str(),
    }
}

/// A count as the wire carries it. A count beyond `i32::MAX` saturates, and no
/// reachable approver set comes near it.
fn count(value: usize) -> i32 { i32::try_from(value).unwrap_or(i32::MAX) }
