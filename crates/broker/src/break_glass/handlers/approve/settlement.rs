//! The step that runs one request against the stored proposal and writes the
//! result to the metadata log.
//!
//! The decision itself is pure. This is where the record it produces reaches
//! the controller, and where a rejected append becomes a wire error code that
//! an operator can act on.

use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassProposalRecord, MetadataRecord};
use krabka_protocol::krabka::break_glass::ApproveBreakGlassRequest;

use super::{Attempt, decide};
use crate::{
    break_glass::{
        action_name,
        config::BreakGlassPolicy,
        handlers::{
            PrivilegedAudit, Refusal, admit_privileged_request, from_wire_uuid, principal_name,
            submit_error,
        },
    },
    broker::Broker,
    codes,
    handlers::RequestContext,
};

/// Apply the request to the stored proposal and write the result.
pub(super) async fn settle(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    policy: BreakGlassPolicy<'_>,
    stored: Option<&BreakGlassProposalRecord>,
    req: &ApproveBreakGlassRequest,
) -> Result<BreakGlassProposalRecord, Refusal> {
    let stored = stored.ok_or_else(|| {
        Refusal::new(
            codes::RESOURCE_NOT_FOUND,
            format!(
                "no break-glass proposal {}",
                from_wire_uuid(req.proposal_id)
            ),
        )
    })?;
    let updated = decide(
        policy,
        &broker.config.operator_keys,
        stored,
        &Attempt {
            principal: &principal_name(ctx),
            key_id: &req.key_id,
            signature: &req.signature,
            withdraw: req.withdraw,
            now_ms: crate::time_util::now_ms(),
        },
    )?;
    let counterparties: Vec<String> = updated
        .approvals
        .iter()
        .map(|approval| approval.principal.clone())
        .collect();
    admit_privileged_request(
        broker.audit_log.as_ref(),
        ctx,
        policy.fingerprint(),
        &PrivilegedAudit {
            outcome: AuditOutcome::Success,
            phase: if req.withdraw {
                PrivilegedPhase::Consumed
            } else {
                PrivilegedPhase::Approved
            },
            action: action_name(updated.action),
            target: &updated.target,
            proposal_id: Some(updated.proposal_id),
            counterparties: &counterparties,
            key_id: &req.key_id,
            signature: &req.signature,
            signature_verified: !req.key_id.is_empty(),
            reason: &updated.reason,
        },
    )
    .await?;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1BreakGlassProposal(updated.clone())])
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "ApproveBreakGlass: submit_change failed");
            let (code, message) = submit_error(&error);
            Refusal::new(code, message)
        })?;
    Ok(updated)
}
