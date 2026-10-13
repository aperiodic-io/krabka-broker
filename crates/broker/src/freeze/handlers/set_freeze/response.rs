//! The audit trail and the wire response of a `SetTopicFreeze` request.
//!
//! Every outcome reaches the audit log, and a refusal reaches it as surely as a
//! success. The caller reads the outcome from the response's own `error_code`,
//! never from a transport failure.

use {
    super::outcome::{Accepted, Refusal},
    crate::{
        broker::Broker,
        codes,
        freeze::{
            freeze_target,
            handlers::{FreezeAudit, audit_freeze, pattern_type_concrete},
        },
        handlers::{RequestContext, audit_admin},
    },
    krabka_audit::{AuditOutcome, AuditResource, PrivilegedPhase},
    krabka_protocol::krabka::freeze::{SetTopicFreezeRequest, SetTopicFreezeResponse},
    uuid::Uuid,
};

/// The audit action name of a freeze.
pub(super) const FREEZE_ACTION: &str = "set_topic_freeze";

/// The audit action name of a thaw. It is the break-glass action's own name, so
/// the audit event and the `break_glass_*` metric labels read alike.
pub(super) const THAW_ACTION: &str = "thaw_topic_freeze";

/// Audit the outcome and build the response.
///
/// Every outcome reaches the audit log, and a refusal reaches it as surely as a
/// success. A success also emits the ordinary administrative event, so a SIEM
/// rule that already reads those sees a freeze and a thaw.
pub(super) fn respond(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req: &SetTopicFreezeRequest,
    outcome: &Result<Accepted, Refusal>,
) -> SetTopicFreezeResponse {
    let target = pattern_type_concrete(req.pattern_type)
        .map_or_else(|| req.scope.clone(), |kind| freeze_target(kind, &req.scope));
    let proposal_id = Uuid::from_bytes(req.proposal_id.0);
    let (code, message, signature_verified, reason) = match outcome {
        Ok(accepted) => (
            codes::NONE,
            None,
            accepted.signature_verified,
            req.reason.clone(),
        ),
        Err(refusal) => (
            refusal.code,
            Some(refusal.message.clone()),
            refusal.signature_verified,
            refusal.message.clone(),
        ),
    };
    let succeeded = code == codes::NONE;
    let audit = FreezeAudit {
        action: if req.frozen {
            FREEZE_ACTION
        } else {
            THAW_ACTION
        },
        target: target.clone(),
        proposal_id,
        key_id: &req.key_id,
        signature: &req.signature,
        signature_verified,
        // On a success this is the stamp the accepted record carries, which is
        // the operator's own on a signed request. A refusal has no record, so
        // the event carries the stamp the caller presented: that is the value
        // their signature covers, and an auditor re-checking a refused
        // signature needs the same bytes the broker checked.
        set_at_ms: match outcome {
            Ok(accepted) => accepted.record.set_at_ms,
            Err(_) => req.set_at_ms,
        },
        reason,
    };
    audit_freeze(
        broker.audit_log.as_ref(),
        ctx,
        if succeeded {
            AuditOutcome::Success
        } else {
            AuditOutcome::Failure
        },
        if succeeded {
            PrivilegedPhase::Applied
        } else {
            PrivilegedPhase::Refused
        },
        &audit,
        &broker.config.break_glass.approvers,
    );
    if succeeded {
        audit_admin(
            broker.audit_log.as_ref(),
            ctx,
            "SetTopicFreeze",
            AuditOutcome::Success,
            admin_resources(&target, req.frozen, proposal_id),
        );
    }

    SetTopicFreezeResponse {
        throttle_time_ms: 0,
        error_code: code,
        error_message: message,
        ..SetTopicFreezeResponse::default()
    }
}

/// The resources that the ordinary administrative event names.
///
/// A thaw names the proposal it spent as well as the scope, so a rule that
/// reads the administrative events joins the approval to the transition on that
/// id.
pub(super) fn admin_resources(target: &str, frozen: bool, proposal_id: Uuid) -> Vec<AuditResource> {
    let mut resources = vec![AuditResource {
        resource_type: "TopicFreeze".to_owned(),
        name: target.to_owned(),
    }];
    if !frozen {
        resources.push(AuditResource {
            resource_type: "break-glass-proposal".to_owned(),
            name: proposal_id.to_string(),
        });
    }
    resources
}
