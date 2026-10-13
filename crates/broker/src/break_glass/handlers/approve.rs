//! `ApproveBreakGlass`, api key 1018.
//!
//! One request adds one approval to a break-glass proposal, or withdraws the
//! proposal. The `withdraw` flag picks between the two, which follows
//! `AlterableBarrierGroup` and its `delete` flag.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. A denied request
//! answers `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! # Three checks make it a two-person rule
//!
//! The approver must be in `break_glass.approvers`, must not be the proposer,
//! and must not already appear in the approval list. Without all three the rule
//! is a two-click rule.
//!
//! # The broker reads the approver set here, and not when it acts
//!
//! `break_glass.approvers` comes from each broker's own file. This handler is
//! the only place that reads it. The gate that spends an approval never reads
//! it again, for two reasons.
//!
//! A second check at consumption time would make the consume non-deterministic
//! across brokers. The set is a per-node file value, and two nodes can
//! legitimately disagree during a rolling configuration change. Two brokers have
//! to reach the same answer about one record.
//!
//! The operator-facing consequence is also the right one. An operator who
//! removes a person stops that person from making new approvals. The removal
//! does not silently invalidate an incident response that is already under way.
//! The safety bound is `break_glass.proposal_ttl`: wait it out, and every
//! pending approval by that principal is dead.
//!
//! Each audit event records
//! [`BreakGlassPolicy::fingerprint`](crate::break_glass::config::BreakGlassPolicy::fingerprint),
//! so a broker that disagrees with its peers about the set is visible in the
//! audit log after the fact.

pub(crate) use self::decision::{Attempt, decide};
use {
    self::{
        report::{Report, phase_of, reason},
        settlement::settle,
    },
    crate::{
        break_glass::{
            config::BreakGlassPolicy,
            handlers::{PrivilegedAudit, Refusal, audit_privileged, from_wire_uuid},
        },
        codes,
        handlers::{cluster_alter_denied, encode_response},
    },
    krabka_audit::AuditOutcome,
    krabka_protocol::krabka::break_glass::{ApproveBreakGlassRequest, ApproveBreakGlassResponse},
};

mod decision;
mod report;
mod settlement;

#[cfg(test)]
mod tests;

wire_handler!(
    handle,
    "handle_approve_break_glass",
    "ApproveBreakGlass",
    "info",
    ApproveBreakGlassRequest,
    |broker, version, req, ctx| {
        let policy = BreakGlassPolicy::new(&broker.config.break_glass);
        let image = broker.controller.current_image();
        let stored = image.break_glass_proposal(from_wire_uuid(req.proposal_id));

        let outcome = if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            Err(Refusal::new(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                "approve-break-glass denied",
            ))
        } else {
            settle(broker, ctx, policy, stored, &req).await
        };

        let report = Report::of(stored, outcome.as_ref().ok(), policy);
        audit_privileged(
            broker.audit_log.as_ref(),
            ctx,
            policy.fingerprint(),
            &PrivilegedAudit {
                outcome: if outcome.is_ok() {
                    AuditOutcome::Success
                } else {
                    AuditOutcome::Failure
                },
                phase: phase_of(&outcome, req.withdraw),
                action: report.action,
                target: report.target,
                proposal_id: report.proposal_id,
                counterparties: &report.counterparties,
                key_id: &req.key_id,
                signature: &req.signature,
                signature_verified: outcome.is_ok() && !req.key_id.is_empty(),
                reason: reason(&outcome),
            },
        );

        let response = match outcome {
            Ok(_) => ApproveBreakGlassResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                error_message: None,
                approvals_held: report.held,
                approvals_required: report.required,
                ..ApproveBreakGlassResponse::default()
            },
            Err(refusal) => ApproveBreakGlassResponse {
                throttle_time_ms: 0,
                error_code: refusal.code,
                error_message: Some(refusal.message),
                approvals_held: report.held,
                approvals_required: report.required,
                ..ApproveBreakGlassResponse::default()
            },
        };
        encode_response(&response, version)
    }
);
