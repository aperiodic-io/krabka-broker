//! `ProposeBreakGlass`, api key 1017.
//!
//! One request opens one break-glass proposal. The controller gives it an id
//! and an expiry, and holds it in the metadata log until a transition consumes
//! it, an operator withdraws it, or it expires.
//!
//! A proposal carries no approval. The proposer sends an
//! [`ApproveBreakGlass`](super::approve) request from a second principal before
//! the action can run, and the proposer cannot be that second principal.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. A denied request
//! answers `CLUSTER_AUTHORIZATION_FAILED` (31).

use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataRecord};
use krabka_protocol::krabka::break_glass::{ProposeBreakGlassRequest, ProposeBreakGlassResponse};
use krabka_units::{Time, convert::TimeExt as _};
use uuid::Uuid;

use crate::{
    break_glass::{
        action_from_wire, action_name,
        config::BreakGlassPolicy,
        handlers::{
            PrivilegedAudit, Refusal, UNKNOWN_ACTION, admit_privileged_request, audit_privileged,
            principal_name, submit_error, to_wire_uuid,
        },
    },
    broker::Broker,
    codes,
    handlers::{RequestContext, cluster_alter_denied, encode_response},
};

/// The `ttl_ms` value that asks for the configured lifetime.
pub(crate) const TTL_CONFIGURED: i64 = 0;

wire_handler!(
    handle,
    "handle_propose_break_glass",
    "ProposeBreakGlass",
    "info",
    ProposeBreakGlassRequest,
    |broker, version, req, ctx| {
        let policy = BreakGlassPolicy::new(&broker.config.break_glass);
        let image = broker.controller.current_image();
        let action = action_from_wire(req.action);
        let action_label = action.map_or(UNKNOWN_ACTION, action_name);

        let outcome = if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            Err(Refusal::new(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                "propose-break-glass denied",
            ))
        } else {
            propose(broker, ctx, policy, action, &req).await
        };

        let (proposal, refusal) = match outcome {
            Ok(proposal) => (Some(proposal), None),
            Err(refusal) => (None, Some(refusal)),
        };
        audit_privileged(
            broker.audit_log.as_ref(),
            ctx,
            policy.fingerprint(),
            &PrivilegedAudit {
                outcome: if refusal.is_some() {
                    AuditOutcome::Failure
                } else {
                    AuditOutcome::Success
                },
                phase: if refusal.is_some() {
                    PrivilegedPhase::Refused
                } else {
                    PrivilegedPhase::Proposed
                },
                action: action_label,
                target: &req.target,
                proposal_id: proposal.as_ref().map(|p| p.proposal_id),
                counterparties: &[],
                key_id: "",
                signature: &[],
                signature_verified: false,
                reason: refusal.as_ref().map_or(req.reason.as_str(), |r| &r.message),
            },
        );

        let response = match (proposal, refusal) {
            (Some(proposal), _) => ProposeBreakGlassResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                error_message: None,
                proposal_id: to_wire_uuid(proposal.proposal_id),
                expires_at_ms: proposal.expires_at_ms,
                ..ProposeBreakGlassResponse::default()
            },
            (None, refusal) => {
                let refusal = refusal.unwrap_or_else(|| {
                    Refusal::new(codes::UNKNOWN_SERVER_ERROR, "the proposal did not open")
                });
                ProposeBreakGlassResponse {
                    throttle_time_ms: 0,
                    error_code: refusal.code,
                    error_message: Some(refusal.message),
                    ..ProposeBreakGlassResponse::default()
                }
            }
        };
        encode_response(&response, version)
    }
);

/// Build the proposal, write it to the metadata log, and return it.
async fn propose(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    policy: BreakGlassPolicy<'_>,
    action: Option<BreakGlassAction>,
    req: &ProposeBreakGlassRequest,
) -> Result<BreakGlassProposalRecord, Refusal> {
    let action = action.ok_or_else(|| {
        Refusal::new(
            codes::INVALID_REQUEST,
            format!("{} names no break-glass action", req.action),
        )
    })?;
    let proposal = decide(
        policy,
        &ProposalRequest {
            action,
            target: &req.target,
            reason: &req.reason,
            ttl_ms: req.ttl_ms,
            proposer: &principal_name(ctx),
            proposal_id: Uuid::new_v4(),
            now_ms: crate::time_util::now_ms(),
        },
    )?;
    admit_privileged_request(
        broker.audit_log.as_ref(),
        ctx,
        policy.fingerprint(),
        &PrivilegedAudit {
            outcome: AuditOutcome::Success,
            phase: PrivilegedPhase::Proposed,
            action: action_name(proposal.action),
            target: &proposal.target,
            proposal_id: Some(proposal.proposal_id),
            counterparties: &[],
            key_id: "",
            signature: &[],
            signature_verified: false,
            reason: &proposal.reason,
        },
    )
    .await?;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1BreakGlassProposal(proposal.clone())])
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "ProposeBreakGlass: submit_change failed");
            let (code, message) = submit_error(&error);
            Refusal::new(code, message)
        })?;
    Ok(proposal)
}

/// What a caller asked to propose.
pub(crate) struct ProposalRequest<'a> {
    /// The transition to gate.
    pub action: BreakGlassAction,
    /// What the transition applies to.
    pub target: &'a str,
    /// Free text that says why the operator needs the transition.
    pub reason: &'a str,
    /// Lifetime in milliseconds, or [`TTL_CONFIGURED`].
    pub ttl_ms: i64,
    /// The authenticated principal of the connection.
    pub proposer: &'a str,
    /// The id the controller gives the new proposal.
    pub proposal_id: Uuid,
    /// The controller's clock, in epoch milliseconds.
    pub now_ms: i64,
}

/// The proposal record that a request opens.
///
/// The proposer must be in `break_glass.approvers`. A proposer outside the set
/// could open a proposal that two approvers then sign, which turns a rule about
/// three people into a rule about two people and a stranger.
///
/// [`TTL_CONFIGURED`] asks for `break_glass.proposal_ttl`, and a longer
/// lifetime is capped at it. An operator can shorten a proposal, and cannot
/// lengthen one past what the broker configured.
///
/// # Errors
///
/// Returns [`Refusal`] with `BREAK_GLASS_NOT_AN_APPROVER` (1008) when the
/// proposer is outside the approver set, and with `INVALID_REQUEST` (42) when
/// the target is empty or the lifetime is negative.
pub(crate) fn decide(
    policy: BreakGlassPolicy<'_>,
    request: &ProposalRequest<'_>,
) -> Result<BreakGlassProposalRecord, Refusal> {
    if !policy.is_approver(request.proposer) {
        let message = if policy.is_enabled() {
            format!("{} is not a break-glass approver", request.proposer)
        } else {
            "this broker configures no break-glass approver set".to_owned()
        };
        return Err(Refusal::new(codes::BREAK_GLASS_NOT_AN_APPROVER, message));
    }
    if request.target.is_empty() {
        return Err(Refusal::new(
            codes::INVALID_REQUEST,
            "a break-glass proposal names a target",
        ));
    }
    if request.ttl_ms < TTL_CONFIGURED {
        return Err(Refusal::new(
            codes::INVALID_REQUEST,
            format!("a lifetime of {} milliseconds is negative", request.ttl_ms),
        ));
    }
    let ttl = effective_ttl(policy.proposal_ttl(), request.ttl_ms);
    Ok(BreakGlassProposalRecord {
        proposal_id: request.proposal_id,
        action: request.action,
        target: request.target.to_owned(),
        proposer: request.proposer.to_owned(),
        reason: request.reason.to_owned(),
        created_at_ms: request.now_ms,
        expires_at_ms: request.now_ms.saturating_add(ttl),
        approvals: Vec::new(),
        consumed_at_ms: 0,
        withdrawn: false,
    })
}

/// The lifetime a proposal takes, in milliseconds.
fn effective_ttl(configured: Time, requested_ms: i64) -> i64 {
    let configured_ms = configured.millis_i64();
    if requested_ms == TTL_CONFIGURED {
        configured_ms
    } else {
        requested_ms.min(configured_ms)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{minutes, secs};

    use super::*;
    use crate::{
        break_glass::gate::tests::{CREATED_MS, config},
        config::BreakGlassConfig,
    };

    fn request<'a>(proposer: &'a str, target: &'a str, ttl_ms: i64) -> ProposalRequest<'a> {
        ProposalRequest {
            action: BreakGlassAction::DeleteTopic,
            target,
            reason: "incident 42",
            ttl_ms,
            proposer,
            proposal_id: Uuid::from_u128(1),
            now_ms: CREATED_MS,
        }
    }

    #[test]
    fn an_approver_opens_a_proposal_with_no_approval_on_it() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);

        let proposal = decide(policy, &request("User:alice", "doomed", TTL_CONFIGURED))
            .expect("an approver may propose");

        let expected = BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(1),
            action: BreakGlassAction::DeleteTopic,
            target: "doomed".to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: CREATED_MS,
            expires_at_ms: CREATED_MS + minutes(3).millis_i64(),
            approvals: Vec::new(),
            consumed_at_ms: 0,
            withdrawn: false,
        };
        check!(proposal == expected);
    }

    #[test]
    fn a_proposal_takes_the_shorter_of_the_asked_and_the_configured_lifetime() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let configured_ms = minutes(3).millis_i64();
        let cases = [
            ("the configured lifetime", TTL_CONFIGURED, configured_ms),
            ("a shorter lifetime", 1_000, 1_000),
            (
                "the configured lifetime exactly",
                configured_ms,
                configured_ms,
            ),
            (
                "a longer lifetime is capped",
                configured_ms * 10,
                configured_ms,
            ),
        ];
        for (label, ttl_ms, expected) in cases {
            let proposal = decide(policy, &request("User:alice", "doomed", ttl_ms))
                .expect("an approver may propose");
            check!(
                proposal.expires_at_ms == CREATED_MS + expected,
                "case {label}"
            );
        }
    }

    #[test]
    fn the_proposer_must_be_in_the_approver_set() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);

        let outcome = decide(policy, &request("User:mallory", "doomed", TTL_CONFIGURED));

        assert!(let Err(refusal) = outcome);
        check!(refusal.code == codes::BREAK_GLASS_NOT_AN_APPROVER);
        check!(refusal.message.contains("User:mallory"));
    }

    #[test]
    fn a_broker_with_no_approver_set_says_so() {
        let config = BreakGlassConfig::default();
        let policy = BreakGlassPolicy::new(&config);

        let outcome = decide(policy, &request("User:alice", "doomed", TTL_CONFIGURED));

        assert!(let Err(refusal) = outcome);
        check!(refusal.code == codes::BREAK_GLASS_NOT_AN_APPROVER);
        check!(refusal.message.contains("no break-glass approver set"));
    }

    #[test]
    fn a_malformed_request_is_refused_before_it_reaches_the_log() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let cases = [
            ("an empty target", "", TTL_CONFIGURED),
            ("a negative lifetime", "doomed", -1),
        ];
        for (label, target, ttl_ms) in cases {
            let outcome = decide(policy, &request("User:alice", target, ttl_ms));
            assert!(let Err(refusal) = outcome, "case {label}");
            check!(refusal.code == codes::INVALID_REQUEST, "case {label}");
        }
    }

    #[test]
    fn a_clock_at_the_end_of_time_saturates_instead_of_wrapping() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let late = ProposalRequest {
            now_ms: i64::MAX,
            ..request("User:alice", "doomed", TTL_CONFIGURED)
        };

        let proposal = decide(policy, &late).expect("an approver may propose");

        check!(proposal.expires_at_ms == i64::MAX);
    }

    #[test]
    fn the_effective_lifetime_reads_the_configured_one_for_zero() {
        let cases = [
            ("zero asks for the configured one", 0_i64, 30_000_i64),
            ("one millisecond", 1, 1),
            ("far beyond the configured one", i64::MAX, 30_000),
        ];
        for (label, requested_ms, expected) in cases {
            check!(
                effective_ttl(secs(30), requested_ms) == expected,
                "case {label}"
            );
        }
    }
}
