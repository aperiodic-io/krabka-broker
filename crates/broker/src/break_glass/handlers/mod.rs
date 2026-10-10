//! The three private APIs of the break-glass workflow.
//!
//! | Api key | Module | Purpose | Authorization |
//! | --- | --- | --- | --- |
//! | 1017 | [`propose`] | open a proposal | `Alter` on `Cluster("kafka-cluster")` |
//! | 1018 | [`approve`] | approve a proposal, or withdraw one | `Alter` on `Cluster("kafka-cluster")` |
//! | 1019 | [`describe`] | read proposals and their approvals | `Describe` on `Cluster("kafka-cluster")` |
//!
//! All three keys sit in the krabka-private range at 1000 and above, and all
//! three speak version 0 only with flexible framing. The broker registers them
//! for dispatch and never advertises them, so `kafka-broker-api-versions.sh`
//! prints no row for them. A denied request answers
//! `CLUSTER_AUTHORIZATION_FAILED` (31), which is what every other private key
//! answers.
//!
//! A withdraw rides the approve key on a `withdraw` flag, which is the shape
//! `AlterBarrierGroups` uses for its `delete` flag. Approve and withdraw both
//! name a proposal that exists and both act on it, so they share a request that
//! already carries a proposal id.
//!
//! # Every phase reaches the audit log
//!
//! A proposal, each approval, a withdrawal, and every refusal produce one
//! [`krabka_audit::AuditEvent::PrivilegedAction`] event. The event names the
//! acting principal, the other people who approved, the key id, the raw
//! signature, and a fingerprint of this broker's approver set. A two-person
//! rule whose record names one person is not a two-person rule.
//!
//! The gated transitions themselves live in other modules — `elect_leaders`,
//! `unregister_broker`, `alter_partition_reassignments`, `delete_topics` and
//! `delete_records` — and all five emit their event through [`audit`], so the
//! shape of a transition event is written once.

pub(crate) mod approve;
pub(crate) mod audit;
pub(crate) mod batch;
pub(crate) mod describe;
pub(crate) mod propose;

use krabka_audit::{
    AuditEndpoint, AuditError, AuditEvent, AuditLog, AuditMode, AuditOutcome, AuditPrincipal,
    PrivilegedPhase,
};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_raft::RaftError;
use uuid::Uuid;

// The `Describe` gate that `describe` applies. It lives in `crate::handlers`
// beside its `Alter` twin, so the break-glass control plane does not reach into
// another subsystem for an ACL gate.
pub(crate) use crate::handlers::cluster_describe_denied;
use crate::{codes, handlers::RequestContext};

/// The audited action name of a request whose wire action names no transition.
pub(crate) const UNKNOWN_ACTION: &str = "unknown";

/// A refused request, and the wire code the response carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    /// The Kafka error code of the response.
    pub code: i16,
    /// The text the response carries as its `error_message`, and the text the
    /// audit event carries as its reason.
    pub message: String,
}

impl Refusal {
    /// Refuse with `code` and `message`.
    pub(crate) fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// The `KafkaPrincipal` string of the connection, such as `"User:alice"`.
///
/// `break_glass.approvers`, `[[operator_keys]]`, and the stored approvals all
/// name a principal in this form, so one spelling compares against another.
pub(crate) fn principal_name(ctx: &RequestContext<'_>) -> String {
    ctx.principal.to_kafka().to_string()
}

/// The `uuid` form of a wire id.
pub(crate) fn from_wire_uuid(id: WireUuid) -> Uuid {
    Uuid::from_bytes(id.0)
}

/// The wire form of a `uuid` id.
pub(crate) fn to_wire_uuid(id: Uuid) -> WireUuid {
    WireUuid(*id.as_bytes())
}

/// The error code and the text that a failed `submit_change` answers with.
///
/// A raft submit fails for two reasons that a caller acts on differently. This
/// broker is not the controller leader, which the caller retries somewhere
/// else, or the quorum did not take the record, which the caller retries later.
pub(crate) fn submit_error(error: &RaftError) -> (i16, String) {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => (
            codes::NOT_CONTROLLER,
            "this broker is not the active controller".to_owned(),
        ),
        RaftError::UncommittedTail => (
            codes::NOT_CONTROLLER,
            "the controller has not committed its metadata log yet".to_owned(),
        ),
        other => (
            codes::COORDINATOR_NOT_AVAILABLE,
            format!("the metadata quorum did not take the record: {other}"),
        ),
    }
}

/// The fields of one [`AuditEvent::PrivilegedAction`] event.
///
/// The struct exists so that one call site names each field. The event carries
/// thirteen of them, and a positional call would be unreadable and easy to
/// transpose.
#[derive(Clone, Copy)]
pub(crate) struct PrivilegedAudit<'a> {
    /// Whether the phase succeeded.
    pub outcome: AuditOutcome,
    /// Which step of the workflow this event records.
    pub phase: PrivilegedPhase,
    /// The name of the gated transition, from
    /// [`action_name`](crate::break_glass::action_name). A propose whose wire
    /// action names no transition audits [`UNKNOWN_ACTION`], so a refusal that
    /// the broker cannot name still reaches the audit log.
    pub action: &'a str,
    /// What the transition applies to.
    pub target: &'a str,
    /// The proposal, or `None` before one exists.
    pub proposal_id: Option<Uuid>,
    /// The other people who approved, in the order they approved.
    pub counterparties: &'a [String],
    /// The operator key that signed, or an empty string.
    pub key_id: &'a str,
    /// The detached signature, or an empty slice.
    pub signature: &'a [u8],
    /// Whether the broker verified the signature.
    pub signature_verified: bool,
    /// Free text that says what happened, or why it did not.
    pub reason: &'a str,
}

/// Emit one privileged-action audit event.
///
/// Every phase calls this, refusals included. A refusal that reaches no audit
/// log is a refusal an auditor cannot count.
pub(crate) fn audit_privileged(
    audit_log: &AuditLog,
    ctx: &RequestContext<'_>,
    approver_set_fingerprint: String,
    event: &PrivilegedAudit<'_>,
) {
    audit_log.emit(privileged_event(
        ctx,
        approver_set_fingerprint,
        event,
        NO_SIGNED_STAMP,
    ));
}

/// Write the action's intent before a fail-closed caller mutates state.
///
/// Fail-open keeps the existing single outcome record. Fail-closed records an
/// `Attempted` event durably; only then may the caller commit the action.
pub(crate) async fn require_privileged(
    audit_log: &AuditLog,
    ctx: &RequestContext<'_>,
    approver_set_fingerprint: String,
    event: &PrivilegedAudit<'_>,
) -> Result<(), AuditError> {
    if audit_log.mode() != AuditMode::FailClosed {
        return Ok(());
    }
    let attempted = PrivilegedAudit {
        outcome: AuditOutcome::Success,
        phase: PrivilegedPhase::Attempted,
        ..*event
    };
    audit_log
        .emit_required(privileged_event(
            ctx,
            approver_set_fingerprint,
            &attempted,
            NO_SIGNED_STAMP,
        ))
        .await
}

/// Refuse a workflow request with the same policy error when its durable audit intent fails.
pub(crate) async fn admit_privileged_request(
    audit_log: &AuditLog,
    ctx: &RequestContext<'_>,
    approver_set_fingerprint: String,
    event: &PrivilegedAudit<'_>,
) -> Result<(), Refusal> {
    require_privileged(audit_log, ctx, approver_set_fingerprint, event)
        .await
        .map_err(|error| {
            Refusal::new(
                crate::codes::POLICY_VIOLATION,
                format!("privileged action refused: {error}"),
            )
        })
}

/// The `signed_at_ms` of a break-glass event.
///
/// A break-glass approval signs the proposal's `created_at_ms` and
/// `expires_at_ms`, not one stamp, so there is nothing to put there.
const NO_SIGNED_STAMP: i64 = 0;

/// Build the [`AuditEvent::PrivilegedAction`] that `event` describes.
///
/// `signed_at_ms` is the stamp inside the signed preimage, which a write-freeze
/// carries and a break-glass event does not.
pub(crate) fn privileged_event(
    ctx: &RequestContext<'_>,
    approver_set_fingerprint: String,
    event: &PrivilegedAudit<'_>,
    signed_at_ms: i64,
) -> AuditEvent {
    AuditEvent::PrivilegedAction {
        outcome: event.outcome,
        phase: event.phase,
        action: event.action.to_owned(),
        target: event.target.to_owned(),
        proposal_id: event
            .proposal_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        principal: AuditPrincipal {
            name: principal_name(ctx),
            auth_method: format!("{:?}", ctx.principal.auth_method),
        },
        counterparties: event
            .counterparties
            .iter()
            .map(|name| AuditPrincipal {
                name: name.clone(),
                auth_method: String::new(),
            })
            .collect(),
        approver_set_fingerprint,
        key_id: event.key_id.to_owned(),
        signature: event.signature.to_vec(),
        signature_verified: event.signature_verified,
        signed_at_ms,
        source: AuditEndpoint {
            ip: ctx.peer.ip().to_string(),
            port: ctx.peer.port(),
        },
        reason: event.reason.to_owned(),
        time_ms: crate::time_util::now_ms(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use assert2::{assert, check};
    use krabka_security::Principal;

    use super::*;

    pub(crate) fn principal(name: &str) -> Principal {
        crate::test_support::sasl_principal(name)
    }

    pub(crate) fn peer() -> std::net::SocketAddr {
        "10.0.0.7:51120".parse().expect("a loopback peer address")
    }

    pub(crate) fn context<'a>(
        principal: &'a Principal,
        peer: &'a std::net::SocketAddr,
    ) -> RequestContext<'a> {
        RequestContext::new(
            principal,
            peer,
            "krabka-guard",
            "test-connection",
            false,
            "PLAINTEXT",
        )
    }

    pub(crate) fn audit_channel() -> (std::sync::Arc<AuditLog>, krabka_audit::AuditReceiver) {
        AuditLog::new(8)
    }

    #[test]
    fn a_connection_principal_takes_its_kafka_string_form() {
        let principal = principal("alice");
        let peer = peer();

        check!(principal_name(&context(&principal, &peer)) == "User:alice");
    }

    #[test]
    fn a_proposal_id_round_trips_through_its_wire_form() {
        let id = Uuid::from_u128(0x0BAD_C0FF_EE00_4000_8000_0102_0304_0506);

        check!(from_wire_uuid(to_wire_uuid(id)) == id);
        check!(to_wire_uuid(id).0 == *id.as_bytes());
    }

    #[test]
    fn a_submit_failure_names_the_code_of_its_cause() {
        let cases = [
            (
                "not the leader",
                RaftError::NotLeader {
                    current_leader: None,
                },
                codes::NOT_CONTROLLER,
            ),
            (
                "no leader known",
                RaftError::LeaderUnknown,
                codes::NOT_CONTROLLER,
            ),
            (
                "the leader has an uncommitted tail",
                RaftError::UncommittedTail,
                codes::NOT_CONTROLLER,
            ),
            (
                "the quorum refused the record",
                RaftError::ChangeRejected("append timed out".to_owned()),
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
        ];
        for (label, error, expected) in cases {
            let (code, text) = submit_error(&error);
            check!(code == expected, "case {label}");
            check!(!text.is_empty(), "case {label}");
        }
    }

    #[tokio::test]
    async fn an_audit_event_carries_both_people_and_the_key_material() {
        let principal = principal("bob");
        let peer = peer();
        let ctx = context(&principal, &peer);
        let (audit_log, mut events) = audit_channel();
        let counterparties = ["User:carol".to_owned()];

        audit_privileged(
            &audit_log,
            &ctx,
            "fingerprint".to_owned(),
            &PrivilegedAudit {
                outcome: AuditOutcome::Success,
                phase: PrivilegedPhase::Approved,
                action: "delete_topic",
                target: "doomed",
                proposal_id: Some(Uuid::from_u128(1)),
                counterparties: &counterparties,
                key_id: "bob-yubi",
                signature: &[7; 4],
                signature_verified: true,
                reason: "incident 42",
            },
        );

        assert!(let Some(event) = events.recv().await);
        assert!(let
            AuditEvent::PrivilegedAction {
                outcome,
                phase,
                action,
                target,
                proposal_id,
                principal,
                counterparties,
                approver_set_fingerprint,
                key_id,
                signature,
                signature_verified,
                source,
                reason,
                ..
            } = event
        );
        check!(outcome == AuditOutcome::Success);
        check!(phase == PrivilegedPhase::Approved);
        check!(action == "delete_topic");
        check!(target == "doomed");
        check!(proposal_id == Uuid::from_u128(1).to_string());
        check!(principal.name == "User:bob");
        check!(
            counterparties
                .iter()
                .map(|p| p.name.clone())
                .collect::<Vec<_>>()
                == ["User:carol"]
        );
        check!(approver_set_fingerprint == "fingerprint");
        check!(key_id == "bob-yubi");
        check!(signature == vec![7; 4]);
        check!(signature_verified);
        check!(source.ip == "10.0.0.7");
        check!(reason == "incident 42");
    }

    #[tokio::test]
    async fn an_event_with_no_proposal_carries_an_empty_id() {
        let principal = principal("alice");
        let peer = peer();
        let ctx = context(&principal, &peer);
        let (audit_log, mut events) = audit_channel();

        audit_privileged(
            &audit_log,
            &ctx,
            String::new(),
            &PrivilegedAudit {
                outcome: AuditOutcome::Failure,
                phase: PrivilegedPhase::Refused,
                action: "thaw_topic_freeze",
                target: "literal:orders",
                proposal_id: None,
                counterparties: &[],
                key_id: "",
                signature: &[],
                signature_verified: false,
                reason: "no approved proposal covers the request",
            },
        );

        assert!(let Some(event) = events.recv().await);
        assert!(let
            AuditEvent::PrivilegedAction {
                proposal_id,
                counterparties,
                signature,
                ..
            } = event
        );
        check!(proposal_id.is_empty());
        check!(counterparties.is_empty());
        check!(signature.is_empty());
    }
}
