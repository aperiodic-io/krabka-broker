//! Internal audit event model.
//!
//! This module is the source of truth for the KSI-MLA-LET catalog.

use serde::Serialize;

/// Outcome of an audited action.
///
/// `as_str` is the lowercase status the audit sink records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, krabka_macros::EnumStr)]
#[enum_str(case = "lowercase")]
pub enum AuditOutcome {
    Success,
    Failure,
}

/// The actor responsible for an audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditPrincipal {
    pub name: String,
    pub auth_method: String,
}

/// Network source of the action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEndpoint {
    pub ip: String,
    pub port: u16,
}

/// A resource affected by an admin operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditResource {
    pub resource_type: String,
    pub name: String,
}

/// Broker lifecycle transitions worth auditing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LifecycleKind {
    BrokerStarted,
    BrokerStopping,
    ConfigApplied,
    TlsReloaded,
}

/// Stage of a privileged action that the audit record captures.
///
/// `Attempted` is the durable write-ahead admission record. A freeze then
/// reaches `Applied` directly. A two-person action walks `Proposed` ->
/// `Approved` -> `Consumed` -> `Applied`. `Refused` records a gate that fell
/// closed, and `Bypassed` records a gated action that ran without an approval.
///
/// `as_name` is the stable lowercase name for the OCSF body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, krabka_macros::EnumStr)]
#[enum_str(case = "lowercase", as_str = as_name, parse = from_name)]
pub enum PrivilegedPhase {
    Attempted,
    Proposed,
    Approved,
    Consumed,
    Applied,
    Refused,
    Bypassed,
}

/// OCSF class group for record headers and routing.
///
/// `as_header` is the stable lowercase identifier for the `event_class` record
/// header value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", as_str = as_header, parse = from_header)]
pub enum AuditEventClass {
    Authentication,
    Authorization,
    ApiActivity,
    ApplicationLifecycle,
    /// Internal meta-record for a signed chain checkpoint. It is not an OCSF event.
    Checkpoint,
    /// Internal meta-record that declares audit records were lost. It is not an OCSF event.
    RecordsLost,
}

impl AuditEventClass {
    /// Compact tag for the spool frame format.
    #[must_use]
    pub fn tag(self) -> u8 {
        match self {
            AuditEventClass::Authentication => 0,
            AuditEventClass::Authorization => 1,
            AuditEventClass::ApiActivity => 2,
            AuditEventClass::ApplicationLifecycle => 3,
            AuditEventClass::Checkpoint => 4,
            AuditEventClass::RecordsLost => 5,
        }
    }

    /// Inverse of [`Self::tag`].
    #[must_use]
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(AuditEventClass::Authentication),
            1 => Some(AuditEventClass::Authorization),
            2 => Some(AuditEventClass::ApiActivity),
            3 => Some(AuditEventClass::ApplicationLifecycle),
            4 => Some(AuditEventClass::Checkpoint),
            5 => Some(AuditEventClass::RecordsLost),
            _ => None,
        }
    }
}

/// A single auditable security event.
///
/// The caller supplies the times as epoch-millis, so the crate stays pure and
/// deterministically testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditEvent {
    Authentication {
        outcome: AuditOutcome,
        mechanism: String,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        reason: Option<String>,
        time_ms: i64,
    },
    AuthorizationDenied {
        principal: AuditPrincipal,
        source: AuditEndpoint,
        resource_type: String,
        resource_name: String,
        operation: String,
        time_ms: i64,
    },
    AdminOperation {
        outcome: AuditOutcome,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        operation: String,
        resources: Vec<AuditResource>,
        time_ms: i64,
    },
    /// A privileged action, with the evidence that authorised it.
    ///
    /// A topic freeze, a thaw, an unclean leader election, and every other
    /// action behind the two-person rule write this variant. It is not named
    /// for break-glass, because a freeze is not a break-glass act and still
    /// carries the same evidence.
    ///
    /// The record keeps the detached `signature` itself, not only
    /// `signature_verified`. The audit log is hash-chained and its checkpoints
    /// are Ed25519-signed, so an auditor who trusts the audit chain and holds
    /// the operator public keys can re-verify who set every freeze from the
    /// audit topic alone, with no broker and no raft log. That is a second,
    /// independent copy of the proof.
    ///
    /// `proposal_id` is empty when the action needed no proposal, such as a
    /// freeze. `counterparties` holds the approvers and is empty for the same
    /// case. `key_id` and `signature` are empty when the action was unsigned.
    /// `approver_set_fingerprint` is the SHA-256 hex of the sorted configured
    /// approver list, evaluated at approval time, so a later divergence of
    /// that list is visible after the fact.
    PrivilegedAction {
        outcome: AuditOutcome,
        phase: PrivilegedPhase,
        action: String,
        target: String,
        proposal_id: String,
        principal: AuditPrincipal,
        counterparties: Vec<AuditPrincipal>,
        approver_set_fingerprint: String,
        key_id: String,
        signature: Vec<u8>,
        signature_verified: bool,
        /// The timestamp that the operator's detached signature covers.
        ///
        /// For a topic freeze this is the record's `set_at_ms`, which sits
        /// inside the signed preimage. It is carried here because `time_ms` is
        /// taken when the event is emitted, which is a different instant: an
        /// auditor rebuilding the preimage from this record needs the stamp the
        /// operator signed, not the one the broker logged.
        ///
        /// `0` where the event carries no signature over a timestamp, which is
        /// every unsigned action and every break-glass act -- a break-glass
        /// approval signs the proposal's `created_at_ms` and `expires_at_ms`
        /// instead, and neither is a single stamp this field could hold.
        signed_at_ms: i64,
        source: AuditEndpoint,
        reason: String,
        time_ms: i64,
    },
    Lifecycle {
        kind: LifecycleKind,
        node_id: i64,
        time_ms: i64,
    },
}

impl AuditEvent {
    /// The OCSF class this event maps to.
    #[must_use]
    pub fn class(&self) -> AuditEventClass {
        match self {
            AuditEvent::Authentication { .. } => AuditEventClass::Authentication,
            AuditEvent::AuthorizationDenied { .. } => AuditEventClass::Authorization,
            AuditEvent::AdminOperation { .. } | AuditEvent::PrivilegedAction { .. } => {
                AuditEventClass::ApiActivity
            }
            AuditEvent::Lifecycle { .. } => AuditEventClass::ApplicationLifecycle,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::test_support::{admin, authentication, denied, privileged_freeze};

    #[test]
    fn class_tag_round_trips_and_header_maps() {
        for c in [
            AuditEventClass::Authentication,
            AuditEventClass::Authorization,
            AuditEventClass::ApiActivity,
            AuditEventClass::ApplicationLifecycle,
            AuditEventClass::Checkpoint,
            AuditEventClass::RecordsLost,
        ] {
            check!(AuditEventClass::from_tag(c.tag()) == Some(c));
            check!(AuditEventClass::from_header(c.as_header()) == Some(c));
        }
        check!(AuditEventClass::from_tag(99) == None);
        check!(AuditEventClass::from_header("nope") == None);
    }

    #[test]
    fn event_class_maps_each_variant() {
        let authn = authentication(51120, Some("authentication failed"), 1_700_000_000_000);
        let denied = denied(1);
        let admin = admin(2);
        let privileged = privileged_freeze(vec![0xde, 0xad], 3, 4);
        let life = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 3,
        };
        check!(
            (
                authn.class(),
                denied.class(),
                admin.class(),
                privileged.class(),
                life.class()
            ) == (
                AuditEventClass::Authentication,
                AuditEventClass::Authorization,
                AuditEventClass::ApiActivity,
                AuditEventClass::ApiActivity,
                AuditEventClass::ApplicationLifecycle,
            )
        );
    }

    #[test]
    fn privileged_phase_name_round_trips() {
        for (label, phase, name) in [
            ("attempted", PrivilegedPhase::Attempted, "attempted"),
            ("proposed", PrivilegedPhase::Proposed, "proposed"),
            ("approved", PrivilegedPhase::Approved, "approved"),
            ("consumed", PrivilegedPhase::Consumed, "consumed"),
            ("applied", PrivilegedPhase::Applied, "applied"),
            ("refused", PrivilegedPhase::Refused, "refused"),
            ("bypassed", PrivilegedPhase::Bypassed, "bypassed"),
        ] {
            check!(phase.as_name() == name, "case {label}");
            check!(
                PrivilegedPhase::from_name(phase.as_name()) == Some(phase),
                "case {label}"
            );
        }
        check!(PrivilegedPhase::from_name("nope") == None);
    }
}
