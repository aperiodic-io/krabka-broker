//! The wire handlers of the write-freeze control plane (KFC-9).
//!
//! | Api key | Module | Purpose | Authorization |
//! | --- | --- | --- | --- |
//! | 1015 | [`set_freeze`] | freeze a scope, or thaw one with an approval | `Alter` on `Cluster` |
//! | 1016 | [`describe_freezes`] | read the registry | `Describe` on `Cluster` |
//!
//! Both keys sit in the krabka-private range at 1000 and above, both speak
//! version 0 only with flexible framing, and the broker registers them for
//! dispatch and never advertises them. A denied request answers
//! `CLUSTER_AUTHORIZATION_FAILED` (31), which is what every other private key
//! answers.
//!
//! # Refusals
//!
//! A refusal rides the response's own `error_code` field and never a transport
//! failure, so a caller reads one response shape whatever the outcome.
//!
//! | Code | When |
//! | --- | --- |
//! | `FREEZE_SCOPE_INVALID` (1011) | the scope is empty, or it reaches a `__` name |
//! | `FREEZE_LIMIT_EXCEEDED` (1012) | the registry already holds `freeze.max_entries` entries |
//! | `OPERATOR_SIGNATURE_REQUIRED` (1010) | the action needs a signature and the request carries none |
//! | `OPERATOR_SIGNATURE_INVALID` (1009) | a signature check failed; the message says which |
//! | `BREAK_GLASS_APPROVAL_REQUIRED` (1006) | a thaw named no approved proposal |

pub(crate) mod describe_freezes;
pub(crate) mod set_freeze;

use krabka_audit::{AuditError, AuditEvent, AuditLog, AuditMode, AuditOutcome, PrivilegedPhase};
use krabka_metadata::PatternType;
use krabka_protocol::krabka::freeze::{
    PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED,
};

// The `Describe` gate that `describe_freezes` applies. It lives in
// `crate::handlers` beside its `Alter` twin, so the freeze control plane does
// not reach into another subsystem for an ACL gate.
pub(crate) use crate::handlers::cluster_describe_denied;
use crate::{
    break_glass::handlers::{PrivilegedAudit, privileged_event},
    handlers::{DESCRIBE_TOPIC_FREEZES_API_KEY, RequestContext, SET_TOPIC_FREEZE_API_KEY},
    operator_keys::approver_set_fingerprint,
};

// The dispatch constant and the codec must name one api key. The registry
// reads the broker's constant and the framing reads the codec's, so a drift
// between the two would register a handler under a key that no message
// decodes.
const _: [(); SET_TOPIC_FREEZE_API_KEY as usize] =
    [(); krabka_protocol::krabka::freeze::set_topic_freeze::API_KEY as usize];
const _: [(); DESCRIBE_TOPIC_FREEZES_API_KEY as usize] =
    [(); krabka_protocol::krabka::freeze::describe_topic_freezes::API_KEY as usize];

/// The pattern type that a request's `pattern_type` byte names, or `None` when
/// the byte is not one this build knows.
///
/// The values are Kafka's ACL pattern types, so `kafka-acls` and a freeze use
/// one vocabulary. A request that sets a freeze needs a concrete value, which
/// `ANY` and `UNKNOWN` are not.
pub(crate) fn pattern_type_concrete(byte: i8) -> Option<PatternType> {
    match byte {
        PATTERN_TYPE_LITERAL => Some(PatternType::Literal),
        PATTERN_TYPE_PREFIXED => Some(PatternType::Prefixed),
        _ => None,
    }
}

/// The pattern type that a describe filter selects, or `None` when the filter
/// reads every pattern type.
///
/// [`PATTERN_TYPE_ANY`] and the `UNKNOWN` byte `0` both read every pattern
/// type, which is the shape an ACL filter already has. A byte this build does
/// not know matches nothing, so an unknown filter returns an empty registry
/// rather than the whole of it.
pub(crate) fn pattern_type_filter(byte: i8) -> FreezeFilter {
    const PATTERN_TYPE_UNKNOWN: i8 = 0;

    match byte {
        PATTERN_TYPE_ANY | PATTERN_TYPE_UNKNOWN => FreezeFilter::EveryPatternType,
        PATTERN_TYPE_LITERAL => FreezeFilter::One(PatternType::Literal),
        PATTERN_TYPE_PREFIXED => FreezeFilter::One(PatternType::Prefixed),
        _ => FreezeFilter::Nothing,
    }
}

/// What a describe request's `pattern_type_filter` selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreezeFilter {
    /// Every entry, whatever its pattern type.
    EveryPatternType,
    /// Entries of one pattern type.
    One(PatternType),
    /// No entry, because the filter byte names no pattern type this build
    /// knows.
    Nothing,
}

impl FreezeFilter {
    /// Whether an entry of `pattern_type` passes the filter.
    pub(crate) fn accepts(self, pattern_type: PatternType) -> bool {
        match self {
            FreezeFilter::EveryPatternType => true,
            FreezeFilter::One(wanted) => wanted == pattern_type,
            FreezeFilter::Nothing => false,
        }
    }
}

/// The evidence one write-freeze outcome puts in the audit log.
///
/// The freeze path carries the same evidence a break-glass act does, which is
/// why one audit variant covers both. `counterparties` stays empty here,
/// because a freeze has one actor and a thaw's approvers are named on the
/// proposal's own approve events.
pub(crate) struct FreezeAudit<'a> {
    /// `set_topic_freeze` for a freeze, `thaw_topic_freeze` for a thaw.
    pub action: &'static str,
    /// The scope, as `"<pattern>:<scope>"`.
    pub target: String,
    /// The proposal that authorized a thaw, or the nil uuid.
    pub proposal_id: uuid::Uuid,
    /// The operator key that signed the request, empty when unsigned.
    pub key_id: &'a str,
    /// The detached signature, empty when unsigned.
    pub signature: &'a [u8],
    /// Whether the broker verified that signature.
    pub signature_verified: bool,
    /// The `set_at_ms` the operator signed.
    ///
    /// This is inside the signed preimage, and it is not the instant the event
    /// is emitted. Carrying it is what lets an auditor holding the audit topic
    /// and the operator public keys rebuild the bytes and check the signature,
    /// which is the claim KFC-9 makes for this event.
    pub set_at_ms: i64,
    /// The operator's reason on a success, or the refusal text on a failure.
    pub reason: String,
}

/// Emit one `PrivilegedAction` event for a write-freeze outcome.
///
/// Every outcome reaches the audit log, and a refusal reaches it as surely as
/// a success. The event keeps the raw signature beside the verification
/// result, so an auditor who holds the operator public keys re-verifies who
/// set a freeze from the audit topic alone, with no broker and no metadata
/// log.
pub(crate) fn audit_freeze(
    audit_log: &AuditLog,
    ctx: &RequestContext<'_>,
    outcome: AuditOutcome,
    phase: krabka_audit::PrivilegedPhase,
    audit: &FreezeAudit<'_>,
    approvers: &[String],
) {
    audit_log.emit(freeze_event(ctx, outcome, phase, audit, approvers));
}

/// Durably record a fail-closed freeze or thaw before its raft append.
pub(crate) async fn require_freeze(
    audit_log: &AuditLog,
    ctx: &RequestContext<'_>,
    audit: &FreezeAudit<'_>,
    approvers: &[String],
) -> Result<(), AuditError> {
    if audit_log.mode() != AuditMode::FailClosed {
        return Ok(());
    }
    audit_log
        .emit_required(freeze_event(
            ctx,
            AuditOutcome::Success,
            PrivilegedPhase::Attempted,
            audit,
            approvers,
        ))
        .await
}

fn freeze_event(
    ctx: &RequestContext<'_>,
    outcome: AuditOutcome,
    phase: PrivilegedPhase,
    audit: &FreezeAudit<'_>,
    approvers: &[String],
) -> AuditEvent {
    // The principal is the Kafka form, which is what the break-glass events
    // carry. An auditor joins a freeze to the approval that authorized it by
    // principal, and two spellings of one person break that join.
    privileged_event(
        ctx,
        approver_set_fingerprint(approvers),
        &PrivilegedAudit {
            outcome,
            phase,
            action: audit.action,
            target: &audit.target,
            proposal_id: Some(audit.proposal_id),
            counterparties: &[],
            key_id: audit.key_id,
            signature: audit.signature,
            signature_verified: audit.signature_verified,
            reason: &audit.reason,
        },
        audit.set_at_ms,
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn a_concrete_pattern_type_byte_names_one_pattern_type() {
        for (label, byte, expected) in [
            ("literal", 3_i8, Some(PatternType::Literal)),
            ("prefixed", 4, Some(PatternType::Prefixed)),
            ("unknown", 0, None),
            ("any", 1, None),
            ("match", 2, None),
            ("a byte no build knows", 9, None),
            ("a negative byte", -1, None),
        ] {
            check!(pattern_type_concrete(byte) == expected, "{label}");
        }
    }

    #[test]
    fn a_filter_byte_selects_the_entries_it_names() {
        for (label, byte, expected) in [
            (
                "any reads every entry",
                1_i8,
                FreezeFilter::EveryPatternType,
            ),
            (
                "unknown reads every entry",
                0,
                FreezeFilter::EveryPatternType,
            ),
            (
                "literal reads literal entries",
                3,
                FreezeFilter::One(PatternType::Literal),
            ),
            (
                "prefixed reads prefixed entries",
                4,
                FreezeFilter::One(PatternType::Prefixed),
            ),
            (
                "a byte no build knows reads nothing",
                9,
                FreezeFilter::Nothing,
            ),
            ("a negative byte reads nothing", -1, FreezeFilter::Nothing),
        ] {
            check!(pattern_type_filter(byte) == expected, "{label}");
        }
    }

    #[test]
    fn a_filter_accepts_the_pattern_types_it_selects() {
        for (label, filter, pattern_type, expected) in [
            (
                "every pattern type takes a literal entry",
                FreezeFilter::EveryPatternType,
                PatternType::Literal,
                true,
            ),
            (
                "every pattern type takes a prefixed entry",
                FreezeFilter::EveryPatternType,
                PatternType::Prefixed,
                true,
            ),
            (
                "one pattern type takes its own",
                FreezeFilter::One(PatternType::Literal),
                PatternType::Literal,
                true,
            ),
            (
                "one pattern type refuses the other",
                FreezeFilter::One(PatternType::Literal),
                PatternType::Prefixed,
                false,
            ),
            (
                "nothing takes no literal entry",
                FreezeFilter::Nothing,
                PatternType::Literal,
                false,
            ),
            (
                "nothing takes no prefixed entry",
                FreezeFilter::Nothing,
                PatternType::Prefixed,
                false,
            ),
        ] {
            check!(filter.accepts(pattern_type) == expected, "{label}");
        }
    }
}
