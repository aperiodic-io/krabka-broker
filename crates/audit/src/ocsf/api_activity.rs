//! The OCSF API Activity records (class 6003), which report an operation the
//! broker performed on a client's behalf. Two audit events map here: a plain
//! admin operation, and a privileged action, whose body additionally carries
//! the two-person-control evidence -- the proposal, the approvers, and the
//! signature over the request.

use {
    super::{
        ProductInfo,
        envelope::{metadata, status_id},
    },
    crate::{
        chain::to_hex,
        event::{AuditEndpoint, AuditOutcome, AuditPrincipal, AuditResource, PrivilegedPhase},
    },
    serde_json::json,
};

pub(super) fn ocsf_admin_operation(
    outcome: AuditOutcome,
    principal: &AuditPrincipal,
    source: &AuditEndpoint,
    operation: &str,
    resources: &[AuditResource],
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown/Other (operation in `api`).
    let class_uid = 6003_i64;
    let activity_id = 0_i64;
    let res: Vec<serde_json::Value> = resources
        .iter()
        .map(|r| json!({ "type": r.resource_type, "name": r.name }))
        .collect();
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "time": time_ms,
        "status_id": status_id(outcome),
        "api": { "operation": operation, "service": { "name": "kafka" } },
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": res,
        "metadata": metadata(product),
    })
}

/// One principal in the `{ "name": …, "type": … }` shape the OCSF bodies use
/// for `actor.user`. The privileged-action body needs it for the actor and for
/// every counterparty, so the shape lives in one place.
fn user(principal: &AuditPrincipal) -> serde_json::Value {
    json!({ "name": principal.name, "type": principal.auth_method })
}

/// Borrowed view of the [`AuditEvent::PrivilegedAction`] fields.
///
/// The variant carries more fields than an argument list reads well with, so
/// the dispatch arm groups them here.
///
/// [`AuditEvent::PrivilegedAction`]: crate::event::AuditEvent::PrivilegedAction
pub(super) struct PrivilegedFields<'a> {
    pub(super) outcome: AuditOutcome,
    pub(super) phase: PrivilegedPhase,
    pub(super) action: &'a str,
    pub(super) target: &'a str,
    pub(super) proposal_id: &'a str,
    pub(super) principal: &'a AuditPrincipal,
    pub(super) counterparties: &'a [AuditPrincipal],
    pub(super) approver_set_fingerprint: &'a str,
    pub(super) key_id: &'a str,
    pub(super) signature: &'a [u8],
    pub(super) signature_verified: bool,
    pub(super) signed_at_ms: i64,
    pub(super) source: &'a AuditEndpoint,
    pub(super) reason: &'a str,
    pub(super) time_ms: i64,
}

pub(super) fn ocsf_privileged_action(
    f: &PrivilegedFields<'_>,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown/Other (operation in `api`).
    let class_uid = 6003_i64;
    let activity_id = 0_i64;
    let counterparties: Vec<serde_json::Value> = f.counterparties.iter().map(user).collect();
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "time": f.time_ms,
        "status_id": status_id(f.outcome),
        "status_detail": f.reason,
        "api": {
            "operation": format!("{}.{}", f.action, f.phase.as_name()),
            "service": { "name": "kafka" },
        },
        "actor": { "user": user(f.principal) },
        "src_endpoint": { "ip": f.source.ip, "port": f.source.port },
        "privileged_action": {
            "phase": f.phase.as_name(),
            "action": f.action,
            "target": f.target,
            "proposal_id": f.proposal_id,
            "counterparties": counterparties,
            "approver_set_fingerprint": f.approver_set_fingerprint,
            "key_id": f.key_id,
            // Lowercase hex, as `Checkpoint::to_record` encodes its signature
            // and public key. An auditor re-verifies the signature from this
            // field alone, so the encoding must stay the crate's one encoding.
            "signature": to_hex(f.signature),
            "signature_verified": f.signature_verified,
            // The stamp the signature covers, which is not `time` above. An
            // auditor rebuilding the signed preimage reads it from here.
            "signed_at_ms": f.signed_at_ms,
        },
        "metadata": metadata(product),
    })
}
