//! OCSF serialization for audit events.
//!
//! OCSF is the Open Cybersecurity Schema Framework.
//!
//! [`to_ocsf`] is the one entry point. It matches an [`AuditEvent`] variant and
//! hands it to the child module that builds the OCSF record class for that
//! variant, so each class's field names and id numbers live in exactly one
//! place. [`ProductInfo`] names the emitting product in every record.

use {
    self::{
        api_activity::{PrivilegedFields, ocsf_admin_operation, ocsf_privileged_action},
        authentication::ocsf_authentication,
        authorization::ocsf_authorization_denied,
        lifecycle::ocsf_lifecycle,
    },
    crate::{event::AuditEvent, ids::EpochMs},
    krabka_ids::NodeId,
};

mod api_activity;
mod authentication;
mod authorization;
mod envelope;
mod lifecycle;

#[cfg(test)]
mod tests;

/// Product identity for every OCSF record's `metadata` field.
#[derive(Debug, Clone)]
pub struct ProductInfo {
    pub vendor_name: String,
    pub name: String,
    pub version: String,
}

/// Serialize an [`AuditEvent`] to an OCSF JSON object.
#[must_use]
pub fn to_ocsf(event: &AuditEvent, product: &ProductInfo) -> serde_json::Value {
    match event {
        AuditEvent::Authentication {
            outcome,
            mechanism,
            principal,
            source,
            reason,
            time_ms,
        } => ocsf_authentication(
            *outcome,
            mechanism,
            principal,
            source,
            reason.as_ref(),
            *time_ms,
            product,
        ),
        AuditEvent::AuthorizationDenied {
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            time_ms,
        } => ocsf_authorization_denied(
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            *time_ms,
            product,
        ),
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms,
        } => ocsf_admin_operation(
            *outcome, principal, source, operation, resources, *time_ms, product,
        ),
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
            signed_at_ms,
            source,
            reason,
            time_ms,
        } => ocsf_privileged_action(
            &PrivilegedFields {
                outcome: *outcome,
                phase: *phase,
                action,
                target,
                proposal_id,
                principal,
                counterparties,
                approver_set_fingerprint,
                key_id,
                signature,
                signature_verified: *signature_verified,
                signed_at_ms: *signed_at_ms,
                source,
                reason,
                time_ms: *time_ms,
            },
            product,
        ),
        AuditEvent::Lifecycle {
            kind,
            node_id,
            time_ms,
        } => ocsf_lifecycle(
            *kind,
            // `node_id` is the Kafka `broker.id` (an `int32` widened to `i64`
            // at the broker boundary), always non-negative; wrap it into the
            // canonical `u64` `NodeId`.
            NodeId(u64::try_from(*node_id).unwrap_or(0)),
            EpochMs(*time_ms),
            product,
        ),
    }
}
