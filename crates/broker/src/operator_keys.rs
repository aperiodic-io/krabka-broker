//! The operator key trust set, loaded from `[[operator_keys]]`.
//!
//! Two subsystems verify detached Ed25519 signatures against one shared set of
//! operator keys: the topic write-freeze registry and the break-glass approval
//! workflow. Both reach the keys through [`OperatorKeys`], so an operator is
//! provisioned once and a signature is checked one way.
//!
//! [`OperatorKeys::load`] reads every configured public key file, so an
//! unreadable path or a malformed key stops the broker at boot instead of in
//! the middle of an incident. Verification calls
//! [`krabka_audit::signing::verify_signature`], the same code path that checks
//! an audit checkpoint, so operator key material has the shape the audit
//! checkpoint keys already have.
//!
//! This module holds no canonical-bytes builder. The freeze subsystem and the
//! break-glass subsystem each define their own signed payload, under their own
//! domain separator, and pass the finished bytes to [`OperatorKeys::verify`].

pub use self::{
    error::OperatorKeyError,
    fingerprint::approver_set_fingerprint,
    key_file::OperatorKeyEntry,
    trust_set::{OperatorKey, OperatorKeys},
};

/// The signed fields of one topic-freeze record, shared by the broker and the
/// operator CLI. Each side still encodes these fields independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreezeSigningInput<'a> {
    pub cluster_id: &'a str,
    pub pattern_type: i8,
    pub scope: &'a str,
    pub frozen: bool,
    pub reason: &'a str,
    pub set_by: &'a str,
    pub set_at_ms: i64,
    pub proposal_id: [u8; 16],
}

#[bon::builder]
pub fn freeze_signing_input<'a>(
    cluster_id: &'a str,
    entry: &'a krabka_protocol::krabka::freeze::DescribedTopicFreeze,
    frozen: bool,
) -> FreezeSigningInput<'a> {
    FreezeSigningInput {
        cluster_id,
        pattern_type: entry.pattern_type,
        scope: &entry.scope,
        frozen,
        reason: &entry.reason,
        set_by: &entry.set_by,
        set_at_ms: entry.set_at_ms,
        proposal_id: entry.proposal_id.0,
    }
}

mod error;
mod fingerprint;
mod key_file;
mod trust_set;

#[cfg(test)]
mod tests;
