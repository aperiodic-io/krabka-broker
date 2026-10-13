//! The canonical freeze-signing bytes, rebuilt outside the broker.
//!
//! `krabka_broker::freeze::signing::freeze_signing_bytes` is `pub(crate)`
//! inside a `pub(crate)` module, so this suite carries its own copy of the
//! layout that `crates/broker/src/freeze/signing.rs` documents.
//! [`signed_request`] signs with it the way `krabka-guard --sign-with` does,
//! and [`verifies_locally`] re-checks a registry entry with it on the reader's
//! own machine. A drift between the two layouts fails both directions at once.

use {
    crate::support::OperatorKey,
    krabka_broker::operator_keys::freeze_signing_input,
    krabka_protocol::{
        krabka::freeze::{DescribedTopicFreeze, SetTopicFreezeRequest},
        primitives::uuid::Uuid as WireUuid,
    },
};

/// Domain separator for a freeze-record signature.
///
/// It is the value `crate::signing_domains::FREEZE_DOMAIN` holds inside the
/// broker. That constant is `pub(crate)`, so this suite carries the literal.
const FREEZE_DOMAIN: &[u8] = b"krabka-topic-freeze-v1\0";

/// Rebuild the canonical bytes that the broker verifies against.
///
/// The layout is the one `crates/broker/src/freeze/signing.rs` documents:
/// the domain separator, then `cluster_id`, `pattern_type`, `scope`, `frozen`,
/// `reason`, `set_by`, `set_at_ms` and `proposal_id`, with every variable field
/// behind a `u32` big-endian length. The length prefixes are what stop a scope
/// of `"a"` with a reason of `"bc"` from signing the same bytes as a scope of
/// `"ab"` with a reason of `"c"`.
fn signing_bytes(input: &krabka_broker::operator_keys::FreezeSigningInput<'_>) -> Vec<u8> {
    fn put(bytes: &mut Vec<u8>, field: &[u8]) {
        let len = u32::try_from(field.len()).expect("a field inside u32");
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(field);
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(FREEZE_DOMAIN);
    put(&mut bytes, input.cluster_id.as_bytes());
    bytes.push(input.pattern_type.to_be_bytes()[0]);
    put(&mut bytes, input.scope.as_bytes());
    bytes.push(u8::from(input.frozen));
    put(&mut bytes, input.reason.as_bytes());
    put(&mut bytes, input.set_by.as_bytes());
    bytes.extend_from_slice(&input.set_at_ms.to_be_bytes());
    bytes.extend_from_slice(&input.proposal_id);
    bytes
}

/// Everything a signed `SetTopicFreeze` needs beyond the record itself.
pub(super) struct SignedFreeze<'a> {
    pub(super) key: &'a OperatorKey,
    pub(super) cluster_id: &'a str,
    pub(super) pattern_type: i8,
    pub(super) scope: &'a str,
    pub(super) frozen: bool,
    pub(super) reason: &'a str,
    pub(super) set_at_ms: i64,
    pub(super) proposal_id: uuid::Uuid,
}

/// Sign a freeze or a thaw on the caller's own machine, exactly as
/// `krabka-guard --sign-with` does: the private key never reaches the broker,
/// and only the `key_id` and the detached signature go on the wire.
pub(super) fn signed_request(signed: &SignedFreeze<'_>) -> SetTopicFreezeRequest {
    let proposal_id = *signed.proposal_id.as_bytes();
    let bytes = signing_bytes(&krabka_broker::operator_keys::FreezeSigningInput {
        cluster_id: signed.cluster_id,
        pattern_type: signed.pattern_type,
        scope: signed.scope,
        frozen: signed.frozen,
        reason: signed.reason,
        set_by: &signed.key.principal,
        set_at_ms: signed.set_at_ms,
        proposal_id,
    });
    SetTopicFreezeRequest {
        scope: signed.scope.to_owned(),
        pattern_type: signed.pattern_type,
        frozen: signed.frozen,
        reason: signed.reason.to_owned(),
        proposal_id: WireUuid(proposal_id),
        set_at_ms: signed.set_at_ms,
        key_id: signed.key.key_id.clone(),
        signature: signed.key.pair().sign(&bytes).as_ref().to_vec(),
        ..SetTopicFreezeRequest::default()
    }
}

/// Re-verify a registry entry's signature the way `freeze list
/// --verify-signatures` does: on the reader's own machine, against the operator
/// public key, with no trust in the broker that served it.
pub(super) fn verifies_locally(
    key: &OperatorKey,
    cluster_id: &str,
    entry: &DescribedTopicFreeze,
) -> bool {
    let public = std::fs::read(&key.public_path).expect("read the operator public key");
    let bytes = signing_bytes(
        &freeze_signing_input()
            .cluster_id(cluster_id)
            .entry(entry)
            .frozen(true)
            .call(),
    );
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public)
        .verify(&bytes, &entry.signature)
        .is_ok()
}
