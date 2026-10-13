//! What one manifest's signature is worth to a verification run.
//!
//! The check is separate from the chain walk because it grades three outcomes
//! the walk treats differently: a manifest with no signature and a manifest
//! signed by an untrusted `key_id` are counted and the diagnostic walk
//! continues, but its report cannot be `ok`; a signature that fails against a
//! trusted key stops the walk.

use {
    super::TrustedManifestKeys,
    crate::worm::manifest::{SegmentManifest, verify_manifest_signature},
    krabka_verified::{WormSignatureDecision, worm_signature_decision},
};

/// What one manifest's signature is worth to this run.
pub(super) enum SignatureState {
    /// No signature at all.
    Unsigned,
    /// Signed by a `key_id` this run does not trust.
    Untrusted,
    /// Signed by a trusted key, and the signature verifies.
    Valid,
    /// Signed by a trusted key, and the signature does not verify.
    Invalid(String),
}

/// Checks one manifest's signature against the trusted key it names.
pub(super) fn signature_state(
    manifest: &SegmentManifest,
    trusted: &TrustedManifestKeys,
) -> SignatureState {
    let signature = manifest.signature.as_ref();
    let public_key = signature.and_then(|signature| trusted.get(&signature.key_id));
    let canonical_valid = signature
        .zip(public_key)
        .is_some_and(|(signature, public_key)| {
            signature.public_key.0.as_slice() == public_key
                && verify_manifest_signature(manifest, public_key)
        });
    match worm_signature_decision(signature.is_some(), public_key.is_some(), canonical_valid) {
        WormSignatureDecision::Unsigned => SignatureState::Unsigned,
        WormSignatureDecision::Untrusted => SignatureState::Untrusted,
        WormSignatureDecision::Invalid => SignatureState::Invalid(format!(
            "signature envelope or canonical binding does not verify against the trusted key `{}`",
            signature.map_or("", |signature| signature.key_id.as_str())
        )),
        WormSignatureDecision::Admit => SignatureState::Valid,
    }
}
