//! The Ed25519 attestation over a manifest's chain head.
//!
//! `manifest_signing_bytes` fixes what a signature covers, the writer signs
//! it, and `verify_manifest_signature` recomputes the head and checks the
//! claim. The signature is a sibling of the body, never a member of it.

use {
    super::{
        ChainHead, EpochId, HexBytes, ManifestSeq, SegmentManifest, encoding::push_bytes,
        manifest_head,
    },
    krabka_audit::signing::verify_signature,
    serde::{Deserialize, Serialize},
};

/// Domain separation for the manifest signature. Distinct from
/// [`krabka_audit::signing::CHECKPOINT_DOMAIN`] — never share a domain
/// across signature purposes.
pub const MANIFEST_DOMAIN: &[u8] = b"krabka-worm-manifest-v1\0";

/// Ed25519 signature over one manifest's chain head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSignature {
    /// Identifier of the key that produced the signature.
    pub key_id: String,
    /// Raw Ed25519 public key of that key.
    pub public_key: HexBytes,
    /// Signature over [`manifest_signing_bytes`].
    pub signature: HexBytes,
}

/// Canonical signed payload for a manifest.
///
/// The layout is
/// `MANIFEST_DOMAIN ‖ kid_len(u64 BE) ‖ kid ‖ epoch_id(16) ‖ seq(u64 BE) ‖ head(32)`.
/// The writer calls it to sign and the verifier calls it to verify.
#[must_use]
pub fn manifest_signing_bytes(
    key_id: &str,
    epoch_id: EpochId,
    seq: ManifestSeq,
    head: ChainHead,
) -> Vec<u8> {
    let kid = key_id.as_bytes();
    let mut out = Vec::with_capacity(MANIFEST_DOMAIN.len() + 8 + kid.len() + 16 + 8 + 32);
    out.extend_from_slice(MANIFEST_DOMAIN);
    push_bytes(&mut out, kid);
    out.extend_from_slice(epoch_id.0.as_bytes());
    out.extend_from_slice(&seq.0.to_be_bytes());
    out.extend_from_slice(&head.0);
    out
}

/// Recomputes the head from the body, then checks the signature against
/// `public_key`.
///
/// Returns `false` for an unsigned manifest. A caller that treats "unsigned"
/// and "bad signature" differently must test `manifest.signature.is_none()`
/// first.
#[must_use]
pub fn verify_manifest_signature(manifest: &SegmentManifest, public_key: &[u8]) -> bool {
    let Some(signature) = manifest.signature.as_ref() else {
        return false;
    };
    let head = manifest_head(&manifest.body);
    let message = manifest_signing_bytes(
        &signature.key_id,
        manifest.body.chain.epoch_id,
        manifest.body.chain.seq,
        head,
    );
    verify_signature(public_key, &message, &signature.signature.0)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::manifest::{
            MANIFEST_BODY_DOMAIN, ManifestBody, Sha256Digest, canonical_manifest_bytes,
            test_support::{KEY_ID, sample_body},
        },
        assert2::check,
        krabka_audit::{
            ids::{EpochMs, Seq},
            signing::{CHECKPOINT_DOMAIN, FileEd25519Signer, checkpoint_signing_bytes},
        },
        ring::{rand::SystemRandom, signature::Ed25519KeyPair},
        uuid::Uuid,
    };

    struct FreshSigner {
        signer: FileEd25519Signer,
        public_key: Vec<u8>,
    }

    fn fresh_signer(key_id: &str) -> FreshSigner {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer =
            FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_string()).unwrap();
        let public_key = signer.public_key();
        FreshSigner { signer, public_key }
    }

    fn sign_manifest(body: ManifestBody, signer: &FileEd25519Signer) -> SegmentManifest {
        let head = manifest_head(&body);
        let message =
            manifest_signing_bytes(signer.key_id(), body.chain.epoch_id, body.chain.seq, head);
        SegmentManifest {
            signature: Some(ManifestSignature {
                key_id: signer.key_id().to_string(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }),
            body,
        }
    }

    #[test]
    fn manifest_signature_round_trips_and_rejects_tampering() {
        let FreshSigner { signer, public_key } = fresh_signer(KEY_ID);
        let base = sample_body();
        let authentic = sign_manifest(base.clone(), &signer);

        let json = serde_json::to_string(&authentic).unwrap();
        let reparsed: SegmentManifest = serde_json::from_str(&json).unwrap();
        check!(reparsed == authentic, "signature survives JSON");

        let mut tampered_body = authentic.clone();
        tampered_body.body.objects[0].sha256 = Sha256Digest::of(b"swapped body");

        let mut tampered_seq = authentic.clone();
        tampered_seq.body.chain.seq = ManifestSeq(base.chain.seq.0 + 1);

        // A signature made over a head that this body does not produce.
        let wrong_head_message = manifest_signing_bytes(
            KEY_ID,
            base.chain.epoch_id,
            base.chain.seq,
            ChainHead([0xab; 32]),
        );
        let tampered_head = SegmentManifest {
            body: base.clone(),
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(public_key.clone()),
                signature: HexBytes(signer.sign(&wrong_head_message)),
            }),
        };

        let mut tampered_key_id = authentic.clone();
        tampered_key_id.signature.as_mut().unwrap().key_id = "worm-key-2".to_string();

        let unsigned = SegmentManifest {
            body: base,
            signature: None,
        };

        for (name, manifest, expected) in [
            ("untampered", &reparsed, true),
            ("tampered body", &tampered_body, false),
            ("tampered seq", &tampered_seq, false),
            ("signature over another head", &tampered_head, false),
            ("tampered key id", &tampered_key_id, false),
            ("unsigned", &unsigned, false),
        ] {
            check!(
                verify_manifest_signature(manifest, &public_key) == expected,
                "case {name}"
            );
        }

        let FreshSigner {
            public_key: other_public_key,
            ..
        } = fresh_signer("worm-key-2");
        check!(!verify_manifest_signature(&authentic, &other_public_key));
    }

    /// Pins the exact bytes an Ed25519 signature covers.
    ///
    /// This layout is a wire contract. An auditor writing an independent
    /// verifier reads the rustdoc on [`manifest_signing_bytes`] and reproduces
    /// it, so the doc being wrong is worse than a slow encoder. Nothing else
    /// here fixes these bytes: every other row goes through sign-then-verify,
    /// which agrees with itself whatever the layout happens to be, so a drift
    /// between the documented layout and the encoder passes unnoticed. That is
    /// precisely what happened when the length prefix widened from `u32` to
    /// `u64` and the rustdoc kept saying `u32`.
    ///
    /// The expectation is assembled from its parts rather than from the
    /// function under test, so this asserts against the documented layout and
    /// not against whatever the encoder currently emits.
    #[test]
    fn manifest_signing_bytes_match_the_documented_layout() {
        let key_id = "worm-key-1";
        let epoch_id = EpochId(Uuid::from_u128(0x99));
        let seq = ManifestSeq(7);
        let head = ChainHead([0xab; 32]);

        let mut expected = Vec::new();
        expected.extend_from_slice(MANIFEST_DOMAIN);
        expected.extend_from_slice(&(key_id.len() as u64).to_be_bytes());
        expected.extend_from_slice(key_id.as_bytes());
        expected.extend_from_slice(epoch_id.0.as_bytes());
        expected.extend_from_slice(&seq.0.to_be_bytes());
        expected.extend_from_slice(&head.0);

        check!(manifest_signing_bytes(key_id, epoch_id, seq, head) == expected);
        // The widths the doc names, restated as arithmetic so a change to any
        // one of them fails here and not only in the byte comparison above.
        check!(expected.len() == MANIFEST_DOMAIN.len() + 8 + key_id.len() + 16 + 8 + 32);
    }

    #[test]
    fn manifest_domain_differs_from_audit_checkpoint_domain() {
        check!(MANIFEST_DOMAIN != CHECKPOINT_DOMAIN);
        check!(MANIFEST_BODY_DOMAIN != CHECKPOINT_DOMAIN);
        check!(MANIFEST_DOMAIN != MANIFEST_BODY_DOMAIN);

        let body = sample_body();
        check!(canonical_manifest_bytes(&body).starts_with(MANIFEST_BODY_DOMAIN));
        let head = manifest_head(&body);
        let manifest_bytes =
            manifest_signing_bytes(KEY_ID, body.chain.epoch_id, body.chain.seq, head);
        check!(manifest_bytes.starts_with(MANIFEST_DOMAIN));
        check!(!manifest_bytes.starts_with(CHECKPOINT_DOMAIN));

        // Behaviourally: an audit checkpoint signature over this very head, by
        // this very key, is not a manifest signature.
        let FreshSigner { signer, public_key } = fresh_signer(KEY_ID);
        let checkpoint_bytes =
            checkpoint_signing_bytes(KEY_ID, Seq(body.chain.seq.0), &head.0, EpochMs(0));
        let cross_purpose = SegmentManifest {
            body,
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(public_key.clone()),
                signature: HexBytes(signer.sign(&checkpoint_bytes)),
            }),
        };
        check!(!verify_manifest_signature(&cross_purpose, &public_key));
    }
}
