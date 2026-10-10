//! Tests for the detached operator signature that an approval can carry.
//!
//! A configured action refuses an unsigned approval, and every signature the
//! broker stores is one it verified against the trusted key set under the
//! approving principal.

use assert2::{assert, check};
use krabka_metadata::BreakGlassProposalRecord;
use ring::signature::Ed25519KeyPair;
use tempfile::TempDir;

use super::{attempt, pending};
use crate::{
    break_glass::{
        config::BreakGlassPolicy,
        gate::tests::config,
        handlers::approve::{Attempt, decide},
        signing::approval_signing_bytes,
    },
    codes,
    config::BreakGlassConfig,
    operator_keys::{OperatorKeyEntry, OperatorKeys},
};

// An operator key bound to `principal`, plus the signer for it.
fn operator_key(
    dir: &TempDir,
    key_id: &str,
    principal: &str,
) -> (Ed25519KeyPair, OperatorKeyEntry) {
    let (pair, path) = crate::test_support::ed25519_public_key_file(
        dir.path(),
        crate::test_support::OperatorKeyFileSetup {
            name: &format!("{key_id}.pub"),
            context: "write key file",
        },
    );
    (
        pair,
        OperatorKeyEntry {
            key_id: key_id.to_owned(),
            principal: principal.to_owned(),
            public_key_path: path,
        },
    )
}

#[test]
fn an_action_that_needs_a_signature_refuses_an_unsigned_approval() {
    let config = BreakGlassConfig {
        signed_actions: vec!["delete_topic".to_owned()],
        ..config()
    };
    let policy = BreakGlassPolicy::new(&config);

    let outcome = decide(
        policy,
        &OperatorKeys::default(),
        &pending(),
        &attempt("User:bob"),
    );

    assert!(let Err(refusal) = outcome);
    check!(refusal.code == codes::OPERATOR_SIGNATURE_REQUIRED);
}

#[test]
fn a_signature_verifies_against_the_bound_principal_and_the_signed_bytes() {
    let dir = TempDir::new().expect("tempdir");
    let (bob, bob_entry) = operator_key(&dir, "bob-yubi", "User:bob");
    let (_, carol_entry) = operator_key(&dir, "carol-yubi", "User:carol");
    let keys = OperatorKeys::load(&[bob_entry, carol_entry]).expect("load the trust set");
    let config = BreakGlassConfig {
        signed_actions: vec!["delete_topic".to_owned()],
        ..config()
    };
    let policy = BreakGlassPolicy::new(&config);
    let stored = pending();
    let good = bob.sign(&approval_signing_bytes(&stored)).as_ref().to_vec();
    let other = bob
        .sign(&approval_signing_bytes(&BreakGlassProposalRecord {
            target: "another-topic".to_owned(),
            ..stored.clone()
        }))
        .as_ref()
        .to_vec();
    let cases = [
        (
            "bob's own signature",
            "User:bob",
            "bob-yubi",
            good.clone(),
            None,
        ),
        (
            "bob's key under carol's name",
            "User:carol",
            "bob-yubi",
            good.clone(),
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
        (
            "carol's key over bob's signature",
            "User:carol",
            "carol-yubi",
            good.clone(),
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
        (
            "a key that is not in the trust set",
            "User:bob",
            "mallory-yubi",
            good,
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
        (
            "a signature over another proposal",
            "User:bob",
            "bob-yubi",
            other,
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
    ];
    for (label, principal, key_id, signature, expected) in cases {
        let outcome = decide(
            policy,
            &keys,
            &stored,
            &Attempt {
                key_id,
                signature: &signature,
                ..attempt(principal)
            },
        );
        match expected {
            None => {
                assert!(let Ok(updated) = outcome, "case {label}");
                check!(updated.approvals[0].key_id == key_id, "case {label}");
                check!(!updated.approvals[0].signature.is_empty(), "case {label}");
            }
            Some(code) => {
                assert!(let Err(refusal) = outcome, "case {label}");
                check!(refusal.code == code, "case {label}");
            }
        }
    }
}

#[test]
fn a_signature_on_an_action_that_needs_none_is_still_verified() {
    let dir = TempDir::new().expect("tempdir");
    let (bob, bob_entry) = operator_key(&dir, "bob-yubi", "User:bob");
    let keys = OperatorKeys::load(&[bob_entry]).expect("load the trust set");
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = pending();
    let signature = bob.sign(&approval_signing_bytes(&stored)).as_ref().to_vec();

    let accepted = decide(
        policy,
        &keys,
        &stored,
        &Attempt {
            key_id: "bob-yubi",
            signature: &signature,
            ..attempt("User:bob")
        },
    );
    let refused = decide(
        policy,
        &keys,
        &stored,
        &Attempt {
            key_id: "bob-yubi",
            signature: &[0; 64],
            ..attempt("User:bob")
        },
    );

    check!(accepted.is_ok());
    assert!(let Err(refusal) = refused);
    check!(refusal.code == codes::OPERATOR_SIGNATURE_INVALID);
}
