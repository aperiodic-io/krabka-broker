//! Tests for the signature check that the trust set runs.
//!
//! A signature verifies only under the key that made it, only for the
//! principal that key speaks for, and only over the message that was signed.

use {
    super::{entry, fresh_key, write_key_file},
    crate::operator_keys::OperatorKeys,
    assert2::check,
    tempfile::TempDir,
};

#[test]
fn verify_accepts_only_the_bound_principal_and_the_signing_key() {
    let dir = TempDir::new().expect("tempdir");
    let (alice, alice_public) = fresh_key();
    let (_, bob_public) = fresh_key();
    let keys = OperatorKeys::load(&[
        entry(
            "alice-yubi",
            "User:alice",
            write_key_file(&dir, "alice.pub", &alice_public),
        ),
        entry(
            "bob-yubi",
            "User:bob",
            write_key_file(&dir, "bob.pub", &bob_public),
        ),
    ])
    .expect("load trust set");
    let message = b"krabka-topic-freeze-v1\0orders";
    let signature = alice.sign(message).as_ref().to_vec();

    for (name, key_id, principal, msg, expected) in [
        (
            "alice's signature under alice's key and principal",
            "alice-yubi",
            "User:alice",
            message.as_slice(),
            true,
        ),
        (
            "alice's key claiming bob",
            "alice-yubi",
            "User:bob",
            message.as_slice(),
            false,
        ),
        (
            "bob's key over alice's signature",
            "bob-yubi",
            "User:bob",
            message.as_slice(),
            false,
        ),
        (
            "an unconfigured key_id",
            "carol-yubi",
            "User:carol",
            message.as_slice(),
            false,
        ),
        (
            "a tampered message",
            "alice-yubi",
            "User:alice",
            b"krabka-topic-freeze-v1\0ordering".as_slice(),
            false,
        ),
    ] {
        check!(
            keys.verify(key_id, principal, msg, &signature) == expected,
            "case {name}"
        );
    }
}
