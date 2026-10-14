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

#[yare::parameterized(
    signing_key_and_bound_principal = {
        "alice-yubi",
        "User:alice",
        b"krabka-topic-freeze-v1\0orders".as_slice(),
        true,
    },
    wrong_principal = {
        "alice-yubi",
        "User:bob",
        b"krabka-topic-freeze-v1\0orders".as_slice(),
        false,
    },
    different_signing_key = {
        "bob-yubi",
        "User:bob",
        b"krabka-topic-freeze-v1\0orders".as_slice(),
        false,
    },
    unconfigured_key = {
        "carol-yubi",
        "User:carol",
        b"krabka-topic-freeze-v1\0orders".as_slice(),
        false,
    },
    tampered_message = {
        "alice-yubi",
        "User:alice",
        b"krabka-topic-freeze-v1\0ordering".as_slice(),
        false,
    },
)]
fn verify_accepts_only_the_bound_principal_and_the_signing_key(
    key_id: &str,
    principal: &str,
    msg: &[u8],
    expected: bool,
) {
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

    check!(keys.verify(key_id, principal, msg, &signature) == expected);
}
