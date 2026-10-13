//! Tests for the entries a trust set keeps, and the entries it refuses.
//!
//! A loaded set binds every key to one principal and finds it by `key_id`. An
//! entry that is blank, unreadable, malformed, or repeated stops the load, so
//! the broker never runs against a partial trust set.

use {
    super::{entry, fresh_key, write_key_file},
    crate::operator_keys::{OperatorKey, OperatorKeyError, OperatorKeys},
    assert2::{assert, check},
    tempfile::TempDir,
};

#[test]
fn load_binds_every_key_to_its_principal() {
    let dir = TempDir::new().expect("tempdir");
    let (_, alice_public) = fresh_key();
    let (_, bob_public) = fresh_key();
    let entries = vec![
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
    ];

    let keys = OperatorKeys::load(&entries).expect("load trust set");

    check!(keys.len() == 2);
    check!(!keys.is_empty());
    for (name, key_id, expected) in [
        (
            "alice by id",
            "alice-yubi",
            Some(("User:alice", alice_public.clone())),
        ),
        ("bob by id", "bob-yubi", Some(("User:bob", bob_public))),
        ("an unconfigured id", "carol-yubi", None),
    ] {
        let found = keys
            .get(key_id)
            .map(|key| (key.principal().to_owned(), key.public_key().to_vec()));
        let expected = expected.map(|(principal, public)| (principal.to_owned(), public.clone()));
        check!(found == expected, "case {name}");
    }
    check!(keys.get("alice-yubi").map(OperatorKey::key_id) == Some("alice-yubi"));
}

#[test]
fn an_empty_configuration_loads_an_empty_trust_set() {
    let keys = OperatorKeys::load(&[]).expect("load empty trust set");

    check!(keys.is_empty());
    check!(keys.len() == 0);
    check!(OperatorKeys::default() == keys);
}

#[test]
fn load_rejects_an_unusable_entry() {
    let dir = TempDir::new().expect("tempdir");
    let (_, public) = fresh_key();
    let good = write_key_file(&dir, "good.pub", &public);
    let short = write_key_file(&dir, "short.pub", &public[..31]);
    let long = write_key_file(&dir, "long.pub", &[public.as_slice(), b"\n"].concat());
    let missing = dir.path().join("absent.pub");

    for (name, entries, expect) in [
        (
            "a blank key_id",
            vec![entry("", "User:alice", good.clone())],
            "BlankField",
        ),
        (
            "a blank principal",
            vec![entry("alice-yubi", "  ", good.clone())],
            "BlankField",
        ),
        (
            "an unreadable public_key_path",
            vec![entry("alice-yubi", "User:alice", missing)],
            "Unreadable",
        ),
        (
            "a public key one byte short",
            vec![entry("alice-yubi", "User:alice", short)],
            "Malformed",
        ),
        (
            "a public key with a trailing newline",
            vec![entry("alice-yubi", "User:alice", long)],
            "Malformed",
        ),
        (
            "a duplicate key_id",
            vec![
                entry("alice-yubi", "User:alice", good.clone()),
                entry("alice-yubi", "User:bob", good.clone()),
            ],
            "DuplicateKeyId",
        ),
        (
            "a duplicate principal",
            vec![
                entry("alice-yubi", "User:alice", good.clone()),
                entry("alice-backup", "User:alice", good.clone()),
            ],
            "DuplicatePrincipal",
        ),
    ] {
        assert!(let Err(error) = OperatorKeys::load(&entries), "case {name}");
        let variant = match error {
            OperatorKeyError::BlankField { .. } => "BlankField",
            OperatorKeyError::Unreadable { .. } => "Unreadable",
            OperatorKeyError::Malformed { .. } => "Malformed",
            OperatorKeyError::DuplicateKeyId { .. } => "DuplicateKeyId",
            OperatorKeyError::DuplicatePrincipal { .. } => "DuplicatePrincipal",
        };
        check!(variant == expect, "case {name}");
    }
}
