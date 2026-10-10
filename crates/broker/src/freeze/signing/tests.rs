//! The trust set and the records that the signing unit tests share.
//!
//! Two operators, Alice and Bob, hold keys that the loaded trust set knows, so
//! a test can present a record either of them signed as well as one that no
//! configured key made. Every fixture builds an unsigned record first and signs
//! it afterwards, which is the order an operator's own command follows.

use std::path::PathBuf;

use krabka_metadata::{PatternType, TopicFreezeRecord};
use krabka_units::minutes;
use ring::signature::Ed25519KeyPair;
use tempfile::TempDir;
use uuid::Uuid;

use super::{FreezeSignatureCheck, freeze_signing_bytes};
use crate::operator_keys::{OperatorKeyEntry, OperatorKeys};

mod accepted;
mod canonical_bytes;
mod forgery;
mod refusal_reasons;
mod timestamps;

const CLUSTER_ID: &str = "5150ba5e-0000-4000-8000-00000000c0de";
const OTHER_CLUSTER_ID: &str = "deadbeef-0000-4000-8000-00000000c0de";
const ALICE: &str = "User:alice";
const ALICE_KEY: &str = "alice-yubi";
const BOB: &str = "User:bob";
const BOB_KEY: &str = "bob-yubi";
const NOW_MS: i64 = 1_770_000_000_000;
const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

// Two loaded operator keys and the key pairs that sign for them.
struct Trust {
    keys: OperatorKeys,
    alice: Ed25519KeyPair,
    bob: Ed25519KeyPair,
    _dir: TempDir,
}

fn fresh_key(dir: &TempDir, name: &str) -> (Ed25519KeyPair, PathBuf) {
    crate::test_support::ed25519_public_key_file(
        dir.path(),
        crate::test_support::OperatorKeyFileSetup {
            name,
            ..Default::default()
        },
    )
}

fn trust() -> Trust {
    let dir = TempDir::new().expect("tempdir");
    let (alice, alice_path) = fresh_key(&dir, "alice.pub");
    let (bob, bob_path) = fresh_key(&dir, "bob.pub");
    let keys = OperatorKeys::load(&[
        OperatorKeyEntry {
            key_id: ALICE_KEY.to_owned(),
            principal: ALICE.to_owned(),
            public_key_path: alice_path,
        },
        OperatorKeyEntry {
            key_id: BOB_KEY.to_owned(),
            principal: BOB.to_owned(),
            public_key_path: bob_path,
        },
    ])
    .expect("load trust set");
    Trust {
        keys,
        alice,
        bob,
        _dir: dir,
    }
}

// An unsigned freeze record with every field set.
fn record() -> TopicFreezeRecord {
    TopicFreezeRecord {
        scope: "orders".to_owned(),
        pattern_type: PatternType::Literal,
        frozen: true,
        reason: "DR cutover".to_owned(),
        set_by: ALICE.to_owned(),
        set_at_ms: NOW_MS,
        proposal_id: Uuid::nil(),
        key_id: ALICE_KEY.to_owned(),
        signature: Vec::new(),
    }
}

// `record` signed by `pair` for `cluster_id`.
fn signed(
    pair: &Ed25519KeyPair,
    cluster_id: &str,
    record: &TopicFreezeRecord,
) -> TopicFreezeRecord {
    let bytes = freeze_signing_bytes(cluster_id, record);
    TopicFreezeRecord {
        signature: pair.sign(&bytes).as_ref().to_vec(),
        ..record.clone()
    }
}

fn check_against<'a>(trust: &'a Trust, principal: &'a str) -> FreezeSignatureCheck<'a> {
    FreezeSignatureCheck {
        keys: &trust.keys,
        cluster_id: CLUSTER_ID,
        connection_principal: principal,
        max_skew: minutes(5),
        now_ms: NOW_MS,
        replaces: None,
    }
}
