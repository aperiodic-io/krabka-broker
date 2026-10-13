//! Tests for the forged, replayed, and edited records that the check refuses.
//!
//! The attack table is the point of the module: every row is a record an
//! attacker could build from what a captured request gives them, and every row
//! answers the one code. The bit sweep covers the rest of the payload, so a
//! field the table forgets still cannot be edited without breaking the
//! signature.

use {
    super::{
        ALICE, ALICE_KEY, BOB, BOB_KEY, CLUSTER_ID, NOW_MS, OTHER_CLUSTER_ID, SigningFixture,
        check_against, record, signed, signing_fixture, trust,
    },
    crate::{
        codes,
        freeze::signing::{
            FreezeSignatureCheck, WireRefusal, freeze_signing_bytes, verify_freeze_signature,
        },
    },
    assert2::{assert, check},
    krabka_metadata::TopicFreezeRecord,
    krabka_units::{convert::TimeExt as _, minutes},
};

/// The attack table. Every row is refused, every row answers 1009, and no
/// row's code says which check it failed.
#[test]
fn every_forged_or_replayed_record_is_refused_with_one_code() {
    struct Case<'a> {
        label: &'static str,
        record: TopicFreezeRecord,
        check: FreezeSignatureCheck<'a>,
    }

    let SigningFixture {
        trust,
        good,
        replaced,
    } = signing_fixture();

    // Each case builds the record the broker sees and the check it runs.
    let cases: Vec<Case<'_>> = vec![
        Case {
            label: "a signature over frozen: true presented as the thaw",
            record: TopicFreezeRecord {
                frozen: false,
                ..signed(&trust.alice, CLUSTER_ID, &good)
            },
            check: check_against(&trust, ALICE),
        },
        Case {
            label: "a signature made for another cluster",
            record: signed(&trust.alice, OTHER_CLUSTER_ID, &good),
            check: check_against(&trust, ALICE),
        },
        Case {
            label: "an unknown key_id",
            record: TopicFreezeRecord {
                key_id: "carol-yubi".to_owned(),
                ..signed(&trust.alice, CLUSTER_ID, &good)
            },
            check: check_against(&trust, ALICE),
        },
        Case {
            label: "an author that the signing key does not speak for",
            record: signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_by: BOB.to_owned(),
                ..good.clone()
            }),
            check: check_against(&trust, BOB),
        },
        Case {
            label: "an author that is not the principal on the connection",
            record: signed(&trust.alice, CLUSTER_ID, &good),
            check: check_against(&trust, BOB),
        },
        Case {
            label: "another operator's key over the same record",
            record: TopicFreezeRecord {
                key_id: BOB_KEY.to_owned(),
                set_by: BOB.to_owned(),
                ..signed(&trust.alice, CLUSTER_ID, &good)
            },
            check: check_against(&trust, BOB),
        },
        Case {
            label: "a timestamp older than the skew window",
            record: signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_at_ms: NOW_MS - minutes(5).millis_i64() - 1,
                ..good.clone()
            }),
            check: check_against(&trust, ALICE),
        },
        Case {
            label: "a timestamp replayed from the entry it replaces",
            record: signed(&trust.alice, CLUSTER_ID, &good),
            check: FreezeSignatureCheck {
                replaces: Some(&replaced),
                ..check_against(&trust, ALICE)
            },
        },
        Case {
            label: "a timestamp older than the entry it replaces",
            record: signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_at_ms: NOW_MS - 1,
                ..good.clone()
            }),
            check: FreezeSignatureCheck {
                replaces: Some(&replaced),
                ..check_against(&trust, ALICE)
            },
        },
        Case {
            label: "an empty signature",
            record: TopicFreezeRecord {
                signature: Vec::new(),
                ..good.clone()
            },
            check: check_against(&trust, ALICE),
        },
        Case {
            label: "a signature truncated by one byte",
            record: {
                let mut record = signed(&trust.alice, CLUSTER_ID, &good);
                record.signature.pop();
                record
            },
            check: check_against(&trust, ALICE),
        },
    ];

    for Case {
        label,
        record,
        check,
    } in cases
    {
        assert!(
            let Err(refusal) = verify_freeze_signature(&check, &record),
            "case {label}"
        );
        let WireRefusal { code, message } = refusal.wire();
        check!(code == codes::OPERATOR_SIGNATURE_INVALID, "{label}");
        check!(code == 1009, "{label}");
        check!(!message.is_empty(), "{label}");
    }
}

/// A one-bit flip anywhere in the canonical bytes breaks the signature.
/// The signature is made over the good bytes and then verified against a
/// record that rebuilds one flipped bit, which is what an attacker who
/// edits a captured record produces.
#[test]
fn a_one_bit_flip_anywhere_in_the_canonical_bytes_is_refused() {
    let trust = trust();
    let good = signed(&trust.alice, CLUSTER_ID, &record());
    let bytes = freeze_signing_bytes(CLUSTER_ID, &good);

    for index in 0..bytes.len() {
        for bit in 0..8_u32 {
            let mut flipped = bytes.clone();
            flipped[index] ^= 1_u8 << bit;
            check!(
                !trust
                    .keys
                    .verify(ALICE_KEY, ALICE, &flipped, &good.signature),
                "byte {index} bit {bit}"
            );
        }
    }
}
