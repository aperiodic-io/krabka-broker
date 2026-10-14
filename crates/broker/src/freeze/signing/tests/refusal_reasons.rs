//! Tests that each check reaches its own refusal under the one shared code.
//!
//! The response message is the only thing that separates the six checks, so a
//! reason that no input reaches, or two reasons that read the same, would
//! leave an operator with nothing to act on. Each row here names the check it
//! expects and the messages are compared for uniqueness at the end.

use {
    super::{
        ALICE, BOB, CLUSTER_ID, NOW_MS, OTHER_CLUSTER_ID, SigningFixture, check_against, signed,
        signed_at, signed_record_with_unknown_key_id, signing_fixture,
    },
    crate::{
        codes,
        freeze::signing::{FreezeSignatureCheck, SignatureRefusal, verify_freeze_signature},
    },
    assert2::check,
    krabka_metadata::TopicFreezeRecord,
    krabka_units::{convert::TimeExt as _, minutes},
};

/// The refusal reasons are what the response message reads, so each one
/// has to be reachable and each one has to name its own check.
#[test]
fn each_refusal_names_its_own_check_under_one_code() {
    struct Case<'a> {
        label: &'static str,
        record: TopicFreezeRecord,
        check: FreezeSignatureCheck<'a>,
        expected: SignatureRefusal,
    }

    let SigningFixture {
        trust,
        good,
        replaced,
    } = signing_fixture();

    let cases: Vec<Case<'_>> = vec![
        Case {
            label: "an unknown key_id",
            record: signed_record_with_unknown_key_id(&trust, &good),
            check: check_against(&trust, ALICE),
            expected: SignatureRefusal::UnknownKeyId,
        },
        Case {
            label: "an author the key does not speak for",
            record: signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_by: BOB.to_owned(),
                ..good.clone()
            }),
            check: check_against(&trust, BOB),
            expected: SignatureRefusal::AuthorIsNotTheKeyPrincipal,
        },
        Case {
            label: "an author that is not the connection principal",
            record: signed(&trust.alice, CLUSTER_ID, &good),
            check: check_against(&trust, BOB),
            expected: SignatureRefusal::AuthorIsNotTheConnectionPrincipal,
        },
        Case {
            label: "a timestamp outside the skew window",
            record: signed_at(&trust.alice, NOW_MS + minutes(5).millis_i64() + 1, &good),
            check: check_against(&trust, ALICE),
            expected: SignatureRefusal::TimestampOutsideSkewWindow,
        },
        Case {
            label: "a replayed timestamp",
            record: signed(&trust.alice, CLUSTER_ID, &good),
            check: FreezeSignatureCheck {
                replaces: Some(&replaced),
                ..check_against(&trust, ALICE)
            },
            expected: SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces,
        },
        Case {
            label: "a signature over another cluster",
            record: signed(&trust.alice, OTHER_CLUSTER_ID, &good),
            check: check_against(&trust, ALICE),
            expected: SignatureRefusal::SignatureDidNotVerify,
        },
    ];

    let mut messages: Vec<&'static str> = Vec::new();
    for Case {
        label,
        record,
        check,
        expected,
    } in cases
    {
        check!(
            verify_freeze_signature(&check, &record) == Err(expected),
            "{label}"
        );
        check!(
            expected.wire().code == codes::OPERATOR_SIGNATURE_INVALID,
            "{label}"
        );
        messages.push(expected.message());
    }
    let unique: std::collections::BTreeSet<&str> = messages.iter().copied().collect();
    check!(unique.len() == messages.len());
}
