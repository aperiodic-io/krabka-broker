//! Tests that each check reaches its own refusal under the one shared code.
//!
//! The response message is the only thing that separates the six checks, so a
//! reason that no input reaches, or two reasons that read the same, would
//! leave an operator with nothing to act on. Each row here names the check it
//! expects and the messages are compared for uniqueness at the end.

use {
    super::{
        ALICE, BOB, CLUSTER_ID, NOW_MS, OTHER_CLUSTER_ID, SigningFixture, check_against, signed,
        signing_fixture,
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
    let SigningFixture {
        trust,
        good,
        replaced,
    } = signing_fixture();

    let cases: Vec<(
        &str,
        TopicFreezeRecord,
        FreezeSignatureCheck<'_>,
        SignatureRefusal,
    )> = vec![
        (
            "an unknown key_id",
            TopicFreezeRecord {
                key_id: "carol-yubi".to_owned(),
                ..signed(&trust.alice, CLUSTER_ID, &good)
            },
            check_against(&trust, ALICE),
            SignatureRefusal::UnknownKeyId,
        ),
        (
            "an author the key does not speak for",
            signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_by: BOB.to_owned(),
                ..good.clone()
            }),
            check_against(&trust, BOB),
            SignatureRefusal::AuthorIsNotTheKeyPrincipal,
        ),
        (
            "an author that is not the connection principal",
            signed(&trust.alice, CLUSTER_ID, &good),
            check_against(&trust, BOB),
            SignatureRefusal::AuthorIsNotTheConnectionPrincipal,
        ),
        (
            "a timestamp outside the skew window",
            signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
                set_at_ms: NOW_MS + minutes(5).millis_i64() + 1,
                ..good.clone()
            }),
            check_against(&trust, ALICE),
            SignatureRefusal::TimestampOutsideSkewWindow,
        ),
        (
            "a replayed timestamp",
            signed(&trust.alice, CLUSTER_ID, &good),
            FreezeSignatureCheck {
                replaces: Some(&replaced),
                ..check_against(&trust, ALICE)
            },
            SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces,
        ),
        (
            "a signature over another cluster",
            signed(&trust.alice, OTHER_CLUSTER_ID, &good),
            check_against(&trust, ALICE),
            SignatureRefusal::SignatureDidNotVerify,
        ),
    ];

    let mut messages: Vec<&'static str> = Vec::new();
    for (label, record, check, expected) in cases {
        check!(
            verify_freeze_signature(&check, &record) == Err(expected),
            "{label}"
        );
        check!(
            expected.wire().0 == codes::OPERATOR_SIGNATURE_INVALID,
            "{label}"
        );
        messages.push(expected.message());
    }
    let unique: std::collections::BTreeSet<&str> = messages.iter().copied().collect();
    check!(unique.len() == messages.len());
}
