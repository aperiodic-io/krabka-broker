//! Tests for the byte string that an operator signs over a freeze record.
//!
//! The documented layout is written out by hand here, so a builder that stops
//! following it fails even though nothing else in the broker notices. The
//! pattern-type byte and the length prefixes get a test of their own, because
//! each one answers a way two different records could sign the same bytes.

use {
    super::{ALICE, ALICE_KEY, BOB_KEY, CLUSTER_ID, PROPOSAL, record},
    crate::{freeze::signing::freeze_signing_bytes, signing_domains::FREEZE_DOMAIN},
    assert2::check,
    krabka_metadata::{PatternType, TopicFreezeRecord},
};

/// The documented layout, written out by hand rather than by the code that
/// [`freeze_signing_bytes`] runs. A change to either one that the other
/// does not follow fails here.
#[test]
fn the_canonical_bytes_match_the_documented_layout() {
    let record = TopicFreezeRecord {
        scope: "tenant-a.".to_owned(),
        pattern_type: PatternType::Prefixed,
        frozen: false,
        reason: "thaw".to_owned(),
        set_by: ALICE.to_owned(),
        set_at_ms: 0x0102_0304_0506_0708,
        proposal_id: PROPOSAL,
        key_id: ALICE_KEY.to_owned(),
        signature: vec![0xAB; 64],
    };

    let mut expected = Vec::new();
    expected.extend_from_slice(b"krabka-topic-freeze-v1\0");
    expected.extend_from_slice(&[0, 0, 0, 36]);
    expected.extend_from_slice(CLUSTER_ID.as_bytes());
    expected.push(4);
    expected.extend_from_slice(&[0, 0, 0, 9]);
    expected.extend_from_slice(b"tenant-a.");
    expected.push(0);
    expected.extend_from_slice(&[0, 0, 0, 4]);
    expected.extend_from_slice(b"thaw");
    expected.extend_from_slice(&[0, 0, 0, 10]);
    expected.extend_from_slice(b"User:alice");
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

    check!(freeze_signing_bytes(CLUSTER_ID, &record) == expected);
    // The key id and the signature are not in the payload. A signature
    // cannot cover itself, and the key that made it is named beside it.
    check!(
        freeze_signing_bytes(CLUSTER_ID, &TopicFreezeRecord {
            key_id: BOB_KEY.to_owned(),
            signature: Vec::new(),
            ..record
        }) == expected
    );
}

#[test]
fn a_literal_freeze_takes_the_kafka_pattern_type_byte() {
    let literal = freeze_signing_bytes(CLUSTER_ID, &record());
    let prefixed = freeze_signing_bytes(CLUSTER_ID, &TopicFreezeRecord {
        pattern_type: PatternType::Prefixed,
        ..record()
    });
    let pattern_byte = FREEZE_DOMAIN.len() + 4 + CLUSTER_ID.len();

    check!(literal[pattern_byte] == 3);
    check!(prefixed[pattern_byte] == 4);
}

/// A length prefix is what keeps two different records from building one
/// byte string.
#[test]
fn moving_a_character_between_two_fields_changes_the_bytes() {
    let left = TopicFreezeRecord {
        scope: "ab".to_owned(),
        reason: "c".to_owned(),
        ..record()
    };
    let right = TopicFreezeRecord {
        scope: "a".to_owned(),
        reason: "bc".to_owned(),
        ..record()
    };

    check!(freeze_signing_bytes(CLUSTER_ID, &left) != freeze_signing_bytes(CLUSTER_ID, &right));
}
