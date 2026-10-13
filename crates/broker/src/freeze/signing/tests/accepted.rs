//! Tests for the records that the signature check accepts.
//!
//! A freeze, a thaw that names the break-glass proposal it spends, and a
//! record that the second operator in the trust set signed all verify. The
//! last one is what proves the trust set holds every configured key rather
//! than one.

use {
    super::{ALICE, BOB, BOB_KEY, CLUSTER_ID, PROPOSAL, check_against, record, signed, trust},
    crate::freeze::signing::verify_freeze_signature,
    assert2::check,
    krabka_metadata::TopicFreezeRecord,
};

#[test]
fn a_good_signature_verifies() {
    let trust = trust();
    let record = signed(&trust.alice, CLUSTER_ID, &record());

    check!(verify_freeze_signature(&check_against(&trust, ALICE), &record) == Ok(()));
}

#[test]
fn a_signature_survives_a_thaw_that_names_its_proposal() {
    let trust = trust();
    let thaw = signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
        frozen: false,
        proposal_id: PROPOSAL,
        ..record()
    });

    check!(verify_freeze_signature(&check_against(&trust, ALICE), &thaw) == Ok(()));
}

/// Every other positive case here signs as one operator. If
/// [`OperatorKeys::load`] kept only the first entry, or matched a key by
/// position rather than by `key_id`, each of them would still pass and the
/// trust set would hold one operator instead of the configured set. A
/// second operator signing a record of their own is what rules that out,
/// and a two-person rule is worth nothing if only one person can sign.
#[test]
fn a_second_operator_signs_a_valid_freeze() {
    let trust = trust();
    let record = signed(&trust.bob, CLUSTER_ID, &TopicFreezeRecord {
        key_id: BOB_KEY.to_owned(),
        set_by: BOB.to_owned(),
        ..record()
    });

    check!(verify_freeze_signature(&check_against(&trust, BOB), &record) == Ok(()));
}
