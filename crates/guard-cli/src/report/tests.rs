//! Tests for the exit code one broker error code becomes, and for the words the
//! tool prints beside it.

use {super::*, assert2::check, krabka_broker::codes};

/// A runbook branches on `$?`, so each number has to mean one thing. The
/// three codes that get their own number are the three an operator acts on
/// differently; every other refusal is a refusal.
#[yare::parameterized(
    none = { codes::NONE, 0 },
    approval_required = { codes::BREAK_GLASS_APPROVAL_REQUIRED, EXIT_NO_APPROVAL },
    invalid_signature = { codes::OPERATOR_SIGNATURE_INVALID, EXIT_BAD_SIGNATURE },
    missing_signature = { codes::OPERATOR_SIGNATURE_REQUIRED, EXIT_BAD_SIGNATURE },
    duplicate_approver = { codes::BREAK_GLASS_DUPLICATE_APPROVER, EXIT_REFUSED },
    not_approver = { codes::BREAK_GLASS_NOT_AN_APPROVER, EXIT_REFUSED },
    invalid_freeze_scope = { codes::FREEZE_SCOPE_INVALID, EXIT_REFUSED },
    freeze_limit = { codes::FREEZE_LIMIT_EXCEEDED, EXIT_REFUSED },
    unauthorized = { codes::CLUSTER_AUTHORIZATION_FAILED, EXIT_REFUSED },
    invalid_request = { codes::INVALID_REQUEST, EXIT_REFUSED },
    not_controller = { codes::NOT_CONTROLLER, EXIT_REFUSED },
    unknown_code = { codes::UNKNOWN_SERVER_ERROR, EXIT_REFUSED },
)]
fn every_broker_code_maps_to_the_exit_code_a_runbook_expects(code: i16, expected: i32) {
    check!(exit_for_code(code) == expected);
}

/// An approval says who made it, and a signed one says which key. The
/// broker never stores a signature it did not check, so a `key_id` here is
/// already proof that the signature verified on the broker.
#[test]
fn an_approval_reports_the_evidence_it_carries() {
    let unsigned = bg::BreakGlassApproval {
        principal: "User:bob".to_owned(),
        approved_at_ms: 1_770_000_000_000,
        ..bg::BreakGlassApproval::default()
    };
    check!(approval_evidence(&unsigned) == "unsigned");

    let signed = bg::BreakGlassApproval {
        key_id: "bob-yubi".to_owned(),
        signature: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06],
        ..unsigned
    };
    check!(approval_evidence(&signed) == "signed by bob-yubi (deadbeef01020304...)");
}

/// The private codes are the ones an operator of this tool meets that no
/// Kafka reference lists, so each one gets a word beside its number.
#[test]
fn a_private_code_reads_as_more_than_a_number() {
    check!(code_name(codes::BREAK_GLASS_APPROVAL_REQUIRED).contains("break-glass"));
    check!(code_name(codes::OPERATOR_SIGNATURE_INVALID).contains("signature"));
    check!(code_name(codes::FREEZE_LIMIT_EXCEEDED).contains("freeze.max_entries"));
    check!(code_name(codes::NOT_CONTROLLER) == "error 41");
}
