//! Tests for the approve/withdraw decision.
//!
//! These cover the list an approval appends to and the three checks that keep
//! the rule a two-person rule. The child modules cover the proposal
//! lifecycle, the operator signature, and the counts one request reports.

use {
    super::*,
    crate::{
        break_glass::gate::tests::{NOW_MS, approval, config, proposal},
        operator_keys::OperatorKeys,
    },
    assert2::{assert, check},
    krabka_metadata::{BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord},
};

fn attempt(principal: &str) -> Attempt<'_> {
    Attempt {
        principal,
        key_id: "",
        signature: &[],
        withdraw: false,
        now_ms: NOW_MS,
    }
}

fn pending() -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        approvals: Vec::new(),
        ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
    }
}

fn approved_by_bob() -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        approvals: vec![approval("User:bob")],
        ..pending()
    }
}

#[test]
fn a_second_principal_adds_an_approval_to_the_list() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = pending();

    let updated = decide(
        policy,
        &OperatorKeys::default(),
        &stored,
        &attempt("User:bob"),
    )
    .expect("a second approver may approve");

    let expected = BreakGlassProposalRecord {
        approvals: vec![BreakGlassApproval {
            principal: "User:bob".to_owned(),
            approved_at_ms: NOW_MS,
            key_id: String::new(),
            signature: Vec::new(),
        }],
        ..stored
    };
    check!(updated == expected);
}

#[test]
fn an_approval_appends_to_the_list_it_found() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = approved_by_bob();

    let updated = decide(
        policy,
        &OperatorKeys::default(),
        &stored,
        &attempt("User:carol"),
    )
    .expect("a third principal may approve");

    let names: Vec<&str> = updated
        .approvals
        .iter()
        .map(|a| a.principal.as_str())
        .collect();
    check!(names == ["User:bob", "User:carol"]);
}

#[test]
fn the_three_checks_that_make_it_a_two_person_rule() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let with_bob = BreakGlassProposalRecord {
        approvals: vec![approval("User:bob")],
        ..pending()
    };
    let cases = [
        (
            "the proposer approves their own proposal",
            pending(),
            "User:alice",
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
        ),
        (
            "an approver approves twice",
            with_bob,
            "User:bob",
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
        ),
        (
            "a principal outside the set approves",
            pending(),
            "User:mallory",
            codes::BREAK_GLASS_NOT_AN_APPROVER,
        ),
    ];
    for (label, stored, principal, expected) in cases {
        let outcome = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &attempt(principal),
        );
        assert!(let Err(refusal) = outcome, "case {label}");
        check!(refusal.code == expected, "case {label}");
    }
}

// ── the proposal lifecycle ───────────────────────────────────────
//
// The states that take no approval, the expiry boundary, and the withdrawal
// that an expired proposal still takes.
mod lifecycle;

// ── the counts one settled request reports ───────────────────────
//
// The response body and the audit event read the same report, and the audit
// phase follows the outcome and the withdraw flag.
mod reporting;

// ── the operator signature ───────────────────────────────────────
//
// The actions that need a detached signature, and the trust set that binds a
// key id to the approving principal.
mod signatures;
