//! Tests for the report that the response body and the audit event share.
//!
//! A request that moves the proposal reports the list it leaves behind, and a
//! refusal reports the stored counts. The audit phase follows the outcome and
//! the withdraw flag.

use {
    super::pending,
    crate::{
        break_glass::{
            config::BreakGlassPolicy,
            gate::tests::{approval, config},
            handlers::{
                Refusal, UNKNOWN_ACTION,
                approve::report::{Report, phase_of},
            },
        },
        codes,
    },
    assert2::check,
    krabka_audit::PrivilegedPhase,
    krabka_metadata::BreakGlassProposalRecord,
};

#[test]
fn a_report_of_a_missing_proposal_names_no_action() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);

    let report = Report::of(None, None, policy);

    check!(report.action == UNKNOWN_ACTION);
    check!(report.proposal_id == None);
    check!(report.held == 0);
    check!(report.required == 2);
}

#[test]
fn a_report_counts_the_approvals_the_request_leaves_behind() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = BreakGlassProposalRecord {
        approvals: vec![approval("User:bob")],
        ..pending()
    };
    let settled = BreakGlassProposalRecord {
        approvals: vec![approval("User:bob"), approval("User:carol")],
        ..pending()
    };

    let refused = Report::of(Some(&stored), None, policy);
    let approved = Report::of(Some(&stored), Some(&settled), policy);

    check!(refused.held == 1);
    check!(refused.counterparties == vec!["User:bob".to_owned()]);
    check!(approved.held == 2);
    check!(approved.counterparties == vec!["User:bob".to_owned(), "User:carol".to_owned()]);
    check!(approved.action == "delete_topic");
    check!(approved.target == "doomed");
}

#[test]
fn the_audit_phase_follows_the_outcome_and_the_flag() {
    let cases = [
        (
            "an approval",
            Ok(pending()),
            false,
            PrivilegedPhase::Approved,
        ),
        (
            "a withdrawal",
            Ok(pending()),
            true,
            PrivilegedPhase::Consumed,
        ),
        (
            "a refused approval",
            Err(Refusal::new(codes::POLICY_VIOLATION, "no")),
            false,
            PrivilegedPhase::Refused,
        ),
        (
            "a refused withdrawal",
            Err(Refusal::new(codes::POLICY_VIOLATION, "no")),
            true,
            PrivilegedPhase::Refused,
        ),
    ];
    for (label, outcome, withdraw, expected) in cases {
        check!(phase_of(&outcome, withdraw) == expected, "case {label}");
    }
}
