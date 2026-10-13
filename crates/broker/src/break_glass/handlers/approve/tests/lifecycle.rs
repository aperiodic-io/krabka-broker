//! Tests for the states a proposal can be in when a request arrives.
//!
//! A withdrawn, consumed, or expired proposal takes no further approval, and
//! the expiry comparison is inclusive. A withdrawal is the one action that an
//! expired proposal still takes, because it only removes authority.

use {
    super::{attempt, pending},
    crate::{
        break_glass::{
            config::BreakGlassPolicy,
            gate::tests::{EXPIRES_MS, NOW_MS, config},
            handlers::approve::{Attempt, decide},
        },
        codes,
        config::BreakGlassConfig,
        operator_keys::OperatorKeys,
    },
    assert2::{assert, check},
    krabka_metadata::BreakGlassProposalRecord,
};

#[test]
fn a_settled_or_expired_proposal_takes_no_approval() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let cases = [
        ("an expired proposal", BreakGlassProposalRecord {
            expires_at_ms: NOW_MS,
            ..pending()
        }),
        ("a withdrawn proposal", BreakGlassProposalRecord {
            withdrawn: true,
            ..pending()
        }),
        ("a consumed proposal", BreakGlassProposalRecord {
            consumed_at_ms: NOW_MS - 1,
            ..pending()
        }),
    ];
    for (label, stored) in cases {
        let outcome = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &attempt("User:bob"),
        );
        assert!(let Err(refusal) = outcome, "case {label}");
        check!(refusal.code == codes::POLICY_VIOLATION, "case {label}");
    }
}

#[test]
fn an_expired_proposal_still_takes_a_withdrawal() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = BreakGlassProposalRecord {
        expires_at_ms: NOW_MS,
        ..pending()
    };

    let updated = decide(policy, &OperatorKeys::default(), &stored, &Attempt {
        withdraw: true,
        ..attempt("User:alice")
    })
    .expect("the proposer may withdraw an expired proposal");

    check!(updated.withdrawn);
}

#[test]
fn the_proposer_and_every_approver_may_withdraw() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let cases = [
        ("the proposer", "User:alice", true),
        ("a configured approver", "User:carol", true),
        ("a principal outside the set", "User:mallory", false),
    ];
    for (label, principal, expected) in cases {
        let outcome = decide(policy, &OperatorKeys::default(), &pending(), &Attempt {
            withdraw: true,
            ..attempt(principal)
        });
        check!(outcome.is_ok() == expected, "case {label}");
        if let Ok(updated) = outcome {
            check!(updated.withdrawn, "case {label}");
            check!(updated.approvals.is_empty(), "case {label}");
        }
    }
}

#[test]
fn a_withdrawal_ignores_the_key_id_and_the_signature() {
    let config = BreakGlassConfig {
        signed_actions: vec!["delete_topic".to_owned()],
        ..config()
    };
    let policy = BreakGlassPolicy::new(&config);

    let updated = decide(policy, &OperatorKeys::default(), &pending(), &Attempt {
        withdraw: true,
        key_id: "nobody",
        signature: &[9; 64],
        ..attempt("User:bob")
    })
    .expect("a withdrawal needs no signature");

    check!(updated.withdrawn);
    check!(updated.approvals.is_empty());
}

#[test]
fn a_proposal_that_expires_exactly_now_is_expired() {
    let config = config();
    let policy = BreakGlassPolicy::new(&config);
    let stored = pending();

    let before = decide(policy, &OperatorKeys::default(), &stored, &Attempt {
        now_ms: EXPIRES_MS - 1,
        ..attempt("User:bob")
    });
    let at = decide(policy, &OperatorKeys::default(), &stored, &Attempt {
        now_ms: EXPIRES_MS,
        ..attempt("User:bob")
    });

    check!(before.is_ok());
    assert!(let Err(refusal) = at);
    check!(refusal.code == codes::POLICY_VIOLATION);
}
