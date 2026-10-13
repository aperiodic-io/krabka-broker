//! Tests for the checks that decide whether a covering proposal is spendable.
//!
//! Each check earns its own refusal, the approval count is over distinct
//! principals rather than rows, and an action named in `signed_actions` needs a
//! signature on every approval it holds.

use {
    super::{NOW_MS, approval, config, image_of, proposal, signed_approval},
    crate::{
        break_glass::gate::{BreakGlassDenial, DenialReason, authorize},
        config::BreakGlassConfig,
    },
    assert2::{assert, check},
    krabka_metadata::{BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord},
};

#[test]
fn the_gate_refuses_a_proposal_that_is_not_usable() {
    let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
    let id = base.proposal_id;
    let cases = [
        (
            "one approval short of the rule",
            BreakGlassProposalRecord {
                approvals: vec![approval("User:bob")],
                ..base.clone()
            },
            DenialReason::NotEnoughApprovals {
                proposal_id: id,
                held: 1,
                required: 2,
            },
        ),
        (
            "two approvals from one principal",
            BreakGlassProposalRecord {
                approvals: vec![approval("User:bob"), approval("User:bob")],
                ..base.clone()
            },
            DenialReason::NotEnoughApprovals {
                proposal_id: id,
                held: 1,
                required: 2,
            },
        ),
        (
            "no approval at all",
            BreakGlassProposalRecord {
                approvals: Vec::new(),
                ..base.clone()
            },
            DenialReason::NotEnoughApprovals {
                proposal_id: id,
                held: 0,
                required: 2,
            },
        ),
        (
            "an expired proposal",
            BreakGlassProposalRecord {
                expires_at_ms: NOW_MS,
                ..base.clone()
            },
            DenialReason::Expired {
                proposal_id: id,
                expires_at_ms: NOW_MS,
            },
        ),
        (
            "a withdrawn proposal",
            BreakGlassProposalRecord {
                withdrawn: true,
                ..base.clone()
            },
            DenialReason::Withdrawn { proposal_id: id },
        ),
        (
            "an already consumed proposal",
            BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS - 1,
                ..base.clone()
            },
            DenialReason::Consumed {
                proposal_id: id,
                consumed_at_ms: NOW_MS - 1,
            },
        ),
    ];
    for (label, stored, reason) in cases {
        let image = image_of(&[stored]);
        let expected = BreakGlassDenial {
            action: BreakGlassAction::DeleteTopic,
            target: "doomed".to_owned(),
            reason,
        };

        let outcome = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        );

        check!(outcome == Err(expected), "case {label}");
    }
}

#[test]
fn an_action_that_needs_a_signature_refuses_an_unsigned_approval() {
    let config = BreakGlassConfig {
        signed_actions: vec!["delete_topic".to_owned()],
        ..config()
    };
    let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
    let id = base.proposal_id;
    let cases = [
        (
            "no approval carries a signature",
            vec![approval("User:bob"), approval("User:carol")],
            Err(DenialReason::Unsigned { proposal_id: id }),
        ),
        (
            "one of two approvals is unsigned",
            vec![
                signed_approval("User:bob", "bob-yubi"),
                approval("User:carol"),
            ],
            Err(DenialReason::Unsigned { proposal_id: id }),
        ),
        (
            "an approval carries a key id and no signature",
            vec![
                signed_approval("User:bob", "bob-yubi"),
                BreakGlassApproval {
                    signature: Vec::new(),
                    ..signed_approval("User:carol", "carol-yubi")
                },
            ],
            Err(DenialReason::Unsigned { proposal_id: id }),
        ),
        (
            "every approval is signed",
            vec![
                signed_approval("User:bob", "bob-yubi"),
                signed_approval("User:carol", "carol-yubi"),
            ],
            Ok(()),
        ),
    ];
    for (label, approvals, expected) in cases {
        let image = image_of(&[BreakGlassProposalRecord {
            approvals,
            ..base.clone()
        }]);

        let outcome = authorize(
            &image,
            &config,
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        );

        check!(outcome.is_ok() == expected.is_ok(), "case {label}");
        if let Err(reason) = expected {
            assert!(let Err(denial) = outcome, "case {label}");
            check!(denial.reason == reason, "case {label}");
        }
    }
}

#[test]
fn an_unsigned_approval_stays_usable_when_the_action_needs_no_signature() {
    let image = image_of(&[proposal(1, BreakGlassAction::DeleteTopic, "doomed")]);

    let outcome = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        NOW_MS,
    );

    check!(outcome.is_ok());
}
