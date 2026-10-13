//! Tests for the refusal the gate reports when nothing authorizes a request.
//!
//! An image with no covering proposal answers `NoProposal`, several unusable
//! proposals answer with the nearest of them, and the denial names the proposal
//! an operator can act on.

use {
    super::{NOW_MS, approval, config, image_of, proposal},
    crate::break_glass::gate::{BreakGlassDenial, DenialReason, authorize},
    assert2::{assert, check},
    krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataImage},
    uuid::Uuid,
};

#[test]
fn an_empty_registry_refuses_with_no_proposal() {
    let image = MetadataImage::new(Uuid::nil());

    let outcome = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        NOW_MS,
    );

    check!(
        outcome
            == Err(BreakGlassDenial {
                action: BreakGlassAction::DeleteTopic,
                target: "doomed".to_owned(),
                reason: DenialReason::NoProposal,
            })
    );
}

#[test]
fn the_gate_reports_the_refusal_that_came_nearest() {
    let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
    let short = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(2),
        approvals: vec![approval("User:bob")],
        ..base.clone()
    };
    let withdrawn = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(3),
        withdrawn: true,
        ..base.clone()
    };
    let image = image_of(&[withdrawn, short]);

    let outcome = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        NOW_MS,
    );

    assert!(let Err(denial) = outcome);
    check!(
        denial.reason
            == DenialReason::NotEnoughApprovals {
                proposal_id: Uuid::from_u128(2),
                held: 1,
                required: 2,
            }
    );
    check!(denial.proposal_id() == Some(Uuid::from_u128(2)));
}

#[test]
fn a_denial_names_no_proposal_when_none_covers_the_request() {
    let denial = BreakGlassDenial {
        action: BreakGlassAction::DeleteTopic,
        target: "doomed".to_owned(),
        reason: DenialReason::NoProposal,
    };

    check!(denial.proposal_id() == None);
    check!(denial.to_string().contains("delete_topic"));
    check!(denial.to_string().contains("doomed"));
}
