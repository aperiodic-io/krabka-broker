//! Tests for the record a spent proposal becomes, and the fixtures the rest of
//! the break-glass tests build on.
//!
//! The helpers here are the one description of an approved proposal that the
//! whole crate shares, so a handler test and a gate test disagree about a
//! record only when they mean to. The child modules cover target matching, the
//! checks that make a proposal usable, and the refusal the gate reports.

use {
    super::{authorize, is_gated},
    crate::{config::BreakGlassConfig, time_util::now_ms},
    assert2::{assert, check},
    krabka_metadata::{
        BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord, MetadataImage,
        MetadataRecord,
    },
    krabka_units::minutes,
    uuid::Uuid,
};

pub(crate) const CREATED_MS: i64 = 1_770_000_000_000;
pub(crate) const EXPIRES_MS: i64 = 1_770_000_180_000;
pub(crate) const NOW_MS: i64 = 1_770_000_060_000;

pub(crate) fn config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob", "User:carol"]
            .map(str::to_owned)
            .to_vec(),
        required_approvals: 2,
        proposal_ttl: minutes(3),
        signed_actions: Vec::new(),
        ..BreakGlassConfig::default()
    }
}

pub(crate) fn approval(principal: &str) -> BreakGlassApproval {
    BreakGlassApproval {
        principal: principal.to_owned(),
        approved_at_ms: CREATED_MS + 1_000,
        key_id: String::new(),
        signature: Vec::new(),
    }
}

pub(crate) fn signed_approval(principal: &str, key_id: &str) -> BreakGlassApproval {
    BreakGlassApproval {
        key_id: key_id.to_owned(),
        signature: vec![7; 64],
        ..approval(principal)
    }
}

pub(crate) fn proposal(
    id: u128,
    action: BreakGlassAction,
    target: &str,
) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(id),
        action,
        target: target.to_owned(),
        proposer: "User:alice".to_owned(),
        reason: "incident 42".to_owned(),
        created_at_ms: CREATED_MS,
        expires_at_ms: EXPIRES_MS,
        approvals: vec![approval("User:bob"), approval("User:carol")],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

/// The id [`approved_proposal`] gives its proposal.
pub(crate) const APPROVED_PROPOSAL_ID: Uuid =
    Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

/// A proposal for `action` on `target` that two people approved, and that has
/// not expired against the wall clock a gated handler reads.
///
/// The handler tests drive the gate through code that stamps the time itself,
/// so this fixture is anchored to [`now_ms`] rather than to [`NOW_MS`]. A test
/// that passes a fixed `now_ms` of its own still finds it usable, because the
/// gate reads only the expiry and the expiry lies ten minutes past the wall
/// clock.
pub(crate) fn approved_proposal(
    action: BreakGlassAction,
    target: &str,
) -> BreakGlassProposalRecord {
    let now = now_ms();
    BreakGlassProposalRecord {
        proposal_id: APPROVED_PROPOSAL_ID,
        action,
        target: target.to_owned(),
        proposer: "User:carol".to_owned(),
        reason: "incident 42".to_owned(),
        created_at_ms: now - 1_000,
        expires_at_ms: now + 600_000,
        approvals: vec![approval("User:alice"), approval("User:bob")],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

pub(crate) fn image_of(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}

fn consumed_record(record: &MetadataRecord) -> &BreakGlassProposalRecord {
    assert!(let MetadataRecord::V1BreakGlassProposal(record) = record);
    record
}

#[test]
fn an_approved_proposal_comes_back_stamped_consumed() {
    let approved = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
    let image = image_of(std::slice::from_ref(&approved));

    let record = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        NOW_MS,
    )
    .expect("the proposal authorizes the deletion");

    let expected = BreakGlassProposalRecord {
        consumed_at_ms: NOW_MS,
        ..approved
    };
    check!(consumed_record(&record) == &expected);
}

#[test]
fn a_broker_with_no_approver_set_gates_nothing() {
    let off = BreakGlassConfig::default();

    check!(!is_gated(&off));
    check!(is_gated(&config()));
}

#[test]
fn a_clock_that_reads_zero_still_stamps_a_consumed_record() {
    let image = image_of(&[BreakGlassProposalRecord {
        created_at_ms: -1,
        expires_at_ms: 1,
        ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
    }]);

    let record = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        0,
    )
    .expect("the proposal has not expired at zero");

    check!(consumed_record(&record).consumed_at_ms == 1);
}

// ── target matching and the choice between candidates ────────────
//
// Which stored proposals reach a request, the topic names that read as a
// partition of another topic, and the proposal the gate spends when two are
// usable.
mod selection;

// ── the checks that make a proposal usable ───────────────────────
//
// Each refusal a covering proposal can earn, the count over distinct
// principals, and the signature an action in `signed_actions` needs.
mod usability;

// ── the refusal the gate reports ─────────────────────────────────
//
// The answer when nothing covers the request, and the nearest refusal when
// several covering proposals are unusable.
mod denial;
