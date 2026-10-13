//! Tests for the proposals a request reaches, and the one of them it spends.
//!
//! Target matching covers the partition rule, the actions it does not apply
//! to, and the topic names that read as a partition of another topic. When two
//! proposals authorize the same request, the one that expires first is spent.

use {
    super::{NOW_MS, config, consumed_record, image_of, proposal},
    crate::break_glass::{ALL_ACTIONS, action_name, gate::authorize},
    assert2::check,
    krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord},
    uuid::Uuid,
};

#[test]
fn a_proposal_covers_a_target_by_the_documented_rule() {
    let cases = [
        (
            "the same partition",
            BreakGlassAction::DeleteRecords,
            "orders-3",
            "orders-3",
            true,
        ),
        (
            "the topic of the partition",
            BreakGlassAction::DeleteRecords,
            "orders",
            "orders-3",
            true,
        ),
        (
            "a topic name that itself holds a dash",
            BreakGlassAction::DeleteRecords,
            "my-orders",
            "my-orders-11",
            true,
        ),
        (
            "another partition of the same topic",
            BreakGlassAction::DeleteRecords,
            "orders-4",
            "orders-3",
            false,
        ),
        (
            "another topic",
            BreakGlassAction::DeleteRecords,
            "payments",
            "orders-3",
            false,
        ),
        (
            "a partition proposal does not cover the whole topic",
            BreakGlassAction::DeleteRecords,
            "orders-3",
            "orders",
            false,
        ),
        (
            "a topic-scoped action takes the exact target only",
            BreakGlassAction::DeleteTopic,
            "logs",
            "logs-2024",
            false,
        ),
        (
            "a topic-scoped action on its own topic",
            BreakGlassAction::DeleteTopic,
            "logs-2024",
            "logs-2024",
            true,
        ),
        (
            "a non-numeric suffix is part of the topic name",
            BreakGlassAction::DeleteRecords,
            "orders",
            "orders-east",
            false,
        ),
        (
            "an empty partition suffix",
            BreakGlassAction::DeleteRecords,
            "orders",
            "orders-",
            false,
        ),
        (
            "an empty topic before the suffix",
            BreakGlassAction::DeleteRecords,
            "",
            "-3",
            false,
        ),
        (
            "a broker id target",
            BreakGlassAction::UnregisterBroker,
            "7",
            "7",
            true,
        ),
        (
            "a freeze scope target",
            BreakGlassAction::ThawTopicFreeze,
            "literal:orders",
            "literal:orders",
            true,
        ),
    ];
    for (label, action, proposal_target, request_target, expected) in cases {
        let image = image_of(&[proposal(1, action, proposal_target)]);

        let outcome = authorize(&image, &config(), action, request_target, NOW_MS);

        check!(outcome.is_ok() == expected, "case {label}");
    }
}

#[test]
fn a_proposal_for_another_action_never_covers_the_request() {
    for stored in ALL_ACTIONS {
        let image = image_of(&[proposal(1, stored, "orders")]);
        for asked in ALL_ACTIONS {
            let outcome = authorize(&image, &config(), asked, "orders", NOW_MS);
            check!(
                outcome.is_ok() == (stored == asked),
                "{} stored, {} asked",
                action_name(stored),
                action_name(asked)
            );
        }
    }
}

#[test]
fn the_gate_spends_the_proposal_that_expires_first() {
    let early = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(9),
        expires_at_ms: NOW_MS + 1_000,
        ..proposal(9, BreakGlassAction::DeleteTopic, "doomed")
    };
    let late = BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(2),
        expires_at_ms: NOW_MS + 60_000,
        ..proposal(2, BreakGlassAction::DeleteTopic, "doomed")
    };
    let image = image_of(&[late, early]);

    let record = authorize(
        &image,
        &config(),
        BreakGlassAction::DeleteTopic,
        "doomed",
        NOW_MS,
    )
    .expect("one of the two proposals authorizes the deletion");

    check!(consumed_record(&record).proposal_id == Uuid::from_u128(9));
}
