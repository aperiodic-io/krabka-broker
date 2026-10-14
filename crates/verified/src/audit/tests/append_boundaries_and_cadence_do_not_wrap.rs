use super::*;

#[test]
fn append_boundaries_and_cadence_do_not_wrap() {
    check!(
        spool_append_decision(u64::MAX - 1, 1, u64::MAX, 1, 3)
            == SpoolAppendDecision {
                accepted: true,
                new_bytes: u64::MAX,
                sync: false,
                next_unsynced: 2,
            }
    );
    check!(
        spool_append_decision(u64::MAX, 1, u64::MAX, 2, 3)
            == SpoolAppendDecision {
                accepted: false,
                new_bytes: u64::MAX,
                sync: false,
                next_unsynced: 2,
            }
    );
    check!(
        spool_append_decision(0, 0, 0, u64::MAX - 1, u64::MAX)
            == SpoolAppendDecision {
                accepted: true,
                new_bytes: 0,
                sync: true,
                next_unsynced: 0,
            }
    );
}

#[yare::parameterized(
    reports_every_pending_loss = {
        AuditLosses { generation: 4, count: 2 },
        AuditLosses { generation: 4, count: 2 },
        AuditLosses { generation: 4, count: 0 },
    },
    later_loss_moves_to_new_generation = {
        AuditLosses { generation: 4, count: 3 },
        AuditLosses { generation: 4, count: 2 },
        AuditLosses { generation: 5, count: 1 },
    },
    different_generation_marker_settles_nothing = {
        AuditLosses { generation: 5, count: 1 },
        AuditLosses { generation: 4, count: 2 },
        AuditLosses { generation: 5, count: 1 },
    },
    over_report_clamps_to_zero = {
        AuditLosses { generation: 4, count: 1 },
        AuditLosses { generation: 4, count: 2 },
        AuditLosses { generation: 4, count: 0 },
    },
    final_generation_saturates = {
        AuditLosses { generation: u64::MAX, count: 3 },
        AuditLosses { generation: u64::MAX, count: 1 },
        AuditLosses { generation: u64::MAX, count: 2 },
    },
)]
fn loss_settlement_keeps_later_losses_in_a_fresh_generation(
    state: AuditLosses,
    batch: AuditLosses,
    expected: AuditLosses,
) {
    check!(settle_loss_batch(state, batch) == expected);
}

#[test]
fn checkpoint_requires_an_exact_nonempty_position() {
    use AuditCheckpointAdmission::{Admit, RejectHead, RejectSequence, RejectSignature};

    check!(audit_checkpoint_admission(true, true, 3, 2) == Admit);
    check!(audit_checkpoint_admission(false, true, 3, 2) == RejectSignature);
    check!(audit_checkpoint_admission(true, false, 3, 2) == RejectHead);
    check!(audit_checkpoint_admission(true, true, 0, 0) == RejectSequence);
    check!(audit_checkpoint_admission(true, true, u64::MAX, u64::MAX) == RejectSequence);
}

#[yare::parameterized(
    exact_marker = { true, 2, 3, Some(2), 1, AuditLossMarkerAdmission::Admit { generation: 2 } },
    earlier_generation = {
        true,
        2,
        3,
        Some(1),
        0,
        AuditLossMarkerAdmission::Admit { generation: 1 },
    },
    missing_generation_one_field = { true, 1, 3, None, 0, AuditLossMarkerAdmission::Reject },
    missing_generation = { true, 2, 3, None, 0, AuditLossMarkerAdmission::Reject },
    too_many_fields = { true, 3, 3, Some(2), 1, AuditLossMarkerAdmission::Reject },
    wrong_header = { false, 2, 3, Some(2), 1, AuditLossMarkerAdmission::Reject },
    empty_count = { true, 2, 0, Some(2), 1, AuditLossMarkerAdmission::Reject },
    previous_not_before_generation = { true, 2, 3, Some(1), 1, AuditLossMarkerAdmission::Reject },
    maximum_generation = { true, 2, 3, Some(u64::MAX), u64::MAX, AuditLossMarkerAdmission::Reject },
)]
fn loss_marker_shape_count_and_generation_are_exact(
    header: bool,
    fields: u64,
    count: u64,
    generation: Option<u64>,
    previous: u64,
    expected: AuditLossMarkerAdmission,
) {
    check!(audit_loss_marker_admission(header, fields, count, generation, previous) == expected);
}
