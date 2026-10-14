use super::*;

#[yare::parameterized(
    scanned_live = { true, RestoreSnapshotState::Live, RestoreReconcileDecision::Keep },
    scanned_delete_started = {
        true,
        RestoreSnapshotState::DeleteStarted,
        RestoreReconcileDecision::Exclude,
    },
    scanned_missing = { true, RestoreSnapshotState::Missing, RestoreReconcileDecision::Disagree },
    scanned_delete_finished = {
        true,
        RestoreSnapshotState::DeleteFinished,
        RestoreReconcileDecision::Disagree,
    },
    unscanned_live = { false, RestoreSnapshotState::Live, RestoreReconcileDecision::Disagree },
    unscanned_delete_started = {
        false,
        RestoreSnapshotState::DeleteStarted,
        RestoreReconcileDecision::Exclude,
    },
    unscanned_delete_finished = {
        false,
        RestoreSnapshotState::DeleteFinished,
        RestoreReconcileDecision::Exclude,
    },
    unscanned_missing = { false, RestoreSnapshotState::Missing, RestoreReconcileDecision::Exclude },
)]
fn archive_reconciliation_covers_every_scan_and_snapshot_pair(
    scanned: bool,
    state: RestoreSnapshotState,
    expected: RestoreReconcileDecision,
) {
    check!(restore_archive_reconcile(scanned, state) == expected);
}

#[test]
fn batch_step_is_contiguous_and_overflow_safe() {
    check!(restore_batch_step(10, 10, 2) == Some(13));
    check!(restore_batch_step(10, 11, 0) == Some(12));
    check!(restore_batch_step(10, 9, 0) == None);
    check!(restore_batch_step(10, 10, -1) == None);
    check!(restore_batch_step(i64::MAX, i64::MAX, 0) == None);
    check!(restore_batch_step(i64::MAX - 1, i64::MAX - 1, 0) == Some(i64::MAX));
}

#[yare::parameterized(
    create_time_adds_delta = {
        "create time adds the delta",
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(1, -5),
        Some((11, 95)),
    },
    log_append_uses_batch_max = {
        "log append time reports the batch max timestamp",
        frame(RestoreTimestampType::LogAppendTime, 2_000, 1_000),
        deltas(1, 50),
        Some((11, 1_000)),
    },
    log_append_ignores_delta_overflow = {
        "log append time ignores an overflowing producer delta",
        frame(RestoreTimestampType::LogAppendTime, i64::MAX, 1_000),
        deltas(0, 1),
        Some((10, 1_000)),
    },
    create_time_overflow_above = {
        "create time overflow above",
        frame(RestoreTimestampType::CreateTime, i64::MAX, i64::MAX),
        deltas(0, 1),
        None,
    },
    create_time_overflow_below = {
        "create time overflow below",
        frame(RestoreTimestampType::CreateTime, i64::MIN, i64::MAX),
        deltas(0, -1),
        None,
    },
    create_time_reaches_min = {
        "create time reaches i64::MIN exactly",
        frame(RestoreTimestampType::CreateTime, i64::MIN + 5, i64::MAX),
        deltas(0, -5),
        Some((10, i64::MIN)),
    },
    negative_delta = {
        "negative offset delta",
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(-1, 0),
        None,
    },
    delta_past_span = {
        "offset delta past the span",
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(5, 0),
        None,
    },
    negative_span = {
        "negative span",
        RestoreBatchFrame { last_offset_delta: -1, ..frame(RestoreTimestampType::CreateTime, 100, 110) },
        deltas(0, 0),
        None,
    },
    offset_overflow = {
        "offset overflow",
        RestoreBatchFrame { base_offset: i64::MAX, ..frame(RestoreTimestampType::CreateTime, 100, 110) },
        deltas(1, 0),
        None,
    },
    extreme_representable = {
        "extreme but representable",
        RestoreBatchFrame {
            base_offset: i64::MAX,
            last_offset_delta: 0,
            ..frame(RestoreTimestampType::CreateTime, i64::MAX, i64::MAX)
        },
        deltas(0, 0),
        Some((i64::MAX, i64::MAX)),
    },
)]
fn record_coordinates_follow_kafka_timestamp_type(
    name: &'static str,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
    expected: Option<(i64, i64)>,
) {
    check!(
        restore_record_coordinates(frame, record) == expected,
        "{name}"
    );
}

fn producer(
    control: bool,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> RestoreProducer {
    RestoreProducer {
        control,
        transactional,
        producer_id,
        producer_epoch,
        base_sequence,
    }
}

#[yare::parameterized(
    non_idempotent = { "non-idempotent data", layout(10, 2, 2), NON_IDEMPOTENT, Some(13) },
    idempotent_empty = {
        "idempotent emptied data",
        layout(10, 2, 0),
        producer(false, false, 7, 2, 4),
        Some(13),
    },
    transactional_data = {
        "transactional data",
        layout(10, 2, 3),
        producer(false, true, 7, 2, 4),
        Some(13),
    },
    transaction_marker = {
        "transaction marker",
        layout(10, 0, 1),
        producer(true, true, 7, 2, -1),
        Some(11),
    },
    compacted_marker = {
        "compacted-away marker kept empty",
        layout(10, 0, 0),
        producer(true, true, 7, 2, -1),
        Some(11),
    },
    barrier_control = {
        "barrier control batch",
        layout(0, 0, 1),
        producer(true, false, -1, -1, -1),
        Some(1),
    },
    control_spans_offsets = {
        "control spanning two offsets",
        layout(10, 1, 1),
        producer(true, false, -1, -1, -1),
        None,
    },
    control_has_two_records = {
        "control with two records",
        layout(10, 0, 2),
        producer(true, false, -1, -1, -1),
        None,
    },
    marker_has_sequence = {
        "marker carrying a sequence",
        layout(10, 0, 1),
        producer(true, true, 7, 2, 4),
        None,
    },
    transactional_marker_without_producer = {
        "transactional marker without producer",
        layout(10, 0, 1),
        producer(true, true, -1, -1, -1),
        None,
    },
    transactional_data_without_producer = {
        "transactional data without producer",
        layout(10, 2, 2),
        producer(false, true, -1, -1, -1),
        None,
    },
    partial_sentinel_epoch = {
        "partial sentinel epoch",
        layout(10, 2, 2),
        producer(false, false, -1, 0, -1),
        None,
    },
    producer_without_epoch = {
        "producer without epoch",
        layout(10, 2, 2),
        producer(false, false, 7, -1, 4),
        None,
    },
    negative_span = { "negative span", layout(0, -1, 1), NON_IDEMPOTENT, None },
    negative_count = { "negative count", layout(10, 2, -1), NON_IDEMPOTENT, None },
    negative_base = { "negative base", layout(-1, 0, 1), NON_IDEMPOTENT, None },
    frontier_overflow = { "frontier overflow", layout(i64::MAX, 0, 0), NON_IDEMPOTENT, None },
)]
fn rewritten_headers_admit_exactly_kafka_producer_and_control_layouts(
    name: &'static str,
    layout: RestoreLayout,
    producer: RestoreProducer,
    expected: Option<i64>,
) {
    check!(
        restore_rewritten_batch_header(layout, producer) == expected,
        "{name}"
    );
}
