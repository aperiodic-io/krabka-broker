use super::*;

#[test]
fn archive_reconciliation_covers_every_scan_and_snapshot_pair() {
    use {
        RestoreReconcileDecision::{Disagree, Exclude, Keep},
        RestoreSnapshotState::{DeleteFinished, DeleteStarted, Live, Missing},
    };
    for (scanned, state, expected) in [
        (true, Live, Keep),
        (true, DeleteStarted, Exclude),
        (true, Missing, Disagree),
        (true, DeleteFinished, Disagree),
        (false, Live, Disagree),
        (false, DeleteStarted, Exclude),
        (false, DeleteFinished, Exclude),
        (false, Missing, Exclude),
    ] {
        check!(restore_archive_reconcile(scanned, state) == expected);
    }
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

#[test]
fn record_coordinates_follow_kafka_timestamp_type() {
    use RestoreTimestampType::{CreateTime, LogAppendTime};
    for (name, frame, record, expected) in [
        (
            "create time adds the delta",
            frame(CreateTime, 100, 110),
            deltas(1, -5),
            Some((11, 95)),
        ),
        (
            "log append time reports the batch max timestamp",
            frame(LogAppendTime, 2_000, 1_000),
            deltas(1, 50),
            Some((11, 1_000)),
        ),
        (
            "log append time ignores an overflowing producer delta",
            frame(LogAppendTime, i64::MAX, 1_000),
            deltas(0, 1),
            Some((10, 1_000)),
        ),
        (
            "create time overflow above",
            frame(CreateTime, i64::MAX, i64::MAX),
            deltas(0, 1),
            None,
        ),
        (
            "create time overflow below",
            frame(CreateTime, i64::MIN, i64::MAX),
            deltas(0, -1),
            None,
        ),
        (
            "create time reaches i64::MIN exactly",
            frame(CreateTime, i64::MIN + 5, i64::MAX),
            deltas(0, -5),
            Some((10, i64::MIN)),
        ),
        (
            "negative offset delta",
            frame(CreateTime, 100, 110),
            deltas(-1, 0),
            None,
        ),
        (
            "offset delta past the span",
            frame(CreateTime, 100, 110),
            deltas(5, 0),
            None,
        ),
        (
            "negative span",
            RestoreBatchFrame {
                last_offset_delta: -1,
                ..frame(CreateTime, 100, 110)
            },
            deltas(0, 0),
            None,
        ),
        (
            "offset overflow",
            RestoreBatchFrame {
                base_offset: i64::MAX,
                ..frame(CreateTime, 100, 110)
            },
            deltas(1, 0),
            None,
        ),
        (
            "extreme but representable",
            RestoreBatchFrame {
                base_offset: i64::MAX,
                last_offset_delta: 0,
                ..frame(CreateTime, i64::MAX, i64::MAX)
            },
            deltas(0, 0),
            Some((i64::MAX, i64::MAX)),
        ),
    ] {
        check!(
            restore_record_coordinates(frame, record) == expected,
            "{name}"
        );
    }
}

#[test]
fn rewritten_headers_admit_exactly_kafka_producer_and_control_layouts() {
    let idempotent = RestoreProducer {
        producer_id: 7,
        producer_epoch: 2,
        base_sequence: 4,
        ..NON_IDEMPOTENT
    };
    let transactional = RestoreProducer {
        transactional: true,
        ..idempotent
    };
    let marker = RestoreProducer {
        control: true,
        base_sequence: -1,
        ..transactional
    };
    let barrier = RestoreProducer {
        control: true,
        ..NON_IDEMPOTENT
    };
    for (name, layout, producer, expected) in [
        (
            "non-idempotent data",
            layout(10, 2, 2),
            NON_IDEMPOTENT,
            Some(13),
        ),
        (
            "idempotent emptied data",
            layout(10, 2, 0),
            idempotent,
            Some(13),
        ),
        (
            "transactional data",
            layout(10, 2, 3),
            transactional,
            Some(13),
        ),
        ("transaction marker", layout(10, 0, 1), marker, Some(11)),
        (
            "compacted-away marker kept empty",
            layout(10, 0, 0),
            marker,
            Some(11),
        ),
        ("barrier control batch", layout(0, 0, 1), barrier, Some(1)),
        (
            "control spanning two offsets",
            layout(10, 1, 1),
            barrier,
            None,
        ),
        ("control with two records", layout(10, 0, 2), barrier, None),
        (
            "marker carrying a sequence",
            layout(10, 0, 1),
            RestoreProducer {
                base_sequence: 4,
                ..marker
            },
            None,
        ),
        (
            "transactional marker without producer",
            layout(10, 0, 1),
            RestoreProducer {
                transactional: true,
                ..barrier
            },
            None,
        ),
        (
            "transactional data without producer",
            layout(10, 2, 2),
            RestoreProducer {
                transactional: true,
                ..NON_IDEMPOTENT
            },
            None,
        ),
        (
            "partial sentinel epoch",
            layout(10, 2, 2),
            RestoreProducer {
                producer_epoch: 0,
                ..NON_IDEMPOTENT
            },
            None,
        ),
        (
            "producer without epoch",
            layout(10, 2, 2),
            RestoreProducer {
                producer_epoch: -1,
                ..idempotent
            },
            None,
        ),
        ("negative span", layout(0, -1, 1), NON_IDEMPOTENT, None),
        ("negative count", layout(10, 2, -1), NON_IDEMPOTENT, None),
        ("negative base", layout(-1, 0, 1), NON_IDEMPOTENT, None),
        (
            "frontier overflow",
            layout(i64::MAX, 0, 0),
            NON_IDEMPOTENT,
            None,
        ),
    ] {
        check!(
            restore_rewritten_batch_header(layout, producer) == expected,
            "{name}"
        );
    }
}
