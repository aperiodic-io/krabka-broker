use super::*;

#[yare::parameterized(
    first_record = {
        "first record",
        None,
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(1, 5),
        Some((11, 105)),
    },
    max_timestamp = {
        "last record at the max",
        Some(1),
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(4, 10),
        Some((14, 110)),
    },
    repeated_delta = {
        "repeated delta",
        Some(1),
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(1, 5),
        None,
    },
    regressing_delta = {
        "regressing delta",
        Some(3),
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(2, 5),
        None,
    },
    past_span = {
        "past the span",
        None,
        frame(RestoreTimestampType::CreateTime, 100, 110),
        deltas(5, 5),
        None,
    },
    create_time_above_max = {
        "create time above the max",
        None,
        frame(RestoreTimestampType::CreateTime, 100, 104),
        deltas(1, 5),
        None,
    },
    create_time_overflow = {
        "create time overflow",
        None,
        frame(RestoreTimestampType::CreateTime, i64::MAX, i64::MAX),
        deltas(1, 1),
        None,
    },
    log_append_clock_ahead = {
        "log append time with producer clock ahead",
        None,
        frame(RestoreTimestampType::LogAppendTime, 2_000, 1_000),
        deltas(0, 0),
        Some((10, 1_000)),
    },
    log_append_later_record = {
        "log append time later record",
        Some(0),
        frame(RestoreTimestampType::LogAppendTime, 2_000, 1_000),
        deltas(3, 900),
        Some((13, 1_000)),
    },
    log_append_still_strict = {
        "log append time still strict",
        Some(3),
        frame(RestoreTimestampType::LogAppendTime, 2_000, 1_000),
        deltas(3, 0),
        None,
    },
)]
fn rewritten_records_are_strict_bounded_and_timestamp_checked(
    name: &'static str,
    previous: Option<i32>,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
    expected: Option<(i64, i64)>,
) {
    check!(
        restore_rewritten_record(previous, frame, record) == expected,
        "{name}"
    );
}

#[yare::parameterized(
    inside_bounds = { "inside both bounds", 10, 99, Some(10), Some(100), NONE, true },
    past_offset = { "past the offset bound", 11, 99, Some(10), Some(100), NONE, false },
    at_timestamp = { "at the timestamp bound", 10, 100, Some(10), Some(100), NONE, false },
    producer_excluded = {
        "producer excluded",
        10,
        99,
        Some(10),
        Some(100),
        RestoreExclusions { producer: true, ..NONE },
        false,
    },
    offset_excluded = {
        "offset excluded",
        10,
        99,
        Some(10),
        Some(100),
        RestoreExclusions { offset: true, ..NONE },
        false,
    },
    key_excluded = {
        "key excluded",
        10,
        99,
        Some(10),
        Some(100),
        RestoreExclusions { content: RestoreContentExclusions { key: true, ..NO_CONTENT }, ..NONE },
        false,
    },
    header_excluded = {
        "header excluded",
        10,
        99,
        Some(10),
        Some(100),
        RestoreExclusions { content: RestoreContentExclusions { header: true, ..NO_CONTENT }, ..NONE },
        false,
    },
    unbounded = { "unbounded", i64::MIN, i64::MAX, None, None, NONE, true },
)]
fn record_selection_uses_inclusive_offset_and_exclusive_time_bounds(
    name: &'static str,
    offset: i64,
    timestamp: i64,
    max_offset: Option<i64>,
    max_timestamp: Option<i64>,
    exclusions: RestoreExclusions,
    expected: bool,
) {
    check!(
        restore_record_selected(offset, max_offset, timestamp, max_timestamp, exclusions)
            == expected,
        "{name}"
    );
}

#[test]
fn batch_fold_and_skip_are_exact() {
    check!(restore_batch_filter_decision(false, false) == RestoreFilterDecision::Keep);
    check!(restore_batch_filter_decision(true, false) == RestoreFilterDecision::Keep);
    check!(restore_batch_filter_decision(false, true) == RestoreFilterDecision::Empty);
    check!(restore_batch_filter_decision(true, true) == RestoreFilterDecision::Filter);
    check!(!restore_batch_past_offset_bound(10, Some(10)));
    check!(restore_batch_past_offset_bound(11, Some(10)));
    check!(!restore_batch_past_offset_bound(i64::MAX, None));
}
