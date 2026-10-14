use {super::*, crate::log_index::IndexEntry};

#[yare::parameterized(
    ordinary_range = { 100, 104, Some(4) },
    zero_base = { 0, 0, Some(0) },
    largest_relative_span = { 0, i64::from(u32::MAX), Some(i64::from(u32::MAX)) },
    first_span_over_limit = { 0, i64::from(u32::MAX) + 1, None },
    signed_overflow = { 0, i64::MAX, None },
    negative_base = { -1, 104, None },
    end_before_base = { 104, 100, None },
)]
fn index_frontier_is_the_u32_relative_span(base: i64, end: i64, expected: Option<i64>) {
    check!(restore_index_frontier(base, end) == expected);
}

#[test]
fn offset_and_time_index_entries_are_strict_and_bounded() {
    check!(restore_offset_index_entry_valid(None, 0, 0, 4, 100));
    check!(restore_offset_index_entry_valid(
        Some(IndexEntry::new(0, 0)),
        3,
        50,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(
        Some(IndexEntry::new(3, 50)),
        3,
        60,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(
        Some(IndexEntry::new(0, 50)),
        3,
        50,
        4,
        100
    ));
    check!(!restore_offset_index_entry_valid(None, 5, 0, 4, 100));
    check!(!restore_offset_index_entry_valid(None, 0, 100, 4, 100));
    check!(restore_offset_index_entry_valid(
        None,
        u32::MAX,
        0,
        i64::from(u32::MAX),
        1
    ));

    check!(restore_time_index_entry_valid(None, 10, 0, 4));
    // krabka's writer repeats the running maximum timestamp.
    check!(restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 0)),
        10,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 3)),
        11,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(
        Some(IndexEntry::new(10, 0)),
        9,
        3,
        4
    ));
    check!(!restore_time_index_entry_valid(None, 10, 5, 4));
}

#[yare::parameterized(
    one_abort = { vec![txn(7, 100, 102)], SEGMENT, true },
    sequential_aborts = { vec![txn(7, 100, 102), txn(8, 103, 104)], SEGMENT, true },
    interleaved_abort_marker_order = { vec![txn(2, 105, 108), txn(1, 100, 110)], SEGMENT, true },
    transaction_spans_segment_roll = {
        vec![txn(1_000, 0, 3)],
        RestoreSegmentExtent { base_offset: 3, last_offset: 4 },
        true,
    },
    marker_order_regresses = { vec![txn(1, 100, 110), txn(2, 105, 108)], SEGMENT, false },
    repeated_marker = { vec![txn(1, 100, 104), txn(2, 101, 104)], SEGMENT, false },
    marker_before_segment = { vec![txn(7, 90, 99)], SEGMENT, false },
    marker_past_segment = { vec![txn(7, 100, 111)], SEGMENT, false },
    start_after_marker = { vec![txn(7, 105, 104)], SEGMENT, false },
    negative_start = { vec![txn(7, -1, 104)], SEGMENT, false },
    negative_producer = { vec![txn(-1, 100, 102)], SEGMENT, false },
)]
fn transaction_index_follows_kafka_marker_order(
    entries: Vec<RestoreAbortedTxn>,
    segment: RestoreSegmentExtent,
    expected: bool,
) {
    check!(txn_index_valid(&entries, segment) == expected);
}

#[test]
fn leader_epoch_entries_are_strict_and_bounded() {
    check!(restore_leader_epoch_entry_valid(None, 0, 100, 100, 104));
    check!(restore_leader_epoch_entry_valid(
        Some((0, 100)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(
        Some((1, 100)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(
        Some((0, 103)),
        1,
        103,
        100,
        104
    ));
    check!(!restore_leader_epoch_entry_valid(None, -1, 100, 100, 104));
    check!(!restore_leader_epoch_entry_valid(None, 0, 105, 100, 104));
}

#[test]
fn producer_ids_are_canonical_and_unique() {
    check!(restore_producer_ids_strict(&[]));
    check!(restore_producer_ids_strict(&[7]));
    check!(restore_producer_ids_strict(&[7, 8, 9]));
    check!(!restore_producer_ids_strict(&[7, 9, 8]));
    check!(!restore_producer_ids_strict(&[7, 9, 9]));
}
