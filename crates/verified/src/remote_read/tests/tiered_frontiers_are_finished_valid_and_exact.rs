use {super::*, crate::log_index::IndexEntry};

fn entries_with_duplicate_timestamp() -> [IndexEntry<i64, u32>; 4] {
    [
        IndexEntry::new(1_000, 0),
        IndexEntry::new(2_000, 10),
        IndexEntry::new(2_000, 20),
        IndexEntry::new(3_000, 30),
    ]
}

#[test]
fn tiered_frontiers_are_finished_valid_and_exact() {
    let starts = [20, 0, -1, 40];
    let ends = [39, 19, 100, 59];
    let finished = [true, true, true, false];
    check!(tiered_earliest_finished_index(&starts, &ends, &finished) == Some(1));
    check!(tiered_latest_finished_index(&starts, &ends, &finished) == Some(0));
    check!(tiered_earliest_finished_index(&[10, 20], &[15, 25], &[true, true]) == Some(0));
    check!(tiered_latest_finished_index(&[10, 20], &[15, 25], &[true, true]) == Some(1));
    check!(tiered_earliest_finished_index(&[], &[], &[]) == None);
    check!(tiered_latest_finished_index(&[], &[], &[]) == None);
}

#[test]
fn tiered_epoch_owner_has_the_greatest_valid_start() {
    let epochs = [0, 2, -1, 4];
    let starts = [20, 30, 38, 35];
    check!(tiered_owning_epoch_index(&epochs, &starts, 20, 39) == Some(3));
    check!(tiered_owning_epoch_index(&[0, 1], &[30, 20], 10, 50) == Some(0));
    check!(tiered_owning_epoch_index(&[7], &[40], 20, 39) == None);
}

#[yare::parameterized(
    before_first = {
        &entries_with_duplicate_timestamp()[..],
        500,
        0,
    },
    at_first = {
        &entries_with_duplicate_timestamp()[..],
        1_000,
        0,
    },
    between_first_second = {
        &entries_with_duplicate_timestamp()[..],
        1_500,
        1,
    },
    duplicate_timestamp = {
        &entries_with_duplicate_timestamp()[..],
        2_000,
        1,
    },
    after_duplicate = {
        &entries_with_duplicate_timestamp()[..],
        2_500,
        3,
    },
    after_last = {
        &entries_with_duplicate_timestamp()[..],
        4_000,
        4,
    },
)]
fn remote_time_index_selects_strict_predecessor_prefix(
    entries: &[IndexEntry<i64, u32>],
    target: i64,
    expected: usize,
) {
    check!(remote_time_index_candidate_count(entries, target) == expected);
}

#[yare::parameterized(
    zeroed_padding = {
        "zeroed padding ends the index",
        &[
            IndexEntry::new(1_000, 0),
            IndexEntry::new(2_000, 10),
            IndexEntry::new(0, 0),
            IndexEntry::new(9_000, 20),
        ][..],
        i64::MAX,
        2,
    },
    repeated_offset = {
        "a repeated offset is padding",
        &[IndexEntry::new(1_000, 0), IndexEntry::new(2_000, 10), IndexEntry::new(3_000, 10)][..],
        i64::MAX,
        2,
    },
    empty_index = { "an empty index", &[][..], i64::MAX, 0 },
    first_entry_at_target = {
        "the first entry at the target",
        &[IndexEntry::new(1_000, 0), IndexEntry::new(2_000, 10)][..],
        1_000,
        0,
    },
    unsorted_timestamps = {
        "unsorted timestamps stop at the first one at the target",
        &[IndexEntry::new(1_000, 0), IndexEntry::new(5_000, 10), IndexEntry::new(2_000, 20)][..],
        3_000,
        1,
    },
)]
fn remote_time_index_stops_at_padding_or_the_target(
    what: &str,
    entries: &[IndexEntry<i64, u32>],
    target: i64,
    expected: usize,
) {
    check!(
        remote_time_index_candidate_count(entries, target) == expected,
        "{what}"
    );
}

#[yare::parameterized(
    one_before = { 0, 2, 1, Some(0) },
    exact_end = { 0, 1, 1, None },
    empty_limit = { 0, 1, 0, None },
    max_minus_two = { u32::MAX - 2, u32::MAX, 1, Some(u32::MAX - 2) },
    max_minus_one = { u32::MAX - 1, u32::MAX, 1, None },
    max_boundary = { u32::MAX, u32::MAX, u32::MAX, None },
)]
fn fetch_end_position_handles_exact_and_overflow_boundaries(
    start: u32,
    segment: u32,
    max_bytes: u32,
    expected: Option<u32>,
) {
    check!(remote_fetch_end_position(start, segment, max_bytes) == expected);
}

#[yare::parameterized(
    first_offset = { 100, 199, 100, true, Some(100), None, Some(0) },
    last_offset = { 100, 199, 199, true, Some(100), None, Some(99) },
    before_start = { 100, 199, 99, true, Some(100), None, None },
    after_end = { 100, 199, 200, true, Some(100), None, None },
    reversed_range = { 200, 100, 150, true, Some(200), None, None },
    unfinished = { 100, 199, 150, false, Some(100), None, None },
    missing_epoch_start = { 100, 199, 150, true, None, None, None },
    before_next_epoch = { 0, 99, 49, true, Some(0), Some(50), Some(49) },
    exactly_before_next_epoch = { 0, 99, 49, true, Some(0), Some(99), Some(49) },
    at_next_epoch = { 0, 99, 50, true, Some(0), Some(50), None },
    epoch_start_after_request = { 0, 99, 49, true, Some(50), None, None },
    request_at_epoch_start = { 0, 99, 50, true, Some(50), None, Some(50) },
    next_epoch_after_range = { 0, 99, 49, true, Some(0), Some(100), None },
    overflowing_relative_offset = {
        i64::MIN,
        i64::MAX,
        i64::MAX,
        true,
        Some(i64::MIN),
        None,
        None,
    },
    zero_relative_offset = { i64::MIN, i64::MAX, i64::MIN, true, Some(i64::MIN), None, Some(0) },
    max_relative_offset = {
        i64::MIN,
        i64::MAX,
        i64::MIN + i64::from(u32::MAX),
        true,
        Some(i64::MIN),
        None,
        Some(u32::MAX),
    },
)]
fn admits_exact_finished_lineage_range_and_rejects_every_other_case(
    start: i64,
    end: i64,
    requested: i64,
    finished: bool,
    epoch_start: Option<i64>,
    next_epoch: Option<i64>,
    expected: Option<u32>,
) {
    check!(
        remote_read_relative_offset(start, end, requested, finished, epoch_start, next_epoch)
            == expected
    );
}
