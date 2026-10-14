use {super::*, crate::log_index::IndexEntry};

#[yare::parameterized(
    before_first = { -1, None },
    first_coordinate = { 0, Some(0) },
    gap_selects_successor = { 5, Some(1) },
    last_coordinate = { 15, Some(2) },
    after_last = { 16, None },
)]
fn logical_range_selects_the_cover_or_the_successor_after_a_gap(
    requested: i64,
    expected: Option<usize>,
) {
    let entries = [
        IndexEntry::new(0, 4),
        IndexEntry::new(7, 9),
        IndexEntry::new(12, 15),
    ];
    check!(diskless_logical_range(&entries, requested) == expected);
}

#[yare::parameterized(
    contiguous_within_cap = { 10, 5, 15, 7, true, 12, Some(12) },
    contiguous_over_cap = { 10, 5, 15, 7, true, 11, None },
    gap = { 10, 5, 16, 7, true, 12, None },
    different_object = { 10, 5, 15, 7, false, 12, None },
    end_overflow = { u64::MAX, 1, 0, 1, true, 2, None },
    total_overflow = { 0, u64::MAX, u64::MAX, 1, true, u64::MAX, None },
)]
fn span_extends_only_across_a_contiguous_range_of_the_same_object(
    start: u64,
    len: u64,
    next_start: u64,
    next_len: u64,
    same_object: bool,
    max_bytes: u64,
    expected: Option<u64>,
) {
    check!(
        diskless_span_extension(start, len, next_start, next_len, same_object, max_bytes)
            == expected
    );
}

#[yare::parameterized(
    below_floor = { None, 0, 10, 0, 0, 1, 5, DisklessBatchStep::Skip(10) },
    at_floor = { None, 10, 10, 1, 0, 1, 5, DisklessBatchStep::Start(20) },
    within_cap = { Some(10), 20, 10, 2, 0, 1, 20, DisklessBatchStep::Continue(30) },
    first_batch = { Some(20), 20, 10, 2, 0, 1, 20, DisklessBatchStep::Continue(30) },
    over_cap = { Some(10), 20, 10, 2, 0, 1, 19, DisklessBatchStep::Stop },
    empty = { None, 0, 0, 0, 0, 1, 5, DisklessBatchStep::Invalid },
    negative_delta = { None, 0, 10, 0, -1, 1, 5, DisklessBatchStep::Invalid },
    end_overflow = { None, usize::MAX, 1, 0, 0, 0, usize::MAX, DisklessBatchStep::Invalid },
    last_offset_overflow = { None, 0, 1, i64::MAX, 1, 0, usize::MAX, DisklessBatchStep::Invalid },
    selected_start_after_batch = { Some(21), 20, 10, 2, 0, 1, 20, DisklessBatchStep::Invalid },
)]
fn batch_steps_advance_by_the_encoded_length_or_stop(
    selected: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base: i64,
    delta: i32,
    floor: i64,
    max_bytes: usize,
    expected: DisklessBatchStep,
) {
    check!(
        diskless_batch_step(
            selected,
            batch_start,
            encoded_len,
            base,
            delta,
            floor,
            max_bytes
        ) == expected
    );
}
