use super::*;

#[yare::parameterized(
    read_uncommitted_consumer = {
        false,
        false,
        OPEN_TXN,
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 8,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    read_committed_consumer = {
        false,
        true,
        OPEN_TXN,
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 6,
            effective_lso: 6,
            read_committed_aborts: true,
            response_hw: 8,
            response_lso: 6,
        },
    },
    follower_reads_to_log_end = {
        true,
        false,
        OPEN_TXN,
        8,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 10,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    caught_up_follower = {
        true,
        false,
        OPEN_TXN,
        10,
        FetchVisibility {
            out_of_range: false,
            empty: true,
            limit_offset: 10,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    below_log_start = {
        false,
        false,
        OPEN_TXN,
        1,
        FetchVisibility {
            out_of_range: true,
            empty: false,
            limit_offset: 8,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    lso_capped_at_high_watermark = {
        false,
        true,
        FetchWatermarks { lso: 9, ..OPEN_TXN },
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 8,
            effective_lso: 8,
            read_committed_aborts: true,
            response_hw: 8,
            response_lso: 8,
        },
    },
    follower_ignores_delivery_watermark = {
        true,
        false,
        FetchWatermarks { deliverable: 2, ..OPEN_TXN },
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 10,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    consumer_caps_before_undelivered_batch = {
        false,
        false,
        FetchWatermarks { deliverable: 5, ..OPEN_TXN },
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 5,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    consumer_at_delivery_watermark_is_empty = {
        false,
        false,
        FetchWatermarks { deliverable: 5, ..OPEN_TXN },
        5,
        FetchVisibility {
            out_of_range: false,
            empty: true,
            limit_offset: 5,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    read_committed_uses_lowest_delivery_bound = {
        false,
        true,
        FetchWatermarks { deliverable: 4, ..OPEN_TXN },
        3,
        FetchVisibility {
            out_of_range: false,
            empty: false,
            limit_offset: 4,
            effective_lso: 6,
            read_committed_aborts: true,
            response_hw: 8,
            response_lso: 6,
        },
    },
    delivery_above_high_watermark_is_capped = {
        false,
        false,
        FetchWatermarks { deliverable: 10, ..OPEN_TXN },
        8,
        FetchVisibility {
            out_of_range: false,
            empty: true,
            limit_offset: 8,
            effective_lso: 6,
            read_committed_aborts: false,
            response_hw: 8,
            response_lso: 6,
        },
    },
    delivery_below_log_start_leaves_empty_window = {
        false,
        true,
        FetchWatermarks { deliverable: 0, ..OPEN_TXN },
        2,
        FetchVisibility {
            out_of_range: false,
            empty: true,
            limit_offset: 0,
            effective_lso: 6,
            read_committed_aborts: true,
            response_hw: 8,
            response_lso: 6,
        },
    },
)]
fn fetch_visibility_matches_kafka_fetch_scenarios(
    is_follower: bool,
    read_committed: bool,
    watermarks: FetchWatermarks,
    fetch_offset: i64,
    expected: FetchVisibility,
) {
    // Followers and consumers both report the partition's own high watermark
    // and last stable offset, even when their read bounds differ.
    assert2::assert!(
        fetch_visibility(is_follower, read_committed, watermarks, fetch_offset) == expected
    );
}

#[derive(Clone, Copy)]
struct FetchContext {
    is_follower: bool,
    read_committed: bool,
}

fn assert_offset_visibility_case(
    context: FetchContext,
    limit_offset: i64,
    read_committed_aborts: bool,
    offset: i64,
    out_of_range: bool,
    empty: bool,
) {
    let watermarks = FetchWatermarks {
        log_start: 2,
        hw: 8,
        lso: 7,
        log_end: 10,
        deliverable: 8,
    };
    assert2::assert!(
        fetch_visibility(
            context.is_follower,
            context.read_committed,
            watermarks,
            offset
        ) == FetchVisibility {
            out_of_range,
            empty,
            limit_offset,
            effective_lso: 7,
            read_committed_aborts,
            response_hw: 8,
            response_lso: 7,
        }
    );
}

#[yare::parameterized(
    uncommitted_below_log_start = { 8, false, 1, true, false },
    uncommitted_at_log_start = { 8, false, 2, false, false },
    uncommitted_between_high_watermark_and_log_end = { 8, false, 9, false, true },
    uncommitted_at_log_end = { 8, false, 10, false, true },
    uncommitted_beyond_log_end = { 8, false, 11, true, false },
    uncommitted_maximum_offset = { 8, false, i64::MAX, true, false },
    read_committed_below_log_start = { 7, true, 1, true, false },
    read_committed_at_log_start = { 7, true, 2, false, false },
    read_committed_between_high_watermark_and_log_end = { 7, true, 9, false, true },
    read_committed_at_log_end = { 7, true, 10, false, true },
    read_committed_beyond_log_end = { 7, true, 11, true, false },
    read_committed_maximum_offset = { 7, true, i64::MAX, true, false },
)]
fn consumer_fetch_offsets_are_classified(
    limit_offset: i64,
    read_committed: bool,
    offset: i64,
    out_of_range: bool,
    empty: bool,
) {
    // Kafka accepts LEO itself as an empty read; offsets beyond LEO are errors.
    assert_offset_visibility_case(
        FetchContext {
            is_follower: false,
            read_committed,
        },
        limit_offset,
        read_committed,
        offset,
        out_of_range,
        empty,
    );
}

#[yare::parameterized(
    uncommitted_below_log_start = { false, 1, true, false },
    uncommitted_at_log_start = { false, 2, false, false },
    uncommitted_before_log_end = { false, 9, false, false },
    uncommitted_at_log_end = { false, 10, false, true },
    uncommitted_beyond_log_end = { false, 11, true, false },
    uncommitted_maximum_offset = { false, i64::MAX, true, false },
    read_committed_below_log_start = { true, 1, true, false },
    read_committed_at_log_start = { true, 2, false, false },
    read_committed_before_log_end = { true, 9, false, false },
    read_committed_at_log_end = { true, 10, false, true },
    read_committed_beyond_log_end = { true, 11, true, false },
    read_committed_maximum_offset = { true, i64::MAX, true, false },
)]
fn follower_fetch_offsets_are_classified(
    read_committed: bool,
    offset: i64,
    out_of_range: bool,
    empty: bool,
) {
    assert_offset_visibility_case(
        FetchContext {
            is_follower: true,
            read_committed,
        },
        10,
        false,
        offset,
        out_of_range,
        empty,
    );
}
