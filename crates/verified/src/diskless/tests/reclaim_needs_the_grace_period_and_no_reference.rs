use super::*;

const THREE_TIMESTAMPS: &[i64] = &[10, 20, 30];
const THREE_BYTE_LENS: &[u64] = &[100, 100, 100];
const THREE_LAST_OFFSETS: &[i64] = &[4, 9, 14];

#[test]
fn reclaim_needs_the_grace_period_and_no_reference() {
    check!(diskless_object_reclaimable(false, true));
    check!(!diskless_object_reclaimable(true, true));
    check!(!diskless_object_reclaimable(false, false));
}

#[yare::parameterized(
    below_hwm_within_lag = { 90, 100, 10, 50, (true, 90) },
    hwm_below_frontier = { 100, 90, 10, 50, (true, 80) },
    frontier_equals_current = { 80, 90, 10, 80, (false, 80) },
    frontier_below_current = { 70, 90, 10, 80, (false, 80) },
    maximum_frontier = { i64::MAX, i64::MAX, 0, i64::MAX - 1, (true, i64::MAX) },
    saturated_subtraction = { i64::MAX, 0, i64::MAX, 0, (false, 0) },
    negative_lag = { 10, 10, -1, 0, (true, 10) },
    negative_frontier = { -1, 10, 0, 0, (false, 0) },
    negative_hwm = { 10, -1, 0, 0, (false, 0) },
    negative_current = { 10, 10, 0, -1, (false, -1) },
    all_zero = { 0, 0, 0, 0, (false, 0) },
)]
fn trim_is_bounded_non_regressing_and_overflow_safe(
    frontier: i64,
    high_watermark: i64,
    lag: i64,
    current: i64,
    expected: (bool, i64),
) {
    let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
    check!(
        decision
            == DisklessTrimDecision {
                should_trim: expected.0,
                target: expected.1,
            }
    );
}

/// Every case runs over the same three ranges: 100 bytes each, covering
/// offsets 0-4, 5-9 and 10-14, read at `now_ms = 1_000`. Only the batch
/// timestamps and the topic's retention change, which is what each Kafka
/// predicate keys on.
#[yare::parameterized(
    no_policy = { [10, 20, 30], None, None, 0, 0 },
    time_keeps_newest_range = { [100, 200, 900], Some(500), None, 0, 2 },
    time_past_every_range = { [100, 200, 300], Some(500), None, 0, 2 },
    time_stops_before_first_kept = { [100, 900, 100], Some(500), None, 0, 1 },
    bytes_oldest_only = { [10, 20, 30], None, Some(150), 0, 1 },
    bytes_exact_debt = { [10, 20, 30], None, Some(200), 0, 1 },
    bytes_budget_too_small = { [10, 20, 30], None, Some(250), 0, 0 },
    index_already_fits_budget = { [10, 20, 30], None, Some(300), 0, 0 },
    delete_floor = { [10, 20, 30], None, None, 5, 1 },
    floor_past_every_range = { [10, 20, 30], None, None, 99, 2 },
    predicates_union = { [100, 100, 900], Some(500), None, 5, 2 },
    expired_range_still_pays_size_debt = { [100, 900, 900], Some(500), Some(150), 0, 1 },
)]
fn retention_prefix_applies_each_kafka_predicate_and_keeps_the_newest_range(
    max_timestamps: [i64; 3],
    retention_ms: Option<i64>,
    retention_bytes: Option<u64>,
    floor: i64,
    expired: usize,
) {
    const BYTE_LENS: [u64; 3] = [100, 100, 100];
    const LAST_OFFSETS: [i64; 3] = [4, 9, 14];
    const NOW_MS: i64 = 1_000;

    let prefix = diskless_retention_prefix(
        &max_timestamps,
        &BYTE_LENS,
        &LAST_OFFSETS,
        policy()
            .maybe_retention(retention_ms.map(RetentionMillis))
            .maybe_bytes(retention_bytes.map(RetentionBytes))
            .start(floor)
            .now(NOW_MS)
            .call(),
    );
    check!(prefix == expired);
}

#[yare::parameterized(
    floor_equal_to_last_offset = {
        &[100, 200][..],
        &[10, 10][..],
        &[10, 20][..],
        policy().start(10).call(),
        0,
    },
    timestamp_equal_to_horizon = {
        &[500, 900][..],
        &[10, 10][..],
        &[10, 20][..],
        policy().maybe_retention(Some(500)).call(),
        0,
    },
    zero_diff_zero_byte_range = {
        &[100, 200][..],
        &[0, 10][..],
        &[10, 20][..],
        policy().maybe_bytes(Some(10)).call(),
        1,
    },
    horizon_below_min = {
        THREE_TIMESTAMPS,
        THREE_BYTE_LENS,
        THREE_LAST_OFFSETS,
        policy().maybe_retention(Some(1)).now(i64::MIN).call(),
        0,
    },
    horizon_above_max = {
        THREE_TIMESTAMPS,
        THREE_BYTE_LENS,
        THREE_LAST_OFFSETS,
        policy().maybe_retention(Some(-1)).now(i64::MAX).call(),
        0,
    },
    size_sum_does_not_overflow = {
        THREE_TIMESTAMPS,
        &[u64::MAX, u64::MAX, u64::MAX][..],
        THREE_LAST_OFFSETS,
        policy().maybe_bytes(Some(u64::MAX)).call(),
        2,
    },
    newest_range_is_preserved = {
        &[10][..],
        &[100][..],
        &[4][..],
        policy().maybe_retention(Some(1)).maybe_bytes(Some(0)).start(99).call(),
        0,
    },
    empty_index = {
        &[][..],
        &[][..],
        &[][..],
        policy().maybe_retention(Some(1)).maybe_bytes(Some(0)).start(99).call(),
        0,
    },
)]
fn retention_prefix_boundaries_follow_kafkas_strict_and_inclusive_comparisons(
    max_timestamps: &[i64],
    byte_lens: &[u64],
    last_offsets: &[i64],
    policy: DisklessRetentionPolicy,
    expired: usize,
) {
    check!(diskless_retention_prefix(max_timestamps, byte_lens, last_offsets, policy) == expired);
}
