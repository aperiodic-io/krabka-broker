use super::*;

#[yare::parameterized(
    remote_checkpoint_below_local = { true, Some(3), 8, 3 },
    remote_without_checkpoint = { true, None, 8, 0 },
    local_uses_segment_base_over_checkpoint = { false, Some(3), 8, 8 },
    local_without_checkpoint = { false, None, 8, 8 },
)]
fn reload_log_start_follows_kafka_log_loader(
    remote: bool,
    established: Option<i64>,
    local: i64,
    expected: i64,
) {
    assert2::check!(producer_snapshot_reload_log_start(remote, established, local) == expected);
}

#[yare::parameterized(
    below_log_start = { 4, false },
    at_log_start = { 5, false },
    inside_log_range = { 6, true },
    at_log_end = { 10, true },
    above_log_end = { 11, false },
    negative_offset = { -1, false },
)]
fn reload_keeps_exactly_the_half_open_range_above_the_log_start(offset: i64, kept: bool) {
    assert2::check!(producer_snapshot_reload_keeps(offset, RANGE) == kept);
}

#[yare::parameterized(
    all_match_segment_bases = {
        &[0_i64, 4, 8][..],
        &[0_i64, 4, 8][..],
        &[false, false, false][..],
    },
    older_strays_removed = { &[2_i64, 4, 6][..], &[0_i64, 4, 8][..], &[true, false, true][..] },
    clean_shutdown_snapshot_preserved = {
        &[4_i64, 8, 11][..],
        &[0_i64, 4, 8][..],
        &[false, false, false][..],
    },
    only_newest_above_bases_preserved = {
        &[9_i64, 11][..],
        &[0_i64, 4, 8][..],
        &[true, false][..],
    },
    newest_snapshot_without_segments = { &[3_i64, 7][..], &[][..], &[true, false][..] },
    input_order_is_irrelevant = {
        &[11_i64, 2, 4][..],
        &[8_i64, 0, 4][..],
        &[false, true, false][..],
    },
)]
fn stray_snapshots_follow_kafka_remove_stray_snapshots(
    snapshots: &[i64],
    bases: &[i64],
    removed: &[bool],
) {
    let actual: Vec<_> = (0..removed.len())
        .map(|index| producer_snapshot_stray(snapshots, index, bases))
        .collect();
    assert2::check!(actual == removed);
}

#[yare::parameterized(
    no_snapshots = { &[][..], RANGE, None },
    all_offsets_out_of_range = { &[3, 5, 11, -1][..], RANGE, None },
    newest_kept_in_unsorted_list = { &[6, 10, 7, 12][..], RANGE, Some(1) },
    start_boundary_is_excluded = { &[5, 6][..], RANGE, Some(1) },
    duplicate_latest_keeps_first = { &[10, 10][..], RANGE, Some(0) },
    maximum_offset_is_valid = { &[i64::MAX][..], ProducerReloadRange {
        log_start: i64::MAX - 1, local_start: i64::MAX - 1, log_end: i64::MAX,
    }, Some(0) },
)]
fn latest_snapshot_is_the_newest_one_the_reload_keeps(
    offsets: &[i64],
    range: ProducerReloadRange,
    expected: Option<usize>,
) {
    assert2::check!(producer_snapshot_latest_index(offsets, range) == expected);
}

/// One row per validity rule over a snapshot at offset 10, each a change
/// to one valid entry: producer 7 at epoch 2, whose last batch is
/// sequences ..=4 at offsets 6..=9 (delta 3), with a transaction open
/// since offset 5 under coordinator epoch 0.
const VALID: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
    producer_id: 7,
    producer_epoch: 2,
    last_sequence: 4,
    last_offset: 9,
    offset_delta: 3,
    coordinator_epoch: 0,
    current_txn_first_offset: 5,
};
const MARKER_ONLY: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
    producer_id: 0,
    producer_epoch: 0,
    last_sequence: -1,
    last_offset: -1,
    offset_delta: 0,
    coordinator_epoch: -1,
    current_txn_first_offset: -1,
};

#[yare::parameterized(
    real_producer = { 10, VALID, true },
    marker_only_producer = { 10, MARKER_ONLY, true },
    integer_extremes = { i64::MAX, ProducerSnapshotEntryFacts {
        producer_id: i64::MAX, producer_epoch: i16::MAX, last_sequence: i32::MAX,
        last_offset: i64::MAX - 1, offset_delta: i32::MAX, coordinator_epoch: i32::MAX,
        current_txn_first_offset: i64::MAX - 2,
    }, true },
    negative_snapshot_offset = { -1, MARKER_ONLY, false },
    negative_producer_id = { 10, ProducerSnapshotEntryFacts { producer_id: -1, ..VALID }, false },
    negative_producer_epoch = {
        10,
        ProducerSnapshotEntryFacts { producer_epoch: -1, ..VALID },
        false,
    },
    coordinator_epoch_below_sentinel = { 10, ProducerSnapshotEntryFacts {
        coordinator_epoch: -2, ..VALID
    }, false },
    last_record_at_boundary = {
        10,
        ProducerSnapshotEntryFacts { last_offset: 10, ..VALID },
        false,
    },
    delta_below_zero = { 10, ProducerSnapshotEntryFacts {
        last_offset: 0, offset_delta: 1, current_txn_first_offset: -1, ..VALID
    }, false },
    sentinel_with_delta = {
        10,
        ProducerSnapshotEntryFacts { offset_delta: 1, ..MARKER_ONLY },
        false,
    },
    transaction_at_boundary = { 10, ProducerSnapshotEntryFacts {
        current_txn_first_offset: 10, ..VALID
    }, false },
    transaction_after_last_record = { 10, ProducerSnapshotEntryFacts {
        last_offset: 4, offset_delta: 0, ..VALID
    }, false },
    transaction_below_sentinel = { 10, ProducerSnapshotEntryFacts {
        current_txn_first_offset: -2, ..VALID
    }, false },
)]
fn entry_validation_is_exact_and_snapshot_bounded(
    snapshot_offset: i64,
    entry: ProducerSnapshotEntryFacts,
    expected: bool,
) {
    assert2::check!(producer_snapshot_entry_valid(snapshot_offset, entry) == expected);
}
