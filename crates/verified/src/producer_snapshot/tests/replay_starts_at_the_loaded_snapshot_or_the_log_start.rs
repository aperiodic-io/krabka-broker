use super::*;

#[yare::parameterized(
    empty = { ProducerReloadRange { log_start: 0, local_start: 0, log_end: 0 }, None, Some(0) },
    no_snapshot = { RANGE, None, Some(5) },
    snapshot_inside = { RANGE, Some(7), Some(7) },
    snapshot_at_end = { RANGE, Some(10), Some(10) },
    snapshot_at_local_start = { RANGE, Some(5), None },
    snapshot_below_local_start = { RANGE, Some(3), None },
    snapshot_past_end = { RANGE, Some(11), None },
    negative_snapshot = { RANGE, Some(-1), None },
    tiered_no_snapshot = {
        ProducerReloadRange { log_start: 0, local_start: 5, log_end: 10 },
        None,
        Some(5),
    },
    tiered_snapshot_below_local = {
        ProducerReloadRange { log_start: 0, local_start: 5, log_end: 10 },
        Some(3),
        Some(5),
    },
    tiered_snapshot_in_local = {
        ProducerReloadRange { log_start: 0, local_start: 5, log_end: 10 },
        Some(8),
        Some(8),
    },
    trimmed_below_local = {
        ProducerReloadRange { log_start: 7, local_start: 5, log_end: 10 },
        None,
        Some(7),
    },
    reversed_range = {
        ProducerReloadRange { log_start: 5, local_start: 5, log_end: 4 },
        None,
        None,
    },
    negative_log_start = {
        ProducerReloadRange { log_start: -1, local_start: 0, log_end: 10 },
        None,
        None,
    },
    local_start_past_end = {
        ProducerReloadRange { log_start: 0, local_start: 11, log_end: 10 },
        None,
        None,
    },
    negative_local_start = {
        ProducerReloadRange { log_start: 0, local_start: -1, log_end: 10 },
        None,
        None,
    },
)]
fn replay_starts_at_the_loaded_snapshot_or_the_log_start(
    range: ProducerReloadRange,
    snapshot: Option<i64>,
    expected: Option<i64>,
) {
    assert2::check!(
        producer_snapshot_replay_start(range, snapshot) == expected,
        "{range:?} {snapshot:?}"
    );
}
