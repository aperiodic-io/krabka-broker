//! Total functions over log offsets: the append invariant, the commit window a
//! high-watermark advance opens, and the half-open ranges a fetch may read
//! from. They are free functions so each rule can be checked without an engine.

use {crate::error::RaftError, krabka_ids::Offset};

pub fn assigned_record_offset(assign_base: Offset, delta: i64) -> i64 {
    assign_base.0.saturating_add(delta)
}

pub fn append_result_is_consistent(
    expected_base: Offset,
    returned_base: Offset,
    log_end_after: Offset,
) -> bool {
    returned_base.cmp(&expected_base).is_eq() && log_end_after.cmp(&expected_base).is_gt()
}

pub fn validate_append_result(
    context: &str,
    expected_base: Offset,
    returned_base: Offset,
    log_end_after: Offset,
) -> Result<(), RaftError> {
    if append_result_is_consistent(expected_base, returned_base, log_end_after) {
        Ok(())
    } else {
        Err(RaftError::ChangeRejected(format!(
            "{context} append invariant failed: expected base {expected_base}, got {returned_base}, log end {log_end_after}"
        )))
    }
}

pub fn submit_waiter_need_offset(base: Offset, blob_count: usize) -> Offset {
    base + i64::try_from(blob_count).unwrap_or(1)
}

/// Whether the leader's own append is already a majority, so it commits
/// without a follower fetch: the voter set holds one voter, and that voter is
/// the leader.
///
/// A leader that an uncommitted `VotersRecord` has removed still leads until
/// the record commits. If the new set holds one voter, that voter is another
/// node, and the leader's log does not count toward it. Only that voter's
/// fetch can commit the record.
pub fn leader_alone_is_majority(majority: usize, leader_is_voter: bool) -> bool {
    leader_is_voter && matches!(majority, 1)
}

pub fn batch_base_in_apply_window(base_offset: i64, prev_hwm: Offset, applied_hwm: Offset) -> bool {
    krabka_verified::raft::in_half_open_window(base_offset, prev_hwm.0, applied_hwm.0)
}

pub fn committed_records_since_snapshot(hwm: Offset, last_snapshot_end_offset: Offset) -> u64 {
    u64::try_from(hwm.0.saturating_sub(last_snapshot_end_offset.0)).unwrap_or(0)
}

pub fn snapshot_interval_reached(advanced: u64, snapshot_interval_records: u64) -> bool {
    snapshot_interval_records != 0
        && matches!(
            advanced.cmp(&snapshot_interval_records),
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
        )
}

/// (KIP-630) whether the bytes committed since the last snapshot have reached
/// `max_bytes_between_snapshots`. `0` disables the byte-size cap.
pub fn snapshot_bytes_reached(bytes_since_snapshot: u64, max_bytes_between_snapshots: u64) -> bool {
    max_bytes_between_snapshots != 0 && bytes_since_snapshot >= max_bytes_between_snapshots
}

/// (KIP-630) whether the wall-clock time since the last snapshot has reached
/// `max_snapshot_interval_ms`. `0` disables the time-based cap.
pub fn snapshot_time_reached(elapsed_ms: u64, max_snapshot_interval_ms: u64) -> bool {
    max_snapshot_interval_ms != 0 && elapsed_ms >= max_snapshot_interval_ms
}

pub fn expected_hwm_after_advance(prev_hwm: Offset, new_hwm: Offset, log_end: Offset) -> Offset {
    Offset(krabka_verified::raft::advance_high_watermark(
        prev_hwm.0, new_hwm.0, log_end.0,
    ))
}

pub fn hwm_advanced_as_expected(applied_hwm: Offset, expected_hwm: Offset) -> bool {
    krabka_verified::raft::frontier_reaches(applied_hwm.0, expected_hwm.0)
}

pub fn hwm_reaches_waiter(hwm: Offset, need_offset: Offset) -> bool {
    krabka_verified::raft::frontier_reaches(hwm.0, need_offset.0)
}

pub fn metadata_fetch_offset_in_committed_window(
    fetch_offset: Offset,
    high_watermark: Offset,
) -> bool {
    krabka_verified::raft::in_half_open_window(fetch_offset.0, 0, high_watermark.0)
}

/// Whether an observer's metadata fetch has fallen below the retained log, so
/// the records it asks for are gone and it must install a snapshot instead.
/// The window is half-open: a fetch *at* `log_start` still reads from the log.
pub fn metadata_fetch_offset_below_log_start(fetch_offset: Offset, log_start: Offset) -> bool {
    krabka_verified::raft::in_half_open_window(fetch_offset.0, 0, log_start.0)
}

pub fn fetch_batch_committed_before_hwm(base_offset: i64, high_watermark: Offset) -> bool {
    krabka_verified::raft::in_half_open_window(base_offset, i64::MIN, high_watermark.0)
}

pub fn fetch_offset_has_records(fetch_offset: Offset, log_end: Offset) -> bool {
    krabka_verified::raft::in_half_open_window(fetch_offset.0, 0, log_end.0)
}
