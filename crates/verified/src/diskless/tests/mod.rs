use assert2::check;

use super::*;

#[derive(Clone, Copy)]
struct RetentionMillis(i64);

#[derive(Clone, Copy)]
struct RetentionBytes(u64);

#[derive(Clone, Copy, Default)]
struct LogicalOffset(i64);

#[derive(Clone, Copy)]
struct UnixMillis(i64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DisklessRetentionSetup {
    retention: Option<RetentionMillis>,
    bytes: Option<RetentionBytes>,
    start: LogicalOffset,
    #[default(UnixMillis(1_000))]
    now: UnixMillis,
}

fn policy(setup: DisklessRetentionSetup) -> DisklessRetentionPolicy {
    DisklessRetentionPolicy {
        retention_ms: setup.retention.map(|duration| duration.0),
        retention_bytes: setup.bytes.map(|limit| limit.0),
        log_start_offset: setup.start.0,
        now_ms: setup.now.0,
    }
}

mod reclaim_needs_the_grace_period_and_no_reference;

mod logical_range_selects_the_cover_or_the_successor_after_a_gap;
