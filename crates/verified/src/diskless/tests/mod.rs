use {super::*, assert2::check};

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct RetentionMillis(i64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct RetentionBytes(u64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct LogicalOffset(i64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct UnixMillis(i64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DisklessRetentionSetup {
    retention: Option<RetentionMillis>,
    bytes: Option<RetentionBytes>,
    start: LogicalOffset,
    #[default(UnixMillis(1_000))]
    now: UnixMillis,
}

#[bon::builder]
fn policy(
    #[builder(into)] retention: Option<RetentionMillis>,
    #[builder(into)] bytes: Option<RetentionBytes>,
    #[builder(into, default)] start: LogicalOffset,
    #[builder(into, default = UnixMillis (1_000))] now: UnixMillis,
) -> DisklessRetentionPolicy {
    let setup = DisklessRetentionSetup {
        retention,
        bytes,
        start,
        now,
    };
    DisklessRetentionPolicy {
        retention_ms: setup.retention.map(|duration| duration.0),
        retention_bytes: setup.bytes.map(|limit| limit.0),
        log_start_offset: setup.start.0,
        now_ms: setup.now.0,
    }
}

mod reclaim_needs_the_grace_period_and_no_reference;

mod logical_range_selects_the_cover_or_the_successor_after_a_gap;
