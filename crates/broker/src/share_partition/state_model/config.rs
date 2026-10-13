//! The bounded model configurations: the cluster-shape knobs each run fixes,
//! the four named configs the tests drive, and the single acquisition-lock
//! duration the whole model shares.
//!
//! The config lives apart from the fingerprinted state because it never
//! changes during a run. Keeping the bounds and the reasoning that picked them
//! on one screen is what lets a reader check that a config is still both
//! memory-safe and non-vacuous.

use {
    krabka_log::Offset,
    std::time::{Duration, Instant},
};

/// The single acquisition-lock duration used by the model. A lock taken at
/// logical time `clock` has deadline `t0 + LOCK*(clock + 1)`, so it expires once
/// the clock reaches `clock + 1`.
pub(super) const LOCK: Duration = Duration::from_secs(1);

/// Bounded model config. It lives here, not in the fingerprinted state.
pub(super) struct ShareModel {
    /// Base instant. All `now` values are `t0 + LOCK*clock`. The model captures
    /// it once per run, so deadlines come from a finite, hashable set.
    pub(super) t0: Instant,
    /// Number of consumer members (named `m0`..`m{members-1}`).
    pub(super) members: u8,
    /// High-watermark and window cap: records produced over a path.
    pub(super) max_offset: Offset,
    /// Logical-clock cap.
    pub(super) max_tick: u8,
    /// Delivery-attempt limit before a record is archived as a poison pill.
    pub(super) max_attempts: i16,
    /// The record lock limit: the most records the window holds, that
    /// `materialize` grows it up to.
    pub(super) max_inflight: i32,
    /// Whether the model generates the leader-failover `Reload` action
    /// (Task 3).
    pub(super) allow_reload: bool,
    /// Whether the model generates the KFC-1 `Defer` and `PromoteDeferred`
    /// actions.
    pub(super) allow_defer: bool,
    /// Whether the model generates `AdvanceLogStart` (retention /
    /// `DeleteRecords` moving the log start offset past the SPSO).
    pub(super) allow_log_start_advance: bool,
}

impl ShareModel {
    /// Concurrency config: the full action set EXCEPT `Reload`. Bounds start
    /// small, at a proven memory-safe size. Task 4 scales `max_offset`
    /// empirically.
    pub(super) fn concurrency(max_offset: i64, max_inflight: i32) -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(max_offset),
            max_tick: 2,
            max_attempts: 2,
            max_inflight,
            allow_reload: false,
            allow_defer: false,
            allow_log_start_advance: false,
        }
    }

    /// Failover config: it adds `Reload` over a small window. It focuses on the
    /// `acknowledged_is_terminal` durability invariant across crash-recovery.
    pub(super) fn failover() -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(2),
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 2,
            allow_reload: true,
            allow_defer: false,
            allow_log_start_advance: false,
        }
    }

    /// Deferral-across-failover config: the failover config plus KFC-1 `Defer`
    /// and `PromoteDeferred`. It is what checks the persist-and-reload round
    /// trip over a deferred window.
    ///
    /// The deferral coordinate is a subset of the Available offsets, so it
    /// roughly doubles the reachable space per offset in the window. Two
    /// offsets is the smallest window that still lets a due record sit behind a
    /// waiting one, and it is as wide as this config goes: three offsets with
    /// `Reload` generates 205016 states, which is past `MAX_STATES` and so
    /// proves nothing. [`ShareModel::deferral_wide`] takes the wider window
    /// instead, and pays for it elsewhere.
    pub(super) fn deferral() -> Self {
        Self {
            allow_defer: true,
            ..Self::failover()
        }
    }

    /// Wide deferral config: three offsets, so a due record can sit two behind
    /// a waiting one and a deferral can span a range rather than a single
    /// offset.
    ///
    /// It buys that width with one member and no `Reload`. Both are covered
    /// elsewhere: the concurrency configs hold the two-member interleavings,
    /// and [`ShareModel::deferral`] holds the reload. Keeping either here puts
    /// the generated count within 300 states of `MAX_STATES`, which is not a
    /// bound anyone can build on.
    pub(super) fn deferral_wide() -> Self {
        Self {
            t0: Instant::now(),
            members: 1,
            max_offset: Offset(3),
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 3,
            allow_reload: false,
            allow_defer: true,
            allow_log_start_advance: false,
        }
    }

    /// Log-start-advance config: retention/`DeleteRecords` moving the log
    /// start offset past the SPSO (issue #948), combined with `Acquire`,
    /// `Acknowledge`, `Renew`, `ExpireLocks` (always available), and
    /// `Reload` (leader failover) so the interaction between an in-flight
    /// retention move and a crash-recovery round trip is checked too. One
    /// member and a 3-offset window: wide enough for an `Acquired` run to
    /// block an advance while an `Available` run ahead of it is archived
    /// (mirrors `advance_past_log_start_leaves_other_members_acquired_records_alone`
    /// in `share_partition::state::acquire`'s unit tests).
    pub(super) fn log_start_advance() -> Self {
        Self {
            allow_reload: true,
            allow_defer: false,
            allow_log_start_advance: true,
            ..Self::deferral_wide()
        }
    }

    pub(super) fn now(&self, clock: u8) -> Instant { self.t0 + LOCK * u32::from(clock) }

    pub(super) fn member_name(member: u8) -> String { format!("m{member}") }
}
