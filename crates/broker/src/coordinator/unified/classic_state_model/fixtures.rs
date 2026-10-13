//! The deterministic time base the model runs on and the [`Member`] builder
//! that stamps a heartbeat onto it.
//!
//! Both the exhaustive search and the proptest fuzz drive the real
//! `ClassicGroup` over the same synthetic clock, so the tick-to-[`Instant`]
//! mapping and the member fixture are shared rather than duplicated. The clock
//! is a single process-wide epoch plus a whole number of one-second ticks, so
//! two states that carry the same tick carry the same instant and the state
//! fingerprint stays canonical.

use {
    crate::coordinator::unified::classic_state::Member,
    bytes::Bytes,
    std::{
        sync::OnceLock,
        time::{Duration, Instant},
    },
};

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}
const UNIT: Duration = Duration::from_secs(1);
const SESSION: Duration = Duration::from_secs(2); // 2 * UNIT

pub(super) fn at(clock: i64) -> Instant {
    epoch() + UNIT * u32::try_from(clock.max(0)).unwrap_or(0)
}

pub(super) fn mk_member(mid: &str, iid: Option<&str>, clock: i64) -> Member {
    let mut m = Member::new(mid, "c", "h", SESSION, Duration::from_secs(10), vec![(
        "range".to_string(),
        Bytes::new(),
    )])
    .with_instance_id(iid.map(str::to_string));
    m.last_heartbeat = at(clock);
    m
}
