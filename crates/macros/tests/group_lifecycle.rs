//! Lifecycle methods select the protocol's capabilities and delegate removals.

use {
    assert2::assert,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

mod metadata_epoch {
    pub(crate) fn next_i32(epoch: i32) -> Option<i32> { epoch.checked_add(1) }
}

mod coordinator {
    pub(crate) mod unified {
        use std::time::{Duration, Instant};

        pub(crate) fn expired_member_ids<'a>(
            members: impl Iterator<Item = (&'a str, Instant)>,
            now: Instant,
            timeout: Duration,
        ) -> Vec<String> {
            let mut expired = Vec::new();
            for (id, last_seen) in members {
                if now
                    .checked_duration_since(last_seen)
                    .is_some_and(|age| age > timeout)
                {
                    expired.push(id.to_owned());
                }
            }
            expired
        }

        pub(crate) mod member_helpers {
            use std::time::Duration;

            pub(crate) fn can_compute_next_target_assignment(
                timestamp: i64,
                interval: Duration,
                now: i64,
            ) -> bool {
                let millis = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
                timestamp == 0
                    || interval == Duration::ZERO
                    || now >= timestamp.wrapping_add(millis)
            }
        }
    }
}

struct Member {
    last_seen: Instant,
}

#[derive(Default, krabka_macros::GroupLifecycle)]
#[group_lifecycle(protocol = "consumer")]
struct Consumer<T> {
    group_epoch: i32,
    members: HashMap<String, Member>,
    assignment_timestamp_ms: i64,
    removed: Vec<String>,
    due: Vec<String>,
    unrelated: Vec<T>,
}

impl<T> Consumer<T> {
    fn remove_member(&mut self, id: &str) {
        self.removed.push(id.into());
        self.members.remove(id);
    }

    fn rebalance_timeouts_due(&mut self, _: Instant) -> Vec<String> {
        std::mem::take(&mut self.due)
    }
}

#[derive(Default, krabka_macros::GroupLifecycle)]
#[group_lifecycle(protocol = "share")]
struct Share {
    assignment_timestamp_ms: i64,
    removed: bool,
    members: HashMap<String, Member>,
    group_epoch: i32,
}

impl Share {
    fn remove_member(&mut self, id: &str) { self.removed |= self.members.remove(id).is_some(); }
}

#[derive(Default, krabka_macros::GroupLifecycle)]
#[group_lifecycle(protocol = "streams")]
struct Streams {
    dirty: bool,
    group_epoch: i32,
    due: Option<String>,
    members: HashMap<String, Member>,
}

impl Streams {
    fn remove_member(&mut self, id: &str) { self.dirty |= self.members.remove(id).is_some(); }

    fn rebalance_timeouts_due(&mut self, _: Instant) -> Vec<String> {
        self.due.take().into_iter().collect()
    }
}

fn sessions(base: Instant) -> HashMap<String, Member> {
    [
        SessionAge {
            id: "old",
            offset: 0,
        },
        SessionAge {
            id: "boundary",
            offset: 1,
        },
        SessionAge {
            id: "future",
            offset: 12,
        },
    ]
    .into_iter()
    .map(|SessionAge { id, offset }| {
        (id.into(), Member {
            last_seen: base + Duration::from_secs(offset),
        })
    })
    .collect()
}

#[test]
fn queries_are_immutable_and_eviction_runs_each_protocols_removal() {
    let base = Instant::now();
    let now = base + Duration::from_secs(11);
    let timeout = Duration::from_secs(10);
    let mut consumer = Consumer::<u8> {
        members: sessions(base),
        unrelated: vec![7],
        ..Default::default()
    };
    let mut share = Share {
        members: sessions(base),
        ..Default::default()
    };
    let mut streams = Streams {
        members: sessions(base),
        ..Default::default()
    };
    assert!(consumer.expired_members(now, timeout) == ["old"]);
    assert!(share.expired_members(now, timeout) == ["old"]);
    assert!(streams.expired_members(now, timeout) == ["old"]);
    assert!(consumer.members.len() == 3 && consumer.removed.is_empty());
    assert!(share.members.len() == 3 && !share.removed);
    assert!(streams.members.len() == 3 && !streams.dirty);
    assert!(streams.evict_expired(base, timeout).is_empty());
    assert!(!streams.dirty);
    assert!(consumer.evict_expired(now, timeout) == ["old"]);
    assert!(share.evict_expired(now, timeout) == ["old"]);
    assert!(streams.evict_expired(now, timeout) == ["old"]);
    assert!(consumer.removed == ["old"] && consumer.unrelated == [7]);
    assert!(share.removed && streams.dirty);
    assert!(consumer.members.len() == 2 && share.members.len() == 2 && streams.members.len() == 2);
}

#[test]
fn epoch_transitions_change_dirty_only_after_a_successful_streams_bump() {
    let mut consumer = Consumer::<u8>::default();
    let mut share = Share::default();
    assert!(consumer.bump_epoch() && consumer.group_epoch == 1);
    assert!(share.bump_epoch() && share.group_epoch == 1);
    consumer.group_epoch = i32::MAX;
    share.group_epoch = i32::MAX;
    assert!(!consumer.bump_epoch() && consumer.group_epoch == i32::MAX);
    assert!(!share.bump_epoch() && share.group_epoch == i32::MAX);
}

#[yare::parameterized(clean = { false }, already_dirty = { true })]
fn streams_preserve_dirty_state_when_epoch_cannot_advance(dirty: bool) {
    let mut streams = Streams {
        dirty,
        ..Default::default()
    };
    assert2::assert!(streams.bump_epoch() && streams.group_epoch == 1 && streams.dirty);
    streams.group_epoch = i32::MAX;
    streams.dirty = dirty;
    assert2::assert!(!streams.bump_epoch());
    assert2::assert!(streams.group_epoch == i32::MAX && streams.dirty == dirty);
}

#[test]
fn consumer_and_share_preserve_zero_interval_boundaries() {
    let mut consumer = Consumer::<u8>::default();
    let mut share = Share::default();
    let interval = Duration::from_millis(10);
    assert!(!consumer.assignment_delayed(interval, 1));
    assert!(!share.assignment_delayed(interval, 1));
    consumer.assignment_timestamp_ms = 100;
    share.assignment_timestamp_ms = 100;
    assert!(!consumer.assignment_delayed(Duration::ZERO, 100));
    assert!(!share.assignment_delayed(Duration::ZERO, 100));
}

#[yare::parameterized(
    before = { 109, true },
    at_boundary = { 110, false },
    after = { 111, false },
)]
fn consumer_and_share_preserve_assignment_delay_boundaries(now: i64, delayed: bool) {
    let mut consumer = Consumer::<u8>::default();
    let mut share = Share::default();
    let interval = Duration::from_millis(10);
    consumer.assignment_timestamp_ms = 100;
    share.assignment_timestamp_ms = 100;
    assert2::assert!(consumer.assignment_delayed(interval, now) == delayed);
    assert2::assert!(share.assignment_delayed(interval, now) == delayed);
}

#[test]
fn rebalance_fences_preserve_due_order_and_use_protocol_removal() {
    let now = Instant::now();
    let mut consumer = Consumer::<u8> {
        members: sessions(now),
        due: vec!["future".into(), "old".into()],
        ..Default::default()
    };
    let mut streams = Streams {
        due: Some("boundary".into()),
        members: sessions(now),
        ..Default::default()
    };
    assert!(consumer.fence_rebalance_timeouts(now) == ["future", "old"]);
    assert!(consumer.removed == ["future", "old"] && consumer.members.len() == 1);
    assert!(streams.fence_rebalance_timeouts(now) == ["boundary"]);
    assert!(streams.dirty && !streams.members.contains_key("boundary"));
    assert!(consumer.fence_rebalance_timeouts(now).is_empty());
    assert!(streams.fence_rebalance_timeouts(now).is_empty());
}

struct SessionAge {
    id: &'static str,
    offset: u64,
}
