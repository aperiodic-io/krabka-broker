//! Request-level helpers that every group protocol shares: minting the member
//! id of a first join and finding the members whose session has expired.
//!
//! The classic, next-gen, share, and streams paths all call them, and they are
//! pure functions over request fields, so they sit apart from the coordinator
//! that calls them.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use krabka_protocol::primitives::uuid::Uuid;

/// Query expiration consistently across group protocols.
macro_rules! expired_members_method {
    () => {
        /// The members whose session expired at `now`, without removing them.
        #[must_use]
        pub fn expired_members(
            &self,
            now: std::time::Instant,
            session_timeout: std::time::Duration,
        ) -> Vec<String> {
            $crate::coordinator::unified::expired_member_ids(
                self.members
                    .iter()
                    .map(|(id, member)| (id.as_str(), member.last_seen)),
                now,
                session_timeout,
            )
        }
    };
}
pub(crate) use expired_members_method;

/// Consumer and streams groups fence through their own deadline and removal transitions.
macro_rules! fence_rebalance_timeouts_method {
    ($(#[$doc:meta])*) => {
        $(#[$doc])*
        pub fn fence_rebalance_timeouts(&mut self, now: std::time::Instant) -> Vec<String> {
            let fenced = self.rebalance_timeouts_due(now);
            for member_id in &fenced {
                self.remove_member(member_id);
            }
            fenced
        }
    };
}
pub(crate) use fence_rebalance_timeouts_method;

/// Evicts expired members through the group's own removal transition.
macro_rules! evict_expired {
    ($(#[$doc:meta])*) => {
        $(#[$doc])*
        pub fn evict_expired(
            &mut self,
            now: std::time::Instant,
            session_timeout: std::time::Duration,
        ) -> Vec<String> {
            $crate::coordinator::unified::member_helpers::evict_expired!(self, now, session_timeout)
        }
    };
    ($group:expr, $now:expr, $timeout:expr) => {{
        let group = $group;
        let expired = $crate::coordinator::unified::expired_member_ids(
            group
                .members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            $now,
            $timeout,
        );
        for id in &expired {
            group.remove_member(id);
        }
        expired
    }};
}

pub(crate) use evict_expired;

/// Every group refuses the proved successor at exhaustion; only selected protocols mark it dirty.
macro_rules! bump_group_epoch {
    ($(#[$doc:meta])* $group:ident; $($after:tt)*) => {
        $(#[$doc])*
        pub fn bump_epoch(&mut $group) -> bool {
            let Some(epoch) = $crate::metadata_epoch::next_i32($group.group_epoch) else {
                return false;
            };
            $group.group_epoch = epoch;
            $($after)*
            true
        }
    };
}
pub(crate) use bump_group_epoch;

/// The same assignment-delay query on each protocol's stored
/// `AssignmentTimestamp`.
macro_rules! assignment_delay_method {
    () => {
        /// Kafka's `GroupMetadataManager.canComputeNextTargetAssignment`, negated:
        /// `true` while the assignment `interval` holds the next target
        /// assignment back at `now_ms`.
        #[must_use]
        pub(crate) fn assignment_delayed(
            &self,
            interval: std::time::Duration,
            now_ms: i64,
        ) -> bool {
            !$crate::coordinator::unified::member_helpers::can_compute_next_target_assignment(
                self.assignment_timestamp_ms,
                interval,
                now_ms,
            )
        }
    };
}
pub(crate) use assignment_delay_method;

/// An absent heartbeat field keeps the stored value; allocate only when it changes.
pub(crate) fn update_present<T: Clone + PartialEq>(
    stored: &mut Option<T>,
    incoming: Option<&T>,
) -> bool {
    if let Some(incoming) = incoming
        && stored.as_ref() != Some(incoming)
    {
        match stored {
            Some(value) => value.clone_from(incoming),
            None => *stored = Some(incoming.clone()),
        }
        true
    } else {
        false
    }
}

pub(crate) fn first_join_member_id(request_member_id: &str) -> String {
    if request_member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_member_id.to_string()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ClientIdentity<'a> {
    pub id: &'a str,
    pub host: &'a str,
}

impl ClientIdentity<'_> {
    /// Updates stored client metadata, allocating only for changed fields.
    pub fn update_metadata(self, client_id: &mut String, client_host: &mut String) -> bool {
        let mut changed = false;
        for (stored, incoming) in [(client_id, self.id), (client_host, self.host)] {
            if stored.as_str() != incoming {
                *stored = incoming.to_string();
                changed = true;
            }
        }
        changed
    }
}

pub(crate) fn expired_member_ids<'a>(
    members: impl IntoIterator<Item = (&'a str, Instant)>,
    now: Instant,
    session_timeout: Duration,
) -> Vec<String> {
    members
        .into_iter()
        .filter(|(_, last_seen)| now.duration_since(*last_seen) > session_timeout)
        .map(|(id, _)| id.to_string())
        .collect()
}

/// Kafka's `Time.SYSTEM.milliseconds()`: the wall-clock time in milliseconds,
/// which the group coordinator stamps the `AssignmentTimestamp` of a target
/// assignment with and compares the assignment interval against.
///
/// It reads the system clock, moved by however far tokio's clock runs ahead
/// of the real monotonic clock. Outside a paused tokio runtime the two
/// monotonic clocks agree and this is the system clock itself, wall-clock
/// steps included, as in Kafka. A test on a paused runtime moves it with
/// `tokio::time::advance` or `sleep`.
#[must_use]
pub(crate) fn wall_clock_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let std_now = Instant::now();
    let tokio_now = tokio::time::Instant::now().into_std();
    let system = SystemTime::now();
    let wall = system
        .checked_add(tokio_now.saturating_duration_since(std_now))
        .and_then(|wall| wall.checked_sub(std_now.saturating_duration_since(tokio_now)))
        .unwrap_or(system);
    wall.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
    })
}

/// Kafka's `GroupMetadataManager.canComputeNextTargetAssignment`: whether a
/// group whose last target assignment calculation finished at
/// `assignment_timestamp_ms` may compute the next one at `now_ms`.
///
/// The timestamp is the `AssignmentTimestamp` (KIP-1263) of the group's
/// target assignment metadata record, in wall-clock milliseconds, and 0 when
/// there is no previous assignment or its time is unknown. The next assignment
/// computes at once then, and when `interval` is zero, which is Kafka's escape
/// hatch for a wall clock that stepped back. Otherwise it waits until the
/// interval has elapsed since the last one.
#[must_use]
pub(crate) fn can_compute_next_target_assignment(
    assignment_timestamp_ms: i64,
    interval: Duration,
    now_ms: i64,
) -> bool {
    if assignment_timestamp_ms == 0 || interval.is_zero() {
        return true;
    }
    // Java adds two `long`s, so an overflow wraps around.
    let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
    now_ms >= assignment_timestamp_ms.wrapping_add(interval_ms)
}

/// One member's assignment: the partitions of each topic, by topic id.
type Assignment = HashMap<Uuid, Vec<i32>>;

/// Kafka's `TargetAssignmentBuilder.newMemberAssignment` over a whole group:
/// the target each of `member_ids` gets from the assignor's `per_member`
/// output, and the members whose target differs from the one in `held`.
///
/// A member the assignor gave nothing gets an empty target. Each target drops
/// its empty topics and sorts its partitions. A member whose target is not
/// [`same_assignment`] as the one it held, a member that held none included,
/// is in the returned list, sorted: the members for which Kafka's builder
/// writes a target assignment record.
///
/// [`same_assignment`]: crate::coordinator::unified::consumer_state::same_assignment
pub(crate) fn new_target_assignment<'a>(
    member_ids: impl ExactSizeIterator<Item = &'a String>,
    mut per_member: HashMap<String, Assignment>,
    held: &HashMap<String, Assignment>,
) -> (HashMap<String, Assignment>, Vec<String>) {
    let mut changed = Vec::new();
    let mut target = HashMap::with_capacity(member_ids.len());
    for member_id in member_ids {
        let mut assignment = per_member.remove(member_id).unwrap_or_default();
        assignment.retain(|_, partitions| !partitions.is_empty());
        for partitions in assignment.values_mut() {
            partitions.sort_unstable();
        }
        if !held.get(member_id).is_some_and(|held| {
            crate::coordinator::unified::consumer_state::same_assignment(held, &assignment)
        }) {
            changed.push(member_id.clone());
        }
        target.insert(member_id.clone(), assignment);
    }
    changed.sort_unstable();
    (target, changed)
}

#[cfg(test)]
mod helper_tests {
    use assert2::{assert, check};

    use super::*;

    /// The cases of Kafka's `GroupMetadataManagerTest`
    /// `testCanComputeNextTargetAssignment*`: no previous assignment, a zero
    /// interval, before, at and after the interval, and Java's wrap-around of
    /// a sum past `Long.MAX_VALUE`.
    #[test]
    fn the_next_target_assignment_waits_for_the_interval() {
        let second = Duration::from_secs(1);
        // (last assignment timestamp, interval, now, may compute)
        let rows = [
            (0, second, 1_000, true),
            (1_000, Duration::ZERO, 1_000, true),
            (1_000, second, 1_999, false),
            (1_000, second, 2_000, true),
            (1_000, second, 61_000, true),
            (i64::MAX, second, 0, true),
        ];
        for (timestamp, interval, now, expected) in rows {
            check!(
                can_compute_next_target_assignment(timestamp, interval, now) == expected,
                "{timestamp} {interval:?} {now}"
            );
        }
    }

    #[test]
    fn first_join_member_id_preserves_client_supplied_id() {
        assert!(first_join_member_id("member-a") == "member-a");
    }

    #[test]
    fn first_join_member_id_mints_uuid_for_empty_id() {
        let member_id = first_join_member_id("");

        check!(!member_id.is_empty());
        assert!(uuid::Uuid::parse_str(&member_id).is_ok());
    }

    #[test]
    fn expired_member_ids_returns_only_members_past_timeout() {
        let now = Instant::now();
        let session_timeout = Duration::from_secs(10);
        let expired = now
            .checked_sub(Duration::from_secs(11))
            .expect("past instant");
        let active = now
            .checked_sub(Duration::from_secs(10))
            .expect("past instant");
        let future = now
            .checked_add(Duration::from_secs(1))
            .expect("future instant");

        let expired = expired_member_ids(
            [("expired", expired), ("active", active), ("future", future)],
            now,
            session_timeout,
        );

        assert!(expired == vec!["expired".to_string()]);
    }
}
