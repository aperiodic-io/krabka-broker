//! Classic-protocol per-`group_id` state machine (`Group`).
//!
//! This module holds pure data and transitions. The unified per-group actor
//! owns them. Committed offsets are protocol-agnostic and live on the unified
//! [`super::group::CoordinatorGroup`] container, not here.
//!
//! This file is the module root. The [`ClassicGroup`] record and its
//! rebalance-lifecycle transitions live in `group`, the membership transitions
//! in `membership`, the [`Member`] record in `member`, and the protocol vote in
//! `protocol`. [`OffsetEntry`] stays here: it is a leaf record with no
//! behaviour of its own.

mod group;
mod member;
mod membership;
mod protocol;
mod rebalance;

#[cfg(test)]
mod test_support;

pub use self::{
    group::{ClassicGroup, GroupState},
    member::Member,
    protocol::select_protocol,
};

super::persistence::committed_offset_type! {
    /// A committed offset entry, keyed by `(topic, partition)` in
    /// [`Group::committed_offsets`].
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OffsetEntry {
        /// KIP-211: the absolute expiry a v2-v4 `OffsetCommit` asked for through
        /// `retention_time_ms`. `None` means the entry expires on the broker's
        /// `offsets.retention.minutes` instead.
        expire_timestamp_ms,
        /// The topic id the metadata image held for the topic name when the
        /// offset was committed, as Kafka's `OffsetAndMetadata.topicId` does. The
        /// `OffsetCommitValue` record carries it from version 4, so a replayed or
        /// transaction-published offset keeps it too. `None` is Kafka's zero id:
        /// the image did not know the topic, or the record predates version 4.
        /// A topic deletion removes an offset whose id is `None` or the deleted
        /// id, so an offset committed to a topic created again with the same name
        /// survives the deletion of the old one, and an `OffsetFetch` that names
        /// another topic id does not see it.
        topic_id,
    }
}

impl OffsetEntry {
    /// `true` when the retention sweep may tombstone this entry at `now_ms`.
    ///
    /// A per-commit expiry wins outright, which is what Kafka's
    /// `OffsetExpirationConditionImpl.isOffsetExpired` does. Otherwise the
    /// entry expires `retention_ms` after a base timestamp, and
    /// `empty_since_ms` says which one: it is Kafka's `currentStateTimestamp`,
    /// so `None` falls back to the commit alone the way Kafka's
    /// `currentStateTimestamp.orElse(commitTimestamp)` does. A caller that has
    /// one measures from it whatever the commit time, so an offset committed to
    /// an already-empty group does not outlive the group's own retention clock.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64, empty_since_ms: Option<i64>, retention_ms: i64) -> bool {
        if let Some(expire_timestamp_ms) = self.expire_timestamp_ms {
            return now_ms >= expire_timestamp_ms;
        }
        let base = empty_since_ms.unwrap_or(self.commit_timestamp_ms);
        now_ms >= base.saturating_add(retention_ms)
    }
}

#[cfg(test)]
mod offset_entry_tests {
    use {super::*, assert2::check, krabka_log::Offset};

    fn entry(commit_timestamp_ms: i64, expire_timestamp_ms: Option<i64>) -> OffsetEntry {
        OffsetEntry {
            offset: Offset(7),
            leader_epoch: -1,
            metadata: String::new(),
            commit_timestamp_ms,
            expire_timestamp_ms,
            topic_id: None,
        }
    }

    #[test]
    fn expiry_reads_the_per_commit_deadline_then_the_group_empty_clock_then_the_commit() {
        const RETENTION_MS: i64 = 1_000;
        // (commit_ts, per-commit expiry, empty_since, now, expired?)
        let cases = [
            // No per-commit expiry: retention runs from the moment the group
            // emptied.
            (0, None, Some(5_000), 5_999, false),
            (0, None, Some(5_000), 6_000, true),
            // The commit came after the group emptied. Kafka's
            // `currentStateTimestamp.orElse(commitTimestamp)` still measures
            // from the emptying, so the commit does not extend retention.
            (9_000, None, Some(0), 999, false),
            (9_000, None, Some(0), 1_000, true),
            (9_000, None, Some(0), 9_999, true),
            // No group-empty clock at all: the commit alone decides, which is
            // what Kafka does for a simple group and for a KIP-848 group.
            (9_000, None, None, 9_999, false),
            (9_000, None, None, 10_000, true),
            // A per-commit expiry wins outright, early...
            (0, Some(10), Some(0), 9, false),
            (0, Some(10), Some(0), 10, true),
            // ...and late, past the broker retention that would have expired it.
            (0, Some(1_000_000), Some(0), 999_999, false),
        ];
        for (commit_ts, expire_ts, empty_since, now, want) in cases {
            let entry = entry(commit_ts, expire_ts);
            check!(
                entry.is_expired(now, empty_since, RETENTION_MS) == want,
                "commit_ts={commit_ts} expire_ts={expire_ts:?} empty_since={empty_since:?} now={now}"
            );
        }
    }
}

#[cfg(test)]
#[path = "classic_state_model.rs"]
mod classic_state_model;
