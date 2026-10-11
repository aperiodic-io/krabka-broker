//! The follower half of a KIP-227 incremental fetch session.
//!
//! Kafka's `ReplicaFetcherThread` folds every partition it owns into one
//! `FetchRequest` through a `FetchSessionHandler`. The first request of a
//! session is full: it names every partition and asks the leader to open a
//! session. Every later request is incremental: it names only the partitions
//! whose desired state changed, forgets the ones the follower stopped
//! following, and the leader answers with only the partitions whose state
//! changed on its side. A follower with ten thousand caught-up partitions
//! therefore sends an almost empty request per round instead of ten thousand
//! full ones.
//!
//! This is that handler. It is deliberately ignorant of what a partition is:
//! it takes the rows the caller wants sent this round, decides which of them
//! actually go on the wire, and tracks the session the leader granted.
//!
//! The leader half lives in [`crate::fetch_session`], and the two share the
//! wire sentinels: `(0, INITIAL_EPOCH)` opens a session, `(id, epoch)`
//! continues one, and a response `session_id` of
//! [`INVALID_SESSION_ID`](crate::fetch_session::INVALID_SESSION_ID) means the
//! leader granted none and every request stays full.

use std::collections::BTreeMap;

use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    codes,
    fetch_session::{
        FetchSessionEpoch, FetchSessionId, INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
    },
};

/// The identity a session row is keyed by. `topic_id` is the KIP-516 identity
/// the leader matches on at Fetch v13 and above; the name rides along for the
/// older versions and for the `ForgottenTopic` rows, which carry both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionKey {
    pub(super) topic: String,
    pub(super) topic_id: WireUuid,
    pub(super) partition: i32,
}

// The wire `Uuid` is a byte array with no ordering of its own, and the map has
// to be ordered so that a request groups a topic's partitions into one
// `FetchTopic` by walking the keys once. Ordering by name first also makes the
// grouping read in the order an operator would expect in a trace.
impl Ord for SessionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.topic, self.topic_id.0, self.partition).cmp(&(
            &other.topic,
            other.topic_id.0,
            other.partition,
        ))
    }
}

impl PartialOrd for SessionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// What one round wants sent, before the session decides what actually is.
pub(super) type WantedRows = BTreeMap<SessionKey, FetchPartition>;

/// The wire fields one round's request carries.
#[derive(Debug, PartialEq)]
pub(super) struct SessionRequest {
    pub(super) session_id: FetchSessionId,
    pub(super) session_epoch: FetchSessionEpoch,
    pub(super) topics: Vec<FetchTopic>,
    pub(super) forgotten_topics_data: Vec<ForgottenTopic>,
}

/// The follower's view of one leader's fetch session.
#[derive(Debug, Default)]
pub(super) struct FollowerFetchSession {
    /// The id the leader granted, or [`INVALID_SESSION_ID`] before it granted
    /// one and after it dropped one.
    session_id: FetchSessionId,
    /// The epoch the next request carries. [`INITIAL_EPOCH`] means the next
    /// request is a full one.
    next_epoch: FetchSessionEpoch,
    /// The state the leader is believed to hold, which is what the next
    /// incremental request is diffed against.
    sent: WantedRows,
}

impl FollowerFetchSession {
    /// Whether the next request will be a full one.
    pub(super) fn is_full(&self) -> bool {
        self.next_epoch == INITIAL_EPOCH
    }

    /// Forgets the session and everything the leader was believed to hold, so
    /// the next request opens a new session and names every partition.
    ///
    /// Used when the leader refuses the session, and when a request may or may
    /// not have reached the leader -- a transport failure leaves the leader's
    /// cached set unknowable, and a full request is the only shape that is
    /// correct either way.
    pub(super) fn reset(&mut self) {
        self.session_id = INVALID_SESSION_ID;
        self.next_epoch = INITIAL_EPOCH;
        self.sent.clear();
    }

    /// Builds the request rows for one round.
    ///
    /// A full round sends every wanted row. An incremental round sends only
    /// the rows whose desired state differs from what the leader is believed
    /// to hold, and forgets the rows this follower no longer wants.
    ///
    /// The believed state is advanced to `wanted` here rather than when the
    /// response arrives, because that is what the leader does: a request it
    /// received changed its cached set whether or not the follower liked the
    /// answer. The two failure modes that make the belief wrong -- a lost
    /// request and a refused session -- both go through [`Self::reset`].
    pub(super) fn build(&mut self, wanted: WantedRows) -> SessionRequest {
        let full = self.is_full();
        let mut rows: Vec<(&SessionKey, &FetchPartition)> = Vec::new();
        for (key, partition) in &wanted {
            if full || self.sent.get(key) != Some(partition) {
                rows.push((key, partition));
            }
        }
        let forgotten = if full {
            Vec::new()
        } else {
            forgotten_topics(self.sent.keys().filter(|key| !wanted.contains_key(key)))
        };
        let request = SessionRequest {
            session_id: self.session_id,
            session_epoch: self.next_epoch,
            topics: fetch_topics(rows),
            forgotten_topics_data: forgotten,
        };
        self.sent = wanted;
        request
    }

    /// Applies already-known changed and removed rows without rescanning the
    /// whole session. The caller uses this after the full opening request;
    /// Fetch responses identify exactly which offsets can have moved.
    pub(super) fn build_changes(
        &mut self,
        changed: WantedRows,
        removed: &[SessionKey],
    ) -> SessionRequest {
        if self.is_full() {
            return self.build(changed);
        }
        let topics = fetch_topics(changed.iter().collect());
        let forgotten_topics_data = forgotten_topics(removed.iter());
        for key in removed {
            self.sent.remove(key);
        }
        self.sent.extend(changed);
        SessionRequest {
            session_id: self.session_id,
            session_epoch: self.next_epoch,
            topics,
            forgotten_topics_data,
        }
    }

    /// Folds one response's session fields back into the handler, and reports
    /// whether the round's partition rows may be applied at all.
    ///
    /// A top-level `FETCH_SESSION_ID_NOT_FOUND` or
    /// `INVALID_FETCH_SESSION_EPOCH` carries no partition rows: the leader
    /// refused the request before reading it. The session is dropped so the
    /// next round is full, and the caller skips the round.
    pub(super) fn handle_response(
        &mut self,
        error_code: i16,
        session_id: FetchSessionId,
    ) -> SessionOutcome {
        if matches!(
            error_code,
            codes::FETCH_SESSION_ID_NOT_FOUND | codes::INVALID_FETCH_SESSION_EPOCH
        ) {
            self.reset();
            return SessionOutcome::SessionLost;
        }
        if error_code != codes::NONE {
            return SessionOutcome::Usable;
        }
        if session_id == INVALID_SESSION_ID {
            // The leader granted no session -- its cache was full of
            // privileged sessions, or it does not cache at all. Every request
            // stays full, which is correct and merely larger.
            self.reset();
        } else {
            self.session_id = session_id;
            self.next_epoch = next_epoch(self.next_epoch);
        }
        SessionOutcome::Usable
    }
}

/// Whether a response's partition rows may be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionOutcome {
    /// The response is about this follower's partitions.
    Usable,
    /// The leader refused the session. The response carries no partition rows
    /// and the next round opens a new session.
    SessionLost,
}

/// Groups the chosen rows into one `FetchTopic` per topic, in key order.
///
/// The key owns the identity, so each row's `partition` index is taken from
/// it: a caller that filled the value's index differently cannot send the
/// leader a row addressed to a partition other than the one it is diffed
/// against.
fn fetch_topics(rows: Vec<(&SessionKey, &FetchPartition)>) -> Vec<FetchTopic> {
    let mut topics: Vec<FetchTopic> = Vec::new();
    for (key, partition) in rows {
        let row = FetchPartition {
            partition: key.partition,
            ..partition.clone()
        };
        match topics.last_mut() {
            Some(last) if last.topic_id == key.topic_id && last.topic == key.topic => {
                last.partitions.push(row);
            }
            _ => topics.push(FetchTopic {
                topic: key.topic.clone(),
                topic_id: key.topic_id,
                partitions: vec![row],
                ..FetchTopic::default()
            }),
        }
    }
    topics
}

/// Groups the dropped keys into one `ForgottenTopic` per topic, in key order.
fn forgotten_topics<'keys>(keys: impl Iterator<Item = &'keys SessionKey>) -> Vec<ForgottenTopic> {
    let mut topics: Vec<ForgottenTopic> = Vec::new();
    for key in keys {
        match topics.last_mut() {
            Some(last) if last.topic_id == key.topic_id && last.topic == key.topic => {
                last.partitions.push(key.partition);
            }
            _ => topics.push(ForgottenTopic {
                topic: key.topic.clone(),
                topic_id: key.topic_id,
                partitions: vec![key.partition],
                ..ForgottenTopic::default()
            }),
        }
    }
    topics
}

#[cfg(test)]
mod tests;
