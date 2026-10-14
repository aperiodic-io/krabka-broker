//! KIP-227 incremental-fetch-session cache.
//!
//! A `FetchSession` lets a Kafka consumer or replicator send the broker its
//! subscription set once. After that it sends small "delta" fetch requests.
//! Each delta carries only the partitions whose desired state has changed (new
//! offset, new max-bytes), plus a `forgotten_topics_data` list of partitions
//! to drop. The broker answers with only the partitions whose state has
//! changed since the previous response.
//!
//! For a caught-up consumer with hundreds of partitions, this reduces a
//! continuous stream of identical fetches to almost no wire traffic until
//! something changes.
//!
//! ## Wire-level state machine
//!
//! Every `FetchRequest` carries `session_id: i32` and `session_epoch: i32`.
//! Which class a request falls into is decided by `session_epoch` alone,
//! matching Kafka's `FetchMetadata.isFull()`; `session_id` is looked up only
//! on the incremental path:
//!
//! | `session_epoch`      | `session_id`  | Meaning                                                |
//! |-----------------------|---------------|---------------------------------------------------------|
//! | -1 (FINAL)             | any           | Full fetch. Closes the named session first, if any, and caches nothing. |
//! | 0 (INITIAL)            | any           | Full fetch. Closes the named session first, if any, then may open a new one. |
//! | E (positive)           | N (existing, `E == expected`) | Incremental fetch on session `N`.       |
//!
//! A full fetch (`session_epoch` of `-1` or `0`) never epoch-checks the
//! session id it carries: this is the Java consumer's ordinary reconnect
//! path (`nextCloseExistingAttemptNew`), which sends `INITIAL_EPOCH` with its
//! old id and expects a fresh session, not an error. An incremental fetch
//! (any other epoch) with an unknown id -- id `0` included, which is never
//! allocated -- returns `FETCH_SESSION_ID_NOT_FOUND`; one with a known id but
//! a mismatched epoch returns `INVALID_FETCH_SESSION_EPOCH`. Both are
//! top-level response codes. So is `FETCH_SESSION_TOPIC_ID_ERROR`, for an
//! incremental fetch whose version does not match the topic-id use of the
//! request that created its session (v13 and later name topics by id). An
//! incremental fetch that forgets its last
//! cached partitions and adds none is dropped, and the response reports
//! `session_id = 0`.
//!
//! ## Cache & eviction
//!
//! The cache holds sessions in one bounded map, keyed by allocated id. Its
//! capacity is `BrokerConfig::max_incremental_fetch_session_cache_slots`.
//! When the map is full, an allocation displaces a session only where Kafka's
//! `FetchSessionCacheShard.tryEvict` does:
//!
//! 1. the session unused the longest, when it has been unused for more than two
//!    minutes, whoever asks;
//! 2. otherwise the cheapest session the caller may displace, when the
//!    newcomer outranks it. A consumer may displace only a consumer session
//!    created more than two minutes ago that caches fewer partitions than the
//!    newcomer. A follower fetch (`replica_id >= 0`) may displace any consumer
//!    session, or a follower session created more than two minutes ago that
//!    caches fewer partitions.
//!
//! Finding that victim reads the first entry of one of the ordered sets in the
//! `order` submodule, so a full cache costs an allocation a lookup, not a scan
//! of every live session with the cache mutex held. `benches/fetch_session.rs`
//! measures it.
//!
//! When there is no eligible victim, `try_allocate` returns
//! `INVALID_SESSION_ID` and the caller falls back to a sessionless response.
//! This matches Apache Kafka, and it is what keeps a full broker stable: a
//! session in use is never displaced, so no client is pushed into reconnecting
//! with a full fetch and displacing another.

mod cache;
mod classify;
mod diff;
mod epoch;
mod eviction;
mod order;
mod state;
#[cfg(test)]
mod test_support;

/// A topic name or id paired with its partition in a Fetch request or response.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FetchPartitionKey<T> {
    pub(crate) topic: T,
    pub(crate) partition: i32,
}

#[cfg(test)]
pub(crate) use self::diff::apply_incremental;
pub use self::{
    cache::{FetchSession, FetchSessionCache},
    classify::SessionDecision,
    epoch::{
        FINAL_EPOCH, FIRST_TOPIC_ID_FETCH_VERSION, FetchSessionEpoch, FetchSessionId,
        INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
    },
    state::{CachedPartitionState, FetchSessionKey},
};

#[cfg(test)]
#[path = "fetch_session_model.rs"]
mod fetch_session_model;
