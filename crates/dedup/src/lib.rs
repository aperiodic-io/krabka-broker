//! Index for produce-time, windowed, first-wins record deduplication.
//!
//! The broker drops a record at produce time when a record with the same dedup identity was already admitted within the window. The identity is the record's Kafka key, or the value of one configured header. This crate holds the index that makes that decision. It parses no batches and reads no clock: the caller gives it digested identities, record timestamps, and the broker's time.
//!
//! # Model
//!
//! - **First-wins.** The first record for an identity is kept. A later record for that identity whose timestamp is within `window_ms` of the stored one is dropped. This also applies between records of one produce request.
//! - **Advisory and derived from the log.** Only the leader consults the index, and only before replication, so a wrong answer cannot corrupt the log. Followers rebuild the same index from the replicated records with [`DedupWindowFilter::populate`], which is idempotent and keeps the highest timestamp per identity. Leader and follower paths sweep at the same point relative to their insert, so replicas that see one identity sequence hold the same entries in the same order.
//! - **Bounded work.** No operation does work proportional to the index. A cursor visits a few entries for each new identity. It drops expired entries and, near the entry limit, the oldest quarter that its last full pass measured. The index is [`DedupWindowFilter::SEGMENTS`] hash tables, so a table that grows or shrinks moves one segment's entries and not the whole index.
//! - **Bounded memory.** Each entry is a fixed 128-bit digest and a timestamp, whatever the size of the identity. A per-partition entry limit and an optional broker-wide [`DedupBudget`] cap the count. A segment that falls to a quarter of its capacity shrinks, so the heap follows the live entries down after a burst.
//! - **Fail-open.** When the index is full and a bounded step frees nothing, the record is admitted but its identity is not indexed. The filter counts each such record.
//! - **Clamp.** Record timestamps are client-supplied. Each one is clamped to the broker's time before it reaches the index, so one future-dated record cannot move the expiry cutoff past every entry and disable dedup.
//!
//! # Key Types
//!
//! - [`DedupWindowFilter`] — the index for one partition.
//! - [`IdentityDigest`] — the fixed-width digest the index stores.
//! - [`identity_of`] — the rule that picks a record's identity.
//! - [`DedupBudget`] — the entry count shared by every filter on a broker.
//!
//! # Examples
//!
//! ```
//! use krabka_dedup::{
//!     DedupRecord, DedupWindowFilter, IdentityDigest, IdentityLookup, identity_of,
//! };
//!
//! let mut filter = DedupWindowFilter::new(60_000, DedupWindowFilter::DEFAULT_MAX_ENTRIES, None);
//! let now_ms = 1_700_000_000_000;
//!
//! let record = |key: &[u8], timestamp_ms| {
//!     let identity = match identity_of(Some(key), [], None) {
//!         IdentityLookup::Identity(bytes) => Some(IdentityDigest::of(bytes)),
//!         IdentityLookup::NoIdentity | IdentityLookup::MissingHeader => None,
//!     };
//!     DedupRecord {
//!         identity,
//!         timestamp_ms,
//!     }
//! };
//!
//! let first = filter.classify(&[record(b"order-1", now_ms)], now_ms);
//! assert2::assert!(first.all_kept());
//!
//! let again = filter.classify(&[record(b"order-1", now_ms)], now_ms);
//! assert2::assert!(again.none_kept());
//!
//! // Replication of the first request failed: forget what it indexed.
//! filter.revert(&first.undo);
//! assert2::assert!(filter.is_empty());
//! ```

mod budget;
mod digest;
mod filter;
mod identity;

pub use self::{
    budget::DedupBudget,
    digest::{DigestBuildHasher, DigestHasher, IdentityDigest},
    filter::{Classification, DedupCounters, DedupRecord, DedupWindowFilter, Undo, UndoEntry},
    identity::{IdentityLookup, identity_of},
};
