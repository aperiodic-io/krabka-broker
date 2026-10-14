//! Index for produce-time, windowed, first-wins record deduplication.
//!
//! The broker drops a record at produce time when a record with the same dedup identity was already admitted within the window. The identity is the record's Kafka key, or the value of one configured header. This crate holds the index that makes that decision. It parses no batches and reads no clock: the caller gives it digested identities, record timestamps, and the broker's time, and ticks it at a regular interval.
//!
//! # Model
//!
//! - **First-wins.** The first record for an identity is kept. A later record for that identity is dropped while the stored entry is live and the two timestamps are within `window_ms` of each other. This also applies between records of one produce request.
//! - **Broker clock.** Expiry uses the broker's time, as Kafka's `retention.ms` does. Every call moves a watermark to the `now_ms` it gives, and the watermark never moves back. An entry is live while it is at most `window_ms` behind the watermark, and a lookup treats an entry that is not live as absent. A record that is already more than the window behind on arrival, such as a backfill with old `CreateTime`s, is admitted and not indexed, and counted. So an index rebuilt from the last window of the log agrees with one that has run all along.
//! - **Advisory and derived from the log.** The leader classifies each produce batch under the partition's log lock, immediately before it appends the batch, and [`DedupWindowFilter::revert`] undoes the classification when the append fails. Plain and idempotent batches are deduplicated: a dropped record leaves a gap in the offsets, so producer sequence numbers stay intact. Transactional batches bypass dedup. Every replica indexes what it appends with [`DedupWindowFilter::populate`], which is idempotent and keeps the highest timestamp per identity. Leader and follower paths sweep at the same point relative to their insert, so replicas that make the same calls at the same times hold the same entries in the same order.
//! - **Bounded work.** No operation does work proportional to the index. A cursor visits a few entries for each new identity, and [`DedupWindowFilter::TICK_VISITS`] for each [`DedupWindowFilter::tick`]. It drops entries that are not live and, for a new identity near the entry limit, the oldest quarter that its last full pass measured. Ticks release the entries of a partition with no traffic, and their share of the budget. The index is [`DedupWindowFilter::SEGMENTS`] hash tables, so a table that grows or shrinks moves one segment's entries and not the whole index.
//! - **Bounded memory.** Each entry is a fixed 128-bit digest and a timestamp, whatever the size of the identity. A per-partition entry limit and an optional broker-wide [`DedupBudget`] cap the count. A segment that falls to a quarter of its capacity shrinks, so the heap follows the live entries down after a burst.
//! - **Fail-open.** When the index is full and a bounded step frees nothing, the record is admitted but its identity is not indexed. The filter counts each such record.
//! - **Clamp.** Record timestamps are client-supplied. Each one is clamped to the broker's time before it reaches the index, so an entry dated ahead of the broker cannot stay live past its window.
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
//!     let identity = match identity_of(Some(key), |_| None, None) {
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
//! // The append of the first request failed: forget what it indexed.
//! filter.revert(&first.undo);
//! assert2::assert!(filter.is_empty());
//!
//! // Once a window has passed, ticks release expired entries with no traffic.
//! filter.classify(&[record(b"order-2", now_ms)], now_ms);
//! filter.tick(now_ms + 60_001);
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
    identity::{IdentityLookup, first_header, identity_of},
};
