//! KFC-10 produce-time record deduplication, as the broker runs it.
//!
//! A topic that sets `dedup.window.ms` drops a record at produce time when a
//! record with the same identity was admitted within the window. The identity
//! is the record key, or the value of the header that `dedup.key.header`
//! names. [`krabka_dedup`] holds the index itself. This module connects that
//! index to the partition writer, the produce handler, and the log.
//!
//! The index is a function of the partition log. Each partition writer owns a
//! [`PartitionDedup`] for its replica. The leader classifies each client batch
//! under the log lock, immediately before the append, so the classification
//! and the append are one step. A follower indexes every batch it replicates,
//! with the same rules, so a promoted follower has an index that is ready to
//! use. A truncation, a dedup config change, or a broker restart marks the
//! index stale. The writer then rebuilds it from the last window of the log
//! before the next append, so the index never depends on state that the log
//! does not hold. Nothing that this module keeps goes to disk.
//!
//! A dropped record leaves a gap in the offsets of its batch, as compaction
//! does. A batch whose records were all dropped is appended as an empty
//! batch. So the produce response reports the exact offset of each record
//! that the producer sent, an idempotent producer's sequence numbers stay
//! continuous, and an `acks=all` duplicate waits for the high watermark to
//! pass the record that it duplicates.

mod config;
mod identity;
mod partition;
mod rebuild;
mod rewrite;

pub use self::config::{DedupConfig, DedupRuntime, DedupSettings};
pub(crate) use self::{
    identity::{BatchSource, CollectedIdentities, IdentityCollector},
    partition::{ClassifiedAppend, DedupJob, PartitionDedup},
};

#[cfg(test)]
mod stress_tests;
