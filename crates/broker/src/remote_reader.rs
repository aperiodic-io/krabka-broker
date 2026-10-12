//! KIP-405 remote read path.
//!
//! This module wraps the broker's shared [`RemoteStorageManager`] and
//! [`RemoteLogMetadataManager`] pair. It serves `Fetch` and `ListOffsets`
//! requests for offsets that have no local copy any more.
//!
//! The RSM and RLMM SPIs are synchronous and blocking. This module therefore
//! wraps byte-range reads, index reads, and `ListOffsets` metadata scans in
//! [`crate::blocking::spawn_blocking`], so those remote-tier operations do not
//! stall the broker's reactor. It decodes the fetched bytes with
//! [`krabka_remote_storage::index`], whose lookups mirror
//! `krabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the Kafka-format
//! index bytes that the copy path wrote verbatim.

use std::sync::Arc;

use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    LogOffset, RemoteIndexCache, RemoteLogMetadataManager, RemoteStorageManager,
};

mod aborted_txns;
mod blocking;
mod delivery;
mod fetch;
mod partition_read;
mod pool;
#[cfg(test)]
pub(crate) mod test_support;
mod tiered_offsets;
mod timestamp_lookup;

pub(crate) use delivery::segment_activation_frontier;
pub(crate) use pool::ReaderPool;

/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: LogOffset,
    pub(crate) last_offset: LogOffset,
    pub(crate) producer_id: i64,
}

/// The bounds a `RemoteReader` reads under: how many cold reads may be in
/// flight, how many may wait, and the index cache that keeps a consumer
/// walking one segment from re-downloading its indexes.
///
/// [`RemoteReaderLimits::unbounded`] is the shape the in-process tests use:
/// no cap and no cache, which is what the reader did before KIP-405's own
/// limits were wired in.
pub(crate) struct RemoteReaderLimits {
    pub(crate) index_cache: Arc<RemoteIndexCache>,
    pub(crate) pool: ReaderPool,
}

impl RemoteReaderLimits {
    /// No concurrency cap and no index cache: the shape the in-process tests
    /// that are not about the limits themselves read under.
    #[cfg(test)]
    pub(crate) fn unbounded() -> Self {
        Self {
            index_cache: Arc::new(RemoteIndexCache::disabled()),
            pool: ReaderPool::unbounded(),
        }
    }
}

/// Holds the broker's shared `RSM` and `RLMM`, and serves remote reads.
pub(crate) struct RemoteReader {
    pub(crate) rsm: Arc<dyn RemoteStorageManager>,
    pub(crate) rlmm: Arc<dyn RemoteLogMetadataManager>,
    /// The bounded on-disk cache of the segments' index objects.
    pub(crate) index_cache: Arc<RemoteIndexCache>,
    /// The cap on concurrent cold-tier reads.
    pub(crate) pool: ReaderPool,
}

/// The last offset durably copied to the remote tier and the leader epoch
/// that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TieredOffset {
    pub(crate) offset: LogOffset,
    pub(crate) leader_epoch: LeaderEpoch,
}

impl RemoteReader {
    /// A reader with no concurrency cap and no index cache.
    #[cfg(test)]
    pub(crate) fn new(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
    ) -> Self {
        Self::with_limits(rsm, rlmm, RemoteReaderLimits::unbounded())
    }

    /// A reader bounded by `limits`. This is what the broker builds when
    /// tiered storage is on.
    pub(crate) fn with_limits(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
        limits: RemoteReaderLimits,
    ) -> Self {
        Self {
            rsm,
            rlmm,
            index_cache: limits.index_cache,
            pool: limits.pool,
        }
    }
}
