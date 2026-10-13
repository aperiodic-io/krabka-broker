//! `RemoteIndexCache` keeps a bounded on-disk copy of the index objects a
//! remote segment carries, so a consumer reading through a cold segment
//! downloads each index once instead of once per `Fetch`.
//!
//! Mirrors Kafka's `RemoteIndexCache`
//! (`org.apache.kafka.storage.internals.log.RemoteIndexCache`): entries live
//! under `<log_dir>/remote-log-index-cache`, are keyed by the segment's
//! [`RemoteLogSegmentId`](crate::RemoteLogSegmentId) together with the
//! [`IndexType`], and the cache is sized by a total-byte budget
//! (Kafka's `remote.log.index.file.cache.total.size.bytes`, 1 GiB by default)
//! rather than by entry count, because a `.txnindex` and a `.timeindex` differ
//! in size by orders of magnitude. Reaching the budget evicts the
//! least-recently-used entries until the new one fits.
//!
//! The cache directory is emptied on construction. This is a cache of bytes
//! that the remote tier still holds, so a restart re-downloading them costs
//! latency and nothing else, whereas trusting files written by an earlier
//! process would mean trusting a size budget and a segment lineage that the
//! new process cannot check. Kafka reloads its directory; krabka does not,
//! because krabka has no on-disk entry header to validate one against.
//!
//! [`RemoteIndexCache::disabled`] is the pass-through the broker's in-process
//! tests and the [`RemoteReader`]-equivalent unbounded path use: every lookup
//! is a miss and nothing is written.
//!
//! Eviction is also driven from the segment lifecycle:
//! [`RemoteIndexCache::remove_segment`] drops every index of one segment when
//! the `RemoteLogManager` moves it to
//! [`DeleteSegmentStarted`](crate::RemoteLogSegmentState::DeleteSegmentStarted),
//! so a deleted segment's bytes do not hold the budget against live segments.
//!
//! [`RemoteReader`]: https://docs.rs/krabka-broker

use {
    crate::{error::RemoteStorageError, storage_manager::IndexType},
    std::{
        collections::{BTreeMap, HashMap},
        path::{Path, PathBuf},
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    },
    uuid::Uuid,
};

/// The directory, relative to the broker's log directory, that holds the
/// cached index files. The name is Kafka's.
pub const REMOTE_INDEX_CACHE_DIR: &str = "remote-log-index-cache";

/// Kafka's `remote.log.index.file.cache.total.size.bytes` default: 1 GiB.
pub const DEFAULT_INDEX_CACHE_TOTAL_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

/// What a lookup did, so the caller can count hits and misses without
/// reaching into the cache's internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCacheOutcome {
    /// The bytes came from the cache directory; no remote fetch ran.
    Hit,
    /// The bytes came from the fetcher and were written to the cache.
    Miss,
    /// The cache is disabled, so the fetcher ran and nothing was stored.
    Disabled,
}

/// A point-in-time reading of the cache's counters, for the broker's metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexCacheStats {
    /// Lookups answered from the cache directory.
    pub hits: u64,
    /// Lookups that had to download the index object.
    pub misses: u64,
    /// Entries dropped to stay inside the byte budget.
    pub evictions: u64,
    /// Entries currently held.
    pub entries: u64,
    /// Bytes currently held.
    pub bytes: u64,
}

/// One index object of one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CacheKey {
    segment_id: Uuid,
    index_type: IndexType,
}

/// The bookkeeping for one cached file.
#[derive(Debug)]
struct CacheEntry {
    /// Size on disk, which is what the budget counts.
    size: u64,
    /// The `clock` reading of this entry's last use; its key in `order`.
    last_used: u64,
}

/// The mutable half of the cache, behind one lock. The lock is held across the
/// small filesystem operations that keep the map and the directory in step;
/// holding it across the *remote* fetch would serialize every cold read, so
/// the fetcher runs outside it and the result is inserted afterwards.
#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    /// `last_used` → key, so the least-recently-used entry is the first one.
    order: BTreeMap<u64, CacheKey>,
    clock: u64,
    total_bytes: u64,
}

impl CacheState {
    /// Records a use of `key`, moving it to the most-recently-used end.
    fn touch(&mut self, key: CacheKey) {
        self.clock += 1;
        let clock = self.clock;
        if let Some(entry) = self.entries.get_mut(&key) {
            self.order.remove(&entry.last_used);
            entry.last_used = clock;
            self.order.insert(clock, key);
        }
    }

    /// Drops one entry from the bookkeeping and returns the bytes it held.
    fn forget(&mut self, key: CacheKey) -> Option<u64> {
        let entry = self.entries.remove(&key)?;
        self.order.remove(&entry.last_used);
        self.total_bytes = self.total_bytes.saturating_sub(entry.size);
        Some(entry.size)
    }

    /// The least-recently-used key, or `None` when the cache is empty.
    fn lru(&self) -> Option<CacheKey> { self.order.first_key_value().map(|(_, key)| *key) }
}

/// A bounded on-disk cache of remote segment index objects.
#[derive(Debug)]
pub struct RemoteIndexCache {
    /// The cache directory, or `None` when the cache is disabled.
    root: Option<PathBuf>,
    /// The total-byte budget the entries must fit inside.
    max_bytes: u64,
    state: Mutex<CacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl RemoteIndexCache {
    /// Opens (and empties) `<log_dir>/remote-log-index-cache` and returns a
    /// cache bounded by `max_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Io`] when the directory cannot be
    /// emptied or created.
    pub fn new(log_dir: &Path, max_bytes: u64) -> Result<Self, RemoteStorageError> {
        let root = log_dir.join(REMOTE_INDEX_CACHE_DIR);
        if let Err(e) = std::fs::remove_dir_all(&root)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(RemoteStorageError::Io(e));
        }
        std::fs::create_dir_all(&root).map_err(RemoteStorageError::Io)?;
        Ok(Self::empty(Some(root), max_bytes))
    }

    /// A cache that stores nothing: every lookup runs the fetcher and reports
    /// [`IndexCacheOutcome::Disabled`].
    #[must_use]
    pub fn disabled() -> Self { Self::empty(None, 0) }

    fn empty(root: Option<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root,
            max_bytes,
            state: Mutex::new(CacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Whether this cache stores anything.
    #[must_use]
    pub fn is_enabled(&self) -> bool { self.root.is_some() }

    /// The counters the broker publishes.
    ///
    /// # Panics
    ///
    /// Panics if the cache lock was poisoned by a panic inside the cache.
    #[must_use]
    pub fn stats(&self) -> IndexCacheStats {
        let state = self.state.lock().expect("remote index cache lock poisoned");
        IndexCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            bytes: state.total_bytes,
        }
    }

    /// Returns `segment_id`'s `index_type` bytes, running `fetch` only when
    /// the cache does not already hold them.
    ///
    /// `fetch` is called outside the cache lock, so several partitions can
    /// miss concurrently. Two concurrent misses on the same key both download
    /// and the second write replaces the first with identical bytes; a remote
    /// index object is immutable once its segment is finished, so the race
    /// costs one extra download and never returns the wrong bytes.
    ///
    /// # Errors
    ///
    /// Returns whatever `fetch` returns on a miss. A cache-directory failure
    /// is not an error: the fetched bytes are returned and the entry is
    /// simply not stored.
    ///
    /// # Panics
    ///
    /// Panics if the cache lock was poisoned by a panic inside the cache.
    pub fn get_or_fetch<F>(
        &self,
        segment_id: Uuid,
        index_type: IndexType,
        fetch: F,
    ) -> Result<(Vec<u8>, IndexCacheOutcome), RemoteStorageError>
    where
        F: FnOnce() -> Result<Vec<u8>, RemoteStorageError>,
    {
        let Some(root) = self.root.as_ref() else {
            return Ok((fetch()?, IndexCacheOutcome::Disabled));
        };
        let key = CacheKey {
            segment_id,
            index_type,
        };
        let path = entry_path(root, key);
        if self.claim_hit(key) {
            match std::fs::read(&path) {
                Ok(bytes) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok((bytes, IndexCacheOutcome::Hit));
                }
                Err(error) => {
                    // The file went missing under us. Drop the bookkeeping and
                    // fall through to a download rather than failing a read
                    // the remote tier can still answer.
                    tracing::debug!(
                        error = %error,
                        path = %path.display(),
                        "remote index cache: entry unreadable; re-fetching"
                    );
                    self.drop_entry(key, &path);
                }
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let bytes = fetch()?;
        self.store(key, &path, &bytes);
        Ok((bytes, IndexCacheOutcome::Miss))
    }

    /// Drops every cached index of `segment_id`.
    ///
    /// The `RemoteLogManager` calls this when a segment enters
    /// `DeleteSegmentStarted`, so the deleted segment's bytes stop counting
    /// against the budget.
    ///
    /// # Panics
    ///
    /// Panics if the cache lock was poisoned by a panic inside the cache.
    pub fn remove_segment(&self, segment_id: Uuid) {
        let Some(root) = self.root.as_ref() else {
            return;
        };
        for index_type in IndexType::ALL {
            let key = CacheKey {
                segment_id,
                index_type,
            };
            self.drop_entry(key, &entry_path(root, key));
        }
    }

    /// Whether the cache currently claims to hold `key`; bumps its recency
    /// when it does. The read that follows is what settles whether the file is
    /// really there.
    fn claim_hit(&self, key: CacheKey) -> bool {
        let mut state = self.state.lock().expect("remote index cache lock poisoned");
        if !state.entries.contains_key(&key) {
            return false;
        }
        state.touch(key);
        true
    }

    /// Removes one entry from the map and from the directory.
    fn drop_entry(&self, key: CacheKey, path: &Path) {
        let mut state = self.state.lock().expect("remote index cache lock poisoned");
        if state.forget(key).is_some() {
            remove_quietly(path);
        }
    }

    /// Writes `bytes` into the cache directory and makes room for them.
    ///
    /// An entry larger than the whole budget is fetched and returned but never
    /// stored: caching it would evict everything else and then evict itself.
    fn store(&self, key: CacheKey, path: &Path, bytes: &[u8]) {
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if size > self.max_bytes {
            return;
        }
        // Publish a completed file, never truncate a path that a cache hit
        // may already be reading. Keep publication and eviction serialized
        // so an old eviction cannot unlink a newly inserted entry.
        let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
        if let Err(error) = std::fs::write(&temporary, bytes) {
            remove_quietly(&temporary);
            tracing::debug!(error = %error, path = %path.display(),
                "remote index cache: temporary entry not stored");
            return;
        }
        let mut state = self.state.lock().expect("remote index cache lock poisoned");
        let published = std::fs::rename(&temporary, path);
        if let Err(error) = published {
            remove_quietly(&temporary);
            tracing::debug!(
                error = %error,
                path = %path.display(),
                "remote index cache: entry not stored"
            );
            return;
        }
        let evicted = {
            state.forget(key);
            let mut evicted = Vec::new();
            while state.total_bytes + size > self.max_bytes {
                let Some(victim) = state.lru() else { break };
                state.forget(victim);
                evicted.push(victim);
            }
            state.clock += 1;
            let clock = state.clock;
            state.entries.insert(key, CacheEntry {
                size,
                last_used: clock,
            });
            state.order.insert(clock, key);
            state.total_bytes += size;
            evicted
        };
        if let Some(root) = self.root.as_ref() {
            for victim in &evicted {
                remove_quietly(&entry_path(root, *victim));
            }
        }
        drop(state);
        self.evictions.fetch_add(
            u64::try_from(evicted.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

/// The cache file for one index of one segment. Segment ids are unique across
/// partitions, so the id and the Kafka suffix name the file on their own.
fn entry_path(root: &Path, key: CacheKey) -> PathBuf {
    root.join(format!(
        "{}{}",
        key.segment_id.as_hyphenated(),
        key.index_type.suffix()
    ))
}

/// Deletes a cache file, treating a failure as nothing worth failing a read
/// over: the entry is already gone from the map, so the file is at worst
/// stale bytes that no lookup can reach.
fn remove_quietly(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::debug!(
            error = %error,
            path = %path.display(),
            "remote index cache: stale entry not removed"
        ),
    }
}

#[cfg(test)]
mod tests;
