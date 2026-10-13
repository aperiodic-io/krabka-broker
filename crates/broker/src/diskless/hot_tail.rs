//! In-memory cache for quorum-committed diskless WAL tail batches.

use {
    crate::config::DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE,
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_protocol::records::RecordBatch,
    krabka_units::convert::ByteSizeExt as _,
    std::{
        collections::{BTreeMap, HashMap, VecDeque},
        sync::Mutex,
    },
    uuid::Uuid,
};

/// Advisory cache of recently quorum-committed diskless WAL batches.
#[derive(Debug)]
pub(crate) struct HotTailCache {
    max_bytes: usize,
    state: Mutex<HotTailState>,
}

impl Default for HotTailCache {
    fn default() -> Self {
        Self::new(
            DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE
                .bytes_u64()
                .try_into()
                .unwrap_or(usize::MAX),
        )
    }
}

impl HotTailCache {
    #[must_use]
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(HotTailState::default()),
        }
    }

    pub(crate) fn insert_run(&self, topic_id: Uuid, partition: PartitionIndex, bytes: &Bytes) {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let mut cur = bytes.slice(offset..);
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                return;
            };
            let len = batch.encoded_len();
            if len == 0 || offset + len > bytes.len() {
                return;
            }
            self.insert_batch(
                topic_id,
                partition,
                bytes.slice(offset..offset + len),
                &batch,
            );
            offset += len;
        }
    }

    /// The cached batch that covers `fetch_offset`, or `None`.
    ///
    /// `limit_offset` is the exclusive upper bound of the fetch's visibility
    /// window, and the cache honours it in whole batches: a batch that reaches
    /// at or beyond it is a miss, and the fetch falls through to the log read
    /// path that can clamp inside the run. Nothing here may be looser than that
    /// window. A `read_uncommitted` fetch is capped at the lower of the high
    /// watermark and KFC-1's delivery watermark, and the delivery watermark can
    /// sit below a batch that is durable and quorum-committed but not yet due.
    pub(crate) fn get(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        fetch_offset: i64,
        limit_offset: i64,
        max_bytes: usize,
    ) -> Option<Bytes> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let map = state.entries.get(&(topic_id, partition.0))?;
        let (_base, entry) = map.range(..=fetch_offset).next_back()?;
        if fetch_offset <= entry.last_offset
            && entry.last_offset < limit_offset
            && entry.bytes.len() <= max_bytes
        {
            Some(entry.bytes.clone())
        } else {
            None
        }
    }

    fn insert_batch(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        bytes: Bytes,
        batch: &RecordBatch,
    ) {
        let base_offset = batch.base_offset;
        let last_offset = base_offset + i64::from(batch.last_offset_delta);
        let key = (topic_id, partition.0);
        let len = bytes.len();
        let bytes = if len <= self.max_bytes {
            Bytes::copy_from_slice(&bytes)
        } else {
            bytes
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(old) = state
            .entries
            .get_mut(&key)
            .and_then(|batches| batches.remove(&base_offset))
        {
            state.total_bytes -= old.bytes.len();
            state.order.retain(|cached| *cached != (key, base_offset));
        }
        if len > self.max_bytes {
            if state.entries.get(&key).is_some_and(BTreeMap::is_empty) {
                state.entries.remove(&key);
            }
            return;
        }

        state
            .entries
            .entry(key)
            .or_default()
            .insert(base_offset, HotTailEntry { last_offset, bytes });
        state.order.push_back((key, base_offset));
        state.total_bytes += len;
        while state.total_bytes > self.max_bytes {
            let Some((old_key, old_offset)) = state.order.pop_front() else {
                break;
            };
            let removed = state
                .entries
                .get_mut(&old_key)
                .and_then(|batches| batches.remove(&old_offset));
            if let Some(removed) = removed {
                state.total_bytes -= removed.bytes.len();
            }
            if state.entries.get(&old_key).is_some_and(BTreeMap::is_empty) {
                state.entries.remove(&old_key);
            }
        }
    }

    pub(crate) fn remove_partition(&self, topic_id: Uuid, partition: PartitionIndex) {
        let key = (topic_id, partition.0);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entries) = state.entries.remove(&key) {
            state.total_bytes -= entries
                .into_values()
                .map(|entry| entry.bytes.len())
                .sum::<usize>();
            state.order.retain(|(cached, _)| *cached != key);
        }
    }
}

#[derive(Debug, Default)]
struct HotTailState {
    total_bytes: usize,
    entries: HashMap<(Uuid, i32), BTreeMap<i64, HotTailEntry>>,
    order: VecDeque<((Uuid, i32), i64)>,
}

#[derive(Debug, Clone)]
struct HotTailEntry {
    last_offset: i64,
    bytes: Bytes,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, bytes::BytesMut, krabka_protocol::records::Record};

    /// An offset above every cached batch, for a lookup the window does not
    /// bound.
    const UNBOUNDED: i64 = i64::MAX;

    #[test]
    fn hot_tail_cache_floor_lookup_and_bound() {
        let topic_id = Uuid::from_u128(7);
        let first = batch_bytes(0, 1);
        let second = batch_bytes(2, 2);
        let cache = HotTailCache::new(second.len());

        cache.insert_run(topic_id, PartitionIndex(0), &first);
        cache.insert_run(topic_id, PartitionIndex(0), &second);

        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, UNBOUNDED, usize::MAX)
                .is_none()
        );
        check!(cache.get(topic_id, PartitionIndex(0), 2, UNBOUNDED, usize::MAX) == Some(second));
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 3, UNBOUNDED, usize::MAX)
                .is_some()
        );
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 3, UNBOUNDED, 1)
                .is_none()
        );
    }

    #[test]
    fn hot_tail_cache_serves_only_a_batch_that_ends_below_the_limit() {
        let cache = HotTailCache::new(usize::MAX);
        let topic_id = Uuid::from_u128(11);
        // One batch per run: [0, 1] and [2, 3].
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(0, 2));
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(2, 2));

        // The window ends inside the second batch, which is what a delivery
        // watermark below a batch that is not due yet looks like.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 2, 3, usize::MAX)
                .is_none()
        );
        // The window ends exactly at the batch's last offset, so the batch is
        // still partly outside it.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 1, usize::MAX)
                .is_none()
        );
        // The window covers the whole batch.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 2, usize::MAX)
                .is_some()
        );
        // Nothing at all is deliverable yet.
        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, 0, usize::MAX)
                .is_none()
        );
    }

    #[test]
    fn hot_tail_cache_is_byte_bounded_across_partitions() {
        let cache = HotTailCache::new(batch_bytes(2, 1).len());
        let topic_id = Uuid::from_u128(12);
        let first = batch_bytes(0, 1);
        let second = batch_bytes(2, 1);

        cache.insert_run(topic_id, PartitionIndex(0), &first);
        cache.insert_run(topic_id, PartitionIndex(1), &second);

        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, UNBOUNDED, usize::MAX)
                .is_none()
        );
        check!(cache.get(topic_id, PartitionIndex(1), 2, UNBOUNDED, usize::MAX) == Some(second));
    }

    #[test]
    fn oversized_replacement_removes_the_cached_batch() {
        let topic_id = Uuid::from_u128(13);
        let first = batch_bytes(0, 1);
        let cache = HotTailCache::new(first.len());

        cache.insert_run(topic_id, PartitionIndex(0), &first);
        cache.insert_run(topic_id, PartitionIndex(0), &batch_bytes(0, 2));

        check!(
            cache
                .get(topic_id, PartitionIndex(0), 0, UNBOUNDED, usize::MAX)
                .is_none()
        );
    }

    #[test]
    fn cached_batch_does_not_retain_the_run_allocation() {
        let topic_id = Uuid::from_u128(14);
        let large = batch_bytes(0, 100);
        let small = batch_bytes(100, 1);
        let mut run = BytesMut::with_capacity(large.len() + small.len());
        run.extend_from_slice(&large);
        run.extend_from_slice(&small);
        let run = run.freeze();
        let small_in_run = run[large.len()..].as_ptr();
        let cache = HotTailCache::new(small.len());

        cache.insert_run(topic_id, PartitionIndex(0), &run);

        let cached = cache
            .get(topic_id, PartitionIndex(0), 100, UNBOUNDED, usize::MAX)
            .expect("small batch is cached");
        check!(cached.as_ptr() != small_in_run);
        check!(cached == small);
    }

    fn batch_bytes(base_offset: i64, records: i32) -> Bytes {
        let mut batch = RecordBatch {
            base_offset,
            last_offset_delta: records - 1,
            ..RecordBatch::default()
        };
        for offset_delta in 0..records {
            batch.records.push(Record {
                offset_delta,
                ..Record::default()
            });
        }
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    }
}
