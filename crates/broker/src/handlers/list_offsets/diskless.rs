//! The earliest offset a diskless partition can answer.
//!
//! A diskless topic keeps its oldest records in shared object storage rather
//! than in the local log, so the `EARLIEST` sentinel takes the lower of the
//! local log start offset and the first offset the WAL index still answers
//! for. The index raises that to the partition's `DeleteRecords` floor, so a
//! trim an operator ran is visible here from the moment it lands rather than
//! from the flush tick that tombstones the ranges below it.

use crate::diskless::read::DisklessReadHandle;
#[cfg(test)]
use crate::diskless::wal_index::WalIndexCache;

pub(super) async fn diskless_earliest_candidate(
    diskless_read: Option<&DisklessReadHandle>,
    topic_id: Option<uuid::Uuid>,
    partition: i32,
) -> Option<i64> {
    let (handle, topic_id) = diskless_read.zip(topic_id)?;
    handle
        .index
        .lock()
        .await
        .earliest_covered(topic_id, partition)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::diskless::wal_index::WalFlushRecord, assert2::assert, std::sync::Arc};

    #[tokio::test]
    async fn diskless_earliest_uses_object_floor_but_leaves_local_floor_visible() {
        let topic_id = uuid::Uuid::from_u128(42);
        let mut cache = WalIndexCache::default();
        cache.apply(&WalFlushRecord {
            object_key: "o".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 8,
                byte_start: 0,
                byte_len: 1,
                max_timestamp_ms: 0,
            }],
        });
        let handle = DisklessReadHandle::new(
            Arc::new(tokio::sync::Mutex::new(cache)),
            Arc::new(object_store::memory::InMemory::new()),
        );

        let earliest = diskless_earliest_candidate(Some(&handle), Some(topic_id), 0).await;
        assert!(earliest == Some(0));

        handle.index.lock().await.raise_delete_floor(topic_id, 0, 4);

        let after_trim = diskless_earliest_candidate(Some(&handle), Some(topic_id), 0).await;
        assert!(after_trim == Some(4));
    }
}
