//! Remote reads: a byte range of a segment body, and a whole index object.
//!
//! Each read derives one key and issues one request: an absent object is a
//! miss, with no second key to try. Both pass the write-only archive guard
//! first, which refuses the read before a key is derived or a request leaves
//! the process.

use krabka_object_store::{ObjectOps, ObjectStoreError};
use object_store::GetRange;

use super::S3RemoteStorage;
use crate::{
    error::RemoteStorageError, metadata::RemoteLogSegmentMetadata, storage_manager::IndexType,
    worm::WormError,
};

fn object_bytes(
    result: Result<bytes::Bytes, ObjectStoreError>,
    metadata: &RemoteLogSegmentMetadata,
) -> Result<Vec<u8>, RemoteStorageError> {
    match result {
        Ok(bytes) => Ok(bytes.to_vec()),
        Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        )),
        Err(other) => Err(other.into()),
    }
}

impl S3RemoteStorage {
    /// Refuses a remote read when the archive is configured write-only.
    ///
    /// Callers must invoke this before they derive a key or reach the store,
    /// so that no request the archive is going to reject ever leaves the
    /// process.
    fn refuse_read_when_write_only(&self) -> Result<(), RemoteStorageError> {
        if self.worm.as_ref().is_some_and(|worm| worm.write_only) {
            return Err(RemoteStorageError::Worm(WormError::ReadRefused));
        }
        Ok(())
    }

    /// Reads `[start_position, end_position]` of the segment body, or the
    /// whole tail from `start_position` when no end is given.
    pub(super) fn fetch_segment_range(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.refuse_read_when_write_only()?;
        let key = self.log_key(metadata);
        if let Some(end) = end_position
            && end < start_position
        {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "end_position {end} < start_position {start_position}"
            )));
        }
        let range = match end_position {
            Some(end) => {
                // GetRange::Bounded is half-open [start, end); the trait
                // contract is inclusive end, so add 1 and saturate.
                GetRange::Bounded(u64::from(start_position)..u64::from(end).saturating_add(1))
            }
            None => GetRange::Offset(u64::from(start_position)),
        };
        object_bytes(Self::block_os(self.ops.get_range(&key, range)), metadata)
    }

    /// Reads one whole index object of a segment.
    pub(super) fn fetch_index_bytes(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.refuse_read_when_write_only()?;
        let key = self.index_key(metadata, index_type);
        object_bytes(Self::block_os(self.ops.get(&key)), metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_object_store::fault::StoreOp;
    use object_store::{ObjectStore, memory::InMemory};
    use tempfile::TempDir;

    use super::{IndexType, RemoteStorageError, WormError};
    use crate::{
        s3::test_support::{
            counting_rsm, rsm, sample_metadata, seeded_blocking, stamped_metadata, worm_rsm,
        },
        storage_manager::RemoteStorageManager,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_object_is_one_request() {
        let (store, counter) = counting_rsm();
        let md = sample_metadata(uuid::Uuid::from_u128(11));
        tokio::task::spawn_blocking(move || {
            let before = counter.attempts(StoreOp::Get);
            check!(matches!(
                store.fetch_log_segment(&md, 0, None),
                Err(RemoteStorageError::SegmentNotFound(_))
            ));
            // One key, one request: a miss is a miss, not a second attempt.
            check!(counter.attempts(StoreOp::Get) - before == 1);

            let before = counter.attempts(StoreOp::Get);
            check!(matches!(
                store.fetch_index(&md, IndexType::Offset),
                Err(RemoteStorageError::SegmentNotFound(_))
            ));
            check!(counter.attempts(StoreOp::Get) - before == 1);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_partial_byte_ranges() {
        let store = rsm(None);
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup::without_transaction_index(),
            move |store, md| {
                // Inclusive [2, 5] -> "2345".
                assert!(store.fetch_log_segment(&md, 2, Some(5)).unwrap() == b"2345");
                // Open-ended from 7 -> "789".
                assert!(store.fetch_log_segment(&md, 7, None).unwrap() == b"789");
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_single_byte_range_start_equals_end() {
        let store = rsm(None);
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup::without_transaction_index(),
            move |store, md| {
                // Inclusive [3, 3] is a valid single-byte range -> "3" (the guard
                // is `end < start_position`, not `<=`/`==`).
                assert!(store.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_each_index_type() {
        let store = rsm(None);
        let md = sample_metadata(uuid::Uuid::from_u128(11));
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: md,
                ..Default::default()
            },
            move |store, md| {
                crate::test_support::check_sample_indexes(&store, &md);
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_before_copy_is_not_found() {
        let store = rsm(None);
        let md = sample_metadata(uuid::Uuid::from_u128(404));
        let err = tokio::task::spawn_blocking(move || store.fetch_log_segment(&md, 0, None))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_optional_txn_index_is_not_found() {
        let store = rsm(None);
        let md = sample_metadata(uuid::Uuid::from_u128(12));
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: md,
                transaction_index: crate::test_support::TransactionIndex::Omitted,
            },
            move |store, md| {
                let err = store.fetch_index(&md, IndexType::Transaction).unwrap_err();
                assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
            },
        )
        .await;
    }

    /// The write-only guard must fire before a key is derived or a request is
    /// built.
    ///
    /// The proof is the missing Tokio runtime. `block_os` reaches the store
    /// only through `Handle::try_current`, so on a plain thread any call that
    /// got as far as the store would come back as
    /// [`RemoteStorageError::Backend`]. A `ReadRefused` from that thread can
    /// therefore only mean the guard returned first. The twin archive over the
    /// same backing store proves the object is there to be read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_only_worm_refuses_fetch_without_touching_the_store() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let readable_keys = TempDir::new().unwrap();
        let sealed_keys = TempDir::new().unwrap();
        let readable = worm_rsm(
            Arc::clone(&backing),
            &readable_keys,
            crate::s3::test_support::WormAccess::ReadWrite,
        );
        let write_only = worm_rsm(
            backing,
            &sealed_keys,
            crate::s3::test_support::WormAccess::WriteOnly,
        );
        let md = stamped_metadata(crate::s3::test_support::StampedMetadataSetup {
            segment_id: uuid::Uuid::from_u128(56),
            ..Default::default()
        });

        let readable_md = md.clone();
        seeded_blocking(
            readable,
            crate::s3::test_support::SeededCopySetup {
                metadata: readable_md,
                transaction_index: crate::test_support::TransactionIndex::Omitted,
            },
            move |readable, readable_md| {
                check!(readable.fetch_log_segment(&readable_md, 0, None).unwrap() == b"0123456789");
                check!(
                    readable
                        .fetch_index(&readable_md, IndexType::Offset)
                        .unwrap()
                        == b"OFFSET-IDX"
                );
            },
        )
        .await;

        let (segment, index) = std::thread::spawn(move || {
            (
                write_only.fetch_log_segment(&md, 0, None),
                write_only.fetch_index(&md, IndexType::Offset),
            )
        })
        .join()
        .unwrap();

        check!(matches!(
            segment,
            Err(RemoteStorageError::Worm(WormError::ReadRefused))
        ));
        check!(matches!(
            index,
            Err(RemoteStorageError::Worm(WormError::ReadRefused))
        ));
    }
}
