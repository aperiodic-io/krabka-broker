//! Remote reads: a byte range of a segment body, and a whole index object.
//!
//! Each read derives one key: an absent object is a miss, with no second key
//! to try. Index reads check metadata before a capped download. Both pass the write-only archive guard
//! first, which refuses the read before a key is derived or a request leaves
//! the process.

use {
    super::S3RemoteStorage,
    crate::{
        error::RemoteStorageError, metadata::RemoteLogSegmentMetadata, storage_manager::IndexType,
        worm::WormError,
    },
    krabka_object_store::{ObjectOps, ObjectStoreError},
    object_store::GetRange,
};

fn object_bytes(
    result: Result<bytes::Bytes, ObjectStoreError>,
    metadata: &RemoteLogSegmentMetadata,
) -> Result<Vec<u8>, RemoteStorageError> {
    match result {
        Ok(bytes) => Ok(bytes.into()),
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

    /// Reads one whole index object within the configured memory limit.
    pub(super) fn fetch_index_bytes(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.refuse_read_when_write_only()?;
        let key = self.index_key(metadata, index_type);
        object_bytes(
            Self::block_os(self.ops.get_capped(&key, self.max_index_bytes)),
            metadata,
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{IndexType, RemoteStorageError, S3RemoteStorage, WormError},
        crate::{
            s3::test_support::{
                SeededCopySetup, WormAccess, counting_rsm, rsm, sample_metadata, seeded_blocking,
                stamped_metadata, worm_rsm,
            },
            storage_manager::RemoteStorageManager,
            test_support::{TransactionIndex, check_sample_indexes},
        },
        assert2::{assert, check},
        krabka_object_store::{ObjectOps as _, PutRequest, fault::StoreOp},
        object_store::{ObjectStore, memory::InMemory},
        std::sync::Arc,
        tempfile::TempDir,
    };

    macro_rules! seeded_without_transaction_index {
        ($store:expr, $then:expr) => {{
            let setup = SeededCopySetup::without_transaction_index();
            seeded_blocking()
                .store($store)
                .metadata(setup.metadata)
                .transaction_index(setup.transaction_index)
                .then($then)
                .call()
        }};
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn whole_index_reads_enforce_the_configured_cap_for_every_index_type() {
        let (store, counter) = counting_rsm();
        let store = store.with_max_index_bytes(4);
        let md = sample_metadata(uuid::Uuid::from_u128(11));
        for index in IndexType::ALL {
            let key = store.index_key(&md, index);
            store
                .ops
                .put(
                    &key,
                    bytes::Bytes::from_static(b"12345"),
                    PutRequest::default(),
                )
                .await
                .unwrap();
        }
        tokio::task::spawn_blocking(move || {
            for index in IndexType::ALL {
                let before = counter.attempts(StoreOp::Get);
                let error = store.fetch_index(&md, index).unwrap_err();
                let key = store.index_key(&md, index);
                assert!(matches!(error, RemoteStorageError::Backend(ref message)
                    if message == &format!("object `{key}` is 5 bytes, exceeds cap of 4 bytes")));
                // Refuse at HEAD, before downloading the body.
                assert!(counter.attempts(StoreOp::Get) - before == 1);
                S3RemoteStorage::block_os(store.ops.put(
                    &key,
                    bytes::Bytes::from_static(b"1234"),
                    PutRequest::default(),
                ))
                .unwrap();
                assert!(store.fetch_index(&md, index).unwrap() == b"1234");
            }
        })
        .await
        .unwrap();
    }

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
        seeded_without_transaction_index!(store, move |store, md| {
            // Inclusive [2, 5] -> "2345".
            assert!(store.fetch_log_segment(&md, 2, Some(5)).unwrap() == b"2345");
            // Open-ended from 7 -> "789".
            assert!(store.fetch_log_segment(&md, 7, None).unwrap() == b"789");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_single_byte_range_start_equals_end() {
        let store = rsm(None);
        seeded_without_transaction_index!(store, move |store, md| {
            // Inclusive [3, 3] is a valid single-byte range -> "3" (the guard
            // is `end < start_position`, not `<=`/`==`).
            assert!(store.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_each_index_type() {
        let store = rsm(None);
        let md = sample_metadata(uuid::Uuid::from_u128(11));
        seeded_blocking()
            .store(store)
            .metadata(md)
            .then(move |store, md| {
                check_sample_indexes(&store, &md);
            })
            .call()
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
        seeded_blocking()
            .store(store)
            .metadata(md)
            .transaction_index(TransactionIndex::Omitted)
            .then(move |store, md| {
                let err = store.fetch_index(&md, IndexType::Transaction).unwrap_err();
                assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
            })
            .call()
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
        let readable = worm_rsm(Arc::clone(&backing), &readable_keys, WormAccess::ReadWrite);
        let write_only = worm_rsm(backing, &sealed_keys, WormAccess::WriteOnly);
        let md = stamped_metadata()
            .segment_id(uuid::Uuid::from_u128(56))
            .call();

        let readable_md = md.clone();
        seeded_blocking()
            .store(readable)
            .metadata(readable_md)
            .transaction_index(TransactionIndex::Omitted)
            .then(move |readable, readable_md| {
                check!(readable.fetch_log_segment(&readable_md, 0, None).unwrap() == b"0123456789");
                check!(
                    readable
                        .fetch_index(&readable_md, IndexType::Offset)
                        .unwrap()
                        == b"OFFSET-IDX"
                );
            })
            .call()
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
