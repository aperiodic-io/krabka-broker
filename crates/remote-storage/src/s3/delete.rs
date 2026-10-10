//! Segment deletion: the idempotent sweep of every key a copy could have
//! written, and the archive backstop that refuses to issue a delete at all.
//!
//! The sweep issues one delete per artifact a copy could have written, and
//! nothing else. A WORM archive never reaches the sweep, because a store that
//! is never asked cannot be talked into obliging.

use krabka_object_store::{ObjectOps, ObjectStoreError};

use super::S3RemoteStorage;
use crate::{
    error::RemoteStorageError, metadata::RemoteLogSegmentMetadata,
    storage_manager::segment_suffixes, worm::WormError,
};

impl S3RemoteStorage {
    /// Deletes every object of one segment: one request per artifact.
    pub(super) fn delete_segment_objects(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        if self.worm.is_some() {
            // The backstop that makes the write-once guarantee hold against
            // any caller. No delete is issued at all, not even for an object
            // that is absent: a store that is never asked cannot be talked
            // into obliging.
            return Err(RemoteStorageError::Worm(WormError::DeleteRefused {
                key: self.log_key(metadata).to_string(),
            }));
        }
        for key in segment_suffixes().map(|suffix| self.segment_key(metadata, suffix)) {
            match Self::block_os(self.ops.delete(&key)) {
                // Idempotent: deleting an absent object succeeds.
                Ok(()) | Err(ObjectStoreError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use assert2::{assert, check};
    use krabka_object_store::fault::StoreOp;
    use tempfile::TempDir;

    use super::{ObjectOps, RemoteStorageError, S3RemoteStorage, WormError};
    use crate::{
        s3::test_support::{
            counting_rsm, rsm, sample_data, sample_metadata, seeded_blocking, stamped_metadata,
        },
        storage_manager::{IndexType, RemoteStorageManager},
    };

    /// Every key the backing store currently holds, sorted.
    fn all_keys(store: &S3RemoteStorage) -> Vec<String> {
        let mut keys: Vec<String> = S3RemoteStorage::block_os(store.ops.list(None))
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        keys.sort();
        keys
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_issues_one_request_per_current_key() {
        let (store, counter) = counting_rsm();
        let md = sample_metadata(uuid::Uuid::from_u128(14));
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: md,
                ..Default::default()
            },
            move |store, md| {
                let expected = {
                    let mut keys = vec![
                        store.log_key(&md).to_string(),
                        store.index_key(&md, IndexType::Offset).to_string(),
                        store.index_key(&md, IndexType::Timestamp).to_string(),
                        store
                            .index_key(&md, IndexType::ProducerSnapshot)
                            .to_string(),
                        store.index_key(&md, IndexType::LeaderEpoch).to_string(),
                        store.index_key(&md, IndexType::Transaction).to_string(),
                    ];
                    keys.sort();
                    keys
                };
                check!(all_keys(&store) == expected);

                let before = counter.attempts(StoreOp::Delete);
                store.delete_log_segment_data(&md).unwrap();

                // Six requests, one per artifact: no second layout to sweep.
                check!(counter.attempts(StoreOp::Delete) - before == 6);
                check!(all_keys(&store) == Vec::<String>::new());
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_is_idempotent() {
        let store = rsm(None);
        let md = sample_metadata(uuid::Uuid::from_u128(13));
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: md,
                ..Default::default()
            },
            move |store, md| {
                store.delete_log_segment_data(&md).unwrap();
                store.delete_log_segment_data(&md).unwrap();
                assert!(matches!(
                    store.fetch_log_segment(&md, 0, None).unwrap_err(),
                    RemoteStorageError::SegmentNotFound(_)
                ));
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_are_isolated_by_id() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let a = sample_metadata(uuid::Uuid::from_u128(20));
        let b = sample_metadata(uuid::Uuid::from_u128(21));
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: a,
                transaction_index: crate::test_support::TransactionIndex::Omitted,
            },
            move |store, a| {
                store
                    .copy_log_segment_data(
                        &b,
                        &sample_data(src.path(), crate::test_support::TransactionIndex::Omitted),
                    )
                    .unwrap();
                store.delete_log_segment_data(&a).unwrap();
                assert!(store.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_delete_is_refused() {
        let (_keys, store) = crate::s3::test_support::memory_worm_archive();
        let md = stamped_metadata(crate::s3::test_support::StampedMetadataSetup {
            segment_id: uuid::Uuid::from_u128(55),
            ..Default::default()
        });
        seeded_blocking(
            store,
            crate::s3::test_support::SeededCopySetup {
                metadata: md,
                ..Default::default()
            },
            move |store, md| {
                let before = all_keys(&store);

                assert!(let Err(err) = store.delete_log_segment_data(&md));
                check!(
                    matches!(&err, RemoteStorageError::Worm(WormError::DeleteRefused { key })
                    if *key == store.log_key(&md).to_string())
                );

                // Not one object left the archive.
                check!(all_keys(&store) == before);
                check!(before.len() == 7, "six objects plus the manifest");
            },
        )
        .await;
    }
}
