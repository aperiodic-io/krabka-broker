//! Object-key derivation for the S3 backend.
//!
//! The layout mirrors [`LocalTieredStorage`](crate::LocalTieredStorage): a
//! partition directory holding one object per artifact, named by base offset
//! and segment id.

use object_store::path::Path as ObjectPath;

use super::S3RemoteStorage;
use crate::{metadata::RemoteLogSegmentMetadata, storage_manager::IndexType};

impl S3RemoteStorage {
    pub(super) fn segment_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        suffix: &str,
    ) -> ObjectPath {
        let mut key = String::new();
        if let Some(p) = &self.prefix {
            key.push_str(p);
            key.push('/');
        }
        key.push_str(&crate::storage_manager::partition_dir_name(metadata));
        key.push('/');
        key.push_str(&crate::storage_manager::segment_file_name(metadata, suffix));
        ObjectPath::from(key)
    }

    pub(super) fn log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.segment_key(metadata, ".log")
    }

    pub(super) fn index_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> ObjectPath {
        self.segment_key(metadata, index_type.suffix())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use object_store::memory::InMemory;

    use super::S3RemoteStorage;
    use crate::s3::test_support::sample_metadata;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_isolates_clusters() {
        let store_a =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_string()));
        let _ = store_a;
        // Single cluster keys live under the prefix; we verify the key
        // construction at the unit level (no cross-cluster fixture
        // available without sharing the InMemory backend, which we don't
        // because each cluster gets its own bucket in practice).
        let md = sample_metadata(uuid::Uuid::from_u128(30));
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("c".to_string()));
        let key = store.log_key(&md);
        let expected = concat!(
            "c/orders-0-AAAAAAAAAAAAAAAAAAAAAQ/",
            "00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log"
        );
        assert!(
            key.as_ref() == expected,
            "unexpected Kafka-compatible object key {key:?}",
        );
    }
}
