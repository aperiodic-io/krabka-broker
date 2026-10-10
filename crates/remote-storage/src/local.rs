//! [`LocalTieredStorage`] is a filesystem-backed reference
//! [`RemoteStorageManager`]. It mirrors Kafka's test fixture of the same
//! name. It uses the same partition directories and segment filenames as
//! Kafka 4.0, so a JVM `LocalTieredStorage` can read files copied by Krabka.
//! It is useful for tests and single-node setups. Production deployments use
//! an object-store-backed implementation behind the same trait.

use std::{fs, path::PathBuf};

use crate::{
    error::RemoteStorageError,
    metadata::RemoteLogSegmentMetadata,
    storage_manager::{
        ArtifactBody, IndexType, RemoteStorageManager, remote_operation, segment_suffixes,
    },
};

/// A [`RemoteStorageManager`] that keeps offloaded segments on a local
/// filesystem under `root`.
///
/// On-disk layout, per segment:
///
/// ```text
/// <root>/<topic>-<partition>-<topic_id_base64>/
///     <base_offset>-<segment_id_base64>.log
///     <base_offset>-<segment_id_base64>.index
///     <base_offset>-<segment_id_base64>.timeindex
///     <base_offset>-<segment_id_base64>.snapshot
///     <base_offset>-<segment_id_base64>.leader_epoch_checkpoint
///     <base_offset>-<segment_id_base64>.txnindex  (when present)
/// ```
#[derive(Debug, Clone)]
pub struct LocalTieredStorage {
    root: PathBuf,
}

impl LocalTieredStorage {
    /// Constructs a store rooted at `root`. The store creates the directory
    /// on the first copy.
    ///
    /// Kafka's JVM implementation appends `kafka-tiered-storage` to its
    /// configured parent directory. To share a tier with it, pass
    /// `<remote.log.storage.local.dir>/kafka-tiered-storage` here.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory that holds every remote segment for one partition.
    fn partition_dir(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.root
            .join(crate::storage_manager::partition_dir_name(metadata))
    }

    fn segment_path(&self, metadata: &RemoteLogSegmentMetadata, suffix: &str) -> PathBuf {
        self.partition_dir(metadata)
            .join(crate::storage_manager::segment_file_name(metadata, suffix))
    }

    fn log_path(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.segment_path(metadata, ".log")
    }

    fn index_path(&self, metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> PathBuf {
        self.segment_path(metadata, index_type.suffix())
    }
}

impl RemoteStorageManager for LocalTieredStorage {
    remote_operation! {
        copy(self, metadata, data) {
            let dir = self.partition_dir(metadata);
            fs::create_dir_all(&dir)?;

            for (suffix, body) in data.artifacts() {
                let path = self.segment_path(metadata, suffix);
                match body {
                    ArtifactBody::File(source) => {
                        fs::copy(source, path)?;
                    }
                    ArtifactBody::Memory(bytes) => fs::write(path, bytes)?,
                }
            }
            // A local store needs no opaque key echoed back.
            Ok(None)
        }
    }

    remote_operation! {
        fetch(self, metadata, start_position, end_position) {
            let path = self.log_path(metadata);
            if !path.exists() {
                return Err(RemoteStorageError::SegmentNotFound(
                    metadata.remote_log_segment_id().clone(),
                ));
            }
            let bytes = fs::read(&path)?;
            let len = bytes.len();
            let start = usize::try_from(start_position).expect("u32 fits usize");
            if start > len {
                return Err(RemoteStorageError::InvalidArgument(format!(
                    "start_position {start} exceeds segment length {len}"
                )));
            }
            let end_exclusive = match end_position {
                Some(end) => {
                    let end = usize::try_from(end).expect("u32 fits usize");
                    if end < start {
                        return Err(RemoteStorageError::InvalidArgument(format!(
                            "end_position {end} < start_position {start}"
                        )));
                    }
                    // `end` is inclusive; clamp to the segment length.
                    end.saturating_add(1).min(len)
                }
                None => len,
            };
            Ok(bytes[start..end_exclusive].to_vec())
        }
    }

    remote_operation! {
        index(self, metadata, index_type) {
            let path = self.index_path(metadata, index_type);
            if !path.exists() {
                return Err(RemoteStorageError::SegmentNotFound(
                    metadata.remote_log_segment_id().clone(),
                ));
            }
            Ok(fs::read(&path)?)
        }
    }

    remote_operation! {
        delete(self, metadata) {
            for path in segment_suffixes().map(|suffix| self.segment_path(metadata, suffix)) {
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(RemoteStorageError::Io(error)),
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::test_support::{sample_data, sample_metadata as metadata};

    type LocalFixture = (
        tempfile::TempDir,
        tempfile::TempDir,
        LocalTieredStorage,
        RemoteLogSegmentMetadata,
    );

    fn local_store() -> LocalFixture {
        let remote = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let store = LocalTieredStorage::new(remote.path());
        (remote, source, store, metadata(uuid::Uuid::from_u128(10)))
    }

    fn copied_store(with_transaction_index: crate::test_support::TransactionIndex) -> LocalFixture {
        let (remote, source, store, metadata) = local_store();
        store
            .copy_log_segment_data(
                &metadata,
                &sample_data(source.path(), with_transaction_index),
            )
            .unwrap();
        (remote, source, store, metadata)
    }

    #[test]
    fn copy_then_fetch_full_segment() {
        let (_remote, src, rsm, md) = local_store();
        assert!(
            rsm.copy_log_segment_data(
                &md,
                &sample_data(src.path(), crate::test_support::TransactionIndex::Present)
            )
            .unwrap()
            .is_none()
        );
        let full = rsm.fetch_log_segment(&md, 0, None).unwrap();
        assert!(full == b"0123456789");
    }

    #[test]
    fn fetch_partial_byte_ranges() {
        let (_remote, _src, rsm, md) = copied_store(crate::test_support::TransactionIndex::Omitted);
        for (start, end, want) in [
            // Inclusive [2, 5] -> "2345".
            (2, Some(5), b"2345".as_ref()),
            // Open-ended from 7 -> "789".
            (7, None, b"789".as_ref()),
            // End past EOF clamps.
            (8, Some(99), b"89".as_ref()),
            // Start at EOF -> empty.
            (10, None, b"".as_ref()),
        ] {
            check!(
                rsm.fetch_log_segment(&md, start, end).unwrap() == want,
                "range [{start}, {end:?}]"
            );
        }
    }

    #[test]
    fn fetch_single_byte_range_start_equals_end() {
        let (_remote, _src, rsm, md) = copied_store(crate::test_support::TransactionIndex::Omitted);
        // Inclusive [3, 3] is a valid single-byte range -> "3". (The guard is
        // `end < start`, not `<=`/`==`, so an equal start/end must succeed.)
        assert!(rsm.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
    }

    #[test]
    fn fetch_each_index_type() {
        let (_remote, _src, rsm, md) = copied_store(crate::test_support::TransactionIndex::Present);
        crate::test_support::check_sample_indexes(&rsm, &md);
    }

    #[test]
    fn copied_files_use_kafka_local_tiered_storage_layout() {
        let (remote, _src, _rsm, _md) =
            copied_store(crate::test_support::TransactionIndex::Present);

        let partition = remote.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        for suffix in [
            ".log",
            ".index",
            ".timeindex",
            ".snapshot",
            ".leader_epoch_checkpoint",
            ".txnindex",
        ] {
            check!(
                partition
                    .join(format!(
                        "00000000000000000000-AAAAAAAAAAAAAAAAAAAACg{suffix}"
                    ))
                    .is_file(),
                "missing Kafka layout artifact {suffix}"
            );
        }
    }

    #[test]
    fn missing_optional_txn_index_is_not_found() {
        let (_remote, _src, rsm, md) = copied_store(crate::test_support::TransactionIndex::Omitted);
        let err = rsm.fetch_index(&md, IndexType::Transaction).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn fetch_before_copy_is_not_found() {
        let remote = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(uuid::Uuid::from_u128(404));
        let err = rsm.fetch_log_segment(&md, 0, None).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn delete_is_idempotent_and_removes_data() {
        let (_remote, _src, rsm, md) = copied_store(crate::test_support::TransactionIndex::Present);
        rsm.delete_log_segment_data(&md).unwrap();
        // Second delete is a no-op.
        rsm.delete_log_segment_data(&md).unwrap();
        assert!(matches!(
            rsm.fetch_log_segment(&md, 0, None).unwrap_err(),
            RemoteStorageError::SegmentNotFound(_)
        ));
    }

    #[test]
    fn segments_are_isolated_by_id() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let a = metadata(uuid::Uuid::from_u128(10));
        let b = metadata(uuid::Uuid::from_u128(11));
        rsm.copy_log_segment_data(
            &a,
            &sample_data(src.path(), crate::test_support::TransactionIndex::Omitted),
        )
        .unwrap();
        rsm.copy_log_segment_data(
            &b,
            &sample_data(src.path(), crate::test_support::TransactionIndex::Omitted),
        )
        .unwrap();
        rsm.delete_log_segment_data(&a).unwrap();
        // Deleting `a` leaves `b` intact.
        assert!(rsm.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
    }
}
