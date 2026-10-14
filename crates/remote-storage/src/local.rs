//! [`LocalTieredStorage`] is a filesystem-backed reference
//! [`RemoteStorageManager`]. It mirrors Kafka's test fixture of the same
//! name. It uses the same partition directories and segment filenames as
//! Kafka 4.0, so a JVM `LocalTieredStorage` can read files copied by Krabka.
//! It is useful for tests and single-node setups. Production deployments use
//! an object-store-backed implementation behind the same trait.

use {
    crate::{
        error::RemoteStorageError,
        metadata::{CustomMetadata, RemoteLogSegmentMetadata},
        storage_manager::{
            ArtifactBody, IndexType, LogSegmentData, RemoteStorageManager, SegmentArtifact,
            partition_dir_name, segment_file_name, segment_suffixes,
        },
    },
    std::{fs, path::PathBuf},
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
    pub fn new(root: impl Into<PathBuf>) -> Self { Self { root: root.into() } }

    /// The directory that holds every remote segment for one partition.
    fn partition_dir(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.root.join(partition_dir_name(metadata))
    }

    fn segment_path(&self, metadata: &RemoteLogSegmentMetadata, suffix: &str) -> PathBuf {
        self.partition_dir(metadata)
            .join(segment_file_name(metadata, suffix))
    }

    fn log_path(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.segment_path(metadata, ".log")
    }

    fn index_path(&self, metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> PathBuf {
        self.segment_path(metadata, index_type.suffix())
    }
}

impl RemoteStorageManager for LocalTieredStorage {
    #[krabka_macros::remote_operation]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let dir = self.partition_dir(metadata);
        fs::create_dir_all(&dir)?;

        for SegmentArtifact { suffix, body } in data.artifacts() {
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

    #[krabka_macros::remote_operation]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
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

    #[krabka_macros::remote_operation]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let path = self.index_path(metadata, index_type);
        if !path.exists() {
            return Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ));
        }
        Ok(fs::read(&path)?)
    }

    #[krabka_macros::remote_operation]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{
            TransactionIndex, check_sample_indexes, sample_data, sample_metadata as metadata,
        },
        assert2::{assert, check},
    };

    /// A local tiered store, the directories around it, and one segment's metadata.
    struct LocalFixture {
        remote: tempfile::TempDir,
        source: tempfile::TempDir,
        store: LocalTieredStorage,
        metadata: RemoteLogSegmentMetadata,
    }

    fn local_store() -> LocalFixture {
        let remote = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        let store = LocalTieredStorage::new(remote.path());
        LocalFixture {
            remote,
            source,
            store,
            metadata: metadata(uuid::Uuid::from_u128(10)),
        }
    }

    fn copied_store(with_transaction_index: TransactionIndex) -> LocalFixture {
        let fixture = local_store();
        fixture
            .store
            .copy_log_segment_data(
                &fixture.metadata,
                &sample_data(fixture.source.path(), with_transaction_index),
            )
            .unwrap();
        fixture
    }

    #[test]
    fn copy_then_fetch_full_segment() {
        let LocalFixture {
            remote: _remote,
            source: src,
            store: rsm,
            metadata: md,
        } = local_store();
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

    #[yare::parameterized(
        inclusive_range = { 2, Some(5), b"2345".as_ref() },
        open_ended_range = { 7, None, b"789".as_ref() },
        end_clamps_at_eof = { 8, Some(99), b"89".as_ref() },
        start_at_eof = { 10, None, b"".as_ref() },
    )]
    fn fetch_partial_byte_ranges(start: u32, end: Option<u32>, want: &[u8]) {
        let LocalFixture {
            remote: _remote,
            source: _src,
            store: rsm,
            metadata: md,
        } = copied_store(TransactionIndex::Omitted);
        check!(
            rsm.fetch_log_segment(&md, start, end).unwrap() == want,
            "range [{start}, {end:?}]"
        );
    }

    #[test]
    fn fetch_single_byte_range_start_equals_end() {
        let LocalFixture {
            remote: _remote,
            source: _src,
            store: rsm,
            metadata: md,
        } = copied_store(TransactionIndex::Omitted);
        // Inclusive [3, 3] is a valid single-byte range -> "3". (The guard is
        // `end < start`, not `<=`/`==`, so an equal start/end must succeed.)
        assert!(rsm.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
    }

    #[test]
    fn fetch_each_index_type() {
        let LocalFixture {
            remote: _remote,
            source: _src,
            store: rsm,
            metadata: md,
        } = copied_store(TransactionIndex::Present);
        check_sample_indexes(&rsm, &md);
    }

    #[yare::parameterized(
        log = { ".log" },
        offset_index = { ".index" },
        time_index = { ".timeindex" },
        snapshot = { ".snapshot" },
        leader_epoch_checkpoint = { ".leader_epoch_checkpoint" },
        transaction_index = { ".txnindex" },
    )]
    fn copied_files_use_kafka_local_tiered_storage_layout(suffix: &str) {
        let LocalFixture { remote, .. } = copied_store(TransactionIndex::Present);

        let partition = remote.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        check!(
            partition
                .join(format!(
                    "00000000000000000000-AAAAAAAAAAAAAAAAAAAACg{suffix}"
                ))
                .is_file(),
            "missing Kafka layout artifact {suffix}"
        );
    }

    #[test]
    fn missing_optional_txn_index_is_not_found() {
        let LocalFixture {
            remote: _remote,
            source: _src,
            store: rsm,
            metadata: md,
        } = copied_store(TransactionIndex::Omitted);
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
        let LocalFixture {
            remote: _remote,
            source: _src,
            store: rsm,
            metadata: md,
        } = copied_store(TransactionIndex::Present);
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
        rsm.copy_log_segment_data(&a, &sample_data(src.path(), TransactionIndex::Omitted))
            .unwrap();
        rsm.copy_log_segment_data(&b, &sample_data(src.path(), TransactionIndex::Omitted))
            .unwrap();
        rsm.delete_log_segment_data(&a).unwrap();
        // Deleting `a` leaves `b` intact.
        assert!(rsm.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
    }
}
