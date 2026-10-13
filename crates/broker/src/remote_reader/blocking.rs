//! The `spawn_blocking` wrappers around the synchronous remote-storage SPIs.
//!
//! The `RemoteStorageManager` and `RemoteLogMetadataManager` traits are
//! blocking, so every segment-listing, index and log read that the reader
//! performs goes to the `tokio` blocking pool through one of these helpers,
//! by way of [`crate::blocking::spawn_blocking`].
//! Each helper also maps a panicked blocking task onto
//! `RemoteStorageError::Io`, so a defective SPI implementation surfaces as a
//! read error rather than as a lost future.

use {
    super::RemoteReader,
    crate::blocking::spawn_blocking,
    krabka_remote_storage::{
        BytePosition, IndexType, RemoteLogSegmentMetadata, RemoteStorageError, TopicIdPartition,
    },
    tracing::warn,
};

impl RemoteReader {
    pub(super) async fn list_remote_log_segments_blocking(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let rlmm = self.rlmm.clone();
        let tp = tp.clone();
        match spawn_blocking(move || rlmm.list_remote_log_segments(&tp)).await {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "remote-reader: list_remote_log_segments task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "list_remote_log_segments task panicked",
                )))
            }
        }
    }

    /// Reads one of a segment's indexes, from the bounded index cache when it
    /// is already there and from the remote tier otherwise.
    ///
    /// The cache lookup and the download both run on the blocking pool: a hit
    /// reads a local file, which is blocking work too, and running both there
    /// keeps one code path rather than two. A consumer walking a cold segment
    /// therefore downloads each index object once instead of once per `Fetch`,
    /// which is what Kafka's `RemoteIndexCache` buys `RemoteLogManager.read`.
    pub(super) async fn fetch_index_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        kind: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        let cache = self.index_cache.clone();
        let segment_id = metadata.remote_log_segment_id().id;
        match spawn_blocking(move || {
            cache.get_or_fetch(segment_id, kind, || rsm.fetch_index(&metadata, kind))
        })
        .await
        {
            Ok(res) => res.map(|(bytes, _)| bytes),
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_index task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_index task panicked",
                )))
            }
        }
    }

    pub(super) async fn fetch_log_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        start_position: BytePosition,
        end_position: Option<BytePosition>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        match spawn_blocking(move || rsm.fetch_log_segment(&metadata, start_position, end_position))
            .await
        {
            Ok(res) => res,
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_log_segment task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_log_segment task panicked",
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_reader::test_support::tp,
        assert2::assert,
        krabka_ids::LeaderEpoch,
        krabka_remote_storage::{LocalTieredStorage, RemoteLogMetadataManager},
        std::sync::Arc,
    };

    struct SlowListRlmm {
        reactor_ticked: Arc<std::sync::atomic::AtomicBool>,
        observed_tick: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RemoteLogMetadataManager for SlowListRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _metadata: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn update_remote_log_segment_metadata(
            &self,
            _update: krabka_remote_storage::RemoteLogSegmentMetadataUpdate,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn remote_log_segment_metadata(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(None)
        }

        fn highest_offset_for_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Option<i64>, RemoteStorageError> {
            Ok(None)
        }

        fn list_remote_log_segments(
            &self,
            _topic_id_partition: &TopicIdPartition,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.observed_tick.store(
                self.reactor_ticked
                    .load(std::sync::atomic::Ordering::Acquire),
                std::sync::atomic::Ordering::Release,
            );
            Ok(Vec::new())
        }

        fn list_remote_log_segments_by_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(Vec::new())
        }

        fn put_remote_partition_delete_metadata(
            &self,
            _metadata: krabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metadata_listing_does_not_block_the_reactor() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let reactor_ticked = Arc::new(AtomicBool::new(false));
        let observed_tick = Arc::new(AtomicBool::new(false));
        let tick = reactor_ticked.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            tick.store(true, Ordering::Release);
        });
        let remote_dir = tempfile::tempdir().unwrap();
        let reader = RemoteReader::new(
            Arc::new(LocalTieredStorage::new(remote_dir.path())),
            Arc::new(SlowListRlmm {
                reactor_ticked,
                observed_tick: observed_tick.clone(),
            }),
        );

        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
        assert!(
            observed_tick.load(Ordering::Acquire),
            "the current-thread reactor must run while the blocking RLMM call is in flight"
        );
    }
}
