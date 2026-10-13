//! Scheduled delivery over archived batches, using stored activation headers.

use {
    super::RemoteReader,
    crate::blocking::spawn_blocking,
    krabka_ids::Offset,
    krabka_log::{DeliveryPolicy, batch_is_deliverable},
    krabka_protocol::records::{HEADER_LEN, RecordBatchHeader},
    krabka_remote_storage::{
        RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager,
        TopicIdPartition,
    },
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
    zerocopy::FromBytes as _,
};

impl RemoteReader {
    /// The first pending archived offset below `local_start`, or `None` when
    /// the complete archive band is due and adds no cap to the local frontier.
    /// The minimum is an established deletion floor, never an inferred floor.
    /// Incomplete coverage remains an error rather than evidence of activation.
    pub(crate) async fn delivery_watermark(
        &self,
        tp: &TopicIdPartition,
        minimum: Offset,
        local_start: Offset,
        uncertainty: Time,
        now_ms: i64,
    ) -> Result<Option<Offset>, RemoteStorageError> {
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let mut segments = self.list_remote_log_segments_blocking(tp).await?;
        segments.retain(|metadata| {
            metadata.state() == RemoteLogSegmentState::CopySegmentFinished
                && metadata.end_offset() >= minimum.0
                && metadata.start_offset() < local_start.0
        });
        segments.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        let mut covered_through = minimum.0;
        for metadata in segments {
            if metadata.start_offset() > covered_through {
                return Err(invalid_header());
            }
            let end = metadata
                .end_offset()
                .checked_add(1)
                .ok_or_else(invalid_header)?;
            let rsm = Arc::clone(&self.rsm);
            let cancellation = cancellation.clone();
            let frontier = spawn_blocking(move || {
                segment_activation_frontier(
                    rsm.as_ref(),
                    &metadata,
                    minimum,
                    uncertainty,
                    now_ms,
                    Some(&cancellation),
                )
            })
            .await
            .map_err(|_| invalid_header())??;
            if frontier.0 < end {
                return Ok((frontier < local_start).then_some(frontier));
            }
            covered_through = covered_through.max(end);
        }
        if covered_through < local_start.0 {
            return Err(invalid_header());
        }
        Ok(None)
    }
}

fn invalid_header() -> RemoteStorageError {
    RemoteStorageError::Backend("incomplete archived activation scan".into())
}

pub(crate) fn segment_activation_frontier(
    rsm: &dyn RemoteStorageManager,
    metadata: &RemoteLogSegmentMetadata,
    minimum: Offset,
    uncertainty: Time,
    now_ms: i64,
    cancellation: Option<&CancellationToken>,
) -> Result<Offset, RemoteStorageError> {
    let size = u32::try_from(metadata.segment_size_in_bytes()).map_err(|_| invalid_header())?;
    let header_size = u32::try_from(HEADER_LEN).map_err(|_| invalid_header())?;
    let mut position = 0_u32;
    let mut last_offset = None;
    while position < size {
        // Dropping the async caller (including a ListOffsets timeout) stops
        // further GETs after the one synchronous backend call already in flight.
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(RemoteStorageError::Backend(
                "archived activation scan cancelled".into(),
            ));
        }
        let end = position
            .checked_add(header_size)
            .filter(|end| *end <= size)
            .ok_or_else(invalid_header)?;
        let bytes = rsm.fetch_log_segment(metadata, position, Some(end - 1))?;
        let header = RecordBatchHeader::ref_from_bytes(&bytes).map_err(|_| invalid_header())?;
        let length = u32::try_from(header.batch_length.get())
            .ok()
            .and_then(|length| length.checked_add(12))
            .filter(|length| *length >= header_size)
            .ok_or_else(invalid_header)?;
        let next = position
            .checked_add(length)
            .filter(|next| *next <= size)
            .ok_or_else(invalid_header)?;
        let base = header.base_offset.get();
        let delta = header.last_offset_delta.get();
        let last = base
            .checked_add(i64::from(delta))
            .ok_or_else(invalid_header)?;
        if header.magic != 2
            || delta < 0
            || base < metadata.start_offset()
            || last > metadata.end_offset()
            || last_offset.is_some_and(|previous| base <= previous)
        {
            return Err(invalid_header());
        }
        if last >= minimum.0
            && !batch_is_deliverable(
                DeliveryPolicy::Scheduled,
                uncertainty.millis_i64_trunc(),
                header.max_timestamp.get(),
                now_ms,
            )
        {
            return Ok(Offset(base).max(minimum));
        }
        last_offset = Some(last);
        position = next;
    }
    if last_offset != Some(metadata.end_offset()) {
        return Err(invalid_header());
    }
    metadata
        .end_offset()
        .checked_add(1)
        .map(Offset)
        .ok_or_else(invalid_header)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_reader::test_support::{
            ReaderWithDir, empty_reader, sparse_remote_segment_reader, tp,
        },
        assert2::assert,
        krabka_ids::LeaderEpoch,
        krabka_remote_storage::{
            LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
            RemoteLogSegmentMetadataUpdate,
        },
        krabka_units::millis,
    };

    krabka_macros::timestamped_batch!(batch_at);

    fn add_finished_archive(
        reader: &RemoteReader,
        start: i64,
        end: i64,
        activation_ms: impl Fn(i64) -> i64,
    ) {
        let source = tempfile::tempdir().unwrap();
        let mut bytes = bytes::BytesMut::new();
        let mut max_timestamp = i64::MIN;
        for offset in start..=end {
            let timestamp = activation_ms(offset);
            max_timestamp = max_timestamp.max(timestamp);
            batch_at(offset, &[timestamp], b'x')
                .encode(&mut bytes)
                .unwrap();
        }
        let log_path = source.path().join("segment.log");
        let index_path = source.path().join("empty.index");
        std::fs::write(&log_path, &bytes).unwrap();
        std::fs::write(&index_path, []).unwrap();
        let id = RemoteLogSegmentId::new(tp(), uuid::Uuid::new_v4());
        let metadata = RemoteLogSegmentMetadata::new(
            id.clone(),
            start,
            end,
            max_timestamp,
            1,
            1_000,
            RemoteLogSegmentDetails::new(
                i32::try_from(bytes.len()).unwrap(),
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! { LeaderEpoch(0) => start },
            ),
        )
        .unwrap();
        reader
            .rlmm
            .add_remote_log_segment_metadata(metadata.clone())
            .unwrap();
        reader
            .rsm
            .copy_log_segment_data(&metadata, &LogSegmentData {
                log_segment: log_path,
                offset_index: index_path.clone(),
                time_index: index_path,
                transaction_index: None,
                producer_snapshot_index: None,
                leader_epoch_index: bytes::Bytes::new(),
            })
            .unwrap();
        reader
            .rlmm
            .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id,
                event_timestamp_ms: 1_000,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn scheduled_archive_requires_coverage_from_the_established_floor() {
        let ReaderWithDir {
            reader,
            dir: _remote,
        } = sparse_remote_segment_reader();
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(0), Offset(17), millis(100), 3_000)
                .await
                .is_err()
        );
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(10), Offset(17), millis(100), 1_799)
                .await
                .unwrap()
                == Some(Offset(10))
        );
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(10), Offset(17), millis(100), 1_800)
                .await
                .unwrap()
                == Some(Offset(14))
        );
        // An established partial-batch deletion floor clamps the waiting batch.
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(15), Offset(17), millis(100), 1_800)
                .await
                .unwrap()
                == Some(Offset(15))
        );
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(15), Offset(17), millis(100), 2_500)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn scheduled_archive_keeps_coverage_across_nested_finished_copies() {
        let (_remote, reader) = empty_reader();
        for (start, end) in [(0, 9), (5, 5), (10, 14)] {
            add_finished_archive(&reader, start, end, |_| 1_000);
        }
        assert!(
            reader
                .delivery_watermark(&tp(), Offset(0), Offset(15), millis(0), 1_000)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn scheduled_archive_adds_no_cap_for_pending_records_at_or_above_the_local_floor() {
        for pending_from in [4, 5] {
            let (_remote, reader) = empty_reader();
            add_finished_archive(&reader, 0, 5, |offset| {
                if offset >= pending_from { 2_000 } else { 1_000 }
            });
            // The copy spans 0..=5, but offsets from 4 onward remain local.
            // Every archived-only offset is due; the local watermark owns
            // the pending batch even though it is present in this copy too.
            assert!(
                reader
                    .delivery_watermark(&tp(), Offset(0), Offset(4), millis(100), 1_100)
                    .await
                    .unwrap()
                    .is_none()
            );
            // Extending the archived-only band includes the waiting batch,
            // establishing that the scanner really found its stored header.
            assert!(
                reader
                    .delivery_watermark(&tp(), Offset(0), Offset(6), millis(100), 1_100)
                    .await
                    .unwrap()
                    == Some(Offset(pending_from))
            );
        }
    }

    /// Holds a fetch until the test releases it.
    struct ReleaseGate {
        released: std::sync::Mutex<bool>,
        wake: std::sync::Condvar,
    }

    struct PausingRsm {
        inner: Arc<dyn RemoteStorageManager>,
        entered: Arc<tokio::sync::Notify>,
        finished: Arc<tokio::sync::Notify>,
        release: Arc<ReleaseGate>,
        fetches: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for PausingRsm {
        fn drop(&mut self) { self.finished.notify_one(); }
    }

    impl RemoteStorageManager for PausingRsm {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            data: &LogSegmentData,
        ) -> Result<Option<krabka_remote_storage::CustomMetadata>, RemoteStorageError> {
            self.inner.copy_log_segment_data(metadata, data)
        }

        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            start: u32,
            end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            let mut released = self.release.released.lock().unwrap();
            while !*released {
                released = self.release.wake.wait(released).unwrap();
            }
            self.inner.fetch_log_segment(metadata, start, end)
        }

        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            kind: krabka_remote_storage::IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            self.inner.fetch_index(metadata, kind)
        }

        fn delete_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            self.inner.delete_log_segment_data(metadata)
        }
    }

    #[tokio::test]
    async fn scheduled_archive_timeout_stops_after_the_header_fetch_already_in_flight() {
        let ReaderWithDir {
            reader: source,
            dir: _remote,
        } = sparse_remote_segment_reader();
        let entered = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(ReleaseGate {
            released: std::sync::Mutex::new(false),
            wake: std::sync::Condvar::new(),
        });
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let rsm = Arc::new(PausingRsm {
            inner: Arc::clone(&source.rsm),
            entered: Arc::clone(&entered),
            finished: Arc::clone(&finished),
            release: Arc::clone(&release),
            fetches: Arc::clone(&fetches),
        });
        let reader = RemoteReader::new(rsm, Arc::clone(&source.rlmm));
        let topic = tp();
        let mut scan =
            Box::pin(reader.delivery_watermark(&topic, Offset(10), Offset(17), millis(0), 3_000));
        tokio::select! {
            () = entered.notified() => {},
            result = &mut scan => panic!("scan returned before the backend was released: {result:?}"),
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), scan)
                .await
                .is_err()
        );
        drop(reader);
        *release.released.lock().unwrap() = true;
        release.wake.notify_all();
        // The wrapper drops only after the detached blocking scan releases its
        // last RSM reference, so the count is observed after that scan stops.
        tokio::time::timeout(std::time::Duration::from_secs(3), finished.notified())
            .await
            .unwrap();
        assert!(fetches.load(std::sync::atomic::Ordering::SeqCst) == 1);
    }
}
