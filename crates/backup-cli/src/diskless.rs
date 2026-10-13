//! Committed diskless-WAL index capture.

use {
    krabka_remote_storage::diskless::{DisklessWalCapture, REPLAY_FENCE_KEY, WalCaptureProjection},
    krabka_remote_storage_topic::{MetadataEventLog, MetadataLogError},
    std::{collections::HashMap, sync::Arc, time::Duration},
    uuid::Uuid,
};

/// How long the whole index read may take before the capture gives up.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Replay the committed keyed index state up to each partition's high
/// watermark and freeze it.
///
/// The capture only reads. Each partition's cutoff is its high watermark
/// (`ListOffsets` `LATEST`) when the capture starts, and the capture reads
/// from the partition's low watermark up to that cutoff. It applies each
/// record as it arrives rather than buffer the range, so an index larger than
/// memory still captures. An empty partition is complete at once. Replay
/// fences that brokers publish into the index are not index state, so the
/// capture skips them. Kafka refuses a `Produce` to an internal topic from any
/// client but `__admin_client`, so a capture that wrote its own fence would
/// fail against a Kafka-compatible broker.
///
/// # Errors
/// Returns an error when the index watermarks cannot be read, when the read
/// fails, when the watermark lookups and the read together take longer than
/// 30 seconds, or when a record cannot be decoded or projected.
pub async fn capture_projection<S: std::hash::BuildHasher>(
    log: Arc<dyn MetadataEventLog>,
    topics: &HashMap<Uuid, (String, i32), S>,
    captured_at_ms: u64,
) -> Result<DisklessWalCapture, String> {
    capture_projection_within(log.as_ref(), topics, captured_at_ms, CAPTURE_TIMEOUT).await
}

/// [`capture_projection`] with the deadline as a parameter.
async fn capture_projection_within<S: std::hash::BuildHasher>(
    log: &dyn MetadataEventLog,
    topics: &HashMap<Uuid, (String, i32), S>,
    captured_at_ms: u64,
    timeout: Duration,
) -> Result<DisklessWalCapture, String> {
    let CommittedIndex {
        projection,
        cutoffs,
    } = tokio::time::timeout(timeout, project_committed_index(log))
        .await
        .map_err(|_| "diskless WAL index capture timed out".to_owned())??;
    projection.capture(topics, cutoffs, captured_at_ms)
}

/// The projection of the committed index, with the high watermarks it was read up to.
struct CommittedIndex {
    projection: WalCaptureProjection,
    cutoffs: Vec<i64>,
}

/// Read the watermarks, then project every partition from its low watermark
/// up to its high watermark. Returns the projection and the high watermarks.
async fn project_committed_index(log: &dyn MetadataEventLog) -> Result<CommittedIndex, String> {
    let cutoffs = log
        .high_water_marks()
        .await
        .map_err(|error| error.to_string())?;
    let starts = log
        .low_water_marks()
        .await
        .map_err(|error| error.to_string())?;
    let partition_count = usize::try_from(log.partition_count())
        .map_err(|_| "negative diskless WAL index partition count".to_owned())?;
    if cutoffs.len() != partition_count || starts.len() != partition_count {
        return Err(format!(
            "diskless WAL index has {partition_count} partitions but reported {} high and {} low \
             watermarks",
            cutoffs.len(),
            starts.len()
        ));
    }
    let mut projection = WalCaptureProjection::default();
    for (partition, (&start, &cutoff)) in starts.iter().zip(&cutoffs).enumerate() {
        if start >= cutoff {
            continue;
        }
        let partition = i32::try_from(partition)
            .map_err(|_| "diskless WAL index partition overflow".to_owned())?;
        // A record the projection rejects stops the read. The rejection, not
        // the read error that carries it out, is what the capture reports.
        let mut rejected = None;
        let read = log
            .visit_range(partition, start, cutoff, &mut |record| {
                if record.key.as_deref() == Some(REPLAY_FENCE_KEY) {
                    return Ok(());
                }
                projection
                    .apply(
                        record.key.as_deref(),
                        (!record.tombstone).then_some(record.payload.as_ref()),
                    )
                    .map_err(|error| {
                        let stop = MetadataLogError::Other(error.clone());
                        rejected = Some((record.offset, error));
                        stop
                    })
            })
            .await;
        if let Some((offset, error)) = rejected {
            return Err(format!(
                "diskless WAL index partition {partition} offset {offset}: {error}"
            ));
        }
        read.map_err(|error| error.to_string())?;
    }
    Ok(CommittedIndex {
        projection,
        cutoffs,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        async_trait::async_trait,
        bytes::Bytes,
        krabka_remote_storage::diskless::{
            CapturedWalRange, DisklessPartitionCapture, WalFlushRecord, WalIndexEntry,
        },
        krabka_remote_storage_topic::{
            InProcessMetadataEventLog, MetadataEventRecord, RangeVisitor,
        },
        std::sync::Mutex,
    };

    const TOPIC_ID: Uuid = Uuid::from_u128(9);

    /// How [`ScriptedLog`] departs from the log it wraps.
    #[derive(Clone, Copy, Default)]
    enum Fault {
        /// Behave like the wrapped log.
        #[default]
        None,
        /// Never answer the high-watermark lookup.
        HangOnHighWatermarks,
        /// Report one high watermark too few.
        ShortHighWatermarks,
        /// Report each high watermark one past the last record, so the range
        /// read stops short of it.
        HighWatermarksPastTheEnd,
    }

    /// The position of one record a range read handed over.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct VisitedRecord {
        partition: i32,
        offset: i64,
    }

    /// An in-process index that records what each range read handed over,
    /// and can misbehave on its watermark lookups.
    struct ScriptedLog {
        inner: Arc<InProcessMetadataEventLog>,
        fault: Fault,
        /// Each visited record, in visit order.
        visited: Mutex<Vec<VisitedRecord>>,
    }

    impl ScriptedLog {
        fn new(inner: Arc<InProcessMetadataEventLog>, fault: Fault) -> Self {
            Self {
                inner,
                fault,
                visited: Mutex::new(Vec::new()),
            }
        }
    }

    #[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic)]
    #[async_trait]
    impl MetadataEventLog for ScriptedLog {
        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
            let mut marks = self.inner.high_water_marks().await?;
            match self.fault {
                Fault::None => {}
                Fault::HangOnHighWatermarks => std::future::pending::<()>().await,
                Fault::ShortHighWatermarks => {
                    marks.pop();
                }
                Fault::HighWatermarksPastTheEnd => {
                    for mark in &mut marks {
                        *mark += 1;
                    }
                }
            }
            Ok(marks)
        }

        async fn visit_range(
            &self,
            partition: i32,
            start: i64,
            end: i64,
            visit: &mut RangeVisitor<'_>,
        ) -> Result<(), MetadataLogError> {
            self.inner
                .visit_range(partition, start, end, &mut |record: MetadataEventRecord| {
                    self.visited.lock().unwrap().push(VisitedRecord {
                        partition: record.partition,
                        offset: record.offset,
                    });
                    visit(record)
                })
                .await
        }
    }

    fn entry(first_offset: i64, last_offset: i64) -> WalIndexEntry {
        WalIndexEntry {
            topic_id: TOPIC_ID,
            partition: 0,
            first_offset,
            last_offset,
            byte_start: 0,
            byte_len: 10,
            max_timestamp_ms: 1,
        }
    }

    /// Publish the keyed index record a broker flush publishes for `entry`.
    async fn publish_flush(
        log: &InProcessMetadataEventLog,
        object_key: &str,
        entry: WalIndexEntry,
    ) {
        let (key, value) = WalFlushRecord::keyed_entry(object_key, entry).unwrap();
        log.publish_keyed(0, key, Some(value)).await.unwrap();
    }

    krabka_macros::wal_capture_topic_fixture!(orders, TOPIC_ID, "orders", 1);

    fn visited_offsets(log: &ScriptedLog) -> Vec<VisitedRecord> {
        log.visited.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn capture_skips_replay_fences_and_projects_the_rest() {
        let index = InProcessMetadataEventLog::new(1);
        index
            .publish_keyed(0, Bytes::from_static(REPLAY_FENCE_KEY), Some(Bytes::new()))
            .await
            .unwrap();
        publish_flush(&index, "diskless-wal/1/a.ckwl", entry(0, 3)).await;
        let log = ScriptedLog::new(index, Fault::None);

        let capture = capture_projection_within(&log, &orders(), 5, CAPTURE_TIMEOUT)
            .await
            .unwrap();

        assert!(
            capture
                == DisklessWalCapture {
                    format_version: DisklessWalCapture::FORMAT_VERSION,
                    captured_at_ms: 5,
                    source_cutoffs: vec![2],
                    partitions: vec![DisklessPartitionCapture {
                        topic: "orders".to_owned(),
                        topic_id: TOPIC_ID,
                        partition: 0,
                        delete_floor: 0,
                        recovery_cutoff: 4,
                        ranges: vec![CapturedWalRange {
                            object_key: "diskless-wal/1/a.ckwl".to_owned(),
                            entry: entry(0, 3),
                        }],
                    }],
                    metadata_snapshot_sha256: None,
                    rlmm_snapshot_sha256: None,
                    group_offsets_sha256: None,
                    authentication: None,
                }
        );
        assert!(
            visited_offsets(&log)
                == vec![
                    VisitedRecord {
                        partition: 0,
                        offset: 0
                    },
                    VisitedRecord {
                        partition: 0,
                        offset: 1
                    }
                ]
        );
    }

    #[tokio::test]
    async fn a_record_that_does_not_project_stops_the_read_where_it_is() {
        let index = InProcessMetadataEventLog::new(1);
        publish_flush(&index, "diskless-wal/1/a.ckwl", entry(0, 3)).await;
        index
            .publish_keyed(
                0,
                Bytes::from_static(b"not an index key"),
                Some(Bytes::new()),
            )
            .await
            .unwrap();
        publish_flush(&index, "diskless-wal/1/b.ckwl", entry(4, 7)).await;
        let log = ScriptedLog::new(index, Fault::None);

        let error = capture_projection_within(&log, &orders(), 5, CAPTURE_TIMEOUT)
            .await
            .unwrap_err();

        check!(
            error
                == "diskless WAL index partition 0 offset 1: unknown diskless WAL index key version \
                    28271"
        );
        check!(
            visited_offsets(&log)
                == vec![
                    VisitedRecord {
                        partition: 0,
                        offset: 0
                    },
                    VisitedRecord {
                        partition: 0,
                        offset: 1
                    }
                ]
        );
    }

    #[tokio::test]
    async fn a_read_that_stops_short_of_the_cutoff_fails_the_capture() {
        let index = InProcessMetadataEventLog::new(1);
        publish_flush(&index, "diskless-wal/1/a.ckwl", entry(0, 3)).await;
        let log = ScriptedLog::new(index, Fault::HighWatermarksPastTheEnd);

        let error = capture_projection_within(&log, &orders(), 5, CAPTURE_TIMEOUT)
            .await
            .unwrap_err();

        assert!(error == "metadata log error: partition 0 ends at 1, before 2");
    }

    #[tokio::test]
    async fn watermarks_that_do_not_cover_every_partition_fail_the_capture() {
        let log = ScriptedLog::new(
            InProcessMetadataEventLog::new(2),
            Fault::ShortHighWatermarks,
        );

        let error = capture_projection_within(&log, &orders(), 5, CAPTURE_TIMEOUT)
            .await
            .unwrap_err();

        assert!(
            error == "diskless WAL index has 2 partitions but reported 1 high and 2 low watermarks"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_covers_the_watermark_lookups() {
        let log = ScriptedLog::new(
            InProcessMetadataEventLog::new(1),
            Fault::HangOnHighWatermarks,
        );

        let error = capture_projection_within(&log, &orders(), 5, CAPTURE_TIMEOUT)
            .await
            .unwrap_err();

        assert!(error == "diskless WAL index capture timed out");
    }
}
