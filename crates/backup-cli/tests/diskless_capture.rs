use {
    assert2::assert,
    async_trait::async_trait,
    bytes::Bytes,
    krabka_backup::diskless::capture_projection,
    krabka_remote_storage::diskless::{
        CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture, REPLAY_FENCE_KEY,
        WalFlushRecord, WalIndexEntry,
    },
    krabka_remote_storage_topic::{
        AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventStream,
        MetadataLogError, PartitionStart,
    },
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    uuid::Uuid,
};

const TOPIC_ID: Uuid = Uuid::from_u128(9);

/// A diskless WAL index as a Kafka-compatible broker serves it to a backup
/// client: reads work, and every `Produce` to the internal topic fails with
/// `INVALID_TOPIC_EXCEPTION`, because the client is not `__admin_client`.
///
/// Reading the high watermarks also lands one record just past them, the way
/// a broker flush that commits while the capture runs does.
struct InternalTopicLog {
    inner: Arc<InProcessMetadataEventLog>,
    publishes: AtomicUsize,
    subscriptions: AtomicUsize,
    after_cutoff: Mutex<Option<(i32, Bytes, Bytes)>>,
}

impl InternalTopicLog {
    fn new(
        inner: Arc<InProcessMetadataEventLog>,
        after_cutoff: Option<(i32, Bytes, Bytes)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            publishes: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
            after_cutoff: Mutex::new(after_cutoff),
        })
    }

    fn refuse(&self) -> MetadataLogError {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        MetadataLogError::Publish("broker error_code 17".into())
    }
}

#[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic, range)]
#[async_trait]
impl MetadataEventLog for InternalTopicLog {
    fn partition_count(&self) -> i32 { self.inner.partition_count() }

    async fn publish(&self, _partition: i32, _event: Bytes) -> Result<i64, MetadataLogError> {
        Err(self.refuse())
    }

    async fn publish_keyed(
        &self,
        _partition: i32,
        _key: Bytes,
        _event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        Err(self.refuse())
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        self.inner.subscribe(assignment)
    }

    async fn low_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        self.inner.low_water_marks().await
    }

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let cutoffs = self.inner.high_water_marks().await?;
        let late = self.after_cutoff.lock().unwrap().take();
        if let Some((partition, key, value)) = late {
            self.inner
                .publish_keyed(partition, key, Some(value))
                .await?;
        }
        Ok(cutoffs)
    }
}

fn entry(first_offset: i64, last_offset: i64, byte_start: u64) -> WalIndexEntry {
    WalIndexEntry {
        topic_id: TOPIC_ID,
        partition: 0,
        first_offset,
        last_offset,
        byte_start,
        byte_len: 10,
        max_timestamp_ms: 1,
    }
}

/// The keyed index record a broker flush publishes for `entry`.
fn flush(object_key: &str, entry: WalIndexEntry) -> (Bytes, Bytes) {
    let (key, value) = WalFlushRecord::keyed_entry(object_key, entry).unwrap();
    (key, value)
}

krabka_macros::wal_capture_topic_fixture!(orders, TOPIC_ID, "orders", 1);

fn expected_capture(
    captured_at_ms: u64,
    source_cutoffs: Vec<i64>,
    partition: DisklessPartitionCapture,
) -> DisklessWalCapture {
    DisklessWalCapture {
        format_version: DisklessWalCapture::FORMAT_VERSION,
        captured_at_ms,
        source_cutoffs,
        partitions: vec![partition],
        metadata_snapshot_sha256: None,
        rlmm_snapshot_sha256: None,
        group_offsets_sha256: None,
        authentication: None,
    }
}

#[tokio::test]
async fn capture_reads_committed_index_state_to_the_high_watermarks_without_producing() {
    let index = InProcessMetadataEventLog::new(3);
    // p0: a live range, then a broker's replay fence.
    let (live_key, live_value) = flush("diskless-wal/1/a.ckwl", entry(4, 7, 6));
    index
        .publish_keyed(0, live_key, Some(live_value))
        .await
        .unwrap();
    index
        .publish_keyed(0, Bytes::from_static(REPLAY_FENCE_KEY), Some(Bytes::new()))
        .await
        .unwrap();
    // p1: a range that a later tombstone deleted.
    let (deleted_key, deleted_value) = flush("diskless-wal/1/b.ckwl", entry(8, 9, 0));
    index
        .publish_keyed(1, deleted_key.clone(), Some(deleted_value))
        .await
        .unwrap();
    index.publish_keyed(1, deleted_key, None).await.unwrap();
    // p2 stays empty. A flush that commits on p0 after the capture reads the
    // watermarks lies past its cutoff.
    let late = flush("diskless-wal/1/c.ckwl", entry(10, 11, 0));
    let log = InternalTopicLog::new(index, Some((0, late.0, late.1)));

    let capture = capture_projection(log.clone(), &orders(), 42)
        .await
        .unwrap();

    assert!(
        capture
            == expected_capture(42, vec![2, 2, 0], DisklessPartitionCapture {
                topic: "orders".to_owned(),
                topic_id: TOPIC_ID,
                partition: 0,
                delete_floor: 0,
                recovery_cutoff: 8,
                ranges: vec![CapturedWalRange {
                    object_key: "diskless-wal/1/a.ckwl".to_owned(),
                    entry: entry(4, 7, 6),
                }],
            })
    );
    assert!(log.publishes.load(Ordering::SeqCst) == 0);
    assert!(log.subscriptions.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn capture_of_an_empty_index_completes_at_zero_cutoffs() {
    let log = InternalTopicLog::new(InProcessMetadataEventLog::new(2), None);

    let capture = capture_projection(log.clone(), &orders(), 7).await.unwrap();

    assert!(
        capture
            == expected_capture(7, vec![0, 0], DisklessPartitionCapture {
                topic: "orders".to_owned(),
                topic_id: TOPIC_ID,
                partition: 0,
                delete_floor: 0,
                recovery_cutoff: 0,
                ranges: Vec::new(),
            })
    );
    assert!(log.publishes.load(Ordering::SeqCst) == 0);
}
