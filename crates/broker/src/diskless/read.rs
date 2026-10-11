//! Diskless WAL cold-read path from flushed object-store runs.

use std::sync::Arc;

use bytes::Bytes;
use krabka_protocol::records::{RecordBatch, RecordsPayload};
use krabka_verified::{DisklessBatchStep, diskless_batch_step};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::wal_index::WalIndexCache;
#[cfg(test)]
use crate::config::BrokerConfig;
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::fetch::{LiveOffsets, PendingRead},
    partition::Partition,
};

/// Shared state that serves diskless offsets. The broker trimmed these offsets
/// locally, but the committed WAL object index still covers them.
pub(crate) struct DisklessReadHandle {
    pub(crate) index: Arc<AsyncMutex<WalIndexCache>>,
    store: Arc<dyn ObjectStore>,
    /// The index log the flusher currently owns, or `None` while the
    /// bootstrap is between incarnations of it.
    ///
    /// A handler that has to *write* to the index topic -- `DeleteRecords`
    /// recording its floor is the only one -- publishes through here rather
    /// than holding its own clone, because a stalled replay makes the
    /// bootstrap rebuild the log and a captured clone would go stale.
    index_log: Arc<arc_swap::ArcSwapOption<super::index_log::DisklessIndexLog>>,
}

impl DisklessReadHandle {
    #[must_use]
    pub(crate) fn new(index: Arc<AsyncMutex<WalIndexCache>>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            index,
            store,
            index_log: Arc::new(arc_swap::ArcSwapOption::empty()),
        }
    }

    /// The slot the diskless bootstrap publishes each new index log into.
    #[must_use]
    pub(crate) fn index_log_slot(
        &self,
    ) -> Arc<arc_swap::ArcSwapOption<super::index_log::DisklessIndexLog>> {
        Arc::clone(&self.index_log)
    }

    /// The index log currently serving the projection, if one is running.
    #[must_use]
    pub(crate) fn index_log(&self) -> Option<Arc<super::index_log::DisklessIndexLog>> {
        self.index_log.load_full()
    }

    /// Clone the raw object-store handle for the background WAL flusher.
    #[must_use]
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    async fn read_run(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Option<Bytes>, BrokerError> {
        if self.index_log().is_some_and(|log| !log.is_valid()) {
            return Err(BrokerError::Txn(
                "diskless WAL index projection is unavailable or invalid".into(),
            ));
        }
        let Some((object_key, byte_start, byte_len)) = self
            .index
            .lock()
            .await
            .lookup_fetch_range(topic_id, partition, offset, max_bytes)
        else {
            return Ok(None);
        };
        let range_end = byte_start
            .checked_add(byte_len)
            .ok_or_else(|| BrokerError::Txn("diskless WAL byte range overflow".into()))?;
        self.store
            .get_opts(
                &Path::from(object_key),
                GetOptions {
                    range: Some(GetRange::Bounded(byte_start..range_end)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless WAL get: {error}")))?
            .bytes()
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless WAL body: {error}")))
            .map(Some)
    }

    async fn read_records(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Option<Bytes>, BrokerError> {
        let Some(run) = self
            .read_run(topic_id, partition, offset, max_bytes)
            .await?
        else {
            return Ok(None);
        };
        let Some(records) = first_batch_bytes_at_or_after(&run, offset, max_bytes)? else {
            return Err(BrokerError::Txn(format!(
                "diskless WAL indexed range contains no batch at offset {offset}"
            )));
        };
        Ok(Some(records))
    }
}

/// Tries to satisfy a local `OFFSET_OUT_OF_RANGE` fetch from the diskless WAL
/// objects.
pub(crate) async fn try_diskless_read(
    broker: &Broker,
    p: &mut PendingRead,
    part: &Partition,
) -> Option<usize> {
    if !part.diskless || p.topic_id == krabka_protocol::primitives::uuid::Uuid::ZERO {
        return None;
    }
    let remote_storage_enable = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.config_snapshot().remote_storage_enable
    };
    if remote_storage_enable {
        return None;
    }

    let handle = broker.diskless_read.clone()?;
    let topic_id = Uuid::from_bytes(p.topic_id.0);
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);
    let records = match handle
        .read_records(topic_id, p.partition_index, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(records)) => records,
        Ok(None) => {
            broker.metrics.diskless_wal_cold_read_misses_total.inc();
            return None;
        }
        Err(error) => {
            broker.metrics.diskless_wal_cold_read_errors_total.inc();
            tracing::warn!(
                topic = %p.topic_name,
                partition = p.partition_index,
                offset = p.fetch_offset,
                %error,
                "diskless WAL cold read failed"
            );
            p.out.error_code = codes::KAFKA_STORAGE_ERROR;
            p.out.records = None;
            return Some(0);
        }
    };
    broker.metrics.diskless_wal_cold_read_hits_total.inc();
    let bytes_est = records.len();
    // The row may carry the refused out-of-range answer (all -1); a served read
    // reports the partition's live bounds, as a local read does.
    LiveOffsets::of(part).await.report(&mut p.out);
    p.out.error_code = codes::NONE;
    if p.read_committed && !p.is_follower_fetch {
        p.out.aborted_transactions = Some(Vec::new());
    }
    p.out.records = Some(RecordsPayload::Raw(records));
    Some(bytes_est)
}

fn first_batch_bytes_at_or_after(
    run: &Bytes,
    floor: i64,
    max_bytes: usize,
) -> Result<Option<Bytes>, BrokerError> {
    let mut offset = 0;
    let mut selected = None;
    while offset < run.len() {
        let slice = run.slice(offset..);
        let mut cur: &[u8] = &slice;
        let batch = RecordBatch::decode(&mut cur).map_err(|error| {
            BrokerError::Txn(format!(
                "diskless WAL indexed range contains an invalid batch: {error}"
            ))
        })?;
        let encoded_len = slice.len() - cur.len();
        match diskless_batch_step(
            selected,
            offset,
            encoded_len,
            batch.base_offset,
            batch.last_offset_delta,
            floor,
            max_bytes,
        ) {
            DisklessBatchStep::Invalid => {
                return Err(BrokerError::Txn(
                    "diskless WAL batch coordinates are invalid".into(),
                ));
            }
            DisklessBatchStep::Skip(next) | DisklessBatchStep::Continue(next) => offset = next,
            DisklessBatchStep::Start(next) => {
                selected = Some(offset);
                offset = next;
            }
            DisklessBatchStep::Stop => {
                let start = selected.expect("proved selected batch start");
                return Ok(Some(run.slice(start..offset)));
            }
        }
    }
    Ok(selected.map(|start| run.slice(start..offset)))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_compression::CompressionType;
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig};
    use krabka_protocol::{
        Decode, Encode,
        owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        primitives::uuid::Uuid as WireUuid,
        records::{Attributes, Record, RecordBatch},
    };
    use object_store::{ObjectStoreExt, PutPayload, path::Path};

    use super::*;
    use crate::{
        broker::spawn_partition,
        config::{RemoteStorageBackend, RlmmKind},
        diskless::wal_index::WalFlushRecord,
    };

    fn batch(base_offset: i64, value: &'static [u8]) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(value)),
                headers: vec![],
            }],
        }
    }

    fn encode_batches(batches: &[RecordBatch]) -> Bytes {
        let mut bytes = BytesMut::new();
        for batch in batches {
            batch.encode(&mut bytes).unwrap();
        }
        bytes.freeze()
    }

    fn round_trip_partition(topic_id: WireUuid, partition: PartitionData) -> PartitionData {
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "orders".into(),
                topic_id,
                partitions: vec![partition],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut encoded = BytesMut::new();
        response.encode(&mut encoded, 13).unwrap();
        FetchResponse::decode(&mut &encoded[..], 13)
            .unwrap()
            .responses[0]
            .partitions[0]
            .clone()
    }

    #[test]
    fn cold_read_returns_byte_exact_covering_batch() {
        let first = batch(0, b"a");
        let second = batch(1, b"b");
        let run = encode_batches(&[first.clone(), second.clone()]);
        let mut expected = BytesMut::new();
        second.encode(&mut expected).unwrap();

        let got = first_batch_bytes_at_or_after(&run, 1, usize::MAX)
            .unwrap()
            .unwrap();

        assert!(got == expected.freeze());
    }

    #[test]
    fn cold_read_miss_leaves_out_of_range() {
        let run = encode_batches(&[batch(0, b"a")]);

        assert!(
            first_batch_bytes_at_or_after(&run, 5, usize::MAX)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn mid_batch_positioning_returns_covering_batch_boundary() {
        let run = encode_batches(&[RecordBatch {
            base_offset: 10,
            last_offset_delta: 2,
            records: (0..3)
                .map(|offset_delta| Record {
                    offset_delta,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }]);

        let got = first_batch_bytes_at_or_after(&run, 11, usize::MAX)
            .unwrap()
            .unwrap();

        assert!(got == run);
    }

    #[test]
    fn max_bytes_keeps_whole_batches_and_always_returns_the_first() {
        let first = encode_batches(&[batch(0, b"a")]);
        let second = encode_batches(&[batch(1, b"b")]);
        let run = encode_batches(&[batch(0, b"a"), batch(1, b"b")]);

        assert!(first_batch_bytes_at_or_after(&run, 0, 1).unwrap().unwrap() == first);
        assert!(
            first_batch_bytes_at_or_after(&run, 0, first.len() + second.len())
                .unwrap()
                .unwrap()
                == run
        );
    }

    #[test]
    fn compressed_batches_advance_by_consumed_source_bytes() {
        let mut compressed = batch(0, b"");
        compressed.records[0].value = Some(Bytes::from(vec![0; 4096]));
        compressed.attributes = compressed
            .attributes
            .with_compression(CompressionType::Gzip);
        let first = encode_batches(&[compressed.clone()]);
        let second = encode_batches(&[batch(1, b"next")]);
        let run = encode_batches(&[compressed, batch(1, b"next")]);

        assert!(first.len() < run.len());
        assert!(
            first_batch_bytes_at_or_after(&run, 0, first.len())
                .unwrap()
                .unwrap()
                == first
        );
        assert!(
            first_batch_bytes_at_or_after(&run, 1, usize::MAX)
                .unwrap()
                .unwrap()
                == second
        );
    }

    #[test]
    fn malformed_indexed_range_is_an_error() {
        let error =
            first_batch_bytes_at_or_after(&Bytes::from_static(b"bad"), 0, usize::MAX).unwrap_err();

        assert!(error.to_string().contains("invalid batch"));
    }

    #[test]
    fn truncated_indexed_batch_is_an_error() {
        let run = encode_batches(&[batch(0, b"value")]);
        let error =
            first_batch_bytes_at_or_after(&run.slice(..run.len() - 1), 0, usize::MAX).unwrap_err();

        assert!(error.to_string().contains("invalid batch"));
    }

    #[test]
    fn logical_batch_offset_overflow_is_an_error() {
        let mut overflowing = batch(i64::MAX, b"a");
        overflowing.last_offset_delta = 1;
        let run = encode_batches(&[overflowing]);

        let error = first_batch_bytes_at_or_after(&run, i64::MAX, usize::MAX).unwrap_err();

        assert!(error.to_string().contains("coordinates are invalid"));
    }

    #[tokio::test]
    async fn indexed_object_range_read_returns_byte_exact_covering_batch() {
        let topic_id = Uuid::from_u128(7);
        let first = encode_batches(&[batch(0, b"a")]);
        let second = encode_batches(&[batch(1, b"b")]);
        let mut object = BytesMut::new();
        object.extend_from_slice(&first);
        let byte_start = u64::try_from(object.len()).unwrap();
        object.extend_from_slice(&second);

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &Path::from("diskless-wal/o"),
                PutPayload::from(object.freeze()),
            )
            .await
            .unwrap();

        let mut cache = WalIndexCache::default();
        cache.apply(&super::super::wal_index::WalFlushRecord {
            object_key: "diskless-wal/o".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![super::super::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 1,
                last_offset: 1,
                byte_start,
                byte_len: u32::try_from(second.len()).unwrap(),
                max_timestamp_ms: 0,
            }],
        });
        let handle = DisklessReadHandle::new(Arc::new(AsyncMutex::new(cache)), store);

        let got = handle
            .read_records(topic_id, 0, 1, usize::MAX)
            .await
            .unwrap()
            .unwrap();

        assert!(got == second);
    }

    #[tokio::test]
    async fn max_bytes_and_store_failure_map_the_whole_fetch_partition() {
        let dir = tempfile::tempdir().unwrap();
        let object_dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.remote_storage_backend = Some(RemoteStorageBackend::Local {
            dir: object_dir.path().to_path_buf(),
        });
        config.remote_log_metadata = RlmmKind::InMemory;
        let broker_handle = Broker::start(config).await.unwrap();
        let broker = broker_handle.broker_arc_for_test();
        let topic_id = Uuid::from_u128(9);
        let wire_topic_id = WireUuid(topic_id.into_bytes());
        let first_batch = batch(0, b"a");
        let first = encode_batches(std::slice::from_ref(&first_batch));
        let second = encode_batches(&[batch(1, b"b")]);
        let mut run = BytesMut::new();
        run.extend_from_slice(&first);
        run.extend_from_slice(&second);
        let read_handle = broker.diskless_read.as_ref().unwrap();
        read_handle
            .object_store()
            .put(
                &Path::from("diskless-wal/present"),
                PutPayload::from(run.freeze()),
            )
            .await
            .unwrap();
        read_handle
            .index
            .lock()
            .await
            .apply(&super::super::wal_index::WalFlushRecord {
                object_key: "diskless-wal/present".into(),
                format_version: WalFlushRecord::FORMAT_VERSION,
                entries: vec![
                    super::super::wal_index::WalIndexEntry {
                        topic_id,
                        partition: 0,
                        first_offset: 0,
                        last_offset: 0,
                        byte_start: 0,
                        byte_len: u32::try_from(first.len()).unwrap(),
                        max_timestamp_ms: 0,
                    },
                    super::super::wal_index::WalIndexEntry {
                        topic_id,
                        partition: 0,
                        first_offset: 1,
                        last_offset: 1,
                        byte_start: u64::try_from(first.len()).unwrap(),
                        byte_len: u32::try_from(second.len()).unwrap(),
                        max_timestamp_ms: 0,
                    },
                ],
            });
        let part_dir = dir.path().join("orders-0");
        std::fs::create_dir_all(&part_dir).unwrap();
        let part = spawn_partition(
            "orders".into(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).unwrap(),
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            true,
        );
        let mut pending = PendingRead {
            topic_name: "orders".into(),
            topic_id: wire_topic_id,
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: 1,
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: Some(part.clone()),
            out: PartitionData {
                error_code: codes::OFFSET_OUT_OF_RANGE,
                high_watermark: 7,
                log_start_offset: 3,
                ..Default::default()
            },
            cpu_micros: 0,
        };

        assert!(try_diskless_read(&broker, &mut pending, &part).await == Some(first.len()));
        assert!(broker.metrics.diskless_wal_cold_read_hits_total.get() == 1);
        assert!(
            round_trip_partition(wire_topic_id, pending.out.clone())
                == PartitionData {
                    // A served read reports the partition's live bounds, not
                    // the refused row's; this partition is at HW 0, start 0.
                    error_code: codes::NONE,
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    records: Some(first_batch.into()),
                    ..Default::default()
                }
        );

        pending.fetch_offset = 99;
        pending.out.error_code = codes::OFFSET_OUT_OF_RANGE;
        assert!(
            try_diskless_read(&broker, &mut pending, &part)
                .await
                .is_none()
        );
        assert!(broker.metrics.diskless_wal_cold_read_misses_total.get() == 1);
        pending.fetch_offset = 0;

        read_handle
            .index
            .lock()
            .await
            .apply(&super::super::wal_index::WalFlushRecord {
                object_key: "diskless-wal/missing".into(),
                format_version: WalFlushRecord::FORMAT_VERSION,
                entries: vec![super::super::wal_index::WalIndexEntry {
                    topic_id,
                    partition: 0,
                    first_offset: 0,
                    last_offset: 0,
                    byte_start: 0,
                    byte_len: 1,
                    max_timestamp_ms: 0,
                }],
            });
        pending.out = PartitionData {
            error_code: codes::OFFSET_OUT_OF_RANGE,
            high_watermark: 7,
            log_start_offset: 3,
            ..Default::default()
        };
        assert!(try_diskless_read(&broker, &mut pending, &part).await == Some(0));
        assert!(broker.metrics.diskless_wal_cold_read_errors_total.get() == 1);
        assert!(
            round_trip_partition(wire_topic_id, pending.out)
                == PartitionData {
                    error_code: codes::KAFKA_STORAGE_ERROR,
                    high_watermark: 7,
                    log_start_offset: 3,
                    ..Default::default()
                }
        );
        broker_handle.shutdown().await;
    }
}
