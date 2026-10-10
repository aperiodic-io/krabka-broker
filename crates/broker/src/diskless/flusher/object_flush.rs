//! One flush of the committed tails into object storage: build a WAL object
//! from the flushable partitions, upload it, publish its `WalFlushRecord` on
//! the diskless index log, wait for the projection to catch up, and trim the
//! local logs behind the durable frontier.

use std::{sync::Arc, time::Duration};

use krabka_log::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::{FlushConfig, FlushPartition};
use crate::diskless::{
    index_log::DisklessIndexLog,
    wal_index::{WalFlushRecord, WalIndexCache, WalIndexEntry},
    wal_object::WalObjectBuilder,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush_once(
    object_store: Arc<dyn ObjectStore>,
    broker_id: i32,
    metrics: &crate::metrics::BrokerMetrics,
    index_log: &DisklessIndexLog,
    cache: Arc<AsyncMutex<WalIndexCache>>,
    partitions: &[FlushPartition],
    config: &FlushConfig,
    is_current_leader: impl Fn(&FlushPartition) -> bool,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let mut builder = WalObjectBuilder::new();
    for partition in partitions {
        if !is_current_leader(partition) {
            continue;
        }
        let remaining = config
            .max_size
            .bytes_usize()
            .saturating_sub(builder.body_len());
        if remaining == 0 {
            break;
        }
        let start = cache
            .lock()
            .await
            .flushed_frontier(partition.topic_id, partition.handle.index.get());
        let raw = {
            let log = partition
                .handle
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let start = start.unwrap_or_else(|| log.log_start_offset().0);
            metrics.record_diskless_wal_projection_lag(
                partition.topic_id,
                partition.handle.index,
                partition.high_watermark.0.saturating_sub(start),
            );
            log.read_raw(
                Offset(start),
                partition.high_watermark,
                ByteSize::from_bytes(u64::try_from(remaining).unwrap_or(u64::MAX)),
            )
            .map_err(crate::error::BrokerError::from)?
        };
        let Some(last_offset) = raw.last_offset else {
            continue;
        };
        builder.append_run(
            partition.topic_id,
            partition.handle.index.get(),
            raw.start_offset.0,
            last_offset.0,
            &raw.bytes,
        );
    }

    if builder.is_empty() {
        return Ok(None);
    }
    metrics.diskless_wal_flush_attempts_total.inc();
    let object_key = format!("diskless-wal/{broker_id}/{}.ckwl", Uuid::new_v4());
    let object = builder.finish();
    if let Err(error) = object_store
        .put(
            &Path::from(object_key.clone()),
            PutPayload::from(object.clone()),
        )
        .await
    {
        metrics.diskless_wal_flush_failures_total.inc();
        return Err(crate::error::BrokerError::Txn(format!(
            "diskless wal put: {error}"
        )));
    }
    metrics
        .diskless_wal_flush_bytes_total
        .inc_by(u64::try_from(object.len()).unwrap_or(u64::MAX));

    let entries = match batch_index_entries(&object, crate::time_util::now_ms()) {
        Ok(entries) => entries,
        Err(error) => {
            metrics.diskless_wal_flush_failures_total.inc();
            return Err(error);
        }
    };
    let record = WalFlushRecord {
        object_key,
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries,
    };
    if let Err(error) = index_log.publish_flush(&record).await {
        metrics.diskless_wal_flush_failures_total.inc();
        return Err(error);
    }
    if let Err(error) =
        wait_for_committed_projection(index_log, &record, config.index_projection_timeout).await
    {
        metrics.diskless_wal_flush_failures_total.inc();
        return Err(error);
    }

    for partition in partitions {
        if !is_current_leader(partition) {
            continue;
        }
        if let Some(frontier) = cache
            .lock()
            .await
            .flushed_frontier(partition.topic_id, partition.handle.index.get())
        {
            metrics.record_diskless_wal_projection_lag(
                partition.topic_id,
                partition.handle.index,
                partition.high_watermark.0.saturating_sub(frontier),
            );
            if let Some(lag) = config.trim_safety_lag {
                let current_start = partition
                    .handle
                    .log
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .log_start_offset()
                    .0;
                let decision = krabka_verified::diskless_trim_decision(
                    frontier,
                    partition.high_watermark.0,
                    lag,
                    current_start,
                );
                let trim_frontier = if decision.should_trim {
                    match partition
                        .handle
                        .trim_to_offset(Offset(decision.target))
                        .await
                    {
                        Ok(offset) => offset.0,
                        Err(error) => {
                            metrics.diskless_wal_flush_failures_total.inc();
                            return Err(error);
                        }
                    }
                } else {
                    current_start
                };
                metrics.record_diskless_wal_trim_frontier(
                    partition.topic_id,
                    partition.handle.index,
                    trim_frontier,
                );
            }
        }
    }

    Ok(Some(record))
}

/// One index entry per v2 batch in the object.
///
/// `flushed_at_ms` stands in for a batch that carries Kafka's "no timestamp"
/// sentinel, the way `LogSegment.largestTimestamp` falls back to the segment
/// file's modification time when its `maxTimestampSoFar` is negative. Without
/// it a producer that sends no timestamp would make `retention.ms` unable to
/// place the range at all.
fn batch_index_entries(
    object: &bytes::Bytes,
    flushed_at_ms: i64,
) -> Result<Vec<WalIndexEntry>, crate::error::BrokerError> {
    let mut entries = Vec::new();
    for run in crate::diskless::wal_object::parse_wal_object(object)
        .map_err(|error| crate::error::BrokerError::Txn(error.to_string()))?
    {
        let mut byte_start = usize::try_from(run.byte_start).map_err(|_| {
            crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
        })?;
        let run_end = byte_start
            .checked_add(usize::try_from(run.byte_len).unwrap_or(usize::MAX))
            .ok_or_else(|| {
                crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
            })?;
        while byte_start < run_end {
            let mut cursor = &object[byte_start..run_end];
            let batch = RecordBatch::decode(&mut cursor).map_err(|error| {
                crate::error::BrokerError::Txn(format!("diskless WAL batch: {error}"))
            })?;
            let byte_len = run_end - byte_start - cursor.len();
            entries.push(WalIndexEntry {
                topic_id: run.topic_id,
                partition: run.partition,
                first_offset: batch.base_offset,
                last_offset: batch
                    .base_offset
                    .checked_add(i64::from(batch.last_offset_delta))
                    .ok_or_else(|| {
                        crate::error::BrokerError::Txn("diskless WAL batch offset overflow".into())
                    })?,
                byte_start: u64::try_from(byte_start).unwrap_or(u64::MAX),
                byte_len: u32::try_from(byte_len).map_err(|_| {
                    crate::error::BrokerError::Txn("diskless WAL batch exceeds 4 GiB".into())
                })?,
                max_timestamp_ms: if batch.max_timestamp < 0 {
                    flushed_at_ms
                } else {
                    batch.max_timestamp
                },
            });
            byte_start = byte_start.checked_add(byte_len).ok_or_else(|| {
                crate::error::BrokerError::Txn("diskless WAL byte range overflow".into())
            })?;
        }
    }
    Ok(entries)
}

async fn wait_for_committed_projection(
    index_log: &DisklessIndexLog,
    record: &WalFlushRecord,
    timeout: Duration,
) -> Result<(), crate::error::BrokerError> {
    index_log
        .wait_until_applied(record, timeout)
        .await
        .then_some(())
        .ok_or_else(|| {
            crate::error::BrokerError::Txn("diskless wal index projection timed out".into())
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_protocol::records::{Record, RecordBatch};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    use super::*;
    use crate::diskless::flusher::test_support::{
        orders_partition, test_index_log, test_partition,
    };

    #[test]
    fn flush_index_has_one_range_per_batch() {
        let topic_id = Uuid::from_u128(10);
        // The second batch carries Kafka's "no timestamp" sentinel, which the
        // index has to replace with the flush time so `retention.ms` can place
        // the range at all.
        let batches = [(4, 700), (5, -1)].map(|(base_offset, max_timestamp)| RecordBatch {
            base_offset,
            max_timestamp,
            records: vec![Record {
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            }],
            ..Default::default()
        });
        let mut run = BytesMut::new();
        for batch in &batches {
            batch.encode(&mut run).unwrap();
        }
        let object = crate::diskless::wal_object::WalObjectBuilder::new()
            .finish_with_run(topic_id, 0, 4, 5, &run);

        let entries = batch_index_entries(&object, 4_242).unwrap();

        assert!(entries.len() == 2);
        assert!(entries[0].first_offset == 4 && entries[0].last_offset == 4);
        assert!(entries[1].first_offset == 5 && entries[1].last_offset == 5);
        assert!(entries[0].byte_start + u64::from(entries[0].byte_len) == entries[1].byte_start);
        assert!(entries[0].max_timestamp_ms == 700);
        assert!(entries[1].max_timestamp_ms == 4_242);
    }

    #[tokio::test]
    async fn flusher_writes_object_and_publishes_index() {
        let dir = tempdir().unwrap();
        let handle = orders_partition(dir.path());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log = krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let topic_id = Uuid::from_u128(11);
        let cache = index.cache();
        let record = flush_once(
            store.clone(),
            7,
            &crate::metrics::BrokerMetrics::new(),
            &index,
            cache.clone(),
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
                high_watermark: Offset(3),
            }],
            &FlushConfig::default(),
            |_| true,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries[0].first_offset == 0);
        assert!(record.entries[0].last_offset == 2);
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
        assert!(store.head(&Path::from(record.object_key)).await.is_ok());
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(2));
    }

    #[tokio::test]
    async fn leadership_loss_does_not_recreate_shard_metrics() {
        let dir = tempdir().unwrap();
        let handle = orders_partition(dir.path());
        let index = test_index_log().await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let topic_id = Uuid::from_u128(11);
        let checks = AtomicUsize::new(0);
        metrics.initialize_diskless_wal_flusher_metrics(
            topic_id,
            krabka_ids::PartitionIndex(0),
            0,
            0,
        );

        flush_once(
            Arc::new(InMemory::new()),
            7,
            &metrics,
            &index,
            index.cache(),
            &[FlushPartition {
                topic_id,
                handle,
                high_watermark: Offset(3),
            }],
            &FlushConfig::default(),
            |_| {
                if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                    true
                } else {
                    metrics.remove_diskless_wal_shard(topic_id, krabka_ids::PartitionIndex(0), &[]);
                    true
                }
            },
        )
        .await
        .unwrap();

        crate::metrics::test_support::render_registry!(metrics, body, registry; unwrap());
        assert!(!body.contains(&topic_id.to_string()));
    }

    #[tokio::test]
    async fn put_failure_is_exported_by_the_real_metric() {
        let dir = tempdir().unwrap();
        let store_root = dir.path().join("store");
        std::fs::create_dir(&store_root).unwrap();
        let store = object_store::local::LocalFileSystem::new_with_prefix(&store_root).unwrap();
        std::fs::remove_dir(&store_root).unwrap();
        std::fs::write(&store_root, b"not a directory").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let handle = orders_partition(dir.path());
        let index = test_index_log().await;
        let metrics = crate::metrics::BrokerMetrics::new();

        let error = flush_once(
            store,
            7,
            &metrics,
            &index,
            index.cache(),
            &[FlushPartition {
                topic_id: Uuid::from_u128(11),
                handle,
                high_watermark: Offset(3),
            }],
            &FlushConfig::default(),
            |_| true,
        )
        .await
        .expect_err("the object-store root became a file");
        assert!(error.to_string().contains("diskless wal put"));

        crate::metrics::test_support::render_registry!(metrics, body, registry; unwrap());
        assert!(body.contains("krabka_broker_diskless_wal_flush_failures_total 1"));
    }

    #[tokio::test]
    async fn flusher_skips_noop_trim_when_writer_is_stopped() {
        let dir = tempdir().unwrap();
        let handle = orders_partition(dir.path());
        let writer = handle
            .writer_handle
            .lock()
            .unwrap()
            .take()
            .expect("partition writer");
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = test_index_log().await;
        let cache = index.cache();
        let topic_id = Uuid::from_u128(11);
        let record = flush_once(
            store,
            7,
            &crate::metrics::BrokerMetrics::new(),
            &index,
            cache,
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
                high_watermark: Offset(3),
            }],
            &FlushConfig {
                trim_safety_lag: Some(3),
                ..FlushConfig::default()
            },
            |_| true,
        )
        .await
        .expect("a no-op trim must not depend on the partition writer")
        .expect("the durable prefix is flushed");

        assert!(record.entries[0].last_offset == 2);
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(0));
    }

    #[tokio::test]
    async fn committed_projection_wait_requires_the_record_to_be_applied() {
        let event_log = krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let record = WalFlushRecord {
            object_key: "diskless-wal/test.ckwl".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![WalIndexEntry {
                topic_id: Uuid::from_u128(11),
                partition: 0,
                first_offset: 0,
                last_offset: 2,
                byte_start: 0,
                byte_len: 1,
                max_timestamp_ms: 0,
            }],
        };

        let error = wait_for_committed_projection(&index, &record, Duration::from_millis(10))
            .await
            .expect_err("an unapplied record must time out");
        assert!(error.to_string().contains("projection timed out"));

        let (published, projected) = tokio::join!(
            async {
                tokio::task::yield_now().await;
                index.publish_flush(&record).await
            },
            wait_for_committed_projection(&index, &record, Duration::from_secs(1)),
        );
        published.unwrap();
        projected.expect("the exact applied record is visible");
    }

    #[tokio::test]
    async fn combined_object_stops_after_size_budget() {
        let dir = tempdir().unwrap();
        let first = orders_partition(dir.path());
        let second = test_partition(
            dir.path(),
            crate::diskless::flusher::test_support::FlusherPartitionSetup {
                partition: krabka_ids::PartitionIndex(1),
                ..Default::default()
            },
        );
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = test_index_log().await;
        let cache = index.cache();
        let config = FlushConfig {
            max_size: ByteSize::from_bytes(1),
            trim_safety_lag: None,
            ..FlushConfig::default()
        };

        let record = flush_once(
            store,
            7,
            &crate::metrics::BrokerMetrics::new(),
            &index,
            cache,
            &[
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: first,
                    high_watermark: Offset(3),
                },
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: second,
                    high_watermark: Offset(3),
                },
            ],
            &config,
            |_| true,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries.len() == 1);
        assert!(record.entries[0].partition == 0);
    }
}
