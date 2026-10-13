//! Projection of committed diskless WAL index events.

use {
    super::wal_index::{
        WalDeleteFloorKey, WalDeleteFloorRecord, WalFlushRecord, WalIndexCache, WalIndexKey,
    },
    crate::{error::BrokerError, metrics::BrokerMetrics},
    bytes::Bytes,
    futures_util::StreamExt,
    krabka_remote_storage::diskless::{REPLAY_FENCE_KEY, wal_index_key_version},
    krabka_remote_storage_topic::{MetadataEventLog, PartitionStart},
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    tokio::sync::{Mutex, watch},
};

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";

/// How far the pump has walked the replay it started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayProgress {
    /// Records delivered for whichever still-unreplayed partition has seen
    /// the fewest. It only rises when the slowest partition moves, so one
    /// partition going silent freezes it however busy the rest are.
    slowest_pending: u64,
    /// Whether every partition has reached its replay target.
    caught_up: bool,
    invalid: bool,
}

#[derive(Clone)]
pub(crate) struct DisklessIndexLog {
    log: Arc<dyn MetadataEventLog>,
    cache: Arc<Mutex<WalIndexCache>>,
    progress: watch::Receiver<ReplayProgress>,
    applied: watch::Receiver<u64>,
    valid: Arc<AtomicBool>,
}

impl DisklessIndexLog {
    #[cfg(test)]
    pub(crate) async fn start(log: Arc<dyn MetadataEventLog>) -> Result<Self, BrokerError> {
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        Self::start_with_cache(log, cache, BrokerMetrics::new()).await
    }

    /// Subscribe the projection to the index topic and replay it from the
    /// start.
    ///
    /// The subscription is established *before* the end offsets are read.
    /// `subscribe` replays each partition's backlog and then forwards live
    /// appends, so a watermark taken afterwards names only offsets the stream
    /// is bound to deliver — including a record another broker appended
    /// between the two calls, which a watermark taken first would have left
    /// out of the target. A stable keyed fence is then appended to every
    /// non-empty partition. The fence offsets are the catch-up target that
    /// [`Self::wait_until_caught_up`] reports against. Kafka compaction can
    /// leave holes, including at the former end offset, but the current fence
    /// remains present and proves the pump walked the whole committed view.
    ///
    /// # Errors
    ///
    /// Returns an error when the index topic's end offsets cannot be read.
    pub(crate) async fn start_with_cache(
        log: Arc<dyn MetadataEventLog>,
        cache: Arc<Mutex<WalIndexCache>>,
        metrics: BrokerMetrics,
    ) -> Result<Self, BrokerError> {
        let starts = (0..log.partition_count())
            .map(|partition| PartitionStart {
                partition,
                start_offset: 0,
            })
            .collect();
        let (mut stream, _assignment) = log.subscribe(starts);

        let high_water_marks = log
            .high_water_marks()
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless index end offsets: {error}")))?;
        let mut pending = HashMap::new();
        for partition in 0..log.partition_count() {
            let index = usize::try_from(partition).expect("partition non-negative");
            if high_water_marks.get(index).copied().unwrap_or(0) == 0 {
                continue;
            }
            let offset = log
                .publish_keyed(
                    partition,
                    Bytes::from_static(REPLAY_FENCE_KEY),
                    Some(Bytes::new()),
                )
                .await
                .map_err(|error| {
                    BrokerError::Txn(format!("diskless index replay fence: {error}"))
                })?;
            pending.insert(partition, offset);
        }
        let (progress_tx, progress_rx) = watch::channel(ReplayProgress {
            slowest_pending: 0,
            caught_up: pending.is_empty(),
            invalid: false,
        });
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let valid = Arc::new(AtomicBool::new(true));

        let pump_cache = cache.clone();
        let pump_valid = Arc::clone(&valid);
        tokio::spawn(async move {
            let mut delivered: HashMap<i32, u64> =
                pending.keys().map(|partition| (*partition, 0)).collect();
            while let Some(event) = stream.next().await {
                // A floor key and a range key never decode as each other, so
                // the two projections stay independent however the index
                // topic's partitions interleave them.
                let floor_key = event.key.as_deref().and_then(WalDeleteFloorKey::from_bytes);
                let range_key = event.key.as_deref().and_then(WalIndexKey::from_bytes);
                // A key that is neither, and not the replay fence, is of a key
                // version this build does not read. Skipping it would leave a
                // range or floor out of the projection, so it is as unsafe as
                // an unkeyed record.
                let unknown_key = event.key.as_deref().filter(|key| {
                    *key != REPLAY_FENCE_KEY && floor_key.is_none() && range_key.is_none()
                });
                if let Some(key) = unknown_key {
                    invalidate_projection(&metrics, &pump_valid, &progress_tx);
                    tracing::error!(
                        partition = event.partition,
                        offset = event.offset,
                        key_version = ?wal_index_key_version(key),
                        "diskless WAL index record has an unknown key version; projection is \
                         unsafe"
                    );
                } else if event.tombstone {
                    if let Some(key) = floor_key {
                        pump_cache.lock().await.clear_delete_floor(key);
                    } else if let Some(key) = range_key {
                        pump_cache.lock().await.remove(key);
                    }
                } else if event.key.as_deref() != Some(REPLAY_FENCE_KEY) {
                    let result = if let Some(key) = floor_key {
                        WalDeleteFloorRecord::from_bytes(&event.payload)
                            .map(|record| (Some((key, record.floor)), None))
                    } else {
                        WalFlushRecord::from_bytes(&event.payload)
                            .map(|record| (None, Some(record)))
                    };
                    match result {
                        Ok((Some((key, floor)), None)) => {
                            pump_cache.lock().await.raise_delete_floor(
                                key.topic_id,
                                key.partition,
                                floor,
                            );
                            applied_tx.send_modify(|generation| {
                                *generation = generation.wrapping_add(1);
                            });
                        }
                        Ok((None, Some(record))) => {
                            // Every index record is keyed (`publish_flush`);
                            // an unkeyed one has no range it is the latest
                            // for, so the projection can no longer be trusted.
                            if let Some(key) = range_key {
                                pump_cache.lock().await.apply_keyed(key, &record);
                                applied_tx.send_modify(|generation| {
                                    *generation = generation.wrapping_add(1);
                                });
                            } else {
                                invalidate_projection(&metrics, &pump_valid, &progress_tx);
                                tracing::error!(
                                    partition = event.partition,
                                    offset = event.offset,
                                    "unkeyed diskless WAL index record; projection is unsafe"
                                );
                            }
                        }
                        Err(error) => {
                            invalidate_projection(&metrics, &pump_valid, &progress_tx);
                            tracing::error!(
                                partition = event.partition,
                                offset = event.offset,
                                %error,
                                "diskless WAL index record rejected; projection is unsafe"
                            );
                        }
                        _ => unreachable!("decode result has exactly one record kind"),
                    }
                }
                let Some(target) = pending.get(&event.partition).copied() else {
                    continue;
                };
                *delivered.entry(event.partition).or_default() += 1;
                if event.offset >= target {
                    pending.remove(&event.partition);
                    delivered.remove(&event.partition);
                }
                let next = ReplayProgress {
                    slowest_pending: delivered.values().copied().min().unwrap_or(u64::MAX),
                    caught_up: pending.is_empty(),
                    invalid: !pump_valid.load(Ordering::Acquire),
                };
                // Notify only on a real change, so a waiter's stall timer is
                // not reset by a busy partition while another one is stuck.
                progress_tx.send_if_modified(|progress| {
                    let changed = *progress != next;
                    *progress = next;
                    changed
                });
            }
        });
        Ok(Self {
            log,
            cache,
            progress: progress_rx,
            applied: applied_rx,
            valid,
        })
    }

    /// Resolve `true` once the projection has replayed every record the index
    /// topic held when this log started.
    ///
    /// Resolve `false` when the replay makes no progress for `stall_timeout`,
    /// or when the pump stops outright. Neither is recoverable on this
    /// subscription: a [`krabka_remote_storage_topic::KafkaMetadataEventLog`]
    /// partition whose fetch loop died while connecting goes silent forever
    /// without closing the shared stream, so a caller that keeps waiting
    /// never flushes again. Rebuild the log instead.
    pub(crate) async fn wait_until_caught_up(&self, stall_timeout: Duration) -> bool {
        let mut progress = self.progress.clone();
        loop {
            let state = *progress.borrow_and_update();
            if state.invalid {
                return false;
            }
            if state.caught_up {
                return true;
            }
            match tokio::time::timeout(stall_timeout, progress.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return false,
            }
        }
    }

    #[must_use]
    pub(crate) fn cache(&self) -> Arc<Mutex<WalIndexCache>> { self.cache.clone() }

    pub(crate) fn is_valid(&self) -> bool { self.valid.load(Ordering::Acquire) }

    /// Wait until the live projection becomes invalid or its pump stops.
    pub(crate) async fn wait_until_unusable(&self) {
        let mut progress = self.progress.clone();
        loop {
            if progress.borrow_and_update().invalid || progress.changed().await.is_err() {
                return;
            }
        }
    }

    /// Wait until `record` appears in the committed projection.
    pub(crate) async fn wait_until_applied(
        &self,
        record: &WalFlushRecord,
        timeout: Duration,
    ) -> bool {
        let mut applied = self.applied.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if self.cache.lock().await.contains_record(record) {
                    return true;
                }
                if applied.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    pub(crate) async fn publish_flush(&self, record: &WalFlushRecord) -> Result<i64, BrokerError> {
        let mut last_offset = -1;
        for entry in &record.entries {
            let key = WalIndexKey::from(entry).to_bytes();
            let bytes = WalFlushRecord {
                object_key: record.object_key.clone(),
                format_version: record.format_version,
                entries: vec![entry.clone()],
            }
            .to_bytes()
            .map_err(BrokerError::Txn)?;
            last_offset = self
                .log
                .publish_keyed(
                    index_partition(&key, self.log.partition_count()),
                    key,
                    Some(bytes),
                )
                .await
                .map_err(|error| BrokerError::Txn(format!("diskless index publish: {error}")))?;
        }
        Ok(last_offset)
    }

    /// Publish one partition's `DeleteRecords` floor and wait until the
    /// committed projection carries it.
    ///
    /// `DeleteRecords` acknowledges only after this returns, so a client that
    /// was told its records are gone cannot see them again after a restart or
    /// a leadership move.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be published, or when the
    /// projection does not report it within `timeout`.
    pub(crate) async fn publish_delete_floor(
        &self,
        topic_id: uuid::Uuid,
        partition: i32,
        floor: i64,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let key = WalDeleteFloorKey {
            topic_id,
            partition,
        };
        let bytes = WalDeleteFloorRecord {
            topic_id,
            partition,
            floor,
        }
        .to_bytes()
        .map_err(BrokerError::Txn)?;
        let key_bytes = key.to_bytes();
        self.log
            .publish_keyed(
                index_partition(&key_bytes, self.log.partition_count()),
                key_bytes,
                Some(bytes),
            )
            .await
            .map_err(|error| BrokerError::Txn(format!("diskless delete floor publish: {error}")))?;

        let mut applied = self.applied.clone();
        tokio::time::timeout(timeout, async {
            loop {
                if self.cache.lock().await.delete_floor(topic_id, partition) >= floor {
                    return true;
                }
                if applied.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
        .then_some(())
        .ok_or_else(|| BrokerError::Txn("diskless delete floor projection timed out".into()))
    }

    async fn publish_tombstone(&self, bytes: Bytes, context: &str) -> Result<(), BrokerError> {
        self.log
            .publish_keyed(
                index_partition(&bytes, self.log.partition_count()),
                bytes,
                None,
            )
            .await
            .map(|_| ())
            .map_err(|error| BrokerError::Txn(format!("{context}: {error}")))
    }

    /// Tombstone one partition's floor, so a deleted topic leaves none behind.
    pub(crate) async fn tombstone_delete_floor(
        &self,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), BrokerError> {
        let key = WalDeleteFloorKey {
            topic_id,
            partition,
        };
        self.publish_tombstone(key.to_bytes(), "diskless delete floor tombstone")
            .await?;
        self.cache.lock().await.clear_delete_floor(key);
        Ok(())
    }

    /// Tombstone every projected range for a deleted topic.
    pub(crate) async fn tombstone_topic(&self, topic_id: uuid::Uuid) -> Result<(), BrokerError> {
        let keys = self.cache.lock().await.keys_for_topic(topic_id);
        self.tombstone_ranges(&keys).await
    }

    /// Tombstone named logical ranges, one keyed record each, and drop them
    /// from the projection.
    ///
    /// A partial failure leaves the ranges it already published tombstoned and
    /// the rest untouched. That is safe to retry: a tombstone for a range the
    /// projection no longer holds is a no-op, and the caller recomputes the
    /// remaining set on its next pass.
    pub(crate) async fn tombstone_ranges(&self, keys: &[WalIndexKey]) -> Result<(), BrokerError> {
        for key in keys {
            self.publish_tombstone(key.to_bytes(), "diskless index tombstone")
                .await?;
            self.cache.lock().await.remove(*key);
        }
        Ok(())
    }
}

fn invalidate_projection(
    metrics: &BrokerMetrics,
    valid: &AtomicBool,
    progress: &watch::Sender<ReplayProgress>,
) {
    metrics.diskless_wal_index_decode_failures_total.inc();
    valid.store(false, Ordering::Release);
    progress.send_modify(|progress| progress.invalid = true);
}

fn index_partition(key: &[u8], partitions: i32) -> i32 {
    let hash = key.iter().copied().fold(0u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    i32::try_from(hash % u32::try_from(partitions).expect("positive partition count"))
        .expect("index partition fits i32")
}

#[cfg(test)]
mod tests {
    use {
        super::{
            test_support::{PacedReplayLog, RacingAppendLog, ReplayPace},
            *,
        },
        crate::{
            diskless::wal_index::{WalFlushRecord, WalIndexEntry},
            metrics::BrokerMetrics,
        },
        assert2::assert,
        async_trait::async_trait,
        krabka_remote_storage_topic::{
            AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventStream,
            MetadataLogError, PartitionStart,
        },
        std::sync::atomic::{AtomicBool, Ordering},
        tokio::time::timeout,
        uuid::Uuid,
    };

    #[derive(Clone, Copy)]
    enum PublishOutcome {
        Succeed,
        Fail,
    }

    struct TogglePublishLog {
        inner: Arc<dyn MetadataEventLog>,
        fail: AtomicBool,
    }

    impl TogglePublishLog {
        fn new(inner: Arc<dyn MetadataEventLog>, outcome: PublishOutcome) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail: AtomicBool::new(matches!(outcome, PublishOutcome::Fail)),
            })
        }

        fn set_outcome(&self, outcome: PublishOutcome) {
            self.fail
                .store(matches!(outcome, PublishOutcome::Fail), Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl MetadataEventLog for TogglePublishLog {
        fn partition_count(&self) -> i32 { self.inner.partition_count() }

        async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
            self.inner.publish(partition, event).await
        }

        async fn publish_keyed(
            &self,
            partition: i32,
            key: Bytes,
            event: Option<Bytes>,
        ) -> Result<i64, MetadataLogError> {
            if self.fail.load(Ordering::Relaxed) {
                Err(MetadataLogError::Closed)
            } else {
                self.inner.publish_keyed(partition, key, event).await
            }
        }

        fn subscribe(
            &self,
            assignment: Vec<PartitionStart>,
        ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
            self.inner.subscribe(assignment)
        }

        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
            self.inner.high_water_marks().await
        }
    }

    use super::test_support::{flush_record, second_object_flush_record};

    async fn seeded_index(
        event_log: Arc<dyn MetadataEventLog>,
        topic_id: Uuid,
        ranges: &[(&str, i64, i64)],
    ) -> DisklessIndexLog {
        let seed = DisklessIndexLog::start(event_log).await.unwrap();
        for &(key, first, last) in ranges {
            seed.publish_flush(
                &flush_record()
                    .topic_id(topic_id)
                    .object_key(key)
                    .first_offset(first)
                    .last_offset(last)
                    .max_timestamp(0)
                    .call(),
            )
            .await
            .unwrap();
        }
        seed
    }

    async fn two_range_backlog(
        partitions: i32,
    ) -> (Arc<InProcessMetadataEventLog>, Uuid, DisklessIndexLog) {
        let event_log = InProcessMetadataEventLog::new(partitions);
        let topic_id = Uuid::from_u128(7);
        let seed = seeded_index(event_log.clone(), topic_id, &[
            ("object-a", 0, 3),
            ("object-b", 4, 7),
        ])
        .await;
        (event_log, topic_id, seed)
    }

    async fn check_replayed_backlog(index: &DisklessIndexLog, topic_id: Uuid, context: &str) {
        assert!(index.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(
            index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8),
            "{context}"
        );
    }

    #[tokio::test]
    async fn catch_up_resolves_only_once_the_topic_backlog_is_projected() {
        let (event_log, topic_id, _seed) = two_range_backlog(2).await;

        // A restart against the now-populated topic.
        let restarted = DisklessIndexLog::start(event_log).await.unwrap();
        check_replayed_backlog(
            &restarted,
            topic_id,
            "catch-up must not resolve before the whole backlog is projected",
        )
        .await;
    }

    #[tokio::test]
    async fn incompatible_index_record_fails_replay_and_increments_metric() {
        let event_log = InProcessMetadataEventLog::new(1);
        let mut old = flush_record().object_key("old-object").call();
        old.format_version = WalFlushRecord::FORMAT_VERSION - 1;
        let bytes =
            <serde_wincode::SerdeCompat<WalFlushRecord> as wincode::Serialize>::serialize(&old)
                .unwrap();
        event_log.publish(0, Bytes::from(bytes)).await.unwrap();

        let metrics = BrokerMetrics::new();
        let failures = metrics.diskless_wal_index_decode_failures_total.clone();
        let index = DisklessIndexLog::start_with_cache(
            event_log,
            Arc::new(Mutex::new(WalIndexCache::default())),
            metrics,
        )
        .await
        .unwrap();

        assert!(!index.wait_until_caught_up(Duration::from_secs(1)).await);
        assert!(!index.is_valid());
        assert!(failures.get() == 1);
    }

    async fn check_invalid_index_log(event_log: Arc<InProcessMetadataEventLog>, case: &str) {
        let metrics = BrokerMetrics::new();
        let failures = metrics.diskless_wal_index_decode_failures_total.clone();
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        let index = DisklessIndexLog::start_with_cache(event_log, cache.clone(), metrics)
            .await
            .unwrap();

        assert!(
            !index.wait_until_caught_up(Duration::from_secs(1)).await,
            "case {case}"
        );
        assert!(!index.is_valid(), "case {case}");
        assert!(failures.get() == 1, "case {case}");
        assert!(
            cache
                .lock()
                .await
                .lookup(Uuid::from_u128(7), 0, 0)
                .is_none(),
            "case {case}"
        );
    }

    #[tokio::test]
    async fn an_index_record_of_an_unknown_key_version_fails_replay_and_increments_metric() {
        let record = flush_record().object_key("future-key").call();
        let mut future_key = WalIndexKey::from(&record.entries[0]).to_bytes().to_vec();
        future_key[..2].copy_from_slice(&7_i16.to_be_bytes());
        for (name, payload) in [
            ("value", Some(record.to_bytes().unwrap())),
            ("tombstone", None),
        ] {
            let event_log = InProcessMetadataEventLog::new(1);
            event_log
                .publish_keyed(0, Bytes::from(future_key.clone()), payload)
                .await
                .unwrap();

            check_invalid_index_log(event_log, name).await;
        }
    }

    #[tokio::test]
    async fn an_unkeyed_index_record_fails_replay_and_increments_metric() {
        let event_log = InProcessMetadataEventLog::new(1);
        let record = flush_record().object_key("unkeyed").call();
        event_log
            .publish(0, record.to_bytes().unwrap())
            .await
            .unwrap();

        check_invalid_index_log(event_log, "unkeyed").await;
    }

    #[tokio::test]
    async fn catch_up_resolves_immediately_for_an_empty_index_topic() {
        let index = DisklessIndexLog::start(InProcessMetadataEventLog::new(2))
            .await
            .unwrap();
        timeout(
            Duration::from_secs(1),
            index.wait_until_caught_up(Duration::from_secs(5)),
        )
        .await
        .expect("an empty topic has no backlog to replay");
    }

    #[tokio::test]
    async fn catch_up_gives_up_when_the_replay_stops_making_progress() {
        let event_log = InProcessMetadataEventLog::new(1);
        let _seed =
            seeded_index(event_log.clone(), Uuid::from_u128(7), &[("object-a", 0, 3)]).await;

        // A silent-but-open stream is what a dead partition fetch loop leaves
        // behind. Waiting on it forever would never flush again.
        let stalled = DisklessIndexLog::start(PacedReplayLog::new(event_log, ReplayPace::Never))
            .await
            .unwrap();
        assert!(
            !stalled
                .wait_until_caught_up(Duration::from_millis(50))
                .await
        );
    }

    #[tokio::test]
    async fn catch_up_keeps_waiting_while_a_slow_replay_still_advances() {
        let (event_log, topic_id, _seed) = two_range_backlog(1).await;

        // Every record lands well inside the stall window, but the replay as a
        // whole outlasts it: progress, not elapsed time, is what the gate
        // measures.
        let paced = DisklessIndexLog::start(PacedReplayLog::new(
            event_log,
            ReplayPace::OneEvery(Duration::from_millis(40)),
        ))
        .await
        .unwrap();
        assert!(paced.wait_until_caught_up(Duration::from_millis(400)).await);
        assert!(paced.cache().lock().await.flushed_frontier(topic_id, 0) == Some(8));
    }

    #[tokio::test]
    async fn catch_up_covers_a_record_appended_while_the_subscription_opens() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let _seed = seeded_index(event_log.clone(), topic_id, &[("object-a", 0, 3)]).await;

        // The previous leader's in-flight flush lands while this projection is
        // subscribing. Pacing the replay keeps the assertion off the pump's
        // heels, so a gate that stopped one record short stays caught short.
        let racing = second_object_flush_record(topic_id);
        let racing_key = WalIndexKey::from(&racing.entries[0]).to_bytes();
        let restarted = DisklessIndexLog::start(
            RacingAppendLog::build()
                .inner(PacedReplayLog::new(
                    event_log,
                    ReplayPace::OneEvery(Duration::from_millis(40)),
                ))
                .key(racing_key)
                .event(racing.to_bytes().unwrap())
                .call(),
        )
        .await
        .unwrap();

        check_replayed_backlog(
            &restarted,
            topic_id,
            "the racing append must be part of the replay target",
        )
        .await;
    }

    /// The floor has to be a durable record, not a per-broker mirror.
    ///
    /// The range covering offsets 0-7 straddles a floor of 4, so retention
    /// must keep it -- and it is the newest range besides, which retention
    /// never expires. A projection rebuilt over the same index topic therefore
    /// replays that range in full, and only the floor record stops it from
    /// answering for the deleted offsets again.
    #[tokio::test]
    async fn a_delete_floor_survives_a_rebuilt_projection() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let seed = seeded_index(event_log.clone(), topic_id, &[("object-a", 0, 7)]).await;
        seed.publish_delete_floor(topic_id, 0, 4, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(seed.cache().lock().await.delete_floor(topic_id, 0) == 4);

        let restarted = DisklessIndexLog::start(event_log.clone()).await.unwrap();
        assert!(restarted.wait_until_caught_up(Duration::from_secs(5)).await);

        let cache = restarted.cache();
        let cache = cache.lock().await;
        assert!(
            cache.delete_floor(topic_id, 0) == 4,
            "a restart must not resurrect the deleted offsets"
        );
        assert!(cache.earliest_covered(topic_id, 0) == Some(4));
        assert!(cache.lookup_fetch_range(topic_id, 0, 3, 4096).is_none());
        assert!(cache.lookup_fetch_range(topic_id, 0, 4, 4096).is_some());
        // The range itself is intact: retention may not expire it, because it
        // still holds every offset from 4 up.
        assert!(cache.flushed_frontier(topic_id, 0) == Some(8));
        drop(cache);

        // A tombstone takes the floor away with its topic.
        restarted.tombstone_delete_floor(topic_id, 0).await.unwrap();
        let after = DisklessIndexLog::start(event_log).await.unwrap();
        assert!(after.wait_until_caught_up(Duration::from_secs(5)).await);
        assert!(after.cache().lock().await.delete_floor(topic_id, 0) == 0);
    }

    /// A floor that only moves forward, so neither a stale client retry nor a
    /// compacted replay that delivers an older record last can widen what a
    /// consumer sees.
    #[tokio::test]
    async fn a_delete_floor_never_moves_backwards() {
        let event_log = InProcessMetadataEventLog::new(1);
        let topic_id = Uuid::from_u128(7);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        index
            .publish_delete_floor(topic_id, 0, 9, Duration::from_secs(5))
            .await
            .unwrap();

        let stale = index
            .publish_delete_floor(topic_id, 0, 2, Duration::from_millis(50))
            .await;

        // The publish itself is fine; the projection simply keeps the higher
        // floor, which is what the wait reports against.
        assert!(stale.is_ok());
        assert!(index.cache().lock().await.delete_floor(topic_id, 0) == 9);
    }

    #[tokio::test]
    async fn index_log_projects_published_flush_records() {
        let event_log = InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log).await.unwrap();
        let topic_id = Uuid::from_u128(7);
        let record = WalFlushRecord {
            object_key: "object-a".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                byte_start: 6,
                byte_len: 10,
                max_timestamp_ms: 0,
            }],
        };

        index.publish_flush(&record).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(4) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(index.cache().lock().await.lookup(topic_id, 0, 2).is_some());
    }

    #[tokio::test]
    async fn startup_surfaces_a_replay_fence_publish_failure() {
        let inner = InProcessMetadataEventLog::new(1);
        inner
            .publish(0, Bytes::from_static(b"record"))
            .await
            .unwrap();

        let Err(error) =
            DisklessIndexLog::start(TogglePublishLog::new(inner, PublishOutcome::Fail)).await
        else {
            panic!("the replay fence publish must fail");
        };
        assert!(error.to_string().contains("diskless index replay fence"));
    }

    async fn projected_first_object(
        transport: Arc<dyn MetadataEventLog>,
    ) -> (DisklessIndexLog, Uuid, WalFlushRecord) {
        let index = DisklessIndexLog::start(transport).await.unwrap();
        let topic_id = Uuid::from_u128(7);
        let record = flush_record().topic_id(topic_id).call();
        index.publish_flush(&record).await.unwrap();
        assert!(
            index
                .wait_until_applied(&record, Duration::from_secs(1))
                .await
        );
        (index, topic_id, record)
    }

    #[tokio::test]
    async fn flush_and_tombstone_publish_failures_are_returned() {
        let transport =
            TogglePublishLog::new(InProcessMetadataEventLog::new(1), PublishOutcome::Succeed);
        let (index, topic_id, _record) = projected_first_object(transport.clone()).await;

        transport.set_outcome(PublishOutcome::Fail);

        let publish_error = index
            .publish_flush(&second_object_flush_record(topic_id))
            .await
            .unwrap_err();
        assert!(publish_error.to_string().contains("diskless index publish"));

        let tombstone_error = index.tombstone_topic(topic_id).await.unwrap_err();
        assert!(
            tombstone_error
                .to_string()
                .contains("diskless index tombstone")
        );
    }

    #[tokio::test]
    async fn consumed_tombstone_removes_the_projected_range() {
        let transport = InProcessMetadataEventLog::new(1);
        let (index, topic_id, record) = projected_first_object(transport.clone()).await;
        let key = WalIndexKey::from(&record.entries[0]);

        let bytes = key.to_bytes();
        transport
            .publish_keyed(
                index_partition(&bytes, transport.partition_count()),
                bytes,
                None,
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(1), async {
            while index.cache().lock().await.lookup(topic_id, 0, 0).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
