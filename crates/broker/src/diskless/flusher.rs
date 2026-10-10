//! Diskless WAL object-store flusher.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::TryStreamExt as _;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_units::convert::{ByteSizeExt, TimeExt};
use krabka_verified::diskless::DisklessRetentionPolicy;
use object_store::{ObjectStore, ObjectStoreExt as _};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{index_log::DisklessIndexLog, wal_index::WalFlushRecord};
use crate::{partition::Partition, partition_registry::PartitionRegistry};

mod config;
mod object_flush;
#[cfg(test)]
mod test_support;

pub(crate) use self::{config::FlushConfig, object_flush::flush_once};

/// Every broker sweeps the shared prefix, so objects from removed brokers are
/// covered too. The observation grace lets independent index consumers apply
/// replacements before any broker deletes the old object.
const RECLAIM_INTERVAL: Duration = Duration::from_secs(30);
const RECLAIM_GRACE: Duration = Duration::from_mins(5);

pub(crate) struct FlushPartition {
    pub(crate) topic_id: Uuid,
    pub(crate) handle: Arc<Partition>,
    pub(crate) high_watermark: Offset,
}

/// Dependencies owned by the broker's diskless flush task.
pub(crate) struct FlusherContext {
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) image_rx: tokio::sync::watch::Receiver<Arc<MetadataImage>>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) index_log: DisklessIndexLog,
    pub(crate) node_id: NodeId,
    pub(crate) broker_id: i32,
    pub(crate) metrics: crate::metrics::BrokerMetrics,
    /// Set once the first tick is allowed to fire, which is after the index
    /// projection has replayed the index topic.
    pub(crate) ready: Arc<AtomicBool>,
}

/// Why [`run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlusherExit {
    /// Shutdown was requested. The flusher is done.
    ShutDown,
    /// The index projection never replayed the index topic, so no tick ever
    /// fired. This is not recoverable on the index log the flusher was handed:
    /// the caller has to rebuild it. See
    /// [`DisklessIndexLog::wait_until_caught_up`].
    ReplayStalled,
    /// The live index projection became invalid or stopped. Flushing and
    /// reclamation must stop until the caller rebuilds it.
    ProjectionUnavailable,
}

/// Flush committed tails until broker shutdown. A failed tick does not move
/// the durable index frontier, so the next tick safely retries the same tail.
///
/// The first tick waits for the index projection to replay the index topic.
/// A tick that fires before that reads a frontier that is behind what object
/// storage already holds, re-uploads the flushed prefix under a fresh key,
/// and — because [`WalIndexCache::apply`] keys on `first_offset` — leaves the
/// object the previous incarnation wrote unreferenced forever.
///
/// [`WalIndexCache::apply`]: crate::diskless::wal_index::WalIndexCache::apply
pub(crate) async fn run(
    context: FlusherContext,
    config: FlushConfig,
    shutdown: CancellationToken,
) -> FlusherExit {
    let caught_up = tokio::select! {
        biased;
        () = shutdown.cancelled() => return FlusherExit::ShutDown,
        caught_up = context
            .index_log
            .wait_until_caught_up(config.index_projection_timeout) => caught_up,
    };
    if !caught_up {
        tracing::warn!(
            stall_timeout_ms = config.index_projection_timeout.as_millis(),
            "diskless WAL index replay made no progress; no flush has run"
        );
        return FlusherExit::ReplayStalled;
    }
    let projection_unusable = context.index_log.wait_until_unusable();
    tokio::pin!(projection_unusable);
    let mut reclaimer = Reclaimer::new(RECLAIM_GRACE);
    tokio::select! {
        biased;
        () = shutdown.cancelled() => return flusher_shut_down(),
        () = &mut projection_unusable => return FlusherExit::ProjectionUnavailable,
        () = reclaimer.sweep(&context) => {}
    }
    context.ready.store(true, Ordering::Release);
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reclaim_ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + RECLAIM_INTERVAL,
        RECLAIM_INTERVAL,
    );
    reclaim_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rotation = 0usize;
    // Shutdown comes before both tickers, and it also interrupts the tick in
    // flight. A tick that outlasts the interval leaves the next tick already
    // due, so a ticker polled first would win every pass and the flusher would
    // never see shutdown. A flush tick outlasts the interval whenever the
    // index publish cannot reach a leader, which is exactly what happens while
    // the rest of the cluster stops. Leaving a tick unfinished is safe: a
    // flush is built to survive a crash between any two of its steps, and the
    // durable index frontier moves only when its record is published.
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return flusher_shut_down(),
            () = &mut projection_unusable => {
                tracing::error!("diskless WAL index projection became unavailable; stopping flusher");
                return FlusherExit::ProjectionUnavailable;
            }
            _ = ticker.tick() => {
                let flushed = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return flusher_shut_down(),
                    flushed = flush_tick(&context, &config, rotation) => flushed,
                };
                if let Err(error) = flushed {
                    tracing::warn!(%error, "diskless WAL flush failed; retrying");
                }
                rotation = rotation.wrapping_add(1);
            }
            _ = reclaim_ticker.tick() => {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return flusher_shut_down(),
                    () = reclaimer.sweep(&context) => {}
                }
            }
        }
    }
}

fn flusher_shut_down() -> FlusherExit {
    tracing::debug!("diskless WAL flusher shutting down");
    FlusherExit::ShutDown
}

async fn flush_tick(
    context: &FlusherContext,
    config: &FlushConfig,
    rotation: usize,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let image = context.image_rx.borrow().clone();
    tombstone_deleted_topics(context, &image).await?;
    let mut partitions = flushable_partitions(&context.partitions, &image, context.node_id).await;
    expire_retention_breached_ranges(context, &image, &partitions, crate::time_util::now_ms())
        .await?;
    if !partitions.is_empty() {
        let start = rotation % partitions.len();
        partitions.rotate_left(start);
    }
    flush_once(
        Arc::clone(&context.object_store),
        context.broker_id,
        &context.metrics,
        &context.index_log,
        context.index_log.cache(),
        &partitions,
        config,
        |partition| partition.handle.current_leader.load(Ordering::Relaxed) == context.node_id,
    )
    .await
}

async fn tombstone_deleted_topics(
    context: &FlusherContext,
    image: &MetadataImage,
) -> Result<(), crate::error::BrokerError> {
    let topic_ids = context.index_log.cache().lock().await.topic_ids();
    for topic_id in topic_ids {
        if image.topic_by_id(&topic_id).is_none() {
            context.index_log.tombstone_topic(topic_id).await?;
            let floors = context
                .index_log
                .cache()
                .lock()
                .await
                .delete_floor_partitions(topic_id);
            for partition in floors {
                context
                    .index_log
                    .tombstone_delete_floor(topic_id, partition)
                    .await?;
            }
        }
    }
    Ok(())
}

/// Tombstone the index ranges each led partition's retention has expired.
///
/// Kafka runs the same three predicates on every `LogManager` cleanup tick,
/// against both the local segments and, on a tiered topic, the remote ones.
/// On a diskless topic the object tier is the only tier that holds the
/// records, so the tombstone is what makes retention mean anything at all.
///
/// The object itself is freed by [`Reclaimer`] on a later sweep, and only once
/// no range in it is referenced. One object holds runs from several
/// partitions, so a partition whose ranges all expire can still leave the
/// object in the bucket until its co-tenants expire too.
///
/// KFC-9: a write freeze stops this pass, as it stops the cleaner and both of
/// the remote-log-manager's retention passes. A frozen topic's prefix has to
/// stay byte-identical, and expiring a range here would let the reclaimer
/// delete the object bytes behind it. A thaw makes the next tick eligible
/// again with no further operator step.
async fn expire_retention_breached_ranges(
    context: &FlusherContext,
    image: &MetadataImage,
    partitions: &[FlushPartition],
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    for partition in partitions {
        if matches!(
            crate::freeze::resolve::resolve_freeze_mutation(
                image,
                &partition.handle.topic,
                true,
                krabka_verified::FreezeMutationKind::Retention,
            ),
            crate::freeze::resolve::FreezeMutationResolution::Frozen(_)
        ) {
            tracing::debug!(
                topic = %partition.handle.topic,
                partition = partition.handle.index.get(),
                "diskless WAL retention held by a write freeze"
            );
            continue;
        }
        let config = partition
            .handle
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config_snapshot();
        let retention_ms = config.retention.map(TimeExt::millis_i64_trunc);
        let retention_bytes = config.retention_size.map(ByteSizeExt::bytes_u64);
        let index = partition.handle.index.get();
        let keys = {
            let cache = context.index_log.cache();
            let cache = cache.lock().await;
            // The `DeleteRecords` floor, not the local log start: the flusher
            // trims the local log behind the committed index on every tick, so
            // the log start says only how much of the WAL has reached the
            // bucket, never what an operator asked to delete.
            let log_start_offset = cache.delete_floor(partition.topic_id, index);
            cache.retention_expired_keys(
                partition.topic_id,
                index,
                DisklessRetentionPolicy {
                    retention_ms,
                    retention_bytes,
                    log_start_offset,
                    now_ms,
                },
            )
        };
        if keys.is_empty() {
            continue;
        }
        context.index_log.tombstone_ranges(&keys).await?;
        context
            .metrics
            .diskless_wal_expired_ranges_total
            .inc_by(u64::try_from(keys.len()).unwrap_or(u64::MAX));
    }
    Ok(())
}

struct Reclaimer {
    first_seen: HashMap<String, Instant>,
    grace: Duration,
}

impl Reclaimer {
    fn new(grace: Duration) -> Self {
        Self {
            first_seen: HashMap::new(),
            grace,
        }
    }

    async fn sweep(&mut self, context: &FlusherContext) {
        self.sweep_at(context, Instant::now()).await;
    }

    async fn sweep_at(&mut self, context: &FlusherContext, now: Instant) {
        let referenced = context.index_log.cache().lock().await.referenced_objects();
        let prefix = object_store::path::Path::from("diskless-wal");
        let objects = match context
            .object_store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(objects) => objects,
            Err(error) => {
                tracing::warn!(%error, "diskless WAL reclaim listing failed; retrying");
                return;
            }
        };
        let mut unreferenced = HashSet::new();
        for object in objects {
            let object_key = object.location.to_string();
            if referenced.contains(&object_key) {
                self.first_seen.remove(&object_key);
                continue;
            }
            unreferenced.insert(object_key.clone());
            let first_seen = *self.first_seen.entry(object_key.clone()).or_insert(now);
            let grace_elapsed = now.duration_since(first_seen) >= self.grace;
            if !krabka_verified::diskless_object_reclaimable(false, grace_elapsed) {
                continue;
            }
            let cache = context.index_log.cache();
            let cache = cache.lock().await;
            if !krabka_verified::diskless_object_reclaimable(
                cache.references_object(&object_key),
                true,
            ) {
                self.first_seen.remove(&object_key);
                continue;
            }
            match context.object_store.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    self.first_seen.remove(&object_key);
                }
                Err(error) => {
                    tracing::warn!(%object_key, %error, "diskless WAL reclaim failed; retrying");
                }
            }
        }
        self.first_seen
            .retain(|object_key, _| unreferenced.contains(object_key));
    }
}

async fn flushable_partitions(
    registry: &PartitionRegistry,
    image: &MetadataImage,
    node_id: NodeId,
) -> Vec<FlushPartition> {
    // Snapshot registry handles before awaiting any partition state.
    let mut out = Vec::new();
    for handle in registry.arcs() {
        if !handle.diskless || handle.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        let Some(topic_id) = image.topic(&handle.topic).map(|topic| topic.topic_id) else {
            continue;
        };
        let high_watermark = handle.high_watermark().await;
        out.push(FlushPartition {
            topic_id,
            handle,
            high_watermark,
        });
    }
    out.sort_unstable_by(|left, right| {
        left.handle
            .topic
            .cmp(&right.handle.topic)
            .then_with(|| left.handle.index.cmp(&right.handle.index))
    });
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use krabka_remote_storage_topic::MetadataEventLog;
    use krabka_units::ByteSize;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use tempfile::tempdir;

    use super::{
        test_support::{flush_record, orders_partition, test_index_log, test_partition},
        *,
    };
    use crate::{
        diskless::index_log::test_support::{PacedReplayLog, ReplayPace},
        test_support::FreezeSetup,
    };

    // Keep field evaluation order, including metrics and readiness allocation.
    macro_rules! test_context {
        ($partitions:expr, $image_rx:expr, $store:expr, $index:expr $(,)?) => {
            test_context!(
                $partitions,
                $image_rx,
                $store,
                $index,
                Arc::new(AtomicBool::new(false))
            )
        };
        ($partitions:expr, $image_rx:expr, $store:expr, $index:expr, $ready:expr $(,)?) => {
            FlusherContext {
                partitions: $partitions,
                image_rx: $image_rx,
                object_store: $store,
                index_log: $index,
                node_id: NodeId(1),
                broker_id: 7,
                metrics: crate::metrics::BrokerMetrics::new(),
                ready: $ready,
            }
        };
    }

    // Bind owning fixtures in the original caller scope and declaration order.
    macro_rules! memory_flusher_dependencies {
        ($topic_id:ident, $image:ident, $image_rx:ident, $store:ident; $count:expr) => {
            let ($topic_id, $image) = orders_image($count);
            let (_, $image_rx) = tokio::sync::watch::channel(Arc::new($image));
            let $store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        };
    }

    macro_rules! silent_replay_fixture {
        ($dir:ident, $ready:ident, $context:ident, $store:ident) => {
            let $dir = tempdir().unwrap();
            let $ready = Arc::new(AtomicBool::new(false));
            let ($context, $store) = silent_replay_flusher($dir.path(), Arc::clone(&$ready)).await;
        };
    }

    macro_rules! lock_cache {
        ($context:ident, $cache:ident) => {
            let $cache = $context.index_log.cache();
            let $cache = $cache.lock().await;
        };
    }

    macro_rules! assert_cached_range {
        ($context:ident, $topic_id:expr, $partition:expr, $offset:expr; $presence:ident) => {
            assert!(
                $context
                    .index_log
                    .cache()
                    .lock()
                    .await
                    .lookup($topic_id, $partition, $offset)
                    .$presence()
            );
        };
    }

    macro_rules! rotation_fixture {
        ($dir:ident, $topic_id:ident, $context:ident, $cache:ident) => {
            let $dir = tempdir().unwrap();
            let ($topic_id, $context) = rotation_context($dir.path()).await;
            let $cache = $context.index_log.cache();
        };
    }

    macro_rules! retention_fixture {
        ($dir:ident, $topic_id:ident, $context:ident, $store:ident, $handle:ident; $($image:tt)+) => {
            let $dir = tempdir().unwrap();
            let $topic_id = Uuid::from_u128(11);
            let ($context, $store, $handle, $($image)+) =
                seeded_retention_flusher($dir.path(), $topic_id, AGED_AND_FRESH_RANGES).await;
        };
    }

    async fn assert_replay_unflushed(ready: &AtomicBool, store: &Arc<dyn ObjectStore>) {
        assert!(!ready.load(Ordering::Acquire));
        assert!(object_keys(store).await.is_empty());
    }

    async fn stop_worker(shutdown: &CancellationToken, task: tokio::task::JoinHandle<FlusherExit>) {
        shutdown.cancel();
        assert!(task.await.unwrap() == FlusherExit::ShutDown);
    }

    async fn wait_for_frontier(
        cache: &Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
        topic_id: Uuid,
        partition: i32,
        frontier: i64,
    ) {
        wait_for_cache(cache, |cache| {
            cache.flushed_frontier(topic_id, partition) == Some(frontier)
        })
        .await;
    }

    async fn expire_and_reclaim(
        context: &FlusherContext,
        image: &MetadataImage,
        partitions: &[FlushPartition],
        now_ms: i64,
    ) {
        expire_retention_breached_ranges(context, image, partitions, now_ms)
            .await
            .unwrap();
        Reclaimer::new(Duration::ZERO).sweep(context).await;
    }

    fn orders_image(partitions: i32) -> (Uuid, MetadataImage) {
        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions,
            replication_factor: 1,
        }));
        (topic_id, image)
    }

    async fn rotation_context(root: &std::path::Path) -> (Uuid, FlusherContext) {
        let partitions = Arc::new(PartitionRegistry::new());
        for partition in 0..2 {
            partitions.insert(
                "orders".into(),
                krabka_ids::PartitionIndex(partition),
                test_partition(
                    root,
                    crate::diskless::flusher::test_support::FlusherPartitionSetup {
                        partition: krabka_ids::PartitionIndex(partition),
                        ..Default::default()
                    },
                ),
            );
        }
        let (topic_id, image) = orders_image(2);
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let index_log = test_index_log().await;
        (
            topic_id,
            test_context!(partitions, image_rx, Arc::new(InMemory::new()), index_log),
        )
    }

    #[tokio::test]
    async fn tick_rotates_size_limited_flush_start() {
        rotation_fixture!(dir, topic_id, context, cache);

        flush_tick(
            &context,
            &FlushConfig {
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            1,
        )
        .await
        .unwrap();

        let cache = cache.lock().await;
        assert!(cache.flushed_frontier(topic_id, 0).is_none());
        assert!(cache.flushed_frontier(topic_id, 1) == Some(3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_rotates_size_limited_flushes_without_starvation() {
        rotation_fixture!(dir, topic_id, context, cache);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            context,
            FlushConfig {
                interval: Duration::from_millis(1),
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        wait_for_cache(&cache, |cache| {
            cache.flushed_frontier(topic_id, 0) == Some(3)
                && cache.flushed_frontier(topic_id, 1) == Some(3)
        })
        .await;
        stop_worker(&shutdown, task).await;
    }

    /// Every object key the store holds, sorted.
    async fn object_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        use futures_util::TryStreamExt as _;

        let mut keys: Vec<String> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        keys
    }

    async fn wait_for_cache(
        cache: &Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
        predicate: impl Fn(&crate::diskless::wal_index::WalIndexCache) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if predicate(&*cache.lock().await) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_object(
        cache: &Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
        topic_id: Uuid,
        object_key: &str,
    ) {
        wait_for_cache(cache, |cache| {
            cache
                .lookup(topic_id, 0, 0)
                .is_some_and(|(key, _, _)| key == object_key)
        })
        .await;
    }

    #[tokio::test]
    async fn reclaim_grace_then_deletes_a_superseded_object_from_another_broker() {
        let topic_id = Uuid::from_u128(11);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in ["diskless-wal/9/old.ckwl", "diskless-wal/7/new.ckwl"] {
            store
                .put(&Path::from(key), PutPayload::from_static(b"object"))
                .await
                .unwrap();
        }
        let index = test_index_log().await;
        let cache = index.cache();
        for key in ["diskless-wal/9/old.ckwl", "diskless-wal/7/new.ckwl"] {
            index
                .publish_flush(&flush_record(
                    crate::diskless::index_log::test_support::WalFlushSetup {
                        topic_id,
                        object_key: key,
                        ..crate::diskless::index_log::test_support::WalFlushSetup::three_records()
                    },
                ))
                .await
                .unwrap();
            wait_for_object(&cache, topic_id, key).await;
        }
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(MetadataImage::new(Uuid::nil())));
        let context = test_context!(
            Arc::new(PartitionRegistry::new()),
            image_rx,
            Arc::clone(&store),
            index
        );

        let mut reclaimer = Reclaimer::new(Duration::from_mins(1));
        let observed = Instant::now();
        reclaimer.sweep_at(&context, observed).await;
        assert!(
            store
                .head(&Path::from("diskless-wal/9/old.ckwl"))
                .await
                .is_ok(),
            "the grace period protects lagging projections"
        );
        reclaimer
            .sweep_at(&context, observed + Duration::from_mins(1))
            .await;

        assert!(
            store
                .head(&Path::from("diskless-wal/9/old.ckwl"))
                .await
                .is_err()
        );
        assert!(
            store
                .head(&Path::from("diskless-wal/7/new.ckwl"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn deleted_topic_leaves_the_cache_and_object_store() {
        let topic_id = Uuid::from_u128(11);
        let object_key = "diskless-wal/7/deleted.ckwl";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(object_key), PutPayload::from_static(b"object"))
            .await
            .unwrap();
        let index = test_index_log().await;
        let cache = index.cache();
        index
            .publish_flush(&flush_record(
                crate::diskless::index_log::test_support::WalFlushSetup {
                    topic_id,
                    object_key,
                    ..crate::diskless::index_log::test_support::WalFlushSetup::three_records()
                },
            ))
            .await
            .unwrap();
        wait_for_object(&cache, topic_id, object_key).await;
        let image = MetadataImage::new(Uuid::nil());
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image.clone()));
        let context = test_context!(
            Arc::new(PartitionRegistry::new()),
            image_rx,
            Arc::clone(&store),
            index
        );

        tombstone_deleted_topics(&context, &image).await.unwrap();
        Reclaimer::new(Duration::ZERO).sweep(&context).await;

        assert!(cache.lock().await.lookup(topic_id, 0, 0).is_none());
        assert!(store.head(&Path::from(object_key)).await.is_err());
    }

    /// A flusher over one led diskless partition whose projection already
    /// holds `ranges` -- `(object key, first offset, last offset, batch max
    /// timestamp)`, one keyed record each -- with each object in the store.
    ///
    /// The partition itself is only here for its log configuration and its
    /// index, which is what the retention pass reads.
    async fn seeded_retention_flusher(
        dir: &std::path::Path,
        topic_id: Uuid,
        ranges: &[(&str, i64, i64, i64)],
    ) -> (
        FlusherContext,
        Arc<dyn ObjectStore>,
        Arc<Partition>,
        MetadataImage,
    ) {
        let handle = orders_partition(dir);
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            Arc::clone(&handle),
        );
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = test_index_log().await;
        for (object_key, first_offset, last_offset, max_timestamp_ms) in ranges.iter().copied() {
            store
                .put(&Path::from(object_key), PutPayload::from_static(b"object"))
                .await
                .unwrap();
            let record = flush_record(crate::diskless::index_log::test_support::WalFlushSetup {
                topic_id,
                object_key,
                first_offset: krabka_log::Offset(first_offset),
                last_offset: krabka_log::Offset(last_offset),
                max_timestamp: crate::test_support::UnixMillis(max_timestamp_ms),
                byte_len: krabka_units::bytes(6),
            });
            index.publish_flush(&record).await.unwrap();
            assert!(
                index
                    .wait_until_applied(&record, Duration::from_secs(1))
                    .await
            );
        }
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(
            crate::test_support::single_partition_topic("orders", topic_id),
        ));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image.clone()));
        (
            test_context!(partitions, image_rx, Arc::clone(&store), index),
            store,
            handle,
            image,
        )
    }

    const AGED_AND_FRESH_RANGES: &[(&str, i64, i64, i64)] = &[
        ("diskless-wal/7/aged.ckwl", 0, 2, 1_000),
        ("diskless-wal/7/fresh.ckwl", 3, 5, 5_000),
    ];

    fn set_retention(handle: &Partition, retention: krabka_units::Time) {
        let mut config = handle.log.lock().unwrap().config_snapshot();
        config.retention = Some(retention);
        handle.log.lock().unwrap().set_config(config);
    }

    fn led_orders_partition(dir: &std::path::Path) -> Arc<PartitionRegistry> {
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            orders_partition(dir),
        );
        partitions
    }

    fn flush_partition(topic_id: Uuid, handle: &Arc<Partition>) -> FlushPartition {
        FlushPartition {
            topic_id,
            handle: Arc::clone(handle),
            high_watermark: Offset(3),
        }
    }

    #[tokio::test]
    async fn retention_ms_expires_an_aged_range_and_frees_its_object() {
        retention_fixture!(dir, topic_id, context, store, handle; image);
        set_retention(&handle, krabka_units::millis(1));
        let partitions = [flush_partition(topic_id, &handle)];

        // Far enough past both batches that only the "keep the newest range"
        // rule stands between retention and an empty index.
        expire_and_reclaim(&context, &image, &partitions, 10_000).await;

        lock_cache!(context, cache);
        assert!(cache.lookup(topic_id, 0, 0).is_none());
        assert!(cache.lookup(topic_id, 0, 3).is_some());
        drop(cache);
        assert!(
            store
                .head(&Path::from("diskless-wal/7/aged.ckwl"))
                .await
                .is_err()
        );
        assert!(
            store
                .head(&Path::from("diskless-wal/7/fresh.ckwl"))
                .await
                .is_ok()
        );

        crate::metrics::test_support::render_registry!(context.metrics, body, registry; unwrap());
        assert!(body.contains("krabka_broker_diskless_wal_expired_ranges_total 1"));
    }

    /// KFC-9 refuses retention eviction on a frozen topic, and says so for
    /// local and remote retention alike. The diskless object tier is a third
    /// place bytes can be removed from, so the same row binds here.
    #[tokio::test]
    async fn a_write_freeze_holds_the_retention_pass() {
        retention_fixture!(dir, topic_id, context, store, handle; mut image);
        set_retention(&handle, krabka_units::millis(1));
        image.apply(&MetadataRecord::V1TopicFreeze(
            crate::test_support::topic_freeze_record(FreezeSetup {
                pattern_type: krabka_metadata::PatternType::Literal,
                reason: "a cutover is in flight",
                ..Default::default()
            }),
        ));
        let partitions = [flush_partition(topic_id, &handle)];

        // The same clock and retention that expire the aged range in
        // `retention_ms_expires_an_aged_range_and_frees_its_object`.
        expire_and_reclaim(&context, &image, &partitions, 10_000).await;

        assert_cached_range!(context, topic_id, 0, 0; is_some);
        assert!(
            store
                .head(&Path::from("diskless-wal/7/aged.ckwl"))
                .await
                .is_ok(),
            "a frozen topic's prefix has to stay byte-identical"
        );
    }

    #[tokio::test]
    async fn an_unlimited_retention_expires_nothing() {
        let dir = tempdir().unwrap();
        let topic_id = Uuid::from_u128(11);
        let (context, store, handle, image) = seeded_retention_flusher(
            dir.path(),
            topic_id,
            &[
                ("diskless-wal/7/first.ckwl", 0, 2, 1_000),
                ("diskless-wal/7/second.ckwl", 3, 5, 5_000),
            ],
        )
        .await;
        // Kafka's unlimited sentinel for both windows, which is what a topic
        // with `retention.ms=-1` and `retention.bytes=-1` reaches the flusher
        // as. The clock is then irrelevant, so this uses the largest one.
        let mut config = handle.log.lock().unwrap().config_snapshot();
        config.retention = None;
        config.retention_size = None;
        handle.log.lock().unwrap().set_config(config);
        let partitions = [flush_partition(topic_id, &handle)];

        expire_and_reclaim(&context, &image, &partitions, i64::MAX).await;

        assert_cached_range!(context, topic_id, 0, 0; is_some);
        assert!(
            store
                .head(&Path::from("diskless-wal/7/first.ckwl"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_delete_records_floor_expires_the_ranges_below_it() {
        let dir = tempdir().unwrap();
        let topic_id = Uuid::from_u128(11);
        let (context, store, handle, image) = seeded_retention_flusher(
            dir.path(),
            topic_id,
            &[
                ("diskless-wal/7/deleted.ckwl", 0, 2, 1_000),
                ("diskless-wal/7/kept.ckwl", 3, 5, 1_000),
            ],
        )
        .await;
        // The local log start is the flusher's trim frontier and says nothing
        // about a delete, so the floor the handler recorded is what the
        // retention pass reads.
        context
            .index_log
            .cache()
            .lock()
            .await
            .raise_delete_floor(topic_id, 0, 3);
        let partitions = [flush_partition(topic_id, &handle)];

        expire_and_reclaim(&context, &image, &partitions, 1_000).await;

        lock_cache!(context, cache);
        assert!(cache.lookup(topic_id, 0, 0).is_none());
        assert!(cache.earliest_covered(topic_id, 0) == Some(3));
        drop(cache);
        assert!(
            store
                .head(&Path::from("diskless-wal/7/deleted.ckwl"))
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restarted_worker_does_not_reupload_an_already_flushed_range() {
        let dir = tempdir().unwrap();
        let partitions = led_orders_partition(dir.path());

        memory_flusher_dependencies!(topic_id, image, image_rx, store; 1);
        let event_log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        // Retain the flushed prefix locally, so a projection that has not
        // caught up reads offset 0 as the flush start and re-uploads it.
        let config = FlushConfig {
            interval: Duration::from_millis(1),
            trim_safety_lag: None,
            ..FlushConfig::default()
        };

        let index = DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            test_context!(
                Arc::clone(&partitions),
                image_rx.clone(),
                Arc::clone(&store),
                index
            ),
            config.clone(),
            shutdown.clone(),
        ));
        wait_for_frontier(&cache, topic_id, 0, 3).await;
        stop_worker(&shutdown, task).await;
        let flushed = object_keys(&store).await;
        assert!(flushed.len() == 1);

        // Restart: a fresh projection over the already-populated index topic,
        // replaying far more slowly than the flush interval.
        let index = DisklessIndexLog::start(PacedReplayLog::new(
            event_log,
            ReplayPace::OneEvery(Duration::from_millis(200)),
        ))
        .await
        .unwrap();
        let cache = index.cache();
        let ready = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            test_context!(
                partitions,
                image_rx,
                Arc::clone(&store),
                index,
                Arc::clone(&ready)
            ),
            config,
            shutdown.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the restarted flusher becomes ready once the replay finishes");
        // Well inside the replay delay, so a flusher that ticked without
        // waiting has already landed its object by now.
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_worker(&shutdown, task).await;

        // A tick during the replay re-uploads offsets 0..=2 under a second
        // key, orphaning the object the first incarnation wrote.
        assert!(object_keys(&store).await == flushed);
        // Readiness means the replay is complete, so the first tick sees
        // everything object storage holds.
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
    }

    /// A flusher over one led diskless partition whose index topic already
    /// holds a flush record, but whose replay never delivers -- the shape a
    /// dead partition fetch loop leaves behind, with the stream open and
    /// silent. Returns the context and the object store behind it.
    async fn silent_replay_flusher(
        dir: &std::path::Path,
        ready: Arc<AtomicBool>,
    ) -> (FlusherContext, Arc<dyn ObjectStore>) {
        let partitions = led_orders_partition(dir);

        memory_flusher_dependencies!(topic_id, image, image_rx, store; 1);
        let event_log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let seed = DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        seed.publish_flush(&flush_record(
            crate::diskless::index_log::test_support::WalFlushSetup {
                topic_id,
                object_key: "diskless-wal/7/seed.ckwl",
                first_offset: krabka_log::Offset(0),
                last_offset: krabka_log::Offset(2),
                max_timestamp: crate::test_support::UnixMillis(0),
                ..Default::default()
            },
        ))
        .await
        .unwrap();

        let index = DisklessIndexLog::start(PacedReplayLog::new(event_log, ReplayPace::Never))
            .await
            .unwrap();
        (
            test_context!(partitions, image_rx, Arc::clone(&store), index, ready),
            store,
        )
    }

    fn stall_after(timeout: Duration) -> FlushConfig {
        FlushConfig {
            interval: Duration::from_millis(1),
            index_projection_timeout: timeout,
            trim_safety_lag: None,
            ..FlushConfig::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_stops_when_live_index_projection_becomes_invalid() {
        let event_log: Arc<dyn MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(MetadataImage::new(Uuid::nil())));
        let ready = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(run(
            test_context!(
                Arc::new(PartitionRegistry::new()),
                image_rx,
                Arc::new(InMemory::new()),
                index,
                Arc::clone(&ready)
            ),
            FlushConfig {
                interval: Duration::from_mins(1),
                ..FlushConfig::default()
            },
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        event_log
            .publish(0, bytes::Bytes::from_static(b"invalid index record"))
            .await
            .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("invalid projection stops the flusher")
            .unwrap();
        assert!(exit == FlusherExit::ProjectionUnavailable);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_reports_a_stalled_replay_instead_of_flushing_or_hanging() {
        silent_replay_fixture!(dir, ready, context, store);

        let exit = tokio::time::timeout(
            Duration::from_secs(5),
            run(
                context,
                stall_after(Duration::from_millis(50)),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("a stalled replay must return, not hang the flusher forever");

        // The bootstrap needs this to rebuild the index log; a fetch loop that
        // died on connect never recovers on its own subscription.
        assert!(exit == FlusherExit::ReplayStalled);
        assert_replay_unflushed(&ready, &store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_stops_at_shutdown_without_finishing_the_replay() {
        silent_replay_fixture!(dir, ready, context, store);

        // Shutdown during a replay must not wait it out: the stall window is
        // far longer than this test would tolerate.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let exit = tokio::time::timeout(
            Duration::from_secs(1),
            run(context, stall_after(Duration::from_mins(1)), shutdown),
        )
        .await
        .expect("shutdown during a replay returns promptly");

        assert!(exit == FlusherExit::ShutDown);
        assert_replay_unflushed(&ready, &store).await;
    }

    /// A flush tick that is slower than the flush interval must not hide
    /// shutdown from the flusher.
    ///
    /// When such a tick ends, the next tick is already due. A loop that polls
    /// its ticker before shutdown then takes a new tick on every pass, and
    /// `BrokerHandle::shutdown`, which awaits this task, never returns. A
    /// broker whose index publish cannot reach a leader is in exactly that
    /// state while the rest of the cluster stops. The second row is a tick
    /// that never ends at all: shutdown must not wait for it either.
    ///
    /// Every PUT fails after its latency, so no tick moves the frontier and
    /// each one uploads again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_stops_at_shutdown_while_its_flushes_outlast_the_interval() {
        use krabka_object_store::fault::{
            FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp,
        };

        let cases = [
            (
                "each flush fails after more than one interval",
                OpFault::always(FaultKind::Throttled).with_latency(Duration::from_millis(20)),
                2,
            ),
            (
                "the flush in flight never ends",
                OpFault::stall(Duration::from_hours(1)),
                1,
            ),
        ];
        for (case, put_fault, puts_before_shutdown) in cases {
            let dir = tempdir().unwrap();
            let partitions = led_orders_partition(dir.path());
            let mut image = MetadataImage::new(Uuid::nil());
            image.apply(&MetadataRecord::V1Topic(
                crate::test_support::single_partition_topic("orders", Uuid::from_u128(11)),
            ));
            let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
            let store = Arc::new(FaultInjectingStore::new(
                Arc::new(InMemory::new()),
                FaultPolicy::none().with(StoreOp::Put, put_fault),
            ));
            let index = DisklessIndexLog::start(
                krabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
            )
            .await
            .unwrap();
            let shutdown = CancellationToken::new();
            let task = tokio::spawn(run(
                test_context!(
                    partitions,
                    image_rx,
                    Arc::clone(&store) as Arc<dyn ObjectStore>,
                    index
                ),
                FlushConfig {
                    interval: Duration::from_millis(1),
                    trim_safety_lag: None,
                    ..FlushConfig::default()
                },
                shutdown.clone(),
            ));

            tokio::time::timeout(Duration::from_secs(5), async {
                while store.attempts(StoreOp::Put) < puts_before_shutdown {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{case}: the flusher never uploaded"));
            shutdown.cancel();

            let exit = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .map(Result::unwrap);
            assert!(exit == Ok(FlusherExit::ShutDown), "{case}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_flushes_only_led_diskless_partitions_and_stops() {
        let dir = tempdir().unwrap();
        let led = orders_partition(dir.path());
        let follower = test_partition(
            dir.path(),
            crate::diskless::flusher::test_support::FlusherPartitionSetup {
                partition: krabka_ids::PartitionIndex(1),
                leader: NodeId(2),
                ..Default::default()
            },
        );
        let local = test_partition(
            dir.path(),
            crate::diskless::flusher::test_support::FlusherPartitionSetup {
                partition: krabka_ids::PartitionIndex(2),
                storage: crate::test_support::StorageMode::Local,
                ..Default::default()
            },
        );
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), led);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(1), follower);
        partitions.insert("orders".into(), krabka_ids::PartitionIndex(2), local);

        memory_flusher_dependencies!(topic_id, image, image_rx, store; 3);
        let index = test_index_log().await;
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            test_context!(partitions, image_rx, Arc::clone(&store), index),
            FlushConfig {
                interval: Duration::from_millis(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        wait_for_frontier(&cache, topic_id, 0, 3).await;
        stop_worker(&shutdown, task).await;

        assert!(cache.lock().await.flushed_frontier(topic_id, 1).is_none());
        assert!(cache.lock().await.flushed_frontier(topic_id, 2).is_none());
        let object_key = cache.lock().await.lookup(topic_id, 0, 0).unwrap().0;
        assert!(object_key.starts_with("diskless-wal/7/"));
        assert!(store.head(&Path::from(object_key)).await.is_ok());
    }
}
