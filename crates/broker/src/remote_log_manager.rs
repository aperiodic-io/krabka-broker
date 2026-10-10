//! `RemoteLogManager`: the KIP-405 tiered-storage sweep.
//!
//! Every `interval`, the manager walks the partition registry and sweeps each
//! partition whose topic has `remote.storage.enable=true`. Leadership splits
//! the sweep, the way Kafka splits it between `RemoteLogManager`'s leader and
//! follower tasks:
//!
//! - Where this broker leads the partition, it copies the sealed log segments
//!   that are not yet in the remote tier to a [`RemoteStorageManager`],
//!   recording each copy in a [`RemoteLogMetadataManager`]
//!   (`CopySegmentStarted` → `CopySegmentFinished`), and it enforces the
//!   topic's remote retention against that tier. Both are the leader's alone:
//!   one writer per partition owns the remote tier.
//! - **Every** replica, leader and follower alike, enforces local retention on
//!   its own disk. A follower's sealed segment is droppable for the same
//!   reason the leader's is -- the RLMM says the leader finished copying it --
//!   and the RLMM is shared, so the follower reads the same
//!   `CopySegmentFinished` set. Without this a follower would hold every
//!   segment it ever fetched until it was elected, and its disk would grow to
//!   the full `retention.ms` footprint rather than the `local.retention.ms`
//!   one.
//!
//! This is the copy path. Their own modules implement local-retention deletion
//! of copied segments and the remote read path on `Fetch`. The
//! remote-storage SPIs are blocking, so each copy and each delete
//! runs on the `tokio` blocking pool.
//!
//! One tick sweeps every partition concurrently, under two bounds that are
//! Kafka's two `RemoteLogManager` thread pools: a partition's copy holds a
//! slot of the copier bound (`remote.log.manager.copier.thread.pool.size`),
//! and its retention passes hold a slot of the expiration bound
//! (`remote.log.manager.expiration.thread.pool.size`). The bounds are what
//! keep a broker leading hundreds of tiered partitions from opening hundreds
//! of concurrent uploads; sweeping concurrently at all is what keeps one
//! partition whose object store stalls from holding tiering for every other
//! partition behind it, which a serial sweep did.
//!
//! A KFC-9 write freeze splits the sweep in two. The copy runs on a frozen
//! topic, because it adds a replica and takes nothing away, and tiering a
//! frozen topic is what a migration wants. Both retention passes stop, on
//! every replica, because each one removes data from the topic's log.

use std::sync::{Arc, atomic::Ordering};

use futures_util::future::join_all;
use krabka_metadata::NodeId;
use krabka_remote_storage::{RemoteLogMetadataManager, RemoteStorageManager, TopicIdPartition};
use krabka_units::{
    ByteSize, Time, bytes,
    convert::{ByteSizeExt as _, TimeExt as _},
    secs,
};
use krabka_verified::FreezeMutationKind;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    metrics::BrokerMetrics,
    partition::Partition,
    partition_registry::PartitionRegistry,
    time_util::now_ms,
};

mod archive;
mod copy;
mod copy_segment;
mod delete;
mod leader_epoch;
mod local_retention;
mod remote_retention;
mod rlmm;

/// KIP-950's disable-and-delete: erase what the tier holds for a partition
/// whose `remote.storage.enable` has gone `true -> false`, then raise its
/// global log start offset to the local one.
///
/// Kafka's `RemoteLogManager.stopPartitions(..., deleteRemoteLog = true)` does
/// both halves, and the order is what makes a crash between them safe: the
/// segments go first, so a floor that has not moved yet only means a fetch is
/// answered from the local log rather than from a segment that is gone.
///
/// Under [`ArchiveMode::WriteOnce`] the cascade clears the partition's remote
/// metadata and removes nothing from the archive, exactly as a `DeleteTopics`
/// cascade does: turning tiered storage off is a cluster operation, not an
/// instruction to erase a compliance archive. The floor still moves, because
/// what the broker will serve is what the metadata says it has.
async fn disable_and_delete_remote(
    partition: &Arc<Partition>,
    image: &krabka_metadata::MetadataImage,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    index_cache: &Arc<krabka_remote_storage::RemoteIndexCache>,
) {
    let Some(tp) = topic_partition(partition, image) else {
        return;
    };
    // Nothing to do once the tier is empty, which is every tick after the
    // first: the sweep runs on a timer, and this must not re-mark a partition
    // it already erased.
    match rlmm.list_remote_log_segments(&tp) {
        Ok(segments) if segments.is_empty() => return,
        Ok(_) => {}
        Err(error) => {
            warn!(topic = %tp.topic, partition = tp.partition, %error,
                  "remote-log-manager: failed to list segments for a disabled partition");
            return;
        }
    }
    cascade_remote_partition_delete(
        tp.clone(),
        broker_id,
        archive,
        Arc::clone(rsm),
        Arc::clone(rlmm),
        Arc::clone(index_cache),
    )
    .await;

    let local_start = {
        let log = partition.log.lock().expect("log mutex poisoned");
        log.local_log_start_offset()
    };
    let mut log = partition.log.lock().expect("log mutex poisoned");
    if let Err(error) = log.set_log_start_offset(local_start) {
        warn!(topic = %tp.topic, partition = tp.partition, %error,
              "remote-log-manager: failed to raise the log start offset after disabling tiering");
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
pub(crate) use self::copy::copy_eligible;
pub(crate) use self::{
    archive::ArchiveMode,
    copy::{CopyDelay, copy_eligible_delayed},
    delete::cascade_remote_partition_delete,
    local_retention::{LocalRetentionBounds, local_retention_pass},
    remote_retention::{LocalLogFootprint, RemoteRetentionBounds, remote_retention_pass},
};

/// Default cadence of the tiered-storage sweep (copy and retention passes).
const DEFAULT_TIERING_INTERVAL: Time = secs(30);

/// The floor of every size-budget walk in this module.
const NO_BYTES: ByteSize = bytes(0);

/// Tunables for [`run`].
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub(crate) struct RemoteLogManagerConfig {
    #[default(DEFAULT_TIERING_INTERVAL)]
    pub interval: Time,
    /// Deadline on one segment copy. See [`RemoteTier::copy_timeout`].
    #[default(crate::config::DEFAULT_REMOTE_COPY_TIMEOUT)]
    pub copy_timeout: Time,
    /// How wide one tick sweeps. See [`SweepConcurrency`].
    pub concurrency: SweepConcurrency,
    /// See [`RemoteTier::unstable_api_versions`].
    #[default(crate::api_catalog::UnstableApiVersions::Disabled)]
    pub unstable_api_versions: crate::api_catalog::UnstableApiVersions,
}

/// How many partition passes of each kind one tick may have in flight.
///
/// These are Kafka's two `RemoteLogManager` thread pools, which is why they
/// are two numbers and not one: `copier` bounds the partitions whose sealed
/// segments are being uploaded (`remote.log.manager.copier.thread.pool.size`),
/// `expiration` the partitions whose retention passes are running
/// (`remote.log.manager.expiration.thread.pool.size`). A partition takes one
/// slot for the whole of its pass, so the bound counts partitions in flight
/// rather than object-store calls in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub(crate) struct SweepConcurrency {
    #[default(crate::config::DEFAULT_REMOTE_COPIER_THREADS)]
    pub copier: usize,
    #[default(crate::config::DEFAULT_REMOTE_EXPIRATION_THREADS)]
    pub expiration: usize,
}

/// The two bounds of [`SweepConcurrency`], as the semaphores one tick hands
/// its partition passes.
///
/// A permit is taken around the pass itself and released the moment it ends,
/// so a partition waiting for a slot costs a parked future and nothing else:
/// the sweep still reaches every partition on the broker in one tick, however
/// slow the object store is for one of them.
struct SweepPermits {
    copier: Semaphore,
    expiration: Semaphore,
}

impl SweepPermits {
    fn new(concurrency: SweepConcurrency) -> Self {
        Self {
            copier: Semaphore::new(concurrency.copier),
            expiration: Semaphore::new(concurrency.expiration),
        }
    }

    async fn copier(&self) -> SemaphorePermit<'_> {
        self.copier
            .acquire()
            .await
            .expect("the sweep's copier semaphore is never closed")
    }

    async fn expiration(&self) -> SemaphorePermit<'_> {
        self.expiration
            .acquire()
            .await
            .expect("the sweep's expiration semaphore is never closed")
    }
}

pub(crate) struct RemoteLogManagerContext {
    pub partitions: Arc<PartitionRegistry>,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// Whether `rsm` is a write-once archive. It gates every delete this
    /// module would otherwise issue, and turns on manifest chaining.
    pub archive: ArchiveMode,
    pub rsm: Arc<dyn RemoteStorageManager>,
    pub rlmm: Arc<dyn RemoteLogMetadataManager>,
    /// The reader's index cache, so a segment this task deletes stops holding
    /// the cache's byte budget.
    pub index_cache: Arc<krabka_remote_storage::RemoteIndexCache>,
    pub metrics: BrokerMetrics,
    pub node_id: NodeId,
    pub broker_id: i32,
}

/// The remote tier a sweep writes through, and the counters that watch it.
///
/// Bundled because they travel together down the whole copy path and nothing
/// below picks one without the others: the archive mode decides whether a
/// manifest is sealed at all, the two managers do the sealing, the metrics are
/// where the outcome is recorded, and the index cache is what a delete has to
/// release so a segment nothing can read stops holding the read path's byte
/// budget.
pub(crate) struct RemoteTier<'a> {
    pub archive: ArchiveMode,
    pub rsm: &'a Arc<dyn RemoteStorageManager>,
    pub rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
    pub metrics: &'a BrokerMetrics,
    pub index_cache: &'a Arc<krabka_remote_storage::RemoteIndexCache>,
    /// How long one segment copy may take before the sweep abandons it.
    ///
    /// A partition's copy holds one of the sweep's copier slots for as long
    /// as it runs, so an object store that stalls would otherwise spend the
    /// whole copier bound on partitions that are moving no bytes. Past the
    /// deadline the copy is left in `CopySegmentStarted` -- which local
    /// retention refuses to delete against -- and the next tick retries the
    /// segment under a fresh id.
    pub copy_timeout: Time,
    /// Whether the broker runs Kafka trunk's tiered-storage behavior where it
    /// differs from the released 4.3.1: today, how local retention ages a
    /// segment whose newest record claims a timestamp in the future. See
    /// [`local_retention::local_retention_target`].
    pub unstable_api_versions: crate::api_catalog::UnstableApiVersions,
}

impl RemoteLogManagerContext {
    fn tier(&self, cfg: &RemoteLogManagerConfig) -> RemoteTier<'_> {
        RemoteTier {
            archive: self.archive,
            rsm: &self.rsm,
            rlmm: &self.rlmm,
            metrics: &self.metrics,
            index_cache: &self.index_cache,
            copy_timeout: cfg.copy_timeout,
            unstable_api_versions: cfg.unstable_api_versions,
        }
    }
}

/// Spawned task entry point. Ticks every `cfg.interval` until `shutdown`.
// task dependencies; bundling would obscure them
pub(crate) async fn run(
    context: RemoteLogManagerContext,
    cfg: RemoteLogManagerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval.to_std());
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("remote-log-manager task shutting down");
                return;
            }
        }
        tick_all(
            &context.partitions,
            &*context.controller,
            &context.tier(&cfg),
            context.node_id,
            context.broker_id,
            cfg.concurrency,
        )
        .await;
    }
}

/// One tick of the sweep: every partition the registry holds, swept
/// concurrently under `concurrency`'s two bounds.
///
/// Each partition's pass is one future and the tick ends when the last of
/// them does. Nothing is spawned, so the sweep is still the single task
/// [`run`] owns; what a serial walk cost was head-of-line blocking, where one
/// partition awaiting a slow object store held every partition behind it --
/// and, because local retention only evicts what the tier already holds,
/// filled their disks while it waited.
async fn tick_all(
    partitions: &PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    tier: &RemoteTier<'_>,
    node_id: NodeId,
    broker_id: i32,
    concurrency: SweepConcurrency,
) {
    // Snapshot first to avoid holding any registry guard across an await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    let image = controller.current_image();
    let permits = SweepPermits::new(concurrency);
    join_all(snapshot.into_iter().map(|partition| {
        tick_partition(PartitionSweep {
            partition,
            image: &image,
            tier,
            node_id,
            broker_id,
            permits: &permits,
        })
    }))
    .await;
}

/// One partition's place in a tick: the partition itself, the metadata image
/// the tick read once for all of them, the tier it writes through, and the
/// permits that bound how many such passes run at once.
struct PartitionSweep<'a> {
    partition: Arc<Partition>,
    image: &'a krabka_metadata::MetadataImage,
    tier: &'a RemoteTier<'a>,
    node_id: NodeId,
    broker_id: i32,
    permits: &'a SweepPermits,
}

/// Sweep one partition: the copy pass under a copier permit, then the two
/// retention passes under an expiration permit.
///
/// The permits are taken around the passes and not around this whole
/// function, so the work that waits for a slot is the work that talks to the
/// remote tier, not the log-lock snapshot that decides whether there is any.
async fn tick_partition(sweep: PartitionSweep<'_>) {
    let PartitionSweep {
        partition,
        image,
        tier,
        node_id,
        broker_id,
        permits,
    } = sweep;
    // Leadership decides which halves of the sweep run, not whether it
    // runs at all: the copy and the remote-retention pass are the
    // leader's, local retention is every replica's.
    let is_leader = partition.current_leader.load(Ordering::Relaxed) == node_id;
    // Read config, both readings of the global log start and the
    // sealed-segment list under one hold of the log lock, then drop it.
    // The floors ride along with the config because remote retention
    // measures a segment against them, and a value read under a second
    // lock could describe a different `DeleteRecords` than the segment
    // list does.
    // The high watermark is read before the lock, because reading it awaits.
    let high_watermark = partition.high_watermark().await;
    let (
        log_config,
        log_start_offset,
        (deleted_below, earliest_epoch),
        local_exports,
        local_log_size,
        lso,
    ) = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        let cfg = log.config_snapshot();
        (
            cfg,
            log.log_start_offset(),
            // What remote retention measures a segment against: the floor
            // somebody established, and the epoch the epoch cache was cut to
            // by it.
            (log.established_log_start(), log.log_start_epoch()),
            log.tierable_segments(),
            log.size(),
            // Kafka's `UnifiedLog.lastStableOffset`, which bounds what the
            // copy may upload.
            log.last_stable_offset(high_watermark),
        )
    };
    if !log_config.remote_storage_enable {
        // KIP-950: the alter paths refuse this flip unless
        // `remote.log.delete.on.disable` came with it, so a partition that
        // arrives here with the flag set is one an operator asked to have
        // erased. Everything else is an ordinary non-tiered partition.
        if is_leader && log_config.remote_tier.delete_on_disable {
            let _permit = permits.expiration().await;
            disable_and_delete_remote(
                &partition,
                image,
                broker_id,
                tier.archive,
                tier.rsm,
                tier.rlmm,
                tier.index_cache,
            )
            .await;
        }
        return;
    }
    // A sealed segment that ends below the global floor is deleted data,
    // whatever its file is still doing on disk. Kafka's copy task starts
    // at `max(logStartOffset, lastCopiedOffset)` for the same reason:
    // without this the log-start breach in `remote_retention_pass` would
    // delete the remote copy, the next tick would upload it again off the
    // local file, and the two would cycle for as long as the file sat
    // there.
    let exports: Vec<krabka_log::SegmentExport> = local_exports
        .iter()
        .filter(|export| export.last_offset >= log_start_offset)
        .cloned()
        .collect();
    let Some(tp) = topic_partition(&partition, image) else {
        // Topic vanished from the metadata image between snapshots; skip.
        return;
    };
    // Only the copy pass has nothing to do without sealed local segments.
    // The retention passes below still do: a partition whose whole local
    // log has already been evicted is exactly the one whose remote
    // segments age out, or fall below a `DeleteRecords` floor, with no
    // local segment left to notice it.
    // KIP-950 `remote.log.copy.disable`: the read-only tier. Nothing new
    // is copied, and every pass below still runs — retention over what the
    // tier already holds keeps working, which is what makes the state a
    // freeze of the copy rather than an abandonment of the data.
    if is_leader && !exports.is_empty() && !log_config.remote_tier.copy_disable {
        // Atomic stores the raw epoch; wrap for the remote-storage
        // metadata seam.
        let leader_epoch =
            krabka_ids::LeaderEpoch(partition.current_leader_epoch.load(Ordering::Acquire));
        let _permit = permits.copier().await;
        let delay = copy_delay(
            (image, node_id, &partition.topic),
            &log_config,
            tier.unstable_api_versions,
            (local_log_size, &local_exports),
        );
        copy_eligible_delayed(
            tier,
            &tp,
            (broker_id, leader_epoch),
            exports.clone(),
            (lso, delay),
        )
        .await;
    }
    // KFC-9: the copy above stays allowed on a frozen topic, and both
    // retention passes below stop. A freeze refuses every operation that
    // removes data from the topic's log, and a copy removes none: it adds
    // a replica, which is exactly what a migration out of a frozen topic
    // needs.
    //
    // This is a different question from `archive`, and it does not
    // contradict the reason that gate gives below. `archive` says the
    // remote tier cannot accept a delete, which leaves the local eviction
    // free precisely because it deletes nothing remote. A freeze says this
    // topic's log must not lose bytes anywhere, so it stops the local
    // eviction too.
    if matches!(
        resolve_freeze_mutation(image, &partition.topic, true, FreezeMutationKind::Retention,),
        FreezeMutationResolution::Frozen(_)
    ) {
        debug!(topic = %partition.topic, partition = tp.partition,
               "remote-log-manager: a write freeze holds both retention passes");
        return;
    }
    let _permit = permits.expiration().await;
    retention_passes(
        RetentionPasses {
            partition: &partition,
            tp: &tp,
            exports: &exports,
            log_config: &log_config,
            log_start_offset,
            deleted_below,
            earliest_epoch,
            local: LocalLogFootprint {
                sealed: &local_exports,
                size: local_log_size,
            },
            high_watermark,
            is_leader,
            broker_id,
        },
        tier,
    )
    .await;
}

/// The delay Kafka trunk's `remote.copy.lag.ms` and `remote.copy.lag.bytes`
/// (KIP-1241) put on the copy pass over `topic`, resolved against the image as
/// broker `node` sees it. Kafka 4.3.1 has neither key, so a broker not serving
/// trunk's keys (`unstable`) copies a segment as soon as it is sealed.
///
/// `local_log_size` is the whole local log and `sealed` its sealed segments, so
/// what they leave is the active segment.
fn copy_delay(
    (image, node, topic): (&krabka_metadata::MetadataImage, NodeId, &str),
    log_config: &krabka_log::LogConfig,
    unstable: crate::api_catalog::UnstableApiVersions,
    (local_log_size, sealed): (ByteSize, &[krabka_log::SegmentExport]),
) -> CopyDelay {
    if unstable != crate::api_catalog::UnstableApiVersions::Enabled {
        return CopyDelay::IMMEDIATE;
    }
    let sealed_bytes: u64 = sealed.iter().map(|ex| ex.size.bytes_u64()).sum();
    CopyDelay::resolve(
        crate::config_keys::resolve_remote_copy_lag(image, node, topic),
        log_config,
        now_ms(),
        local_log_size.bytes_u64().saturating_sub(sealed_bytes),
    )
}

/// What the two retention passes over one partition measure themselves
/// against: the segments and floors the tick read under a single log lock,
/// and whether this replica leads the partition.
struct RetentionPasses<'a> {
    partition: &'a Arc<Partition>,
    tp: &'a TopicIdPartition,
    exports: &'a [krabka_log::SegmentExport],
    log_config: &'a krabka_log::LogConfig,
    log_start_offset: krabka_log::Offset,
    deleted_below: Option<krabka_log::Offset>,
    earliest_epoch: Option<krabka_ids::LeaderEpoch>,
    local: LocalLogFootprint<'a>,
    /// The high watermark the tick read before it snapshotted the log.
    high_watermark: krabka_log::Offset,
    is_leader: bool,
    broker_id: i32,
}

/// Local retention on every replica, then remote retention on the leader.
async fn retention_passes(pass: RetentionPasses<'_>, tier: &RemoteTier<'_>) {
    let RetentionPasses {
        partition,
        tp,
        exports,
        log_config,
        log_start_offset,
        deleted_below,
        earliest_epoch,
        local,
        high_watermark,
        is_leader,
        broker_id,
    } = pass;
    // Local retention is deliberately not gated on `archive`: evicting a
    // local segment that the archive already holds is the whole point of
    // tiering, and it deletes nothing from the remote tier.
    local_retention_pass(
        tp,
        partition,
        exports,
        log_config,
        tier.rlmm,
        LocalRetentionBounds {
            now_ms: now_ms(),
            high_watermark,
        },
        tier.unstable_api_versions,
    );
    if !is_leader {
        return;
    }
    let outcome = remote_retention_pass(
        tp,
        broker_id,
        RemoteRetentionBounds {
            log_config,
            log_start_offset,
            deleted_below,
            earliest_epoch,
            now_ms: now_ms(),
            local,
        },
        tier,
    )
    .await;
    // The records the pass deleted are now in no tier at all, so the
    // partition's global floor follows them (Kafka's
    // `handleLogStartOffsetUpdate`). `set_log_start_offset` only moves
    // forward, so a `DeleteRecords` that landed while the pass ran
    // keeps the higher of the two floors.
    if let Some(new_start) = outcome.log_start {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        if let Err(error) = log.set_log_start_offset(new_start) {
            debug!(topic = %partition.topic, partition = tp.partition, %error,
                   "remote-log-manager: could not advance the log start after a remote delete");
        }
        // The local files under the new floor go with it. No reader
        // may ask for those offsets any more and no tier answers for
        // them, so leaving the files behind would only hold disk and
        // keep the copy filter above working around them on every
        // tick.
        if let Err(error) = log.delete_local_segments_through(new_start) {
            debug!(topic = %partition.topic, partition = tp.partition, %error,
                   "remote-log-manager: could not drop the local segments under the new floor");
        }
    }
}

fn topic_partition(
    partition: &Partition,
    image: &krabka_metadata::MetadataImage,
) -> Option<TopicIdPartition> {
    image.topic(&partition.topic).map(|topic| {
        TopicIdPartition::new(
            topic.topic_id,
            partition.topic.clone(),
            partition.index.get(),
        )
    })
}

#[cfg(test)]
pub(crate) use copy_segment::{ProducerSnapshotExport, export_epoch_map, export_segment_data};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use fixtures::{local_backends, partition_log, partition_log_fixture, sweep_counts};
    use krabka_ids::PartitionIndex;
    use krabka_log::{LogConfig, Offset};
    use krabka_metadata::{MetadataImage, MetadataRecord};
    use krabka_remote_storage::RemoteLogSegmentState;
    use krabka_units::millis;

    use super::{
        test_support::{fixed_source, rolled_tiered_partition_with_config, tier, tp},
        *,
    };
    use crate::remote_log_manager::test_support as fixtures;

    mod concurrency;
    mod epoch_cache;
    mod freeze;
    mod store_faults;

    fn image_with_orders_topic() -> MetadataImage {
        test_support::orders_image(1)
    }

    fn rolled_tiered_partition(log_dir: &std::path::Path) -> Arc<Partition> {
        rolled_tiered_partition_with_config(log_dir, fixtures::rolled_partition_config())
    }

    async fn wait_for_remote_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
                if listed.len() >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote-log-manager run loop did not copy expected segments");
    }

    #[tokio::test]
    async fn run_ticks_and_copies_eligible_segments() {
        let (log_dir, remote_dir) = fixtures::temporary_dirs();
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = fixtures::sealed_segment_count(&partition);
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".into(), PartitionIndex(0), partition);

        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(fixed_source(image_with_orders_topic()));
        let (rsm, rlmm) = local_backends(remote_dir.path());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            RemoteLogManagerContext {
                partitions,
                controller,
                archive: ArchiveMode::Mutable,
                rsm,
                rlmm: rlmm.clone(),
                index_cache: Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
                metrics: BrokerMetrics::new(),
                node_id: NodeId(1),
                broker_id: 1,
            },
            RemoteLogManagerConfig {
                interval: millis(10),
                ..RemoteLogManagerConfig::default()
            },
            shutdown.clone(),
        ));

        wait_for_remote_segments(&rlmm, export_count).await;
        shutdown.cancel();
        task.await.expect("remote-log-manager task panicked");

        test_support::assert_finished_segments(&rlmm, export_count);
    }

    #[tokio::test]
    async fn tick_all_copies_local_leader_remote_enabled_partition() {
        fixtures::rolled_partition_fixture!(log_dir, remote_dir, partitions, partition);
        let export_count = fixtures::sealed_segment_count(&partition);
        assert!(export_count >= 2, "test needs multiple sealed segments");
        fixtures::registered_sweep_fixture!(
            partitions, controller, rsm, rlmm, partition, remote_dir
        );

        test_support::assert_finished_segments(&rlmm, export_count);
    }

    // What one sweep left of a partition: the offset ranges the remote tier
    // finished copying, the sealed segments still on local disk, and the
    // local log start, which is what `ListOffsets(EARLIEST_LOCAL)` answers.
    #[derive(Debug, PartialEq, Eq)]
    struct Tiered {
        remote: Vec<(i64, i64)>,
        local_sealed: Vec<(Offset, Offset)>,
        local_log_start: Offset,
    }

    // A tiered partition that stops taking writes is tiered through its
    // active segment, as Kafka tiers it.
    //
    // Every record sits in the active segment, which neither `segment.bytes`
    // nor `segment.ms` will roll and the copy never uploads. Kafka's
    // retention check rolls it once it breaches `local.retention.ms`, the
    // next copy uploads the sealed records, and the next check drops them
    // from local disk. Kafka's `ShareConsumerDLQTieredStorageTest` waits on
    // exactly that: `ListOffsets(EARLIEST_LOCAL)` reaching the last record of
    // a partition nothing writes to any more.
    #[tokio::test]
    async fn two_sweeps_tier_an_idle_partition_through_its_active_segment() {
        partition_log_fixture!(
            log_dir,
            remote_dir,
            log,
            LogConfig {
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            }
        );
        for _ in 0..3 {
            log.append(&mut test_support::batch(2)).unwrap();
        }
        let partition =
            test_support::leading_partition_over(PartitionIndex(0), log_dir.path(), log);
        let partitions = PartitionRegistry::new();
        fixtures::register_fixture!(
            partitions,
            controller,
            rsm,
            rlmm,
            Arc::clone(&partition),
            remote_dir
        );

        for (sweep, expected) in [
            (
                "the first sweep rolls the breached active segment",
                Tiered {
                    remote: vec![],
                    local_sealed: vec![(Offset(0), Offset(5))],
                    local_log_start: Offset(0),
                },
            ),
            (
                "the second sweep copies it and drops it from local disk",
                Tiered {
                    remote: vec![(0, 5)],
                    local_sealed: vec![],
                    local_log_start: Offset(6),
                },
            ),
        ] {
            fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;

            // The next sweep copies what this one rolled once the rollover
            // flush lands, so wait for it, as a sweep interval would.
            let mut log = fixtures::partition_log_guard(&partition);
            log.sync().expect("flush rolled segments");
            let observed = Tiered {
                remote: crate::remote_log_manager::local_retention::finished_segment_ranges(
                    &rlmm.list_remote_log_segments(&tp()).unwrap(),
                ),
                local_sealed: log
                    .tierable_segments()
                    .iter()
                    .map(|export| (export.base_offset, export.last_offset))
                    .collect(),
                local_log_start: log.local_log_start_offset(),
            };
            check!(observed == expected, "{sweep}");
        }
    }

    /// Sweep `partition` once and count the segments the remote tier holds
    /// afterwards.
    async fn segments_copied_by_one_sweep(partition: Arc<Partition>) -> usize {
        segments_copied_by_one_sweep_of(
            partition,
            image_with_orders_topic(),
            crate::api_catalog::UnstableApiVersions::Disabled,
        )
        .await
    }

    /// [`segments_copied_by_one_sweep`] against a chosen metadata image, on a
    /// broker serving `unstable`.
    async fn segments_copied_by_one_sweep_of(
        partition: Arc<Partition>,
        image: MetadataImage,
        unstable: crate::api_catalog::UnstableApiVersions,
    ) -> usize {
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        partitions.insert("orders".into(), PartitionIndex(0), partition);
        let controller = fixed_source(image);
        let (rsm, rlmm) = local_backends(remote_dir.path());
        tick_all(
            &partitions,
            &controller,
            &RemoteTier {
                unstable_api_versions: unstable,
                ..tier(ArchiveMode::Mutable, &rsm, &rlmm)
            },
            NodeId(1),
            1,
            SweepConcurrency::default(),
        )
        .await;
        rlmm.list_remote_log_segments(&tp()).unwrap().len()
    }

    /// Kafka trunk's `remote.copy.lag.ms` and `remote.copy.lag.bytes` on a
    /// topic keep the sweep from copying a sealed segment that is neither old
    /// enough nor far enough behind, and a broker that does not serve trunk's
    /// keys never reads them. Either key can come from the cluster-wide
    /// `log.remote.copy.lag.*` broker default too.
    #[tokio::test]
    async fn tick_all_honours_the_remote_copy_lag_on_a_trunk_broker() {
        use krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID};

        use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

        let sealed = {
            let dir = tempfile::tempdir().unwrap();
            let partition = rolled_tiered_partition(dir.path());
            partition.log.lock().unwrap().tierable_segments().len()
        };
        assert!(sealed >= 2, "test needs several sealed segments");
        let topic_lag = |ms: &str, bytes: &str| {
            let mut image = image_with_orders_topic();
            image.apply(&MetadataRecord::V1TopicConfig(
                krabka_metadata::TopicConfigRecord {
                    topic: "orders".into(),
                    overrides: maplit::btreemap! {
                        "remote.copy.lag.ms".to_string() => ms.to_string(),
                        "remote.copy.lag.bytes".to_string() => bytes.to_string(),
                    },
                },
            ));
            image
        };
        let cluster_lag = |key: &str, value: &str| {
            let mut image = image_with_orders_topic();
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(value.into()),
            }));
            image
        };
        // A lag of `i64::MAX` ms is one no segment has aged into, and of a
        // gigabyte one no test log has grown into.
        for (label, image, unstable, want) in [
            (
                "no lag configured",
                image_with_orders_topic(),
                Enabled,
                sealed,
            ),
            (
                "a lag no segment has reached",
                topic_lag("9223372036854775807", "1000000000"),
                Enabled,
                0,
            ),
            (
                "the same lag on a broker that does not serve the keys",
                topic_lag("9223372036854775807", "1000000000"),
                Disabled,
                sealed,
            ),
            (
                "a zero time lag copies at once",
                topic_lag("0", "1000000000"),
                Enabled,
                sealed,
            ),
            (
                "a zero size lag copies at once",
                topic_lag("9223372036854775807", "0"),
                Enabled,
                sealed,
            ),
            (
                "a cluster-wide default holds the copy",
                cluster_lag("log.remote.copy.lag.ms", "9223372036854775807"),
                Enabled,
                0,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let partition = rolled_tiered_partition(dir.path());
            partition.replica_state.lock().await.hw = Offset(i64::MAX);

            let copied = segments_copied_by_one_sweep_of(partition, image, unstable).await;

            check!(copied == want, "{label}: copied {copied}, wanted {want}");
        }
    }

    /// Kafka's `RLMCopyTask.copyLogSegmentsToRemote` copies only segments that
    /// end below `log.lastStableOffset()`, and the remote tier must hold only
    /// committed, acknowledged records: after a leader change it would
    /// otherwise hold records the new leader never had. The high watermark is
    /// the first bound on it.
    #[tokio::test]
    async fn tick_all_copies_only_segments_below_the_high_watermark() {
        let log_dir = tempfile::tempdir().unwrap();
        let sealed_ends: Vec<Offset> = {
            let partition = rolled_tiered_partition(log_dir.path());
            let log = partition.log.lock().expect("partition log mutex poisoned");
            log.tierable_segments()
                .iter()
                .map(|export| export.last_offset)
                .collect()
        };
        assert!(sealed_ends.len() >= 3, "test needs several sealed segments");

        // `(label, high watermark, sealed segments copied)`.
        let cases = [
            ("nothing is replicated yet", Offset(0), 0),
            (
                "the high watermark is inside the first segment",
                sealed_ends[0],
                0,
            ),
            (
                "the high watermark is at the first segment's end",
                sealed_ends[0] + 1,
                1,
            ),
            (
                "the high watermark is inside the third segment",
                sealed_ends[2],
                2,
            ),
            (
                "everything is replicated",
                Offset(i64::MAX),
                sealed_ends.len(),
            ),
        ];
        for (label, high_watermark, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let partition = rolled_tiered_partition(dir.path());
            partition.replica_state.lock().await.hw = high_watermark;

            let copied = segments_copied_by_one_sweep(partition).await;

            check!(copied == want, "{label}: copied {copied}, wanted {want}");
        }
    }

    /// The other bound on `lastStableOffset` is the first offset of an open
    /// transaction: nothing at or after it is tiered while the transaction has
    /// no marker, even when the high watermark is past it.
    #[tokio::test]
    async fn tick_all_holds_the_copy_at_an_open_transaction() {
        let log_dir = tempfile::tempdir().unwrap();
        let mut log = partition_log(
            log_dir.path(),
            PartitionIndex(0),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                retention: None,
                retention_size: None,
                ..LogConfig::default()
            },
        );
        for i in 0..12 {
            let mut batch = test_support::batch(2);
            if i == 5 {
                // A transaction that opens here and is never closed.
                batch.producer_id = 7;
                batch.producer_epoch = 0;
                batch.base_sequence = 0;
                batch.attributes =
                    krabka_protocol::records::Attributes::default().with_transactional(true);
            }
            log.append(&mut batch).unwrap();
        }
        log.sync().unwrap();
        let first_unstable = log.last_stable_offset(Offset(i64::MAX));
        let sealed_ends: Vec<Offset> = log
            .tierable_segments()
            .iter()
            .map(|export| export.last_offset)
            .collect();
        let want = sealed_ends
            .iter()
            .filter(|end| **end < first_unstable)
            .count();
        assert!(
            0 < want && want < sealed_ends.len(),
            "the open transaction has to split the sealed segments: first unstable offset \
             {first_unstable:?}, sealed segments ending at {sealed_ends:?}"
        );
        let partition =
            test_support::leading_partition_over(PartitionIndex(0), log_dir.path(), log);
        // Everything is replicated, so only the transaction holds the copy.
        partition.replica_state.lock().await.hw = Offset(i64::MAX);

        let copied = segments_copied_by_one_sweep(partition).await;

        check!(copied == want);
    }

    /// A `DeleteRecords` floor takes the segments under it out of the copy
    /// pass, and keeps them out on every later tick.
    ///
    /// Without the filter the log-start breach in `remote_retention_pass`
    /// deletes the remote copy of a below-floor segment, the next tick uploads
    /// it again off the local file the floor has not removed, and the two
    /// cycle for as long as the file is there -- unbounded copies, deletes and
    /// metadata for records nobody may read.
    #[tokio::test]
    async fn tick_all_never_copies_a_segment_under_the_log_start() {
        fixtures::rolled_partition_fixture!(log_dir, remote_dir, partitions, partition);
        // A floor at the second sealed segment's base, with every local file
        // still on disk: `set_log_start_offset` moves the pointer and deletes
        // nothing, which is the state a remote-retention advance leaves.
        let (floor, export_count) = {
            let mut log = fixtures::partition_log_guard(&partition);
            let exports = log.tierable_segments();
            assert!(exports.len() >= 3, "test needs several sealed segments");
            let floor = exports[1].base_offset;
            log.set_log_start_offset(floor).expect("move the log start");
            (floor, exports.len())
        };
        fixtures::register_fixture!(
            partitions,
            controller,
            rsm,
            rlmm,
            Arc::clone(&partition),
            remote_dir
        );

        // Two sweeps: the second is where a copy/delete cycle would show.
        for sweep in 1..=2 {
            fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;

            let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
            assert!(
                listed.len() == export_count - 1,
                "sweep {sweep}: every segment but the one under the floor"
            );
            assert!(
                listed.iter().all(|md| md.end_offset() >= floor.0),
                "sweep {sweep}: nothing under the floor was copied"
            );
        }
    }

    /// What one [`tick_all`] over a follower replica of `orders` left behind:
    /// what the remote tier holds, and what is still sealed on local disk.
    struct FollowerSweep {
        sealed_before: usize,
        remote_finished: usize,
        local_sealed_after: usize,
    }

    /// Drive one sweep over an `orders` partition that node 2 leads, with this
    /// broker (node 1) hosting a follower replica of it. The topic's local
    /// budget is zero, so every segment the RLMM reports copied is past the
    /// local-retention window the moment the sweep sees it.
    ///
    /// When `leader_already_copied`, the copy the real leader would have run
    /// is run first against the same RSM and RLMM. That is what a follower
    /// meets in a cluster: the metadata is shared, so the follower reads the
    /// leader's `CopySegmentFinished` set off `__remote_log_metadata` without
    /// ever having copied a byte itself.
    async fn follower_sweep(leader_already_copied: bool) -> FollowerSweep {
        fixtures::rolled_partition_fixture!(
            log_dir,
            remote_dir,
            partitions,
            partition,
            LogConfig {
                local_retention_size: Some(NO_BYTES),
                ..fixtures::rolled_partition_config()
            }
        );
        partition.current_leader.store(2, Ordering::Relaxed);
        let exports = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments();
        let sealed_before = exports.len();
        fixtures::register_fixture!(
            partitions,
            controller,
            rsm,
            rlmm,
            Arc::clone(&partition),
            remote_dir
        );
        if leader_already_copied {
            copy_eligible(
                &tier(ArchiveMode::Mutable, &rsm, &rlmm),
                &tp(),
                2,
                krabka_ids::LeaderEpoch(0),
                exports,
            )
            .await;
        }

        fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;

        let (remote_finished, local_sealed_after) = sweep_counts(&partition, &rlmm);
        FollowerSweep {
            sealed_before,
            remote_finished,
            local_sealed_after,
        }
    }

    #[tokio::test]
    async fn tick_all_on_a_follower_evicts_locally_and_never_copies() {
        // KIP-405 splits the sweep by leadership, not by replica: the copy is
        // the leader's alone, and local retention runs on every replica. A
        // follower that skipped it would hold `retention.ms` worth of disk
        // while the leader held `local.retention.ms` worth.
        let cases = [
            ("a follower whose leader has not copied yet", false),
            ("a follower whose leader already copied", true),
        ];
        for (label, leader_already_copied) in cases {
            let outcome = follower_sweep(leader_already_copied).await;

            check!(
                outcome.sealed_before >= 2,
                "{label}: the fixture needs multiple sealed segments"
            );
            // The follower never adds to the remote tier: whatever is there
            // is what the leader's copy put there.
            let want_remote = if leader_already_copied {
                outcome.sealed_before
            } else {
                0
            };
            check!(
                outcome.remote_finished == want_remote,
                "{label}: segments in the remote tier after the sweep"
            );
            // ...but it does drop its own copy of what the leader finished.
            // The zero budget covers its active segment too, so it rolls that
            // segment, as Kafka's `deletableSegments` does on every replica,
            // and the rolled segment is the one left.
            let want_local = if leader_already_copied {
                1
            } else {
                outcome.sealed_before
            };
            check!(
                outcome.local_sealed_after == want_local,
                "{label}: sealed segments still on the follower's disk"
            );
        }
    }

    #[tokio::test]
    async fn tick_all_skips_remote_storage_disabled_partition() {
        fixtures::rolled_partition_fixture!(
            log_dir,
            remote_dir,
            partitions,
            partition,
            LogConfig {
                remote_storage_enable: false,
                ..fixtures::rolled_partition_config()
            }
        );
        fixtures::registered_sweep_fixture!(
            partitions, controller, rsm, rlmm, partition, remote_dir
        );

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    /// KIP-950 `remote.log.copy.disable`: the read-only tier copies nothing
    /// new. The same partition with the flag off copies its sealed segments,
    /// so the difference is the flag and not the fixture.
    #[tokio::test]
    async fn copy_disable_stops_the_copy_and_nothing_else() {
        for (case, copy_disable, want_segments) in
            [("copying", false, true), ("copy disabled", true, false)]
        {
            fixtures::rolled_partition_fixture!(
                log_dir,
                remote_dir,
                partitions,
                partition,
                LogConfig {
                    remote_tier: krabka_log::RemoteTierFlags {
                        copy_disable,
                        delete_on_disable: false,
                    },
                    ..fixtures::rolled_partition_config()
                }
            );
            fixtures::registered_sweep_fixture!(
                partitions, controller, rsm, rlmm, partition, remote_dir
            );

            check!(
                !rlmm.list_remote_log_segments(&tp()).unwrap().is_empty() == want_segments,
                "{case}"
            );
        }
    }

    /// KIP-950's disable-and-delete: a partition whose `remote.storage.enable`
    /// went `true -> false` with `remote.log.delete.on.disable=true` loses its
    /// remote segments, and its global log start offset rises to the local
    /// one, so no fetch is left pointing at a segment that is gone.
    #[tokio::test]
    async fn disabling_with_delete_on_disable_erases_the_tier() {
        fixtures::rolled_partition_fixture!(log_dir, remote_dir, partitions, partition);
        fixtures::register_fixture!(
            partitions,
            controller,
            rsm,
            rlmm,
            Arc::clone(&partition),
            remote_dir
        );

        // Copy first: the delete below has to have something to erase.
        fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;
        assert!(!rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());

        // The alter an operator makes, as the partition sees it.
        {
            let log = partition.log.lock().unwrap();
            let mut config = log.config_snapshot();
            config.remote_storage_enable = false;
            config.remote_tier.delete_on_disable = true;
            log.set_config(config);
        }
        let local_start = partition.log.lock().unwrap().local_log_start_offset();

        fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;

        check!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
        check!(partition.log.lock().unwrap().log_start_offset() == local_start);
    }
}
