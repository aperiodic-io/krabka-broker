//! Fixtures the remote-log-manager unit tests share: stand-in remote-storage
//! and metadata backends, and builders for rolled logs, tiered partitions and
//! synthetic segment exports.

use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{Arc, Mutex, atomic::Ordering},
};

use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_log::{Log, LogConfig, Offset, SegmentExport};
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::{
    CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
    LogSegmentData, ObjectEntry, RemoteLogMetadataManager, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
    RemoteStorageError, RemoteStorageManager, Sha256Digest, TopicIdPartition, WormArchiver,
};
use krabka_units::{ByteSize, bytes};
use uuid::Uuid;

use crate::{
    metrics::BrokerMetrics,
    partition::Partition,
    remote_log_manager::{ArchiveMode, RemoteTier},
    test_support::{FakeMetadataSource, UnixMillis},
};

/// A `BrokerMetrics` shared by every tier a unit test builds.
///
/// One registry serves them all because these tests assert on the archive,
/// not on the counters; a test that reads a counter builds its own metrics
/// and its own [`RemoteTier`] so nothing else can move it.
pub(crate) fn shared_test_metrics() -> &'static BrokerMetrics {
    static METRICS: std::sync::OnceLock<BrokerMetrics> = std::sync::OnceLock::new();
    METRICS.get_or_init(BrokerMetrics::new)
}

/// The index cache every unit-test tier shares. It is disabled: these tests
/// sweep the archive, and a cache that stores nothing cannot make one of them
/// pass by holding bytes a later assertion expects to be gone.
fn shared_test_index_cache() -> &'static Arc<krabka_remote_storage::RemoteIndexCache> {
    static CACHE: std::sync::OnceLock<Arc<krabka_remote_storage::RemoteIndexCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()))
}

/// The copy deadline a unit test sweeps under when it is not the thing being
/// tested. It is long enough that no in-process fake reaches it.
pub(crate) const TEST_COPY_TIMEOUT: krabka_units::Time = krabka_units::secs(60);

/// The tier a unit test sweeps, wired to the shared metrics.
pub(crate) fn tier<'a>(
    archive: ArchiveMode,
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
) -> RemoteTier<'a> {
    configured_tier(
        rsm,
        rlmm,
        TierSetup {
            archive,
            ..Default::default()
        },
    )
}

/// Policies and resources for a test tier. Counter assertions override the shared metrics.
#[derive(Clone, Copy)]
pub struct TierSetup<'a> {
    pub archive: ArchiveMode,
    pub copy_timeout: krabka_units::Time,
    pub unstable_api_versions: crate::api_catalog::UnstableApiVersions,
    pub metrics: &'a BrokerMetrics,
    pub index_cache: &'a Arc<krabka_remote_storage::RemoteIndexCache>,
}

impl Default for TierSetup<'_> {
    fn default() -> Self {
        Self {
            archive: ArchiveMode::Mutable,
            copy_timeout: TEST_COPY_TIMEOUT,
            unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
            metrics: shared_test_metrics(),
            index_cache: shared_test_index_cache(),
        }
    }
}

/// A tier borrowing its backends and the selected test resources.
pub fn configured_tier<'a>(
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
    setup: TierSetup<'a>,
) -> RemoteTier<'a> {
    RemoteTier {
        archive: setup.archive,
        rsm,
        rlmm,
        metrics: setup.metrics,
        index_cache: setup.index_cache,
        unstable_api_versions: setup.unstable_api_versions,
        copy_timeout: setup.copy_timeout,
    }
}

/// Fresh mutable storage and metadata backends for one test's temporary directory.
pub fn local_backends(
    remote_dir: &std::path::Path,
) -> (
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
) {
    (
        Arc::new(LocalTieredStorage::new(remote_dir)),
        Arc::new(InmemoryRemoteLogMetadataManager::new()),
    )
}

/// Stand-in backends that never retain readable segment or index bytes.
/// Copy and deletion behavior remain explicit in each implementation.
macro_rules! missing_remote_reads {
    (delete_ok) => {
        crate::remote_log_manager::test_support::missing_remote_reads!();
        fn delete_log_segment_data(
            &self,
            _metadata: &krabka_remote_storage::RemoteLogSegmentMetadata,
        ) -> Result<(), krabka_remote_storage::RemoteStorageError> {
            Ok(())
        }
    };
    () => {
        fn fetch_log_segment(
            &self,
            metadata: &krabka_remote_storage::RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, krabka_remote_storage::RemoteStorageError> {
            Err(krabka_remote_storage::RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &krabka_remote_storage::RemoteLogSegmentMetadata,
            _index_type: krabka_remote_storage::IndexType,
        ) -> Result<Vec<u8>, krabka_remote_storage::RemoteStorageError> {
            Err(krabka_remote_storage::RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
    };
}
pub(crate) use missing_remote_reads;

/// Copy every supplied fixture segment as broker 1 in leader epoch 0.
/// The assertion stays at the copy checkpoint, before a retention pass runs.
pub async fn copy_all_exports(tier: &RemoteTier<'_>, exports: &[SegmentExport]) {
    let copied = super::copy_eligible(tier, &tp(), 1, LeaderEpoch(0), exports.to_vec()).await;
    assert2::assert!(copied == exports.len());
}

/// One default-concurrency sweep by the fixture broker (node and broker ID 1).
pub async fn sweep_once(
    partitions: &crate::partition_registry::PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    tier: &RemoteTier<'_>,
) {
    super::tick_all(
        partitions,
        controller,
        tier,
        NodeId(1),
        1,
        super::SweepConcurrency::default(),
    )
    .await;
}

/// The sealed exports and their config from one hold of the partition's log lock.
pub fn partition_snapshot(partition: &Partition) -> (Vec<SegmentExport>, LogConfig) {
    let log = partition.log.lock().expect("partition log mutex poisoned");
    (log.tierable_segments(), log.config_snapshot())
}

/// Remote and local counts after a sweep; the local count waits for rollover flush.
pub fn sweep_counts(
    partition: &Partition,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> (usize, usize) {
    let remote_finished = rlmm
        .list_remote_log_segments(&tp())
        .unwrap()
        .iter()
        .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        .count();
    let mut log = partition.log.lock().expect("partition log mutex poisoned");
    log.sync().expect("flush rolled segments");
    (remote_finished, log.tierable_segments().len())
}

/// Fresh mutable backends with every supplied fixture export successfully archived.
pub async fn archived_backends(
    remote_dir: &std::path::Path,
    exports: &[SegmentExport],
) -> (
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
) {
    let (rsm, rlmm) = local_backends(remote_dir);
    copy_all_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports).await;
    (rsm, rlmm)
}

/// A stand-in write-once archive. Every copy seals a real (unsigned) WORM
/// manifest over the segment's leader-epoch bytes, keeps that manifest in
/// memory, and returns the chain receipt the backend would.
///
/// Its delete **panics**. A write-once backend refuses every delete, so a
/// broker that reaches one has already lost: the panic turns that into a
/// test failure instead of a warning nobody reads.
pub struct FakeWormArchive {
    archiver: WormArchiver,
    manifests: Mutex<BTreeMap<Uuid, Vec<u8>>>,
}

impl FakeWormArchive {
    pub fn new() -> Self {
        Self {
            archiver: WormArchiver::new(None),
            manifests: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn archived_segments(&self) -> usize {
        self.manifests
            .lock()
            .expect("archived-manifest mutex poisoned")
            .len()
    }
}

impl RemoteStorageManager for FakeWormArchive {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let body = data.leader_epoch_index.clone();
        let entry = ObjectEntry {
            suffix: IndexType::LeaderEpoch.suffix().to_string(),
            key: format!("{}.leader-epoch", metadata.remote_log_segment_id().id),
            size_bytes: u64::try_from(body.len()).expect("test object fits in u64"),
            sha256: Sha256Digest::of(&body),
            e_tag: None,
            version_id: None,
            create_precondition: true,
        };
        let sealed = self.archiver.seal(metadata, vec![entry])?;
        self.manifests
            .lock()
            .expect("archived-manifest mutex poisoned")
            .insert(metadata.remote_log_segment_id().id, sealed.bytes.to_vec());
        Ok(Some(sealed.receipt.to_custom_metadata()))
    }
    missing_remote_reads!();
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        panic!(
            "a write-once archive must never reach an RSM delete (segment {})",
            metadata.remote_log_segment_id().id
        );
    }
}

/// A metadata source over `image`, with node 1 reported as the controller
/// leader so that the sweep treats this broker as the partition leader.
pub fn fixed_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(Some(NodeId(1)))
        .build()
}

pub fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
}

pub fn batch(n: i32) -> RecordBatch {
    crate::test_support::keyed_records_batch(crate::test_support::KeyedBatchSetup {
        records: crate::test_support::KeyedRecordCount(n),
        ..Default::default()
    })
}

/// Build a log rolled into several sealed segments under `dir`.
pub fn rolled_log(dir: &std::path::Path) -> Log {
    let mut log = Log::open(
        dir,
        LogConfig {
            segment_size: bytes(256), // tiny so we roll fast
            ..LogConfig::default()
        },
    )
    .unwrap();
    crate::remote_log_manager::test_support::append_fixture_batches(&mut log, 12);
    log.sync().unwrap();
    log
}

pub fn rolled_tiered_partition_with_config(
    log_dir: &std::path::Path,
    config: LogConfig,
) -> Arc<Partition> {
    rolled_tiered_partition_at(PartitionIndex(0), log_dir, config)
}

/// The same fixture at a chosen partition index, for the suites that sweep
/// more than one partition of `orders` in a tick. The index is the
/// partition's identity everywhere the sweep looks -- the directory it opens,
/// the `TopicIdPartition` it copies under -- so it cannot be patched in
/// afterwards.
pub fn rolled_tiered_partition_at(
    index: PartitionIndex,
    log_dir: &std::path::Path,
    config: LogConfig,
) -> Arc<Partition> {
    let mut log = crate::remote_log_manager::test_support::partition_log(log_dir, index, config);
    crate::remote_log_manager::test_support::append_fixture_batches(&mut log, 12);
    log.sync().unwrap();
    leading_partition_over(index, log_dir, log)
}

/// A partition of `orders` at `index` that this broker (node 1, epoch 0)
/// leads, over a log the caller has already written. The log must live in the
/// partition directory under `log_dir`.
pub fn leading_partition_over(
    index: PartitionIndex,
    log_dir: &std::path::Path,
    log: Log,
) -> Arc<Partition> {
    let log_end = log.log_end_offset();
    let partition = crate::broker::spawn_partition(
        "orders".to_string(),
        index,
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    partition.current_leader.store(1, Ordering::Relaxed);
    partition.current_leader_epoch.store(0, Ordering::Release);
    // A sole replica has replicated everything it wrote: the high watermark
    // is the log end, so nothing the fixture wrote is withheld from the tier
    // as uncommitted.
    partition
        .replica_state
        .try_lock()
        .expect("nothing else holds a partition that was just spawned")
        .hw = log_end;
    partition
}

/// A sealed-segment export with no files behind it, whose file was last
/// modified at its newest record's timestamp.
#[derive(krabka_macros::FieldDefaults)]
pub struct SegmentExportSetup {
    #[default(Offset(0)..=Offset(9))]
    pub bounds: RangeInclusive<Offset>,
    #[default(UnixMillis(100))]
    pub timestamp: UnixMillis,
    #[default(bytes(64))]
    pub size: ByteSize,
}

pub fn synth_export(setup: SegmentExportSetup) -> SegmentExport {
    let SegmentExportSetup {
        bounds,
        timestamp,
        size,
    } = setup;
    let (base_offset, last_offset) = bounds.into_inner();
    SegmentExport {
        base_offset,
        last_offset,
        max_timestamp: timestamp.0,
        last_modified_ms: timestamp.0,
        size,
        log_path: std::path::PathBuf::new(),
        offset_index_path: std::path::PathBuf::new(),
        time_index_path: std::path::PathBuf::new(),
        transaction_index_path: None,
        producer_snapshot_path: std::path::PathBuf::new(),
        leader_epochs: Vec::new(),
    }
}

/// Add a `CopySegmentStarted` record and leave it there, the way a copy
/// that died after the metadata write but before the backend answered
/// does. Returns the segment's UUID.
pub fn stuck_started_segment(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    id: u128,
    base: i64,
) -> Uuid {
    let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(id));
    let md = RemoteLogSegmentMetadata::new(
        segment_id.clone(),
        base,
        base + 9,
        100,
        1,
        100,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            100,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => base},
        ),
    )
    .unwrap();
    rlmm.add_remote_log_segment_metadata(md).unwrap();
    segment_id.id
}

/// Put `count` `CopySegmentFinished` segments into `rlmm`, ten offsets
/// apart, without going near an RSM.
pub fn seed_finished_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, count: usize) {
    for i in 0..count {
        let index = u128::try_from(i).expect("test segment count fits in u128");
        let base = i64::try_from(i).expect("test segment count fits in i64") * 10;
        let id = 0x5000 + index;
        stuck_started_segment(rlmm, id, base);
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 100,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
    }
}

/// A rolled log and its sealed exports, retaining both directory guards in the caller.
macro_rules! rolled_log_fixture {
    ($local:ident, $remote:ident, $log:ident, $exports:ident) => {
        let $local = tempfile::tempdir().unwrap();
        let $remote = tempfile::tempdir().unwrap();
        let $log = crate::remote_log_manager::test_support::rolled_log($local.path());
        let $exports = $log.tierable_segments();
    };
}
pub(crate) use rolled_log_fixture;

/// The fixture's orders topic in metadata image 9, with an explicit partition count.
pub fn orders_image(partitions: i32) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(9));
    image.apply(&krabka_metadata::MetadataRecord::V1Topic(
        krabka_metadata::TopicRecord {
            name: "orders".into(),
            topic_id: tp().topic_id,
            partitions,
            replication_factor: 1,
        },
    ));
    image
}

/// The ordinary fixture copier: broker 1, epoch 0, and the caller's full export set.
pub async fn copy_exports(tier: &RemoteTier<'_>, exports: Vec<SegmentExport>) -> usize {
    super::copy_eligible(tier, &tp(), 1, LeaderEpoch(0), exports).await
}

/// Three consecutive ten-offset fixture segments, each with 64 bytes of data.
pub fn three_exports() -> Vec<SegmentExport> {
    vec![
        synth_export(SegmentExportSetup::default()),
        synth_export(SegmentExportSetup {
            bounds: Offset(10)..=Offset(19),
            timestamp: UnixMillis(200),
            ..Default::default()
        }),
        synth_export(SegmentExportSetup {
            bounds: Offset(20)..=Offset(29),
            timestamp: UnixMillis(300),
            ..Default::default()
        }),
    ]
}

/// Open the real orders-partition directory under the supplied log root.
pub fn partition_log(log_dir: &std::path::Path, index: PartitionIndex, config: LogConfig) -> Log {
    let part_dir = crate::log_dir::partition_dir(log_dir, "orders", index.get());
    std::fs::create_dir_all(&part_dir).unwrap();
    Log::open(&part_dir, config).unwrap()
}

/// Retain both temporary directories at the caller while opening its configured partition log.
macro_rules! partition_log_fixture {
    ($local:ident, $remote:ident, $log:ident, $config:expr) => {
        let $local = tempfile::tempdir().unwrap();
        let $remote = tempfile::tempdir().unwrap();
        let mut $log = crate::remote_log_manager::test_support::partition_log(
            $local.path(),
            krabka_ids::PartitionIndex(0),
            $config,
        );
    };
}
pub(crate) use partition_log_fixture;

/// Bind a fixture partition to registry/metadata/storage without moving the caller's guards.
macro_rules! register_fixture {
    ($partitions:ident, $controller:ident, $rsm:ident, $rlmm:ident, $partition:expr, $remote:ident) => {
        $partitions.insert("orders".into(), krabka_ids::PartitionIndex(0), $partition);
        let $controller = crate::remote_log_manager::test_support::fixed_source(
            crate::remote_log_manager::test_support::orders_image(1),
        );
        let ($rsm, $rlmm) = crate::remote_log_manager::test_support::local_backends($remote.path());
    };
}
pub(crate) use register_fixture;

/// A metadata backend and per-test metric/cache resources, in their original declaration order.
macro_rules! owned_tier_resources {
    ($rlmm:ident, $metrics:ident, $cache:ident) => {
        let $rlmm: std::sync::Arc<dyn krabka_remote_storage::RemoteLogMetadataManager> =
            std::sync::Arc::new(krabka_remote_storage::InmemoryRemoteLogMetadataManager::new());
        let $metrics = crate::metrics::BrokerMetrics::new();
        let $cache = std::sync::Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
    };
}
pub(crate) use owned_tier_resources;

/// The local and remote guards, declared in the same order as the fixture call sites.
pub fn temporary_dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    let local = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    (local, remote)
}

pub fn orders_label() -> crate::metrics::TopicLabel {
    crate::metrics::TopicLabel {
        topic: Arc::from(tp().topic.as_str()),
    }
}

pub fn append_fixture_batches(log: &mut Log, count: u32) {
    for _ in 0..count {
        let mut records = batch(2);
        log.append(&mut records).unwrap();
    }
}

pub fn multiple_segment_snapshot(partition: &Partition) -> (Vec<SegmentExport>, LogConfig) {
    let snapshot = partition_snapshot(partition);
    assert2::assert!(snapshot.0.len() >= 2, "test needs multiple sealed segments");
    snapshot
}

pub fn partition_log_guard(partition: &Partition) -> std::sync::MutexGuard<'_, Log> {
    partition.log.lock().expect("partition log mutex poisoned")
}

pub fn sealed_segment_count(partition: &Partition) -> usize {
    partition_log_guard(partition).tierable_segments().len()
}

pub async fn local_retention_at(
    partition: &Partition,
    exports: &[SegmentExport],
    config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    policy: (crate::api_catalog::UnstableApiVersions, i64),
) -> usize {
    super::local_retention_pass(
        &tp(),
        partition,
        exports,
        config,
        rlmm,
        super::LocalRetentionBounds {
            now_ms: policy.1,
            high_watermark: partition.high_watermark().await,
        },
        policy.0,
    )
}

pub async fn sweep_mutable(
    partitions: &crate::partition_registry::PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) {
    sweep_once(
        partitions,
        controller,
        &tier(ArchiveMode::Mutable, rsm, rlmm),
    )
    .await;
}

pub fn assert_finished_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, expected: usize) {
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert2::assert!(listed.len() == expected);
    assert2::assert!(
        listed
            .iter()
            .all(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished)
    );
}

pub fn check_partitions_copied(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    partitions: impl IntoIterator<Item = i32>,
) {
    for index in partitions {
        let listed = partition_segments(rlmm, index);
        assert2::check!(
            listed
                .iter()
                .any(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished),
            "partition {index} finished no copy"
        );
    }
}

pub fn write_once_backends() -> (
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
) {
    (
        Arc::new(FakeWormArchive::new()),
        Arc::new(InmemoryRemoteLogMetadataManager::new()),
    )
}

pub fn two_exports() -> Vec<SegmentExport> {
    two_exports_with_size(bytes(64))
}

/// Consecutive ten-offset exports sharing the caller's segment size.
pub fn two_exports_with_size(size: ByteSize) -> Vec<SegmentExport> {
    vec![
        synth_export(SegmentExportSetup {
            size,
            ..Default::default()
        }),
        synth_export(SegmentExportSetup {
            bounds: Offset(10)..=Offset(19),
            timestamp: UnixMillis(200),
            size,
        }),
    ]
}

pub fn partition_segments(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    index: i32,
) -> Vec<RemoteLogSegmentMetadata> {
    rlmm.list_remote_log_segments(&TopicIdPartition::new(tp().topic_id, "orders", index))
        .unwrap()
}

pub fn in_memory_metadata() -> Arc<dyn RemoteLogMetadataManager> {
    Arc::new(InmemoryRemoteLogMetadataManager::new())
}

pub fn check_one_segment_state(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    expected_state: RemoteLogSegmentState,
) -> Vec<RemoteLogSegmentMetadata> {
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert2::check!(listed.len() == 1);
    assert2::check!(listed[0].state() == expected_state);
    listed
}

/// The default rolled topic retains records in both tiers without a time or size limit.
pub fn rolled_partition_config() -> LogConfig {
    LogConfig {
        segment_size: krabka_units::bytes(256),
        remote_storage_enable: true,
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    }
}

/// Retain the log-directory guards while creating the registry and its partition.
macro_rules! rolled_partition_fixture {
    ($local:ident, $remote:ident, $registry:ident, $partition:ident) => {
        crate::remote_log_manager::test_support::rolled_partition_fixture!(
            $local,
            $remote,
            $registry,
            $partition,
            crate::remote_log_manager::test_support::rolled_partition_config()
        );
    };
    ($local:ident, $remote:ident, $registry:ident, $partition:ident, $config:expr) => {
        let ($local, $remote) = crate::remote_log_manager::test_support::temporary_dirs();
        let $registry = crate::partition_registry::PartitionRegistry::new();
        let $partition =
            crate::remote_log_manager::test_support::rolled_tiered_partition_with_config(
                $local.path(),
                $config,
            );
    };
}
pub(crate) use rolled_partition_fixture;

/// Register the caller's partition and run the first mutable-tier sweep.
macro_rules! registered_sweep_fixture {
    ($registry:ident, $controller:ident, $rsm:ident, $rlmm:ident, $partition:expr, $remote:ident) => {
        crate::remote_log_manager::test_support::register_fixture!(
            $registry,
            $controller,
            $rsm,
            $rlmm,
            $partition,
            $remote
        );
        crate::remote_log_manager::test_support::sweep_mutable(
            &$registry,
            &$controller,
            &$rsm,
            &$rlmm,
        )
        .await;
    };
}
pub(crate) use registered_sweep_fixture;

/// Archive the current sealed segments, then run local retention at the future fixture clock.
/// Backend guards and the original exports remain owned by the caller through its assertions.
pub async fn archived_local_retention(
    partition: &Partition,
    remote_dir: &std::path::Path,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> (
    Vec<SegmentExport>,
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
    usize,
) {
    let (exports, config) = multiple_segment_snapshot(partition);
    let (rsm, rlmm) = archived_backends(remote_dir, &exports).await;
    let removed = local_retention_at(
        partition,
        &exports,
        &config,
        &rlmm,
        (unstable, crate::time_util::now_ms() + 1_000_000),
    )
    .await;
    (exports, rsm, rlmm, removed)
}
