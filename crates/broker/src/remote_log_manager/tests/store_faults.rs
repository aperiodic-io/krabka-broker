//! The tiered sweep against an object store that misbehaves.
//!
//! Every other suite in this module drives a backend that answers. A real
//! object store throttles, and it stalls, and the two fail differently: a
//! `503 SlowDown` surfaces as an error the copy path already knows how to
//! roll back, while a stall surfaces as nothing at all until something
//! bounds it. These cases run the production S3 backend --
//! [`krabka_remote_storage::S3RemoteStorage`] over a real
//! `ObjectStoreClient` -- on top of an in-memory store wrapped in
//! [`krabka_object_store::fault::FaultInjectingStore`], so the copy travels
//! the whole path it travels in production and only the store misbehaves.
//!
//! What they pin is the three things an operator is promised when
//! `docs/operations/alert-rules.yaml` fires `KrabkaRemoteCopyErrors` or
//! `KrabkaRemoteCopyLagGrowing`: the failing copy is counted, the lag gauge
//! keeps saying the segment is still owed, and the local copy of a segment
//! whose remote copy did not finish is not deleted.
//!
//! Every case runs on a multi-threaded runtime, because the backend bridges
//! its async object-store calls with `block_in_place`, which a current-thread
//! runtime refuses.

/// The `orders` topic with `partitions` partitions, so a sweep can walk more
/// than one of them.
use fixtures::orders_image as image_with_orders_partitions;
use {
    super::*,
    crate::{
        api_catalog::UnstableApiVersions,
        metadata_source::MetadataSource,
        metrics::{BrokerMetrics, TopicLabel},
        remote_log_manager::test_support::{
            self as fixtures, PartitionSnapshot, RetentionPolicy, TEST_COPY_TIMEOUT, copy_exports,
            partition_snapshot, rolled_tiered_partition_with_config,
        },
    },
    assert2::{assert, check},
    krabka_ids::PartitionIndex,
    krabka_object_store::fault::{FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp},
    krabka_remote_storage::{RemoteLogSegmentState, RemoteStorageManager, S3RemoteStorage},
    std::sync::Arc,
};

/// How long a stalled store holds a call. Long enough that a copy under the
/// deadline below can only end at that deadline, short enough that the
/// abandoned blocking task drains while the test tears down.
const STALL: std::time::Duration = std::time::Duration::from_secs(2);

/// The copy deadline the stall cases run under.
const SHORT_COPY_DEADLINE: Time = millis(150);

struct CopyExportContext {
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn krabka_remote_storage::RemoteLogMetadataManager>,
    metrics: BrokerMetrics,
    index_cache: Arc<krabka_remote_storage::RemoteIndexCache>,
}

#[derive(bon::Builder)]
struct CopyExportRequest {
    exports: Vec<krabka_log::SegmentExport>,
    #[builder(default = TEST_COPY_TIMEOUT)]
    copy_timeout: Time,
}

impl CopyExportContext {
    fn new(rsm: Arc<dyn RemoteStorageManager>) -> Self {
        Self {
            rsm,
            rlmm: Arc::new(krabka_remote_storage::InmemoryRemoteLogMetadataManager::new()),
            metrics: BrokerMetrics::new(),
            index_cache: Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
        }
    }

    async fn copy(&self, request: CopyExportRequest) -> usize {
        copy_exports(
            &fixtures::configured_tier()
                .rsm(&self.rsm)
                .rlmm(&self.rlmm)
                .metrics(&self.metrics)
                .index_cache(&self.index_cache)
                .copy_timeout(request.copy_timeout)
                .call(),
            request.exports,
        )
        .await
    }
}

/// The production S3 backend over `store`, so a fault drill exercises the
/// same `put_from_path` / multipart / key-layout code a real copy does.
fn backend(store: Arc<FaultInjectingStore>) -> Arc<dyn RemoteStorageManager> {
    Arc::new(S3RemoteStorage::with_store(store, None))
}

/// An in-memory store wrapped in `policy`.
fn faulty_store(policy: FaultPolicy) -> Arc<FaultInjectingStore> {
    krabka_object_store::fault::build_faulty_object_store(
        &krabka_object_store::ObjectStoreConfig::InMemory,
        policy,
    )
    .expect("the in-memory store always builds")
}

/// A store that answers every upload with `503 SlowDown`.
fn throttling_store() -> Arc<FaultInjectingStore> {
    faulty_store(FaultPolicy::none().with(StoreOp::Put, OpFault::always(FaultKind::Throttled)))
}

/// A store that accepts every upload and answers it [`STALL`] late.
fn stalling_store() -> Arc<FaultInjectingStore> {
    faulty_store(FaultPolicy::none().with(StoreOp::Put, OpFault::stall(STALL)))
}

/// A tiered partition, its sealed exports and its log config.
struct TieredPartition {
    partition: Arc<Partition>,
    exports: Vec<krabka_log::SegmentExport>,
    config: LogConfig,
}

/// A tiered `orders-0` with segments already sealed on disk, plus its
/// exports and log config.
fn tiered_partition(log_dir: &std::path::Path) -> TieredPartition {
    let partition = rolled_tiered_partition_with_config(log_dir, LogConfig {
        segment_size: bytes(256),
        remote_storage_enable: true,
        local_retention: Some(millis(1)),
        ..LogConfig::default()
    });
    let PartitionSnapshot { exports, config } = partition_snapshot(&partition);
    assert!(exports.len() >= 2, "the fixture needs sealed segments");
    TieredPartition {
        partition,
        exports,
        config,
    }
}

fn orders_label() -> TopicLabel {
    TopicLabel {
        topic: Arc::from(tp().topic.as_str()),
    }
}

/// The control. The same fixture over a store with no faults copies every
/// segment and finishes it, so the refusals below are the store's doing and
/// not the fixture's.
#[tokio::test(flavor = "multi_thread")]
async fn a_healthy_store_finishes_every_copy() {
    let log_dir = tempfile::tempdir().unwrap();
    let TieredPartition { exports, .. } = tiered_partition(log_dir.path());
    let copy_context = CopyExportContext::new(backend(faulty_store(FaultPolicy::none())));

    let copied = copy_context
        .copy(
            CopyExportRequest::builder()
                .exports(exports.clone())
                .build(),
        )
        .await;

    check!(copied == exports.len());
    let listed = copy_context.rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(
        listed
            .iter()
            .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
    );
    check!(
        copy_context
            .metrics
            .remote_copy_errors_total
            .get_or_create(&orders_label())
            .get()
            == 0
    );
}

/// A store answering `503 SlowDown` must leave nothing finished, count every
/// attempt as an error, and leave the copy lag standing at the full backlog.
/// Those are the three series `KrabkaRemoteCopyErrors` and
/// `KrabkaRemoteCopyLagGrowing` read.
#[tokio::test(flavor = "multi_thread")]
async fn a_throttling_store_finishes_nothing_and_moves_the_error_and_lag_series() {
    let FaultyCopyFixture {
        local: _log_dir,
        partition: _partition,
        exports,
        config: _config,
        store,
        rsm,
    } = faulty_copy_fixture(throttling_store);
    let fixtures::OwnedTierResources {
        rlmm,
        metrics,
        index_cache,
    } = fixtures::owned_tier_resources();
    let tier = fixtures::configured_tier()
        .rsm(&rsm)
        .rlmm(&rlmm)
        .metrics(&metrics)
        .index_cache(&index_cache)
        .call();

    let copied = copy_exports(&tier, exports.clone()).await;

    check!(copied == 0);
    check!(
        store.attempts(StoreOp::Put) > 0,
        "the store was never asked"
    );
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(
        listed
            .iter()
            .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished),
        "a throttled copy reached CopySegmentFinished: {listed:?}"
    );
    let label = orders_label();
    let want = u64::try_from(exports.len()).unwrap();
    check!(metrics.remote_copy_errors_total.get_or_create(&label).get() == want);
    check!(metrics.remote_copy_bytes_total.get_or_create(&label).get() == 0);
    // The lag is what the round found waiting. Nothing was copied, so a
    // second round must still find all of it: this is the gauge whose
    // derivative `KrabkaRemoteCopyLagGrowing` alerts on.
    check!(
        metrics.remote_copy_lag_segments.get_or_create(&label).get()
            == i64::try_from(exports.len()).unwrap()
    );
    copy_exports(&tier, exports.clone()).await;
    check!(
        metrics.remote_copy_lag_segments.get_or_create(&label).get()
            == i64::try_from(exports.len()).unwrap()
    );
}

/// A store that accepts the upload and then answers [`STALL`] late must not
/// hold the sweep for that long: the copy deadline abandons it, and it is
/// counted as a failure like any other.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_copy_is_abandoned_at_its_deadline() {
    let FaultyCopyFixture {
        local: _log_dir,
        partition: _partition,
        exports,
        config: _config,
        store: _store,
        rsm,
    } = faulty_copy_fixture(stalling_store);
    let fixtures::OwnedTierResources {
        rlmm,
        metrics,
        index_cache,
    } = fixtures::owned_tier_resources();

    let started = std::time::Instant::now();
    let copied = copy_exports(
        &fixtures::configured_tier()
            .rsm(&rsm)
            .rlmm(&rlmm)
            .copy_timeout(SHORT_COPY_DEADLINE)
            .metrics(&metrics)
            .index_cache(&index_cache)
            .call(),
        vec![exports[0].clone()],
    )
    .await;
    let elapsed = started.elapsed();

    check!(copied == 0);
    check!(
        elapsed < STALL,
        "a {SHORT_COPY_DEADLINE:?} deadline let a stalled copy run for {elapsed:?}"
    );
    check!(
        metrics
            .remote_copy_errors_total
            .get_or_create(&orders_label())
            .get()
            == 1
    );
    // Abandoned, not rolled back: the upload the deadline walked away from is
    // still running against this segment id, so the metadata stays in
    // `CopySegmentStarted` and the next tick re-copies under a fresh id.
    fixtures::check_one_segment_state(&rlmm, RemoteLogSegmentState::CopySegmentStarted);
}

/// A partition whose copy stalls must not stop the sweep from reaching the
/// partitions behind it, deadline or no deadline. Two tiered partitions, one
/// stalled store, one tick: the tick has to return, and the second partition
/// has to have been offered to the store.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_partition_does_not_stop_the_sweep_reaching_the_next() {
    let log_dir = tempfile::tempdir().unwrap();
    let partitions = Arc::new(PartitionRegistry::new());
    let first = tiered_partition(log_dir.path()).partition;
    partitions.insert("orders".into(), PartitionIndex(0), first);
    let second_dir = tempfile::tempdir().unwrap();
    let second = tiered_partition(second_dir.path()).partition;
    second.current_leader.store(1, Ordering::Relaxed);
    partitions.insert("orders".into(), PartitionIndex(1), second);

    let StoreBackend { store, rsm } = store_backend(stalling_store);
    let fixtures::OwnedTierResources {
        rlmm,
        metrics,
        index_cache,
    } = fixtures::owned_tier_resources();
    let controller: Arc<dyn MetadataSource> =
        Arc::new(fixed_source(image_with_orders_partitions(2)));

    let started = std::time::Instant::now();
    tick_all(
        &partitions,
        &*controller,
        &fixtures::configured_tier()
            .rsm(&rsm)
            .rlmm(&rlmm)
            .copy_timeout(SHORT_COPY_DEADLINE)
            .metrics(&metrics)
            .index_cache(&index_cache)
            .call(),
        NodeId(1),
        1,
        SweepConcurrency::default(),
    )
    .await;
    let elapsed = started.elapsed();

    check!(
        elapsed < STALL,
        "one stalled partition held the whole sweep for {elapsed:?}"
    );
    // Both partitions reached the store: had the sweep stopped at the first,
    // the second would have contributed no attempt at all.
    check!(
        store.attempts(StoreOp::Put) >= 2,
        "the sweep offered only {} upload(s) to the store",
        store.attempts(StoreOp::Put)
    );
    for index in [0, 1] {
        let listed = fixtures::partition_segments(&rlmm, index);
        check!(
            listed
                .iter()
                .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished),
            "partition {index} finished a copy the store never completed"
        );
    }
}

/// The failure this issue exists to prevent: local retention deleting a
/// segment whose remote copy is not actually complete. Both misbehaving
/// stores are driven through the same assertion, against a retention clock
/// far enough ahead that every segment is otherwise expired.
#[tokio::test(flavor = "multi_thread")]
async fn local_retention_keeps_segments_whose_copy_never_finished() {
    /// One misbehaving store and the copy deadline it is driven under.
    struct Case {
        name: &'static str,
        store: fn() -> Arc<FaultInjectingStore>,
        copy_timeout: Time,
    }

    let cases: [Case; 2] = [
        Case {
            name: "throttled",
            store: throttling_store,
            copy_timeout: TEST_COPY_TIMEOUT,
        },
        Case {
            name: "stalled",
            store: stalling_store,
            copy_timeout: SHORT_COPY_DEADLINE,
        },
    ];
    for Case {
        name,
        store,
        copy_timeout,
    } in cases
    {
        let log_dir = tempfile::tempdir().unwrap();
        let TieredPartition {
            partition,
            exports,
            config,
        } = tiered_partition(log_dir.path());
        let copy_context = CopyExportContext::new(backend(store()));

        let copied = copy_context
            .copy(
                CopyExportRequest::builder()
                    .exports(exports.clone())
                    .copy_timeout(copy_timeout)
                    .build(),
            )
            .await;
        check!(copied == 0, "{name}");

        // Far past every segment's `local.retention.ms`, so nothing but the
        // missing remote copy can be holding these bytes on disk.
        let removed = fixtures::local_retention_at(
            &partition,
            &exports,
            &config,
            &copy_context.rlmm,
            RetentionPolicy {
                unstable: UnstableApiVersions::Disabled,
                now_ms: now_ms() + 1_000_000,
            },
        )
        .await;

        check!(removed == 0, "{name}");
        let log = partition.log.lock().expect("partition log mutex poisoned");
        check!(
            log.local_log_start_offset() == exports[0].base_offset,
            "{name}"
        );
        check!(log.tierable_segments().len() == exports.len(), "{name}");
    }
}

/// A fault-injecting store and the storage manager built over it.
struct StoreBackend {
    store: Arc<FaultInjectingStore>,
    rsm: Arc<dyn RemoteStorageManager>,
}

fn store_backend(make_store: fn() -> Arc<FaultInjectingStore>) -> StoreBackend {
    let store = make_store();
    let rsm = backend(Arc::clone(&store));
    StoreBackend { store, rsm }
}

struct FaultyCopyFixture {
    local: tempfile::TempDir,
    partition: Arc<Partition>,
    exports: Vec<krabka_log::SegmentExport>,
    config: LogConfig,
    store: Arc<FaultInjectingStore>,
    rsm: Arc<dyn RemoteStorageManager>,
}
/// Keep both the partition and object-store guards through the fault assertions.
fn faulty_copy_fixture(make: fn() -> Arc<FaultInjectingStore>) -> FaultyCopyFixture {
    let local = tempfile::tempdir().unwrap();
    let TieredPartition {
        partition,
        exports,
        config,
    } = tiered_partition(local.path());
    let StoreBackend { store, rsm } = store_backend(make);
    FaultyCopyFixture {
        local,
        partition,
        exports,
        config,
        store,
        rsm,
    }
}
