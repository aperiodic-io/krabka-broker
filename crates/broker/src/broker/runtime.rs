//! The runtime startup phase. It starts every background service that a
//! running broker needs -- audit, replication, liveness, observability,
//! gauges, maintenance, remote storage, and the config-driven watchers -- and
//! collects their handles into one value the final assembly step consumes.

use std::{net::SocketAddr, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_units::{Time, convert::TimeExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::inter_broker_listener_protocol;
use crate::{
    break_glass::{
        metrics::record_proposal_states,
        sweep::{
            BreakGlassController, PROPOSAL_RETENTION, SWEEP_INTERVAL, run as run_break_glass_sweep,
        },
    },
    broker::{
        DisklessRuntime,
        adapters::ControllerAdapter,
        audit::start_audit_pipeline,
        gauges::spawn_broker_gauge_updater,
        liveness::{LivenessStartup, start_liveness_services},
        maintenance::{spawn_cluster_data_maintenance, spawn_storage_security_maintenance},
        observability::{ObservabilityStartup, start_observability},
        remote_storage::{RemoteStorageStartup, start_remote_storage},
        replication::{ReplicatorStorage, spawn_replicator_supervisor},
        rlmm::{KafkaSwapKickoff, kafka_swap_kickoff},
    },
    client_metrics::ClientMetrics,
    config::BrokerConfig,
    cordoned_log_dirs::spawn_watcher,
    delegation_token_cleanup::{DelegationTokenController, run as run_delegation_token_cleanup},
    diskless::read::DisklessReadHandle,
    error::BrokerError,
    fetch_session::FetchSessionCache,
    health::HealthState,
    log_dir_id::LogDirIds,
    log_dir_status::LogDirRegistry,
    metadata_source::{MetadataSource, watch_image_loop},
    metrics::{BrokerMetrics, spawn_metric_series_evictor},
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    producer_state::ProducerState,
    quota::{QuotaBuckets, run as run_quota_refresh, run_expiry},
    remote_reader::RemoteReader,
    share_coordinator::coordinator::ShareCoordinator,
    throttle::{ThrottleState, apply_image, run as run_throttle},
    time_util::now_ms,
    tls_reload::run as run_tls_reload,
    txn::{
        completion::run, coordinator::TxnCoordinator, expiration::run as run_txn_expiration,
        id_expiration::run as run_txn_id_expiration,
    },
};

struct RuntimeCaches {
    fetch_sessions: Arc<FetchSessionCache>,
    quota_buckets: Arc<QuotaBuckets>,
}

/// Publish the KFC-9 gauges that only the metadata image knows.
///
/// One image watch feeds both families, because both read the same image and
/// both must fall as well as rise: the freeze gauge drops when a thaw removes
/// an entry, and a proposal that moves from `Pending` to `Consumed` lowers one
/// series and raises another. A tick loop would hold a stale value between
/// ticks, and two loops would read two different images.
fn spawn_break_glass_gauges(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    metrics: &BrokerMetrics,
    shutdown: CancellationToken,
) {
    let images = controller.watch_image();
    let metrics = metrics.clone();
    let break_glass = config.break_glass.clone();
    tokio::spawn(watch_image_loop(
        images,
        "break-glass and freeze gauges",
        shutdown,
        move |image| {
            metrics.record_topic_freezes_active(
                i64::try_from(image.topic_freezes().count()).unwrap_or(i64::MAX),
            );
            record_proposal_states(&metrics, image, &break_glass, now_ms());
        },
    ));
}

fn start_runtime_watchers(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    throttle_state: &Arc<ThrottleState>,
    txn_coordinator: &Arc<TxnCoordinator>,
    metrics: &BrokerMetrics,
    shutdown: &CancellationToken,
) -> RuntimeCaches {
    if let (Some(dynamic), Some(tls_config)) = (tls_dynamic.cloned(), config.tls_config.clone()) {
        tokio::spawn(run_tls_reload(
            dynamic,
            tls_config,
            config.tls_reload_interval,
            shutdown.child_token(),
        ));
    }
    apply_image(&controller.current_image(), config.node_id, throttle_state);
    tokio::spawn(run_throttle(
        controller.watch_image(),
        config.node_id,
        Arc::clone(throttle_state),
        shutdown.child_token(),
    ));
    let fetch_sessions = Arc::new(FetchSessionCache::new(
        config.max_incremental_fetch_session_cache_slots,
    ));
    // KIP-13's `quota.window.num` * `quota.window.size.seconds`: the window a
    // client's byte rate is averaged over, which is also the burst a bucket
    // allows before it throttles (#418).
    let quota_buckets = Arc::new(QuotaBuckets::with_window(config.quota_window));
    tokio::spawn(run_quota_refresh(
        controller.watch_image(),
        Arc::clone(&quota_buckets),
        shutdown.child_token(),
    ));
    // KIP-13 / KIP-599: a bucket, and the per-entity throttle series it
    // publishes, live only as long as the client behind them keeps arriving
    // (#396). Without this a cluster's `/metrics` body grows by one label set
    // per client id it has ever seen.
    tokio::spawn(run_expiry(
        Arc::clone(&quota_buckets),
        metrics.clone(),
        shutdown.child_token(),
    ));
    if config.delegation_token_secret_key.is_some() {
        let interval = config.delegation_token_expiry_check_interval;
        let token_controller: Arc<dyn DelegationTokenController> = Arc::new(ControllerAdapter {
            handle: Arc::clone(controller),
            node_id: config.node_id,
        });
        tokio::spawn(run_delegation_token_cleanup(
            token_controller,
            interval,
            shutdown.child_token(),
        ));
    }
    // KFC-9. Every broker sweeps, the tombstone is idempotent, and a broker
    // that never sweeps is still safe, so the sweep needs no config gate.
    let break_glass_controller: Arc<dyn BreakGlassController> = Arc::new(ControllerAdapter {
        handle: Arc::clone(controller),
        node_id: config.node_id,
    });
    tokio::spawn(run_break_glass_sweep(
        break_glass_controller,
        SWEEP_INTERVAL,
        PROPOSAL_RETENTION,
        shutdown.child_token(),
    ));
    spawn_break_glass_gauges(config, controller, metrics, shutdown.child_token());
    // Per-partition and per-topic series are created lazily by the data path
    // and are not released by anything else, so the image watch that adds a
    // partition is also what has to take its series away again.
    spawn_metric_series_evictor(
        controller.watch_image(),
        config.node_id,
        metrics.clone(),
        shutdown.child_token(),
    );
    tokio::spawn(run(
        Arc::clone(txn_coordinator),
        Arc::clone(controller),
        shutdown.child_token(),
    ));
    if config.txn_abort_cleanup_interval > <Time as TimeExt>::ZERO {
        tokio::spawn(run_txn_expiration(
            Arc::clone(txn_coordinator),
            Arc::clone(controller),
            config.txn_abort_cleanup_interval,
            shutdown.child_token(),
        ));
    }
    if config.txn_id_expiration_cleanup_interval > <Time as TimeExt>::ZERO {
        tokio::spawn(run_txn_id_expiration(
            Arc::clone(txn_coordinator),
            Arc::clone(controller),
            config.txn_id_expiration_cleanup_interval,
            config.txn_id_expiration,
            shutdown.child_token(),
        ));
    }
    RuntimeCaches {
        fetch_sessions,
        quota_buckets,
    }
}

pub(super) struct BrokerRuntimeStartup {
    pub(super) supervisor_shutdown: CancellationToken,
    pub(super) supervisor_handle: JoinHandle<()>,
    pub(super) disk_scanner_handle: Option<JoinHandle<()>>,
    pub(super) liveness_services: LivenessStartup,
    pub(super) metrics: BrokerMetrics,
    pub(super) metrics_bound_addr: Option<SocketAddr>,
    pub(super) throttle_state: Arc<ThrottleState>,
    pub(super) quota_buckets: Arc<QuotaBuckets>,
    pub(super) fetch_session_cache: Arc<FetchSessionCache>,
    pub(super) remote_reader: Option<Arc<RemoteReader>>,
    pub(super) diskless_read: Option<Arc<DisklessReadHandle>>,
    pub(super) client_metrics: Arc<ClientMetrics>,
    pub(super) audit_log: Arc<krabka_audit::AuditLog>,
    pub(super) audit_writer_handle: Option<JoinHandle<()>>,
    pub(super) audit_led_partition: Option<PartitionIndex>,
    pub(super) kafka_swap_kickoff: Option<KafkaSwapKickoff>,
    pub(super) kafka_swap_target: Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    pub(super) inter_listener_protocol: krabka_security::ListenerProtocol,
}

pub(super) async fn start_broker_runtime(
    config: &mut BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    inter_broker_client: &Arc<InterBrokerClient>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    storage: (
        &Arc<PartitionRegistry>,
        &Arc<ProducerState>,
        &LogDirRegistry,
        &LogDirIds,
    ),
    coordinators: (&Arc<TxnCoordinator>, &Arc<ShareCoordinator>),
    // The three things the runtime needs pre-built: the diskless WAL runtime,
    // the metric registry the coordinators already report into, and the
    // health state the readiness probe and the gauge sampler both read.
    runtime_deps: (&DisklessRuntime, BrokerMetrics, Option<HealthState>),
) -> Result<BrokerRuntimeStartup, BrokerError> {
    let (diskless_runtime, metrics, health) = runtime_deps;
    let supervisor_shutdown = CancellationToken::new();
    let throttle_state = Arc::new(ThrottleState::new());
    // KIP-1066: keep the cordoned set in step with this broker's dynamic
    // `cordoned.log.dirs`, as Kafka's `DynamicLogConfig.reconfigure` does.
    spawn_watcher(
        controller,
        config.node_id,
        storage.0.cordoned_log_dirs().clone(),
        supervisor_shutdown.child_token(),
    );
    let inter_listener_protocol = inter_broker_listener_protocol(config);
    let (audit_led_partition, audit_log, audit_writer_handle) = start_audit_pipeline(
        config,
        &**controller,
        storage.0,
        &metrics,
        &supervisor_shutdown,
    );
    let supervisor_handle = spawn_replicator_supervisor(
        config,
        controller,
        storage.0,
        coordinators,
        inter_broker_client,
        (&supervisor_shutdown, &throttle_state, &metrics),
        ReplicatorStorage {
            log_dir_status: storage.2,
            producer_state: storage.1,
            log_dir_ids: storage.3,
            diskless: diskless_runtime,
        },
    );
    let LivenessStartup {
        liveness,
        want_shutdown,
        should_shutdown,
        unclean_recovery,
        initial_unfence,
    } = start_liveness_services(
        config,
        controller,
        inter_broker_client,
        inter_listener_protocol,
        (&metrics, &audit_log),
        &supervisor_shutdown,
        (storage.2, storage.3, storage.0.cordoned_log_dirs()),
    );
    let ObservabilityStartup {
        metrics_bound_addr,
        client_metrics,
    } = start_observability(config, &metrics, &supervisor_shutdown).await?;
    spawn_broker_gauge_updater(
        (Arc::clone(storage.0), storage.2.clone()),
        Arc::clone(controller),
        Arc::clone(&liveness),
        config.node_id,
        (metrics.clone(), health),
        config,
        supervisor_shutdown.child_token(),
    );
    let disk_scanner_handle = spawn_storage_security_maintenance(
        config,
        storage.0,
        controller,
        inter_broker_client,
        &metrics,
        &supervisor_shutdown,
    );
    spawn_cluster_data_maintenance(
        config,
        controller,
        &liveness,
        storage.0,
        storage.1,
        &metrics,
        &supervisor_shutdown,
    );
    // KIP-405 tiering is a broker component: Kafka builds its
    // `RemoteLogManager` in `BrokerServer` only. A node without the broker role
    // hosts no partition, and it opens no data-plane listener for the
    // topic-backed metadata manager to bootstrap from.
    let (kafka_swap_kickoff, remote) = if config.is_broker() {
        (
            kafka_swap_kickoff(config),
            start_remote_storage(
                config,
                storage.0,
                controller,
                &metrics,
                &supervisor_shutdown,
            )?,
        )
    } else {
        (None, RemoteStorageStartup::default())
    };
    let caches = start_runtime_watchers(
        config,
        controller,
        tls_dynamic,
        &throttle_state,
        coordinators.0,
        &metrics,
        &supervisor_shutdown,
    );
    Ok(BrokerRuntimeStartup {
        supervisor_shutdown,
        supervisor_handle,
        disk_scanner_handle,
        liveness_services: LivenessStartup {
            liveness,
            want_shutdown,
            should_shutdown,
            unclean_recovery,
            initial_unfence,
        },
        metrics,
        metrics_bound_addr,
        throttle_state,
        quota_buckets: caches.quota_buckets,
        fetch_session_cache: caches.fetch_sessions,
        remote_reader: remote.reader,
        diskless_read: remote.diskless_read,
        client_metrics,
        audit_log,
        audit_writer_handle,
        audit_led_partition,
        kafka_swap_kickoff,
        kafka_swap_target: remote.swap_target,
        inter_listener_protocol,
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use crate::{
        Broker,
        config::{
            BrokerConfig,
            NodeRole::{Broker as BrokerRole, Controller},
            RemoteStorageBackend,
        },
    };

    /// KIP-405 tiering runs on a node with the broker role only. A
    /// controller-only node with a remote storage backend configured builds no
    /// remote reader and starts no diskless index bootstrap, which would dial a
    /// data-plane listener that the node does not open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_a_node_with_the_broker_role_starts_remote_storage() {
        // (roles, whether the node starts the remote storage runtime)
        let cases = [
            (vec![Controller], false),
            (vec![Controller, BrokerRole], true),
        ];
        for (roles, starts) in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let objects = tempfile::tempdir().expect("tempdir");
            let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
            config.remote_storage_backend = Some(RemoteStorageBackend::Local {
                dir: objects.path().to_path_buf(),
            });
            config.roles.clone_from(&roles);

            let handle = Broker::start(config).await.expect("start");
            let started = (
                handle.broker.remote_reader.is_some(),
                handle.broker.diskless_read.is_some(),
                handle.diskless_task.is_some(),
            );
            handle.shutdown().await;

            check!(started == (starts, starts, starts), "{roles:?}");
        }
    }
}
