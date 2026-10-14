//! The final assembly step. It binds the data-plane listeners, builds the
//! [`Broker`] from every startup phase's output, starts the accept loops and
//! the deferred bootstrap tasks, and returns the [`BrokerHandle`]. It is its
//! own module because it is the one place that names every phase at once.

use {
    crate::{
        auto_topic_creation::AutoTopicCreation,
        barrier::coordinator::BarrierCoordinator,
        broker::{
            Broker, BrokerHandle, DisklessRuntime,
            diskless_index::{DisklessFlusherStartup, bootstrap_diskless_index_log},
            listeners::{
                ListenerStartup, ListenerTasks, bind_listeners_and_recover_moves,
                spawn_listener_tasks,
            },
            rlmm::{KafkaSwapKickoff, bootstrap_topic_rlmm},
            runtime::BrokerRuntimeStartup,
            transport::StartupTransport,
        },
        config::BrokerConfig,
        controller_admin::BrokerControllerAdminRouter,
        coordinator::GroupCoordinator,
        diskless::{flusher::FlushConfig, read::DisklessReadHandle},
        error::BrokerError,
        handlers::registry::build_registry,
        log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource,
        network::dispatch::RequestByteBudget,
        partition_registry::PartitionRegistry,
        producer_id_manager::ProducerIdManager,
        producer_state::ProducerState,
        share_coordinator::coordinator::ShareCoordinator,
        share_partition::manager::SharePartitionLeaderManager,
        time_util::now_ms,
        txn::coordinator::TxnCoordinator,
    },
    krabka_ids::PartitionIndex,
    krabka_raft::ConnectionLimiter,
    krabka_units::convert::{ByteSizeExt as _, TimeExt as _},
    std::sync::{Arc, atomic::AtomicBool},
    tokio::{net::TcpListener, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

/// The coordinators and managers that startup built before the final assembly.
pub(super) struct BrokerCoordinatorSet {
    pub(super) group_coordinator: Arc<GroupCoordinator>,
    pub(super) producer_ids: Arc<ProducerIdManager>,
    pub(super) producer_state: Arc<ProducerState>,
    pub(super) txn_coordinator: Arc<TxnCoordinator>,
    pub(super) share_coordinator: Arc<ShareCoordinator>,
    pub(super) share_partition_leaders: Arc<SharePartitionLeaderManager>,
    pub(super) barrier_coordinator: Arc<BarrierCoordinator>,
    pub(super) auto_topic_creation: Arc<AutoTopicCreation>,
}

/// The metadata plane the broker is built on.
pub(super) struct BrokerMetadataHandles {
    pub(super) controller: Arc<dyn MetadataSource>,
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) admin_router: Option<Arc<BrokerControllerAdminRouter>>,
}

/// What the diskless bootstrap task hands back.
struct DisklessBootstrap {
    task: JoinHandle<()>,
    /// Set once the diskless flusher is ready.
    flusher_ready: Arc<AtomicBool>,
}

pub(super) struct BrokerStorageStartup {
    pub(super) log_dir_status: LogDirRegistry,
    pub(super) diskless: DisklessRuntime,
}

pub(super) async fn finish_broker_startup(
    mut config: BrokerConfig,
    data_listeners: Vec<TcpListener>,
    metadata: BrokerMetadataHandles,
    coordinators: BrokerCoordinatorSet,
    transport: StartupTransport,
    runtime: BrokerRuntimeStartup,
    storage: BrokerStorageStartup,
) -> Result<BrokerHandle, BrokerError> {
    let BrokerMetadataHandles {
        controller,
        partitions,
        admin_router,
    } = metadata;
    let ListenerStartup {
        bound,
        listen_addr,
        future_logs,
    } = bind_listeners_and_recover_moves(
        &mut config,
        data_listeners,
        &partitions,
        &runtime.throttle_state,
    )
    .await?;
    let connections = ConnectionLimiter::new(config.max_connections, config.max_connections_per_ip);
    let queued_requests_sem = Arc::new(tokio::sync::Semaphore::new(config.queued_max_requests));
    let queued_request_bytes = config
        .queued_max_request_bytes
        .map(|budget| RequestByteBudget::of(budget.bytes_usize()));
    let broker = Arc::new(Broker {
        config,
        controller,
        partitions,
        future_logs,
        group_coordinator: coordinators.group_coordinator,
        producer_ids: coordinators.producer_ids,
        producer_state: coordinators.producer_state,
        txn_coordinator: coordinators.txn_coordinator,
        share_coordinator: coordinators.share_coordinator,
        share_partition_leaders: coordinators.share_partition_leaders,
        barrier_coordinator: coordinators.barrier_coordinator,
        auto_topic_creation: coordinators.auto_topic_creation,
        supervisor_shutdown: runtime.supervisor_shutdown,
        supervisor_handle: tokio::sync::Mutex::new(Some(runtime.supervisor_handle)),
        disk_scanner_handle: tokio::sync::Mutex::new(runtime.disk_scanner_handle),
        liveness: runtime.liveness_services.liveness,
        tls_dynamic: transport.tls_dynamic,
        ktls_enabled: transport.ktls_enabled,
        inter_broker_client: transport.inter_broker_client,
        unclean_recovery: runtime.liveness_services.unclean_recovery,
        metrics: runtime.metrics,
        dedup: runtime.dedup,
        metrics_bound_addr: runtime.metrics_bound_addr,
        throttle_state: runtime.throttle_state,
        quota_buckets: runtime.quota_buckets,
        connections,
        fetch_session_cache: runtime.fetch_session_cache,
        queued_requests_sem,
        queued_request_bytes,
        want_shutdown: runtime.liveness_services.want_shutdown,
        should_shutdown: runtime.liveness_services.should_shutdown,
        remote_reader: runtime.remote_reader,
        diskless_read: runtime.diskless_read,
        hot_tail: storage.diskless.hot_tail,
        wal_shards: storage.diskless.wal_shards,
        log_dir_status: storage.log_dir_status,
        client_metrics: runtime.client_metrics,
        #[cfg(any(test, feature = "test-helpers"))]
        offset_for_leader_epoch_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        audit_log: runtime.audit_log,
        audit_writer_handle: tokio::sync::Mutex::new(runtime.audit_writer_handle),
        handlers: build_registry(),
    });
    // Publish the startup kTLS probe. It is a constant of the running process,
    // so this is the one place it is ever set, and an operator can read
    // "kTLS is available here" beside the drain counter that says whether the
    // offload is carrying fetches.
    broker.metrics.set_ktls_enabled(broker.ktls_enabled);
    broker.auto_topic_creation.bind(&broker).map_err(|error| {
        broker.supervisor_shutdown.cancel();
        BrokerError::Startup(error.into())
    })?;
    if let Some(router) = admin_router {
        router.bind(&broker).map_err(|error| {
            broker.supervisor_shutdown.cancel();
            BrokerError::Startup(error.into())
        })?;
    }
    let diskless_bootstrap = match (
        broker.diskless_read.as_ref(),
        runtime.kafka_swap_kickoff.as_ref(),
    ) {
        (Some(handle), Some(kickoff)) => Some((handle, kickoff)),
        (None, None) => None,
        _ => {
            broker.supervisor_shutdown.cancel();
            return Err(BrokerError::Startup(
                "diskless read and bootstrap configuration must be enabled together".into(),
            ));
        }
    };
    let ListenerTasks {
        shutdown,
        tasks: listener_tasks,
    } = spawn_listener_tasks(&broker, bound);
    emit_broker_started(&broker, runtime.audit_led_partition).await;
    let topic_rlmm_task = spawn_rlmm_bootstrap(
        &broker,
        runtime.kafka_swap_target.as_ref(),
        runtime.kafka_swap_kickoff.as_ref(),
        &shutdown,
    );
    let (diskless_task, diskless_flusher_ready) =
        diskless_bootstrap.map_or((None, None), |(handle, kickoff)| {
            let DisklessBootstrap {
                task,
                flusher_ready,
            } = spawn_diskless_bootstrap(&broker, handle, kickoff, &shutdown);
            (Some(task), Some(flusher_ready))
        });
    Ok(BrokerHandle {
        listen_addr,
        shutdown,
        listener_tasks,
        topic_rlmm_task,
        diskless_task,
        diskless_flusher_ready,
        broker,
    })
}

async fn emit_broker_started(broker: &Broker, audit_partition: Option<PartitionIndex>) {
    let Some(partition_index) = audit_partition else {
        return;
    };
    let topic = broker.config.audit_topic.clone();
    let partitions = Arc::clone(&broker.partitions);
    let _ = tokio::time::timeout(
        broker.config.audit_partition_wait_timeout.to_std(),
        async move {
            while !partitions.contains(&topic, partition_index) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
    )
    .await;
    broker.audit_log.emit(krabka_audit::AuditEvent::Lifecycle {
        kind: krabka_audit::LifecycleKind::BrokerStarted,
        node_id: i64::from(broker.config.broker_id),
        time_ms: now_ms(),
    });
}

fn spawn_rlmm_bootstrap(
    broker: &Arc<Broker>,
    swap_target: Option<&Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    kickoff: Option<&KafkaSwapKickoff>,
    shutdown: &CancellationToken,
) -> Option<JoinHandle<()>> {
    let (Some(swap), Some(kickoff)) = (swap_target, kickoff) else {
        return None;
    };
    let future = bootstrap_topic_rlmm(
        Arc::clone(swap),
        kickoff.clone(),
        tokio::runtime::Handle::current(),
        broker.metrics.clone(),
        broker.config.node_id,
        broker.controller.watch_image(),
        shutdown.clone(),
    );
    let shutdown = shutdown.clone();
    Some(tokio::spawn(async move {
        tokio::select! {
            () = shutdown.cancelled() => tracing::debug!("RLMM bootstrap cancelled"),
            () = future => {}
        }
    }))
}

fn spawn_diskless_bootstrap(
    broker: &Arc<Broker>,
    handle: &Arc<DisklessReadHandle>,
    kickoff: &KafkaSwapKickoff,
    shutdown: &CancellationToken,
) -> DisklessBootstrap {
    let cache = Arc::clone(&handle.index);
    let ready = Arc::new(AtomicBool::new(false));
    let flusher = DisklessFlusherStartup {
        index_log_slot: handle.index_log_slot(),
        partitions: Arc::clone(&broker.partitions),
        image_rx: broker.controller.watch_image(),
        object_store: handle.object_store(),
        node_id: broker.config.node_id,
        broker_id: broker.config.broker_id,
        metrics: broker.metrics.clone(),
        flush_config: FlushConfig::from_broker(&broker.config),
        ready: Arc::clone(&ready),
    };
    let kickoff = kickoff.clone();
    let shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        bootstrap_diskless_index_log(cache, kickoff, flusher, shutdown).await;
    });
    DisklessBootstrap {
        task,
        flusher_ready: ready,
    }
}
