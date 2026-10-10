//! The public [`Broker::start`] entry points and the sequence they drive. This
//! module orders the startup phases -- transport, metadata quorum, storage
//! recovery, coordinators, runtime services, and final assembly -- and holds
//! the commentary that explains why that order is the one that works.

use std::{future::Future, sync::Arc};

use futures_util::future::BoxFuture;
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};

use crate::{
    broker::{
        Broker, BrokerHandle, DisklessRuntime,
        coordinators::{CoordinatorInputs, CoordinatorStartup, start_coordinators},
        finish::{BrokerStorageStartup, finish_broker_startup},
        metadata_phase::{MetadataControlPlane, MetadataPhase, start_metadata_phase},
        runtime::start_broker_runtime,
        storage::{StorageStartup, recover_storage_and_groups},
        transport::{StartupTransport, prepare_startup_transport},
    },
    config::{BrokerConfig, DEFAULT_READINESS_MAX_METADATA_LAG},
    error::BrokerError,
    health::HealthState,
    metadata_source::or_fatal_fault,
};

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start(config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, None, None).await
    }

    /// Like [`Self::start`], but marks its readiness conditions on a
    /// caller-supplied [`HealthState`] as it reaches them.
    ///
    /// The process answers `/healthz` and `/readyz` from the moment it starts,
    /// which is before this call and all the way through it: log-dir recovery
    /// and metadata catch-up both happen inside it. So the caller creates the
    /// state, serves [`crate::health::router`] over it, and passes this clone
    /// in. [`Self::start`] builds its own state instead, which nothing reads.
    ///
    /// # Errors
    ///
    /// Returns an error when log I/O fails, a record or index is corrupt, or
    /// the requested offset violates the segment state.
    pub async fn start_with_health(
        config: BrokerConfig,
        health: HealthState,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners_boxed(config, None, Vec::new(), health).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`.
    ///
    /// Thin wrapper over [`Self::start_with_listeners`] for callers that only
    /// hand off the controller port. The data plane still binds from `config`.
    /// See that method for the full handoff contract.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start_with_controller_listener(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, controller_listener, None).await
    }

    /// Like [`Self::start`], but adopts caller-supplied, already-bound
    /// listeners instead of binding their addresses itself:
    ///
    /// * `controller_listener`: threaded through to
    ///   [`krabka_raft::Controller::start_with_listener`]. Its local address
    ///   MUST equal `config.controller_listen_addr`.
    /// * `data_plane_listeners`: each listener is adopted for the data-plane
    ///   [`ListenerSpec`] whose `bind_addr` equals its local address (for the
    ///   legacy single-listener path that is `config.listen_addr`). Any
    ///   non-matching specs still bind from `config`. A node without the broker
    ///   role opens no data-plane listener, so it closes every supplied one.
    ///
    /// A live socket handoff closes the TOCTOU window that the bind-and-drop
    /// trick leaves open. That trick reads an ephemeral port and then drops the
    /// probe before it re-binds. In the window between the two steps, another
    /// process can claim the just-released port, which is the `AddrInUse` flake
    /// that test harnesses hit under parallel execution. A caller that needs
    /// to know the data-plane port before this call still binds it first and
    /// passes the live listener in, for that reason. A `config.listen_addr`
    /// of port 0 with no supplied listener binds and registers itself, so a
    /// caller that only wants a broker no other process can steal the port
    /// from need not pre-bind at all.
    ///
    /// [`ListenerSpec`]: crate::config::ListenerSpec
    // sequential bring-up; splitting hurts readability more than it helps
    // cargo-mutants: network/socket bring-up, not unit-testable
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn start_with_listeners(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        data_plane_listeners: impl IntoIterator<Item = tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners_boxed(
            config,
            controller_listener,
            data_plane_listeners.into_iter().collect(),
            HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG),
        )
        .await
    }

    fn start_with_listeners_boxed(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        data_plane_listeners: Vec<tokio::net::TcpListener>,
        health: HealthState,
    ) -> BoxFuture<'static, Result<BrokerHandle, BrokerError>> {
        Box::pin(Self::start_with_listeners_inner(
            config,
            controller_listener,
            data_plane_listeners,
            health,
            Self::start_after_quorum_join,
        ))
    }

    /// `after_quorum_join` is [`Self::start_after_quorum_join`] outside tests.
    /// A test passes a tail that never finishes instead, as the wait for the
    /// first unfence does once the controller has stopped, to see the fatal
    /// fault end the start.
    async fn start_with_listeners_inner<Tail, TailFuture>(
        mut config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        mut data_plane_listeners: Vec<tokio::net::TcpListener>,
        health: HealthState,
        after_quorum_join: Tail,
    ) -> Result<BrokerHandle, BrokerError>
    where
        Tail: FnOnce(JoinedQuorum) -> TailFuture,
        TailFuture: Future<Output = Result<BrokerHandle, BrokerError>>,
    {
        // Binds the default data-plane listener now, and publishes its real
        // port into `config`, when the config asks for one and the caller
        // supplied none. `start_metadata_phase` below registers this node
        // with the controller, and that registration must carry the bound
        // port, not a `:0` that names nothing a peer can dial.
        crate::broker::listeners::bind_ephemeral_data_plane_listener(
            &mut config,
            &mut data_plane_listeners,
        )
        .await?;
        let transport = prepare_startup_transport(&config).await?;
        let metrics = crate::metrics::BrokerMetrics::new();
        let diskless_runtime = DisklessRuntime::new(
            config.node_id,
            config.inter_broker_principal_node_ids.clone(),
            config
                .diskless_wal_hot_tail_max_size
                .bytes_u64()
                .try_into()
                .unwrap_or(usize::MAX),
            metrics.clone(),
        );

        // 1. Bring up the metadata quorum BEFORE the client listener so
        //    handlers can read from it the moment they accept their first
        //    connection. The controller owns its own listener bound to
        //    `controller_listen_addr`.
        //
        //    Raft dialer + handshake wiring:
        //
        //    Replication + heartbeat dials route through the data-plane
        //    inter-broker listener (which speaks SASL/TLS when
        //    configured). The Raft RPCs (`AppendEntries`, `Vote`,
        //    `SubmitChange`) dial the *controller* listener which now
        //    shares the same SASL/TLS handshake path via the
        //    `InterBrokerDialer` adapter on the outbound side and
        //    `BrokerRaftHandshake` on the inbound side. With the default
        //    `controller_listener_protocol = Plaintext`, the dialer's
        //    `dial` impl reduces to a `TcpStream::connect` and the
        //    handshake is `None`, so the legacy raw-TCP raft path is
        //    byte-identical for existing deployments and tests.
        //
        //    `BrokerRaftHandshake` needs a `ControllerHandle` to satisfy
        //    SCRAM credential lookups, but the handle isn't built until
        //    `Controller::start` returns. We bridge that with an
        //    `Arc<OnceCell<Arc<ControllerHandle>>>` that's installed into
        //    the handshake up front and `set` once the controller exists.

        // The metadata phase loads the bootstrap records of a controller and
        // gives them to `ControllerConfig`. The active controller writes them
        // when it activates on an empty metadata log, and every node, a
        // broker-only node too, reads them from that log. No node submits
        // them. A `Join` node has no seed voter set and relies on
        // `bootstrap_servers` + auto-join instead.
        let MetadataPhase {
            control_plane:
                MetadataControlPlane {
                    source: controller,
                    admin_router: controller_admin_router,
                },
            audit: raft_handshake_audit,
        } = start_metadata_phase(
            &mut config,
            controller_listener,
            transport.tls_dynamic.as_ref(),
            &transport.inter_broker_client,
            Arc::clone(&diskless_runtime.wal_shards),
        )
        .await?;

        // The controller exists from here on, and it can stop itself over a
        // fatal fault: Kafka halts the process at any point of its startup on
        // such a fault. The rest of the start runs against that fault, so it
        // ends the start at once and names its reason, rather than surfacing
        // later as a stalled unfence wait or a bare "controller shut down".
        or_fatal_fault(
            controller.watch_fatal(),
            Box::pin(after_quorum_join(JoinedQuorum {
                config,
                data_plane_listeners,
                health,
                transport,
                metrics,
                diskless_runtime,
                controller,
                controller_admin_router,
                raft_handshake_audit,
            })),
        )
        .await
    }

    /// The startup phases that follow the join of the metadata quorum: storage
    /// recovery, the coordinators, runtime services, and final assembly. It is
    /// a function of its own so that [`Self::start_with_listeners_inner`] can
    /// race the whole of it against the controller's fatal fault.
    async fn start_after_quorum_join(joined: JoinedQuorum) -> Result<BrokerHandle, BrokerError> {
        let JoinedQuorum {
            mut config,
            data_plane_listeners,
            health,
            transport:
                StartupTransport {
                    tls_dynamic,
                    ktls_enabled,
                    inter_broker_client,
                },
            metrics,
            diskless_runtime,
            controller,
            controller_admin_router,
            raft_handshake_audit,
        } = joined;

        // 1b. KIP-853 controller auto-join. Spawned BEFORE the leader-wait in
        //     step 2: a `Join` broker's empty raft log keeps it in openraft's
        //     Learner state with no leader, so `Broker::start` would block in
        //     step 2 forever. The auto-join loop concurrently sends
        //     `AddRaftVoter(self)` to a `bootstrap_servers` entry; the leader's
        //     handler runs `add_learner` (replicating the log to us) and
        //     promotes us — at which point step 2's `watch_leader` fires and
        //     start proceeds. `run` returns immediately when `auto_join` is
        //     disabled (bootstrap / standalone brokers), so this is a cheap
        //     no-op there. The loop advertises the controller's REAL bound
        //     address, known now that `Controller::start` has bound the
        //     listener.
        // The joiner sends `AddRaftVoter` to a bootstrap server's *client*
        // data-plane listener (where api_key 80 is served), so it speaks the
        // inter-broker listener protocol — not the controller-listener
        // protocol that openraft RPCs use.
        // Auto-join grows the controller *voter* quorum, so only nodes that
        // run a controller participate. A broker-only node is a pure observer
        // and never joins the quorum.
        // `start_metadata_phase` completes auto-join, leader readiness, the
        // wait for `metadata.version`, and registration.

        // The metadata authority exists and this node has reached the quorum,
        // so `/readyz` can compare this node's offset against the quorum's.
        health.install_metadata_progress(crate::health::metadata_progress(Arc::clone(&controller)));

        let StorageStartup {
            log_dir_status,
            log_dir_ids,
            partitions,
            producer_state,
            group_coordinator,
            producer_ids,
        } = recover_storage_and_groups(&config, &controller, &diskless_runtime).await?;
        // Every log dir has been scanned and every recovered partition has a
        // writer, so a Fetch can no longer miss data this node holds.
        health.mark_log_dir_recovery_complete();

        // The share persister and the barrier coordinator create their topics
        // on first use. The broker binds itself to the component once it is
        // built.
        let auto_topic_creation =
            Arc::new(crate::auto_topic_creation::AutoTopicCreation::default());
        // The barrier coordinator reports through the process registry, and it
        // is built here rather than inside the runtime because the coordinators
        // start first. BrokerMetrics clones cheaply.
        let CoordinatorStartup {
            txn_coordinator,
            barrier_coordinator,
            share_coordinator,
            share_partition_leaders,
            share_persister,
            dead_letters,
        } = start_coordinators(
            &config,
            CoordinatorInputs {
                controller: &controller,
                partitions: &partitions,
                group_coordinator: &group_coordinator,
                producer_ids: &producer_ids,
                inter_broker_client: &inter_broker_client,
                auto_topic_creation: &auto_topic_creation,
                metrics: &metrics,
            },
        )
        .await;

        // 4b. Spawn the replicator supervisor. Started AFTER the controller
        //    is up and self-registration succeeded so the supervisor's
        //    initial reconcile already sees this broker in the brokers()
        //    set. With replication_factor=1 the desired follower set is
        //    always empty, so this is a no-op for single-broker setups.
        let runtime = start_broker_runtime(
            &mut config,
            &controller,
            &inter_broker_client,
            tls_dynamic.as_ref(),
            (&partitions, &producer_state, &log_dir_status, &log_dir_ids),
            (&txn_coordinator, &share_coordinator),
            (&diskless_runtime, metrics, Some(health.clone())),
        )
        .await?;

        // KIP-405: a dead-letter copy of a share-group record that only the
        // remote tier holds reads it there, through the reader the runtime
        // just built.
        if let Some(reader) = runtime.remote_reader.as_ref() {
            dead_letters.set_remote_reader(Arc::clone(reader));
        }

        // The controller listener is already accepting connections, but the
        // audit log only exists now. Publish it so controller SASL logins
        // from here on write their `Authentication` rows.
        let _ = raft_handshake_audit.set(Arc::clone(&runtime.audit_log));

        crate::coordinator::leadership::spawn(
            config.node_id,
            Arc::clone(&controller),
            Arc::clone(&partitions),
            Arc::clone(&group_coordinator),
            runtime.supervisor_shutdown.child_token(),
        );

        // Refresh the metadata of the groups that subscribe to a changed
        // topic, and tombstone the committed offsets of every deleted topic,
        // as Kafka's `GroupCoordinatorService.onMetadataUpdate` does.
        crate::coordinator::topic_deletion::spawn(
            config.node_id,
            Arc::clone(&controller),
            Arc::clone(&group_coordinator),
            runtime.supervisor_shutdown.child_token(),
        );

        // Tombstone the share state of every deleted topic, and run the
        // cold-partition snapshot and the prune of `__share_group_state`, as
        // Kafka's `ShareCoordinatorService` does.
        crate::share_coordinator::coordinator::jobs::spawn(
            Arc::clone(&share_coordinator),
            controller.watch_image(),
            runtime.supervisor_shutdown.child_token(),
        );

        // KIP-211. Every broker sweeps the groups whose offsets partition it
        // leads, and the tombstones are idempotent, so the sweep needs no
        // config gate of its own.
        tokio::spawn(crate::coordinator::retention::run(
            config.node_id,
            Arc::clone(&controller),
            Arc::clone(&group_coordinator),
            config.offsets_retention_check_interval(),
            config.offsets_retention(),
            runtime.supervisor_shutdown.child_token(),
        ));

        tokio::spawn(crate::barrier::scheduler::run(
            Arc::clone(&barrier_coordinator),
            Arc::clone(&controller),
            runtime.supervisor_shutdown.child_token(),
        ));

        // KFC-1 deliver-at-time visibility. The task is a liveness aid only: it
        // wakes a consumer parked at the delivery watermark when the next batch
        // comes due. A fetch recomputes that watermark under the log mutex, so
        // this task can die without making a single fetch wrong.
        // The bound is a constant of this process, so it is published once
        // here rather than re-set on every scheduler pass. An alert reads it to
        // compare measured clock uncertainty against the bound the broker
        // actually relies on.
        runtime
            .metrics
            .delivery_clock_uncertainty_seconds
            .set(config.log_config.delivery_clock_uncertainty.secs_f64());

        tokio::spawn(crate::delivery::scheduler::run(
            Arc::clone(&partitions),
            config.node_id,
            crate::delivery::config::DeliveryConfig::default(),
            Arc::new(crate::delivery::metrics::BrokerDeliveryMetrics::new(
                runtime.metrics.clone(),
            )),
            Arc::new(crate::delivery::DeliveryWaker::new()),
            runtime.supervisor_shutdown.child_token(),
        ));

        crate::share_partition::backlog_poller::BacklogPoller {
            node_id: config.node_id,
            coordinator: Arc::clone(&group_coordinator),
            metadata: Arc::clone(&controller),
            partitions: Arc::clone(&partitions),
            persister: share_persister,
            inter_broker: Arc::clone(&inter_broker_client),
            listener_protocol: runtime.inter_listener_protocol,
            listener_name: config.inter_broker_listener_name.clone(),
            period: config.share_group.backlog_poll_interval,
            metrics: runtime.metrics.clone(),
            shutdown: runtime.supervisor_shutdown.child_token(),
        }
        .spawn();

        // The coordinator owns the lifetime of a group's lag series: a group
        // is deleted or moves to another coordinator without the metadata
        // image recording anything the series evictor could act on, so the
        // removal has to release the series itself.
        group_coordinator.set_metrics(runtime.metrics.clone());
        crate::lag::LagPoller {
            node_id: config.node_id,
            coordinator: Arc::clone(&group_coordinator),
            metadata: Arc::clone(&controller),
            partitions: Arc::clone(&partitions),
            inter_broker: Arc::clone(&inter_broker_client),
            listener_protocol: runtime.inter_listener_protocol,
            listener_name: config.inter_broker_listener_name.clone(),
            inter_broker_server_name: config.inter_broker_server_name.clone(),
            period: crate::lag::LAG_POLL_INTERVAL,
            metrics: runtime.metrics.clone(),
            shutdown: runtime.supervisor_shutdown.child_token(),
        }
        .spawn();

        let initial_unfence = runtime.liveness_services.initial_unfence.clone();
        let images = controller.watch_image();
        let is_broker = config.is_broker();
        let node_id = config.node_id;
        let unfence_timeout = config.startup_leader_wait_timeout.to_std();
        let handle = finish_broker_startup(
            config,
            data_plane_listeners,
            (controller, partitions, controller_admin_router),
            (
                group_coordinator,
                producer_ids,
                producer_state,
                txn_coordinator,
                share_coordinator,
                share_partition_leaders,
                barrier_coordinator,
                auto_topic_creation,
            ),
            (tls_dynamic, ktls_enabled, inter_broker_client),
            runtime,
            BrokerStorageStartup {
                log_dir_status,
                diskless: diskless_runtime,
            },
        )
        .await?;
        if is_broker {
            wait_for_initial_unfence(initial_unfence, images, node_id, unfence_timeout).await;
        }
        // `finish_broker_startup` returns only once every data-plane listener
        // is bound and its accept loop is running, so the last readiness
        // condition this node controls on its own is met here.
        health.mark_listeners_bound();
        Ok(handle)
    }
}

/// What startup holds once this node has joined the metadata quorum, handed on
/// to [`Broker::start_after_quorum_join`].
struct JoinedQuorum {
    config: BrokerConfig,
    data_plane_listeners: Vec<tokio::net::TcpListener>,
    health: HealthState,
    transport: StartupTransport,
    metrics: crate::metrics::BrokerMetrics,
    diskless_runtime: DisklessRuntime,
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    controller_admin_router: Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
    raft_handshake_audit: crate::raft_handshake::AuditLogArc,
}

/// Wait for the first heartbeat answer that says this broker is unfenced, as
/// Kafka's `BrokerServer.startup` waits on
/// `BrokerLifecycleManager.initialUnfenceFuture`.
///
/// A new registration is fenced, and the controller unfences it on the first
/// heartbeat that has caught up to it; until then the broker leads nothing
/// and takes no new replica. The heartbeat reaches the controller only once
/// this node's controller listener routes to the broker, so the wait comes
/// after the rest of startup. It is bounded by `startup_leader_wait_timeout`
/// and only warns when that elapses, so a slow controller delays startup but
/// does not fail it. A heartbeat the controller refuses for authorization
/// ends the wait too: only an ACL change could unfence this broker.
///
/// Once the controller has answered, the wait goes on until this node's own
/// image carries the unfenced registration. A broker-only node places replicas
/// against that image, and it trails the controller's.
async fn wait_for_initial_unfence(
    mut initial_unfence: tokio::sync::watch::Receiver<crate::heartbeat::client::InitialUnfence>,
    mut images: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    node_id: krabka_raft::NodeId,
    timeout: std::time::Duration,
) {
    use crate::heartbeat::client::InitialUnfence;
    let settled = async {
        let state = *initial_unfence
            .wait_for(|state| *state != InitialUnfence::Pending)
            .await?;
        if state == InitialUnfence::Unfenced {
            images
                .wait_for(|image| image.broker(node_id).is_some_and(|broker| !broker.fenced))
                .await?;
        }
        Ok::<_, tokio::sync::watch::error::RecvError>(state)
    };
    match tokio::time::timeout(timeout, settled).await {
        Ok(Ok(InitialUnfence::Refused)) => tracing::error!(
            "the controller refused this broker's heartbeat with \
             CLUSTER_AUTHORIZATION_FAILED; it stays fenced"
        ),
        Ok(Ok(_)) => {}
        Ok(Err(_)) => {
            tracing::warn!("the heartbeat client stopped before this broker was unfenced");
        }
        Err(_) => tracing::warn!(
            ?timeout,
            "this broker was not unfenced within the startup wait"
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;
    use crate::test_support::{UNSUPPORTED_LEVEL_FAULT, finalize_unstable_metadata_version};

    #[test]
    fn public_startup_futures_are_unboxed() {
        let boxed_size =
            std::mem::size_of::<BoxFuture<'static, Result<BrokerHandle, BrokerError>>>();
        let start = Broker::start(BrokerConfig::for_tests(std::path::PathBuf::new()));
        let start_with_controller_listener = Broker::start_with_controller_listener(
            BrokerConfig::for_tests(std::path::PathBuf::new()),
            None,
        );
        let start_with_listeners = Broker::start_with_listeners(
            BrokerConfig::for_tests(std::path::PathBuf::new()),
            None,
            None,
        );
        let start_with_health = Broker::start_with_health(
            BrokerConfig::for_tests(std::path::PathBuf::new()),
            HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG),
        );

        assert!(std::mem::size_of_val(&start) > boxed_size);
        assert!(std::mem::size_of_val(&start_with_controller_listener) > boxed_size);
        assert!(std::mem::size_of_val(&start_with_listeners) > boxed_size);
        assert!(std::mem::size_of_val(&start_with_health) > boxed_size);
    }

    // A fault that lands after this node has joined the quorum ends a start
    // that is still in its tail. The stand-in tail submits the unsupported
    // level, which the controller commits and then stops over, and never
    // finishes. The real tail ends in the wait for the first unfence, which a
    // dead controller cannot answer, so it lasts `startup_leader_wait_timeout`,
    // two minutes by default. The join is over by then, so this is the race
    // around the tail and not the one around the join.
    #[tokio::test]
    async fn a_fault_after_the_join_ends_a_start_that_is_still_running_its_tail() {
        let dir = tempdir().unwrap();
        let start = Broker::start_with_listeners_inner(
            BrokerConfig::for_tests(dir.path().to_path_buf()),
            None,
            Vec::new(),
            HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG),
            |joined: JoinedQuorum| async move {
                // The submit's own outcome is not what is under test: it
                // returns once the record commits, or fails once the
                // controller has stopped.
                let _ = joined
                    .controller
                    .submit_change(vec![finalize_unstable_metadata_version()])
                    .await;
                std::future::pending::<Result<BrokerHandle, BrokerError>>().await
            },
        );

        let started = tokio::time::timeout(std::time::Duration::from_secs(60), start)
            .await
            .expect("the fault did not end a start that was still in its tail");

        let Err(refused) = started else {
            panic!("a start whose controller stopped over a fatal fault returned a handle");
        };
        assert!(
            matches!(&refused, BrokerError::FatalFault(fault) if fault == UNSUPPORTED_LEVEL_FAULT),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn start_recovers_existing_partition_dirs() {
        let dir = tempdir().unwrap();
        // Create a partition dir with a log inside.
        let part_dir = dir.path().join("foo-0");
        std::fs::create_dir(&part_dir).unwrap();
        {
            let _log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        }

        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        // We can't easily inspect the partition registry from outside the
        // crate yet, but starting cleanly is the assertion we need here.
        handle.shutdown().await;
    }
}
