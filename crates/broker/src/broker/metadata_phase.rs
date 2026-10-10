//! The metadata-quorum startup phase: raft transport wiring, controller or
//! observer bring-up, auto-join, and the leader wait that gates every later
//! phase. It is separate from the rest of startup because the metadata source
//! must exist before storage recovery, the coordinators, or the listeners.

use std::sync::Arc;

use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use tokio::net::TcpListener;

use crate::{
    broker::{
        endpoints::static_controller_voter_set,
        registration::{
            register_broker, register_controller, spawn_deferred_controller_registration,
            submit_stretch_defaults,
        },
    },
    config::BrokerConfig,
    error::BrokerError,
    metadata_source::or_fatal_fault,
};

struct RaftTransport {
    controller_cell: Arc<tokio::sync::OnceCell<Arc<krabka_raft::ControllerHandle>>>,
    /// Filled by `Broker::start` once the audit pipeline exists, so the
    /// controller listener's SASL logins reach the audit trail.
    audit_cell: crate::raft_handshake::AuditLogArc,
    handshake: Option<Arc<dyn krabka_raft::RaftListenerHandshake>>,
    dialer: Option<Arc<dyn krabka_raft::OutboundDialer>>,
    admin_router: Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
}

fn prepare_raft_transport(
    config: &BrokerConfig,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
) -> RaftTransport {
    let controller_cell = Arc::new(tokio::sync::OnceCell::new());
    let audit_cell: crate::raft_handshake::AuditLogArc = Arc::new(tokio::sync::OnceCell::new());
    // Only a controller binds the controller listener. A broker-only node
    // reads the quorum as an observer and has no such listener to warn about.
    if config.is_controller()
        && config.controller_listener_protocol == krabka_security::ListenerProtocol::Plaintext
    {
        tracing::warn!(
            "controller listener is PLAINTEXT: every peer is ANONYMOUS, and each \
             controller RPC is authorized for that principal"
        );
    }
    // The handshake runs on every protocol. On `PLAINTEXT` it does no
    // authentication, but it still gives each connection the grants that the
    // listener checks for every request.
    let tls_acceptor =
        tls_dynamic.map(|dynamic| tokio_rustls::TlsAcceptor::from(dynamic.current()));
    let handshake = Some(Arc::new(crate::raft_handshake::BrokerRaftHandshake {
        tls_acceptor,
        plain_credentials: config.plain_credentials.as_map().clone(),
        enabled_sasl_mechanisms: config.enabled_sasl_mechanisms.clone(),
        gssapi: config.gssapi.clone(),
        oauthbearer_validator: config.oauthbearer_validator.clone(),
        oauthbearer_jwks_cache_generation: Arc::clone(&config.oauthbearer_jwks_cache_generation),
        oauthbearer_jwks_last_successful_fetch_ms: Arc::clone(
            &config.oauthbearer_jwks_last_successful_fetch_ms,
        ),
        protocol: config.controller_listener_protocol,
        controller: Arc::clone(&controller_cell),
        delegation_token_secret_key: config.delegation_token_secret_key.clone(),
        audit_log: Arc::clone(&audit_cell),
        sasl_max_receive_bytes: config.sasl_server_max_receive.bytes_usize(),
        failed_authentication_delay: config.failed_authentication_delay(),
        authorizer: Arc::clone(&config.authorizer),
        principal_mapper: config.tls_principal_mapper.clone(),
    }) as Arc<dyn krabka_raft::RaftListenerHandshake>);
    let server_name = config
        .controller_server_name
        .clone()
        .unwrap_or_else(|| "localhost".to_owned());
    let dialer = Arc::new(crate::network::client::InterBrokerDialer::new(
        Arc::clone(inter_broker_client),
        config.controller_listener_protocol,
        server_name,
    )) as Arc<dyn krabka_raft::OutboundDialer>;
    RaftTransport {
        controller_cell,
        audit_cell,
        handshake,
        dialer: Some(dialer),
        admin_router: config
            .is_controller()
            .then(|| Arc::new(crate::controller_admin::BrokerControllerAdminRouter::new())),
    }
}

fn prepare_initial_voters(
    config: &BrokerConfig,
    bootstrap_records: &[krabka_metadata::MetadataRecord],
) -> krabka_metadata::VoterSet {
    let voters = crate::bootstrap::initial_voters(bootstrap_records);
    if !voters.is_empty() || config.controller_quorum_voters.is_empty() {
        return voters;
    }
    tracing::info!(
        node_id = config.node_id.0,
        voter_count = config.controller_quorum_voters.len(),
        mode = ?config.bootstrap_mode,
        "deriving static KIP-595 voters from controller_quorum_voters"
    );
    static_controller_voter_set(
        &config.controller_quorum_voters,
        config.node_id,
        config.directory_id,
    )
}

/// The bootstrap metadata this controller writes when it activates on an
/// empty metadata log: Kafka's `BootstrapMetadata`.
///
/// The KIP-853 control records are not metadata. A dynamic format keeps them
/// in the bootstrap checkpoint, and the leader writes them in the
/// `LeaderChange` batch of its first epoch. So they are not in the list.
///
/// A stream without a feature level gets the feature levels of the latest
/// production release, as Kafka's `BootstrapMetadata.fromDirectory` falls
/// back to the default bootstrap without a `bootstrap.checkpoint`. A stream
/// with feature levels is the one `krabka format --feature` selected. Release
/// defaults appended to it would replay after the selected levels and
/// overwrite them.
fn controller_bootstrap_records(
    mut records: Vec<krabka_metadata::MetadataRecord>,
) -> Vec<krabka_metadata::MetadataRecord> {
    records.retain(|record| {
        !matches!(
            record,
            krabka_metadata::MetadataRecord::V1Voters(_)
                | krabka_metadata::MetadataRecord::V1KRaftVersion(_)
        )
    });
    if !records
        .iter()
        .any(|record| matches!(record, krabka_metadata::MetadataRecord::V1FeatureLevel(_)))
    {
        records.extend(krabka_metadata::bootstrap_feature_records(
            crate::features::LATEST_PRODUCTION_METADATA_VERSION,
        ));
    }
    records
}

/// Validate `metadata_snapshot_fetch_max` for the observer's snapshot
/// transfer, the same way a controller validates it for its follower path: a
/// deployment may lower the 1 GiB ceiling but cannot raise it.
fn observer_snapshot_fetch_max(
    config: &BrokerConfig,
) -> Result<krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax, BrokerError> {
    krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax::new(
        config.metadata_snapshot_fetch_max,
    )
    .map_err(BrokerError::Startup)
}

/// Metadata publication and its optional controller administration endpoint.
pub(super) struct MetadataControlPlane {
    pub(super) source: Arc<dyn crate::metadata_source::MetadataSource>,
    pub(super) admin_router: Option<Arc<crate::controller_admin::BrokerControllerAdminRouter>>,
}

/// Handles owned by startup after metadata quorum readiness has completed.
pub(super) struct MetadataPhase {
    pub(super) control_plane: MetadataControlPlane,
    pub(super) audit: crate::raft_handshake::AuditLogArc,
}

async fn start_metadata_source(
    config: &BrokerConfig,
    bootstrap_records: Vec<krabka_metadata::MetadataRecord>,
    controller_listener: Option<tokio::net::TcpListener>,
    transport: RaftTransport,
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
) -> Result<MetadataControlPlane, BrokerError> {
    let RaftTransport {
        controller_cell,
        audit_cell: _,
        handshake,
        dialer,
        admin_router,
    } = transport;
    if config.is_controller() {
        let controller_config = krabka_raft::ControllerConfig {
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            node_id: config.node_id,
            bootstrap_servers: config.bootstrap_servers.clone(),
            directory_id: config.directory_id,
            auto_join: config.auto_join,
            observer_lag_bound: config.observer_lag_bound,
            initial_voters: prepare_initial_voters(config, &bootstrap_records),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.metadata_dir().to_path_buf(),
            election_timeout: config.controller_election_timeout,
            heartbeat_interval: config
                .controller_heartbeat_interval_explicit
                .then_some(config.controller_heartbeat_interval),
            controller_fetch_miss_limit: config.controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity: config.metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max: config.metadata_raft_fetch_max,
            client_id: format!("krabka-broker-{}-controller", config.broker_id),
            bootstrap_mode: config.bootstrap_mode,
            bootstrap_records: controller_bootstrap_records(bootstrap_records),
            // Kafka's `ControllerServer` reads the static `min.insync.replicas`
            // from this node's own configuration.
            default_min_insync_replicas: config.default_min_insync_replicas,
            cluster_id: config.cluster_id,
            dialer,
            handshake,
            shard_router: Some(Arc::new(crate::wal::quorum::registry::WalShardRouter::new(
                wal_shards,
            ))),
            admin_router: admin_router
                .clone()
                .map(|router| router as Arc<dyn krabka_raft::ControllerAdminRouter>),
            unstable_api_versions: config.features.unstable_api_versions,
            unstable_feature_versions: config.features.unstable_feature_versions,
            // Kafka's `ControllerServer` gives its `SocketServer` the same
            // settings a broker listener gets, with the idle window of the
            // controller listener's own name.
            listener_limits: krabka_raft::ListenerLimits {
                max_request_size: config.socket_request_max,
                max_idle: config
                    .connections_max_idle_for(crate::controller_endpoint::CONTROLLER_LISTENER_NAME),
                max_connections: config.max_connections,
                max_connections_per_ip: config.max_connections_per_ip,
            },
            max_bytes_between_snapshots: config.metadata_max_bytes_between_snapshots,
            max_snapshot_interval: config.metadata_max_snapshot_interval,
            snapshot_interval_records: config.metadata_snapshot_interval_records,
            metadata_snapshot_fetch_max: config.metadata_snapshot_fetch_max,
            metadata_log: config.metadata_log,
        };
        let controller = Arc::new(
            krabka_raft::Controller::start_with_listener(controller_config, controller_listener)
                .await
                .map_err(|error| match error {
                    // The refusal of a log that finalized an unsupported
                    // feature level. It reads the same as the fault that a
                    // running controller publishes, as Kafka's handler logs
                    // both alike.
                    krabka_raft::RaftError::FatalFault(fault) => BrokerError::FatalFault(fault),
                    other => BrokerError::Startup(other.to_string()),
                })?,
        );
        let _ = controller_cell.set(Arc::clone(&controller));
        return Ok(MetadataControlPlane {
            source: controller as Arc<dyn crate::metadata_source::MetadataSource>,
            admin_router,
        });
    }

    drop(controller_listener);
    let dialer = dialer.expect("broker-only node requires a raft dialer");
    let observer = crate::metadata_observer::MetadataObserver::start(
        crate::metadata_observer::ObserverConfig {
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            voters: config.controller_quorum_voters.clone(),
            bootstrap_servers: config.bootstrap_servers.clone(),
            dialer: Arc::clone(&dialer),
            client_id: format!("krabka-broker-{}-observer", config.broker_id),
            cluster_id: config.cluster_id.unwrap_or_else(uuid::Uuid::nil),
            node_id: config.node_id,
            directory_id: config.directory_id,
            // The metadata partition directory. The observer keeps its
            // checkpoints in a subdirectory of their own, never beside the
            // controller's: an observer checkpoint has no log to match its
            // boundary, so a controller must not load one. See
            // `metadata_observer::store`.
            data_dir: krabka_raft::metadata_partition_dir(config.metadata_dir()),
            snapshot_interval_records: config.metadata_snapshot_interval_records,
            snapshot_fetch_max: observer_snapshot_fetch_max(config)?,
            max_bytes: config.observer_fetch_max,
            poll_interval: config.observer_poll_interval,
            timer: crate::time_util::system_timer(),
        },
    );
    let forwarder = crate::metadata_source::QuorumForwarder {
        client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
        client_frame_max: config.client_frame_max,
        voters: config.controller_quorum_voters.clone(),
        bootstrap_servers: config.bootstrap_servers.clone(),
        image: observer.watch_image(),
        dialer,
        client_id: format!("krabka-broker-{}-writer", config.broker_id),
        leader: observer.watch_leader(),
    };
    Ok(MetadataControlPlane {
        source: Arc::new(crate::metadata_source::ObserverSource::new(
            observer,
            Arc::new(forwarder),
        )),
        admin_router: None,
    })
}

fn spawn_auto_join(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
) {
    if !config.is_controller() {
        return;
    }
    let listener_protocol = config.controller_listener_protocol;
    let params = crate::auto_join::AutoJoinParams {
        auto_join: config.auto_join,
        retry_backoff: config.auto_join_retry_backoff,
        voter_request_timeout: config.auto_join_voter_request_timeout,
        node_id: config.node_id,
        directory_id: config.directory_id,
        cluster_id: config.cluster_id,
        bootstrap_servers: config.bootstrap_servers.clone(),
        advertised_controller: config
            .controller_quorum_voters
            .iter()
            .find(|(id, _)| *id == config.node_id)
            .map(|(_, endpoint)| endpoint.clone()),
        listener_protocol,
        inter_broker_server_name: config
            .controller_server_name
            .clone()
            .unwrap_or_else(|| config.inter_broker_server_name.clone()),
        controller: Arc::clone(controller),
        inter_broker_client: Arc::clone(inter_broker_client),
    };
    tokio::spawn(crate::auto_join::run(params.clone()));
    tokio::spawn(crate::auto_join::run_voter_updates(params));
}

async fn wait_for_metadata_leader(
    controller: &dyn crate::metadata_source::MetadataSource,
    timeout: std::time::Duration,
) -> Result<(), BrokerError> {
    let mut leaders = controller.watch_leader();
    let deadline = std::time::Instant::now() + timeout;
    while leaders.borrow().is_none() {
        if std::time::Instant::now() > deadline {
            return Err(BrokerError::Startup(format!(
                "no leader elected within {timeout:?}"
            )));
        }
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(100), leaders.changed()).await;
    }
    Ok(())
}

/// Waits until the metadata image of this node finalizes `metadata.version`.
///
/// The bootstrap records finalize it, and the active controller writes them
/// to an empty log. A broker-only node reads them from that log.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] when the image does not finalize
/// `metadata.version` within `timeout`, or when the image channel closes
/// before it does.
async fn wait_for_metadata_version(
    controller: &dyn crate::metadata_source::MetadataSource,
    timeout: std::time::Duration,
) -> Result<(), BrokerError> {
    let mut images = controller.watch_image();
    match tokio::time::timeout(
        timeout,
        images.wait_for(|image| image.finalized_metadata_version().is_some()),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => Err(BrokerError::Startup(
            "the metadata image closed before it finalized metadata.version".into(),
        )),
        Err(_) => Err(BrokerError::Startup(format!(
            "the metadata image did not finalize metadata.version within {timeout:?}"
        ))),
    }
}

/// Binds the controller listener before the quorum starts when the config asks
/// for an OS-assigned port, and publishes the port it got.
///
/// This node's own voter endpoint is where every heartbeat, `AssignReplicasToDirs`
/// and raft peer reaches its controller listener. A `:0` endpoint names nothing
/// that a client can dial. So the bound address replaces the configured one in
/// `controller_listen_addr` and in this node's `controller_quorum_voters`
/// entry, before the initial voter set and every client that reads it are
/// built. The live listener is handed on to the controller, so no other
/// process can take the port in between.
///
/// A caller-supplied listener, a concrete port, and a node without the
/// controller role keep their config as it is.
async fn bind_ephemeral_controller_listener(
    config: &mut BrokerConfig,
    prebound: Option<TcpListener>,
) -> Result<Option<TcpListener>, BrokerError> {
    if prebound.is_some() || !config.is_controller() || config.controller_listen_addr.port() != 0 {
        return Ok(prebound);
    }
    let listener = crate::platform::bind_listener(config.controller_listen_addr).await?;
    let bound = crate::platform::Sockets::TARGET
        .listener_address(&listener, config.controller_listen_addr)?;
    publish_bound_controller_addr(config, bound);
    Ok(Some(listener))
}

/// Writes `bound` into `controller_listen_addr` and into this node's own voter
/// entry when that entry asks for port 0. The entry keeps its host. Entries of
/// other nodes, and an entry with a concrete port, stay as configured.
fn publish_bound_controller_addr(config: &mut BrokerConfig, bound: std::net::SocketAddr) {
    config.controller_listen_addr = bound;
    let node_id = config.node_id;
    for (voter, endpoint) in &mut config.controller_quorum_voters {
        if *voter != node_id {
            continue;
        }
        if let Some((host, 0)) = crate::host_port::parse_host_port(endpoint) {
            *endpoint = format!("{host}:{}", bound.port());
        }
    }
}

pub(super) async fn start_metadata_phase(
    config: &mut BrokerConfig,
    controller_listener: Option<TcpListener>,
    tls_dynamic: Option<&Arc<krabka_security::DynamicServerConfig>>,
    inter_broker_client: &Arc<crate::network::client::InterBrokerClient>,
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
) -> Result<MetadataPhase, BrokerError> {
    let controller_listener =
        bind_ephemeral_controller_listener(config, controller_listener).await?;
    let transport = prepare_raft_transport(config, tls_dynamic, inter_broker_client);
    let audit_cell = Arc::clone(&transport.audit_cell);
    // Kafka's controller loads the bootstrap metadata from
    // `metadata.log.dir`, which is where `krabka-format` writes it. A broker
    // never reads it: it applies the records the controller writes to the
    // log.
    let bootstrap_records = if config.is_controller() {
        crate::bootstrap::load_bootstrap_records(config.metadata_dir())?
    } else {
        Vec::new()
    };
    let controller = start_metadata_source(
        config,
        bootstrap_records,
        controller_listener,
        transport,
        wal_shards,
    )
    .await?;
    spawn_auto_join(config, &controller.source, inter_broker_client);
    // A controller that stops itself over a fatal fault fails every later
    // submit with a bare "controller shut down", and each submit retries under
    // backoff first. Kafka's process halts on that fault at once and with its
    // message, so the fault ends the join and is what a failed start reports.
    or_fatal_fault(
        controller.source.watch_fatal(),
        join_metadata_quorum(config, &controller.source),
    )
    .await?;
    Ok(MetadataPhase {
        control_plane: controller,
        audit: audit_cell,
    })
}

/// Waits for the metadata leader and, on a broker, for the bootstrap
/// records, then registers this node with the leader.
///
/// The active controller writes the bootstrap records when it activates on
/// an empty log, so no node submits them. A broker waits until its image
/// finalizes `metadata.version`, as Kafka's broker publishes no metadata
/// before its `MetadataLoader` has caught up. A controller-only node does not
/// wait: [`register_controller`] defers its registration until the image
/// finalizes a `metadata.version` that supports it.
async fn join_metadata_quorum(
    config: &mut BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> Result<(), BrokerError> {
    let timeout = config.startup_leader_wait_timeout.to_std();
    wait_for_metadata_leader(&**controller, timeout).await?;
    if config.is_broker() {
        wait_for_metadata_version(&**controller, timeout).await?;
    }
    if config.is_controller() || config.is_broker() {
        config.incarnation_id = crate::incarnation::load_or_generate(&config.log_dir);
        // Spend the clean-shutdown proof the last stop left, if it left one.
        // Reading it here -- before this node registers -- is what lets
        // `register_broker` tell a graceful restart from a crash.
        config.previous_broker_epoch = crate::clean_shutdown::take(&config.log_dir);
    }
    submit_stretch_defaults(config, &**controller).await?;
    register_controller(config, &**controller).await?;
    if let Some(epoch) = register_broker(config, &**controller).await? {
        config.broker_epoch = epoch;
    }
    spawn_deferred_controller_registration(config, controller);
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::NodeId;
    use krabka_security::ListenerProtocol;
    use tracing::Level;

    use super::*;
    use crate::{config::NodeRole, test_support::LogCapture};

    // A node warns that its controller listener is PLAINTEXT only when it has
    // one, which only a node with the controller role binds.
    #[test]
    fn only_a_controller_warns_about_a_plaintext_controller_listener() {
        let client = Arc::new(crate::network::client::InterBrokerClient::new(None, None));
        let warning = (
            Level::WARN,
            "controller listener is PLAINTEXT: every peer is ANONYMOUS, and each controller RPC \
             is authorized for that principal"
                .to_string(),
        );
        let combined = vec![NodeRole::Broker, NodeRole::Controller];
        let cases = [
            (
                combined.clone(),
                ListenerProtocol::Plaintext,
                vec![warning.clone()],
            ),
            (
                vec![NodeRole::Controller],
                ListenerProtocol::Plaintext,
                vec![warning],
            ),
            (vec![NodeRole::Broker], ListenerProtocol::Plaintext, vec![]),
            (combined, ListenerProtocol::Ssl, vec![]),
        ];
        let mut warned_rows = Vec::new();
        let mut expected_rows = Vec::new();
        for (roles, protocol, expected) in cases {
            let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
            config.roles.clone_from(&roles);
            config.controller_listener_protocol = protocol;
            let capture = LogCapture::default();
            tracing::dispatcher::with_default(&capture.dispatch(), || {
                LogCapture::span().in_scope(|| prepare_raft_transport(&config, None, &client));
            });
            let warned: Vec<(Level, String)> = capture
                .events()
                .into_iter()
                .filter(|event| event.level <= Level::WARN)
                .map(|event| (event.level, event.message))
                .collect();
            warned_rows.push((roles.clone(), protocol, warned));
            expected_rows.push((roles, protocol, expected));
        }
        assert!(warned_rows == expected_rows);
    }

    /// The controller writes the metadata of the bootstrap stream without
    /// its KIP-853 controls, and the release defaults when the stream sets no
    /// feature level.
    #[test]
    fn the_controller_bootstraps_from_the_streams_metadata_or_the_release_defaults() {
        use krabka_metadata::{
            FeatureLevelRecord, KRaftVersionRecord, MetadataRecord, VotersRecord,
        };

        let selected = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 21,
        });
        let controls = [
            MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
            MetadataRecord::V1Voters(VotersRecord {
                voters: krabka_metadata::VoterSet::default(),
            }),
        ];
        let defaults = krabka_metadata::bootstrap_feature_records(
            crate::features::LATEST_PRODUCTION_METADATA_VERSION,
        );
        // (what, the bootstrap stream, the records the controller writes)
        let cases = [
            ("no stream", vec![], defaults.clone()),
            (
                "a formatted stream",
                vec![selected.clone()],
                vec![selected.clone()],
            ),
            (
                "a stream of controls and a selected level",
                vec![controls[0].clone(), selected.clone(), controls[1].clone()],
                vec![selected],
            ),
            ("a stream of controls alone", controls.to_vec(), defaults),
        ];
        for (what, stream, written) in cases {
            assert!(controller_bootstrap_records(stream) == written, "{what}");
        }
    }

    /// A broker waits until its image finalizes `metadata.version`, which the
    /// active controller writes, and stops the start when the image does not
    /// finalize it in time. Records that set no `metadata.version` do not end
    /// the wait.
    #[tokio::test(start_paused = true)]
    async fn a_broker_waits_until_its_image_finalizes_the_metadata_version() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};

        use crate::test_support::FakeMetadataSource;

        let feature = |name: &str| {
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level: 1,
            })
        };
        let timeout = std::time::Duration::from_secs(5);
        let refusal = "startup failed: the metadata image did not finalize metadata.version \
                       within 5s"
            .to_owned();
        // (what, the records the image gets after a second, the wait's outcome)
        let cases = [
            (
                "the bootstrap records",
                vec![feature("metadata.version"), feature("group.version")],
                Ok(()),
            ),
            (
                "a feature level that is not metadata.version",
                vec![feature("group.version")],
                Err(refusal.clone()),
            ),
            ("nothing", vec![], Err(refusal)),
        ];
        for (what, records, outcome) in cases {
            let source = Arc::new(FakeMetadataSource::builder().build());
            let waiting = tokio::spawn({
                let source = Arc::clone(&source);
                async move {
                    wait_for_metadata_version(&*source, timeout)
                        .await
                        .map_err(|error| error.to_string())
                }
            });
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            assert!(!waiting.is_finished(), "{what}");
            source.set_records(&records);
            assert!(waiting.await.expect("the wait") == outcome, "{what}");
        }
    }

    #[test]
    fn the_bound_port_replaces_only_this_nodes_port_zero_endpoint() {
        let bound: std::net::SocketAddr = "127.0.0.1:40123".parse().expect("static");
        let cases = [
            (
                "own ip endpoint",
                vec![(1, "127.0.0.1:0"), (2, "127.0.0.1:0")],
                vec![(1, "127.0.0.1:40123"), (2, "127.0.0.1:0")],
            ),
            (
                "own host name keeps its host",
                vec![(1, "localhost:0")],
                vec![(1, "localhost:40123")],
            ),
            (
                "own bracketed ipv6 keeps its host",
                vec![(1, "[::1]:0")],
                vec![(1, "[::1]:40123")],
            ),
            (
                "own concrete port stays",
                vec![(1, "127.0.0.1:9093")],
                vec![(1, "127.0.0.1:9093")],
            ),
            ("no own entry", vec![(2, "peer:0")], vec![(2, "peer:0")]),
        ];
        for (name, configured, published) in cases {
            let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
            config.node_id = NodeId(1);
            config.controller_quorum_voters = configured
                .iter()
                .map(|&(id, endpoint)| (NodeId(id), endpoint.to_owned()))
                .collect();

            publish_bound_controller_addr(&mut config, bound);

            let want: Vec<(NodeId, String)> = published
                .iter()
                .map(|&(id, endpoint)| (NodeId(id), endpoint.to_owned()))
                .collect();
            assert!(
                (
                    config.controller_listen_addr,
                    config.controller_quorum_voters
                ) == (bound, want),
                "{name}"
            );
        }
    }
}
