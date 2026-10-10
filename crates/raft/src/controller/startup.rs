//! Cluster formation and controller start-up: the [`Controller`] factory that
//! validates the requested bootstrap mode against the on-disk log, opens or
//! recovers the engine, binds the controller listener, and the on-disk state
//! probe the broker binary shares with that validation.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use super::{
    ControllerHandle, checkpoint::load_latest_checkpoint, feature_check::unsupported_feature_level,
};
use crate::{
    config::{BootstrapMode, ControllerConfig, UnstableFeatureVersions},
    error::RaftError,
    kraft::KraftController,
    network::{OutboundDialer, PlaintextDialer, RealPeerSender},
    server,
};

/// Zero-sized factory for [`ControllerHandle`]s.
pub struct Controller;

impl Controller {
    /// Start a controller node, open the listener, and begin participating in
    /// the quorum.
    ///
    /// `bootstrap_mode` governs cluster formation: `Bootstrap` seeds a fresh
    /// quorum from `initial_voters`; `Join`/`Rejoin` recover or wait. Mismatches
    /// between mode and on-disk log state return [`RaftError::Startup`].
    ///
    /// # Errors
    /// Returns an error if configuration, storage recovery, or startup fails.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr` itself.
    /// The supplied listener's local address MUST equal
    /// `config.controller_listen_addr`.
    ///
    /// # Errors
    /// Returns an error if the listener, storage, or controller cannot start.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = config.node_id.0, mode = ?config.bootstrap_mode),
        err
    )]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        let metadata_snapshot_fetch_max =
            krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(
                config.metadata_snapshot_fetch_max,
            )
            .map_err(RaftError::Startup)?;

        // First-boot orchestration validates mode against on-disk log state.
        // The metadata log, its checkpoints and the quorum-state file all live
        // in the `__cluster_metadata-0` directory under `log_dir`, as Kafka's
        // do under `metadata.log.dir`.
        let data_dir = crate::config::metadata_partition_dir(&config.log_dir);
        let log_exists = metadata_log_nonempty(&data_dir);
        let snapshot_voters = load_latest_checkpoint(&data_dir)
            .and_then(|(_, bytes)| crate::snapshot::SnapshotReader::read(&bytes).ok())
            .and_then(|snapshot| snapshot.control_state.map(|state| state.voters))
            .unwrap_or_default();
        let voters = if config.initial_voters.is_empty() {
            snapshot_voters
        } else {
            config.initial_voters.clone()
        };
        let bootstrap_mode = effective_bootstrap_mode(
            config.bootstrap_mode,
            voters.is_empty(),
            config.auto_join || !config.bootstrap_servers.is_empty(),
        );
        match (bootstrap_mode, log_exists) {
            (BootstrapMode::Bootstrap, false) => {
                if voters.is_empty() {
                    return Err(RaftError::Startup(
                        "Bootstrap mode requires a non-empty initial_voters set".into(),
                    ));
                }
            }
            (BootstrapMode::Join, false) | (BootstrapMode::Rejoin, true) => {}
            (BootstrapMode::Bootstrap, true) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, false) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, true) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin"
                        .into(),
                ));
            }
        }

        let cluster_id = config.cluster_id.unwrap_or_else(Uuid::nil);
        let dialer: Arc<dyn OutboundDialer> = config
            .dialer
            .clone()
            .unwrap_or_else(|| Arc::new(PlaintextDialer));

        // The peer sender starts from the bootstrap view. The engine replaces
        // it immediately when it replays a dynamic voter control record.
        let peers = Arc::new(RealPeerSender::new(
            voters.clone(),
            &config.bootstrap_servers,
            config.client_id.clone(),
            Arc::clone(&dialer),
            config.client_dispatch_queue_capacity,
            config.client_frame_max,
        ));

        // Build / recover the engine. `Join` nodes with an empty log + empty
        // voter set sit unattached; `Bootstrap` seeds the static voter set.
        let engine = KraftController::open(
            data_dir.clone(),
            config.node_id,
            cluster_id,
            config.directory_id,
            voters.clone(),
            config.election_timeout,
            config.heartbeat_interval,
            config.controller_fetch_miss_limit,
            config.metadata_raft_command_queue_capacity,
            config.metadata_raft_fetch_max,
            peers,
            config.snapshot_interval_records,
            config.max_bytes_between_snapshots,
            config.max_snapshot_interval,
            metadata_snapshot_fetch_max,
            config.metadata_log,
            crate::kraft::Activation {
                bootstrap_records: config.bootstrap_records.clone(),
                default_min_insync_replicas: config.default_min_insync_replicas,
            },
        )?;

        // Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws
        // when a record's level is outside what this controller supports, so
        // a log finalized at an unstable level does not start under a node that
        // cannot serve it. The engine has replayed the log and the checkpoint
        // by now, and `open` has published the image that replay left.
        if let Some(refusal) =
            unsupported_feature_level(&engine.current_image(), config.unstable_feature_versions)
        {
            engine.shutdown().await;
            return Err(RaftError::FatalFault(refusal));
        }

        // Controller listener.
        let listener = match prebound {
            Some(l) => l,
            None => bind_controller_listener(config.controller_listen_addr)
                .await
                .map_err(|e| RaftError::Storage(krabka_log::LogError::Io(e)))?,
        };
        let actual_addr = listener_address(&listener, config.controller_listen_addr)
            .map_err(|e| RaftError::Storage(krabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let leader_rx = engine.watch_leader();
        let listener_task = tokio::spawn(server::run(
            listener,
            engine.clone(),
            shutdown.clone(),
            config.handshake.clone(),
            config.shard_router.clone(),
            config.admin_router.clone(),
            (
                server::Unstable {
                    api_versions: config.unstable_api_versions,
                },
                config.listener_limits,
            ),
        ));
        let (fatal_tx, fatal) = watch::channel(None);
        tokio::spawn(stop_on_fatal_fault(
            engine.clone(),
            shutdown.clone(),
            fatal_tx,
            config.unstable_feature_versions,
        ));
        info!(
            node_id = config.node_id.0,
            addr = %actual_addr,
            "controller started"
        );

        Ok(ControllerHandle {
            engine,
            leader: leader_rx,
            shutdown,
            fatal,
            listener_task: Mutex::new(Some(listener_task)),
            data_dir,
            client_id: config.client_id.clone(),
            client_dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            client_frame_max: config.client_frame_max,
            self_node_id: config.node_id,
            voters,
            staged_learners: std::sync::Mutex::new(BTreeMap::new()),
            dialer,
            controller_bound_addr: actual_addr,
        })
    }
}

/// The mode a node starts in once it knows whether it has initial voters.
///
/// A fresh node with no voters, neither configured nor in its bootstrap
/// checkpoint, is a controller formatted with `--no-initial-controllers`.
/// Kafka starts it as an observer that finds the leader through
/// `controller.quorum.bootstrap.servers`. Auto-join or `kafka-metadata-quorum
/// add-controller` makes it a voter later. So `Bootstrap` becomes `Join` for a
/// node that can reach the quorum: it has a bootstrap server, or it auto-joins,
/// which needs one. A node with neither stays in `Bootstrap`, which refuses
/// the empty voter set.
fn effective_bootstrap_mode(
    requested: BootstrapMode,
    no_initial_voters: bool,
    reaches_quorum: bool,
) -> BootstrapMode {
    if requested == BootstrapMode::Bootstrap && no_initial_voters && reaches_quorum {
        BootstrapMode::Join
    } else {
        requested
    }
}

/// Stops the controller over a fatal fault, as Kafka's fatal fault handler
/// does. Three faults are fatal:
///
/// - The replay of a feature level that the controller does not support, as
///   Kafka's fatal fault on a `FeatureControlManager` replay exception. The
///   engine publishes each image it applies, so a node that follows a leader
///   which finalized a level above its own range sees it here and stops
///   serving, and never runs at a level it did not advertise.
/// - The failure of the metadata log directory: a write to it that returned
///   an I/O error. Kafka shuts the node down when its metadata log directory
///   fails (KIP-858), and the engine has already stopped taking part in the
///   quorum.
/// - A controller activation that failed: a new leader of an empty log that
///   could not write the bootstrap records. Kafka's `QuorumController` gives
///   that failure to its `fatalFaultHandler` as `exception while completing
///   controller activation`, and the engine has already stopped.
///
/// The fault goes out on `fatal` first, as [`ControllerHandle::watch_fatal`]
/// documents, so that a process hosting the controller can halt over it, as
/// Kafka's `ProcessTerminatingFaultHandler` does. It is in place before any
/// later submit can fail with [`RaftError::Shutdown`].
async fn stop_on_fatal_fault(
    engine: KraftController,
    shutdown: CancellationToken,
    fatal: watch::Sender<Option<String>>,
    unstable: UnstableFeatureVersions,
) {
    let mut images = engine.watch_image();
    let mut faults = engine.watch_fault();
    loop {
        let (refusal, engine_stopped) = tokio::select! {
            () = shutdown.cancelled() => return,
            changed = images.changed() => match changed {
                Ok(()) => (
                    unsupported_feature_level(&images.borrow_and_update(), unstable),
                    false,
                ),
                Err(_) => (None, true),
            },
            changed = faults.changed() => (None, changed.is_err()),
        };
        if let Some(refusal) = refusal {
            tracing::error!(%refusal, "controller stopping: it replayed an unsupported feature level");
            fatal.send_replace(Some(refusal));
        } else if let Some(fault) = faults.borrow().clone() {
            // The engine publishes the fault and then stops, so the fault can
            // arrive together with the end of the image channel.
            tracing::error!(%fault, "controller stopping over a fault of its engine");
            fatal.send_replace(Some(fault));
        } else if engine_stopped {
            return;
        } else {
            continue;
        }
        shutdown.cancel();
        engine.shutdown().await;
        return;
    }
}

/// Bind the controller listener on `addr`.
#[cfg(not(target_os = "wasi"))]
async fn bind_controller_listener(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr).await
}

/// WASI preview 1 has no `bind`: the embedder adopts a preopened socket and
/// passes it to [`Controller::start_with_listener`].
#[cfg(target_os = "wasi")]
fn bind_controller_listener(
    addr: SocketAddr,
) -> std::future::Ready<std::io::Result<tokio::net::TcpListener>> {
    std::future::ready(Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "cannot bind the controller listener on {addr}: this platform has no bind; \
             pass a preopened listener to Controller::start_with_listener"
        ),
    )))
}

/// The address `listener` is bound to.
///
/// WASI preview 1 has no `getsockname`, so there the configured address
/// stands in. [`Controller::start_with_listener`] requires the two to be
/// equal.
fn listener_address(
    listener: &tokio::net::TcpListener,
    configured: SocketAddr,
) -> std::io::Result<SocketAddr> {
    if cfg!(target_os = "wasi") {
        Ok(configured)
    } else {
        listener.local_addr()
    }
}

/// True when the metadata partition directory `dir` already holds durable
/// raft state, which a node that ran before leaves. A quorum-state file or a
/// log segment that holds bytes is such state.
///
/// `dir` is the metadata partition directory,
/// `<metadata.log.dir>/__cluster_metadata-0`. The broker binary's
/// `detect_bootstrap_mode` calls this so its Bootstrap/Rejoin choice can never
/// disagree with [`Controller::start_with_listener`]'s mode validation.
/// `KraftController::open` creates an empty active segment before the first
/// election, so an empty segment is not state: a node killed mid-election,
/// with that segment but no `quorum-state` yet, reads as un-formatted and
/// re-Bootstraps rather than dying with "Rejoin requires non-empty raft log".
#[must_use]
pub fn metadata_log_nonempty(dir: &std::path::Path) -> bool {
    let qs = dir.join("quorum-state");
    if qs.exists() {
        return true;
    }
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries.flatten().any(|e| {
            e.path().extension().is_some_and(|ext| ext == "log")
                && e.metadata().is_ok_and(|metadata| metadata.len() > 0)
        })
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{
        controller::test_support::{submit_change_with_timeout, wait_for_leader},
        types::NodeId,
    };

    #[test]
    fn metadata_log_nonempty_detects_quorum_state_and_log_segments_only() {
        for (_case, file, expected) in [
            ("empty directory", None, false),
            (
                "quorum state file",
                Some(("quorum-state", b"state".as_slice())),
                true,
            ),
            (
                "log segment",
                Some(("00000000000000000000.log", b"log".as_slice())),
                true,
            ),
            (
                "empty log segment",
                Some(("00000000000000000000.log", b"".as_slice())),
                false,
            ),
            (
                "non-log extension",
                Some(("00000000000000000000.txt", b"log".as_slice())),
                false,
            ),
        ] {
            let dir = TempDir::new().unwrap();
            if let Some((name, contents)) = file {
                std::fs::write(dir.path().join(name), contents).unwrap();
            }
            assert2::assert!(metadata_log_nonempty(dir.path()) == expected);
        }
    }

    /// A write to the metadata log directory that fails stops the controller
    /// with a fatal fault, as Kafka shuts a node down when its metadata log
    /// directory fails (KIP-858).
    #[tokio::test]
    async fn a_failed_metadata_log_directory_stops_the_controller() {
        let dir = TempDir::new().unwrap();
        let ctrl = Controller::start(ControllerConfig::for_tests(
            NodeId(1),
            dir.path().to_path_buf(),
        ))
        .await
        .expect("start");
        wait_for_leader(&ctrl).await;
        // The committed offset is written to this file on every advance. A
        // directory in its place makes the next write fail with an I/O error.
        let high_watermark = crate::metadata_partition_dir(dir.path()).join("high-watermark");
        let _ = std::fs::remove_file(&high_watermark);
        std::fs::create_dir(&high_watermark).unwrap();

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ctrl.submit_change(vec![krabka_metadata::MetadataRecord::V1Topic(
                crate::test_support::single_partition_topic("after-the-failure", Uuid::new_v4()),
            )]),
        )
        .await;
        let mut fatal = ctrl.watch_fatal();
        let fault = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fatal.wait_for(Option::is_some),
        )
        .await
        .expect("the controller publishes a fatal fault")
        .expect("the fault channel stays open until the fault")
        .clone();

        assert2::check!(fault.is_some_and(|fault| fault.starts_with(&format!(
            "the metadata log directory {} has failed: ",
            dir.path().display()
        ))));
        ctrl.shutdown().await;
    }

    /// A controller whose activation records its own leader refuses stops
    /// with a fatal fault, as Kafka's `QuorumController` halts the process
    /// with `exception while completing controller activation`. A process
    /// that hosts it reads the fault from `watch_fatal`.
    #[tokio::test]
    async fn a_refused_activation_stops_the_controller() {
        let dir = TempDir::new().unwrap();
        let ctrl = Controller::start(ControllerConfig {
            bootstrap_records: vec![
                krabka_metadata::MetadataRecord::V1FeatureLevel(
                    krabka_metadata::FeatureLevelRecord {
                        name: "metadata.version".into(),
                        level: crate::LATEST_PRODUCTION_METADATA_VERSION,
                    },
                ),
                // No record creates the topic, so the leader refuses this one.
                krabka_metadata::MetadataRecord::V1Partition(
                    crate::test_support::single_replica_partition(
                        "missing",
                        krabka_ids::PartitionIndex(0),
                        NodeId(1),
                    ),
                ),
            ],
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        })
        .await
        .expect("start");
        let mut fatal = ctrl.watch_fatal();
        let fault = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fatal.wait_for(Option::is_some),
        )
        .await
        .expect("the controller publishes a fatal fault")
        .expect("the fault channel stays open until the fault")
        .clone();

        assert2::check!(
            fault.as_deref()
                == Some(
                    "exception while completing controller activation: metadata: unknown topic 'missing'"
                )
        );
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let (dir, ctrl) =
            crate::controller::test_support::bootstrap_controller("first bootstrap ok").await;
        // Drive a commit so the log is non-empty on the second boot.
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![krabka_metadata::MetadataRecord::V1Topic(
                crate::test_support::single_partition_topic("seed", Uuid::new_v4()),
            )],
            "bootstrap seed",
        )
        .await
        .expect("submit");
        ctrl.shutdown().await;

        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg2).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Bootstrap on existing log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn rejoin_on_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Rejoin,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        match Controller::start(cfg).await {
            Err(err) => assert2::assert!(matches!(err, RaftError::Startup(_))),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Rejoin on empty log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_unattached() {
        let (_dir, ctrl) =
            crate::controller::test_support::joining_controller("Join on empty log starts ok")
                .await;
        // Without voters this node never elects.
        assert2::assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_with_empty_voters_and_auto_join_falls_back_to_join() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
            auto_join: true,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Bootstrap with empty voters and auto_join should fall back to Join");
        ctrl.shutdown().await;
    }

    /// Only a fresh node with no voters and a way to reach the quorum swaps
    /// `Bootstrap` for `Join`; every other request stands.
    #[test]
    fn a_voterless_node_that_reaches_the_quorum_joins_it() {
        use BootstrapMode::{Bootstrap, Join, Rejoin};
        // (requested, no initial voters, reaches the quorum, effective)
        let cases = [
            (Bootstrap, true, true, Join),
            (Bootstrap, true, false, Bootstrap),
            (Bootstrap, false, true, Bootstrap),
            (Bootstrap, false, false, Bootstrap),
            (Join, true, true, Join),
            (Join, false, false, Join),
            (Rejoin, true, true, Rejoin),
            (Rejoin, false, false, Rejoin),
        ];
        let effective: Vec<_> = cases
            .iter()
            .map(|&(requested, no_voters, reaches, _)| {
                effective_bootstrap_mode(requested, no_voters, reaches)
            })
            .collect();
        let expected: Vec<_> = cases.iter().map(|&(.., mode)| mode).collect();
        assert2::assert!(effective == expected);
    }

    /// A controller formatted with `--no-initial-controllers` and pointed at
    /// the quorum through bootstrap servers starts as an observer, as Kafka's
    /// does, rather than refusing its empty voter set.
    #[tokio::test]
    async fn bootstrap_with_empty_voters_and_bootstrap_servers_falls_back_to_join() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
            auto_join: false,
            bootstrap_servers: vec!["127.0.0.1:1".to_owned()],
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("a voterless controller with bootstrap servers starts as an observer");
        assert2::assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }

    async fn check_empty_voter_startup_refused(
        mode: BootstrapMode,
        auto_join: bool,
        message: &str,
    ) {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: mode,
            initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
            auto_join,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let res = Controller::start(cfg).await;
        assert2::assert!(matches!(res, Err(RaftError::Startup(ref msg)) if msg.contains(message)));
    }

    #[tokio::test]
    async fn bootstrap_with_empty_voters_and_no_auto_join_errors() {
        check_empty_voter_startup_refused(BootstrapMode::Bootstrap, false, "initial_voters set")
            .await;
    }

    #[tokio::test]
    async fn rejoin_with_auto_join_on_empty_log_errors() {
        check_empty_voter_startup_refused(
            BootstrapMode::Rejoin,
            true,
            "Rejoin mode requires non-empty",
        )
        .await;
    }
}
