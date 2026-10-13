//! The broker's startup sequence, from the parsed command line through to a
//! controlled shutdown.

use {
    crate::{
        bootstrap::detect_bootstrap_mode,
        cli::Args,
        config::{parse_optional_listen_addr, parse_roles_arg},
        signals::wait_for_termination_signal,
    },
    clap::Parser,
    krabka_broker::{
        Broker, BrokerConfig, BrokerError, BrokerHandle,
        file_config::FileConfig,
        telemetry::{OtlpProtocol, TelemetryGuard},
    },
    krabka_units::{Time, convert::TimeExt as _},
    std::future::Future,
};

/// Where the broker's own client metrics go, when OTLP is on: the endpoint and
/// the protocol of the exporter that the telemetry setup has just built.
struct ClientMetricsOtlp {
    endpoint: Option<String>,
    protocol: OtlpProtocol,
}

/// The config that startup assembles, and the controlled-shutdown drain
/// timeout the file and the flags settle on.
struct AssembledConfig {
    config: BrokerConfig,
    controlled_shutdown_drain_timeout: Time,
}

#[tokio::main]
pub async fn broker_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();

    if args.print_config_schema {
        let schema = krabka_broker::file_config::config_schema();
        println!("{}", serde_json::to_string_pretty(&schema)?);
        return Ok(());
    }

    // The file can name this node's id, and the telemetry resource below is
    // the first thing that reports it.
    let file_config = args.load_config_file()?;

    // Install the tracing subscriber — stdout `fmt` plus an
    // optional OTLP export layer. OTLP stays off unless the environment
    // opts in (see `krabka_broker::telemetry`). Built here, inside the
    // tokio runtime, so the gRPC exporter captures the runtime handle.
    let otlp = krabka_broker::telemetry::OtlpConfig::from_env(
        |k| args.telemetry_value(k),
        &args.broker_id.to_string(),
        env!("CARGO_PKG_VERSION"),
        "krabka-broker",
    )?;
    let client_metrics_otlp = ClientMetricsOtlp {
        endpoint: otlp.as_ref().map(|cfg| cfg.endpoint.clone()),
        protocol: otlp.as_ref().map_or(OtlpProtocol::Grpc, |cfg| cfg.protocol),
    };
    let telemetry = krabka_broker::telemetry::init(
        otlp,
        // The stdout filter. This is the one `BROKER_LOGGER` retargets, and
        // the one whose target directives seed the logger list.
        krabka_broker::config::DEFAULT_LOG_FILTER,
        "info,krabka_broker::request=debug,krabka_log=info",
        "krabka-broker",
    )?;

    let outcome = Box::pin(run(args, file_config, client_metrics_otlp, &telemetry)).await;

    if let Err(error) = &outcome
        && is_fatal_fault(error.as_ref())
    {
        // Kafka's `ProcessTerminatingFaultHandler` logs the fault and halts the
        // process with status 1 (`Exit.halt`), which runs no shutdown hook and
        // waits for nothing. Returning from here would drop the tokio runtime,
        // and that drop waits for every `spawn_blocking` task, the partition
        // writers and the fetch reads among them. A task that hangs would keep
        // the process alive with a dead controller behind it, so the process
        // exits here, once the log line and the telemetry are out.
        tracing::error!(%error, "halting: the metadata controller met a fatal fault");
        telemetry.shutdown();
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
    telemetry.shutdown();
    outcome
}

/// Whether `error` is the fatal fault of the metadata controller that this
/// node hosts, the one failure that Kafka halts the process over.
fn is_fatal_fault(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<BrokerError>(),
        Some(BrokerError::FatalFault(_))
    )
}

/// The open-file limit to set in place of `limit`: the hard limit as the soft
/// one, when the soft one is lower and the hard one is finite.
///
/// A broker holds the log and index files of every segment it hosts open, so
/// the soft limit of 1024 that a login shell or a systemd unit usually starts
/// it with runs out at a few hundred partitions. The JVM raises its own soft
/// limit to the hard limit at start, with the `MaxFDLimit` option that is on
/// by default, so a Kafka broker started the same way does not run out. Linux refuses an
/// infinite soft limit on open files, so an unlimited hard limit is left
/// alone.
#[cfg(unix)]
fn raised_open_file_limit(limit: rustix::process::Rlimit) -> Option<rustix::process::Rlimit> {
    match (limit.current, limit.maximum) {
        (Some(current), Some(maximum)) if current < maximum => Some(rustix::process::Rlimit {
            current: Some(maximum),
            maximum: Some(maximum),
        }),
        _ => None,
    }
}

/// Raises this process's soft limit on open files to its hard limit, as
/// [`raised_open_file_limit`] decides, and logs the change or its failure.
#[cfg(unix)]
fn raise_open_file_limit() {
    use rustix::process::{Resource, getrlimit, setrlimit};
    let limit = getrlimit(Resource::Nofile);
    let Some(raised) = raised_open_file_limit(limit) else {
        return;
    };
    match setrlimit(Resource::Nofile, raised) {
        Ok(()) => tracing::info!(
            from = limit.current,
            to = raised.current,
            "raised the open-file limit to the hard limit"
        ),
        Err(error) => tracing::warn!(
            %error,
            limit = limit.current,
            "could not raise the open-file limit to the hard limit"
        ),
    }
}

/// The `BrokerConfig` that the command line and the config file describe,
/// and the drain timeout of a controlled shutdown.
///
/// The file applies over the flags, and the runtime flags and their
/// environment variables apply over the file. `args` must already have adopted
/// the file's `broker_id`, as [`Args::load_config_file`] does: the raft node
/// id and the seeded self-voter come from it.
fn broker_config(
    args: &mut Args,
    file_config: Option<FileConfig>,
    ClientMetricsOtlp {
        endpoint: client_metrics_otlp_endpoint,
        protocol: client_metrics_otlp_protocol,
    }: ClientMetricsOtlp,
) -> Result<AssembledConfig, Box<dyn std::error::Error>> {
    let file_shutdown_timeout = file_config
        .as_ref()
        .and_then(|file| file.runtime.as_ref())
        .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
    let advertised = args
        .advertised_listener
        .take()
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr = args.resolved_controller_listen_addr();
    let node_id = args.node_id()?;
    let metrics_listen_addr = parse_optional_listen_addr(&args.metrics_listen_addr)?;
    let roles = if args.process_roles.is_empty() {
        None
    } else {
        Some(parse_roles_arg(&args.process_roles)?)
    };
    let mut config = args.base_broker_config(
        advertised,
        controller_addr,
        node_id,
        metrics_listen_addr,
        client_metrics_otlp_endpoint,
        client_metrics_otlp_protocol,
    );
    if let Some(roles) = roles {
        config.roles = roles;
    }
    if let Some(fc) = file_config {
        fc.apply_before_runtime_overlay(&mut config)?;
    }
    seed_standalone_voter(&mut config)?;
    let controlled_shutdown_drain_timeout =
        args.apply_runtime_to(&mut config, file_shutdown_timeout)?;
    Ok(AssembledConfig {
        config,
        controlled_shutdown_drain_timeout,
    })
}

/// Makes a controller with no voter set and no bootstrap servers the single
/// voter of its own quorum, the standalone node a development run starts.
///
/// A node that names bootstrap servers runs a KIP-853 dynamic quorum: its
/// voters come from the `VotersRecord` in the log, so it seeds nothing. A node
/// without the controller role cannot be a voter, and with neither setting it
/// has no controller to reach, which Kafka refuses at startup with the same
/// message.
fn seed_standalone_voter(config: &mut BrokerConfig) -> Result<(), String> {
    if !config.controller_quorum_voters.is_empty() || !config.bootstrap_servers.is_empty() {
        return Ok(());
    }
    if !config.is_controller() {
        return Err(
            "If using process.roles, either controller.quorum.bootstrap.servers must \
                    contain the set of bootstrap controllers or controller.quorum.voters must \
                    contain a parseable set of controllers."
                .to_owned(),
        );
    }
    config.controller_quorum_voters =
        vec![(config.node_id, config.controller_listen_addr.to_string())];
    Ok(())
}

// binary entrypoint: linear startup wiring
async fn run(
    mut args: Args,
    file_config: Option<FileConfig>,
    client_metrics_otlp: ClientMetricsOtlp,
    telemetry: &TelemetryGuard,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    raise_open_file_limit();
    let health_listen_addr = parse_optional_listen_addr(&args.health_listen_addr)?;
    let AssembledConfig {
        mut config,
        controlled_shutdown_drain_timeout,
    } = broker_config(&mut args, file_config, client_metrics_otlp)?;
    // The handle behind the `BROKER_LOGGER` config resource. It drives the
    // stdout layer that the telemetry setup installed, so `kafka-configs
    // --entity-type broker-loggers --alter` retargets the filter of the
    // running process.
    config.log_levels = telemetry.log_levels();
    // Everything below reads the metadata log directory, Kafka's
    // `metadata.log.dir`: the first data directory unless `metadata_log_dir`
    // names another. Resolve it after the file and the flags are applied, so
    // a TOML override picks up its on-disk state rather than the CLI-default
    // empty path. This is the difference between a fresh-pod Bootstrap and a
    // rolled-pod Rejoin against an existing PVC.
    let metadata_log_dir = config.metadata_dir().to_path_buf();
    // Kafka's `KafkaRaftServer.initializeLogDirs`: every directory's
    // `meta.properties` has to belong to this cluster and to this node, and
    // the metadata log directory has to be formatted. KIP-853: the metadata
    // log directory's id is this replica's stable voter identity.
    let identity = krabka_broker::bootstrap::initialize_log_dirs(
        &metadata_log_dir,
        &config.all_log_dirs(),
        config.node_id,
        config.cluster_id,
    )?;
    config.bootstrap_mode = detect_bootstrap_mode(&metadata_log_dir);
    config.cluster_id = Some(identity.cluster_id);
    config.directory_id = identity.directory_id;
    tracing::info!(
        bootstrap_mode = ?config.bootstrap_mode,
        directory_id = %config.directory_id,
        log_dir = %config.log_dir.display(),
        metadata_log_dir = %metadata_log_dir.display(),
        "selected bootstrap mode"
    );

    // Serve the probes before the broker starts, not after. Log-dir recovery
    // and metadata catch-up both run inside `Broker::start`, and those are
    // exactly the windows in which the orchestrator has to be able to ask
    // whether this pod is alive (yes) and ready (not yet). The state goes into
    // the broker by the same handle, so each startup phase marks its own
    // condition on the state these routes read.
    let health = krabka_broker::HealthState::new(
        args.readiness_max_metadata_lag
            .unwrap_or(krabka_broker::config::DEFAULT_READINESS_MAX_METADATA_LAG),
    );
    let health_shutdown = tokio_util::sync::CancellationToken::new();
    if let Some(addr) = health_listen_addr {
        krabka_broker::health::serve(addr, health.clone(), health_shutdown.child_token()).await?;
    }

    let health_for_shutdown = health.clone();
    let controller_role = config.is_controller();
    let handle = Broker::start_with_health(config, health).await?;
    log_started(&handle, controller_role);

    serve(
        handle,
        &health_for_shutdown,
        &health_shutdown,
        controlled_shutdown_drain_timeout.to_std(),
        wait_for_termination_signal(),
    )
    .await
    .map_err(Into::into)
}

/// Logs `krabka-broker started`, the line that says the start of this node is
/// complete. Kafka's `KafkaRaftServer.startup` logs `Kafka Server started` for
/// every `process.roles`, and the ducktape adapter waits for this line as
/// Kafka's `KafkaService` waits for that one.
///
/// The line names the listeners that the node opened: `listen_addr` for the
/// data-plane listener of a node with the broker role, and
/// `controller_listen_addr` for the controller listener of a node with the
/// controller role. A controller-only node opens no data-plane listener, so its
/// line names the controller listener only.
fn log_started(handle: &BrokerHandle, controller_role: bool) {
    tracing::info!(
        listen_addr = handle.data_plane_addr().map(tracing::field::display),
        controller_listen_addr =
            controller_role.then(|| tracing::field::display(handle.controller_addr())),
        "krabka-broker started"
    );
}

/// Runs the started broker until a termination signal or a self-shutdown, stops
/// it, and returns how the process should end: `Ok` for a stop that leadership
/// drained through, and the `FatalFault` of a controller that stopped itself.
async fn serve(
    handle: BrokerHandle,
    health: &krabka_broker::HealthState,
    health_shutdown: &tokio_util::sync::CancellationToken,
    drain_timeout: std::time::Duration,
    termination_signal: impl Future<Output = &'static str>,
) -> Result<(), BrokerError> {
    let mut shutdown_rx = handle.should_shutdown_rx();
    tokio::select! {
        signal = termination_signal => {
            tracing::info!(signal, "shutdown signal received");
        }
        () = async {
            // Wait until the self-shutdown flag flips true.
            loop {
                // Check first in case the flag was already set before we subscribed.
                if *shutdown_rx.borrow_and_update() { break; }
                if shutdown_rx.changed().await.is_err() { break; }
            }
        } => {
            // Two things latch the flag: a fatal fault, which is a fault of
            // the controller this node hosts or the failure of its metadata
            // log directory (KIP-858), and every log dir going offline
            // (KIP-112).
            if let Some(fault) = handle.fatal_fault() {
                tracing::error!(%fault, "self-shutdown triggered by a fatal fault; stopping broker");
            } else {
                tracing::error!("self-shutdown triggered (all log dirs offline); stopping broker");
            }
        }
    }
    // Flip /readyz to 503 so load balancers pull the broker out of rotation
    // before the leadership hand-off starts.
    health.mark_shutting_down();

    let outcome = stop_broker(handle, drain_timeout).await;
    // The probes outlive the broker's own drain deliberately: the kubelet is
    // still polling while `controlled_shutdown` hands leadership over, and a
    // refused connection there is indistinguishable from a crash.
    health_shutdown.cancel();
    tracing::info!("krabka-broker stopped");
    outcome
}

/// Stops the broker and reports how the process should end.
///
/// A termination signal, or every log dir going offline (KIP-112), takes
/// Kafka's controlled shutdown (KIP-500): ask the controller to move
/// leadership of every partition this broker leads onto its other in-sync
/// replicas BEFORE we stop. This is the difference between a near-seamless
/// failover and stranding producers on a dead leader until their request
/// timeout -- `kubectl delete pod` sends SIGTERM, and without this hand-off the
/// partition has no leader until the controller fences us (~tens of seconds).
/// Bounded well under the pod's terminationGracePeriod (30s); on timeout
/// `controlled_shutdown` falls back to a hard stop internally.
///
/// A fatal fault of the controller takes none of that. Kafka handles it with
/// `ProcessTerminatingFaultHandler`, which halts the process with status 1: no
/// leadership hand-off, which a dead controller could not serve anyway, and no
/// orderly stop, so no clean-shutdown proof for the next start
/// (`BrokerHandle::shutdown` writes none after a fault). The stop that runs
/// here only closes what it can within `drain_timeout`, so an async task that
/// hangs cannot hold up the return, and the `FatalFault` it returns is what
/// [`broker_main`] halts the process on.
pub(crate) async fn stop_broker(
    handle: BrokerHandle,
    drain_timeout: std::time::Duration,
) -> Result<(), BrokerError> {
    if let Some(fault) = handle.fatal_fault() {
        if tokio::time::timeout(drain_timeout, handle.shutdown())
            .await
            .is_err()
        {
            tracing::error!(
                ?drain_timeout,
                "the broker did not stop in time after a fatal fault; exiting anyway"
            );
        }
        return Err(BrokerError::FatalFault(fault));
    }
    // The controller can fault while the leadership drain runs, after the check
    // above, and `controlled_shutdown` consumes the handle. Keep a watch on the
    // fault so the process still halts on it, as Kafka's handler would have.
    let fault_watch = handle.fatal_fault_watch();
    match handle.controlled_shutdown(drain_timeout).await {
        Ok(()) => tracing::info!("controlled shutdown complete (leadership drained)"),
        Err(e) => tracing::warn!(error = %e, "controlled shutdown incomplete; hard-stopped"),
    }
    fault_after_shutdown(&fault_watch)
}

/// The `FatalFault` a controller published while the broker was stopping, or
/// `Ok` when it published none.
fn fault_after_shutdown(
    fault_watch: &tokio::sync::watch::Receiver<Option<String>>,
) -> Result<(), BrokerError> {
    match fault_watch.borrow().clone() {
        Some(fault) => Err(BrokerError::FatalFault(fault)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::test_support::env_guard, assert2::assert, std::time::Duration,
        tempfile::tempdir,
    };

    // Kafka 4.3 supports `metadata.version` up to 30, and 33 (`4.4-IV2`) is one
    // of trunk's unstable levels.
    const FAULT: &str = "Tried to apply FeatureLevelRecord \
        FeatureLevelRecord(name='metadata.version', featureLevel=33), \
        but this controller only supports versions 7-30";

    const CLEAN_SHUTDOWN_PROOF: &str = ".kafka_cleanshutdown";

    async fn start_broker(log_dir: &std::path::Path) -> BrokerHandle {
        Broker::start(BrokerConfig::for_tests(log_dir.to_path_buf()))
            .await
            .expect("broker start")
    }

    // The default config supports no unstable feature level, so committing one
    // makes the controller replay a level outside its range.
    async fn make_the_controller_fault(handle: &BrokerHandle) {
        handle
            .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: "metadata.version".into(),
                    level: 33,
                },
            ))
            .await
            .expect("the unsupported level commits before the controller stops");
        let mut should_shutdown = handle.should_shutdown_rx();
        tokio::time::timeout(
            Duration::from_secs(30),
            should_shutdown.wait_for(|down| *down),
        )
        .await
        .expect("the fault did not latch the self-shutdown flag within 30s")
        .expect("the self-shutdown flag closed");
    }

    /// What the process would run after a start: the probe state and its
    /// shutdown token, as `run` builds them.
    fn probes() -> Probes {
        Probes {
            health: krabka_broker::HealthState::new(1_000),
            shutdown: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// The probe state and its shutdown token.
    struct Probes {
        health: krabka_broker::HealthState,
        shutdown: tokio_util::sync::CancellationToken,
    }

    // The `PENDING_CONTROLLED_SHUTDOWN` ordinal of `HealthState::broker_state`,
    // which `/readyz` reports 503 for.
    const PENDING_CONTROLLED_SHUTDOWN: i64 = 6;

    // The post-start path of `krabka-broker`: a controller fault after the
    // broker has come up ends the process with an error that carries Kafka's
    // message, and the stop it runs leaves no clean-shutdown proof, as a
    // halted Kafka process leaves none. The zero-length bound is a stop that
    // does not finish in time, and the error still comes back. The signal never
    // arrives, so it is the fault that ends the wait.
    #[tokio::test]
    async fn a_fault_after_start_ends_in_a_fatal_fault_error_without_a_clean_shutdown_proof() {
        for (name, bound) in [
            ("time to stop", Duration::from_secs(30)),
            ("no time to stop", Duration::ZERO),
        ] {
            let dir = tempdir().unwrap();
            let handle = start_broker(dir.path()).await;
            make_the_controller_fault(&handle).await;
            let Probes {
                health,
                shutdown: health_shutdown,
            } = probes();

            let outcome = serve(
                handle,
                &health,
                &health_shutdown,
                bound,
                std::future::pending(),
            )
            .await;

            assert!(
                matches!(&outcome, Err(BrokerError::FatalFault(fault)) if fault == FAULT),
                "{name}: {outcome:?}"
            );
            assert!(
                !dir.path().join(CLEAN_SHUTDOWN_PROOF).exists(),
                "{name}: a fault-driven stop left a clean-shutdown proof"
            );
            assert!(
                health.broker_state() == PENDING_CONTROLLED_SHUTDOWN,
                "{name}: /readyz was not flipped before the stop"
            );
            assert!(health_shutdown.is_cancelled(), "{name}: probes still up");
        }
    }

    // The control for the test above: a termination signal with no fault ends
    // in success and leaves the proof, so the fault is what the error and the
    // missing proof above come from.
    #[tokio::test]
    async fn a_signal_stops_the_broker_successfully_and_leaves_the_clean_shutdown_proof() {
        let dir = tempdir().unwrap();
        let handle = start_broker(dir.path()).await;
        let Probes {
            health,
            shutdown: health_shutdown,
        } = probes();

        let outcome = serve(
            handle,
            &health,
            &health_shutdown,
            Duration::from_secs(30),
            async { "SIGTERM" },
        )
        .await;

        assert!(outcome.is_ok(), "{outcome:?}");
        assert!(dir.path().join(CLEAN_SHUTDOWN_PROOF).exists());
        assert!(health.broker_state() == PENDING_CONTROLLED_SHUTDOWN);
        assert!(health_shutdown.is_cancelled());
    }

    // `broker_main` halts the process on exactly this failure. The error comes
    // to it boxed, as `run` boxes what `?` and `serve` return.
    #[test]
    fn only_the_fatal_fault_of_the_controller_halts_the_process() {
        struct Row {
            name: &'static str,
            error: Box<dyn std::error::Error>,
            halts: bool,
        }
        let rows: [Row; 4] = [
            Row {
                name: "the controller's fatal fault",
                error: BrokerError::FatalFault(FAULT.to_owned()).into(),
                halts: true,
            },
            Row {
                name: "another failure of the start",
                error: BrokerError::Startup("no leader".to_owned()).into(),
                halts: false,
            },
            Row {
                name: "a broker that is shutting down",
                error: BrokerError::Shutdown.into(),
                halts: false,
            },
            Row {
                name: "a bare message",
                error: "failed to read the config".into(),
                halts: false,
            },
        ];
        for Row { name, error, halts } in rows {
            assert!(is_fatal_fault(error.as_ref()) == halts, "{name}: {error}");
        }
    }
    /// The `BrokerConfig` that startup builds from these flags and a
    /// `--config-file` holding `toml`, by the calls `broker_main` and `run`
    /// make.
    fn configured(flags: &[&str], toml: &str) -> Result<BrokerConfig, String> {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("broker.toml");
        std::fs::write(&path, toml).expect("write broker.toml");
        let config_file = format!("--config-file={}", path.display());
        let mut args = Args::try_parse_from(
            ["krabka-broker", config_file.as_str()]
                .into_iter()
                .chain(flags.iter().copied()),
        )
        .expect("flags parse");
        let file = args.load_config_file()?;
        broker_config(&mut args, file, ClientMetricsOtlp {
            endpoint: None,
            protocol: OtlpProtocol::Grpc,
        })
        .map(|assembled| assembled.config)
        .map_err(|error| error.to_string())
    }

    // A Kafka cluster with a single controller: node 2 is a broker, and its
    // id comes from the file alone. It used to run raft as node 1, the flag's
    // default, and so refused to start as a non-controller in its own quorum.
    #[test]
    fn a_broker_only_node_takes_its_raft_id_from_the_file() {
        let config = configured(
            &[],
            "broker_id = 2\n\
             controller_quorum_voters = [\"1@controller-1:9592\"]\n\
             [process]\n\
             roles = [\"broker\"]\n",
        )
        .expect("the config assembles");

        assert!(config.validate().is_ok());
        assert!(
            (
                config.broker_id,
                config.node_id,
                config.controller_quorum_voters
            ) == (2, krabka_broker::NodeId(2), vec![(
                krabka_broker::NodeId(1),
                "controller-1:9592".to_owned()
            )])
        );
    }

    // With no voter set anywhere, the node is the single voter of its own
    // quorum, under the id the file names.
    #[test]
    fn a_combined_node_without_voters_seeds_itself_under_the_file_id() {
        let config = configured(&[], "broker_id = 42\n").expect("the config assembles");

        assert!(config.validate().is_ok());
        assert!(
            (
                config.broker_id,
                config.node_id,
                config.controller_quorum_voters
            ) == (42, krabka_broker::NodeId(42), vec![(
                krabka_broker::NodeId(42),
                "0.0.0.0:9093".to_owned()
            )])
        );
    }

    /// `--metadata-log-dir` names the metadata log directory over the file's
    /// `metadata_log_dir`, and without either the metadata log is in the first
    /// log directory, as Kafka's `metadata.log.dir` defaults to the first entry
    /// of `log.dirs`. A separate directory is not a data directory.
    #[test]
    fn the_metadata_log_directory_comes_from_the_flag_then_the_file_then_log_dir() {
        struct Case {
            what: &'static str,
            flags: &'static [&'static str],
            toml: &'static str,
            /// The metadata directory the config resolves.
            metadata: &'static str,
            /// The data directories the config resolves.
            data: &'static [&'static str],
        }
        let cases: [Case; 4] = [
            Case {
                what: "neither",
                flags: &[],
                toml: "log_dir = \"/data\"\n",
                metadata: "/data",
                data: &["/data"],
            },
            Case {
                what: "the file",
                flags: &[],
                toml: "log_dir = \"/data\"\nmetadata_log_dir = \"/meta\"\n",
                metadata: "/meta",
                data: &["/data"],
            },
            Case {
                what: "the flag over the file",
                flags: &["--metadata-log-dir=/flag"],
                toml: "log_dir = \"/data\"\nmetadata_log_dir = \"/meta\"\n",
                metadata: "/flag",
                data: &["/data"],
            },
            Case {
                what: "one of the data directories",
                flags: &["--metadata-log-dir=/more"],
                toml: "log_dir = \"/data\"\nextra_log_dirs = [\"/more\"]\n",
                metadata: "/more",
                data: &["/data", "/more"],
            },
        ];
        for Case {
            what,
            flags,
            toml,
            metadata,
            data,
        } in cases
        {
            let config = configured(flags, toml).unwrap_or_else(|error| panic!("{what}: {error}"));
            assert!(
                (config.metadata_dir().to_path_buf(), config.all_log_dirs())
                    == (
                        std::path::PathBuf::from(metadata),
                        data.iter()
                            .map(std::path::PathBuf::from)
                            .collect::<Vec<_>>()
                    ),
                "{what}"
            );
        }
    }

    #[test]
    fn the_flag_wins_over_the_file_and_the_file_over_the_default() {
        struct Case {
            name: &'static str,
            flags: &'static [&'static str],
            toml: &'static str,
            id: i32,
        }
        let cases: [Case; 4] = [
            Case {
                name: "the file alone",
                flags: &[],
                toml: "broker_id = 2\n",
                id: 2,
            },
            Case {
                name: "the flag over the file",
                flags: &["--broker-id=7"],
                toml: "broker_id = 2\n",
                id: 7,
            },
            Case {
                name: "the flag alone",
                flags: &["--broker-id=7"],
                toml: "",
                id: 7,
            },
            Case {
                name: "neither",
                flags: &[],
                toml: "",
                id: krabka_broker::config::DEFAULT_BROKER_ID,
            },
        ];
        for Case {
            name,
            flags,
            toml,
            id,
        } in cases
        {
            let config = configured(flags, toml).unwrap_or_else(|error| panic!("{name}: {error}"));
            let node_id = krabka_broker::NodeId(u64::try_from(id).expect("a positive id"));
            assert!(
                (config.broker_id, config.node_id) == (id, node_id),
                "{name}"
            );
        }
    }

    #[test]
    fn only_a_controller_without_voters_or_bootstrap_servers_seeds_itself() {
        /// A voter of the static quorum.
        #[derive(Debug, PartialEq, Eq)]
        struct Voter {
            node_id: krabka_broker::NodeId,
            endpoint: String,
        }
        type Outcome = Result<Vec<Voter>, String>;
        struct Case {
            name: &'static str,
            toml: &'static str,
            want: Outcome,
        }
        let voter = |id, endpoint: &str| Voter {
            node_id: krabka_broker::NodeId(id),
            endpoint: endpoint.to_owned(),
        };
        let refused = "If using process.roles, either controller.quorum.bootstrap.servers must \
                       contain the set of bootstrap controllers or controller.quorum.voters must \
                       contain a parseable set of controllers.";
        let cases: [Case; 5] = [
            Case {
                name: "a combined node alone",
                toml: "broker_id = 4\n",
                want: Ok(vec![voter(4, "0.0.0.0:9093")]),
            },
            Case {
                name: "a dynamic-quorum controller",
                toml: "broker_id = 3001\nbootstrap_servers = [\"ducker02:9592\"]\n\
                 [process]\nroles = [\"controller\"]\n",
                want: Ok(vec![]),
            },
            Case {
                name: "a dynamic-quorum broker",
                toml: "broker_id = 1\nbootstrap_servers = [\"ducker02:9592\"]\n\
                 [process]\nroles = [\"broker\"]\n",
                want: Ok(vec![]),
            },
            Case {
                name: "a static-quorum broker",
                toml: "broker_id = 1\ncontroller_quorum_voters = [\"3001@ducker02:9592\"]\n\
                 [process]\nroles = [\"broker\"]\n",
                want: Ok(vec![voter(3001, "ducker02:9592")]),
            },
            Case {
                name: "a broker with no controller to reach",
                toml: "broker_id = 1\n[process]\nroles = [\"broker\"]\n",
                want: Err(refused.to_owned()),
            },
        ];
        for Case { name, toml, want } in cases {
            let voters = configured(&[], toml).map(|config| {
                config
                    .controller_quorum_voters
                    .into_iter()
                    .map(|(node_id, endpoint)| Voter { node_id, endpoint })
                    .collect()
            });
            assert!(voters == want, "{name}");
        }
    }

    #[test]
    fn a_negative_file_id_is_refused() {
        let error = configured(&[], "broker_id = -3\n").expect_err("a negative id is refused");
        assert!(error == "broker_id must be non-negative, got -3");
    }

    #[cfg(unix)]
    #[test]
    fn the_open_file_limit_rises_to_a_finite_hard_limit() {
        use rustix::process::Rlimit;
        let limit = |current, maximum| Rlimit { current, maximum };
        let rows = [
            (
                "a login shell's limits",
                limit(Some(1024), Some(524_288)),
                Some(limit(Some(524_288), Some(524_288))),
            ),
            (
                "already at the hard limit",
                limit(Some(524_288), Some(524_288)),
                None,
            ),
            ("an unlimited hard limit", limit(Some(1024), None), None),
            ("no limit at all", limit(None, None), None),
        ];
        for (name, current, raised) in rows {
            assert!(raised_open_file_limit(current) == raised, "{name}");
        }
    }

    // The controller can fault while `controlled_shutdown` drains leadership,
    // after `stop_broker` last looked, and the process must still halt on it.
    #[test]
    fn a_fault_published_while_the_broker_stopped_ends_in_a_fatal_fault_error() {
        let (fault_tx, fault_rx) = tokio::sync::watch::channel(None);
        assert!(fault_after_shutdown(&fault_rx).is_ok());

        fault_tx.send_replace(Some(FAULT.to_owned()));
        assert!(matches!(
            fault_after_shutdown(&fault_rx),
            Err(BrokerError::FatalFault(ref fault)) if fault == FAULT
        ));
    }
}
