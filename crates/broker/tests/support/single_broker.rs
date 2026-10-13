//! One-broker, one-client boots for the simpler integration tests.
//!
//! Every helper here starts a broker and returns a client already connected to
//! it. They differ only in what the broker is configured with: a caller-owned
//! directory that survives a restart, an audit signing key, a deny-all
//! authorizer, or nothing beyond the `for_tests` defaults.

use {
    crate::support::client::{BrokerWithClient, connect_owned, start_broker_client},
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_client_core::Client,
    tempfile::TempDir,
};

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess { start_configured(|_| {}).await }

/// A bare broker with its directory bound before its handle at the call site.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub async fn standalone_broker() -> (TempDir, BrokerHandle) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    (dir, broker)
}

/// [`start`] with `legacy_request_versions_enable` set, for the tests that
/// drive the pre-4.0 `Fetch`, `ListOffsets` and `Produce` versions Kafka 4.x
/// refuses and krabka serves only on request.
pub async fn start_legacy() -> InProcess {
    start_configured(|config| {
        config.features.legacy_request_versions =
            krabka_broker::api_catalog::LegacyRequestVersions::Enabled;
    })
    .await
}

/// [`start`] with `configure` applied to the `for_tests` config first.
pub async fn start_configured(configure: impl FnOnce(&mut BrokerConfig)) -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = heartbeat_config(tempdir.path());
    configure(&mut config);
    // Boxed: the boot future holds the whole `BrokerConfig`, and every test
    // that awaits one of these helpers would otherwise carry it inline,
    // past `clippy::large_futures`.
    let BrokerWithClient { broker, client } =
        Box::pin(boot_with_client(config, "krabka-broker-test")).await;
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

async fn boot_with_client(config: BrokerConfig, client_id: &str) -> BrokerWithClient {
    // Keep startup off the fixture's stack as LogConfig grows.
    let broker = Box::pin(Broker::start(config)).await.expect("broker start");
    broker.wait_until_broker_alive(1).await;
    let client = connect_owned(broker.listen_addr().to_string(), client_id, "client build").await;
    BrokerWithClient { broker, client }
}

/// Start a broker rooted at `dir` (caller owns the directory).
///
/// Restart tests use this helper. Pass the same path across two boots to
/// verify that the broker recovers persistent state (audit chain, spool)
/// correctly. The helper detects an existing raft log and then uses `Rejoin`.
pub async fn start_with_dir(dir: &std::path::Path) -> (BrokerHandle, krabka_client_core::Client) {
    let mut config = heartbeat_config(dir);
    // Mirror the production heuristic from `detect_bootstrap_mode` in
    // broker.rs: key Rejoin on `metadata_log_nonempty` (committed
    // quorum-state), NOT bare directory presence.  The segment dir is created
    // before the first raft commit, so dir-existence would re-bootstrap a node
    // killed mid-election instead of letting it rejoin correctly.
    let metadata_dir = krabka_raft::metadata_partition_dir(dir);
    if krabka_raft::metadata_log_nonempty(&metadata_dir) {
        config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    }
    let BrokerWithClient { broker, client } = boot_with_client(config, "krabka-broker-test").await;
    (broker, client)
}

/// Start a broker configured with an audit signing key and a given checkpoint cadence.
///
/// Uses `every_secs = 3600` so only the count-based trigger fires in tests.
pub fn start_with_audit_key(
    key_path: &std::path::Path,
    key_id: &str,
    every_n: u64,
) -> impl std::future::Future<Output = InProcess> {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = heartbeat_config(tempdir.path());
    config.audit_signing_key_path = Some(key_path.to_path_buf());
    config.audit_signing_key_id = Some(key_id.to_string());
    config.audit_checkpoint_every_n = every_n;
    config.audit_checkpoint_every = krabka_units::hours(1); // only count trigger fires
    Box::pin(async move {
        let BrokerWithClient { broker, client } =
            boot_with_client(config, "krabka-broker-test-audit-key").await;
        InProcess {
            broker,
            client,
            _tempdir: tempdir,
        }
    })
}

/// Start a broker whose authorizer is `SimpleAclAuthorizer` with no ACLs and no
/// super-users (deny-all for the anonymous test client). The `for_tests`
/// defaults enable audit. The broker denies the anonymous client every admin
/// operation, which produces `AuthorizationDenied` audit events.
pub async fn start_with_deny_all_authz() -> InProcess {
    use {krabka_broker::authorizer::SimpleAclAuthorizer, std::collections::HashSet};

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = heartbeat_config(tempdir.path());
    // Replace the default AllowAllAuthorizer with a deny-all SimpleAclAuthorizer
    // (empty ACL store, no super-users). The anonymous test client connects
    // with no credentials so it has no super-user bypass — every operation is
    // denied and the auditing decorator emits AuthorizationDenied events.
    config.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
    // The broker's own heartbeat is denied too, so it stays fenced: it never
    // becomes alive in the liveness registry, and nothing here needs it to.
    let BrokerWithClient { broker, client } = start_broker_client()
        .config(config)
        .client_id("krabka-broker-test-deny")
        .call()
        .await;
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

krabka_macros::bound_start_fixture!(config, bound_config, ::krabka_broker);
krabka_macros::bound_start_fixture!(start, start_bound, ::krabka_broker, expect, bound_config);

/// Hold both listeners through startup and advertise their actual addresses.
pub async fn start_with_bound_listeners(
    customize: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, TempDir) {
    let StartedBoundBroker {
        broker,
        controller_addr: _,
        dir,
    } = start_bound(customize).await;
    (broker, dir)
}

/// A bare `for_tests` broker, with no readiness wait or client-side setup.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let (dir, broker) = standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// A classic-group fixture with the unmodified `for_tests` configuration.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub async fn start_group_coordinator() -> (BrokerHandle, String, TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let started = Box::pin(
        start_group_coordinator_with_config()
            .config(config)
            .directory(tempdir)
            .call(),
    )
    .await;
    (started.broker, started.bootstrap, started.directory)
}

pub struct GroupCoordinatorBoot {
    pub broker: BrokerHandle,
    pub bootstrap: String,
    pub directory: TempDir,
}

/// Start a configured broker and wait until its group coordinator is ready.
#[bon::builder]
pub async fn start_group_coordinator_with_config(
    config: BrokerConfig,
    directory: TempDir,
) -> GroupCoordinatorBoot {
    let broker = Broker::start(config).await.expect("broker must start");
    broker.wait_until_group_coordinator_ready().await;
    let bootstrap = broker.listen_addr().to_string();
    GroupCoordinatorBoot {
        broker,
        bootstrap,
        directory,
    }
}

/// Keep the standard client fixture and wait for its group coordinator after startup.
pub async fn start_ready_group() -> InProcess {
    let p = start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

/// The single-client fixtures' shared heartbeat timeout, before caller overrides.
fn heartbeat_config(dir: &std::path::Path) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.heartbeat_timeout = krabka_units::secs(30);
    config
}
