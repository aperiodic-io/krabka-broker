//! Plaintext single-broker bring-up for the JVM acceptance suites.
//!
//! These helpers start one in-process broker on the allocated client listener,
//! which is what a suite needs when it only drives the JVM tools against a
//! single node.

use std::path::Path;

use krabka_broker::{BootstrapMode, Broker, BrokerConfig};

use super::ports::{broker0_advertised, broker0_listen, controller_addr_0};

pub(crate) type HostBroker = (krabka_broker::BrokerHandle, tempfile::TempDir);

/// Single-node JVM listener configuration on the caller's log directory.
pub(crate) fn host_broker_config(dir: &Path, client_context: &str) -> BrokerConfig {
    let listen = broker0_listen().parse().expect(client_context);
    let controller = controller_addr_0().parse().expect("allocated addr");
    crate::support::jvm_broker_config(
        dir.to_path_buf(),
        crate::support::JvmBrokerSetup {
            listen,
            controller,
            advertised: (broker0_advertised()).to_owned(),
            voters: crate::support::controller_voters(&[(1, controller)]),
            ..Default::default()
        },
    )
}

/// A plain broker and one topic created by the stock JVM admin client.
pub(crate) async fn start_console_broker(topic: &str, partitions: i32) -> HostBroker {
    let fixture = start_host_broker().await;
    super::docker::nc_check_connectivity();
    super::docker::create_console_topic(super::docker::KAFKA_IMAGE, &[], topic, partitions, 1);
    fixture
}

/// The console fixture with legacy protocol versions enabled.
pub(crate) async fn start_legacy_console_broker(topic: &str) -> HostBroker {
    let fixture = start_legacy_host_broker().await;
    super::docker::nc_check_connectivity();
    super::docker::create_console_topic(super::docker::KAFKA_IMAGE, &[], topic, 1, 1);
    fixture
}

/// Spawn the broker on `broker0_listen()`. The advertised listener is
/// an allocated port. Inside the cp-kafka containers, the test
/// adds a hosts entry that points that name at the bridge gateway.
pub(crate) async fn start_host_broker() -> HostBroker {
    start_host_broker_with(|_| {}).await
}

/// [`start_host_broker`] with krabka's legacy request versions enabled
/// (`[runtime] legacy_request_versions_enable`). The pre-4.0 clients of the
/// legacy suite send the Fetch, `ListOffsets` and Produce versions Kafka 4.x
/// refuses, which strict 4.3.1 mode refuses too.
pub(crate) async fn start_legacy_host_broker() -> HostBroker {
    start_host_broker_with(|config| {
        config.features.legacy_request_versions =
            krabka_broker::api_catalog::LegacyRequestVersions::Enabled;
    })
    .await
}

/// [`start_host_broker`], letting the caller adjust the config first.
///
/// A suite that drives one of the coordinators needs its internal topic to be
/// hostable here: the defaults ask for 50 partitions at replication factor 3,
/// which one node cannot satisfy, so the partition a key hashes to may never
/// open.
pub(crate) async fn start_host_broker_with(adjust: impl FnOnce(&mut BrokerConfig)) -> HostBroker {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = start_host_broker_in_with(dir.path(), adjust).await;
    (handle, dir)
}

/// [`start_host_broker`] on a log directory the caller owns.
///
/// A restart case shuts the handle down and calls this again with the same
/// path, which the other two helpers cannot express: they own the temporary
/// directory and hand it back alongside the handle, so the second boot would
/// land on an empty one.
pub(crate) async fn start_host_broker_in(dir: &Path) -> krabka_broker::BrokerHandle {
    start_host_broker_in_with(dir, |_| {}).await
}

/// The one copy of the single-node config every helper above boots.
async fn start_host_broker_in_with(
    dir: &Path,
    adjust: impl FnOnce(&mut BrokerConfig),
) -> krabka_broker::BrokerHandle {
    crate::support::init_jvm_tracing("krabka_broker=debug,info");
    let mut config = BrokerConfig {
        bootstrap_mode: bootstrap_mode(dir),
        ..host_broker_config(dir, "static addr")
    };
    adjust(&mut config);
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    tracing::info!(listen = %broker0_listen(), advertised = %broker0_advertised(), "broker started for jvm acceptance");
    handle
}

/// `Bootstrap` on a fresh directory, `Rejoin` once a committed raft log is
/// there, the same choice `detect_bootstrap_mode` makes in the broker binary.
///
/// A first boot is always the former. The second boot of a restart case is the
/// latter, and asking for `Bootstrap` on top of a non-empty metadata log is
/// rejected at controller start.
fn bootstrap_mode(dir: &Path) -> BootstrapMode {
    if krabka_raft::metadata_log_nonempty(&krabka_raft::metadata_partition_dir(dir)) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

/// Like [`start_host_broker`] but configures a second JBOD data directory
/// (KIP-113). Returns the two host-side log dirs with the handle, so
/// the test can assert which absolute paths `DescribeLogDirs` reports.
pub(crate) async fn start_host_broker_jbod() -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    crate::support::init_jvm_tracing("krabka_broker=debug,info");
    let primary = tempfile::tempdir().expect("tempdir");
    let extra = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig {
        extra_log_dirs: vec![extra.path().to_path_buf()],
        ..host_broker_config(primary.path(), "static addr")
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, primary, extra)
}

/// Create a group by producing and committing one record through the JVM console clients.
pub(crate) async fn start_console_group(topic: &str, group: &str, partitions: i32) -> HostBroker {
    let broker = start_console_broker(topic, partitions).await;
    let _ =
        super::docker::produce_console(super::docker::KAFKA_IMAGE, &[], topic, false, b"alpha\n");
    super::docker::consume_console_group(topic, group, 1, 10_000);
    broker
}
