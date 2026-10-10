//! The two brokers this suite reads an `ApiVersions` table from, and the one
//! JVM tool it reads them with.
//!
//! krabka runs in this process and is addressed from a throwaway tool container
//! over the bridge, the way the rest of the JVM harness does it. The Kafka
//! oracle is the same image running as a broker, and the tool runs inside that
//! container against its own loopback listener -- so the oracle needs no
//! published port and cannot collide with a concurrent suite.
//!
//! The oracle keeps a stock configuration, and the tool reads its PLAINTEXT
//! *broker* listener. Both bound what the Kafka side of the join can hold:
//! Kafka scopes an `ApiVersions` response to the listener the request arrived
//! on (`ApiVersionsResponse.filterApis` takes a `ListenerType`), so its
//! controller-plane APIs answer elsewhere and never reach this table, and it
//! withholds `GetTelemetrySubscriptions` and `PushTelemetry` unless a
//! client-telemetry exporter is configured, which a stock broker has none of.
//!
//! krabka scopes its own table the same way. `start_krabka` binds one
//! `PLAINTEXT` listener with no client-metrics receiver, so it answers with
//! `api_catalog::ListenerKind::Client` and
//! `api_catalog::ClientMetricsReceiver::Absent`, and the keys Kafka withholds
//! here are the keys krabka withholds here. The expectation therefore records
//! no `krabka_only` row: the join is `same` wherever both sides agree and
//! `range_differs` for the four ranges krabka means to widen.

use std::{
    process::{Command, Stdio},
    time::Duration,
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_security::ListenerProtocol;

use crate::{
    probe::{retry_until, wait_bounded},
    support::{JvmListeners, unique_container_name},
};

/// The Kafka release this suite compares krabka against.
///
/// 4.3.1 is the newest image //bazel/images pins, and the newest release is the
/// right oracle for a table that is meant to be current: an older one would
/// report an absent key for every API Kafka has added since.
pub(crate) const ORACLE_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The tool, at its path inside the `apache/kafka` image.
const TOOL: &str = "/opt/kafka/bin/kafka-broker-api-versions.sh";

/// Wall-clock budget the oracle gets to answer before the suite gives up.
const ORACLE_BOOT_BUDGET: Duration = Duration::from_mins(1);

/// The pause between a readiness probe that failed and the next one.
const ORACLE_BOOT_GAP: Duration = Duration::from_secs(1);

/// Boot an in-process krabka broker on `listeners`, advertised under the name
/// the tool containers resolve through `--add-host`.
///
/// The broker gets a separated inter-broker listener, because the oracle it is
/// compared against has one: the `apache/kafka` image ships `PLAINTEXT` for
/// clients and `BROKER` for inter-broker traffic, and it is the client
/// listener the tool dials on both sides. krabka widens the table on the
/// listener `inter.broker.listener.name` names -- peers negotiate
/// `AlterPartition` and the rest of
/// [`INTER_BROKER_ONLY_APIS`][krabka_broker::api_catalog::INTER_BROKER_ONLY_APIS]
/// against what that endpoint advertises -- so comparing a *client* listener
/// against Kafka's is comparing like with like. Point the tool at a
/// single-listener krabka instead and it would read back the eleven extra keys
/// this suite exists to have removed, because there the one listener carries
/// both kinds of traffic.
pub(crate) async fn start_krabka(listeners: &JvmListeners) -> (BrokerHandle, tempfile::TempDir) {
    crate::support::init_tracing();
    let dir = tempfile::tempdir().expect("tempdir");
    let controller_addr: std::net::SocketAddr =
        listeners.controller.parse().expect("allocated addr");
    let client_addr: std::net::SocketAddr = listeners.listen.parse().expect("allocated addr");
    let inter_broker_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", crate::support::free_port())
            .parse()
            .expect("allocated addr");
    let config = BrokerConfig {
        listeners: vec![
            ListenerSpec {
                name: "PLAINTEXT".to_string(),
                bind_addr: client_addr,
                advertised: listeners.advertised.clone(),
                protocol: ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            },
            ListenerSpec {
                name: "BROKER".to_string(),
                bind_addr: inter_broker_addr,
                advertised: inter_broker_addr.to_string(),
                protocol: ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: krabka_broker::SslPrincipalMapper::default(),
            },
        ],
        inter_broker_listener_name: "BROKER".to_string(),
        ..crate::support::jvm_broker_config(
            dir.path().to_path_buf(),
            crate::support::JvmBrokerSetup {
                listen: client_addr,
                controller: controller_addr,
                advertised: listeners.advertised.clone(),
                voters: crate::support::controller_voters(&[(1, controller_addr)]),
                ..Default::default()
            },
        )
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={} advertised={} inter_broker={}",
        listeners.listen, listeners.advertised, inter_broker_addr
    );
    (handle, dir)
}

/// Run the tool from a throwaway container against the krabka broker that
/// `listeners` advertises, and return its stdout.
pub(crate) fn krabka_api_versions(listeners: &JvmListeners) -> String {
    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: ORACLE_IMAGE,
        args: &[TOOL, "--bootstrap-server", &listeners.advertised],
        ..Default::default()
    })
    .output()
    .expect("spawn docker run kafka-broker-api-versions");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "kafka-broker-api-versions against krabka failed: status={} stdout={stdout} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    stdout
}

/// A running single-node Kafka oracle. Dropping it removes the container.
pub(crate) struct OracleBroker {
    name: String,
}

impl OracleBroker {
    /// Boot [`ORACLE_IMAGE`] as a single-node `KRaft` broker and wait until its
    /// own tool can read its `ApiVersions` table.
    ///
    /// # Panics
    ///
    /// Panics when the container will not start, or when the broker does not
    /// answer within [`ORACLE_BOOT_BUDGET`].
    pub(crate) fn start() -> Self {
        let name = unique_container_name("krabka-apiversions-oracle");
        let out = Command::new("docker")
            .args(["run", "-d", "--name", &name])
            .args(crate::support::kafka_single_node_env_args())
            .args(["-e", "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk", ORACLE_IMAGE])
            .output()
            .expect("spawn docker run kafka oracle");
        assert!(
            out.status.success(),
            "docker run {ORACLE_IMAGE} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        let oracle = Self { name };
        oracle.wait_ready();
        oracle
    }

    /// Read the oracle's own `ApiVersions` table, from inside its container.
    pub(crate) fn api_versions(&self) -> String {
        let out = self.exec_tool();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "kafka-broker-api-versions against {ORACLE_IMAGE} failed: status={} stdout={stdout} stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
        stdout
    }

    /// The tool, aimed at the oracle's own loopback listener from inside its
    /// container.
    fn tool_command(&self) -> Command {
        let mut command = Command::new("docker");
        command.args([
            "exec",
            &self.name,
            TOOL,
            "--bootstrap-server",
            "localhost:9092",
        ]);
        command
    }

    fn exec_tool(&self) -> std::process::Output {
        self.tool_command()
            .output()
            .expect("spawn docker exec kafka-broker-api-versions")
    }

    /// Run the tool once against the oracle, discarding its output and giving
    /// it at most `budget` to answer.
    ///
    /// A broker that is still booting refuses the connection and the tool exits
    /// quickly, but one that accepts and then never answers holds the tool for
    /// the client's own API timeout -- a minute -- so the probe bounds the
    /// child rather than trusting it to return. Killing `docker exec` leaves
    /// the process it started inside the container, which goes away with the
    /// container itself in [`Drop`].
    fn probe_ready(&self, budget: Duration) -> bool {
        let mut child = self
            .tool_command()
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn docker exec kafka-broker-api-versions");
        wait_bounded(&mut child, budget).is_some_and(|status| status.success())
    }

    fn wait_ready(&self) {
        match retry_until(ORACLE_BOOT_BUDGET, ORACLE_BOOT_GAP, |remaining| {
            self.probe_ready(remaining)
        }) {
            Ok(attempts) => {
                eprintln!("KRABKA[test] oracle {ORACLE_IMAGE} ready after {attempts} attempts");
            }
            Err(attempts) => panic!(
                "{ORACLE_IMAGE} did not answer within {ORACLE_BOOT_BUDGET:?} ({attempts} attempts); container logs:\n{}",
                self.logs()
            ),
        }
    }

    fn logs(&self) -> String {
        crate::support::docker_logs(&self.name)
    }
}

impl Drop for OracleBroker {
    fn drop(&mut self) {
        crate::support::remove_container(&self.name);
    }
}
