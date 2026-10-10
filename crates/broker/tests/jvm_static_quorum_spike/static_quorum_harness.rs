//! Setup shared by the two static mixed-quorum spikes: the JVM controller image
//! name, the cluster-id encoding both implementations must agree on, the
//! `BrokerConfig` builder for one Krabka controller voter, and the format
//! that pins the voters to the JVM voter's release.
//!
//! Both spikes boot the same topology and differ only in what they do to it
//! afterwards, so the topology lives here and each spike file holds one
//! scenario.

use std::{net::SocketAddr, process::Command};

use base64::Engine as _;
use krabka_broker::BrokerConfig;
use uuid::Uuid;

pub(crate) const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

/// Kafka encodes a 16-byte UUID as URL-safe base64 with no padding. The JVM
/// `--cluster-id` string and Krabka's `uuid::Uuid` must wrap the *same* 16
/// bytes. Otherwise the two sides reject each other on a cluster-id
/// mismatch.
pub(crate) fn kafka_cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

/// Builds a Krabka controller `BrokerConfig` for voter `i` in the shared static
/// 3-voter set, with the shared cluster id. `i` is 0-indexed, and the id is
/// `i+1`.
pub(crate) use crate::support::jvm_static_voter_config as krabka_controller_config;

/// Formats a Krabka voter's log directory at Kafka 4.0's `metadata.version`.
///
/// The JVM voter runs 4.0.0, which supports `metadata.version` only up to
/// `4.0-IV3`. Self-bootstrapped, the Krabka voters would finalize Kafka 4.3's
/// `4.3-IV0`, a level the JVM controller cannot replay, so they format at the
/// release the oldest voter runs, as a Kafka operator mixing in a 4.0 node
/// would. The formatter runs in process because a Bazel test sandbox has no
/// Cargo working tree to spawn it from.
pub(crate) async fn format_at_kafka_4_0(log_dir: &std::path::Path, node: &BrokerConfig) {
    let cluster_id = node.cluster_id.expect("the spikes name their cluster id");
    crate::support::format_jvm_voter(log_dir, &kafka_cluster_id_string(cluster_id), node).await;
}

pub(crate) fn docker_rm(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}

/// Publish the JVM voter, keeping its configuration directory alive for the run.
pub(crate) fn start_jvm_controller(
    name: &str,
    port: u16,
    cluster_id: &str,
    props: &str,
) -> tempfile::TempDir {
    let propdir = tempfile::TempDir::new().unwrap();
    let proppath = propdir.path().join("controller.properties");
    std::fs::write(&proppath, props).unwrap();
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cluster_id} --config /tmp/c.properties --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /tmp/c.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            name,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{port}:{port}"),
            "-v",
            &format!("{}:/tmp/c.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM controller");
    assert2::assert!(status.success(), "docker run failed");
    propdir
}

/// Endpoints for two host voters and one published JVM voter.
pub(crate) struct MixedQuorum {
    pub(crate) ports: [u16; 3],
    clients: Vec<SocketAddr>,
    controllers: Vec<SocketAddr>,
}

impl MixedQuorum {
    pub(crate) async fn start(
        cluster_id: Uuid,
        election_timeout: Option<krabka_units::Time>,
    ) -> (
        [u16; 3],
        [krabka_broker::BrokerHandle; 2],
        [tempfile::TempDir; 2],
    ) {
        let endpoints = Self::allocate().await;
        let (brokers, dirs) = endpoints.start_pair(cluster_id, election_timeout).await;
        (endpoints.ports, brokers, dirs)
    }
    pub(crate) async fn allocate() -> Self {
        let (clients, controllers) = crate::support::bind_and_drop_ports(3).await;
        Self {
            ports: std::array::from_fn(|index| controllers[index].port()),
            clients,
            controllers,
        }
    }

    pub(crate) async fn start_pair(
        &self,
        cluster_id: Uuid,
        election_timeout: Option<krabka_units::Time>,
    ) -> ([krabka_broker::BrokerHandle; 2], [tempfile::TempDir; 2]) {
        let dirs: [tempfile::TempDir; 2] = std::array::from_fn(|_| tempfile::tempdir().unwrap());
        let voters: Vec<_> = self
            .controllers
            .iter()
            .enumerate()
            .map(|(index, address)| (u64::try_from(index + 1).unwrap(), *address))
            .collect();
        let configs: [BrokerConfig; 2] = std::array::from_fn(|index| {
            let bind = SocketAddr::from(([0, 0, 0, 0], self.ports[index]));
            let mut config = krabka_controller_config(
                dirs[index].path(),
                crate::support::JvmStaticVoterSetup {
                    broker: crate::support::JvmBrokerSetup {
                        node: krabka_broker::NodeId(
                            u64::try_from((index) + 1).expect("one-based node id"),
                        ),
                        listen: self.clients[index],
                        advertised: (self.clients[index]).to_string(),
                        controller: bind,
                        voters: crate::support::controller_voters(&voters),
                    },
                    cluster_id,
                },
            );
            if let Some(timeout) = election_timeout {
                config.controller_election_timeout = timeout;
            }
            config
        });
        for (dir, config) in dirs.iter().zip(&configs) {
            format_at_kafka_4_0(dir.path(), config).await;
        }
        let [first, second] =
            configs.map(|config| tokio::spawn(krabka_broker::Broker::start(config)));
        let brokers = [
            first.await.unwrap().expect("krabka voter 1 start"),
            second.await.unwrap().expect("krabka voter 2 start"),
        ];
        (brokers, dirs)
    }
}

/// The common JVM controller voter properties, followed by scenario-specific timeouts.
pub(crate) fn jvm_controller_properties([p1, p2, p3]: [u16; 3], overrides: &str) -> String {
    format!(
        "process.roles=controller\nnode.id=3\ncontroller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\ncontroller.listener.names=CONTROLLER\nlisteners=CONTROLLER://0.0.0.0:{p3}\nlistener.security.protocol.map=CONTROLLER:PLAINTEXT\n{overrides}log.dirs=/tmp/kraft-controller-logs\n"
    )
}
