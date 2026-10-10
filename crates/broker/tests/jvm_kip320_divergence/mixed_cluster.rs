//! The mixed JVM+Krabka cluster the divergence scenarios run on.
//!
//! Three of the four scenarios need the same topology: two Krabka brokers that
//! hold the metadata-quorum majority, plus one `KRaft`-native JVM broker joined
//! over the real wire. This module builds that cluster and owns its lifetime,
//! so each scenario file carries only the divergence it induces.

use std::{
    net::SocketAddr,
    process::Command,
    time::{Duration, Instant},
};

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    docker::{KAFKA_IMAGE_KRAFT, docker_bridge_gateway, docker_rm, kafka_cluster_id_string},
    support,
};

/// A running mixed cluster: two Krabka brokers (ids 1, 2) that hold the
/// metadata-quorum majority, plus one JVM broker (id 3) joined over the real
/// `KRaft` wire. `jvm_container` is the docker container name, already started.
pub struct MixedCluster {
    pub krabka: Vec<(BrokerHandle, TempDir)>,
    jvm_container: String,
    _propdir: TempDir,
    /// Comma-separated `host.docker.internal:<port>` bootstrap for all data
    /// listeners reachable from inside the tool containers.
    pub bootstrap_all: String,
}

impl MixedCluster {
    /// Block, with a bound, until every Krabka broker's view includes `n`
    /// registered brokers. That is, the JVM data-plane broker (id 3) has
    /// finished its `KRaft` join and registered. `CreateTopics(RF=3)` rejects
    /// with `InvalidReplicationFactorException` if it runs before the JVM
    /// broker registers, so every mixed-cluster scenario must gate on this
    /// first. The `AdminClient` can route a request to either Krabka broker,
    /// so one converged view is not enough. This method returns `true` if
    /// every view converged and `false` on timeout. A timeout means the JVM
    /// broker never joined, which is the dominant Linux-vs-Mac difference for
    /// this harness.
    pub async fn wait_for_brokers(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let min_seen = self
                .krabka
                .iter()
                .map(|(h, _)| h.broker_count())
                .min()
                .unwrap_or(0);
            if min_seen >= n {
                return true;
            }
            if Instant::now() > deadline {
                eprintln!(
                    "KRABKA[kip320] only {min_seen}/{n} brokers registered on every Krabka \
                     broker before timeout (JVM broker likely never joined the mixed cluster)"
                );
                return false;
            }
            // intentional: bounded poll for an EXTERNAL JVM broker's KRaft
            // registration; the 2-min bound + bool-on-timeout return (surfacing
            // "JVM never joined") can't be replaced by a 30s panic-on-timeout
            // awaiter.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn shutdown(self) {
        docker_rm(&self.jvm_container);
        for (h, _) in self.krabka {
            h.shutdown().await;
        }
    }
}

/// Build a Krabka broker config that is BOTH a controller voter (in the shared
/// static `KRaft` quorum) and a data-plane broker. Mirrors
/// `jvm_static_quorum_spike.rs::krabka_controller_config` plus a bound data
/// listener.
fn krabka_mixed_config(
    log_dir: &std::path::Path,
    setup: crate::support::JvmStaticVoterSetup,
) -> BrokerConfig {
    let mut cfg = support::jvm_static_voter_config(log_dir, setup);
    cfg.heartbeat_interval = krabka_units::millis(1_000);
    // Kafka's `broker.session.timeout.ms` default. The controller starts a
    // broker's session when its registration lands, and the JVM broker
    // heartbeats every 2 s (`broker.heartbeat.interval.ms`). Under CI load the
    // JVM's first heartbeat can take more than 4 s to reach the controller,
    // and a shorter window then declares the JVM broker dead, shrinks it out
    // of the ISR, and breaks the tests that make it leader.
    cfg.heartbeat_timeout = krabka_units::millis(9_000);
    cfg.replica_lag_time_max = krabka_units::millis(10_000);
    cfg.controller_election_timeout = krabka_units::secs(3);
    cfg.controller_heartbeat_interval = krabka_units::millis(250);
    cfg
}

/// Format a Krabka node's log directory at Kafka 4.0's `metadata.version`.
///
/// The JVM node runs 4.0.0, which supports `metadata.version` only up to
/// `4.0-IV3`, and the Krabka nodes are the controller majority that writes the
/// cluster's bootstrap metadata. Self-bootstrapped, they would finalize Kafka
/// 4.3's `4.3-IV0` and the JVM broker could never register, so they format at
/// the release the oldest node runs, as a Kafka operator mixing in a 4.0 node
/// would. The formatter runs in process because a Bazel test sandbox has no
/// Cargo working tree to spawn it from.
async fn format_at_kafka_4_0(log_dir: &std::path::Path, cluster_id: &str, node: &BrokerConfig) {
    support::format_jvm_voter(log_dir, cluster_id, node).await;
}

/// Stand up two Krabka brokers (the metadata-quorum majority + data plane) and
/// one mirror.gcr.io/apache/kafka:4.0.0 broker, optionally as a controller voter.
/// Returns once the Krabka voters have elected a shared leader. The JVM broker
/// starts detached and the caller polls for it to register.
pub async fn start_mixed_cluster(container: &str, jvm_is_controller: bool) -> MixedCluster {
    support::init_tracing();
    docker_rm(container);

    let cluster_id = Uuid::from_u128(0x4b49_5033_3230_4d49_5845_4451_554f_5255);
    let cid_str = kafka_cluster_id_string(cluster_id);
    let advertised_host = docker_bridge_gateway();

    // Pre-bind 2 Krabka client ports, 3 controller ports.
    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let krabka_client_ports = [client_addrs[0].port(), client_addrs[1].port()];
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();

    let mut krabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
    ];
    if jvm_is_controller {
        krabka_voters.push((3, format!("127.0.0.1:{p3}").parse().unwrap()));
    }

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let cfg1 = krabka_mixed_config(
        dir1.path(),
        crate::support::JvmStaticVoterSetup {
            broker: crate::support::JvmBrokerSetup {
                listen: format!("0.0.0.0:{}", krabka_client_ports[0])
                    .parse()
                    .unwrap(),
                advertised: format!("{}:{}", advertised_host, krabka_client_ports[0]),
                controller: format!("0.0.0.0:{p1}").parse().unwrap(),
                voters: crate::support::controller_voters(&krabka_voters),
                ..Default::default()
            },
            cluster_id,
        },
    );
    let cfg2 = krabka_mixed_config(
        dir2.path(),
        crate::support::JvmStaticVoterSetup {
            broker: crate::support::JvmBrokerSetup {
                node: krabka_broker::NodeId(u64::try_from((1) + 1).expect("one-based node id")),
                listen: format!("0.0.0.0:{}", krabka_client_ports[1])
                    .parse()
                    .unwrap(),
                advertised: format!("{}:{}", advertised_host, krabka_client_ports[1]),
                controller: format!("0.0.0.0:{p2}").parse().unwrap(),
                voters: crate::support::controller_voters(&krabka_voters),
            },
            cluster_id,
        },
    );
    format_at_kafka_4_0(dir1.path(), &cid_str, &cfg1).await;
    format_at_kafka_4_0(dir2.path(), &cid_str, &cfg2).await;
    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("krabka voter 1"),
            s2.await.unwrap().expect("krabka voter 2"),
        )
    };

    // Start JVM node 3, optionally as a controller voter. The capability test
    // uses broker-only mode so UpdateFeatures deterministically reaches Krabka.
    let jvm_data_port = client_addrs[2].port();
    let process_roles = if jvm_is_controller {
        "broker,controller"
    } else {
        "broker"
    };
    let third_voter = if jvm_is_controller {
        format!(",3@localhost:{p3}")
    } else {
        String::new()
    };
    let controller_listener = if jvm_is_controller {
        format!(",CONTROLLER://0.0.0.0:{p3}")
    } else {
        String::new()
    };
    let props = format!(
        "process.roles={process_roles}\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2}{third_voter}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=PLAINTEXT://0.0.0.0:{jvm_data_port}{controller_listener}\n\
         advertised.listeners=PLAINTEXT://{advertised_host}:{jvm_data_port}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n\
         inter.broker.listener.name=PLAINTEXT\n\
         log.dirs=/tmp/kraft-mixed-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("server.properties");
    std::fs::write(&proppath, props).unwrap();
    // The Apache Kafka image runs as a non-root uid. `tempfile` creates its
    // directory as 0700, so a bind-mounted file below it is otherwise present
    // but unreadable on native Linux (the CI runner included).
    #[cfg(unix)]
    {
        crate::support::chmod_for_container(
            propdir.path(),
            0o755,
            "chmod server.properties directory",
        );
        crate::support::chmod_for_container(&proppath, 0o644, "chmod server.properties");
    }
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/s.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/s.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-p",
            &format!("{jvm_data_port}:{jvm_data_port}"),
            "-v",
            &format!("{}:/tmp/s.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_KRAFT,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM broker");
    assert2::assert!(status.success(), "docker run JVM broker failed");

    // Wait for the Krabka voters to elect a shared leader (event-driven: each
    // awaiter resolves once that voter observes a non-zero controller leader).
    c1.wait_until_controller_leader().await;
    c2.wait_until_controller_leader().await;

    let bootstrap_all = format!(
        "{}:{},{}:{},{}:{}",
        advertised_host,
        krabka_client_ports[0],
        advertised_host,
        krabka_client_ports[1],
        advertised_host,
        jvm_data_port,
    );

    MixedCluster {
        krabka: vec![(c1, dir1), (c2, dir2)],
        jvm_container: container.to_string(),
        _propdir: propdir,
        bootstrap_all,
    }
}

/// Advance one partition epoch while retaining its replicas and directory identities.
#[derive(Clone, Copy)]
pub struct PartitionEpochDelta(pub i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SingleLeaderSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    #[default(krabka_broker::NodeId(1))]
    pub leader: krabka_broker::NodeId,
    #[default(krabka_metadata::LeaderEpoch(0))]
    pub epoch: krabka_metadata::LeaderEpoch,
    #[default(PartitionEpochDelta(1))]
    pub partition_epoch_delta: PartitionEpochDelta,
}

pub fn single_leader_record(
    previous: &krabka_metadata::PartitionRecord,
    setup: SingleLeaderSetup<'_>,
) -> krabka_metadata::PartitionRecord {
    krabka_metadata::PartitionRecord {
        topic: setup.topic.to_string(),
        partition: 0,
        leader: setup.leader,
        replicas: previous.replicas.clone(),
        isr: vec![setup.leader],
        leader_epoch: setup.epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: previous.directories.clone(),
        partition_epoch: previous.partition_epoch + setup.partition_epoch_delta.0,
    }
}

/// A registered mixed topology, with a uniquely named JVM container.
pub async fn registered_mixed_cluster(
    container_prefix: &str,
    context: &str,
) -> (String, MixedCluster) {
    let container = support::unique_container_name(container_prefix);
    let cluster = start_mixed_cluster(&container, true).await;
    assert2::assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "{context}"
    );
    (container, cluster)
}

/// Verify the committed prefix independently of the input records that seeded it.
pub fn prefix_log_end(broker: &BrokerHandle, topic: &str, expected: i64, label: &str) -> i64 {
    let prefix_leo = broker
        .local_log_end_offset(topic, 0)
        .expect("Krabka prefix log exists");
    assert2::assert!(
        prefix_leo == expected,
        "expected {label}-record prefix, got LEO {prefix_leo}"
    );
    prefix_leo
}

/// The two independently asserted directions of the mixed-replica divergence tests.
pub enum DivergenceDirection {
    JvmFollower,
    KrabkaFollower,
}

/// Register all three brokers, install the case's ISR and verify its committed prefix.
/// Expected LEO is explicit case data, independent of the count sent to the producer.
pub async fn prepare_divergence(
    topic: &str,
    direction: DivergenceDirection,
) -> (String, MixedCluster, String, i64) {
    let (container_prefix, join_context, required, isr_context, prefix, count, expected, label) =
        match direction {
            DivergenceDirection::JvmFollower => (
                "krabka-kip320-jvm-follower-broker",
                "JVM broker never joined the mixed cluster (only the 2 Krabka brokers registered); the cross-impl KRaft data-plane join is Linux-bound",
                &[3][..],
                "JVM follower never joined ISR",
                "prefix",
                10,
                10,
                "ten",
            ),
            DivergenceDirection::KrabkaFollower => (
                "krabka-kip320-krabka-follower-broker",
                "JVM broker never joined the mixed cluster; cross-impl KRaft join is Linux-bound",
                &[1, 3][..],
                "replicas never converged",
                "rev",
                8,
                8,
                "eight",
            ),
        };
    let (container, cluster) = registered_mixed_cluster(container_prefix, join_context).await;
    let bootstrap = cluster.bootstrap_all.clone();
    crate::topic_admin::create_and_wait_for_isr(&bootstrap, topic, required, isr_context).await;
    crate::docker::produce_committed_prefix(&bootstrap, topic, prefix, count);
    // Intentional: allow the external JVM producer/follower to settle; no broker image or metric expresses this.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leo = prefix_log_end(&cluster.krabka[0].0, topic, expected, label);
    (container, cluster, bootstrap, leo)
}
