//! Differential evidence that `fetch.min.bytes` is a floor and not a hint.
//!
//! The claim under test is Kafka's, not krabka's: `kafka.server.DelayedFetch`
//! completes only once the accumulated bytes reach `minBytes`, on an error
//! condition, or on expiry, so a Fetch asking for more bytes than the log
//! holds waits out its whole `fetch.max.wait.ms` and then answers with what is
//! there. This suite sends one wire exchange -- the same
//! [`wire::min_bytes_exchange`] the hermetic `fetch_min_bytes` suite sends --
//! to a pinned Apache Kafka broker and to krabka, and compares the two answers
//! as one value, plus the bound on how long each was held.
//!
//! The Kafka side runs as a single combined-mode container with its client
//! port published, because the client here is the Rust one in this process
//! rather than a JVM tool in a container: the point is that identical bytes
//! reach both brokers.
//!
//! The case is `#[ignore]`d because it needs Docker, and the Bazel lane that
//! owns this suite runs it with `--ignored`.
//!
//! ```text
//! cargo test -p krabka-broker --test fetch_min_bytes_jvm -- --ignored
//! ```

mod support;
#[path = "fetch_min_bytes/wire.rs"]
mod wire;

use {
    crate::{
        support::discovery::topic_metadata_request,
        wire::{HELD_AT_LEAST, min_bytes_exchange},
    },
    assert2::assert,
    base64::Engine as _,
    krabka_client_core::Client,
    std::{
        process::Command,
        time::{Duration, Instant},
    },
};

/// The Kafka release krabka is compared against: the newest image
/// //bazel/images pins, and so the one whose Fetch purgatory is current.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// How long the oracle gets to answer a `Metadata` request before the suite
/// gives up on it.
const ORACLE_BOOT_BUDGET: Duration = Duration::from_secs(120);

/// A single-node Apache Kafka broker in a container, with its client port
/// published to the host.
struct Oracle {
    container: String,
    bootstrap: String,
    _properties: tempfile::TempDir,
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
    }
}

impl Oracle {
    /// Boot the oracle. The properties file is bind-mounted, the way the rest
    /// of the JVM harness configures a stock image.
    fn start() -> Self {
        let port = support::free_port();
        let controller_port = support::free_port();
        let container = support::unique_container_name("krabka-fetch-min-bytes");
        // The broker advertises the published port on loopback, because the
        // only client is on the host.
        let properties = format!(
            "process.roles=broker,controller\n\
             node.id=1\n\
             inter.broker.listener.name=PLAINTEXT\n\
             controller.quorum.voters=1@localhost:{controller_port}\n\
             controller.listener.names=CONTROLLER\n\
             listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n\
             listeners=PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:{controller_port}\n\
             advertised.listeners=PLAINTEXT://127.0.0.1:{port}\n\
             offsets.topic.replication.factor=1\n\
             transaction.state.log.replication.factor=1\n\
             transaction.state.log.min.isr=1\n\
             log.dirs=/tmp/kraft-fetch-min-bytes\n"
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("server.properties");
        std::fs::write(&path, properties).expect("write server.properties");
        // The Apache Kafka image runs as a non-root uid, and `tempfile`
        // creates its directory as 0700, so a bind-mounted file below it is
        // otherwise present but unreadable.
        #[cfg(unix)]
        {
            support::chmod_for_container(dir.path(), 0o755, "chmod properties directory");
            support::chmod_for_container(&path, 0o644, "chmod server.properties");
        }
        let cluster_id = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(uuid::Uuid::from_u128(0x0FE7_C400_0000_0000_0000_0000_0000_0001).as_bytes());
        let entry = format!(
            "/opt/kafka/bin/kafka-storage.sh format -t {cluster_id} --config /tmp/s.properties \
             --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /tmp/s.properties"
        );
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container,
                "-p",
                &format!("{port}:{port}"),
                "-v",
                &format!("{}:/tmp/s.properties", path.display()),
                "--entrypoint",
                "bash",
                KAFKA_IMAGE,
                "-c",
                &entry,
            ])
            .status()
            .expect("docker run the Kafka oracle");
        assert!(status.success(), "docker run the Kafka oracle failed");
        Self {
            container,
            bootstrap: format!("127.0.0.1:{port}"),
            _properties: dir,
        }
    }

    /// Block until the oracle answers a request.
    ///
    /// The probe is a whole `Metadata` round trip, not a connect: building a
    /// [`Client`] only resolves the bootstrap address, and the published port
    /// is accepted by Docker's proxy from the moment the container starts, so
    /// anything short of a served response says nothing about whether the
    /// broker inside is up. Every request until it is answers `Disconnected`.
    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + ORACLE_BOOT_BUDGET;
        loop {
            if self.metadata_probe().await.is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the Kafka oracle never answered Metadata within {ORACLE_BOOT_BUDGET:?}; \
                 container logs:\n{}",
                self.logs(),
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// One `Metadata` request, on a connection of its own, answered only by a
    /// broker that is serving.
    async fn metadata_probe(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::builder()
            .bootstrap(&self.bootstrap)
            .client_id("krabka-fetch-min-bytes")
            .build()
            .await?;
        let response = client
            .send(topic_metadata_request(Some(Vec::new())))
            .await?;
        if response.brokers.is_empty() {
            return Err("the oracle advertised no broker yet".into());
        }
        Ok(())
    }

    /// Everything the container has printed, for a boot that never finished.
    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.container])
            .output()
            .expect("spawn docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }
}

/// Both brokers hold the same under-`min_bytes` Fetch for the same wait, and
/// then answer it with the same records.
#[tokio::test]
#[ignore = "requires Docker"]
async fn krabka_and_kafka_both_hold_a_fetch_below_its_min_bytes() {
    let oracle = Oracle::start();
    oracle.wait_until_ready().await;
    let (kafka_held, kafka_facts) = min_bytes_exchange(&oracle.bootstrap, "fetch-min-bytes").await;

    let krabka = support::start().await;
    let bootstrap = krabka.broker.listen_addr().to_string();
    let (krabka_held, krabka_facts) = min_bytes_exchange(&bootstrap, "fetch-min-bytes").await;

    assert!(kafka_held >= HELD_AT_LEAST);
    assert!(krabka_held >= HELD_AT_LEAST);
    assert!(krabka_facts == kafka_facts);
}
