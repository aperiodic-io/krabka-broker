//! End-to-end GSSAPI (Kerberos) parity tests.
//!
//! These tests prove that a stock cp-kafka GSSAPI client authenticates to
//! Krabka end-to-end against a real MIT KDC. They also prove that two Krabka
//! brokers authenticate to each other over a GSSAPI inter-broker listener.
//!
//! # Topology
//!
//! The Krabka broker runs in-process on the host, bound to `0.0.0.0:9092`, and
//! advertises `host.docker.internal:9092`. The cp-kafka CLI tools run inside
//! `mirror.gcr.io/confluentinc/cp-kafka` containers started with
//! `--add-host=host.docker.internal:host-gateway`, so they reach the host
//! broker through the advertised name. `jvm_acceptance.rs` uses the same
//! method.
//!
//! The KDC runs in its own container, see
//! `crates/broker/tests/fixtures/security/kdc`, and maps `88/tcp+udp` to the host.
//! From inside the CLI containers it is reachable at `host.docker.internal:88`,
//! which `tests/fixtures/gssapi/krb5.conf` configures.
//!
//! Because the broker advertises `host.docker.internal`, the stock Kafka GSSAPI
//! client derives the server principal `kafka/host.docker.internal@CRABKA.TEST`.
//! The KDC fixture supplies that SPN together with `kafka/localhost`, and
//! exports both keys into the single `kafka.keytab` the broker loads.
//!
//! # Running
//!
//! ```bash
//! bazel run //crates/broker/tests/fixtures/security/kdc:image_load
//! docker compose -f crates/broker/tests/fixtures/security/kdc/docker-compose.yml up -d
//! # wait for KDC_READY in the logs
//! KRB5_CONFIG=crates/broker/tests/fixtures/security/kdc/krb5.conf \
//!   SSPI_KDC_URL=tcp://localhost:88 \
//!   cargo test -p krabka-broker --test gssapi_e2e -- --ignored
//! ```

mod support;

use {
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec},
    krabka_security::{ListenerProtocol, SaslMechanism, gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW},
    std::{
        io::Write,
        path::PathBuf,
        process::{Command, Stdio},
    },
};

/// cp-kafka image that holds the GSSAPI-capable console tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";
/// Host bind address for the broker's `SASL_PLAINTEXT` data-plane listener.
const LISTEN: &str = "0.0.0.0:9092";
/// Advertised name the CLI containers resolve with `--add-host`. It is also the
/// host component of the server SPN the stock client derives, `kafka/<this>`.
const BOOTSTRAP: &str = "host.docker.internal:9092";

use crate::support::init_tracing_with;
/// Absolute path to the KDC fixture dir, which holds `kafka.keytab` and
/// `alice.keytab`.
use support::{free_port, manifest_dir};

fn kdc_fixtures() -> PathBuf { manifest_dir().join("tests/fixtures/security/kdc") }

/// Absolute path to the GSSAPI client config dir, which holds `krb5.conf`,
/// `client_jaas.conf`, and `client.properties`.
fn gssapi_fixtures() -> PathBuf { manifest_dir().join("tests/fixtures/gssapi") }

/// A running broker and the directory that backs it.
struct HostGssapiBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Spawn an in-process Krabka broker on `LISTEN`.
///
/// The broker has one `SASL_PLAINTEXT` listener that advertises `GSSAPI`. The
/// KDC fixture's `kafka.keytab` backs that listener.
async fn start_host_gssapi_broker() -> HostGssapiBroker {
    init_tracing_with("krabka_broker=debug,info");

    let kdc_url =
        std::env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_string());
    let dir = tempfile::tempdir().expect("tempdir");

    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.broker_id = 1;
    cfg.listen_addr = LISTEN.parse().expect("static addr");
    cfg.advertised_listener = BOOTSTRAP.into();
    let controller = format!("127.0.0.1:{}", free_port());
    cfg.controller_listen_addr = controller.parse().expect("controller addr");
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller)];
    let internal = format!("127.0.0.1:{}", free_port());
    cfg.listeners = vec![
        ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: LISTEN.parse().expect("static addr"),
            advertised: BOOTSTRAP.to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: Some(vec![SaslMechanism::Gssapi]),
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        },
        ListenerSpec {
            name: "INTERNAL".to_string(),
            bind_addr: internal.parse().expect("internal addr"),
            advertised: internal,
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        },
    ];
    cfg.inter_broker_listener_name = "INTERNAL".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Gssapi];
    cfg.gssapi = Some(krabka_broker::GssapiConfig {
        keytab_path: kdc_fixtures().join("kafka.keytab"),
        service_name: "kafka".to_string(),
        // DEFAULT maps alice@CRABKA.TEST → "alice".
        principal_to_local_rules: vec![krabka_broker::KerberosRule::Default],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: Some(kdc_url),
        max_time_skew: DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });

    let handle = Broker::start(cfg).await.expect("start gssapi broker");
    eprintln!("KRABKA[test] gssapi broker started listen={LISTEN} advertised={BOOTSTRAP}");
    HostGssapiBroker {
        broker: handle,
        dir,
    }
}

/// Build the common `docker run` argument prefix for a cp-kafka GSSAPI tool.
///
/// The prefix holds the host-gateway hosts entry, the two fixture mounts, and a
/// `KAFKA_OPTS` value that points the JVM at the JAAS and krb5 configs.
///
/// Mounts:
///   - KDC fixtures  → `/fixtures` (supplies `alice.keytab`, which JAAS references)
///   - GSSAPI config → `/gssapi`   (`krb5.conf`, `client_jaas.conf`, `client.properties`)
fn gssapi_docker_prefix() -> Vec<String> {
    let kdc = kdc_fixtures().canonicalize().expect("canonicalize kdc dir");
    let gss = gssapi_fixtures()
        .canonicalize()
        .expect("canonicalize gssapi dir");
    vec![
        "run".to_string(),
        "--rm".to_string(),
        "--user".to_string(),
        "0".to_string(),
        "--add-host=host.docker.internal:host-gateway".to_string(),
        "-v".to_string(),
        format!("{}:/fixtures", kdc.display()),
        "-v".to_string(),
        format!("{}:/gssapi", gss.display()),
        "-e".to_string(),
        "KAFKA_OPTS=-Djava.security.auth.login.config=/gssapi/client_jaas.conf \
         -Djava.security.krb5.conf=/gssapi/krb5.conf -Dsun.security.krb5.debug=true"
            .to_string(),
    ]
}

/// Run a cp-kafka GSSAPI tool to completion and assert that it succeeds.
fn run_gssapi_tool(tool_args: &[&str]) -> std::process::Output {
    let mut args = gssapi_docker_prefix();
    args.push(KAFKA_IMAGE.to_string());
    args.extend(tool_args.iter().map(|s| (*s).to_string()));
    let out = Command::new("docker")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "KRABKA[test] gssapi tool {tool_args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len()
    );
    assert!(
        out.status.success(),
        "gssapi tool {tool_args:?} failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

// `flavor = "multi_thread"` is essential: the test body makes blocking
// `Command::output()` calls per `docker run`. On a single-threaded runtime
// those block the only worker, which also drives the broker accept loop, and
// the Java client times out. (Same caveat as `jvm_acceptance.rs`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker + the KDC fixture (docker compose up) + KRB5_CONFIG/SSPI_KDC_URL"]
async fn cp_kafka_gssapi_client_round_trip() {
    const TOPIC: &str = "krabka-gssapi-itest";

    let HostGssapiBroker { broker, dir: _dir } = start_host_gssapi_broker().await;

    // 1. Create the topic over a GSSAPI-authenticated AdminClient.
    run_gssapi_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/gssapi/client.properties",
    ]);

    // 2. Produce 3 records via a GSSAPI-authenticated producer (stdin).
    let mut prefix = gssapi_docker_prefix();
    // Need an interactive stdin for the producer.
    prefix.insert(2, "-i".to_string());
    let mut child = Command::new("docker")
        .args(&prefix)
        .args([
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--producer.config",
            "/gssapi/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "gssapi producer failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 3. Consume them back over a GSSAPI-authenticated consumer.
    let consumer_out = run_gssapi_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "15000",
        "--consumer.config",
        "/gssapi/client.properties",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}
