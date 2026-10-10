//! KIP-919 `--bootstrap-controller`: the official Kafka admin tools driving
//! krabka's controller listener with no broker endpoint in play.
//!
//! Each tool bootstraps with `DescribeCluster(endpoint_type=CONTROLLERS)`,
//! reads the `ApiVersions` table the controller listener advertises, and then
//! sends its Admin RPCs there. An api key the listener does not route gets no
//! error frame: send a `Metadata` request to a live
//! `mirror.gcr.io/apache/kafka:4.3.1` controller listener and it half-closes,
//! and krabka's does the same. That is why a routing gap surfaces in these
//! tools as a network failure rather than as an unsupported operation, and why
//! these cases assert on the tool exiting zero with the expected output.
//!
//! ## Why not `kafka-topics`
//!
//! `kafka-topics.sh` has no `--bootstrap-controller` option in either pinned
//! image: `mirror.gcr.io/apache/kafka:4.0.0` and `:4.3.1` both exit with
//! `joptsimple.UnrecognizedOptionException` before opening a socket, which
//! [`kafka_topics_has_no_bootstrap_controller_option`] pins. `--describe`
//! would need `Metadata`, whose schema is tagged `broker` only, so no Kafka
//! controller has ever answered it. The five tools that do carry the flag in
//! both images are `kafka-acls`, `kafka-configs`, `kafka-features`,
//! `kafka-metadata-quorum` and `kafka-reassign-partitions`.
//!
//! The JVM `AdminClient` also keeps its own allowlist of calls it will route
//! to a controller, narrower than the listener's advertised set: against a
//! real Kafka 4.3.1 controller, `kafka-configs --bootstrap-controller
//! --entity-type users` fails locally with `UnsupportedEndpointTypeException`
//! even though that controller advertises `AlterUserScramCredentials`. The
//! api keys behind that client-side gate are covered on the wire by
//! `controller_admin_surface` instead; the cases here are the flows a stock
//! Kafka 4.x tool can actually complete.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`.

mod support;

use std::process::Command;

use assert2::check;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};

/// Kafka 4.3.1 is the compatibility oracle for KIP-919.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The principal an unauthenticated controller-listener connection carries.
/// The ACL case makes it a super-user so `kafka-acls` is exercising the ACL
/// store rather than the cluster-Alter gate in front of it.
const CONTROLLER_PRINCIPAL: &str = "ANONYMOUS";

/// Ports for this test process, allocated once rather than fixed, so this
/// suite can run beside the other container suites.
fn ports() -> &'static (String, String, String) {
    static PORTS: std::sync::OnceLock<(String, String, String)> = std::sync::OnceLock::new();
    PORTS.get_or_init(|| {
        let (client, controller) = (support::free_port(), support::free_port());
        (
            format!("host.docker.internal:{client}"),
            format!("0.0.0.0:{client}"),
            format!("0.0.0.0:{controller}"),
        )
    })
}

fn advertised_addr() -> &'static str {
    &ports().0
}

fn listen_addr() -> &'static str {
    &ports().1
}

fn controller_listen() -> &'static str {
    &ports().2
}

/// The controller listener as the CLI containers address it. The voter set
/// carries the same name, because `DescribeCluster` projects the voter
/// endpoints and the tool reconnects to whatever comes back.
fn controller_bootstrap() -> &'static str {
    static ADDR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ADDR.get_or_init(|| controller_listen().replace("0.0.0.0", "host.docker.internal"))
}

/// Boot an in-process broker whose controller listener the CLI containers
/// reach at [`controller_bootstrap`], letting the caller adjust the config.
async fn start_host_broker_with(
    adjust: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, tempfile::TempDir) {
    crate::support::init_jvm_tracing("krabka_broker=info,warn");
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig {
        controller_quorum_voters: vec![(
            krabka_broker::NodeId(1),
            controller_bootstrap().to_string(),
        )],
        ..crate::support::jvm_broker_config(
            dir.path().to_path_buf(),
            crate::support::JvmBrokerSetup {
                listen: listen_addr().parse().expect("allocated addr"),
                controller: controller_listen().parse().expect("allocated addr"),
                advertised: (advertised_addr()).to_owned(),
                voters: crate::support::controller_voters(&[(
                    1,
                    controller_listen().parse().expect("allocated addr"),
                )]),
                ..Default::default()
            },
        )
    };
    adjust(&mut config);
    let handle = Broker::start(config).await.expect("start broker");
    (handle, dir)
}

/// Run one Kafka admin tool with `--bootstrap-controller <controller>` from
/// the oracle container, which reaches the host through
/// `--add-host=host.docker.internal:host-gateway`. Returns its stdout, and
/// fails the test with the captured output when the tool exits non-zero --
/// which is what a routing gap looks like from inside the JVM.
fn kafka_tool(tool: &str, args: &[&str]) -> String {
    let binary = format!("/opt/kafka/bin/{tool}.sh");
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        &binary,
        "--bootstrap-controller",
        controller_bootstrap(),
    ];
    full.extend_from_slice(args);
    let output = Command::new("docker")
        .args(&full)
        .output()
        .unwrap_or_else(|error| panic!("spawn docker run {tool}: {error}"));
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    eprintln!(
        "KRABKA[test] {tool} {args:?} status={}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
    check!(
        output.status.success(),
        "{tool} {args:?} failed: stdout={stdout} stderr={stderr}"
    );
    stdout
}

/// `kafka-configs --bootstrap-controller --entity-type brokers`: `--alter`
/// writes a dynamic config through `IncrementalAlterConfigs` (44) and
/// `--describe` reads it back through `DescribeConfigs` (32), both over the
/// controller listener with no broker endpoint configured anywhere.
///
/// The KIP-73 replication throttle is the config under test because krabka
/// takes that one per broker; the cluster-wide keys would name a different
/// resource and exercise the same two api keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_bootstrap_controller_round_trips_a_dynamic_broker_config() {
    const THROTTLE: &str = "leader.replication.throttled.rate=1048576";

    let (broker, _dir) = start_host_broker_with(|_| {}).await;

    kafka_tool(
        "kafka-configs",
        &[
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--alter",
            "--add-config",
            THROTTLE,
        ],
    );
    let described = kafka_tool(
        "kafka-configs",
        &[
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--describe",
        ],
    );

    check!(
        described.contains(THROTTLE),
        "describe missing the dynamic config just written: {described}"
    );
    broker.shutdown().await;
}

/// `kafka-acls --bootstrap-controller` end to end: `--add`, `--list`,
/// `--remove`, then `--list` again. That is `CreateAcls` (30), `DescribeAcls`
/// (29) and `DeleteAcls` (31) routed over the controller listener, which is
/// the add/read/delete lifecycle KIP-919 exists to keep available when no
/// broker is reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_acls_bootstrap_controller_add_list_remove() {
    const TOPIC: &str = "controller-acl-topic";

    let (broker, _dir) = start_host_broker_with(|config| {
        // The PLAINTEXT controller listener carries `ANONYMOUS`, so that
        // principal is the super user, as Kafka's docs set it up.
        config.authorizer =
            std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
                std::iter::once(CONTROLLER_PRINCIPAL.to_owned()).collect(),
            ));
    })
    .await;

    kafka_tool(
        "kafka-acls",
        &[
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            TOPIC,
        ],
    );
    let listed = kafka_tool("kafka-acls", &["--list", "--topic", TOPIC]);
    check!(
        listed.contains("User:alice") && listed.contains("READ"),
        "list missing the ACL just written: {listed}"
    );

    kafka_tool(
        "kafka-acls",
        &[
            "--remove",
            "--force",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            TOPIC,
        ],
    );
    let remaining = kafka_tool("kafka-acls", &["--list", "--topic", TOPIC]);

    check!(
        !remaining.contains("User:alice"),
        "the ACL survived the remove: {remaining}"
    );
    broker.shutdown().await;
}

/// `kafka-reassign-partitions --bootstrap-controller --list` reaches
/// `ListPartitionReassignments` (46) on the controller listener and reports an
/// empty set rather than a disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_reassign_partitions_bootstrap_controller_lists() {
    let (broker, _dir) = start_host_broker_with(|_| {}).await;

    let listed = kafka_tool("kafka-reassign-partitions", &["--list"]);

    check!(
        listed.contains("No partition reassignments found."),
        "unexpected reassignment listing: {listed}"
    );
    broker.shutdown().await;
}

/// `kafka-topics` carries no `--bootstrap-controller` option in the pinned
/// oracle image, which is why the topic lifecycle is proved on the controller
/// listener by `controller_admin_surface` rather than by this suite. Pinning
/// it here keeps the reason checkable rather than a comment: if a later Kafka
/// grows the option, this case fails and the suite gains a real end-to-end
/// topic test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_has_no_bootstrap_controller_option() {
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-controller",
            "127.0.0.1:1",
            "--list",
        ])
        .output()
        .expect("spawn docker run kafka-topics");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    check!(
        combined.contains("bootstrap-controller is not a recognized option"),
        "kafka-topics grew a --bootstrap-controller option: {combined}"
    );
}

/// `kafka-features --bootstrap-controller`: `describe`, then `downgrade`, then
/// `describe` again. That is `ApiVersions` (18) and `UpdateFeatures` (57)
/// routed over the controller listener.
///
/// KIP-919 names feature management as a thing an operator must be able to do
/// with no broker reachable -- a cluster whose brokers will not start because
/// of a finalized feature level is exactly when this matters, and it is the
/// only tool of the three offering `--bootstrap-controller` that had no
/// container test driving it that way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_features_bootstrap_controller_describes_and_downgrades() {
    let (broker, _dir) = start_host_broker_with(|_| {}).await;

    let described = kafka_tool("kafka-features", &["describe"]);
    let before = support::jvm_finalized_level(&described, "transaction.version");
    check!(
        before == Some(2),
        "expected transaction.version finalized at 2 before the downgrade, got {before:?}: {described}"
    );

    kafka_tool(
        "kafka-features",
        &[
            "downgrade",
            "--feature",
            "transaction.version=1",
            "--unsafe",
        ],
    );

    let after_out = kafka_tool("kafka-features", &["describe"]);
    let after = support::jvm_finalized_level(&after_out, "transaction.version");
    check!(
        after == Some(1),
        "downgrade did not move transaction.version over the controller listener, got {after:?}: {after_out}"
    );
    broker.shutdown().await;
}
