//! KIP-1022 "updating features" — JVM acceptance.
//!
//! Drives the real `mirror.gcr.io/apache/kafka:4.3.1` `kafka-features` and
//! `kafka-metadata-quorum` admin tools against an
//! in-process Krabka broker advertised at `host.docker.internal:9092`, proving
//! the `UpdateFeatures` / `ApiVersions` feature surface round-trips end to end:
//!
//! 1. `describe` lists Krabka's finalized features (`metadata.version`,
//!    `group.version`, `transaction.version`) at the self-bootstrap defaults.
//! 2. `downgrade --feature transaction.version=1` is accepted and a follow-up
//!    `describe` reflects the change.
//! 3. `upgrade --feature transaction.version=2` round-trips it back.
//! 4. `kraft.version` upgrades from 0 to 1, survives `describe`, and cannot be
//!    downgraded.
//! 5. `kafka-metadata-quorum describe` reads the exact live voter identity and
//!    `remove-controller` reaches the last-voter safety check.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`. Binds host port
//! 9092, so it must not run concurrently with `jvm_acceptance` (single test
//! here keeps it self-contained).

mod support;

use {assert2::assert, krabka_broker::BrokerHandle, std::process::Command};

/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

fn controller_listen() -> &'static str { &support::jvm_listeners().controller }

/// The controller as the containers address it.
fn controller_bootstrap() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    V.get_or_init(|| controller_listen().replace("0.0.0.0", "host.docker.internal"))
}

/// The controller's port on its own, for callers that need the number rather
/// than the address -- a `VoterEndpoint` carries host and port separately.
fn controller_port() -> u16 {
    controller_listen()
        .rsplit(':')
        .next()
        .expect("controller addr has a port")
        .parse()
        .expect("controller port is numeric")
}

/// A second controller port, for the node that joins the quorum.
fn joiner_controller() -> &'static str {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    V.get_or_init(|| format!("host.docker.internal:{}", support::free_port()))
}
/// Kafka 4.3.1 is the compatibility oracle for KIP-853 and KIP-1186.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";
const DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(1);
const DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAQ";
const JOINER_DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(2);
const JOINER_DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAg";

/// A running broker and the directory that backs it.
struct StartedBroker {
    handle: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Boot an in-process Krabka broker listening on `listen_addr()`, advertised as
/// `host.docker.internal:9092`. A standalone self-bootstrap finalizes the
/// latest-release feature defaults (metadata.version=25, group.version=1,
/// transaction.version=2).
async fn start_host_broker() -> StartedBroker {
    let (handle, dir) = support::start_jvm_single("krabka_broker=info,warn", |config| {
        config.directory_id = DIRECTORY_ID;
        config.controller_quorum_voters =
            vec![(krabka_broker::NodeId(1), controller_bootstrap().to_string())];
    })
    .await;
    StartedBroker { handle, dir }
}
/// A running controller observer and the directory that backs it.
struct StartedObserver {
    handle: krabka_raft::ControllerHandle,
    dir: tempfile::TempDir,
}

/// Start a caught-up controller observer without auto-join. The official JVM
/// `add-controller` command promotes this exact live identity.
async fn start_host_observer() -> StartedObserver {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config =
        krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(2), dir.path().to_path_buf());
    config.directory_id = JOINER_DIRECTORY_ID;
    config.controller_listen_addr = joiner_controller()
        .replace("host.docker.internal", "0.0.0.0")
        .parse()
        .expect("allocated addr");
    config.bootstrap_mode = krabka_raft::BootstrapMode::Join;
    config.cluster_id = Some(uuid::Uuid::nil());
    config.initial_voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
        id: krabka_raft::NodeId(1),
        directory_id: DIRECTORY_ID,
        endpoints: vec![krabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: controller_port(),
        }],
        kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
    }]);
    let handle = krabka_raft::Controller::start(config)
        .await
        .expect("start observer");
    StartedObserver { handle, dir }
}

/// Run `kafka-features <args>` from the Kafka oracle container that can
/// reach the host broker via `--add-host=host.docker.internal:host-gateway`.
fn kafka_features(args: &[&str]) -> std::process::Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap_addr(),
    ];
    full.extend_from_slice(args);
    let out = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-features");
    eprintln!(
        "KRABKA[test] kafka-features {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Run `kafka-metadata-quorum <args>` against the controller listener.
fn kafka_metadata_quorum(args: &[&str]) -> std::process::Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-metadata-quorum.sh",
        "--bootstrap-controller",
        controller_bootstrap(),
    ];
    full.extend_from_slice(args);
    let output = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-metadata-quorum");
    eprintln!(
        "KRABKA[test] kafka-metadata-quorum {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

/// Run Kafka 4.3.1's `add-controller` with a real controller properties file
/// and Kafka-compatible `meta.properties` for node 2.
fn kafka_add_controller() -> std::process::Output {
    let mount_dir = tempfile::tempdir().expect("controller command config dir");
    let metadata_dir = mount_dir.path().join("metadata");
    std::fs::create_dir(&metadata_dir).expect("create metadata dir");
    let joiner = joiner_controller();
    let quorum = controller_bootstrap();
    let properties = format!(
        "process.roles=controller\n\
         node.id=2\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://{joiner}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.bootstrap.servers={quorum}\n\
         log.dirs=/tmp/kraft-controller-2\n"
    );
    let properties_path = mount_dir.path().join("controller.properties");
    std::fs::write(&properties_path, properties).expect("write controller properties");
    std::fs::write(
        metadata_dir.join("meta.properties"),
        format!(
            "version=1\ncluster.id=AAAAAAAAAAAAAAAAAAAAAA\nnode.id=2\ndirectory.id={JOINER_DIRECTORY_ID_BASE64}\n"
        ),
    )
    .expect("write meta.properties");
    let properties_mount = format!(
        "{}:/tmp/controller.properties:ro",
        properties_path.display()
    );
    let metadata_mount = format!("{}:/tmp/kraft-controller-2", metadata_dir.display());
    let output = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &properties_mount,
            "-v",
            &metadata_mount,
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-metadata-quorum.sh",
            "--bootstrap-controller",
            controller_bootstrap(),
            "--command-config",
            "/tmp/controller.properties",
            "add-controller",
        ])
        .output()
        .expect("spawn docker run kafka-metadata-quorum add-controller");
    eprintln!(
        "KRABKA[test] kafka-metadata-quorum add-controller status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

/// Require the requested feature alteration to succeed through the stock JVM tool.
fn alter_feature(verb: &str, spec: &str) {
    let out = kafka_features(&[verb, "--feature", spec]);
    assert!(
        out.status.success(),
        "{verb} {spec} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_features_describe_and_round_trip() {
    let StartedBroker { handle, dir: _dir } = start_host_broker().await;

    // 1. describe — Krabka advertises + finalizes the feature surface.
    let desc = kafka_features(&["describe"]);
    assert!(desc.status.success(), "describe failed");
    let out = String::from_utf8_lossy(&desc.stdout);
    // (feature, pinned starting finalized level; None = only presence is pinned)
    let features = [
        ("metadata.version", None),
        ("group.version", Some(1)),
        ("transaction.version", Some(2)),
    ];
    for (feature, want_level) in features {
        assert!(
            out.contains(feature),
            "describe must list {feature}:\n{out}"
        );
        if let Some(want) = want_level {
            assert!(
                support::jvm_finalized_level(&out, feature) == Some(want),
                "{feature} must start finalized at {want}:\n{out}"
            );
        }
    }

    // 2. downgrade transaction.version 2 -> 1 (within the advertised range),
    // 3. then upgrade it 1 -> 2 again; a follow-up describe reflects each change.
    let round_trip = [
        ("downgrade", "transaction.version=1", 1),
        ("upgrade", "transaction.version=2", 2),
    ];
    for (verb, spec, want) in round_trip {
        alter_feature(verb, spec);
        let desc = kafka_features(&["describe"]);
        let text = String::from_utf8_lossy(&desc.stdout);
        assert!(
            support::jvm_finalized_level(&text, "transaction.version") == Some(want),
            "transaction.version should be {want} after {verb}:\n{text}"
        );
    }

    // 4. KIP-853 activation is a Raft control operation, not an ordinary
    // FeatureLevelRecord. Kafka 4.3.1 must observe the finalized level and the
    // irreversible level-one boundary.
    let upgrade = kafka_features(&["upgrade", "--feature", "kraft.version=1"]);
    assert!(
        upgrade.status.success(),
        "kraft.version upgrade failed: {}",
        String::from_utf8_lossy(&upgrade.stderr)
    );
    let desc = kafka_features(&["describe"]);
    let text = String::from_utf8_lossy(&desc.stdout);
    assert!(
        support::jvm_finalized_level(&text, "kraft.version") == Some(1),
        "kraft.version should be finalized at 1:\n{text}"
    );
    let downgrade = kafka_features(&["downgrade", "--feature", "kraft.version=0"]);
    assert!(
        !downgrade.status.success(),
        "kraft.version downgrade must fail"
    );

    // 5. Exercise the official quorum tool on the controller listener. A live
    // observer is promoted by `add-controller`, then removed by
    // `remove-controller` before the last-voter rejection is checked.
    let StartedObserver {
        handle: observer,
        dir: _observer_dir,
    } = start_host_observer().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        observer
            .watch_image()
            .wait_for(|image| image.kraft_version() == 1),
    )
    .await
    .expect("observer did not fetch kraft.version=1 within 30s")
    .expect("observer image channel closed");
    let add = kafka_add_controller();
    assert!(
        add.status.success(),
        "add-controller failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    handle
        .wait_for_image(|image| image.voters().len() == 2)
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        observer
            .watch_image()
            .wait_for(|image| image.voters().len() == 2),
    )
    .await
    .expect("observer did not commit the second voter within 30s")
    .expect("observer image channel closed");

    let quorum = kafka_metadata_quorum(&["describe", "--status"]);
    assert!(
        quorum.status.success(),
        "metadata quorum describe failed: {}",
        String::from_utf8_lossy(&quorum.stderr)
    );
    let quorum_text = String::from_utf8_lossy(&quorum.stdout);
    assert!(
        quorum_text
            .lines()
            .any(|line| line.starts_with("LeaderId:") && line.ends_with('1'))
    );
    assert!(
        quorum_text.lines().any(|line| {
            line.starts_with("CurrentVoters:")
                && line.contains("\"id\": 1")
                && line.contains(&format!("CONTROLLER://{}", controller_bootstrap()))
                && line.contains("\"id\": 2")
                && line.contains(joiner_controller())
        }),
        "unexpected voter projection:\n{quorum_text}"
    );

    let remove_joiner = kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "2",
        "--controller-directory-id",
        JOINER_DIRECTORY_ID_BASE64,
    ]);
    assert!(
        remove_joiner.status.success(),
        "remove-controller failed: {}",
        String::from_utf8_lossy(&remove_joiner.stderr)
    );
    handle
        .wait_for_image(|image| image.voters().len() == 1)
        .await;

    let remove = kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "1",
        "--controller-directory-id",
        DIRECTORY_ID_BASE64,
    ]);
    assert!(
        !remove.status.success(),
        "removing the last voter must fail"
    );
    // Kafka's `VoterSet.removeVoter` yields no new set when only one voter
    // remains, and `RemoveVoterHandler` answers that with `VOTER_NOT_FOUND`.
    let remove_error = String::from_utf8_lossy(&remove.stderr);
    assert!(
        remove_error.contains("VoterNotFoundException"),
        "unexpected remove-controller error: {remove_error}"
    );

    observer.shutdown().await;
    handle.shutdown().await;
}

/// KIP-966: `eligible.leader.replicas.version` is a row the stock tool can
/// describe, disable and enable.
///
/// `kafka-features describe` lists every feature the broker advertises in
/// `ApiVersions.supported_features`. A fresh cluster bootstraps at
/// `4.3-IV0`, and `ELRV_1` bootstraps at 4.1-IV0, so the row starts finalized
/// at 1, as it does on a freshly formatted Kafka 4.3 cluster. An operator then
/// turns it off with `downgrade` and on again with `upgrade`, which is what a
/// runbook or a Terraform apply that pins the flag does. The broker-side
/// effect of each level (the controller publishing or clearing ELR) is pinned
/// by the in-process tests; this case pins that the JVM tool can drive it at
/// all, which is the bug the row's absence caused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn kafka_features_describes_and_round_trips_elr() {
    const ELR: &str = "eligible.leader.replicas.version";

    let StartedBroker { handle, dir: _dir } = start_host_broker().await;

    let desc = kafka_features(&["describe"]);
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let out = String::from_utf8_lossy(&desc.stdout);
    assert!(out.contains(ELR), "describe must list {ELR}:\n{out}");
    assert!(
        support::jvm_finalized_level(&out, ELR) == Some(1),
        "{ELR} must start enabled:\n{out}"
    );

    for (verb, want) in [("downgrade", 0), ("upgrade", 1)] {
        let spec = format!("{ELR}={want}");
        alter_feature(verb, &spec);
        let desc = kafka_features(&["describe"]);
        let text = String::from_utf8_lossy(&desc.stdout);
        // A level-0 finalize is KIP-584's delete, so the finalized level is
        // either absent or 0 after the downgrade; both read as disabled.
        assert!(
            support::jvm_finalized_level(&text, ELR).unwrap_or(0) == i64::from(want),
            "{ELR} should read {want} after {verb}:\n{text}"
        );
    }

    handle.shutdown().await;
}
