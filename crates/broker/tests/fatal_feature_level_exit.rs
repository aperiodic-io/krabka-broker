//! `krabka-broker` exits with a failure status when its controller meets a
//! fatal fault: it replays a feature level that it does not support, or its
//! activation cannot write the bootstrap records.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws, and
//! Kafka's `QuorumController` gives a failed activation to its
//! `fatalFaultHandler`. In both cases the `ProcessTerminatingFaultHandler`
//! that the controller server installs halts the process with status 1. One
//! case formats a node at `4.4-IV2`, an unstable `metadata.version` in Kafka
//! 4.3, and boots the binary without `unstable.feature.versions.enable`. The
//! formatted bootstrap records finalize the unstable level as soon as the node
//! leads its own quorum. The other case formats bootstrap records that create
//! one topic name twice, which the active controller refuses to write.
//!
//! The binary's stop after a fault that lands once it is running is a unit test
//! beside `serve`, since no command line can commit an unsupported level to a
//! started broker.

use {
    assert2::{assert, check},
    krabka_metadata::{MetadataRecord, TopicRecord},
    std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    },
};

/// A loopback port that nothing holds at the moment, for the controller
/// listener. `krabka format` records it as the voter's endpoint, so it has to be
/// a known number, and it cannot stay on the binary's default port 9093, which
/// `cli_smoke` binds while this suite may run.
fn free_loopback_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Formats a standalone log directory with the extra `flags`, and `extra`
/// seeded into the bootstrap records. The voter set records `controller_port`
/// as this node's endpoint, and the broker has to listen there.
///
/// Called in process rather than spawned, as `cli_smoke` does: the formatting
/// is setup, not the thing under test.
fn format_standalone(
    log_dir: &std::path::Path,
    controller_port: u16,
    flags: &[&str],
    extra: Vec<MetadataRecord>,
) {
    let mut argv: Vec<String> = [
        "krabka-format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        &format!("127.0.0.1:{controller_port}"),
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    argv.extend(flags.iter().map(|flag| (*flag).to_owned()));
    let code = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
        .block_on(krabka_format::run_from_args_with_records(argv, extra));
    assert!(code == 0, "krabka-format exited {code}");
}

/// A topic record of `orders` with the topic id `id`.
fn orders(id: u128) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: uuid::Uuid::from_u128(id),
        partitions: 1,
        replication_factor: 1,
    })
}

/// One case: what it exercises, the format flags, the records seeded into the
/// bootstrap records, and the fault on stderr.
struct FatalFaultCase {
    what: &'static str,
    flags: &'static [&'static str],
    extra: Vec<MetadataRecord>,
    fault: &'static str,
}

#[test]
fn the_broker_exits_non_zero_with_kafkas_message_over_a_fatal_controller_fault() {
    let cases: [FatalFaultCase; 2] = [
        FatalFaultCase {
            what: "an unsupported feature level",
            flags: &[
                "--release-version",
                "4.4-IV2",
                "--unstable-feature-versions-enable",
            ],
            extra: vec![],
            fault: "Encountered fatal fault: Tried to apply FeatureLevelRecord \
             FeatureLevelRecord(name='metadata.version', featureLevel=33), \
             but this controller only supports versions 7-30",
        },
        FatalFaultCase {
            what: "a failed controller activation",
            flags: &[],
            extra: vec![orders(1), orders(2)],
            fault: "Encountered fatal fault: exception while completing controller activation: \
             metadata: topic 'orders' already exists",
        },
    ];
    for FatalFaultCase {
        what,
        flags,
        extra,
        fault,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log_dir = tmp.path().join("data");
        let controller_port = free_loopback_port();
        format_standalone(&log_dir, controller_port, flags, extra);
        // The logs go to a file, not to a pipe that nothing reads while the
        // test polls for the exit.
        let stdout_path = tmp.path().join("stdout.log");

        let mut child = Command::new(env!("CARGO_BIN_EXE_krabka-broker"))
            // The broker binds and registers its own client port, so there is
            // no probe for it to race another process over.
            .arg("--listen-addr=127.0.0.1:0")
            .arg(format!(
                "--controller-listen-addr=127.0.0.1:{controller_port}"
            ))
            .arg(format!("--log-dir={}", log_dir.display()))
            .arg("--broker-id=1")
            .arg("--metrics-listen-addr=none")
            .arg("--health-listen-addr=none")
            .stdout(Stdio::from(
                std::fs::File::create(&stdout_path).expect("create the stdout log"),
            ))
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn krabka-broker");

        // The broker stops on its own, within seconds: the fault ends its
        // start at once. A hang is the failure, so a deadline turns it into
        // one and kills the process it leaves behind. It is well under the
        // two minutes that a start waits for its first unfence or for
        // `metadata.version`, so a start that ignored the fault fails it.
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll krabka-broker") {
                break Some(status);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                break None;
            }
            // intentional: waiting on a spawned krabka-broker subprocess to
            // exit; no in-process handle exists to await instead.
            std::thread::sleep(Duration::from_millis(100));
        };
        let output = child
            .wait_with_output()
            .expect("collect krabka-broker output");
        let stderr = String::from_utf8_lossy(&output.stderr);

        let status = status
            .unwrap_or_else(|| panic!("{what}: krabka-broker kept running; stderr:\n{stderr}"));
        // Kafka's `Exit.halt(1)`.
        check!(status.code() == Some(1), "{what}: exit status {status}");
        let stdout = std::fs::read_to_string(&stdout_path).expect("read the stdout log");
        check!(
            stdout.contains("halting: the metadata controller met a fatal fault"),
            "{what}: stdout:\n{stdout}"
        );
        check!(stderr.contains(fault), "{what}: stderr:\n{stderr}");
    }
}
