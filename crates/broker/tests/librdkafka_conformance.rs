//! The librdkafka clients against krabka.
//!
//! Two rows, because one client cannot hold both ends. `kcat` has had no
//! release since 1.7.1 and its own build script pins librdkafka 1.8.2, so
//! every published image of it -- Confluent's `cp-kcat:8.2.3`, a 2026 build,
//! included -- still reports `Version 1.7.1 (... librdkafka 1.8.2)`, exactly
//! what `edenhill/kcat:1.7.1` reports. The librdkafka 2.x row therefore runs
//! an image built here rather than pulled: `//bazel/images:kcat_librdkafka_2x`
//! assembles the same `kcat` 1.7.1 CLI against Wolfi's librdkafka 2.15.0, so
//! its banner still says `Version 1.7.1` and the librdkafka it names is what
//! the row is about. The table stays a table so that adding a client is a row.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

use assert2::assert;
use krabka_client_admin::AdminClient;

use crate::jvm_acceptance::{broker0_advertised, start_host_broker, start_host_broker_with};

/// One librdkafka client this suite runs the whole round trip against.
struct ClientCase {
    /// The image, pinned in `MODULE.bazel` and named in the
    /// `librdkafka_conformance` entry of the `docker` map in `BUILD.bazel`.
    image: &'static str,
    /// The binary inside it.
    program: &'static str,
    /// How the KIP matrix names this client's library. The generator reads
    /// this field out of the table, so it is the string the matrix shows.
    library: &'static str,
    /// The substring of the client's own `-V` banner that proves it: the
    /// banner is what says which librdkafka the binary is linked against.
    /// Both images run `kcat` 1.7.1, so only this part of the banner tells
    /// them apart, and a silently downgraded image fails the suite here.
    version_marker: &'static str,
}

/// Every librdkafka client this suite drives.
///
/// The 1.x client predates KIP-848 (`group.protocol=consumer`), KIP-714
/// (metrics push) and topic-id Fetch, so it holds the row that a client from
/// before those KIPs still completes the classic round trip against krabka:
/// version negotiation, metadata, produce, and a classic group. The 2.x client
/// is the one the whole confluent-kafka-{python,go,dotnet} family sits on, and
/// [`next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x`] drives it
/// through the paths the older one cannot reach.
const CLIENTS: [ClientCase; 2] = [
    ClientCase {
        image: "mirror.gcr.io/edenhill/kcat:1.7.1",
        program: "kcat",
        library: "librdkafka 1.8.2",
        version_marker: "librdkafka 1.8.2",
    },
    ClientCase {
        image: "docker.io/krabka-io/kcat:librdkafka-2.15.0",
        program: "kcat",
        library: "librdkafka 2.15.0",
        version_marker: "librdkafka 2.15.0",
    },
];

/// The librdkafka 2.x row of [`CLIENTS`].
fn librdkafka_2x() -> &'static ClientCase {
    &CLIENTS[1]
}

fn check_client_version(image: &str, program: &str, library: &str, marker: &str) {
    let version = run_client(image, program, &["-V"], None);
    let version = format!(
        "{}{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr)
    );
    assert!(
        version.contains(marker),
        "{program} is not linked to {library}: {version}"
    );
}

fn check_consumed_payload(output: &Output, expected: &str, program: &str) {
    assert!(
        String::from_utf8_lossy(&output.stdout) == expected,
        "{program} round-trip mismatch: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn run_client(image: &str, program: &str, args: &[&str], input: Option<&str>) -> Output {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            program,
            image,
        ])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn librdkafka client");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("client stdin")
            .write_all(input.as_bytes())
            .expect("write client stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait for client");
    assert!(
        out.status.success(),
        "{program} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// The highest version at which `trace` shows `<request>Request` being sent.
///
/// librdkafka's `protocol` debug writes one `Sent <name>Request (v<version>,
/// ...` line per request, so the negotiated version of a call is observable in
/// the trace and nowhere else in a client run.
fn max_sent_version(trace: &str, request: &str) -> Option<u32> {
    let needle = format!("Sent {request}Request (v");
    trace
        .match_indices(&needle)
        .filter_map(|(at, _)| {
            let rest = &trace[at + needle.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .max()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn round_trip_group_join_and_api_versions_with_kcat() {
    let (broker, _dir) = start_host_broker().await;
    let bootstrap = broker0_advertised();
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");

    for (
        index,
        ClientCase {
            image,
            program,
            library,
            version_marker,
        },
    ) in CLIENTS.iter().enumerate()
    {
        check_client_version(image, program, library, version_marker);

        let topic = format!("librdkafka-conformance-{index}");
        admin
            .create_topics(
                &[crate::support::admin::topic_spec(topic.clone(), 1, 1)],
                krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
            )
            .await
            .expect("create conformance topic");

        let metadata = run_client(
            image,
            program,
            &[
                "-L",
                "-b",
                bootstrap,
                "-t",
                &topic,
                "-X",
                "api.version.request=true",
                "-d",
                "protocol",
            ],
            None,
        );
        let trace = String::from_utf8_lossy(&metadata.stderr);
        assert!(
            trace.contains("ApiVersionRequest") && trace.contains("ApiVersionResponse"),
            "{program} did not negotiate ApiVersions: {trace}"
        );

        let payload = format!("hello-from-{program}\n");
        run_client(
            image,
            program,
            &["-P", "-b", bootstrap, "-t", &topic, "-p", "0"],
            Some(&payload),
        );

        let group = format!("librdkafka-group-{index}");
        let consumed = run_client(
            image,
            program,
            &[
                "-G",
                &group,
                "-b",
                bootstrap,
                "-X",
                "auto.offset.reset=earliest",
                "-X",
                "enable.auto.commit=false",
                "-X",
                "enable.auto.offset.store=false",
                "-c",
                "1",
                "-f",
                "%s\\n",
                "-d",
                "cgrp,protocol",
                &topic,
            ],
            None,
        );
        check_consumed_payload(&consumed, &payload, program);
        let trace = String::from_utf8_lossy(&consumed.stderr);
        assert!(
            trace.contains("JoinGroup"),
            "{program} did not join a consumer group: {trace}"
        );
    }

    broker.shutdown().await;
}

/// What the 1.x client cannot reach, driven by the librdkafka 2.15.0 image.
///
/// Three things the older row says nothing about, all read out of the client's
/// own `protocol` debug trace, which is where a client's negotiated versions
/// and the requests it actually chose to send are observable:
///
///   - KIP-848: with `group.protocol=consumer` the client subscribes through
///     `ConsumerGroupHeartbeat` and sends neither `JoinGroup` nor `SyncGroup`.
///   - KIP-516: the consumer's `Fetch` is at v13 or above, the versions from
///     which the request carries topic ids in place of topic names, so a
///     krabka `Fetch` reply is being resolved by id for this client.
///   - KIP-98: an idempotent produce opens with `InitProducerId`.
///   - KIP-714: with a `client_metrics_enable` broker the client opens the
///     telemetry handshake with `GetTelemetrySubscriptions`. The broker
///     advertises keys 71 and 72 only behind a configured receiver, so this
///     also establishes that the client reads the advertised set: the same
///     client against the default broker of the test above opens no handshake.
///     `PushTelemetry` is not asserted: krabka answers a subscription-less
///     handshake with a 300s push interval, and no subscription can be
///     configured from here -- `AdminClient::incremental_alter_configs` is
///     topic-typed, and the `CLIENT_METRICS` resource type it would need is
///     what `tests/client_telemetry.rs` reaches through the raw protocol.
///
/// KIP-951 is left out: the leader hint rides a `Produce` or `Fetch` response
/// on a leader change, and kcat's trace names neither the hint nor a metadata
/// refresh caused by one, so a single-broker run has no observable to assert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x() {
    // A receiver has to be configured, or the broker withholds the KIP-714
    // keys and librdkafka opens no telemetry handshake at all.
    let (broker, _dir) = start_host_broker_with(|config| config.client_metrics_enable = true).await;
    let bootstrap = broker0_advertised();
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");

    let ClientCase {
        image,
        program,
        library,
        version_marker,
    } = librdkafka_2x();
    check_client_version(image, program, library, version_marker);

    let topic = "librdkafka-2x-next-gen";
    admin
        .create_topics(
            &[crate::support::admin::topic_spec(topic.to_string(), 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .expect("create conformance topic");

    let payload = "hello-from-librdkafka-2x\n";
    let produced = run_client(
        image,
        program,
        &[
            "-P",
            "-b",
            bootstrap,
            "-t",
            topic,
            "-p",
            "0",
            "-X",
            "enable.idempotence=true",
            "-X",
            "enable.metrics.push=true",
            "-d",
            "protocol",
        ],
        Some(payload),
    );
    let trace = String::from_utf8_lossy(&produced.stderr);
    assert!(
        trace.contains("Sent InitProducerIdRequest")
            && trace.contains("Received InitProducerIdResponse"),
        "idempotent {program} did not acquire a producer id: {trace}"
    );
    assert!(
        trace.contains("Sent GetTelemetrySubscriptionsRequest")
            && trace.contains("Received GetTelemetrySubscriptionsResponse"),
        "{program} opened no telemetry handshake against a client-metrics broker: {trace}"
    );

    let consumed = run_client(
        image,
        program,
        &[
            "-G",
            "librdkafka-2x-consumer-group",
            "-b",
            bootstrap,
            "-X",
            "group.protocol=consumer",
            "-X",
            "auto.offset.reset=earliest",
            "-X",
            "enable.auto.commit=false",
            "-c",
            "1",
            "-f",
            "%s\\n",
            "-d",
            "cgrp,protocol",
            topic,
        ],
        None,
    );
    check_consumed_payload(&consumed, payload, program);
    let trace = String::from_utf8_lossy(&consumed.stderr);
    assert!(
        trace.contains("Sent ConsumerGroupHeartbeatRequest")
            && trace.contains("Received ConsumerGroupHeartbeatResponse"),
        "{program} did not use the KIP-848 group protocol: {trace}"
    );
    assert!(
        !trace.contains("Sent JoinGroupRequest") && !trace.contains("Sent SyncGroupRequest"),
        "{program} fell back to the classic group protocol: {trace}"
    );
    let fetch_version = max_sent_version(&trace, "Fetch");
    assert!(
        fetch_version.is_some_and(|v| v >= 13),
        "{program} did not send a topic-id Fetch (v13 or above): {fetch_version:?} in {trace}"
    );

    broker.shutdown().await;
}
