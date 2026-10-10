// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as `tests/mtls.rs` etc).

//! KIP-511: `ApiVersions` v3+ client-information validation and the
//! `client_software_versions_total` Prometheus counter.
//!
//! These tests boot a broker on a plaintext loopback listener, with the
//! Prometheus exporter bound on `127.0.0.1:0`. Each test drives a raw
//! `ApiVersions` request over TCP instead of going through the `Client`. That
//! lets the test pin the version and the exact `client_software_name` and
//! `client_software_version` bytes the broker sees.

mod support;

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::Broker;
use krabka_protocol::{Encode, owned::api_versions_response::ApiVersionsResponse};
use tokio::net::TcpStream;

use crate::support::discovery::api_versions_request_for;

const INVALID_REQUEST: i16 = 42;

/// Builds a broker with the metrics endpoint enabled. It returns the Kafka
/// listener address, the metrics endpoint address, and the `BrokerHandle`, so
/// that the test can shut down cleanly.
async fn boot() -> (
    SocketAddr,
    SocketAddr,
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
) {
    let tempdir = tempfile::tempdir().unwrap();
    let cfg = crate::support::client::metrics_config(tempdir.path().to_path_buf());

    let handle = Broker::start(cfg).await.expect("broker start");
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");
    (kafka_addr, metrics_addr, handle, tempdir)
}

/// Sends one `ApiVersions` request at the requested version with the given
/// client-info fields, and returns the decoded response.
async fn send_api_versions(
    addr: SocketAddr,
    version: i16,
    software_name: &str,
    software_version: &str,
) -> io::Result<ApiVersionsResponse> {
    let req = api_versions_request_for(software_name.to_string(), software_version.to_string());
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;

    // ApiVersions request header is v2 (flexible) on v3+; v1 (plain) on
    // v0-2. Either way the response header is v0 — ApiVersions intentionally
    // keeps its response header at v0 across versions so v0 clients can
    // parse the error code on negotiated downgrade.
    let flexible = version >= 3;

    let frame = crate::support::wire::request_frame(crate::support::wire::WireFrameSetup {
        version: krabka_ids::ApiVersion(version),
        correlation: crate::support::wire::CorrelationId(99),
        header: crate::support::wire::HeaderEncoding::from_wire(flexible),
        client_id: "krabka-kip-511-test",
        body: &body,
        capacity: Some(crate::support::wire::request_body_capacity(&body)),
        ..Default::default()
    });

    let mut stream = TcpStream::connect(addr).await?;
    crate::support::wire::write_frame(&mut stream, &frame, None).await?;

    let resp = crate::support::wire::read_frame(&mut stream).await?;
    // ApiVersions keeps a non-flexible response header, including on flexible versions.
    crate::support::wire::decode_response_frame(&resp, version, false, "ApiVersionsResponse decode")
}

async fn scrape(addr: SocketAddr) -> String {
    crate::support::client::scrape_metrics(addr).await
}

// ── KIP-511 validation paths ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_valid_client_info_accepted() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "krabka-client-core", "0.1.1")
        .await
        .expect("ApiVersions");
    assert!(resp.error_code == 0, "valid v3 must succeed: {resp:?}");
    assert!(
        !resp.api_keys.is_empty(),
        "valid v3 must return the API list",
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_empty_software_name_rejected_with_invalid_request() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "", "1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "empty name must be rejected: {resp:?}"
    );
    assert!(
        resp.api_keys.is_empty(),
        "error path must not advertise APIs",
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_empty_software_version_rejected_with_invalid_request() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "krabka", "")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "empty version must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_invalid_char_in_name_rejected() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "has space", "1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "spaces must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_leading_dash_in_version_rejected() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "krabka", "-1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "leading dash must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_does_not_validate_client_info() {
    // v0-2 don't carry the fields. The codegen leaves them as default
    // empty strings on the wire — KIP-511 says don't validate. Apache
    // Kafka brokers happily accept v0/v1/v2 calls from old clients that
    // don't know about ClientSoftwareName at all.
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 0, "", "")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == 0,
        "v0 ApiVersions must not validate KIP-511 fields: {resp:?}"
    );
    assert!(!resp.api_keys.is_empty());

    handle.shutdown().await;
}

// ── Prometheus metric ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_v3_handshake_bumps_client_software_versions_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // Drive two distinct (name, version) tuples + one repeat so the
    // expected sample emits with three series, one of them at count 2.
    for _ in 0..2 {
        send_api_versions(kafka_addr, 3, "krabka-it", "1.0.0")
            .await
            .expect("ApiVersions");
    }
    send_api_versions(kafka_addr, 3, "krabka-it", "1.0.1")
        .await
        .expect("ApiVersions");
    send_api_versions(kafka_addr, 3, "another-lib", "9.9.9")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;

    // Three series, one count, plus the family HELP/TYPE lines.
    assert!(
        body.contains("# TYPE krabka_broker_client_software_versions counter"),
        "TYPE line missing in:\n{body}",
    );
    let needle_repeat = "krabka_broker_client_software_versions_total{software_name=\"krabka-it\",software_version=\"1.0.0\"} 2";
    let needle_new = "krabka_broker_client_software_versions_total{software_name=\"krabka-it\",software_version=\"1.0.1\"} 1";
    let needle_other = "krabka_broker_client_software_versions_total{software_name=\"another-lib\",software_version=\"9.9.9\"} 1";
    for needle in [needle_repeat, needle_new, needle_other] {
        assert!(
            body.contains(needle),
            "expected sample {needle:?} not found in:\n{body}",
        );
    }

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_v3_handshake_does_not_bump_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // First send a valid one so the family has at least one series and
    // the metric appears in the scrape — otherwise an empty Family of
    // Counters renders no samples at all.
    send_api_versions(kafka_addr, 3, "valid-client", "1.0.0")
        .await
        .expect("ApiVersions");
    // Now an invalid one. The counter must not gain a row labelled with
    // the rejected name/version.
    send_api_versions(kafka_addr, 3, "bad client", "1.0.0")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;
    assert!(
        !body.contains("software_name=\"bad client\""),
        "rejected handshake must not be recorded:\n{body}",
    );
    // Spot-check the valid one is recorded.
    assert!(
        body.contains("software_name=\"valid-client\""),
        "valid handshake must be recorded:\n{body}",
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_handshake_does_not_bump_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // v0 ApiVersions has no client-info fields, so the counter (which
    // would label series with empty strings) must not increment.
    send_api_versions(kafka_addr, 0, "", "")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;
    // The metric *family* may still appear in the scrape registration
    // (HELP/TYPE lines emit even with no series), so just check no
    // sample row with software_name="" was emitted.
    assert!(
        !body.contains("krabka_broker_client_software_versions_total{software_name=\"\""),
        "v0 handshake must not be recorded:\n{body}",
    );

    handle.shutdown().await;
}
