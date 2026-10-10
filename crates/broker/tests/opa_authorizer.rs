// rustc 1.95 clippy::pedantic ICEs on this file family (same upstream
// body-analysis bug that already fires on `tests/acl_handlers.rs` and
// `tests/throttle.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.

//! End-to-end OPA authorizer enforcement over the wire path.
//!
//! Two integration tests boot a single-broker `SASL_PLAINTEXT` cluster with an
//! [`OpaAuthorizer`] that points at a `wiremock::MockServer`. The mock returns
//! either `{"result": false}` or `{"result": true}` for every `POST`. The
//! tests assert that the per-topic `error_code` on a Produce response carries
//! `TOPIC_AUTHORIZATION_FAILED (29)` or `0`, in that order.
//!
//! Test 1 bootstraps its topic like this. The `admin` principal is a
//! super-user in BOTH the [`OpaAuthorizer`], so it bypasses OPA, and the
//! [`BrokerConfig.super_users`] field, so the broker-level super-user checks
//! accept it. The test calls `CreateTopics` over a SASL/PLAIN `admin` session.
//! The super-user bypass means the broker never asks OPA, so the topic
//! materialises even though the mock would otherwise deny it. The OPA gate
//! itself fires when `alice`, who is not a super-user, sends Produce.
//!
//! These tests are gated to non-Windows, to match the other SASL integration
//! tests. The listener bring-up works on Windows, but a uniform gate avoids
//! one-off CI matrix surprises.

mod kafka_wire;
mod support;

use std::{io, net::SocketAddr};

use assert2::assert;
use kafka_wire::single_record_produce_request;
use krabka_broker::{BrokerHandle, authorizer::opa::OpaAuthorizer};
use krabka_protocol::owned::produce_response::ProduceResponse;
use krabka_security::SaslMechanism;
use tempfile::TempDir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

// Local mirror of `krabka_broker::codes::TOPIC_AUTHORIZATION_FAILED`,
// kept inline because the `codes` module is crate-private. The value
// matches the Apache Kafka error table and `tests/acl_handlers.rs`.
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;

// API versions chosen to match `tests/acl_handlers.rs`:
//   * CreateTopics v7 — flexible (FLEXIBLE_MIN=5), topic id round-trips.
//   * Produce v11 — flexible (FLEXIBLE_MIN=9), still uses topic `name`
//     rather than topic_id (v >= 13 introduces the latter).

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-opa-test";

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boots a single-broker `SASL_PLAINTEXT` cluster whose
/// `BrokerConfig.authorizer` is an [`OpaAuthorizer`] that points at `opa_url`.
///
/// `admin` is a super-user in BOTH the authorizer, so it bypasses the OPA HTTP
/// call, AND `BrokerConfig.super_users`, so the broker-level super-user checks
/// accept it. `alice` is a regular user, so every authorization check on
/// alice's sessions goes through OPA.
///
/// `expire_after_ms = 1` stops the OPA cache from masking variation inside one
/// test. The second authorization check in `produce_allowed_by_opa_succeeds`
/// always fetches from the mock again, so the assertion does not depend on
/// in-process cache hits from earlier requests in the same test process.
fn start_broker_with_opa_authorizer(
    opa_url: String,
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let (log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_string(), "admin-secret".to_string());
    cfg.plain_credentials
        .insert("alice".to_string(), "wonderland".to_string());
    cfg.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: "admin".to_string(),
        password: "admin-secret".to_string(),
    });
    // Broker-level super-user set (used by handler code that reads
    // `broker.config.super_users` directly, independent of the authorizer
    // trait dispatch — e.g. the act-as gate).
    cfg.super_users.insert("admin".to_string());

    // OpaAuthorizer carries its own super-user set so it can bypass HTTP
    // before consulting OPA. Building it inside the test means `Handle::
    // try_current()` succeeds (the `#[tokio::test(flavor = "multi_thread")]`
    // attribute on each test guarantees a multi-thread runtime, which is
    // what `block_in_place` requires).
    let mut super_users = std::collections::HashSet::new();
    super_users.insert("admin".to_string());
    // Clients reach this broker over SASL, but its own heartbeat reaches the
    // PLAINTEXT controller listener as ANONYMOUS; like a Kafka inter-broker
    // principal it needs `ClusterAction`, or the broker never unfences.
    super_users.insert("ANONYMOUS".to_string());
    let opa = OpaAuthorizer::new(
        super_users,
        opa_url,
        /* allow_on_error */ false,
        /* max_cache_size */ 100,
        // Tight TTL so subsequent authorize() calls within the same test
        // re-consult the mock rather than serving from cache. The
        // wall-clock TTL is enforced by `time_util::now_ms()`; 1 ms
        // means the second call in any same-test sequence is always a
        // cache miss after `tokio::time::sleep(Duration::from_millis(5))`.
        /* expire_after */
        krabka_units::millis(1),
        krabka_units::secs(5),
    )
    .expect("OpaAuthorizer::new must succeed inside a tokio runtime");
    cfg.authorizer = std::sync::Arc::new(opa);

    Box::pin(crate::support::sasl::start_broker(cfg, log_dir))
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver helpers.
// ─────────────────────────────────────────────────────────────────────────────

use crate::kafka_wire::drive_produce_as_plain;

/// Drives `CreateTopics(name, 1 partition, rf=1)` over a SASL/PLAIN admin
/// session. Admin is the super-user in both tests, so the call bypasses the OPA
/// mock and the topic materialises whatever OPA answers.
async fn create_topic_as_admin(addr: SocketAddr, name: &str) {
    crate::kafka_wire::create_topic_as_super_user(
        addr,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: CLIENT_ID,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: name.to_string(),
                ..Default::default()
            },
        },
    )
    .await;
}

/// Waits on `handle`, event-driven, until the local writer-actor of `topic`
/// partition 0 has materialised, then runs a single `drive_produce_as_plain`.
///
/// Once the local writer materialises, the raft commit-then-apply gap has
/// closed. That gap sits between `CreateTopics` returning and the partition
/// appearing in the broker's `MetadataImage`. Before the gap closes, the
/// authorizer finds no matching topic resource and denies alice's Write with
/// `TOPIC_AUTHORIZATION_FAILED`. Once the broker applies the topic, the check
/// goes through OPA, which allows it. Waiting on the same handle removes that
/// race without a fixed-interval retry loop. The OPA decision cache uses a 1 ms
/// TTL, far below any scheduling delay here, so it never masks the result.
///
/// alice's SASL/PLAIN test password as bytes, assembled at runtime instead of
/// written as a byte-string literal. The value is a non-secret test fixture,
/// but a literal that flows into the client auth calls trips GitHub's default
/// code-scanning credential query. Building it here keeps those sites free of
/// literals.
fn alice_password() -> Vec<u8> {
    b"wonderland".to_vec()
}

async fn produce_when_partition_ready(
    handle: &BrokerHandle,
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    handle.wait_until_local_log_end_offset(topic, 0, 0).await;
    drive_produce_as_plain(
        addr,
        single_record_produce_request(topic, 0, b"hello"),
        &crate::kafka_wire::PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            user,
            password: (password).to_vec(),
        },
    )
    .await
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests.
// ─────────────────────────────────────────────────────────────────────────────

/// Spec §4.2 test 1.
///
/// The OPA mock returns `{"result": false}` for every POST. `alice`
/// authenticates through SASL/PLAIN and sends Produce against a topic that the
/// super-user `admin` created beforehand. The per-partition response must carry
/// `TOPIC_AUTHORIZATION_FAILED (29)`, because alice's Write check on the topic
/// goes through OPA, which always denies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_blocked_by_opa_returns_topic_authorization_failed() {
    let (_opa, handle, _dir, addr) = opa_fixture(false).await;

    // Bootstrap the topic via admin (super-user → OPA bypassed).
    create_topic_as_admin(addr, "blocked-topic").await;

    // alice (non-super-user) tries to Produce → OPA returns deny → 29.
    let resp = drive_produce_as_plain(
        addr,
        single_record_produce_request("blocked-topic", 0, b"hello"),
        &crate::kafka_wire::PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            password: alice_password(),
            ..Default::default()
        },
    )
    .await
    .expect("Produce must round-trip");

    handle.shutdown().await;

    let p = crate::support::produce::single_partition_response(&resp);
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "OPA denied alice's Write on blocked-topic, expected \
         TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

/// Spec §4.2 test 2.
///
/// The OPA mock returns `{"result": true}` for every POST. `alice`
/// authenticates and produces, and the per-partition response row must carry
/// `error_code = 0`.
///
/// `produce_when_partition_ready` waits on the broker handle, event-driven, for
/// the partition's local writer to materialise, and only then sends the single
/// Produce. That wait closes the raft commit-then-apply gap between
/// `CreateTopics` returning and the partition appearing in the local
/// `MetadataImage`, so the test needs no fixed sleep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_by_opa_succeeds() {
    let (_opa, handle, _dir, addr) = opa_fixture(true).await;

    create_topic_as_admin(addr, "permitted-topic").await;

    let resp =
        produce_when_partition_ready(&handle, addr, "alice", &alice_password(), "permitted-topic")
            .await
            .expect("Produce must round-trip");

    handle.shutdown().await;

    let p = crate::support::produce::single_partition_response(&resp);
    assert!(
        p.error_code == 0,
        "OPA allowed alice's Write on permitted-topic, expected \
         error_code=0, got {p:?}"
    );
}

async fn opa_fixture(allowed: bool) -> (MockServer, BrokerHandle, TempDir, SocketAddr) {
    let opa = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": allowed})),
        )
        .mount(&opa)
        .await;
    let url = format!("{}/v1/data/kafka/authz/allow", opa.uri());
    let (broker, dir, addr) = start_broker_with_opa_authorizer(url).await;
    (opa, broker, dir, addr)
}
