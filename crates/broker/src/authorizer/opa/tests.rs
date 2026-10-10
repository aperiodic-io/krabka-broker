//! Behavior tests for [`OpaAuthorizer`] against a `wiremock` OPA endpoint:
//! the super-user bypass, the decision cache and its TTL, and the fail-open
//! and fail-closed branches that an OPA outage takes.
//!
//! The tests for the Kafka-to-OPA vocabulary mapping live beside that mapping
//! in [`super::wire`].

use std::{collections::HashSet, net::SocketAddr, time::Duration};

use assert2::assert;
use krabka_authz::{AuthorizationRequest, AuthorizationResult, Authorizer};
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_security::Principal;
use krabka_units::{millis, minutes, secs};
use qubit_clock::ManualMonotonicClock;
use uuid::Uuid;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::*;

fn test_principal(name: &str) -> Principal {
    crate::test_support::sasl_principal(name)
}

fn img() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

fn host() -> SocketAddr {
    "1.2.3.4:9092".parse().unwrap()
}

fn req<'a>(p: &'a Principal, h: &'a SocketAddr, topic: &'a str) -> AuthorizationRequest<'a> {
    AuthorizationRequest {
        principal: p,
        host: h,
        resource_type: ResourceType::Topic,
        resource_name: topic,
        operation: AclOperation::Read,
    }
}

fn opa_url(server: &MockServer) -> String {
    format!("{}/v1/data/kafka/authz/allow", server.uri())
}

fn supers(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

fn authorizer(super_users: HashSet<String>, url: String, allow_on_error: bool) -> OpaAuthorizer {
    OpaAuthorizer::new(super_users, url, allow_on_error, 100, minutes(1), secs(5)).unwrap()
}

macro_rules! request_fixture {
    ($image:ident, $principal:ident, $host:ident, $name:literal) => {
        let $image = img();
        let $principal = test_principal($name);
        let $host = host();
    };
}

fn check_alice_authorization(auth: &OpaAuthorizer, expected: AuthorizationResult) {
    request_fixture!(image, p, h, "alice");
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == expected);
}

async fn failing_registry() -> MockServer {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock)
        .await;
    mock
}

#[tokio::test(flavor = "multi_thread")]
async fn super_user_bypasses_opa_call() {
    let mock = MockServer::start().await;
    // expect(0) verifies on drop that no HTTP call landed.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})),
        )
        .expect(0)
        .mount(&mock)
        .await;

    let auth = authorizer(supers(&["admin"]), opa_url(&mock), false);
    request_fixture!(image, p, h, "admin");
    assert!(auth.authorize(&image, &req(&p, &h, "anything")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_returns_cached_decision_without_http_call() {
    check_cached_decision("t", true, AuthorizationResult::Allow).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_preserves_a_deny_decision() {
    check_cached_decision("t", false, AuthorizationResult::Deny).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_miss_calls_opa_and_caches_result() {
    check_cached_decision("fresh-topic", true, AuthorizationResult::Allow).await;
}

async fn check_cached_decision(topic: &str, allow: bool, expected: AuthorizationResult) {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": allow})),
        )
        .expect(1)
        .mount(&mock)
        .await;
    let auth = authorizer(HashSet::new(), opa_url(&mock), false);
    request_fixture!(image, principal, host, "alice");
    // The second call must preserve the decision and use the cache: expect(1)
    // verifies that only the initial authorization reached HTTP.
    for _ in 0..2 {
        assert!(auth.authorize(&image, &req(&principal, &host, topic)) == expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_entry_expires_after_ttl() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .expect(2) // first call + post-expiry call.
        .mount(&mock)
        .await;

    // 10ms decision-cache TTL, driven by an injected manual monotonic clock so
    // the entry expires on a controlled timeline — deterministic, no sleep.
    let timeline = ManualMonotonicClock::new_shared();
    let auth = OpaAuthorizer::with_clock(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        millis(10),
        secs(5),
        timeline.clone(),
    )
    .unwrap();
    request_fixture!(image, p, h, "alice");
    // Cache miss -> HTTP call #1; caches the decision with expires_at = now+10ms.
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    // The exact deadline is stale: freshness is a strict comparison.
    timeline
        .advance(Duration::from_millis(10))
        .expect("manual time moves forward");
    // Cache entry expired -> HTTP call #2 (verified by the mock's expect(2)).
    assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonpositive_cache_ttl_is_rejected() {
    for ttl in [millis(0), krabka_units::Time::from_millis(-1)] {
        let result = OpaAuthorizer::with_clock(
            HashSet::new(),
            "http://opa.invalid/v1/data/kafka/authz/allow".to_string(),
            false,
            1,
            ttl,
            secs(1),
            ManualMonotonicClock::new_shared(),
        );
        assert!(matches!(result, Err(OpaConfigError::InvalidCacheTtl)));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_error_with_allow_on_error_true_returns_allow() {
    let mock = failing_registry().await;

    // allow_on_error=true → 500 maps to Allow.
    let auth = authorizer(HashSet::new(), opa_url(&mock), true);
    check_alice_authorization(&auth, AuthorizationResult::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn http_error_with_allow_on_error_false_returns_deny() {
    let mock = failing_registry().await;

    let auth = authorizer(HashSet::new(), opa_url(&mock), false);
    check_alice_authorization(&auth, AuthorizationResult::Deny);
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_http_timeout_fails_closed() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(250))
                .set_body_json(serde_json::json!({"result": true})),
        )
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(
        HashSet::new(),
        opa_url(&mock),
        false,
        100,
        minutes(1),
        millis(25),
    )
    .unwrap();
    check_alice_authorization(&auth, AuthorizationResult::Deny);
}

#[tokio::test(flavor = "multi_thread")]
async fn json_response_parse_error_returns_per_allow_on_error_config() {
    // 200 OK but body isn't valid OPA JSON. The shape parses as
    // serde-json but lacks the `result` field — should fall through
    // to error_decision().
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json-at-all"))
        .mount(&mock)
        .await;

    let p = test_principal("alice");
    let h = host();
    let image = img();

    let auth_open = authorizer(HashSet::new(), opa_url(&mock), true);
    assert!(auth_open.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);

    let auth_closed = authorizer(HashSet::new(), opa_url(&mock), false);
    assert!(auth_closed.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
}

/// A caller-side cache built on [`Authorizer::decision_ttl`] must not outlive
/// OPA's own decision-cache TTL, since a policy change there never touches
/// the Kafka metadata log a caller might otherwise key its own cache on.
#[tokio::test(flavor = "multi_thread")]
async fn decision_ttl_reports_the_configured_cache_expiry() {
    let auth = authorizer(
        HashSet::new(),
        "http://localhost/v1/data/kafka/authz/allow".into(),
        false,
    );
    assert!(auth.decision_ttl() == Some(Duration::from_secs(60)));
}
