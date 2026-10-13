//! Fixtures the validator's unit tests share: a wiremock registry that binds
//! one schema id, the Confluent framing a producer would apply, and a
//! [`SchemaValidator`] pointed at that registry.
//!
//! They live in one module because the cache tests and the record-check tests
//! need the same registry and the same framed record, and a second copy of
//! either would be free to drift from the first.

use {
    super::{RejectReason, SchemaValidator, UNAVAILABLE_TTL_MS},
    crate::{metrics::BrokerMetrics, schema_validation::ValidationMode},
    krabka_units::{minutes, secs},
    wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    },
};

pub(super) const KNOWN_ID: u32 = 42;
const AVRO: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;

/// Frame a body the way a Confluent serializer does.
pub(super) fn framed(id: u32, body: &[u8]) -> Vec<u8> {
    let mut out = vec![0x00];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A registry that binds `KNOWN_ID` to `orders-value` and resolves it to
/// [`AVRO`]. `expect` bounds how many times each endpoint may be called,
/// which is how the cache tests assert a hit.
pub(super) async fn registry(versions_calls: u64) -> MockServer {
    let server = MockServer::start().await;
    schema_versions(&server, versions_calls).await;
    json_endpoint(
        &server,
        &format!("/schemas/ids/{KNOWN_ID}"),
        serde_json::json!({"schema": AVRO}),
        None,
    )
    .await;
    server
}

pub(super) async fn schema_versions(server: &MockServer, calls: u64) {
    json_endpoint(
        server,
        &format!("/schemas/ids/{KNOWN_ID}/versions"),
        serde_json::json!([
            {"subject": "orders-value", "version": 1}
        ]),
        Some(calls),
    )
    .await;
}

pub(super) async fn json_endpoint(
    server: &MockServer,
    route: &str,
    body: serde_json::Value,
    calls: Option<u64>,
) {
    response_endpoint(
        server,
        route,
        ResponseTemplate::new(200).set_body_json(body),
        calls,
    )
    .await;
}

/// Mount a registry response with the caller's exact body, status and call bound.
pub(super) async fn response_endpoint(
    server: &MockServer,
    route: &str,
    response: ResponseTemplate,
    calls: Option<u64>,
) {
    let mock = Mock::given(method("GET"))
        .and(path(route))
        .respond_with(response);
    let mock = match calls {
        Some(calls) => mock.expect(calls),
        None => mock,
    };
    mock.mount(server).await;
}

pub(super) async fn referenced_avro(schema: &str) -> MockServer {
    let server = MockServer::start().await;
    schema_versions(&server, 1).await;
    json_endpoint(
        &server,
        &format!("/schemas/ids/{KNOWN_ID}"),
        serde_json::json!({
            "schema": schema,
            "references": [{"name":"Base","subject":"order-base","version":1}]
        }),
        Some(1),
    )
    .await;
    json_endpoint(
        &server,
        "/subjects/order-base/versions/1",
        serde_json::json!({
            "subject": "order-base",
            "version": 1,
            "id": 7,
            "schema": r#"{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}"#
        }),
        Some(1),
    )
    .await;
    server
}

pub(super) fn configured_validator(
    url: String,
    fail_open: bool,
) -> Result<SchemaValidator, super::SchemaValidatorError> {
    SchemaValidator::new(url, fail_open, 100, minutes(1), secs(5))
}

pub(super) fn validator(url: String) -> SchemaValidator {
    configured_validator(url, false).expect("validator")
}

pub(super) async fn status_registry(status: u16) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .mount(&server)
        .await;
    server
}

/// Metrics for a check whose counters the test does not assert on. The
/// cache-accounting test binds one instance instead, so its hits and
/// misses accumulate across calls.
pub(super) fn no_metrics() -> BrokerMetrics { BrokerMetrics::new() }

/// Validate the orders value fixture when no counters need to be inspected.
pub(super) async fn value_check(
    validator: &SchemaValidator,
    mode: ValidationMode,
    field: &[u8],
) -> Result<(), RejectReason> {
    validator
        .check("orders", super::Role::Value, mode, field, &no_metrics())
        .await
}

/// [`UNAVAILABLE_TTL_MS`] as a `u64`, so a test advances the clock by the
/// real constant rather than a copy of it that could drift.
pub(super) fn unavailable_ttl_ms() -> u64 {
    u64::try_from(UNAVAILABLE_TTL_MS).expect("the unavailable TTL is positive")
}
