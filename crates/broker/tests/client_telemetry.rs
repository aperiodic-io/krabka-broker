// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-714 client-metrics telemetry handshake, push, and error-path coverage.
//!
//! Krabka implements the full KIP-714 receiver. The broker:
//!   - Assigns a fresh `client_instance_id` when the caller sends nil, and
//!     answers with the caller's id otherwise.
//!   - Returns `accepted_compression_types = [4,3,1,2]`, that is ZSTD, LZ4,
//!     GZIP, and SNAPPY, plus `telemetry_max_bytes = 1_048_576` and
//!     `delta_temporality = true`.
//!   - With no subscriptions configured, leaves `requested_metrics` empty and
//!     sets `push_interval_ms = 300_000`.
//!   - With a matching subscription, sets `requested_metrics` to the matched
//!     prefix set and `push_interval_ms` to the smallest matched interval.
//!   - Answers a `PushTelemetry` from a reserved instance id with
//!     `error_code 42` (`INVALID_REQUEST`), and builds the instance of any
//!     other id it does not hold from the current subscriptions.
//!   - Answers a `PushTelemetry` with a stale `subscription_id` with
//!     `error_code 117` (`UNKNOWN_SUBSCRIPTION_ID`).
//!   - Answers a `PushTelemetry` with an unsupported `compression_type` with
//!     `error_code 76` (`UNSUPPORTED_COMPRESSION_TYPE`).
//!   - Answers a valid push with `error_code 0`.
//!
//! The tests that call `IncrementalAlterConfigs` to set up subscriptions need
//! a controller-backed single-node cluster, `start_n_node_with(1, ..)`, because
//! that RPC goes through Raft. The simple handshake tests use a single-broker
//! helper that also boots in Bootstrap mode and is its own controller. Both
//! turn `client_metrics_enable` on, because the broker advertises the two
//! KIP-714 RPCs only behind a configured receiver and the client negotiates
//! every request against what the broker advertised.

use {
    crate::support::{
        client::connect_owned, configs::client_metrics_subscription,
        discovery::api_versions_request_for,
    },
    assert2::{assert, check},
};
mod support;

use {
    krabka_protocol::{
        owned::{
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            incremental_alter_configs_response::IncrementalAlterConfigsResponse,
            push_telemetry_request::PushTelemetryRequest,
            push_telemetry_response::PushTelemetryResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    support::start_n_node_with,
};

// ── helpers ───────────────────────────────────────────────────────────────────

/// A single broker that advertises the KIP-714 RPCs, plus a client on it.
///
/// `client_metrics_enable` is off by default, the way a Kafka broker with no
/// `ClientTelemetry` metric reporter advertises no telemetry handshake, and
/// `krabka_client_core` negotiates every request against the table the broker
/// advertised. So a suite that drives the handshake configures a receiver
/// first; `api_versions_withholds_telemetry_apis_without_a_receiver` covers
/// the other setting.
async fn start_with_client_metrics() -> support::InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = krabka_broker::BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.client_metrics_enable = true;
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker start");
    let client = build_client(broker.listen_addr()).await;
    support::InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

type MetricsCluster = Vec<(
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerConfig,
    tempfile::TempDir,
)>;

/// A controller-backed cluster and the client negotiated against it.
struct TelemetryCluster {
    cluster: MetricsCluster,
    client: krabka_client_core::Client,
}

/// Keep the controller-backed cluster alive alongside its negotiated client.
async fn start_one_node_with_client_metrics() -> TelemetryCluster {
    let cluster = start_n_node_with(1, |_, cfg| cfg.client_metrics_enable = true)
        .await
        .expect("start_n_node_with");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;
    TelemetryCluster { cluster, client }
}

async fn build_client(addr: std::net::SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "client-telemetry-test",
        "client build",
    )
    .await
}

/// Configures a match-all `CLIENT_METRICS` subscription with
/// `IncrementalAlterConfigs`.
async fn configure_match_all_subscription(
    client: &krabka_client_core::Client,
    name: &str,
    interval_ms: &str,
) {
    let alter_req = client_metrics_subscription()
        .name(name)
        .interval(interval_ms)
        .call();

    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(alter_req)
        .await
        .expect("IncrementalAlterConfigs");

    assert!(
        alter_resp.responses.len() == 1,
        "expected one resource response, got {}",
        alter_resp.responses.len()
    );
    let r = &alter_resp.responses[0];
    assert!(
        r.error_code == 0,
        "IncrementalAlterConfigs CLIENT_METRICS must succeed; error_code={} message={:?}",
        r.error_code,
        r.error_message,
    );
}

/// Builds a minimal valid OTLP `MetricsData` payload. It is uncompressed, so
/// `compression_type=0`.
fn sample_otlp_metrics() -> bytes::Bytes {
    use {
        opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            metric::Data, number_data_point::Value,
        },
        prost::Message,
    };
    let dp = NumberDataPoint {
        value: Some(Value::AsInt(7)),
        ..Default::default()
    };
    let metric = Metric {
        name: "org.apache.kafka.consumer.fetch.size".into(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![dp],
        })),
        ..Default::default()
    };
    let md = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![metric],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    bytes::Bytes::from(md.encode_to_vec())
}

async fn advertised_apis(client: &krabka_client_core::Client) -> std::collections::HashSet<i16> {
    let resp = client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    resp.api_keys.iter().map(|k| k.api_key).collect()
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct SubscriptionId(i32);

#[derive(Clone, Copy)]
struct TelemetryCompressionCode(i8);

#[derive(Clone, Copy, Default)]
enum TelemetryCompression {
    #[default]
    Uncompressed,
    Invalid(TelemetryCompressionCode),
}

#[derive(krabka_macros::FieldDefaults)]
struct TelemetryPushSetup {
    #[default(WireUuid::ZERO)]
    client_instance_id: WireUuid,
    subscription_id: SubscriptionId,
    compression: TelemetryCompression,
    metrics: bytes::Bytes,
}

#[bon::builder]
fn telemetry_push_request(
    #[builder(default = WireUuid :: ZERO)] client_instance_id: WireUuid,
    #[builder(into, default)] subscription_id: SubscriptionId,
    #[builder(default)] compression: TelemetryCompression,
    #[builder(default)] metrics: bytes::Bytes,
) -> PushTelemetryRequest {
    let setup = TelemetryPushSetup {
        client_instance_id,
        subscription_id,
        compression,
        metrics,
    };
    let TelemetryPushSetup {
        client_instance_id,
        subscription_id,
        compression,
        metrics,
    } = setup;
    let compression_type = match compression {
        TelemetryCompression::Uncompressed => 0,
        TelemetryCompression::Invalid(code) => code.0,
    };
    PushTelemetryRequest {
        client_instance_id,
        subscription_id: subscription_id.0,
        terminating: false,
        compression_type,
        metrics,
        ..Default::default()
    }
}

// ── Part 1: fixed legacy tests ────────────────────────────────────────────────

#[tokio::test]
async fn api_versions_advertises_telemetry_apis() {
    let p = start_with_client_metrics().await;
    let advertised = advertised_apis(&p.client).await;
    assert!(
        advertised.contains(&71),
        "ApiVersions must advertise GetTelemetrySubscriptions (71), got {advertised:?}",
    );
    assert!(
        advertised.contains(&72),
        "ApiVersions must advertise PushTelemetry (72), got {advertised:?}",
    );

    p.broker.shutdown().await;
}

/// With no client-metrics receiver -- the default, and what a stock Kafka
/// broker answers when `metric.reporters` holds no `ClientTelemetry`
/// implementation -- neither KIP-714 key is advertised, so a modern Java or
/// librdkafka client opens no telemetry handshake.
#[tokio::test]
async fn api_versions_withholds_telemetry_apis_without_a_receiver() {
    let p = support::start().await;
    let advertised = advertised_apis(&p.client).await;
    check!(
        !advertised.contains(&71),
        "GetTelemetrySubscriptions (71) must stay unadvertised, got {advertised:?}",
    );
    check!(
        !advertised.contains(&72),
        "PushTelemetry (72) must stay unadvertised, got {advertised:?}",
    );

    p.broker.shutdown().await;
}

/// With no subscriptions configured, the broker assigns a fresh id, returns an
/// empty `requested_metrics`, and advertises the standard compression types
/// and limits.
#[tokio::test]
async fn get_telemetry_subscriptions_with_nil_id_returns_assigned_id_and_no_subscription() {
    let p = start_with_client_metrics().await;

    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    check!(resp.error_code == 0, "handler must succeed: {resp:?}");
    check!(
        resp.client_instance_id != WireUuid::ZERO,
        "broker must assign a fresh client_instance_id when caller sent nil"
    );
    // No subscriptions configured → empty requested_metrics (the "don't push" signal).
    check!(
        resp.requested_metrics.is_empty(),
        "no subscription configured → requested_metrics must be empty, got {:?}",
        resp.requested_metrics,
    );
    // Standard KIP-714 compression advertisement: ZSTD(4), LZ4(3), GZIP(1), SNAPPY(2).
    check!(
        resp.accepted_compression_types == vec![4i8, 3, 1, 2],
        "accepted_compression_types must be [4,3,1,2], got {:?}",
        resp.accepted_compression_types,
    );
    check!(
        resp.telemetry_max_bytes == 1_048_576,
        "telemetry_max_bytes must be 1 MiB, got {}",
        resp.telemetry_max_bytes,
    );
    check!(resp.delta_temporality, "delta_temporality must be true",);
    // Default interval when no subscription is matched: 300 000 ms (5 min).
    check!(
        resp.push_interval_ms == 300_000,
        "push_interval_ms must be 300_000 when no subscription configured, got {}",
        resp.push_interval_ms,
    );

    p.broker.shutdown().await;
}

/// Kafka's `ClientMetricsManager` answers with the id the client sent, and the
/// Java client rejects a zero id in a successful response
/// (`ClientTelemetryUtils.validateClientInstanceId`), whatever the schema
/// text says.
#[tokio::test]
async fn get_telemetry_subscriptions_with_set_id_echoes_it() {
    let p = start_with_client_metrics().await;

    let prior_id = WireUuid([0x11; 16]);
    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: prior_id,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert!(resp.error_code == 0);
    assert!(resp.client_instance_id == prior_id);

    p.broker.shutdown().await;
}

/// Kafka answers `INVALID_REQUEST` (42) only for the ids in `Uuid.RESERVED`.
/// For any other id it builds the instance from the current subscriptions, so
/// a push that carries some other subscription id gets
/// `UNKNOWN_SUBSCRIPTION_ID` (117) and the client fetches its subscription.
#[tokio::test]
async fn push_telemetry_unknown_instance_rejected() {
    let p = start_with_client_metrics().await;

    let mut one = [0; 16];
    one[15] = 1;
    for (instance, expected) in [
        (WireUuid::ZERO, 42),
        (WireUuid(one), 42),
        (WireUuid([0x22; 16]), 117),
    ] {
        let resp: PushTelemetryResponse = p
            .client
            .send(
                telemetry_push_request()
                    .client_instance_id(instance)
                    .metrics(bytes::Bytes::from_static(b"\x00\x01\x02"))
                    .call(),
            )
            .await
            .expect("PushTelemetry");

        assert!(
            resp == PushTelemetryResponse {
                error_code: expected,
                ..Default::default()
            },
            "instance {instance:?}"
        );
    }

    p.broker.shutdown().await;
}

// ── Part 2: e2e coverage (controller-backed) ─────────────────────────────────

/// Happy path: configure a subscription, run a `GetTelemetrySubscriptions`
/// handshake, then push a valid OTLP payload. All three must succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_happy_path_after_subscription() {
    let TelemetryCluster {
        cluster: _cluster,
        client,
    } = start_one_node_with_client_metrics().await;

    // ── Step 1: configure a match-all subscription ───────────────────────────
    configure_match_all_subscription(&client, "all", "100").await;

    // ── Step 2: GetTelemetrySubscriptions (nil id → assigned) ────────────────
    let get_resp: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    check!(
        get_resp.error_code == 0,
        "GetTelemetrySubscriptions must succeed: {get_resp:?}"
    );
    check!(
        get_resp.client_instance_id != WireUuid::ZERO,
        "broker must assign a fresh client_instance_id"
    );
    check!(
        get_resp.requested_metrics == vec!["*".to_string()],
        "match-all subscription must reflect as [\"*\"], got {:?}",
        get_resp.requested_metrics,
    );
    check!(
        get_resp.push_interval_ms == 100,
        "push_interval_ms must equal the configured interval (100), got {}",
        get_resp.push_interval_ms,
    );
    check!(
        get_resp.accepted_compression_types == vec![4i8, 3, 1, 2],
        "accepted_compression_types must be [4,3,1,2], got {:?}",
        get_resp.accepted_compression_types,
    );
    check!(get_resp.delta_temporality, "delta_temporality must be true");
    check!(
        get_resp.telemetry_max_bytes == 1_048_576,
        "telemetry_max_bytes must be 1 MiB, got {}",
        get_resp.telemetry_max_bytes,
    );

    let assigned_id = get_resp.client_instance_id;
    let subscription_id = get_resp.subscription_id;

    // ── Step 3: PushTelemetry with the assigned id + subscription id ──────────
    // compression_type = 0 is NONE (uncompressed); sample_otlp_metrics()
    // returns raw (uncompressed) proto bytes, so no codec mismatch.
    let push_resp: PushTelemetryResponse = client
        .send(
            telemetry_push_request()
                .client_instance_id(assigned_id)
                .subscription_id(subscription_id)
                .metrics(sample_otlp_metrics())
                .call(),
        )
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 0,
        "valid push must succeed, got error_code={}",
        push_resp.error_code,
    );
}

/// The broker must reject a push from a registered instance that carries a
/// stale `subscription_id`, with `UNKNOWN_SUBSCRIPTION_ID` (117).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_stale_subscription_id_rejected() {
    let TelemetryCluster {
        cluster: _cluster,
        client,
    } = start_one_node_with_client_metrics().await;

    let get_resp = enroll_telemetry(&client).await;
    let assigned_id = get_resp.client_instance_id;
    let real_sub_id = get_resp.subscription_id;
    // XOR with a constant to produce a definitely-wrong subscription id.
    let stale_sub_id = real_sub_id ^ 0x5555;

    let push_resp: PushTelemetryResponse = client
        .send(
            telemetry_push_request()
                .client_instance_id(assigned_id)
                .subscription_id(stale_sub_id)
                .metrics(sample_otlp_metrics())
                .call(),
        )
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 117,
        "stale subscription_id must yield UNKNOWN_SUBSCRIPTION_ID (117), got {}",
        push_resp.error_code,
    );
}

/// An unsupported `compression_type` must give
/// `UNSUPPORTED_COMPRESSION_TYPE` (76). The manager allows the first push
/// after a `GetTelemetrySubscriptions`, the "`first_after_get`" window, so the
/// request reaches the codec check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_unsupported_compression_rejected() {
    let TelemetryCluster {
        cluster: _cluster,
        client,
    } = start_one_node_with_client_metrics().await;

    let get_resp = enroll_telemetry(&client).await;
    let assigned_id = get_resp.client_instance_id;
    let subscription_id = get_resp.subscription_id;

    // Kafka's `CompressionType.forId` knows the ids 0 to 4 only.
    let push_resp: PushTelemetryResponse = client
        .send(
            telemetry_push_request()
                .client_instance_id(assigned_id)
                .subscription_id(subscription_id)
                .compression(TelemetryCompression::Invalid(TelemetryCompressionCode(5)))
                .metrics(sample_otlp_metrics())
                .call(),
        )
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 76,
        "unsupported compression_type must yield UNSUPPORTED_COMPRESSION_TYPE (76), got {}",
        push_resp.error_code,
    );
}

/// Sends `GetTelemetrySubscriptions` for `id` and returns the error code.
async fn get_error_code(client: &krabka_client_core::Client, id: WireUuid) -> i16 {
    let resp: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: id,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");
    resp.error_code
}

/// Kafka registers `ClientMetricsManager.connectionDisconnectListener` with the
/// socket server, so closing the connection that carried a client's telemetry
/// request drops its instance (#1246). A client that reconnects and asks again
/// inside its push interval gets an assignment, where the instance it left
/// behind would answer `THROTTLING_QUOTA_EXCEEDED` (89).
#[tokio::test]
async fn closing_the_connection_drops_the_client_instance() {
    const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
    let p = start_with_client_metrics().await;
    let addr = p.broker.listen_addr();
    let id = WireUuid([0x33; 16]);

    let first = build_client(addr).await;
    assert!(
        get_error_code(&first, id).await == 0,
        "the first get is assigned"
    );
    assert!(
        get_error_code(&first, id).await == THROTTLING_QUOTA_EXCEEDED,
        "a get inside the push interval is throttled"
    );
    drop(first);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let reconnected = build_client(addr).await;
        let error_code = get_error_code(&reconnected, id).await;
        if error_code == 0 {
            break;
        }
        assert!(
            error_code == THROTTLING_QUOTA_EXCEEDED && std::time::Instant::now() < deadline,
            "the reconnected client's instance survived: error_code={error_code}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    p.broker.shutdown().await;
}

async fn enroll_telemetry(
    client: &krabka_client_core::Client,
) -> GetTelemetrySubscriptionsResponse {
    configure_match_all_subscription(client, "all", "100").await;
    let response: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");
    assert!(response.error_code == 0);
    response
}
