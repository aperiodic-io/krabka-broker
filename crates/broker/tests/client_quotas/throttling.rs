//! Tests for quota enforcement on the request path.
//!
//! Each test sets one quota for alice, drives the request the quota governs,
//! and asserts that the broker reports the delay to the client as a non-zero
//! `throttle_time_ms` (KIP-219 throttle-then-respond) instead of silently
//! stalling the connection.

use {
    super::{
        CLIENT_ID,
        cluster::{
            create_topic_as_admin, seed_alice_write_acl, seed_compat_shim_disable_acl,
            start_single_broker_sasl_plaintext_with_users, wait_partition_exists,
        },
        data_plane::{drive_add_offsets_to_txn, drive_fetch_sasl, drive_produce_sasl},
        quota_admin::drive_alter_client_quotas_sasl,
    },
    crate::{
        cluster::wait_alice_quota,
        kafka_wire::{
            self, AutomaticTopicSetup, ExchangeSetup as WireExchangeSetup,
            PlainAuthenticationSetup, ProduceRecordCount, ProduceValueBytes,
            ResponseHeaderSelection as WireResponseHeaderSelection, ZeroFilledRecordsSetup,
            quotas::{QuotaAlterSetup, QuotaValue, UserQuotaSetup, alter_user_quota},
        },
        support::{
            acl::{TopicAclSetup, seed_topic_acl},
            discovery::api_versions_request_for,
            topics::ConfiguredTopicSetup,
            wire::{CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding},
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            api_versions_response::ApiVersionsResponse,
        },
    },
    std::time::{Duration, Instant},
    tokio::net::TcpStream,
};

/// `ApiVersions` is `api_key` 18.
const API_VERSIONS_KEY: i16 = 18;

/// The broker's default `quota_throttle_max`, in milliseconds. KIP-219 caps
/// the reported back-off at this, so no response may carry more.
const QUOTA_THROTTLE_MAX_MS: i32 = 1000;

macro_rules! throttled_response {
    ($stream:expr, $drive:path, $failure:tt) => {{
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut correlation = 11;
        loop {
            let response = $drive($stream, correlation).await;
            correlation += 1;
            if response.throttle_time_ms > 0 {
                break response;
            }
            assert!(Instant::now() <= deadline, $failure);
        }
    }};
}

/// Test 2: a low `(user=alice) producer_byte_rate` throttles a produce.
///
/// Set `(user=alice) producer_byte_rate=128`. alice produces about 8 KB. Assert
/// `throttle_time_ms > 0`.
///
/// Rate = 128 bytes/sec, burst = 1 second at rate = 128 bytes free. A produce
/// of 8 KB = 8192 bytes is about 7168 bytes over budget. At 128 bytes/sec that
/// is about 56 seconds of debt, which Kafka reports whole: a byte-rate
/// throttle has no cap. This test asserts only `throttle_time_ms` > 0. The
/// exact value is not load-bearing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttles_produce() {
    let (handle, _dir, resp) = throttled_producer_fixture("throttle-produce").await;

    check_successful_produce(&resp);
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 6: a tiny `(user=alice) request_percentage` throttles a produce.
///
/// Set a tiny `(user=alice) request_percentage` and NO byte-rate quota. alice
/// produces a small payload. Assert `throttle_time_ms > 0`.
///
/// This quota is the KIP-124 request quota, a server-side CPU-time throttle.
/// KIP-219 requires the broker to report it in `throttle_time_ms` and to mute
/// the channel. The broker must *not* silently delay the request.
/// `request_percentage=0.001` gives the bucket a budget of about 10µs/sec, far
/// below the processing time of any real produce handler. Even one small
/// produce thus trips the quota, and the response must carry a non-zero
/// throttle time. No `producer_byte_rate` is set, so only the request quota can
/// cause the throttle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttles_produce() {
    let (handle, _dir, addr) = quota_fixture().await;

    prepare_quota_topic(
        &handle,
        addr,
        "throttle-request",
        krabka_metadata::AclOperation::Write,
    )
    .await;

    // Set a tiny request_percentage for alice (no byte-rate quota).
    set_alice_quota(&handle, addr, QuotaSettingSetup::request_time()).await;

    // Alice produces a single small record. Retry past TOPIC_AUTHORIZATION_FAILED
    // (29) while the alice Write ACL propagates to the handler's image snapshot.
    let resp = produce_when_acl_applied(addr, ZeroFilledRecordsSetup {
        topic: ("throttle-request").to_string(),
        value_bytes: ProduceValueBytes(16),
        ..Default::default()
    })
    .await;

    check_successful_produce(&resp);
    assert!(
        resp.throttle_time_ms > 0,
        "expected request-quota throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

use crate::support::client::metric_value;

/// A throttled produce must move the throttle series, attributed to the quota
/// that caused it.
///
/// The setup is Test 2's: `(user=alice) producer_byte_rate=128`, and alice
/// produces 8 KB. That test asserts the client is *told* about the throttle;
/// this one asserts the broker records what it applied, so an operator seeing
/// produce latency rise can tell "the client is over its quota" from "the
/// broker is broken". `quota_type="Produce"` is the byte-rate quota, and it
/// must be the one credited: no `request_percentage` is set, so the KIP-124
/// quota asks for nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttle_moves_the_throttle_metrics() {
    let (handle, _dir, resp) = throttled_producer_fixture("throttle-metrics").await;
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    let applied_seconds = f64::from(resp.throttle_time_ms) / 1000.0;
    let phase = metric_value(
        &handle,
        "krabka_broker_request_throttle_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    assert!(
        phase >= applied_seconds,
        "the throttle phase must cover the delay the response reported \
         ({applied_seconds}s); got {phase}"
    );

    // The byte-rate quota is what fired, so it is what the applied-throttle
    // family credits. The request quota is unset and must stay at zero.
    let by_quota = metric_value(
        &handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Produce\"}",
    )
    .await;
    assert!(
        by_quota >= applied_seconds,
        "producer_byte_rate must be credited with the applied throttle \
         ({applied_seconds}s); got {by_quota}"
    );
    let request_quota = metric_value(
        &handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"Request\"}",
    )
    .await;
    assert!(
        request_quota < 1.0e-9,
        "no request_percentage quota is set, so it must be credited with \
         nothing; got {request_quota}"
    );

    handle.shutdown().await;
}

/// Test 3: a low `(user=alice) consumer_byte_rate` throttles a fetch.
///
/// Set `(user=alice) consumer_byte_rate=128`. Produce 8 KB as admin. alice
/// fetches. Assert `throttle_time_ms > 0`.
///
/// The rate and burst reasoning is the same as Test 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_byte_rate_throttles_fetch() {
    let (handle, _dir, addr) = quota_fixture().await;

    prepare_quota_topic(
        &handle,
        addr,
        "throttle-fetch",
        krabka_metadata::AclOperation::Read,
    )
    .await;

    // Set low consumer quota for alice.
    let alter_resp = alter_user_quota(addr, UserQuotaSetup {
        authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
        key: "consumer_byte_rate",
        value: QuotaValue(128.0),
        ..Default::default()
    })
    .await;
    assert!(
        alter_resp[0].1 == 0,
        "alter consumer_byte_rate must succeed"
    );

    // Wait for the quota to appear in the image.
    wait_alice_quota(&handle, "consumer_byte_rate", 128.0).await;

    // Produce 8 KB as admin (not subject to quota yet).
    seed_alice_write_acl(&handle, "throttle-fetch").await; // give admin path a topic
    let produce_resp = drive_produce_sasl(
        addr,
        ZeroFilledRecordsSetup {
            topic: ("throttle-fetch").to_string(),
            count: ProduceRecordCount(8),
            ..Default::default()
        },
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            user: "admin",
            password: (b"admin-secret").to_vec(),
        },
    )
    .await;
    let part = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "admin produce must succeed, error_code={}",
        part.error_code
    );

    // Alice fetches. Rate = 128 bytes/sec, data = 8 KB → throttle fires.
    // Retry loop: auth can lag.
    let deadline = Instant::now() + Duration::from_secs(15);
    let fetch_resp = loop {
        let r = drive_fetch_sasl(addr, "alice", b"alice-secret", "throttle-fetch").await;
        // If throttle_time_ms > 0 or error_code == 0, we have a real response.
        if r.error_code == 0 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "fetch error after 15s; error_code={}",
            r.error_code
        );
        // real-time wait (not a progress poll): retry cadence between network fetch attempts, deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        fetch_resp.throttle_time_ms > 0,
        "expected consumer throttle_time_ms > 0, got {}",
        fetch_resp.throttle_time_ms
    );
    // Kafka answers a throttled consumer fetch with an empty response: no
    // rows, no error, and no session (`FetchContext.getThrottledResponse`).
    // The client did not get the records, so its position does not move.
    assert!(
        fetch_resp
            == krabka_protocol::owned::fetch_response::FetchResponse {
                throttle_time_ms: fetch_resp.throttle_time_ms,
                ..Default::default()
            }
    );

    handle.shutdown().await;
}

/// Test 4: a `(user, client-id)` quota overrides a user-only quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_client_tuple_overrides_user_specific() {
    let (handle, _dir, addr) = quota_fixture().await;

    prepare_quota_topic(
        &handle,
        addr,
        "precedence-topic",
        krabka_metadata::AclOperation::Write,
    )
    .await;

    // Set a lenient user-only quota.
    let alter_user = alter_user_quota(addr, UserQuotaSetup {
        authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
        value: QuotaValue(8192.0),
        ..Default::default()
    })
    .await;
    assert!(alter_user[0].1 == 0, "alter user quota must succeed");

    // Set a tight tuple quota for `CLIENT_ID`.
    let alter_tuple = drive_alter_client_quotas_sasl(
        addr,
        vec![(
            vec![
                ("user".into(), Some("alice".into())),
                ("client-id".into(), Some("krabka-quota-test".into())),
            ],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        &QuotaAlterSetup {
            authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
            ..Default::default()
        },
    )
    .await;
    assert!(alter_tuple[0].1 == 0, "alter tuple quota must succeed");

    // Wait for both quotas to appear in the image.
    handle
        .wait_for_image(|img| {
            let user_key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            let tuple_key: krabka_metadata::EntityKey = vec![
                ("client-id".into(), Some("krabka-quota-test".into())),
                ("user".into(), Some("alice".into())),
            ];
            let user_rate = img
                .client_quotas()
                .get(&user_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            let tuple_rate = img
                .client_quotas()
                .get(&tuple_key)
                .and_then(|c| c.get("producer_byte_rate"))
                .copied();
            user_rate == Some(8192.0) && tuple_rate == Some(128.0)
        })
        .await;

    // Alice produces 8 KB with `krabka-quota-test`. The 128-byte tuple quota
    // throttles it; the 8192-byte user-only quota would fit in the burst window.
    let resp = produce_when_acl_applied(addr, ZeroFilledRecordsSetup {
        topic: ("precedence-topic").to_string(),
        count: ProduceRecordCount(8),
        ..Default::default()
    })
    .await;

    check_successful_produce(&resp);
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0 because the user/client tuple rate applies; got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 7: the request quota is echoed on an API the dispatch loop patches.
///
/// `Produce` and `Fetch` fill `ThrottleTimeMs` in themselves, before encoding.
/// Everywhere else `apply_request_quota` runs, the delay is written into
/// the already-encoded body by patching its leading int32, which is only safe
/// on the responses whose schema really does put `ThrottleTimeMs` first.
/// `network::dispatch::throttle_audit` pins which those are against the
/// generated encoders; this test proves the patch reaches the wire on one of
/// them and corrupts nothing else.
///
/// `AddOffsetsToTxn` is the API driven here because its dispatch entry carries
/// `RequestQuotaPolicy::ApplyFallbackAccounting`, so the quota path runs, and
/// its response leads with `ThrottleTimeMs`, so the patch applies. alice drives
/// it with no quota set, then again with a tiny `(user=alice)`
/// `request_percentage`. The throttled response must report
/// `throttle_time_ms > 0` and be equal to the unthrottled one in every other
/// field, which is what a leading-int32 patch and nothing more looks like.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttle_is_echoed_on_a_patched_api() {
    let (handle, _dir, addr) = quota_fixture().await;

    // One connection for every alice request: re-authenticating would charge
    // each handshake to the same quota bucket.
    let mut stream = alice_quota_connection(addr, "SASL authenticate for AddOffsetsToTxn").await;

    // Baseline: no quota is configured yet, so there is no delay to report.
    let baseline = drive_add_offsets_to_txn(&mut stream, 10).await;
    assert!(
        baseline.throttle_time_ms == 0,
        "unthrottled response must report throttle_time_ms=0, got {}",
        baseline.throttle_time_ms
    );

    set_alice_quota(&handle, addr, QuotaSettingSetup::request_time()).await;

    // Drive the same request until the request bucket runs dry. Each request
    // charges its own handler time, so the first one over budget is throttled.
    let throttled = throttled_response!(
        &mut stream,
        drive_add_offsets_to_txn,
        "no request-quota throttle on AddOffsetsToTxn after 15s"
    );

    // The patch touched the leading int32 and nothing else: every other field
    // still decodes to what the unthrottled response carried.
    assert!(
        throttled
            == AddOffsetsToTxnResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                ..baseline
            }
    );

    handle.shutdown().await;
}

/// `ApiVersions` v3 is the version driven below: v1 is the first with a
/// `ThrottleTimeMs` at all, and v3 is the first flexible one, which is what a
/// modern client negotiates with.
const API_VERSIONS_VERSION: i16 = 3;

/// Open the fixture's Alice session without charging a second handshake.
async fn alice_quota_connection(addr: std::net::SocketAddr, context: &str) -> TcpStream {
    kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        password: b"alice-secret".to_vec(),
        ..Default::default()
    })
    .await
    .expect(context)
}

/// Drives one `ApiVersions` v3 handshake on an already-authenticated `stream`
/// and returns the decoded response.
///
/// The request body is flexible from v3, but the `ApiVersions` *response*
/// header is v0 at every version -- Kafka's one header exception, so that a
/// client which has not yet learned the broker's versions can always parse the
/// reply. The two header flexibilities therefore differ.
async fn drive_api_versions(stream: &mut TcpStream, corr_id: i32) -> ApiVersionsResponse {
    let req = api_versions_request_for("krabka-quota-test".to_string(), "0.0.1".to_string());
    let mut body = BytesMut::new();
    req.encode(&mut body, API_VERSIONS_VERSION)
        .expect("encode ApiVersions");
    let resp_bytes = kafka_wire::round_trip(
        stream,
        WireExchangeSetup {
            api_key: WireApiKey(API_VERSIONS_KEY),
            version: WireApiVersion(API_VERSIONS_VERSION),
            correlation: WireCorrelationId(corr_id),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            response_header: WireResponseHeaderSelection::Exact(RequestHeaderEncoding::Legacy),
        },
        &body,
    )
    .await
    .expect("ApiVersions round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ApiVersionsResponse::decode(&mut cur, API_VERSIONS_VERSION).expect("decode ApiVersionsResponse")
}

/// Test 9: `ApiVersions` reports the request-quota delay it was held for.
///
/// `ApiVersionsResponse` carries `ThrottleTimeMs` behind the `ApiKeys` array,
/// so the dispatch loop's leading-int32 patch cannot reach it. Its dispatch
/// entry is `RequestQuotaPolicy::SelfAccounted` for that reason: the handler
/// charges the KIP-124 request quota and fills the field in on the typed
/// response, the way Kafka's `handleApiVersionsRequest` answers through
/// `requestHelper.sendResponseMaybeThrottle`. Without that, a client held by
/// the quota reads `throttle_time_ms = 0` and never backs off.
///
/// One authenticated connection drives the handshake until the
/// `(user=alice) request_percentage` bucket runs dry, which is what the
/// `AddOffsetsToTxn` test above does on the patched path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttle_is_reported_on_api_versions() {
    let (handle, _dir, addr) = quota_fixture().await;

    let mut stream = alice_quota_connection(addr, "SASL authenticate for ApiVersions").await;

    // Baseline: no quota is configured yet, so there is no delay to report.
    let baseline = drive_api_versions(&mut stream, 10).await;
    assert!(
        baseline.error_code == 0,
        "ApiVersions must succeed, error_code={}",
        baseline.error_code
    );
    assert!(
        baseline.throttle_time_ms == 0,
        "unthrottled response must report throttle_time_ms=0, got {}",
        baseline.throttle_time_ms
    );

    set_alice_quota(&handle, addr, QuotaSettingSetup::request_time()).await;

    let throttled = throttled_response!(
        &mut stream,
        drive_api_versions,
        "no request-quota throttle on ApiVersions after 15s"
    );

    // The handler set the field on the typed response, so nothing else moved:
    // the advertised table and the feature rows are the baseline's. The
    // finalized features epoch is the metadata offset, as Kafka's
    // `KRaftMetadataCache.features` reports it, so the quota record written in
    // between moves it on.
    assert!(throttled.finalized_features_epoch > baseline.finalized_features_epoch);
    assert!(
        throttled
            == ApiVersionsResponse {
                throttle_time_ms: throttled.throttle_time_ms,
                finalized_features_epoch: throttled.finalized_features_epoch,
                ..baseline
            }
    );
    assert!(
        throttled.throttle_time_ms <= QUOTA_THROTTLE_MAX_MS,
        "reported back-off must stay inside quota_throttle_max, got {}",
        throttled.throttle_time_ms
    );

    handle.shutdown().await;
}

async fn produce_when_acl_applied(
    addr: std::net::SocketAddr,
    records: ZeroFilledRecordsSetup,
) -> krabka_protocol::owned::produce_response::ProduceResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = drive_produce_sasl(addr, records.clone(), &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            password: (b"alice-secret").to_vec(),
            ..Default::default()
        })
        .await;
        let code = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(-1, |p| p.error_code);
        if code != 29 {
            return r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn quota_fixture() -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    std::net::SocketAddr,
) {
    start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", "admin-secret"),
        ("alice", "alice-secret"),
    ])
    .await
}

async fn prepare_quota_topic(
    broker: &krabka_broker::BrokerHandle,
    addr: std::net::SocketAddr,
    topic: &str,
    operation: krabka_metadata::AclOperation,
) {
    seed_compat_shim_disable_acl(broker).await;
    create_topic_as_admin(addr, AutomaticTopicSetup {
        client_id: CLIENT_ID,
        topic: ConfiguredTopicSetup {
            name: (topic).into(),
            ..Default::default()
        },
    })
    .await;
    wait_partition_exists(broker, topic, 0).await;
    seed_topic_acl(broker, TopicAclSetup {
        topic,
        operation,
        ..Default::default()
    })
    .await;
}

fn check_successful_produce(response: &krabka_protocol::owned::produce_response::ProduceResponse) {
    let part = &response.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
}

/// A successful Produce request after Alice's topic ACL and 128-byte quota become visible.
async fn throttled_producer_fixture(
    topic: &str,
) -> (
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    krabka_protocol::owned::produce_response::ProduceResponse,
) {
    let (handle, directory, addr) = quota_fixture().await;
    prepare_quota_topic(&handle, addr, topic, krabka_metadata::AclOperation::Write).await;
    set_alice_quota(&handle, addr, QuotaSettingSetup::low_producer()).await;
    let response = produce_when_acl_applied(addr, ZeroFilledRecordsSetup {
        topic: topic.to_string(),
        count: ProduceRecordCount(8),
        ..Default::default()
    })
    .await;
    (handle, directory, response)
}

#[derive(krabka_macros::FieldDefaults)]
struct QuotaSettingSetup<'a> {
    #[default("producer_byte_rate")]
    key: &'a str,
    value: QuotaValue,
}

impl QuotaSettingSetup<'_> {
    fn low_producer() -> Self {
        Self {
            value: QuotaValue(128.0),
            ..Default::default()
        }
    }

    fn request_time() -> Self {
        Self {
            key: "request_percentage",
            value: QuotaValue(0.001),
        }
    }
}

async fn set_alice_quota(
    broker: &krabka_broker::BrokerHandle,
    addr: std::net::SocketAddr,
    setup: QuotaSettingSetup<'_>,
) {
    kafka_wire::quotas::set_user_quota(broker, addr, UserQuotaSetup {
        authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
        key: setup.key,
        value: setup.value,
        ..Default::default()
    })
    .await;
}
