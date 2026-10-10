//! KIP-219 end-to-end tests for the serve loop: a throttled request is
//! answered immediately and the quota is enforced afterwards by muting the
//! connection.
//!
//! Every test here drives a real socket through `serve_connection_stream`,
//! because the ordering under test is a property of that loop: the write
//! happens first, and the window the handler charged is applied by refusing
//! to read the next request. The pre-KIP-219 broker slept inside the quota
//! code before writing, which each test distinguishes by giving the response
//! a deadline far shorter than the window it must beat.

use std::time::{Duration, Instant};

use assert2::check;
use bytes::BytesMut;
use futures_util::{SinkExt as _, StreamExt as _};
use krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};
use krabka_protocol::{
    Decode, Encode as _,
    owned::{fetch_response::FetchResponse, produce_response::ProduceResponse},
    records::{Record, RecordBatch},
};
use krabka_units::{Time, convert::TimeExt as _, millis};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::request_frame;
use crate::network::{codec::KafkaCodec, test_support::RequestFrameSetup};

/// `Produce` wire `api_key`.
const PRODUCE_KEY: i16 = 0;
/// `Fetch` wire `api_key`.
const FETCH_KEY: i16 = 1;
/// The `Produce` version these tests drive: flexible, and its response carries
/// `throttle_time_ms`.
const PRODUCE_VERSION: i16 = 11;
/// The `Fetch` version these tests drive: flexible, and its response carries
/// `throttle_time_ms` first.
const FETCH_VERSION: i16 = 12;

/// Stands in for a client `request.timeout.ms` well inside a mute window. A
/// response that has to beat it, or a muted read that has to outlast it, is
/// two orders of magnitude away from the real cost of either, so this is loose
/// on a loaded machine and still four times shorter than the shortest window
/// any test here configures.
const CLIENT_TIMEOUT: Duration = Duration::from_millis(250);
/// Scheduling slack on a lower bound measured against a mute window.
const SLACK: Duration = Duration::from_millis(50);
/// An upper bound on a single mute that no scheduling delay is expected to
/// exceed, but that two stacked windows of the same length would.
const ONE_WINDOW_CEILING: Duration = Duration::from_millis(1900);

/// A muted connection is not read from, so a request left in flight over one
/// needs an outer bound that is generous next to the window itself.
const MUTE_LIFT_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts a broker whose quota throttle caps at `throttle_max`, and seeds
/// `quotas` against the ANONYMOUS user.
///
/// PLAINTEXT authenticates every connection as ANONYMOUS, so that is the
/// principal every quota in this module is looked up under. The function
/// returns once the seeded quotas are visible in the metadata image, so a
/// request driven afterwards is guaranteed to see them.
async fn broker_with_anonymous_quotas(
    throttle_max: Time,
    quotas: &[(&str, f64)],
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = broker_with_throttle(throttle_max).await;
    seed_anonymous_quotas(&handle, quotas).await;
    (handle, dir)
}

/// Seeds `quotas` against the ANONYMOUS user on an already-running `handle`,
/// the way [`broker_with_anonymous_quotas`] does at startup.
///
/// PLAINTEXT authenticates every connection as ANONYMOUS, so that is the
/// principal every quota in this module is looked up under. The function
/// returns once the seeded quotas are visible in the metadata image, so a
/// request driven afterwards is guaranteed to see them.
///
/// A caller that also needs a topic to exist should create it *before*
/// calling this: seeding a `request_percentage` quota this low mutes the
/// connection for `throttle_max` on the very next quota-charged request, so a
/// topic created through the wire protocol afterwards would eat the mute
/// window this function's caller is trying to test around.
async fn seed_anonymous_quotas(handle: &crate::broker::BrokerHandle, quotas: &[(&str, f64)]) {
    let records: Vec<MetadataRecord> = quotas
        .iter()
        .map(|(key, value)| {
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: vec![QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("ANONYMOUS".into()),
                }],
                config_key: (*key).into(),
                config_value: Some(*value),
            })
        })
        .collect();
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("seed client quotas");
    handle
        .wait_for_image(|image| {
            let key: EntityKey = vec![("user".into(), Some("ANONYMOUS".into()))];
            let Some(configs) = image.client_quotas().get(&key) else {
                return false;
            };
            quotas
                .iter()
                .all(|(name, value)| configs.get(*name) == Some(value))
        })
        .await;
}

/// Accepts one PLAINTEXT connection and serves it through the real dispatch
/// loop. It returns the join handle for the loop and a client already framed
/// against it.
async fn connect_to_serve_loop(
    handle: &crate::broker::BrokerHandle,
) -> (tokio::task::JoinHandle<()>, Framed<TcpStream, KafkaCodec>) {
    crate::network::dispatch::test_support::plaintext_loop(handle.broker_arc_for_test()).await
}

async fn create_topic(framed: &mut Framed<TcpStream, KafkaCodec>, topic: &str) {
    let body = encoded(
        &configured_topic_request(CreateTopicSetup {
            topic,
            ..Default::default()
        }),
        7,
    );
    send_request(framed, 19, 7, 1, &body).await;
    let response = response_frame(
        framed,
        CLIENT_TIMEOUT,
        "the response must beat the client timeout",
    )
    .await;
    check!(response_correlation_id(&response) == 1);
}

/// Writes a v0 `ApiVersions` request, the cheapest frame that still reaches a
/// real handler through the whole serve loop.
async fn send_api_versions(framed: &mut Framed<TcpStream, KafkaCodec>, correlation_id: i32) {
    let frame = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(super::API_VERSIONS_KEY),
        correlation_id: crate::network::test_support::FrameCorrelationId(correlation_id),
        ..Default::default()
    })
    .freeze();
    framed.send(frame).await.expect("send ApiVersions");
}

/// Writes a flexible request frame — a v2 request header, with its trailing
/// tagged-fields byte — carrying an encoded `body`.
async fn send_request(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &BytesMut,
) {
    let frame = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(api_key),
        api_version: krabka_ids::ApiVersion(version),
        correlation_id: crate::network::test_support::FrameCorrelationId(correlation_id),
        tagged: Some(&[0]),
        body,
        ..Default::default()
    })
    .freeze();
    framed.send(frame).await.expect("send request");
}

/// Reads the leading correlation id of a response frame.
fn response_correlation_id(frame: &BytesMut) -> i32 {
    i32::from_be_bytes(frame[..4].try_into().expect("response correlation id"))
}

krabka_macros::single_partition_produce_fixture!(single_partition_produce);

/// Builds a `Produce` request body for `topic` carrying `count` records of
/// `record_bytes` each.
fn produce_body(topic: &str, acks: i16, record_bytes: usize, count: usize) -> BytesMut {
    let value = bytes::Bytes::from(vec![0u8; record_bytes]);
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).expect("record index"),
            value: Some(value.clone()),
            ..Default::default()
        })
        .collect();
    let request = single_partition_produce(SinglePartitionProduceSetup {
        topic: (topic).into(),
        records: Some(
            RecordBatch {
                last_offset_delta: i32::try_from(count - 1).expect("record count"),
                records,
                ..Default::default()
            }
            .into(),
        ),
        acknowledgements: ProduceAcknowledgements::from_wire(WireAcknowledgements(acks)),
        timeout: ProduceTimeoutMillis(30_000),
        ..Default::default()
    });
    let mut body = BytesMut::new();
    request
        .encode(&mut body, PRODUCE_VERSION)
        .expect("encode Produce");
    body
}

/// Builds a consumer `Fetch` request body for partition 0 of `topic`.
fn fetch_body(topic: &str) -> BytesMut {
    let request = consumer_fetch_request(topic);
    let mut body = BytesMut::new();
    request
        .encode(&mut body, FETCH_VERSION)
        .expect("encode Fetch");
    body
}

/// Strips the flexible (v1) response header — a correlation id plus an empty
/// tagged-fields byte — and decodes the body at `version`.
fn decode_response_body<T: Decode<'static>>(frame: &BytesMut, version: i16) -> T {
    let mut cursor: &[u8] = &frame[5..];
    T::decode(&mut cursor, version).expect("decode response body")
}

/// Reads the response the mute is holding back, and returns how long after
/// `muted_at` it arrived.
///
/// It first pins the mute down: nothing may be served for `CLIENT_TIMEOUT`,
/// which is far shorter than every window configured here.
async fn read_after_mute(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    muted_at: Instant,
) -> (BytesMut, Duration) {
    check!(
        tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
            .await
            .is_err(),
        "a muted connection must serve no further request inside the throttle window"
    );
    let frame = response_frame(
        framed,
        MUTE_LIFT_TIMEOUT,
        "the mute must lift once the window closes",
    )
    .await;
    (frame, muted_at.elapsed())
}

/// KIP-219: a throttled request is answered at once, and the quota is enforced
/// by muting the connection afterwards.
///
/// `request_percentage = 0.0001` gives the KIP-124 bucket a budget of one
/// microsecond of handler time per second, so the first request overruns it by
/// orders of magnitude and earns the configured maximum window. The
/// pre-KIP-219 broker slept for that window *before* writing the response,
/// which is what this pins down: the response has to beat a client timeout far
/// shorter than the window, and the next request must go unserved until the
/// window closes.
#[tokio::test]
async fn throttled_connection_answers_first_and_mutes_afterwards() {
    let mute_window = millis(1000);
    let (handle, _dir, server, mut framed) = request_quota_connection(mute_window).await;

    // The first request trips the quota. Its response must still arrive well
    // inside a client timeout that the throttle window would blow through.
    let sent_at = Instant::now();
    send_api_versions(&mut framed, 1).await;
    let (_first, answered_at) = response_before_mute(
        &mut framed,
        sent_at,
        mute_window,
        "throttled response must beat the client timeout, not wait out the window",
    )
    .await;

    // The connection is now muted: the second request sits unread until the
    // window closes, and is then served.
    finish_muted_connection(framed, server, handle, answered_at, mute_window).await;
}

/// KIP-219 on the `Fetch` path, which writes its response as a plan of raw
/// socket writes rather than through the framed sink.
///
/// The records the consumer is waiting on must go out first, and the KIP-124
/// window the fetch charged is applied only once the whole plan is written.
/// Sleeping inside the fetch quota instead would hold the records back for the
/// full window, which the `CLIENT_TIMEOUT` bound rules out.
#[tokio::test]
async fn a_throttled_fetch_writes_its_plan_before_the_mute() {
    let mute_window = millis(1000);
    let (handle, _dir, server, mut framed) = request_quota_connection(mute_window).await;

    let sent_at = Instant::now();
    send_request(
        &mut framed,
        FETCH_KEY,
        FETCH_VERSION,
        1,
        &fetch_body("no-such-topic"),
    )
    .await;
    let (first, answered_at) = response_before_mute(
        &mut framed,
        sent_at,
        mute_window,
        "the fetch plan must beat the client timeout, not wait out the window",
    )
    .await;

    // The window the connection is about to be muted for is the one the
    // response reports.
    let fetch: FetchResponse = decode_response_body(&first, FETCH_VERSION);
    assert_reported_window(fetch.throttle_time_ms, mute_window);

    finish_muted_connection(framed, server, handle, answered_at, mute_window).await;
}

/// An `acks=0` produce writes no response frame, and still carries a mute
/// window.
///
/// Kafka answers nothing to `acks=0`, but the handler has charged
/// `producer_byte_rate` all the same, so the quota still has to be enforced.
/// The only way left to enforce it is the channel mute, which this pins down:
/// the next request on the connection goes unserved for the window, and when
/// the connection does speak it is that request's response, never a produce
/// response.
///
/// The produce targets a topic created just before it: an `acks = 0` produce
/// that errors on any partition closes the connection instead of muting it
/// (#858/#860), which is a different (and also tested) behavior from the
/// byte-rate mute-then-recover this test is about.
#[tokio::test]
async fn acks_zero_produce_writes_no_response_and_still_mutes() {
    let mute_window = millis(1000);
    // A byte-rate throttle is not bounded (#709): 2800 bytes/sec against an
    // 8 KiB produce, after the one-second burst, is about two seconds of debt,
    // longer than `mute_window` and well inside `MUTE_LIFT_TIMEOUT`.
    let (handle, _dir) =
        broker_with_anonymous_quotas(mute_window, &[("producer_byte_rate", 2800.0)]).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    create_topic(&mut framed, "acks-zero-mute").await;

    let produced_at = Instant::now();
    send_request(
        &mut framed,
        PRODUCE_KEY,
        PRODUCE_VERSION,
        2,
        &produce_body("acks-zero-mute", 0, 1024, 8),
    )
    .await;
    send_api_versions(&mut framed, 3).await;

    // Nothing at all for the window: no produce response, because `acks=0`
    // asks for none, and no `ApiVersions` response, because the produce muted
    // the connection.
    let (frame, muted_for) = read_after_mute(&mut framed, produced_at).await;
    check!(
        response_correlation_id(&frame) == 3,
        "the only response must be the ApiVersions one; acks=0 writes no produce response"
    );
    check!(muted_for >= mute_window.to_std().saturating_sub(SLACK));

    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

/// A request that trips two quotas is muted once, for the longer window, not
/// once per quota and not for their sum.
///
/// A produce charges both `producer_byte_rate` and `request_percentage`. The
/// request quota is seeded far below what this request needs, so it saturates
/// at `quota_throttle_max`. The byte rate leaves about three quarters of a
/// window of debt, which Kafka does not bound — so summing the two is
/// observably different from taking the longest. Kafka reports one
/// `throttle_time_ms` and mutes the channel once, so the response must carry a
/// single window and the connection must be readable again after one.
#[tokio::test]
async fn a_request_tripping_two_quotas_is_muted_once_for_the_longest_window() {
    let mute_window = millis(1000);
    let (handle, _dir) = broker_with_anonymous_quotas(
        mute_window,
        &[
            ("producer_byte_rate", 4800.0),
            ("request_percentage", 0.0001),
        ],
    )
    .await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    send_request(
        &mut framed,
        PRODUCE_KEY,
        PRODUCE_VERSION,
        1,
        &produce_body("no-such-topic", 1, 1024, 8),
    )
    .await;
    let first = response_frame(
        &mut framed,
        CLIENT_TIMEOUT,
        "throttled produce must beat the client timeout, not wait out the window",
    )
    .await;
    let answered_at = Instant::now();
    check!(response_correlation_id(&first) == 1);

    // One window on the wire, not two summed.
    let produce: ProduceResponse = decode_response_body(&first, PRODUCE_VERSION);
    assert_reported_window(produce.throttle_time_ms, mute_window);

    let muted_for = next_response_after_mute(&mut framed, answered_at, mute_window).await;
    check!(
        muted_for < ONE_WINDOW_CEILING,
        "two quotas must mute for one window, not for their sum"
    );

    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

/// KIP-13: a `Produce` is charged its whole request, header and body, once
/// (#748), as Kafka's `KafkaApis.handleProduceRequest` charges
/// `request.sizeInBytes` -- not only the record payloads. At 1000 bytes/sec
/// with a one-second burst, the reported throttle is the frame size less the
/// burst, in milliseconds.
#[tokio::test]
async fn produce_charges_the_whole_request_frame_once() {
    let (handle, _dir) =
        broker_with_anonymous_quotas(millis(1000), &[("producer_byte_rate", 1000.0)]).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    let body = produce_body("frame-charge", 1, 256, 8);
    let frame_len = request_frame(RequestFrameSetup {
        api_key: krabka_ids::ApiKey(PRODUCE_KEY),
        api_version: krabka_ids::ApiVersion(PRODUCE_VERSION),
        tagged: Some(&[0]),
        body: &body,
        ..Default::default()
    })
    .len();
    send_request(&mut framed, PRODUCE_KEY, PRODUCE_VERSION, 1, &body).await;
    let response = response_frame(
        &mut framed,
        CLIENT_TIMEOUT,
        "the response must beat the client timeout",
    )
    .await;
    let produce: ProduceResponse = decode_response_body(&response, PRODUCE_VERSION);

    let expected = i32::try_from(frame_len).expect("frame length") - 1000;
    check!(
        (expected - 5..=expected).contains(&produce.throttle_time_ms),
        "frame of {frame_len} bytes, throttle {}",
        produce.throttle_time_ms
    );

    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

/// One request of [`every_charged_api_reports_its_delay_and_mutes`]: the api,
/// the version, whether the body is flexible, the encoded body, and how to
/// read `throttle_time_ms` back out of the response body.
struct QuotaCase {
    name: &'static str,
    api_key: i16,
    version: i16,
    flexible: bool,
    body: BytesMut,
    throttle_time_ms: fn(&[u8], i16) -> Option<i32>,
}

/// Encodes `request` at `version`.
fn encoded<R: krabka_protocol::Encode>(request: &R, version: i16) -> BytesMut {
    let mut body = BytesMut::new();
    request.encode(&mut body, version).expect("encode request");
    body
}

/// Decodes `body` as `R` at `version`, and reads its `throttle_time_ms`.
fn throttle_of<R: for<'de> Decode<'de>>(body: &[u8], version: i16, read: fn(&R) -> i32) -> i32 {
    let mut cursor = body;
    let response = R::decode(&mut cursor, version).expect("decode response body");
    assert2::assert!(cursor.is_empty(), "the decoder consumed every byte");
    read(&response)
}

/// KIP-124 (#692): every request that Kafka charges to the request quota is
/// charged. Each response
/// reports the delay in `throttle_time_ms`, wherever the schema puts the
/// field, and the connection is muted afterwards. `WriteTxnMarkers` is exempt,
/// as Kafka answers it through `sendResponseExemptThrottle`.
#[tokio::test]
async fn every_charged_api_reports_its_delay_and_mutes() {
    use krabka_protocol::owned::{
        describe_delegation_token_request::DescribeDelegationTokenRequest,
        describe_delegation_token_response::DescribeDelegationTokenResponse,
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::FindCoordinatorResponse,
        get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
        get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
        offset_delete_request::OffsetDeleteRequest, offset_delete_response::OffsetDeleteResponse,
        write_txn_markers_request::WriteTxnMarkersRequest,
    };

    let mute_window = millis(1000);
    let window_ms = i32::try_from(mute_window.millis_i64()).expect("a window");
    let (handle, _dir) = broker_with_throttle(mute_window).await;
    // The `FindCoordinator` case must find `__consumer_offsets`. A lookup
    // that creates it runs the creation on this runtime, and that work would
    // delay the read the mute window is measured from.
    handle.wait_until_group_coordinator_ready().await;
    seed_anonymous_quotas(&handle, &[("request_percentage", 0.0001)]).await;

    let cases = [
        QuotaCase {
            name: "Metadata, a context dispatch",
            api_key: 3,
            version: 12,
            flexible: true,
            body: encoded(
                &MetadataRequest {
                    topics: Some(Vec::new()),
                    ..Default::default()
                },
                12,
            ),
            throttle_time_ms: |body, version| {
                Some(throttle_of::<MetadataResponse>(body, version, |r| {
                    r.throttle_time_ms
                }))
            },
        },
        QuotaCase {
            name: "FindCoordinator, a context dispatch",
            api_key: 10,
            version: 4,
            flexible: true,
            body: encoded(
                &FindCoordinatorRequest {
                    coordinator_keys: vec!["group".to_owned()],
                    ..Default::default()
                },
                4,
            ),
            throttle_time_ms: |body, version| {
                Some(throttle_of::<FindCoordinatorResponse>(body, version, |r| {
                    r.throttle_time_ms
                }))
            },
        },
        QuotaCase {
            name: "OffsetDelete, whose throttle follows the error code",
            api_key: 47,
            version: 0,
            flexible: false,
            body: encoded(
                &OffsetDeleteRequest {
                    group_id: "group".to_owned(),
                    ..Default::default()
                },
                0,
            ),
            throttle_time_ms: |body, version| {
                Some(throttle_of::<OffsetDeleteResponse>(body, version, |r| {
                    r.throttle_time_ms
                }))
            },
        },
        QuotaCase {
            name: "DescribeDelegationToken, an auth dispatch whose throttle is last",
            api_key: 41,
            version: 3,
            flexible: true,
            body: encoded(&DescribeDelegationTokenRequest::default(), 3),
            throttle_time_ms: |body, version| {
                Some(throttle_of::<DescribeDelegationTokenResponse>(
                    body,
                    version,
                    |r| r.throttle_time_ms,
                ))
            },
        },
        QuotaCase {
            name: "GetTelemetrySubscriptions, a telemetry dispatch",
            api_key: 71,
            version: 0,
            flexible: true,
            body: encoded(&GetTelemetrySubscriptionsRequest::default(), 0),
            throttle_time_ms: |body, version| {
                Some(throttle_of::<GetTelemetrySubscriptionsResponse>(
                    body,
                    version,
                    |r| r.throttle_time_ms,
                ))
            },
        },
        QuotaCase {
            name: "WriteTxnMarkers, exempt",
            api_key: 27,
            version: 1,
            flexible: true,
            body: encoded(&WriteTxnMarkersRequest::default(), 1),
            throttle_time_ms: |_, _| None,
        },
    ];

    for case in cases {
        let (server, mut framed) = connect_to_serve_loop(&handle).await;
        let frame = super::request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(case.api_key),
            api_version: krabka_ids::ApiVersion(case.version),
            tagged: case.flexible.then_some(&[0][..]),
            body: &case.body,
            ..Default::default()
        })
        .freeze();
        framed.send(frame).await.expect("send request");
        let response = response_frame(
            &mut framed,
            CLIENT_TIMEOUT,
            "the response must beat the client timeout",
        )
        .await;
        let answered_at = Instant::now();
        check!(response_correlation_id(&response) == 1, "{}", case.name);
        let header_len = if case.flexible { 5 } else { 4 };
        let exempt = case.api_key == 27;
        check!(
            (case.throttle_time_ms)(&response[header_len..], case.version)
                == (!exempt).then_some(window_ms),
            "{}",
            case.name
        );

        send_api_versions(&mut framed, 2).await;
        if exempt {
            let next = tokio::time::timeout(CLIENT_TIMEOUT, framed.next()).await;
            check!(
                next.is_ok(),
                "{}: an exempt request does not mute the connection",
                case.name
            );
        } else {
            let (next, muted_for) = read_after_mute(&mut framed, answered_at).await;
            check!(response_correlation_id(&next) == 2, "{}", case.name);
            check!(
                muted_for >= mute_window.to_std().saturating_sub(SLACK),
                "{}",
                case.name
            );
        }

        close_serve_loop(framed, server).await;
    }
    handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleProduceRequest` charges no request quota for an
/// `acks = 0` produce (#692), so a connection with only a `request_percentage`
/// quota is not muted by one.
///
/// The produce targets a topic created just before the quota is seeded: an
/// `acks = 0` produce that errors on any partition closes the connection
/// instead (#858/#860), which would be indistinguishable here from the
/// request-quota exemption this test is about, and creating the topic
/// *through* the low `request_percentage` quota would itself mute the
/// connection for the window this test needs clear of any mute.
#[tokio::test]
async fn acks_zero_produce_is_exempt_from_the_request_quota() {
    let mute_window = millis(1000);
    let (handle, _dir) = broker_with_throttle(mute_window).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    create_topic(&mut framed, "acks-zero-exempt").await;

    // Seeded only now: the topic exists before the connection has any
    // request-quota debt to charge against it.
    seed_anonymous_quotas(&handle, &[("request_percentage", 0.0001)]).await;

    send_request(
        &mut framed,
        PRODUCE_KEY,
        PRODUCE_VERSION,
        2,
        &produce_body("acks-zero-exempt", 0, 1024, 8),
    )
    .await;
    send_api_versions(&mut framed, 3).await;

    let next = response_frame(
        &mut framed,
        CLIENT_TIMEOUT,
        "an acks=0 produce must not mute the connection",
    )
    .await;
    check!(response_correlation_id(&next) == 3);

    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

/// A `CreateTopics` charges `controller_mutation_rate` in its handler and
/// `request_percentage` in the dispatch loop. Kafka resolves the two as one
/// throttle decision (`sendResponseMaybeThrottleWithControllerQuota`), so the
/// per-api throttle metric observes the request once, not once per quota.
#[tokio::test]
async fn a_controller_mutation_and_the_request_quota_resolve_in_one_observation() {
    const VERSION: i16 = 7;
    let (handle, _dir) = broker_with_anonymous_quotas(
        millis(1000),
        &[
            ("controller_mutation_rate", 1.0),
            ("request_percentage", 0.0001),
        ],
    )
    .await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    let body = encoded(
        &configured_topic_request(CreateTopicSetup {
            topic: "one-observation",
            ..Default::default()
        }),
        VERSION,
    );
    send_request(&mut framed, 19, VERSION, 1, &body).await;
    let response = response_frame(
        &mut framed,
        CLIENT_TIMEOUT,
        "the response must beat the client timeout",
    )
    .await;
    check!(response_correlation_id(&response) == 1);

    let rendered = {
        let metrics = &handle.broker_arc_for_test().metrics;
        let registry = metrics.registry.lock().await;
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &registry).expect("encode registry");
        out
    };
    check!(
        rendered.contains(
            "krabka_broker_request_throttle_duration_seconds_count{api_key=\"CreateTopics\"} 1\n"
        ),
        "{rendered}"
    );

    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

krabka_macros::consumer_fetch_fixture!(consumer_fetch_request);
krabka_macros::create_topic_fixture!(configured_topic_request);

async fn broker_with_throttle(
    throttle_max: Time,
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| cfg.quota_throttle_max = throttle_max).await
}

async fn response_frame(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    timeout: Duration,
    context: &str,
) -> BytesMut {
    tokio::time::timeout(timeout, framed.next())
        .await
        .expect(context)
        .expect("a response frame")
        .expect("response decode")
}

async fn close_serve_loop(
    framed: Framed<TcpStream, KafkaCodec>,
    server: tokio::task::JoinHandle<()>,
) {
    drop(framed);
    server.await.expect("serve loop joins on client EOF");
}

async fn finish_muted_connection(
    mut framed: Framed<TcpStream, KafkaCodec>,
    server: tokio::task::JoinHandle<()>,
    handle: crate::BrokerHandle,
    answered_at: Instant,
    window: Time,
) {
    let _muted_for = next_response_after_mute(&mut framed, answered_at, window).await;
    close_serve_loop(framed, server).await;
    handle.shutdown().await;
}

async fn request_quota_connection(
    window: Time,
) -> (
    crate::BrokerHandle,
    tempfile::TempDir,
    tokio::task::JoinHandle<()>,
    Framed<TcpStream, KafkaCodec>,
) {
    let (handle, dir) =
        broker_with_anonymous_quotas(window, &[("request_percentage", 0.0001)]).await;
    let (server, framed) = connect_to_serve_loop(&handle).await;
    (handle, dir, server, framed)
}

async fn response_before_mute(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    sent_at: Instant,
    window: Time,
    context: &str,
) -> (BytesMut, Instant) {
    let first = response_frame(framed, CLIENT_TIMEOUT, context).await;
    let answered_at = Instant::now();
    check!(response_correlation_id(&first) == 1);
    check!(sent_at.elapsed() < window.to_std());
    (first, answered_at)
}

async fn next_response_after_mute(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    answered_at: Instant,
    window: Time,
) -> Duration {
    send_api_versions(framed, 2).await;
    let (second, muted_for) = read_after_mute(framed, answered_at).await;
    check!(response_correlation_id(&second) == 2);
    // The mute began when the first response was written, marginally before the client's read.
    check!(muted_for >= window.to_std().saturating_sub(SLACK));
    muted_for
}

fn assert_reported_window(throttle_time_ms: i32, expected: Time) {
    let reported = millis(u32::try_from(throttle_time_ms).expect("a window"));
    check!(reported == expected);
}
