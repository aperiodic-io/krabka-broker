//! Unit tests for the dispatch module root: request peeking, the flexible
//! header metadata the KIP-853 voter RPCs need, and an end-to-end drive of the
//! serve loop over a real socket. [`throttle_mute`] holds the KIP-219 tests,
//! which drive the same loop against a seeded client quota.

use assert2::{assert, check};
use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use tokio::net::TcpStream;

use super::{test_support::DEFAULT_MAX_FRAME_BYTES, *};
use crate::{codes, network::test_support::RequestFrameSetup};

mod throttle_mute;

use crate::network::test_support::request_frame;

#[test]
fn peek_api_key_reads_first_two_bytes_big_endian() {
    // api_key=18, version=3, corr_id=1 — only first 2 bytes are inspected.
    let mut buf = BytesMut::new();
    buf.put_i16(18);
    buf.put_i16(3);
    buf.put_i32(1);
    assert!(crate::network::request::peek_api_key(&buf).unwrap() == 18);
}

#[test]
fn peek_api_key_rejects_short_frame() {
    let buf = [0u8; 1];
    assert!(crate::network::request::peek_api_key(&buf).is_err());
}

/// KIP-853 RPCs (80/81/82) route through the registry path and are
/// flexible from v0. This guards the metadata used when parsing their
/// flexible request headers.
#[test]
fn raft_voter_rpcs_peek_and_flex_routing() {
    let registry = crate::handlers::registry::build_registry();

    for api_key in [80i16, 81, 82] {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(0); // version 0
        buf.put_i32(1); // corr_id
        assert!(crate::network::request::peek_api_key(&buf).unwrap() == api_key);
        assert!(
            registry.body_flexible(api_key, 0),
            "api_key {api_key} is flexible from v0"
        );
    }
}

/// The three KIP-853 controller-plane RPCs, `AddRaftVoter` 80,
/// `RemoveRaftVoter` 81, and `UpdateRaftVoter` 82, must reach their
/// registry handlers. This test drives each RPC over a real socket through
/// the whole serve loop and asserts that it reaches its handler. A
/// `DenyAll` authorizer stops every handler at the ACL gate with
/// `CLUSTER_AUTHORIZATION_FAILED` (31), which differs observably from the
/// connection close on the unsupported path.
#[tokio::test]
async fn raft_voter_registry_routes_to_real_handlers() {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request as add_req, add_raft_voter_response as add_resp,
            remove_raft_voter_request as rem_req, remove_raft_voter_response as rem_resp,
            update_raft_voter_request as upd_req, update_raft_voter_response as upd_resp,
        },
    };

    use crate::test_support::DenyAll;

    // Send a flexible (v2-header) request frame carrying `body` for
    // `api_key`/`version` and return the response body with its 5-byte
    // flexible header (corr_id + empty tagged-fields byte) stripped.
    async fn round_trip(
        framed: &mut Framed<TcpStream, crate::network::codec::KafkaCodec>,
        api_key: i16,
        version: i16,
        body: &[u8],
    ) -> Vec<u8> {
        let frame = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(api_key),
            api_version: krabka_ids::ApiVersion(version),
            correlation_id: crate::network::test_support::FrameCorrelationId(7),
            tagged: Some(&[0]),
            body,
            ..Default::default()
        });
        framed.send(frame.freeze()).await.expect("send request");
        let resp = framed
            .next()
            .await
            .expect("a response frame")
            .expect("response decode");
        resp[5..].to_vec()
    }

    fn encode_default<T: Encode + Default>(version: i16) -> BytesMut {
        let mut body = BytesMut::new();
        T::default().encode(&mut body, version).expect("encode");
        body
    }

    let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.authorizer = std::sync::Arc::new(DenyAll);
    })
    .await;
    let broker = handle.broker_arc_for_test();

    let (server, mut framed) = crate::network::dispatch::test_support::plaintext_loop(broker).await;

    let add_body = encode_default::<add_req::AddRaftVoterRequest>(add_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 80, add_req::MAX_VERSION, &add_body).await;
    let add = add_resp::AddRaftVoterResponse::decode(&mut &raw[..], add_resp::MAX_VERSION)
        .expect("decode AddRaftVoterResponse");

    let rem_body = encode_default::<rem_req::RemoveRaftVoterRequest>(rem_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 81, rem_req::MAX_VERSION, &rem_body).await;
    let rem = rem_resp::RemoveRaftVoterResponse::decode(&mut &raw[..], rem_resp::MAX_VERSION)
        .expect("decode RemoveRaftVoterResponse");

    let upd_body = encode_default::<upd_req::UpdateRaftVoterRequest>(upd_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 82, upd_req::MAX_VERSION, &upd_body).await;
    let upd = upd_resp::UpdateRaftVoterResponse::decode(&mut &raw[..], upd_resp::MAX_VERSION)
        .expect("decode UpdateRaftVoterResponse");

    // Each real handler denies at the ACL gate; the fall-through path would
    // instead close the connection.
    check!(add.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(rem.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(upd.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

/// #683: every key in `INTER_BROKER_ONLY_APIS` is scoped exactly the way
/// Kafka scopes a `"listeners": ["controller"]` request schema -- closed, no
/// response frame -- on any listener a client can reach. Table-driven over
/// every key, each sent at its dispatched maximum version; the body content
/// does not matter, because the scope check runs before any body is decoded.
#[tokio::test]
async fn inter_broker_only_apis_close_the_connection_on_a_client_listener() {
    let registry = crate::handlers::registry::build_registry();
    let dispatched = crate::api_catalog::dispatched_apis();

    for &api_key in crate::api_catalog::INTER_BROKER_ONLY_APIS {
        let version = dispatched
            .iter()
            .find(|a| a.api_key == api_key)
            .unwrap_or_else(|| panic!("api_catalog carries api_key {api_key}"))
            .max_version;
        let entry = registry
            .get(api_key)
            .unwrap_or_else(|| panic!("registry carries api_key {api_key}"));

        let (handle, _dir) = crate::test_support::start_broker_with(|_| {}).await;
        let broker = handle.broker_arc_for_test();

        let (addr, serve) = crate::network::dispatch::test_support::serve_loop(
            broker,
            "EXTERNAL",
            krabka_security::ListenerProtocol::Plaintext,
            None,
            None,
            1,
        )
        .await;
        let server = tokio::spawn(serve);

        let mut framed = test_support::connect_framed(addr, "connect").await;
        let frame = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(api_key),
            api_version: krabka_ids::ApiVersion(version),
            correlation_id: crate::network::test_support::FrameCorrelationId(7),
            tagged: entry.body_flexible(version).then_some(&[0][..]),
            ..Default::default()
        });
        framed.send(frame.freeze()).await.expect("send request");
        check!(
            framed.next().await.is_none(),
            "api_key {api_key} on a client listener must carry no response frame"
        );
        server
            .await
            .expect("serve loop joins after closing on a scoped api_key");
        handle.shutdown().await;
    }
}

/// What the serve loop does with one request frame.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// The connection closes with no response frame.
    Closed,
    /// An `ApiVersions` v0 body carrying `UNSUPPORTED_VERSION` and the one
    /// `ApiVersions` range.
    ApiVersionsV0(krabka_protocol::owned::api_versions_response::ApiVersionsResponse),
}

/// Sends each `(api_key, version)` frame on its own connection to a broker
/// started from `cfg` and returns what the serve loop did with it, and the
/// broker's metrics once every connection is done.
async fn drive_one_frame_per_connection(
    cfg: crate::config::BrokerConfig,
    requests: &[(i16, i16)],
) -> (Vec<Outcome>, crate::metrics::BrokerMetrics) {
    use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};

    let handle = Broker::start(cfg).await.expect("start broker");
    let broker = handle.broker_arc_for_test();
    let metrics = broker.metrics.clone();
    let registry = crate::handlers::registry::build_registry();
    let connections = requests.len();

    let (addr, serve) = crate::network::dispatch::test_support::serve_loop(
        broker,
        "PLAINTEXT",
        krabka_security::ListenerProtocol::Plaintext,
        None,
        None,
        connections,
    )
    .await;
    let server = tokio::spawn(serve);

    let mut outcomes = Vec::with_capacity(connections);
    for (correlation_id, &(api_key, version)) in requests.iter().enumerate() {
        let correlation_id = i32::try_from(correlation_id).expect("correlation id");
        let flexible = registry
            .get(api_key)
            .is_some_and(|entry| entry.body_flexible(version));
        let mut framed = test_support::connect_framed(addr, "connect").await;
        let frame = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(api_key),
            api_version: krabka_ids::ApiVersion(version),
            correlation_id: crate::network::test_support::FrameCorrelationId(correlation_id),
            tagged: flexible.then_some(&[0][..]),
            ..Default::default()
        });
        framed.send(frame.freeze()).await.expect("send request");

        outcomes.push(match framed.next().await {
            None => Outcome::Closed,
            Some(response) => {
                let response = response.expect("response frame decode");
                check!(
                    i32::from_be_bytes(response[..4].try_into().expect("correlation id"))
                        == correlation_id
                );
                // `ApiVersions`' response header is always v0: the
                // correlation id alone.
                Outcome::ApiVersionsV0(
                    ApiVersionsResponse::decode(&mut &response[4..], 0)
                        .expect("v0 ApiVersions body"),
                )
            }
        });
    }

    server
        .await
        .expect("serve loop joins after every connection");
    handle.shutdown().await;
    (outcomes, metrics)
}

/// #844: Kafka's `Processor.parseRequestHeader` throws
/// `UnsupportedVersionException` for a version outside an API's range and
/// `SocketServer` closes the channel on it. `ApiVersions` is the one
/// exception (`ApiKeys.isVersionEnabled`): `KafkaApis` answers it with a v0
/// `UNSUPPORTED_VERSION` body. #842: that body carries exactly one entry, the
/// `ApiVersions` range, as `ApiVersionsRequest.getErrorResponse` builds it,
/// whatever listener it arrives on. Table-driven over every dispatched API,
/// one connection per row, at the version above its maximum and at the
/// version below its minimum; an unknown api key closes the connection too.
#[tokio::test]
async fn unsupported_versions_close_the_connection_except_api_versions() {
    use krabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};

    let api_versions_v0 = ApiVersionsResponse {
        error_code: codes::UNSUPPORTED_VERSION,
        api_keys: vec![ApiVersion {
            api_key: API_VERSIONS_KEY,
            min_version: 0,
            max_version: krabka_raft::api_versions_max_version(
                crate::api_catalog::UnstableApiVersions::Disabled,
            ),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut cases: Vec<(i16, i16, Outcome)> = Vec::new();
    for api in crate::api_catalog::dispatched_apis() {
        let outcome = || {
            if api.api_key == API_VERSIONS_KEY {
                Outcome::ApiVersionsV0(api_versions_v0.clone())
            } else {
                Outcome::Closed
            }
        };
        let above = api
            .max_version
            .checked_add(1)
            .expect("maximum API version has a successor");
        cases.push((api.api_key, above, outcome()));
        cases.push((api.api_key, api.min_version - 1, outcome()));
    }
    cases.push((i16::MAX, 0, Outcome::Closed));

    let (_dir, cfg) = crate::network::test_support::broker_config();
    let requests: Vec<(i16, i16)> = cases
        .iter()
        .map(|(key, version, _)| (*key, *version))
        .collect();
    let (outcomes, metrics) = drive_one_frame_per_connection(cfg, &requests).await;
    for ((api_key, version, expected), actual) in cases.into_iter().zip(outcomes) {
        check!(actual == expected, "api_key {api_key} at version {version}");
    }

    let unknown = crate::metrics::ApiKeyLabel {
        api_key: crate::metrics::UNKNOWN_LABEL.into(),
    };
    check!(metrics.api_requests.get_or_create(&unknown).get() == 1);
}

/// #646, #784: under Kafka's `unstable.api.versions.enable` and krabka's
/// `legacy_request_versions_enable`, both off by default, the broker closes a
/// connection that sends a version or an api key Kafka 4.3.1 does not serve,
/// as `Processor.parseRequestHeader` does (`InvalidRequestException`, no
/// response): `InitProducerId` v6, trunk's `TxnOffsetCommit` v6, streams v1,
/// api keys 93 and 94, and the pre-4.0 `Produce`, `Fetch` and `ListOffsets`
/// versions. `ApiVersions` v5 is answered `UNSUPPORTED_VERSION` with the v0-v4
/// range instead. Turning the matching switch on dispatches the request. A
/// dispatched request with an empty body fails to decode and closes as well,
/// so the rows tell "refused before dispatch" from "dispatched" by the
/// unsupported-request metric.
#[tokio::test]
async fn a_gated_version_is_refused_unless_its_switch_is_on() {
    use crate::api_catalog::{
        LegacyRequestVersions::{self, Disabled as NoLegacy, Enabled as Legacy},
        UnstableApiVersions::{self, Disabled as Strict, Enabled as Trunk},
    };

    let cases: &[(
        i16,
        &str,
        i16,
        UnstableApiVersions,
        LegacyRequestVersions,
        bool,
    )] = &[
        (22, "InitProducerId", 6, Strict, NoLegacy, true),
        (22, "InitProducerId", 5, Strict, NoLegacy, false),
        (22, "InitProducerId", 6, Trunk, NoLegacy, false),
        (28, "TxnOffsetCommit", 6, Strict, NoLegacy, true),
        (28, "TxnOffsetCommit", 6, Trunk, NoLegacy, false),
        (88, "StreamsGroupHeartbeat", 1, Strict, NoLegacy, true),
        (88, "StreamsGroupHeartbeat", 1, Trunk, NoLegacy, false),
        (89, "StreamsGroupDescribe", 1, Strict, NoLegacy, true),
        (
            93,
            "StreamsGroupTopologyDescriptionUpdate",
            0,
            Strict,
            NoLegacy,
            true,
        ),
        (
            93,
            "StreamsGroupTopologyDescriptionUpdate",
            0,
            Trunk,
            NoLegacy,
            false,
        ),
        (94, "UnregisterController", 0, Strict, NoLegacy, true),
        (94, "UnregisterController", 0, Trunk, NoLegacy, false),
        (0, "Produce", 2, Strict, NoLegacy, true),
        (0, "Produce", 0, Trunk, NoLegacy, true),
        (0, "Produce", 2, Strict, Legacy, false),
        (1, "Fetch", 3, Strict, NoLegacy, true),
        (1, "Fetch", 3, Strict, Legacy, false),
        (2, "ListOffsets", 0, Strict, NoLegacy, true),
        (2, "ListOffsets", 0, Strict, Legacy, false),
    ];
    for &(api_key, name, frame_version, unstable, legacy, refused) in cases {
        let (_dir, mut cfg) = crate::network::test_support::broker_config();
        cfg.features.unstable_api_versions = unstable;
        cfg.features.legacy_request_versions = legacy;
        let (outcomes, metrics) =
            drive_one_frame_per_connection(cfg, &[(api_key, frame_version)]).await;
        let label = format!("{name} v{frame_version} {unstable:?} {legacy:?}");
        check!(outcomes == vec![Outcome::Closed], "{label}");
        let metric = crate::metrics::ApiKeyLabel {
            api_key: name.into(),
        };
        check!(
            (metrics
                .unsupported_api_requests
                .get_or_create(&metric)
                .get()
                == 1)
                == refused,
            "{label}"
        );
    }
}

/// #784: `ApiVersions` v5 is Kafka trunk's KIP-1242 version. With
/// `unstable.api.versions.enable` off the broker answers it as Kafka 4.3.1
/// answers a version it does not know: `UNSUPPORTED_VERSION` in a v0 body
/// naming v0-v4.
#[tokio::test]
async fn api_versions_v5_is_unsupported_unless_unstable_api_versions_are_enabled() {
    use krabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};

    let (_dir, cfg) = crate::network::test_support::broker_config();
    let (outcomes, _) = drive_one_frame_per_connection(cfg, &[(API_VERSIONS_KEY, 5)]).await;
    check!(
        outcomes
            == vec![Outcome::ApiVersionsV0(ApiVersionsResponse {
                error_code: codes::UNSUPPORTED_VERSION,
                api_keys: vec![ApiVersion {
                    api_key: API_VERSIONS_KEY,
                    min_version: 0,
                    max_version: 4,
                    ..Default::default()
                }],
                ..Default::default()
            })]
    );
}

/// Boots a broker, serves exactly one connection on a listener of `protocol`
/// offering `mechanisms`, sends `requests` down it in order, and returns once
/// the serve loop has exited. What the loop recorded is the return value.
async fn serve_frames(
    protocol: krabka_security::ListenerProtocol,
    mechanisms: Option<Vec<krabka_security::SaslMechanism>>,
    requests: Vec<bytes::Bytes>,
) -> crate::metrics::BrokerMetrics {
    let (handle, _dir) = crate::test_support::start_broker_with(|_| {}).await;
    let broker = handle.broker_arc_for_test();
    let metrics = broker.metrics.clone();

    let (addr, serve) = crate::network::dispatch::test_support::serve_loop(
        broker, "TESTS", protocol, mechanisms, None, 1,
    )
    .await;
    let server = tokio::spawn(serve);

    let mut framed = test_support::connect_framed(addr, "connect").await;
    for request in requests {
        framed.send(request).await.expect("send frame");
    }
    server
        .await
        .expect("serve loop joins once it closes the connection");
    drop(framed);
    handle.shutdown().await;
    metrics
}

/// Every `connection_closes` series, in the enum's own order, so a test can
/// compare the whole family rather than one label of it.
fn close_counts(metrics: &crate::metrics::BrokerMetrics) -> Vec<(&'static str, u64)> {
    crate::metrics::ConnectionCloseReason::ALL
        .into_iter()
        .map(|reason| {
            (
                reason.as_str(),
                metrics
                    .connection_closes
                    .get_or_create(&crate::metrics::ConnectionCloseReasonLabel { reason })
                    .get(),
            )
        })
        .collect()
}

fn active_closes(metrics: &crate::metrics::BrokerMetrics) -> Vec<(&'static str, u64)> {
    close_counts(metrics)
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .collect()
}

/// The family with no series moved, which is what every other scenario's
/// expectation is a single edit away from.
fn no_closes() -> Vec<(&'static str, u64)> {
    vec![
        ("idle", 0),
        ("sasl_session_expired", 0),
        ("decode_error", 0),
        ("peer_closed", 0),
        ("max_connections", 0),
        ("max_connections_per_ip", 0),
    ]
}

/// A frame the broker cannot read as a request closes the connection, and the
/// close is counted under the same reason as bytes the codec itself refused:
/// the peer sent something that is not a Kafka request. Uncounted, the
/// connection would leave `active_connections` with nothing anywhere saying
/// why.
#[tokio::test]
async fn a_frame_that_is_not_a_request_closes_the_connection_as_a_decode_error() {
    // One byte: enough to be a frame, not enough to peek an api_key out of.
    let metrics = serve_frames(
        krabka_security::ListenerProtocol::Plaintext,
        None,
        vec![bytes::Bytes::from_static(&[0x00])],
    )
    .await;

    assert!(
        close_counts(&metrics)
            == vec![
                ("idle", 0),
                ("sasl_session_expired", 0),
                ("decode_error", 1),
                ("peer_closed", 0),
                ("max_connections", 0),
                ("max_connections_per_ip", 0),
            ]
    );
}

/// A request the pre-auth gate does not allow closes the connection with no
/// response, and that close is an authentication failure counted under the
/// mechanism the peer negotiated: the `Unknown` sentinel when no
/// `SaslHandshake` ran, and the handshake's own mechanism when one did. It is
/// not a `connection_closes` reason, because that family counts the
/// connections that ended on their own.
#[tokio::test]
async fn a_gated_pre_auth_request_counts_a_failed_authentication_under_its_mechanism() {
    // Produce (api_key 0) v0: a well-formed request, and not one of the three
    // api_keys an unauthenticated connection may send.
    let produce = || {
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(0),
            ..Default::default()
        })
        .freeze()
    };
    // SaslHandshake (api_key 17) v1 for PLAIN. The body is one non-flexible
    // STRING: an i16 length and the mechanism name.
    let handshake = || {
        let mut body = BytesMut::new();
        body.put_i16(5);
        body.put_slice(b"PLAIN");
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(17),
            api_version: krabka_ids::ApiVersion(1),
            body: &body,
            ..Default::default()
        })
        .freeze()
    };

    let cases = [
        (
            "no handshake",
            vec![produce()],
            crate::metrics::UNKNOWN_LABEL,
        ),
        (
            "after a PLAIN handshake",
            vec![handshake(), produce()],
            "PLAIN",
        ),
    ];
    for (case, requests, mechanism) in cases {
        let metrics = serve_frames(
            krabka_security::ListenerProtocol::SaslPlaintext,
            Some(vec![krabka_security::SaslMechanism::Plain]),
            requests,
        )
        .await;

        let label = crate::metrics::SaslMechanismLabel {
            mechanism: mechanism.to_string(),
        };
        check!(
            metrics.failed_authentication.get_or_create(&label).get() == 1,
            "{case}"
        );
        check!(
            metrics
                .successful_authentication
                .get_or_create(&label)
                .get()
                == 0,
            "{case}"
        );
        // The gate rejects the frame before dispatch, so no Produce was served.
        check!(
            metrics
                .api_requests
                .get_or_create(&crate::metrics::ApiKeyLabel {
                    api_key: "Produce".to_string(),
                })
                .get()
                == 0,
            "{case}"
        );
        check!(close_counts(&metrics) == no_closes(), "{case}");
    }
}

/// Refuses every request of the principal `CN=bad`, and allows every other
/// principal. It stands for a DENY ACL on the certificate principal next to an
/// ALLOW ACL on `User:*`.
#[derive(Debug)]
struct DenyCertificatePrincipal;

/// The certificate principal that [`DenyCertificatePrincipal`] refuses.
const DENIED_CERTIFICATE_PRINCIPAL: &str = "CN=bad";

krabka_macros::test_authorizer!(DenyCertificatePrincipal, request; {
    if request.principal.name == DENIED_CERTIFICATE_PRINCIPAL {
        crate::authorizer::AuthorizationResult::Deny
    } else {
        crate::authorizer::AuthorizationResult::Allow
    }
});

/// Encodes one request frame: the request header and the encoded `body`.
#[derive(Clone, Copy, Default)]
enum RequestHeader {
    #[default]
    Legacy,
    Flexible,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct EncodedFrameSetup {
    #[default(krabka_ids::ApiKey(18))]
    api_key: krabka_ids::ApiKey,
    api_version: krabka_ids::ApiVersion,
    #[default(crate::network::test_support::FrameCorrelationId(1))]
    correlation_id: crate::network::test_support::FrameCorrelationId,
    header: RequestHeader,
}

fn encoded_request_frame<T: krabka_protocol::Encode>(
    body: &T,
    setup: EncodedFrameSetup,
) -> bytes::Bytes {
    let mut encoded = BytesMut::with_capacity(body.encoded_len(setup.api_version.0));
    body.encode(&mut encoded, setup.api_version.0)
        .expect("encode request body");
    request_frame(RequestFrameSetup {
        api_key: setup.api_key,
        api_version: setup.api_version,
        correlation_id: setup.correlation_id,
        tagged: matches!(setup.header, RequestHeader::Flexible).then_some(&[0][..]),
        body: &encoded,
        ..Default::default()
    })
    .freeze()
}

/// The response of one SASL frame that a non-SASL listener received.
#[derive(Debug, PartialEq)]
enum SaslAnswer {
    Handshake(krabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse),
    Authenticate(krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse),
}

/// A `PLAINTEXT` or `SSL` listener decides the principal when it accepts the
/// connection, and no SASL exchange can change it (#761).
///
/// Kafka's `KafkaApis.handleSaslHandshakeRequest` answers `ILLEGAL_SASL_STATE`
/// with no mechanisms, and `KafkaApis.handleSaslAuthenticateRequest` answers
/// `ILLEGAL_SASL_STATE` with a fixed message. The connection stays open, and
/// its next request runs under the same principal. On the `SSL` rows the
/// certificate principal is refused and `ANONYMOUS` is allowed, so a
/// `Metadata` row with `TOPIC_AUTHORIZATION_FAILED` proves that the principal
/// did not change.
#[tokio::test]
async fn a_sasl_frame_on_a_non_sasl_listener_keeps_the_principal() {
    use krabka_protocol::{
        Decode as _,
        owned::{
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
    };

    const TOPIC: &str = "no-such-topic";
    const HANDSHAKE: i16 = 17;
    const AUTHENTICATE: i16 = 36;
    const METADATA: i16 = 3;

    let handshake = |mechanism: &str| {
        encoded_request_frame(
            &SaslHandshakeRequest {
                mechanism: mechanism.to_string(),
                ..Default::default()
            },
            EncodedFrameSetup {
                api_key: krabka_ids::ApiKey(HANDSHAKE),
                api_version: krabka_ids::ApiVersion(1),
                ..Default::default()
            },
        )
    };
    let authenticate = || {
        encoded_request_frame(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from_static(b"\0alice\0wonderland"),
                ..Default::default()
            },
            EncodedFrameSetup {
                api_key: krabka_ids::ApiKey(AUTHENTICATE),
                api_version: krabka_ids::ApiVersion(2),
                header: RequestHeader::Flexible,
                ..Default::default()
            },
        )
    };
    let refused_handshake = SaslAnswer::Handshake(SaslHandshakeResponse {
        error_code: codes::ILLEGAL_SASL_STATE,
        mechanisms: Vec::new(),
        ..Default::default()
    });
    let refused_authenticate = SaslAnswer::Authenticate(SaslAuthenticateResponse {
        error_code: codes::ILLEGAL_SASL_STATE,
        error_message: Some(
            "SaslAuthenticate request received after successful authentication".to_string(),
        ),
        ..Default::default()
    });
    let certificate = krabka_security::Principal {
        name: DENIED_CERTIFICATE_PRINCIPAL.to_string(),
        auth_method: krabka_security::AuthMethod::MTls,
        groups: vec![],
    };

    let cases = [
        (
            "SSL, SaslHandshake PLAIN",
            krabka_security::ListenerProtocol::Ssl,
            Some(certificate.clone()),
            handshake("PLAIN"),
            &refused_handshake,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        (
            "SSL, SaslHandshake SCRAM-SHA-512",
            krabka_security::ListenerProtocol::Ssl,
            Some(certificate.clone()),
            handshake("SCRAM-SHA-512"),
            &refused_handshake,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        (
            "SSL, SaslAuthenticate",
            krabka_security::ListenerProtocol::Ssl,
            Some(certificate.clone()),
            authenticate(),
            &refused_authenticate,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        (
            "PLAINTEXT, SaslHandshake PLAIN",
            krabka_security::ListenerProtocol::Plaintext,
            None,
            handshake("PLAIN"),
            &refused_handshake,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        ),
        (
            "PLAINTEXT, SaslAuthenticate",
            krabka_security::ListenerProtocol::Plaintext,
            None,
            authenticate(),
            &refused_authenticate,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        ),
    ];

    let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.authorizer = std::sync::Arc::new(DenyCertificatePrincipal);
        cfg.plain_credentials
            .insert("alice".to_string(), "wonderland".to_string());
    })
    .await;

    for (case, protocol, mtls_principal, sasl_frame, expected_answer, expected_topic_error) in cases
    {
        let broker = handle.broker_arc_for_test();
        let (addr, serve) = crate::network::dispatch::test_support::serve_loop(
            broker,
            "TESTS",
            protocol,
            None,
            mtls_principal,
            1,
        )
        .await;
        let server = tokio::spawn(serve);
        let mut framed = test_support::connect_framed(addr, "connect").await;

        let sasl_key = i16::from_be_bytes([sasl_frame[0], sasl_frame[1]]);
        framed.send(sasl_frame).await.expect("send SASL frame");
        let response = framed
            .next()
            .await
            .expect("a SASL response, and the connection stays open")
            .expect("SASL response frame");
        let answer = if sasl_key == HANDSHAKE {
            let body = &response[crate::network::response_header_len(HANDSHAKE, false)..];
            SaslAnswer::Handshake(
                SaslHandshakeResponse::decode(&mut &body[..], 1).expect("decode SaslHandshake"),
            )
        } else {
            let body = &response[crate::network::response_header_len(AUTHENTICATE, true)..];
            SaslAnswer::Authenticate(
                SaslAuthenticateResponse::decode(&mut &body[..], 2)
                    .expect("decode SaslAuthenticate"),
            )
        };
        check!(&answer == expected_answer, "{case}");

        framed
            .send(encoded_request_frame(
                &MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: Some(TOPIC.to_string()),
                        ..Default::default()
                    }]),
                    allow_auto_topic_creation: false,
                    ..Default::default()
                },
                EncodedFrameSetup {
                    api_key: krabka_ids::ApiKey(METADATA),
                    api_version: krabka_ids::ApiVersion(12),
                    correlation_id: crate::network::test_support::FrameCorrelationId(2),
                    header: RequestHeader::Flexible,
                },
            ))
            .await
            .expect("send Metadata");
        let response = framed
            .next()
            .await
            .expect("a Metadata response")
            .expect("Metadata response frame");
        let body = &response[crate::network::response_header_len(METADATA, true)..];
        let metadata = MetadataResponse::decode(&mut &body[..], 12).expect("decode Metadata");
        check!(
            metadata.topics
                == vec![MetadataResponseTopic {
                    error_code: expected_topic_error,
                    name: Some(TOPIC.to_string()),
                    topic_authorized_operations: i32::MIN,
                    ..Default::default()
                }],
            "{case}"
        );

        drop(framed);
        server
            .await
            .expect("serve loop joins after the client closes");
    }
    handle.shutdown().await;
}

/// The `queued.max.requests` / `queued.max.request.bytes` budgets of #412.
///
/// Every test here drives `serve_connection_stream` over a loopback socket,
/// which is what the loop is generic over: the sendfile sink a `Fetch`
/// response writes through is a socket, and an in-memory pipe is not one.
mod request_budget {
    use assert2::{assert, check};
    use futures_util::{SinkExt as _, StreamExt as _};

    use super::{KafkaCodec, RequestFrameSetup, active_closes, request_frame};
    use crate::broker::Broker;

    /// One connection served by the loop, and the client end already framed.
    struct Served {
        client: tokio_util::codec::Framed<tokio::net::TcpStream, KafkaCodec>,
        loop_task: tokio::task::JoinHandle<()>,
    }

    async fn serve(broker: &std::sync::Arc<Broker>) -> Served {
        let (loop_task, client) =
            crate::network::dispatch::test_support::plaintext_loop(std::sync::Arc::clone(broker))
                .await;
        Served { client, loop_task }
    }

    /// A byte budget that leaves the broker's own traffic room to move.
    const BUDGET_BYTES: usize = 64 * 1024;

    fn budget() -> krabka_units::ByteSize {
        krabka_units::bytes(u32::try_from(BUDGET_BYTES).expect("the budget fits a u32"))
    }

    /// An `ApiVersions` v0 request with an empty body: the smallest frame the
    /// loop answers without any authorization or metadata in the way.
    fn api_versions_frame(correlation_id: i32) -> bytes::Bytes {
        request_frame(RequestFrameSetup {
            correlation_id: crate::network::test_support::FrameCorrelationId(correlation_id),
            ..Default::default()
        })
        .freeze()
    }

    /// An `ApiVersions` request padded past [`BUDGET_BYTES`]. The body is trailing
    /// bytes the v0 decoder ignores, so what refuses the frame is the budget
    /// and not the decode.
    fn oversized_frame() -> bytes::Bytes {
        request_frame(RequestFrameSetup {
            body: &vec![0_u8; 2 * BUDGET_BYTES],
            ..Default::default()
        })
        .freeze()
    }

    use crate::test_support::start_broker_with as broker_with;

    async fn byte_budget_broker() -> (crate::BrokerHandle, tempfile::TempDir) {
        broker_with(|cfg| cfg.queued_max_request_bytes = Some(budget())).await
    }

    /// `queued.max.requests` bounds what the broker is *handling*, across every
    /// connection. With one permit, a second connection's frame waits for the
    /// first request to finish rather than being handled beside it.
    ///
    /// The permit the first request would hold is taken directly here, because
    /// nothing else can hold a handler open for as long as the assertion needs.
    #[tokio::test]
    async fn a_second_request_waits_for_the_capacity_the_first_holds() {
        let (handle, _dir) = broker_with(|cfg| cfg.queued_max_requests = 1).await;
        let broker = handle.broker_arc_for_test();

        let held = std::sync::Arc::clone(&broker.queued_requests_sem)
            .acquire_owned()
            .await
            .expect("the only permit");

        let mut served = serve(&broker).await;
        served
            .client
            .send(api_versions_frame(1))
            .await
            .expect("send while the budget is spent");

        // The frame is on the wire and the loop has read it; what it cannot do
        // is handle it. A response now would mean the budget bounds nothing.
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(200), served.client.next()).await;
        assert!(
            early.is_err(),
            "a request must not be served past the budget"
        );
        check!(broker.metrics.queued_requests.get() == 0);

        drop(held);
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(5), served.client.next())
                .await
                .expect("the freed permit admits the waiting request")
                .expect("a response frame")
                .expect("response decode");
        check!(i32::from_be_bytes(response[..4].try_into().expect("correlation id")) == 1);

        drop(served.client);
        served
            .loop_task
            .await
            .expect("serve loop joins on client EOF");
        // The guard gave the permit and the byte gauge back.
        check!(broker.queued_requests_sem.available_permits() == 1);
        check!(broker.metrics.queued_request_bytes.get() == 0);
        handle.shutdown().await;
    }

    /// A frame larger than the whole `queued.max.request.bytes` budget can
    /// never be admitted, however long the broker waits, so the connection
    /// ends the way one carrying bytes the broker cannot take does.
    ///
    /// The budget is big enough for the broker's own inter-broker traffic --
    /// a budget under one ordinary request would wedge the broker itself
    /// rather than test anything -- and the frame is deliberately bigger.
    #[tokio::test]
    async fn a_frame_over_the_whole_byte_budget_closes_the_connection() {
        let (handle, _dir) = byte_budget_broker().await;
        let broker = handle.broker_arc_for_test();

        let mut served = serve(&broker).await;
        let frame = oversized_frame();
        assert!(
            frame.len() > BUDGET_BYTES,
            "the frame has to exceed the whole budget"
        );
        served
            .client
            .send(frame)
            .await
            .expect("send an oversized frame");

        check!(
            served.client.next().await.is_none(),
            "the connection must close"
        );
        served
            .loop_task
            .await
            .expect("serve loop joins after the refusal");
        check!(active_closes(&broker.metrics) == vec![("decode_error", 1)]);
        handle.shutdown().await;
    }

    /// A frame that fits the budget is served, and the budget is whole again
    /// once the response is written.
    #[tokio::test]
    async fn a_frame_within_the_byte_budget_is_served_and_gives_the_bytes_back() {
        let (handle, _dir) = byte_budget_broker().await;
        let broker = handle.broker_arc_for_test();
        let broker_for_budget = std::sync::Arc::clone(&broker);
        let available = move || {
            broker_for_budget
                .queued_request_bytes
                .as_ref()
                .expect("the knob is on")
                .available()
        };

        let mut served = serve(&broker).await;
        served
            .client
            .send(api_versions_frame(9))
            .await
            .expect("send a frame inside the budget");
        let response = served
            .client
            .next()
            .await
            .expect("a response frame")
            .expect("response decode");
        check!(i32::from_be_bytes(response[..4].try_into().expect("correlation id")) == 9);

        drop(served.client);
        served
            .loop_task
            .await
            .expect("serve loop joins on client EOF");
        // The broker's own connections draw on the same budget, so the wait
        // is for it to come back whole rather than for one instant of it.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while available() != BUDGET_BYTES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every permit comes back once the requests are answered");
        // The client hung up; the budget refused nothing.
        check!(active_closes(&broker.metrics) == vec![("peer_closed", 1)]);
        handle.shutdown().await;
    }
}

/// The level of each line the serve loop logs about a connection.
///
/// Kafka's `SocketServer` and `Selector` log a connection's setup and close at
/// DEBUG, and its `RequestChannel` logs each request at DEBUG on
/// `kafka.request.logger`. A failure keeps its WARN.
mod log_levels {
    use assert2::assert;
    use futures_util::{SinkExt as _, StreamExt as _};
    use tracing::{Instrument as _, Level};

    use super::{DEFAULT_MAX_FRAME_BYTES, RequestFrameSetup, request_frame};
    use crate::{broker::Broker, network::codec, test_support::LogCapture};

    // What the client end of the one connection does.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Peer {
        // Sends an `ApiVersions` v0 request, reads the response and hangs up.
        OneRequest,
        // Sends a one-byte frame, which is not a request.
        NotARequest,
        // Sends nothing, so the broker closes the connection when it is idle.
        Idle,
    }

    // Serves one connection to `peer`, and returns what the dispatch module
    // logged at DEBUG or above about that connection.
    async fn logged_serving(peer: Peer) -> Vec<(Level, String)> {
        let (_dir, mut cfg) = crate::network::test_support::broker_config();
        if matches!(peer, Peer::Idle) {
            cfg.connections_max_idle = Some(krabka_units::millis(100));
        }
        let handle = Broker::start(cfg).await.expect("start broker");
        let broker = handle.broker_arc_for_test();

        let capture = LogCapture::default();
        let _capturing = tracing::dispatcher::set_default(&capture.dispatch());
        let (addr, serve) = crate::network::dispatch::test_support::serve_loop(
            broker,
            "PLAINTEXT",
            krabka_security::ListenerProtocol::Plaintext,
            None,
            None,
            1,
        )
        .await;
        let loop_task = tokio::spawn(serve.instrument(LogCapture::span()));
        let client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to the serve loop");
        let mut client = codec::frame(client, DEFAULT_MAX_FRAME_BYTES);
        let open_client = match peer {
            Peer::OneRequest => {
                client
                    .send(request_frame(RequestFrameSetup::default()).freeze())
                    .await
                    .expect("send the request");
                client
                    .next()
                    .await
                    .expect("a response frame")
                    .expect("response decode");
                // The hang-up is what ends the serve loop.
                drop(client);
                None
            }
            Peer::NotARequest => {
                client
                    .send(bytes::Bytes::from_static(&[0x00]))
                    .await
                    .expect("send the frame");
                Some(client)
            }
            Peer::Idle => Some(client),
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), loop_task)
            .await
            .expect("the serve loop ends within 30s")
            .expect("the serve loop does not panic");
        drop(open_client);
        handle.shutdown().await;
        capture
            .events()
            .into_iter()
            .filter(|event| {
                event.target.starts_with("krabka_broker::network::dispatch")
                    && event.level <= Level::DEBUG
            })
            .map(|event| (event.level, event.message))
            .collect()
    }

    #[tokio::test]
    async fn the_serve_loop_logs_connections_and_requests_at_debug() {
        let opened = (Level::DEBUG, "connection opened");
        let closed = (Level::DEBUG, "connection closed");
        let cases = [
            (
                Peer::OneRequest,
                vec![opened, (Level::DEBUG, "dispatching request"), closed],
            ),
            (
                Peer::NotARequest,
                vec![
                    opened,
                    (Level::WARN, "frame too small to peek api_key, closing"),
                    closed,
                ],
            ),
            (
                Peer::Idle,
                vec![
                    opened,
                    (
                        Level::DEBUG,
                        "connection idle past connections.max.idle.ms, closing",
                    ),
                    closed,
                ],
            ),
        ];
        let mut logged_rows = Vec::new();
        let mut expected_rows = Vec::new();
        for (peer, expected) in cases {
            logged_rows.push((peer, logged_serving(peer).await));
            expected_rows.push((
                peer,
                expected
                    .into_iter()
                    .map(|(level, message)| (level, message.to_string()))
                    .collect::<Vec<_>>(),
            ));
        }
        assert!(logged_rows == expected_rows);
    }
}

/// `socket.request.max.bytes` bounds a request. Kafka's `NetworkReceive`
/// enforces it on the receive side, and `Processor.sendResponse` writes a
/// response of any size.
mod request_limit {
    use assert2::assert;
    use futures_util::{SinkExt as _, StreamExt as _};
    use krabka_protocol::{
        Encode as _,
        owned::describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    };

    use super::{RequestFrameSetup, request_frame};
    use crate::broker::Broker;

    /// The limit of the broker under test: well over its own requests to
    /// itself, and well under the answer to a documentation-bearing
    /// `DescribeConfigs`.
    const LIMIT: usize = 2048;

    /// A `DescribeConfigs` v4 for this broker's configs, with their documentation.
    /// The request is a few dozen bytes and the answer many kilobytes.
    fn describe_broker_configs(correlation_id: i32) -> bytes::BytesMut {
        let mut body = bytes::BytesMut::new();
        DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: 4,
                resource_name: "1".to_string(),
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: true,
            ..Default::default()
        }
        .encode(&mut body, 4)
        .expect("encode DescribeConfigs");
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(32),
            api_version: krabka_ids::ApiVersion(4),
            correlation_id: crate::network::test_support::FrameCorrelationId(correlation_id),
            tagged: Some(&[0]),
            body: &body,
            ..Default::default()
        })
    }

    /// Serves one connection on a broker whose `socket.request.max.bytes` is
    /// [`LIMIT`], sends `request`, and returns the response frame, or `None`
    /// when the broker closed the connection instead.
    async fn answer_under_limit(request: bytes::BytesMut) -> Option<bytes::BytesMut> {
        let (_dir, mut cfg) = crate::network::test_support::broker_config();
        cfg.socket_request_max = krabka_units::bytes(u32::try_from(LIMIT).expect("limit"));
        let handle = Broker::start(cfg).await.expect("start broker");
        let broker = handle.broker_arc_for_test();

        // The client's own codec is not the broker's limit.
        let (loop_task, mut client) =
            crate::network::dispatch::test_support::plaintext_loop(broker).await;
        client
            .send(request.freeze())
            .await
            .expect("send the request");
        let answer = client.next().await.and_then(Result::ok);
        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(30), loop_task)
            .await
            .expect("the serve loop ends within 30s")
            .expect("the serve loop does not panic");
        handle.shutdown().await;
        answer
    }

    #[tokio::test]
    async fn a_response_over_the_limit_is_written_and_a_request_over_it_is_refused() {
        let small_request = describe_broker_configs(7);
        assert!(small_request.len() - 4 <= LIMIT);
        // An ApiVersions v0 request whose client id alone is over the limit.
        let oversize_request = request_frame(RequestFrameSetup {
            correlation_id: crate::network::test_support::FrameCorrelationId(8),
            client_id: Some(&[b'c'; LIMIT + 1]),
            ..Default::default()
        });
        assert!(oversize_request.len() - 4 > LIMIT);

        let answer = answer_under_limit(small_request)
            .await
            .expect("a request under the limit is answered");
        assert!(answer.len() > LIMIT, "the answer is {} bytes", answer.len());
        assert!(answer[..4] == 7_i32.to_be_bytes());

        assert!(
            answer_under_limit(oversize_request).await.is_none(),
            "a request over the limit closes the connection"
        );
    }
}
