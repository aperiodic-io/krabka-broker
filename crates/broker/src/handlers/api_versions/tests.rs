//! Tests for the `ApiVersions` entry point in [`super`]: the api-key table it
//! advertises, the KIP-511 client-information rejection, and the KIP-1242
//! routing checks.
//!
//! Most of them drive a live broker, so they are kept out of the module root.
//! The feature-row and name-validation tests live beside the code they cover,
//! in [`super::feature_keys`] and [`super::client_info`].

use {
    super::*,
    crate::{
        SslPrincipalMapper,
        api_catalog::{
            ClientMetricsReceiver, ListenerKind, UnstableApiVersions, VersionGates, supported_apis,
        },
        broker::{Broker, BrokerHandle},
        cluster_id::encode,
        codes,
        config::ListenerSpec,
        handlers::RequestContext,
        test_support::{
            self as broker_fixture_support, decode_response as decode_wire_response, peer,
            principal, request_context, start_broker_with,
        },
    },
    assert2::{assert, check},
    bytes::{Bytes, BytesMut},
    krabka_metadata::{FeatureLevelRecord, MetadataRecord},
    krabka_protocol::{
        Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
    },
    krabka_security::Principal,
    std::{net::SocketAddr, sync::Arc},
};

const API_VERSIONS_V3: i16 = 3;
const API_VERSIONS_V5: i16 = 5;

fn request(name: &str, version: &str) -> Bytes {
    let req = ApiVersionsRequest {
        client_software_name: name.into(),
        client_software_version: version.into(),
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(req.encoded_len(API_VERSIONS_V3));
    req.encode(&mut buf, API_VERSIONS_V3)
        .expect("encode ApiVersionsRequest");
    buf.freeze()
}

fn routing_request(cluster_id: Option<String>, node_id: i32) -> Bytes {
    let req = ApiVersionsRequest {
        client_software_name: "krabka-test".into(),
        client_software_version: "1.0.0".into(),
        cluster_id,
        node_id,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(req.encoded_len(API_VERSIONS_V5));
    req.encode(&mut buf, API_VERSIONS_V5)
        .expect("encode ApiVersionsRequest v5");
    buf.freeze()
}

fn decode_response(version: i16, bytes: &Bytes) -> ApiVersionsResponse {
    decode_wire_response(bytes, version)
}

/// The anonymous, plaintext context the dispatch loop builds for a handshake
/// on an unauthenticated connection. `handle` reads it to charge the KIP-124
/// request quota, which no broker in this module configures, so every response
/// below reports `throttle_time_ms = 0`.
fn anonymous_principal() -> krabka_security::Principal { principal("ANONYMOUS") }

/// Retain the broker directory and the anonymous handshake's borrowed resources.
async fn anonymous_handshake() -> (
    BrokerHandle,
    tempfile::TempDir,
    Arc<Broker>,
    Principal,
    SocketAddr,
) {
    let (handle, directory) = start_broker_with(|_| {}).await;
    let broker = handle.broker_arc_for_test();
    let principal = anonymous_principal();
    let peer = peer();
    (handle, directory, broker, principal, peer)
}

/// Metadata-driven handshakes submit their setup only after this controller leads.
async fn submit_handshake_metadata(
    broker: &Broker,
    records: impl FnOnce() -> Vec<MetadataRecord>,
    message: &str,
) {
    broker_fixture_support::wait_for_controller_leader(broker).await;
    broker
        .controller
        .submit_change(records())
        .await
        .expect(message);
}

macro_rules! api_version_wire {
    (($request:ident, $bytes:ident, $response:ident), $broker:ident, $context:ident) => {
        let $request = request("krabka-test", "1.0.0");
        let $bytes = handle(&$broker, API_VERSIONS_V3, &$request, &$context)
            .await
            .expect("ApiVersions handler");
        let $response = decode_response(API_VERSIONS_V3, &$bytes);
    };
}

/// The advertised rows whose ranges are a deliberate choice, pinned whole.
///
/// - Produce: min 0, as Kafka 4.x still advertises it
///   (`ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`, KAFKA-18659),
///   whichever minimum it serves.
/// - Fetch and `ListOffsets`: Kafka 4.x's 4 and 1 by default, 0 under
///   `legacy_request_versions_enable` for pre-4.0 clients (#784).
/// - `InitProducerId`: v6 is `latestVersionUnstable`, and `ApiVersions` v5 is
///   Kafka trunk's, so both are advertised only under
///   `unstable.api.versions.enable` (#646, #784).
#[test]
fn api_versions_advertises_the_deliberate_ranges() {
    use {
        crate::api_catalog::{LegacyRequestVersions, UnstableApiVersions, VersionGates},
        krabka_protocol::owned::api_versions_response::ApiVersion,
    };

    let row = |api_key, min_version, max_version| ApiVersion {
        api_key,
        min_version,
        max_version,
        ..Default::default()
    };
    let gates = |unstable, legacy| VersionGates { unstable, legacy };
    for (gates, expected) in [
        (
            gates(
                UnstableApiVersions::Disabled,
                LegacyRequestVersions::Disabled,
            ),
            vec![
                row(0, 0, 13),
                row(1, 4, 18),
                row(2, 1, 11),
                row(18, 0, 4),
                row(22, 0, 5),
            ],
        ),
        (
            gates(
                UnstableApiVersions::Enabled,
                LegacyRequestVersions::Disabled,
            ),
            vec![
                row(0, 0, 13),
                row(1, 4, 18),
                row(2, 1, 11),
                row(18, 0, 5),
                row(22, 0, 6),
            ],
        ),
        (
            gates(
                UnstableApiVersions::Disabled,
                LegacyRequestVersions::Enabled,
            ),
            vec![
                row(0, 0, 13),
                row(1, 0, 18),
                row(2, 0, 11),
                row(18, 0, 4),
                row(22, 0, 5),
            ],
        ),
    ] {
        let table = supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, gates);
        let pinned: Vec<ApiVersion> = table
            .into_iter()
            .filter(|api| [0, 1, 2, 18, 22].contains(&api.api_key))
            .collect();
        check!(pinned == expected, "{gates:?}");
    }
}

/// Every `ApiVersions` request version on every listener shape, driven
/// through the dispatch loop the way a client sends it (#842).
///
/// Kafka answers a version it does not serve with a v0 body carrying error 35
/// and exactly one entry, the `ApiVersions` range; every served version gets
/// error 0 and a strictly ascending key list, because Kafka iterates
/// `ApiKeys.apisForListener`, an `EnumSet` in id order. Table-driven over a
/// client listener, an inter-broker listener, and both client-telemetry
/// settings.
#[tokio::test]
async fn api_versions_answers_every_version_on_every_listener_shape() {
    use krabka_protocol::owned::api_versions_response::ApiVersion;

    let unsupported = ApiVersionsResponse {
        error_code: codes::UNSUPPORTED_VERSION,
        api_keys: vec![ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 4,
            ..Default::default()
        }],
        ..Default::default()
    };
    for (listener, telemetry) in [
        ("EXTERNAL", false),
        ("EXTERNAL", true),
        ("INTERNAL", false),
        ("INTERNAL", true),
    ] {
        let (broker_handle, _dir) = start_broker_with(|cfg| {
            let external = ListenerSpec {
                name: "EXTERNAL".to_string(),
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                advertised: "127.0.0.1:0".to_string(),
                protocol: krabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: SslPrincipalMapper::default(),
            };
            let internal = ListenerSpec {
                name: "INTERNAL".to_string(),
                bind_addr: "127.0.0.2:0".parse().unwrap(),
                advertised: "127.0.0.2:0".to_string(),
                ..external.clone()
            };
            cfg.listeners = vec![external, internal];
            cfg.inter_broker_listener_name = "INTERNAL".to_string();
            cfg.client_metrics_enable = telemetry;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = anonymous_principal();
        let peer = peer();
        let context = RequestContext::new(
            &principal,
            &peer,
            "krabka-test",
            "test-connection",
            false,
            listener,
        );
        let expected_keys = supported_apis(
            broker.config.listener_kind(listener),
            broker.config.client_metrics_receiver(),
            VersionGates::default(),
        );
        check!(
            expected_keys
                .windows(2)
                .all(|pair| pair[0].api_key < pair[1].api_key),
            "{listener} telemetry={telemetry}"
        );

        // The dispatch loop answers v5 `UNSUPPORTED_VERSION` before the
        // handler sees it while unstable api versions are off.
        for version in 0..=4 {
            let req = ApiVersionsRequest {
                client_software_name: "krabka-test".into(),
                client_software_version: "1.0.0".into(),
                ..Default::default()
            };
            let mut req_bytes = BytesMut::with_capacity(req.encoded_len(version));
            req.encode(&mut req_bytes, version).expect("encode");
            let bytes = handle(&broker, version, &req_bytes, &context)
                .await
                .expect("ApiVersions handler");
            let resp = decode_response(version, &bytes);
            check!(
                (resp.error_code, &resp.api_keys) == (codes::NONE, &expected_keys),
                "{listener} telemetry={telemetry} v{version}"
            );
        }
        let body = unsupported_version_response(UnstableApiVersions::Disabled)
            .expect("unsupported answer");
        check!(
            decode_response(0, &body) == unsupported,
            "{listener} telemetry={telemetry} v5"
        );

        broker_handle.shutdown().await;
    }
}

#[test]
fn api_versions_advertises_kip853_rpcs_and_describe_quorum_v2() {
    use krabka_protocol::owned;
    // UpdateRaftVoter (82) is inter-broker only, so the table that carries all
    // three KIP-853 RPCs is the inter-broker listener's.
    let table = supported_apis(
        ListenerKind::InterBroker,
        ClientMetricsReceiver::Absent,
        VersionGates::default(),
    );
    let by_key = |k: i16| table.iter().find(|v| v.api_key == k);

    for (key, max) in [
        (80i16, owned::add_raft_voter_request::MAX_VERSION),
        (81, owned::remove_raft_voter_request::MAX_VERSION),
        (82, owned::update_raft_voter_request::MAX_VERSION),
    ] {
        let v = by_key(key).unwrap_or_else(|| panic!("api_key {key} advertised"));
        assert!(v.min_version == 0);
        assert!(v.max_version == max, "api_key {key} max matches codegen");
    }

    // DescribeQuorum (55) max follows its schema const — now v2 (KIP-853
    // adds VoterDirectoryId + Nodes).
    let dq = by_key(55).expect("describe_quorum advertised");
    assert!(
        dq.max_version == owned::describe_quorum_request::MAX_VERSION,
        "DescribeQuorum max tracks the codegen const"
    );
    assert!(dq.max_version == 2, "DescribeQuorum is v2 after KIP-853");
}

#[tokio::test]
async fn handle_rejects_each_invalid_v3_client_info_field() {
    let (broker_handle, _dir, broker, principal, peer) = anonymous_handshake().await;
    let context = request_context(&principal, &peer, "krabka-test");

    for (name, version) in [("", "1.0.0"), ("krabka-test", "")] {
        let req = request(name, version);
        let bytes = handle(&broker, API_VERSIONS_V3, &req, &context)
            .await
            .expect("ApiVersions handler");
        let resp = decode_response(API_VERSIONS_V3, &bytes);
        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");
        assert!(resp.api_keys.is_empty(), "{resp:?}");
    }

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_legacy_request_without_client_info() {
    let (broker_handle, _dir, broker, principal, peer) = anonymous_handshake().await;
    let context = request_context(&principal, &peer, "krabka-test");
    let req = ApiVersionsRequest::default();
    let mut req_bytes = BytesMut::with_capacity(req.encoded_len(0));
    req.encode(&mut req_bytes, 0)
        .expect("encode legacy ApiVersionsRequest");

    let bytes = handle(&broker, 0, &req_bytes, &context)
        .await
        .expect("ApiVersions handler");
    let resp = decode_response(0, &bytes);

    assert!(resp.error_code == codes::NONE, "{resp:?}");
    assert!(!resp.api_keys.is_empty(), "{resp:?}");

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_accepts_valid_v3_and_surfaces_catalog_and_features() {
    let (broker_handle, _dir, broker, principal, peer) = anonymous_handshake().await;
    let context = request_context(&principal, &peer, "krabka-test");
    submit_handshake_metadata(
        &broker,
        || {
            vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 24,
            })]
        },
        "submit finalized feature",
    )
    .await;
    // #783: the epoch is the image's metadata offset, which every record
    // moves, not a count of feature records. Ten topic records after the last
    // feature record must move it.
    let records = (0..10)
        .map(|index| {
            MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: format!("epoch-{index}"),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })
        })
        .collect();
    let before_topics = broker.controller.current_metadata_offset();
    broker
        .controller
        .submit_change(records)
        .await
        .expect("submit topic records");
    let metadata_offset = broker.controller.current_metadata_offset();
    assert!(metadata_offset >= before_topics + 10);

    api_version_wire!((req, bytes, resp), broker, context);

    check!(resp.error_code == codes::NONE, "{resp:?}");
    // `request_context` arrives on `PLAINTEXT`, and a test broker leaves
    // `inter_broker_listener_name` at its `PLAINTEXT` default, so this is the
    // single-listener shape: the one listener carries client and inter-broker
    // traffic together (`ListenerKind::ClientAndInterBroker`) and, per #843,
    // withholds the control-plane keys the same as a pure client listener --
    // a client can reach it too.
    check!(
        resp.api_keys
            == crate::api_catalog::supported_apis(
                crate::api_catalog::ListenerKind::ClientAndInterBroker,
                crate::api_catalog::ClientMetricsReceiver::Absent,
                crate::api_catalog::VersionGates::default(),
            ),
        "{resp:?}"
    );
    check!(
        !resp
            .api_keys
            .iter()
            .any(|api| api.api_key == krabka_protocol::owned::alter_partition_request::API_KEY),
        "a client-reachable listener must not advertise AlterPartition: {resp:?}"
    );
    check!(!resp.supported_features.is_empty(), "{resp:?}");
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version supported");
    check!(mv.min_version == crate::features::METADATA_VERSION_MIN);
    // #784: 4.3.1's latest production level while
    // `unstable.feature.versions.enable` is off.
    check!(mv.max_version == crate::features::LATEST_PRODUCTION_METADATA_VERSION);
    check!(resp.finalized_features_epoch == metadata_offset);
    let finalized_mv = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert!(finalized_mv.max_version_level == 24, "{resp:?}");
    assert!(finalized_mv.min_version_level == 24, "{resp:?}");

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_applies_kip1242_routing_checks() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, principal, peer, context);
        boot [broker_fixture_support::start_broker_with(|cfg| cfg.broker_id = 42)];
        user ["ANONYMOUS"];
        client ["krabka-test"];
    };
    let raw_cluster_id = broker.controller.current_image().cluster_id();
    // A real KIP-1242 client learned this cluster id from a `Metadata` or
    // `DescribeCluster` response, which reports Kafka's base64 `Uuid` form
    // (#1082), and echoes that exact string back here.
    let cluster_id = encode(raw_cluster_id);
    let hyphenated_cluster_id = raw_cluster_id.to_string();
    let node_id = i32::try_from(broker.config.node_id.0).expect("node id fits Kafka wire");
    assert!(node_id != broker.config.broker_id);

    for (request_cluster_id, request_node_id, expected_error) in [
        (None, -1, codes::NONE),
        (Some(cluster_id.clone()), -1, codes::INVALID_REQUEST),
        (None, node_id, codes::INVALID_REQUEST),
        (Some(cluster_id.clone()), node_id, codes::NONE),
        // The hyphenated `java.util.UUID` form still matches too.
        (Some(hyphenated_cluster_id.clone()), node_id, codes::NONE),
        (
            Some("wrong-cluster".into()),
            node_id,
            codes::REBOOTSTRAP_REQUIRED,
        ),
        (
            Some(cluster_id.clone()),
            node_id + 1,
            codes::REBOOTSTRAP_REQUIRED,
        ),
    ] {
        let request = routing_request(request_cluster_id, request_node_id);
        let bytes = handle(&broker, API_VERSIONS_V5, &request, &context)
            .await
            .expect("ApiVersions v5 handler");
        let response = decode_response(API_VERSIONS_V5, &bytes);

        assert!(response.error_code == expected_error, "{response:?}");
        assert!(
            response.api_keys.is_empty() == (expected_error != codes::NONE),
            "{response:?}"
        );
    }

    broker_handle.shutdown().await;
}

/// KIP-219 on `ApiVersions`: the handler charges the KIP-124 request quota
/// itself, because the dispatch loop's leading-int32 patch cannot reach a
/// `ThrottleTimeMs` that sits behind the `ApiKeys` array.
///
/// `request_percentage = 0.0001` leaves the bucket a budget of about one
/// microsecond of handler time per second, so a handshake overruns it at once.
/// The delay has to appear in two places: on the response the client decodes,
/// and on the context, which is where the connection loop reads the window it
/// mutes for.
#[tokio::test]
async fn handle_reports_and_records_a_request_quota_throttle() {
    use krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};

    let (broker_handle, _dir, broker, principal, peer) = anonymous_handshake().await;
    let context = request_context(&principal, &peer, "krabka-test");
    submit_handshake_metadata(
        &broker,
        || {
            vec![MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: vec![QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some(principal.name.clone()),
                }],
                config_key: "request_percentage".into(),
                config_value: Some(0.0001),
            })]
        },
        "seed the request quota",
    )
    .await;
    broker_handle
        .wait_for_image(|image| {
            let key: EntityKey = vec![("user".into(), Some("ANONYMOUS".into()))];
            image
                .client_quotas()
                .get(&key)
                .and_then(|configs| configs.get("request_percentage"))
                == Some(&0.0001)
        })
        .await;

    api_version_wire!((req, bytes, resp), broker, context);

    check!(resp.error_code == codes::NONE, "{resp:?}");
    check!(resp.throttle_time_ms > 0, "{resp:?}");
    check!(context.take_throttle() > <krabka_units::Time as krabka_units::convert::TimeExt>::ZERO);

    broker_handle.shutdown().await;
}
