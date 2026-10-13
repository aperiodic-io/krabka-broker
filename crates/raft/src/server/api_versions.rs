//! The `ApiVersions` handshake that every controller-listener connection begins
//! with: the advertised API table, the supported and finalized feature ranges,
//! and the request checks of Kafka's `ControllerApis.handleApiVersionsRequest`.

pub use self::{
    client_software::is_valid_client_info,
    features::{finalized_feature_keys, supported_feature_key, supported_feature_keys},
};
use {
    self::table::CONTROLLER_LISTENER_APIS,
    crate::{
        ControllerAdminRouter, ControllerApiVersion, UnstableApiVersions, UnstableFeatureVersions,
        error::RaftError, kafka_4_3_1_max, kraft::KraftController,
    },
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::{self, ApiVersionsRequest},
            api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
        },
    },
};

mod client_software;
mod features;
pub(super) mod table;

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `krabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
pub(super) const API_KEY_API_VERSIONS: i16 = 18;

/// Lowest `ApiVersions` request version this listener speaks.
const API_VERSIONS_MIN_VERSION: i16 = api_versions_request::MIN_VERSION;
/// Highest `ApiVersions` request version this listener decodes: the generated
/// maximum. [`api_versions_max_version`] is what it serves.
const API_VERSIONS_MAX_VERSION: i16 = api_versions_request::MAX_VERSION;

/// Highest `ApiVersions` version a listener serves under `unstable`, the same
/// maximum its `api_keys` table advertises for API 18: Kafka 4.3.1's v4 by
/// default, and trunk's KIP-1242 v5 under `unstable.api.versions.enable`.
#[must_use]
pub fn api_versions_max_version(unstable: UnstableApiVersions) -> i16 {
    match unstable {
        UnstableApiVersions::Enabled => API_VERSIONS_MAX_VERSION,
        UnstableApiVersions::Disabled => {
            kafka_4_3_1_max(API_KEY_API_VERSIONS).unwrap_or(API_VERSIONS_MAX_VERSION)
        }
    }
}
/// First `ApiVersions` version that carries the KIP-511 client software name
/// and version.
const API_VERSIONS_CLIENT_SOFTWARE_MIN_VERSION: i16 = 3;
/// First `ApiVersions` version that carries the KIP-1242 cluster id and node
/// id.
const API_VERSIONS_ROUTING_MIN_VERSION: i16 = 5;
const API_VERSIONS_UNSUPPORTED_VERSION: i16 = 35;
const API_VERSIONS_INVALID_REQUEST: i16 = 42;

/// What one controller-listener `ApiVersions` answer is built from.
#[derive(Clone, Copy)]
pub(crate) struct ApiVersionsView<'a> {
    /// The applied metadata image: its finalized features.
    pub(crate) image: &'a krabka_metadata::MetadataImage,
    /// The offset of the last record `image` contains, or `-1` before the
    /// first. It is the finalized-features epoch, as Kafka's
    /// `FeaturesPublisher` takes it from `image.provenance().lastContainedOffset()`.
    pub(crate) metadata_offset: i64,
    /// The KIP-919 Admin surface the broker attaches, if any.
    pub(crate) admin_router: Option<&'a dyn ControllerAdminRouter>,
    /// Kafka's `unstable.api.versions.enable`. It caps both the API table and
    /// the supported features: the controller listener's
    /// `SimpleApiVersionManager` is built with it and takes no
    /// `unstable.feature.versions.enable`.
    pub(crate) unstable: UnstableApiVersions,
}

/// The offset of the last record `engine`'s image contains, or `-1` before
/// the first: one below the engine's applied high watermark.
pub(crate) fn applied_metadata_offset(engine: &KraftController) -> i64 {
    engine.quorum_snapshot().high_watermark.saturating_sub(1)
}

/// Answers one controller-listener `ApiVersions` request with the response
/// body. The body always goes out behind a v0 response header.
///
/// The checks follow Kafka's `ControllerApis.handleApiVersionsRequest` and
/// `SaslServerAuthenticator.handleApiVersionsRequest`, which answer the same
/// way:
///
/// 1. A version outside `MIN..=MAX` is not decoded. Kafka's `RequestContext`
///    parses it as an empty v0 request, and `ApiVersionsRequest.getErrorResponse`
///    answers `UNSUPPORTED_VERSION` in a v0 body with one `api_keys` entry, the
///    `ApiVersions` range, so the client retries at a version the listener
///    serves.
/// 2. `ApiVersionsRequest.isValid` fails, and the answer is `INVALID_REQUEST`
///    with an empty table. From v5 the cluster id and the node id must be
///    both set or both unset. From v3 the client software name and version
///    must match the KIP-511 pattern.
/// 3. Otherwise the answer is the full controller-listener table.
///
/// Kafka's controller does not run the KIP-1242 `REBOOTSTRAP_REQUIRED` check.
/// Only `KafkaApis` on the broker listener runs it.
///
/// # Errors
/// Returns the decode error for a supported version whose body does not
/// decode. Kafka's `RequestContext.parseRequest` also fails such a request,
/// and the connection closes.
pub(crate) fn api_versions_response(
    req_version: i16,
    body: &[u8],
    view: ApiVersionsView<'_>,
) -> Result<Bytes, RaftError> {
    if !(API_VERSIONS_MIN_VERSION..=api_versions_max_version(view.unstable)).contains(&req_version)
    {
        return Ok(encode_body(&unsupported_version_response(view.unstable), 0));
    }
    let request = ApiVersionsRequest::decode(&mut &body[..], req_version)?;
    if !is_valid_api_versions_request(&request, req_version) {
        return Ok(encode_body(
            &ApiVersionsResponse {
                error_code: API_VERSIONS_INVALID_REQUEST,
                ..Default::default()
            },
            req_version,
        ));
    }
    Ok(api_versions_response_body(req_version, view))
}

/// Kafka's `ApiVersionsRequest.getErrorResponse` for `UNSUPPORTED_VERSION`
/// (KIP-511): error 35 and exactly one `api_keys` entry, the range of
/// `ApiVersions` itself (`ApiVersionsResponse.toApiVersion(API_VERSIONS)`),
/// which the client reads to pick the version it retries with. Every listener,
/// broker or controller, answers with these bytes in a v0 body.
#[must_use]
pub fn unsupported_version_response(unstable: UnstableApiVersions) -> ApiVersionsResponse {
    ApiVersionsResponse {
        error_code: API_VERSIONS_UNSUPPORTED_VERSION,
        api_keys: vec![ApiVersionEntry {
            api_key: API_KEY_API_VERSIONS,
            min_version: API_VERSIONS_MIN_VERSION,
            max_version: api_versions_max_version(unstable),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Kafka's `ApiVersionsRequest.isValid`.
#[must_use]
pub fn is_valid_api_versions_request(request: &ApiVersionsRequest, version: i16) -> bool {
    if version >= API_VERSIONS_ROUTING_MIN_VERSION
        && (request.cluster_id.is_none() != (request.node_id == -1))
    {
        return false;
    }
    version < API_VERSIONS_CLIENT_SOFTWARE_MIN_VERSION
        || (is_valid_client_info(&request.client_software_name)
            && is_valid_client_info(&request.client_software_version))
}

fn encode_body(response: &ApiVersionsResponse, version: i16) -> Bytes {
    let mut body = BytesMut::with_capacity(response.encoded_len(version));
    // The version is in the generated range, and the fields are defaults or
    // table entries, so the encoder has nothing to refuse.
    let _ = response.encode(&mut body, version);
    body.freeze()
}

/// The controller listener's advertised table under `unstable`, sorted by API
/// key: its own APIs and the Admin router's, each capped at the version
/// `unstable` enables, as Kafka's `ApiKeys.toApiVersion` caps it. An api key
/// the enabled table does not have -- one Kafka 4.3.1 lacks, while unstable
/// api versions are disabled -- is left out.
pub(super) fn advertised_api_keys(
    admin_router: Option<&dyn ControllerAdminRouter>,
    unstable: UnstableApiVersions,
) -> Vec<ApiVersionEntry> {
    let entry = |version: &ControllerApiVersion| {
        version
            .enabled_max(unstable)
            .map(|max_version| ApiVersionEntry {
                api_key: version.api_key,
                min_version: version.min_version,
                max_version,
                ..Default::default()
            })
    };
    let mut api_keys: Vec<ApiVersionEntry> =
        CONTROLLER_LISTENER_APIS.iter().filter_map(entry).collect();
    if let Some(router) = admin_router {
        api_keys.extend(router.api_versions().iter().filter_map(entry));
    }
    api_keys.sort_unstable_by_key(|version| version.api_key);
    api_keys
}

/// Whether `version` of `api_key` is a version that `unstable` disables on
/// this listener: one above the enabled maximum, or any version of an api key
/// the enabled table does not have. Kafka's `Processor.parseRequestHeader`
/// closes the connection on either (`ApiKeys.isVersionEnabled`, and
/// `ApiKeys.forId` for a key the release does not know).
pub(super) fn is_disabled_version(
    api_key: i16,
    version: i16,
    admin_router: Option<&dyn ControllerAdminRouter>,
    unstable: UnstableApiVersions,
) -> bool {
    let router_apis = admin_router.map_or(&[][..], |router| router.api_versions());
    CONTROLLER_LISTENER_APIS
        .iter()
        .chain(router_apis)
        .find(|api| api.api_key == api_key)
        .is_some_and(|api| api.is_disabled_version(version, unstable))
}

/// `ApiVersionsResponse` advertising the controller-listener APIs.
///
/// A real `mirror.gcr.io/apache/kafka:4.0.0` controller dials peers with `ApiVersions v4` over a
/// flexible (v2) request header, then consults the returned table to decide
/// which version of `Vote`/`Fetch`/etc. to send. An EMPTY `api_keys` list made
/// the JVM treat every raft RPC as `UNSUPPORTED_VERSION` and refuse to send
/// `Vote` on the wire. Advertising the KIP-595 APIs at the versions Krabka's
/// engine speaks lets compatible peers proceed to real `Vote`/`Fetch`. Those
/// versions come from [`table::CONTROLLER_LISTENER_APIS`], which derives them
/// from the generated message constants; the KIP-919 Admin surface the broker
/// attaches contributes the rest.
///
/// The feature rows follow Kafka's `SimpleApiVersionManager`: the supported
/// set of `BrokerFeatures.defaultSupportedFeatures(enableUnstableLastVersion)`,
/// filtered for `alterFeatureLevel0` below v4, and the finalized levels above 0
/// with the image offset as their epoch. The controller listener builds that
/// manager with `unstable.api.versions.enable`, so that flag, and not
/// `unstable.feature.versions.enable`, caps the supported levels. The broker
/// listener caps them with the feature flag. [`features`] holds the rules the
/// two share.
///
/// Body is the flexible (v3+) `ApiVersionsResponse` shape: `error_code(i16)`,
/// `api_keys` compact-array of `{api_key(i16), min(i16), max(i16), tagged(0)}`,
/// `throttle_time_ms(i32)`, response-level `tagged(0)`. Per the documented Kafka
/// asymmetry, the *response header* stays v0 (no leading tagged-fields byte) —
/// so this is written via [`super::framing::write_response_no_tagged_fields`].
pub(super) fn api_versions_response_body(req_version: i16, view: ApiVersionsView<'_>) -> Bytes {
    let resp = ApiVersionsResponse {
        api_keys: advertised_api_keys(view.admin_router, view.unstable),
        supported_features: supported_feature_keys(
            req_version,
            UnstableFeatureVersions::from(view.unstable == UnstableApiVersions::Enabled),
        ),
        finalized_features_epoch: view.metadata_offset,
        finalized_features: finalized_feature_keys(view.image),
        ..Default::default()
    };
    // JVM dials at v4 (flexible); Krabka's own client at v0 (non-flexible). The
    // codec emits the correct body shape per version: req v<=2 → non-flexible
    // v0-shaped body, req v>=3 → flexible (compact) body. The v0 ApiVersions
    // response HEADER asymmetry lives in the framing (`write_response_no_tagged_fields`),
    // not here.
    encode_body(
        &resp,
        req_version.clamp(0, api_versions_max_version(view.unstable)),
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            ControllerAdminRequest, ControllerAdminRouteFuture, ControllerAdminRouter,
            ControllerApiVersion, LATEST_PRODUCTION_METADATA_VERSION, test_support::feature_image,
        },
        krabka_metadata::{FeatureLevelRecord, MetadataRecord},
        krabka_protocol::Decode,
        uuid::Uuid,
    };

    /// The view a listener with no Admin router and the default unstable
    /// setting answers from, at metadata offset `metadata_offset`.
    fn view(image: &krabka_metadata::MetadataImage, metadata_offset: i64) -> ApiVersionsView<'_> {
        ApiVersionsView {
            image,
            metadata_offset,
            admin_router: None,
            unstable: UnstableApiVersions::Disabled,
        }
    }

    #[test]
    fn api_versions_body_advertises_kip595_set_both_shapes() {
        use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};
        let mut image = krabka_metadata::MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: 24,
        }));
        for req_v in [0i16, 4i16] {
            let body = super::api_versions_response_body(req_v, view(&image, 41));
            let v = req_v.clamp(0, 4);
            let mut cur = &body[..];
            let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
            assert2::assert!(cur.is_empty());
            assert2::assert!(resp.error_code == 0);
            let keys: std::collections::BTreeSet<i16> =
                resp.api_keys.iter().map(|k| k.api_key).collect();
            for want in [1i16, 18, 52, 53, 54, 59, 70] {
                assert2::assert!(keys.contains(&want));
            }
            // `BrokerRegistration` and `BrokerHeartbeat` reach the controller
            // listener through the Admin router, so the native table must not
            // advertise them on its own.
            // `an_admin_router_contributes_its_own_api_versions` covers the
            // other half: with a router bound, a key is advertised.
            assert2::assert!(!keys.contains(&62i16));
            assert2::assert!(!keys.contains(&63i16));
            // Vote is advertised over its whole schema range, as Kafka's
            // `ApiKeys` does.
            let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
            assert2::assert!(vote.min_version == 0 && vote.max_version == 2);
            if req_v >= 3 {
                let finalized_metadata = resp
                    .finalized_features
                    .iter()
                    .find(|feature| feature.name == "metadata.version")
                    .expect("metadata.version finalized");
                assert2::assert!(
                    (
                        finalized_metadata.min_version_level,
                        finalized_metadata.max_version_level
                    ) == (24, 24)
                );
                assert2::assert!(resp.finalized_features_epoch == 41);
            }
        }
    }

    /// The controller listener advertises what a bound Admin router serves, on
    /// top of the APIs it answers itself. `BrokerHeartbeat` is the case that
    /// matters: a broker reads this table to learn where to send it.
    #[test]
    fn an_admin_router_contributes_its_own_api_versions() {
        use krabka_protocol::owned::api_versions_response::ApiVersionsResponse;

        struct HeartbeatRouter;
        impl ControllerAdminRouter for HeartbeatRouter {
            fn api_versions(&self) -> &[ControllerApiVersion] {
                use krabka_protocol::owned::broker_heartbeat_request;
                &[ControllerApiVersion {
                    api_key: broker_heartbeat_request::API_KEY,
                    min_version: broker_heartbeat_request::MIN_VERSION,
                    max_version: broker_heartbeat_request::MAX_VERSION,
                    released_max: Some(broker_heartbeat_request::MAX_VERSION),
                    flexible_min: broker_heartbeat_request::FLEXIBLE_MIN,
                }]
            }

            fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
                Box::pin(async { Ok(None) })
            }
        }

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        let body = super::api_versions_response_body(4, ApiVersionsView {
            admin_router: Some(&HeartbeatRouter),
            ..view(&image, -1)
        });
        let resp = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode body");

        let heartbeat = resp
            .api_keys
            .iter()
            .find(|key| key.api_key == 63)
            .expect("BrokerHeartbeat advertised through the Admin router");
        assert2::assert!(
            (heartbeat.min_version, heartbeat.max_version)
                == (
                    krabka_protocol::owned::broker_heartbeat_request::MIN_VERSION,
                    krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION,
                )
        );
    }

    /// A router whose one API has an unstable last version, the shape of
    /// Kafka's `InitProducerId` v6 (`latestVersionUnstable`).
    struct UnstableRouter;

    impl ControllerAdminRouter for UnstableRouter {
        fn api_versions(&self) -> &[ControllerApiVersion] {
            &[ControllerApiVersion {
                api_key: 22,
                min_version: 0,
                max_version: 6,
                released_max: Some(5),
                flexible_min: 2,
            }]
        }

        fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
            Box::pin(async { Ok(None) })
        }
    }

    /// #646: an unstable last version is advertised, and accepted, only under
    /// `unstable.api.versions.enable`, as Kafka's `ApiKeys.toApiVersion` and
    /// `ApiKeys.isVersionEnabled` gate it.
    #[test]
    fn unstable_last_versions_follow_unstable_api_versions_enable() {
        for (unstable, advertised_max, v6_disabled) in [
            (UnstableApiVersions::Disabled, 5, true),
            (UnstableApiVersions::Enabled, 6, false),
        ] {
            let api_keys = advertised_api_keys(Some(&UnstableRouter), unstable);
            let row = api_keys
                .iter()
                .find(|key| key.api_key == 22)
                .expect("router api advertised");
            assert2::check!(
                (row.min_version, row.max_version) == (0, advertised_max),
                "{unstable:?}"
            );
            assert2::check!(
                is_disabled_version(22, 6, Some(&UnstableRouter), unstable) == v6_disabled,
                "{unstable:?}"
            );
            assert2::check!(
                !is_disabled_version(22, 5, Some(&UnstableRouter), unstable),
                "{unstable:?}"
            );
            // An out-of-range version is the decoder's business, not this
            // gate's; an api the listener does not know is not disabled.
            assert2::check!(!is_disabled_version(22, 7, Some(&UnstableRouter), unstable));
            assert2::check!(!is_disabled_version(22, 6, None, unstable));
        }
    }

    /// A router api Kafka 4.3.1 does not have -- `UnregisterController` (94),
    /// a trunk key -- is neither advertised nor accepted while unstable api
    /// versions are disabled, and is served whole when they are enabled.
    #[test]
    fn an_api_kafka_4_3_1_lacks_follows_unstable_api_versions_enable() {
        struct TrunkRouter;
        impl ControllerAdminRouter for TrunkRouter {
            fn api_versions(&self) -> &[ControllerApiVersion] {
                &[ControllerApiVersion {
                    api_key: 94,
                    min_version: 0,
                    max_version: 0,
                    released_max: None,
                    flexible_min: 0,
                }]
            }

            fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
                Box::pin(async { Ok(None) })
            }
        }

        for (unstable, advertised, disabled) in [
            (UnstableApiVersions::Disabled, None, true),
            (UnstableApiVersions::Enabled, Some((0, 0)), false),
        ] {
            let row = advertised_api_keys(Some(&TrunkRouter), unstable)
                .into_iter()
                .find(|key| key.api_key == 94)
                .map(|key| (key.min_version, key.max_version));
            assert2::check!(row == advertised, "{unstable:?}");
            assert2::check!(
                is_disabled_version(94, 0, Some(&TrunkRouter), unstable) == disabled,
                "{unstable:?}"
            );
        }
    }

    /// The controller listener's table is strictly ascending by api key, the
    /// router's rows merged in, as Kafka's `apisForListener` `EnumSet` orders
    /// it.
    #[test]
    fn the_advertised_table_is_strictly_ascending() {
        let api_keys = advertised_api_keys(Some(&UnstableRouter), UnstableApiVersions::Disabled);
        assert2::assert!(
            api_keys
                .windows(2)
                .all(|pair| pair[0].api_key < pair[1].api_key)
        );
    }

    /// One row per `ApiVersions` request shape: Kafka's unsupported-version
    /// answer, the `isValid` refusals, and the full table. A request whose
    /// version is not served carries a body that does not decode, because
    /// Kafka does not read it.
    #[test]
    fn api_versions_response_runs_kafka_request_checks() {
        use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

        struct Row {
            label: &'static str,
            unstable: UnstableApiVersions,
            version: i16,
            request: Option<ApiVersionsRequest>,
            expected: Option<(i16, ApiVersionsResponse)>,
        }
        let request = |name: &str, software_version: &str, cluster_id: Option<&str>, node_id| {
            Some(ApiVersionsRequest {
                client_software_name: name.into(),
                client_software_version: software_version.into(),
                cluster_id: cluster_id.map(str::to_string),
                node_id,
                ..Default::default()
            })
        };
        let unsupported = |max_version| {
            Some((0, ApiVersionsResponse {
                error_code: API_VERSIONS_UNSUPPORTED_VERSION,
                api_keys: vec![ApiVersionEntry {
                    api_key: 18,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                }],
                ..Default::default()
            }))
        };
        let (strict, trunk) = (UnstableApiVersions::Disabled, UnstableApiVersions::Enabled);
        let invalid = |version| {
            Some((version, ApiVersionsResponse {
                error_code: API_VERSIONS_INVALID_REQUEST,
                ..Default::default()
            }))
        };
        let rows = [
            Row {
                label: "v6",
                unstable: strict,
                version: 6,
                request: None,
                expected: unsupported(4),
            },
            Row {
                label: "i16::MAX",
                unstable: strict,
                version: i16::MAX,
                request: None,
                expected: unsupported(4),
            },
            Row {
                label: "negative",
                unstable: strict,
                version: -1,
                request: None,
                expected: unsupported(4),
            },
            Row {
                label: "v3 empty name",
                unstable: strict,
                version: 3,
                request: request("", "1.0", None, -1),
                expected: invalid(3),
            },
            Row {
                label: "v4 name with a space",
                unstable: strict,
                version: 4,
                request: request("a b", "1.0", None, -1),
                expected: invalid(4),
            },
            Row {
                label: "v3 empty software version",
                unstable: strict,
                version: 3,
                request: request("krabka", "", None, -1),
                expected: invalid(3),
            },
            Row {
                label: "v5 cluster id without node id",
                unstable: trunk,
                version: 5,
                request: request("krabka", "1.0", Some("cluster"), -1),
                expected: invalid(5),
            },
            Row {
                label: "v5 node id without cluster id",
                unstable: trunk,
                version: 5,
                request: request("krabka", "1.0", None, 7),
                expected: invalid(5),
            },
            Row {
                label: "v5 another cluster and node, no KIP-1242 check",
                unstable: trunk,
                version: 5,
                request: request("krabka", "1.0", Some("other"), 8),
                expected: None,
            },
            Row {
                label: "v5 valid",
                unstable: trunk,
                version: 5,
                request: request("krabka", "1.0", None, -1),
                expected: None,
            },
            Row {
                label: "v0",
                unstable: strict,
                version: 0,
                request: Some(ApiVersionsRequest::default()),
                expected: None,
            },
            Row {
                label: "v6 under unstable api versions",
                unstable: trunk,
                version: 6,
                request: None,
                expected: unsupported(5),
            },
            Row {
                label: "v5 is Kafka 4.3.1's unsupported version",
                unstable: strict,
                version: 5,
                request: request("krabka", "1.0", None, -1),
                expected: unsupported(4),
            },
        ];

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        for row in rows {
            let body = row.request.map_or_else(
                || bytes::Bytes::from_static(&[0xff]),
                |request| {
                    let mut body = BytesMut::new();
                    request.encode(&mut body, row.version).expect("encode");
                    body.freeze()
                },
            );
            let view = ApiVersionsView {
                unstable: row.unstable,
                ..view(&image, -1)
            };
            let answer = super::api_versions_response(row.version, &body, view).expect("answer");
            let (version, expected) = row.expected.unwrap_or_else(|| {
                let full = super::api_versions_response_body(row.version, view);
                (
                    row.version,
                    ApiVersionsResponse::decode(&mut &full[..], row.version).expect("full"),
                )
            });
            let mut cur = &answer[..];
            let decoded = ApiVersionsResponse::decode(&mut cur, version).expect("decode");
            assert2::check!(cur.is_empty(), "{}", row.label);
            assert2::check!(decoded == expected, "{}", row.label);
            if expected.error_code == 0 {
                assert2::check!(!decoded.api_keys.is_empty(), "{}", row.label);
            }
        }
    }

    /// A served version whose body does not decode fails the request, as
    /// Kafka's `RequestContext.parseRequest` does.
    #[test]
    fn api_versions_response_refuses_a_malformed_body() {
        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        assert2::assert!(super::api_versions_response(3, &[0xff], view(&image, -1)).is_err());
    }

    /// #783 on the controller listener, table-driven over the issue's rows:
    /// the supported rows follow Kafka's minimums and `alterFeatureLevel0`,
    /// the finalized rows omit level 0 (`kraft.version` included), and the
    /// epoch is the metadata offset the view carries.
    #[test]
    fn feature_fields_match_kafka_on_the_controller_listener() {
        use {
            krabka_metadata::metadata_version::METADATA_VERSION_MIN,
            krabka_protocol::owned::api_versions_response::{
                FinalizedFeatureKey, SupportedFeatureKey,
            },
        };

        struct Row<'a> {
            version: i16,
            kraft_version: u16,
            levels: &'a [(&'a str, i16)],
            want_supported: &'a Vec<SupportedFeatureKey>,
            want_finalized: Vec<FinalizedFeatureKey>,
        }

        krabka_macros::supported_feature_fixture!(supported);
        krabka_macros::supported_features_fixture!(modern_supported, supported);
        krabka_macros::finalized_feature_fixture!(finalized);
        // The listener's default, `unstable.api.versions.enable=false`, caps
        // metadata.version at 4.3.1's latest production level.
        let metadata_max = LATEST_PRODUCTION_METADATA_VERSION;
        let legacy = vec![supported(
            "metadata.version",
            METADATA_VERSION_MIN,
            metadata_max,
        )];
        let modern = modern_supported(
            supported("metadata.version", METADATA_VERSION_MIN, metadata_max),
            1,
        );
        let rows = [
            Row {
                version: 3,
                kraft_version: 0,
                levels: &[("metadata.version", 25)],
                want_supported: &legacy,
                want_finalized: vec![finalized("metadata.version", 25)],
            },
            Row {
                version: 4,
                kraft_version: 0,
                levels: &[("metadata.version", 25), ("group.version", 1)],
                want_supported: &modern,
                want_finalized: vec![
                    finalized("group.version", 1),
                    finalized("metadata.version", 25),
                ],
            },
            Row {
                version: 4,
                kraft_version: 1,
                levels: &[("metadata.version", 25)],
                want_supported: &modern,
                want_finalized: vec![
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 25),
                ],
            },
            Row {
                version: 4,
                kraft_version: 1,
                levels: &[("metadata.version", 30), ("krabka.version", 1)],
                want_supported: &modern,
                want_finalized: vec![
                    finalized("krabka.version", 1),
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 30),
                ],
            },
        ];
        for Row {
            version,
            kraft_version,
            levels,
            want_supported,
            want_finalized,
        } in rows
        {
            let image = feature_image(kraft_version, levels);
            let body = super::api_versions_response_body(version, view(&image, 1234));
            let resp = ApiVersionsResponse::decode(&mut &body[..], version).expect("decode");
            assert2::check!(
                (
                    &resp.supported_features,
                    &resp.finalized_features,
                    resp.finalized_features_epoch
                ) == (want_supported, &want_finalized, 1234),
                "v{version}, kraft.version {kraft_version}"
            );
        }
    }

    /// Kafka's `ControllerServer` builds the listener's
    /// `SimpleApiVersionManager` with `unstable.api.versions.enable`, so that
    /// flag alone caps the supported `metadata.version` here. With only
    /// `unstable.feature.versions.enable` set the listener stays at the latest
    /// production level, and with only the API flag set it advertises the
    /// highest level krabka knows. The listener takes no feature flag at all:
    /// its view has none.
    #[test]
    fn the_controller_listener_caps_supported_features_with_the_api_flag() {
        use krabka_metadata::metadata_version::METADATA_VERSION_MAX;

        let image = krabka_metadata::MetadataImage::new(Uuid::nil());
        for (unstable, want_max) in [
            (
                UnstableApiVersions::Disabled,
                LATEST_PRODUCTION_METADATA_VERSION,
            ),
            (UnstableApiVersions::Enabled, METADATA_VERSION_MAX),
        ] {
            let view = ApiVersionsView {
                unstable,
                ..view(&image, 0)
            };
            let body = super::api_versions_response_body(4, view);
            let resp = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode");
            let metadata_version = resp
                .supported_features
                .iter()
                .find(|feature| feature.name == "metadata.version")
                .expect("metadata.version is supported");
            assert2::check!(
                metadata_version.max_version == want_max,
                "unstable.api.versions.enable {unstable:?}"
            );
        }
    }
}
