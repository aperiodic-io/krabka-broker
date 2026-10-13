//! `ApiVersions` (`api_key=18`). It returns the (min, max) supported version
//! range for every API key this broker handles.
//!
//! From v3, KIP-511 makes the request carry `client_software_name` and
//! `client_software_version`. The broker validates both against
//! `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`, and rejects the call with
//! `INVALID_REQUEST` if either one is empty or malformed. This mirrors
//! `ApiVersionsRequest.isValid` on the JVM. Each accepted v3+ handshake
//! increments a Prometheus counter for that (name, version) pair,
//! `krabka_broker_client_software_versions_total`, so operators can see which
//! client libraries connect.
//!
//! From v5, KIP-1242 lets a client include the cluster and node it intended to
//! reach. Both fields must be absent or present together. A complete mismatch
//! returns `REBOOTSTRAP_REQUIRED` so the client discards stale metadata. A SASL
//! connection that has not finished authenticating gets no such check: Kafka
//! answers its `ApiVersions` from `SaslServerAuthenticator`, which validates the
//! request and never compares the routing identity, and charges no quota.
//!
//! This file holds the wire entry point. The KIP-511 name check lives in
//! `client_info`, and the KIP-584 feature rows the response carries live in
//! `feature_keys`.
//!
//! KIP-219: `ApiVersionsResponse` puts `ThrottleTimeMs` behind the `ApiKeys`
//! array, so the dispatch loop -- which reports a request-quota delay by
//! patching the leading int32 of an already-encoded body -- cannot reach the
//! field. The handler therefore charges the KIP-124 request quota itself and
//! fills the field in before encoding, which is what Kafka's
//! `KafkaApis.handleApiVersionsRequest` does by answering through
//! `requestHelper.sendResponseMaybeThrottle`. Its dispatch entry is
//! `RequestQuotaPolicy::SelfAccounted` for that reason.

use {
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_protocol::{
        Decode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
    },
};

mod feature_keys;

#[cfg(test)]
mod tests;

pub(crate) use krabka_raft::is_valid_client_info;
use {
    self::feature_keys::{finalized_feature_keys, supported_feature_keys},
    crate::{
        api_catalog::{UnstableApiVersions, supported_apis},
        broker::Broker,
        cluster_id::matches,
        codes,
        error::BrokerError,
        handlers::{RequestContext, decode_request, encode_response},
        metrics::QuotaType,
        quota::throttle_time_ms,
    },
};

/// First `ApiVersions` request version that carries the KIP-511
/// `client_software_name` and `client_software_version` fields.
const CLIENT_INFO_MIN_VERSION: i16 = 3;

/// First `ApiVersions` version that carries the KIP-1242 routing identity.
const ROUTING_IDENTITY_MIN_VERSION: i16 = 5;

/// The v0 body that answers an `ApiVersions` request at a version this broker
/// does not serve.
///
/// Kafka's `ApiVersionsRequest.getErrorResponse` answers `UNSUPPORTED_VERSION`
/// with exactly one `api_keys` entry, the range of `ApiVersions` itself
/// (KIP-511), on every listener: the client reads it to pick the version it
/// retries with, and nothing else. The controller listener answers with the
/// same bytes, from the same `krabka_raft::unsupported_version_response`.
pub(crate) fn unsupported_version_response(
    unstable: UnstableApiVersions,
) -> Result<Bytes, BrokerError> {
    let response = krabka_raft::unsupported_version_response(unstable);
    encode_response(&response, 0)
}

/// Charges the KIP-124 request quota for the handler time this request has
/// taken, records the KIP-219 window on `context` so the dispatch loop mutes
/// the connection once the bytes are written, and returns the delay the
/// response must report.
///
/// This is the `Produce` and `Fetch` accounting, minus the data quotas that
/// `ApiVersions` has none of.
fn charge_request_quota(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    handler_start: std::time::Instant,
) -> i32 {
    let request_delay = context.charge_request_quota(broker, image, handler_start);
    let delay = broker.metrics.record_applied_throttle(
        krabka_protocol::api_key::ApiKey::ApiVersions as i16,
        &[(QuotaType::Request, request_delay).into()],
    );
    // KIP-219: the response goes out now and the connection is muted for the
    // window afterwards. Sleeping here would hold the handshake back past the
    // client's request timeout.
    context.record_throttle(delay);
    throttle_time_ms(delay)
}

pub(crate) fn handle<'a>(
    broker: &'a Broker,
    version: i16,
    req_bytes: &'a [u8],
    context: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    let handler_start = std::time::Instant::now();
    let listener_kind = broker
        .config
        .listener_kind(context.connection_listener_name);
    let metrics = broker.metrics.clone();
    // Read before the image, so the epoch never runs ahead of the features it
    // stamps.
    let metadata_offset = broker.controller.current_metadata_offset();
    let image = broker.controller.current_image();
    let expected_cluster_id = image.cluster_id();
    let expected_node_id = i32::try_from(broker.config.node_id.0).ok();
    Box::pin(async move {
        let req = decode_request::<ApiVersionsRequest>(req_bytes, version)?;

        // Kafka's `SaslServerAuthenticator` answers an `ApiVersions` that
        // arrives before authentication finishes. It checks only
        // `ApiVersionsRequest.isValid`, never compares the routing identity
        // (that is `KafkaApis`, which runs after authentication), and answers
        // with throttle 0 without charging a quota.
        let pre_authentication = context.pre_authentication;
        let error_code = if !krabka_raft::is_valid_api_versions_request(&req, version) {
            Some(codes::INVALID_REQUEST)
        } else if version >= ROUTING_IDENTITY_MIN_VERSION
            && !pre_authentication
            && let Some(cluster_id) = &req.cluster_id
            && (!matches(cluster_id, expected_cluster_id) || Some(req.node_id) != expected_node_id)
        {
            Some(codes::REBOOTSTRAP_REQUIRED)
        } else {
            None
        };
        let throttle_time_ms = || {
            if pre_authentication {
                0
            } else {
                charge_request_quota(broker, &image, context, handler_start)
            }
        };

        // Invalid client information or an incomplete KIP-1242 identity is
        // INVALID_REQUEST. A complete but stale identity asks the client to
        // rebootstrap. Both use the normal v5 response shape with no API list.
        if let Some(error_code) = error_code {
            let resp = ApiVersionsResponse {
                error_code,
                throttle_time_ms: throttle_time_ms(),
                ..Default::default()
            };
            return encode_response(&resp, version);
        }

        // Accepted handshake. Bump the per-(name, version) counter on
        // v3+ only; older requests don't carry the fields.
        if version >= CLIENT_INFO_MIN_VERSION {
            metrics.record_client_software(&req.client_software_name, &req.client_software_version);
        }

        let resp = ApiVersionsResponse {
            // KIP-714 and Kafka's `ApiMessageType.ListenerType`: the table is
            // scoped to the listener this request arrived on and to whether a
            // client-metrics receiver is configured, so a client reads back
            // what it reads back from a Kafka broker.
            api_keys: supported_apis(
                listener_kind,
                broker.config.client_metrics_receiver(),
                broker.config.features.version_gates(),
            ),
            // KIP-584. `supported_features` advertises the broker's
            // `crate::features` table the way Kafka's
            // `BrokerFeatures.defaultSupportedFeatures` does, and
            // `finalized_features` reads the live metadata image. The epoch
            // is the offset of the last record that image contains, as
            // `KRaftMetadataCache.features` reports
            // `image.highestOffsetAndEpoch().offset()`: it rises with every
            // metadata record, and is `-1` before the first.
            supported_features: supported_feature_keys(
                version,
                broker.config.features.unstable_feature_versions,
            ),
            finalized_features_epoch: metadata_offset,
            finalized_features: finalized_feature_keys(&image),
            throttle_time_ms: throttle_time_ms(),
            ..Default::default()
        };
        encode_response(&resp, version)
    })
}

/// Whether `body` decodes as an `ApiVersions` request at `version` that
/// Kafka's `ApiVersionsRequest.isValid` accepts.
///
/// Before the handshake, a valid request moves Kafka's `SaslServerAuthenticator`
/// on to `HANDSHAKE_REQUEST`; an invalid one leaves it where it was.
pub(crate) fn is_valid_request_body(version: i16, mut body: &[u8]) -> bool {
    ApiVersionsRequest::decode(&mut body, version)
        .is_ok_and(|request| krabka_raft::is_valid_api_versions_request(&request, version))
}
