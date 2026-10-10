//! KIP-590 `Envelope` (`api_key` 58): the wire form a forwarding node uses to
//! carry a client's admin write to the active controller. This module holds
//! the pure encode/decode halves of sending one and of serving one.
//!
//! Everything here was settled against `mirror.gcr.io/apache/kafka:4.3.1`
//! rather than the wiki, by loading its own `kafka-clients-4.3.1.jar`:
//!
//! - `ApiKeys.ENVELOPE` is id 58, versions `0..=0`, flexible from 0, and
//!   `clusterAction = true`. Its `listeners` list is exactly `[CONTROLLER]`,
//!   so `Envelope` is advertised on the controller listener and on no other.
//!   `ApiKeys.clientApis()` and `ApiKeys.brokerApis()` both exclude it.
//! - `EnvelopeRequest` v0 is `{request_data: COMPACT_BYTES,
//!   request_principal: COMPACT_NULLABLE_BYTES, client_host_address:
//!   COMPACT_BYTES, _tagged_fields}`, and `EnvelopeResponse` v0 is
//!   `{response_data: COMPACT_NULLABLE_BYTES, error_code: INT16,
//!   _tagged_fields}`. Both `request_data` and `response_data` carry the
//!   embedded *header and* body, not a bare body.
//! - `DefaultKafkaPrincipalBuilder.serialize` is
//!   `MessageUtil.toVersionPrefixedBytes`: a big-endian `int16` schema
//!   version followed by the flexible `DefaultPrincipalData` body
//!   `{type: COMPACT_STRING, name: COMPACT_STRING, token_authenticated:
//!   BOOLEAN, _tagged_fields}`. `serialize(User:alice)` is exactly
//!   `00 00 05 55 73 65 72 06 61 6c 69 63 65 00 00`, and its `deserialize`
//!   refuses any version outside `0..=0`.
//! - `kafka.server.EnvelopeUtils` maps the failures: a request header it
//!   cannot parse, or an embedded body that does not decode, is an
//!   `UnsupportedVersionException`, an inner api key whose
//!   `ApiKeys.forwardable` is false is an `InvalidRequestException`, and a
//!   principal it cannot deserialize is a `PrincipalDeserializationException`.
//!   It performs no principal-*type* check, so neither does this module.

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use krabka_protocol::{
    Decode as _, Encode as _, ProtocolError, UnknownTaggedFields,
    owned::{
        default_principal_data::{self, DefaultPrincipalData},
        envelope_request::EnvelopeRequest,
        envelope_response::EnvelopeResponse,
    },
    primitives::string_bytes::put_nullable_string,
    tagged_fields::read_tagged_fields,
};

use crate::{
    codes,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

/// A failure serving one `Envelope`, and the `EnvelopeResponse.error_code`
/// Kafka answers it with.
///
/// Every variant mirrors one throw site in `kafka.server.EnvelopeUtils`, so
/// the code a JVM forwarder sees is the code its own controller would send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeError {
    /// The outer principal lacks `ClusterAction` on the cluster resource.
    /// `ApiKeys.ENVELOPE.clusterAction` is true, so this is the gate Kafka
    /// applies before it looks at the payload at all.
    ClusterAuthorizationFailed,
    /// `request_principal` was absent, carried an unreadable schema version,
    /// or did not decode as a `DefaultPrincipalData` body.
    PrincipalDeserializationFailure,
    /// The embedded request header did not parse, or its body did not decode.
    UnsupportedVersion,
    /// The envelope itself was malformed, or the embedded request is not one
    /// Kafka forwards.
    InvalidRequest,
}

impl EnvelopeError {
    /// The `EnvelopeResponse.error_code` this failure is reported as.
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::ClusterAuthorizationFailed => codes::CLUSTER_AUTHORIZATION_FAILED,
            Self::PrincipalDeserializationFailure => codes::PRINCIPAL_DESERIALIZATION_FAILURE,
            Self::UnsupportedVersion => codes::UNSUPPORTED_VERSION,
            Self::InvalidRequest => codes::INVALID_REQUEST,
        }
    }
}

/// Declares the forwardable api keys with the request type each one carries,
/// as the key list [`FORWARDABLE_API_KEYS`] and the body check
/// [`embedded_body_decodes`] that `EnvelopeUtils` runs through
/// `RequestContext.parseRequest`.
macro_rules! forwardable_apis {
    ($($key:literal => $module:ident::$request:ident,)*) => {
        /// `ApiKeys.forwardable`, read out of `kafka-clients-4.3.1.jar`, plus
        /// `UnregisterController`, which Kafka trunk's `ApiKeys` declares
        /// forwardable.
        ///
        /// KIP-590 lets a broker wrap only these api keys, and
        /// `EnvelopeUtils.handleEnvelopeRequest` refuses any other embedded key
        /// with `InvalidRequestException`. Accepting a key outside the set would
        /// let a forwarding hop launder a request the client could not have sent
        /// itself.
        const FORWARDABLE_API_KEYS: &[ApiKeyCode] = &[$($key,)*];

        /// Whether `body` decodes as the request of `api_key` at `version`.
        ///
        /// `RequestContext.parseRequest` decodes the body and nothing checks
        /// what is left after it, so neither does this. A key outside the
        /// forwardable set decodes as nothing.
        fn embedded_body_decodes(api_key: ApiKeyCode, version: ApiVersion, body: &[u8]) -> bool {
            let mut cur = body;
            match api_key {
                $($key => krabka_protocol::owned::$module::$request::decode(&mut cur, version).is_ok(),)*
                _ => false,
            }
        }
    };
}

forwardable_apis! {
    19 => create_topics_request::CreateTopicsRequest, // CreateTopics
    20 => delete_topics_request::DeleteTopicsRequest, // DeleteTopics
    30 => create_acls_request::CreateAclsRequest, // CreateAcls
    31 => delete_acls_request::DeleteAclsRequest, // DeleteAcls
    33 => alter_configs_request::AlterConfigsRequest, // AlterConfigs
    37 => create_partitions_request::CreatePartitionsRequest, // CreatePartitions
    38 => create_delegation_token_request::CreateDelegationTokenRequest, // CreateDelegationToken
    39 => renew_delegation_token_request::RenewDelegationTokenRequest, // RenewDelegationToken
    40 => expire_delegation_token_request::ExpireDelegationTokenRequest, // ExpireDelegationToken
    43 => elect_leaders_request::ElectLeadersRequest, // ElectLeaders
    44 => incremental_alter_configs_request::IncrementalAlterConfigsRequest, // IncrementalAlterConfigs
    45 => alter_partition_reassignments_request::AlterPartitionReassignmentsRequest, // AlterPartitionReassignments
    46 => list_partition_reassignments_request::ListPartitionReassignmentsRequest, // ListPartitionReassignments
    49 => alter_client_quotas_request::AlterClientQuotasRequest, // AlterClientQuotas
    51 => alter_user_scram_credentials_request::AlterUserScramCredentialsRequest, // AlterUserScramCredentials
    55 => describe_quorum_request::DescribeQuorumRequest, // DescribeQuorum
    57 => update_features_request::UpdateFeaturesRequest, // UpdateFeatures
    64 => unregister_broker_request::UnregisterBrokerRequest, // UnregisterBroker
    67 => allocate_producer_ids_request::AllocateProducerIdsRequest, // AllocateProducerIds
    80 => add_raft_voter_request::AddRaftVoterRequest, // AddRaftVoter
    81 => remove_raft_voter_request::RemoveRaftVoterRequest, // RemoveRaftVoter
    94 => unregister_controller_request::UnregisterControllerRequest, // UnregisterController (Kafka trunk, KIP-1312)
}

/// Whether Kafka wraps `api_key` in an `Envelope` when a broker forwards it.
pub(crate) fn is_forwardable(api_key: ApiKeyCode) -> bool {
    FORWARDABLE_API_KEYS.contains(&api_key)
}

/// Whether `api_key` is an api key `RequestHeader.parse` accepts on a listener
/// running with `unstable`: one the release has (`ApiKeys.forId`) with a valid
/// version (`hasValidVersion`).
///
/// Kafka 4.3.1 has no key from 93 up, and its removed `LeaderAndIsr`,
/// `StopReplica`, `UpdateMetadata` and `ControlledShutdown` (keys 4 to 7) have
/// no valid version. Trunk's own keys are known while
/// `unstable.api.versions.enable` is on.
fn is_known_api_key(
    api_key: ApiKeyCode,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> bool {
    match unstable {
        crate::api_catalog::UnstableApiVersions::Disabled => {
            krabka_raft::kafka_4_3_1_api(api_key).is_some()
        }
        crate::api_catalog::UnstableApiVersions::Enabled => {
            krabka_protocol::api_key::ApiKey::from_i16(api_key).is_some()
        }
    }
}

/// The client identity a forwarder put in `request_principal`.
///
/// `KafkaPrincipal` carries a name and a token flag and nothing else — no
/// mechanism, no groups — so this is deliberately narrower than
/// [`krabka_security::Principal`]. The caller pairs it with the forwarding
/// hop's own authentication method to build the session identity the handler
/// runs under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardedPrincipal {
    pub(crate) name: String,
    pub(crate) token_authenticated: bool,
}

/// One embedded request, lifted out of an `EnvelopeRequest.request_data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardedRequest {
    pub(crate) api_key: ApiKeyCode,
    pub(crate) api_version: ApiVersion,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) client_id: Option<String>,
    pub(crate) body: Bytes,
    /// Whether the embedded *body* is flexible, which is also what decides
    /// the embedded response header version. See [`wrap_response`].
    pub(crate) body_flexible: bool,
}

/// Read a `request_principal` the way
/// `DefaultKafkaPrincipalBuilder.deserialize` does: a big-endian `int16`
/// schema version, then the flexible `DefaultPrincipalData` body at that
/// version.
///
/// The principal *type* is read but not enforced: Kafka's own `deserialize`
/// hands `DefaultPrincipalData.type` straight to the `KafkaPrincipal`
/// constructor without a check, and krabka authorizes on the name alone.
///
/// # Errors
/// [`EnvelopeError::PrincipalDeserializationFailure`] when the bytes are
/// absent, too short to hold the version prefix, carry a version outside
/// `0..=0`, or do not decode as a `DefaultPrincipalData` body. Kafka's
/// `deserialize` throws `SerializationException` in each of those cases and
/// `EnvelopeUtils` rewraps it as `PrincipalDeserializationException`.
pub(crate) fn deserialize_principal(
    bytes: Option<&[u8]>,
) -> Result<ForwardedPrincipal, EnvelopeError> {
    let mut cur = bytes.ok_or(EnvelopeError::PrincipalDeserializationFailure)?;
    let Some((prefix, rest)) = cur.split_at_checked(2) else {
        return Err(EnvelopeError::PrincipalDeserializationFailure);
    };
    let version = i16::from_be_bytes([prefix[0], prefix[1]]);
    cur = rest;
    if !(default_principal_data::MIN_VERSION..=default_principal_data::MAX_VERSION)
        .contains(&version)
    {
        return Err(EnvelopeError::PrincipalDeserializationFailure);
    }
    let data = DefaultPrincipalData::decode(&mut cur, version)
        .map_err(|_| EnvelopeError::PrincipalDeserializationFailure)?;
    Ok(ForwardedPrincipal {
        name: data.name,
        token_authenticated: data.token_authenticated,
    })
}

/// Read a `client_host_address` the way
/// `kafka.server.EnvelopeUtils.parseForwardedClientAddress` does:
/// `InetAddress.getByAddress(byte[])` over the raw address octets the
/// forwarding hop copied out of its own client's connection.
///
/// `getByAddress` accepts a 4-byte IPv4 address or a 16-byte IPv6 one and
/// nothing else, and it folds an IPv4-mapped `::ffff:a.b.c.d` back to the
/// `Inet4Address` it names — which is what [`std::net::IpAddr::to_canonical`]
/// does here, so a mapped address authorizes against the same host string
/// Kafka would use.
///
/// This address, not the forwarding broker's, is the host the embedded
/// request authorizes and audits against: `EnvelopeUtils` builds the inner
/// `RequestContext` with it, so a host ACL sees the client that actually
/// connected.
///
/// # Errors
/// [`EnvelopeError::InvalidRequest`] for any other length. Kafka's
/// `getByAddress` throws `UnknownHostException` there and `EnvelopeUtils`
/// rewraps it as `InvalidRequestException`.
pub(crate) fn deserialize_client_host_address(
    bytes: &[u8],
) -> Result<std::net::IpAddr, EnvelopeError> {
    if let Ok(octets) = <[u8; 4]>::try_from(bytes) {
        return Ok(std::net::IpAddr::from(octets));
    }
    if let Ok(octets) = <[u8; 16]>::try_from(bytes) {
        return Ok(std::net::IpAddr::from(octets).to_canonical());
    }
    Err(EnvelopeError::InvalidRequest)
}

/// Write a `request_principal` the way `DefaultKafkaPrincipalBuilder.serialize`
/// does: a big-endian `int16` schema version 0, then the flexible
/// `DefaultPrincipalData` body at that version. [`deserialize_principal`]
/// reads it back.
///
/// The principal type is always Kafka's `"User"`, because
/// `DefaultKafkaPrincipalBuilder` builds every principal with that type and
/// krabka authorizes on the name alone.
///
/// # Errors
/// Returns an error if the generated codec rejects the principal body.
pub(crate) fn serialize_principal(principal: &ForwardedPrincipal) -> Result<Bytes, ProtocolError> {
    let data = DefaultPrincipalData {
        type_: "User".to_owned(),
        name: principal.name.clone(),
        token_authenticated: principal.token_authenticated,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let version = default_principal_data::MIN_VERSION;
    let mut out = BytesMut::with_capacity(2 + data.encoded_len(version));
    out.put_i16(version);
    data.encode(&mut out, version)?;
    Ok(out.freeze())
}

/// Write a `client_host_address` the way `InetAddress.getAddress()` does: four
/// octets for an IPv4 host, sixteen for an IPv6 one.
/// [`deserialize_client_host_address`] reads it back.
///
/// An IPv4-mapped `::ffff:a.b.c.d` goes out as its four IPv4 octets. A JVM
/// socket reports such a peer as the `Inet4Address` it names, so a JVM
/// forwarder sends the same four octets for the same client.
pub(crate) fn serialize_client_host_address(ip: std::net::IpAddr) -> Bytes {
    match ip.to_canonical() {
        std::net::IpAddr::V4(v4) => Bytes::copy_from_slice(&v4.octets()),
        std::net::IpAddr::V6(v6) => Bytes::copy_from_slice(&v6.octets()),
    }
}

/// Build an `EnvelopeRequest.request_data`: the embedded request header in
/// front of the embedded body. This is Kafka's
/// `AbstractRequest.serializeWithHeader`, and [`unwrap_request`] reads it
/// back.
///
/// The header is `api_key`, `api_version`, `correlation_id` and a nullable
/// `client_id`. It is v2, with a trailing empty tagged-fields byte, exactly
/// when the embedded body is flexible.
pub(crate) fn wrap_request(forwarded: &ForwardedRequest) -> Bytes {
    let client_id_len = forwarded.client_id.as_ref().map_or(0, String::len);
    let mut out = BytesMut::with_capacity(10 + client_id_len + 1 + forwarded.body.len());
    out.put_i16(forwarded.api_key);
    out.put_i16(forwarded.api_version);
    out.put_i32(forwarded.correlation_id);
    put_nullable_string(&mut out, forwarded.client_id.as_deref());
    if forwarded.body_flexible {
        out.put_u8(0);
    }
    out.put_slice(&forwarded.body);
    out.freeze()
}

/// Build the `EnvelopeRequest` that carries `request_data` to the controller
/// in the name of `principal`, who connected from `client`. This is Kafka's
/// `ForwardingManagerUtil.buildEnvelopeRequest`.
///
/// # Errors
/// Returns an error if the generated codec rejects the principal body.
pub(crate) fn envelope_request(
    request_data: Bytes,
    principal: &ForwardedPrincipal,
    client: std::net::IpAddr,
) -> Result<EnvelopeRequest, ProtocolError> {
    Ok(EnvelopeRequest {
        request_data,
        request_principal: Some(serialize_principal(principal)?),
        client_host_address: serialize_client_host_address(client),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    })
}

/// Split a served `EnvelopeResponse.response_data` into the correlation id of
/// its embedded response header and the embedded body. This is the reverse of
/// [`wrap_response`].
///
/// `body_flexible` says whether the embedded body is flexible at the version
/// the request was sent at. The embedded header then carries a tagged-fields
/// section, except for `ApiVersions`, as [`crate::network::response_header_v1`]
/// says.
///
/// # Errors
/// Returns an error if the data is too short to hold the header, or if its
/// tagged-fields section does not parse.
pub(crate) fn unwrap_response(
    api_key: ApiKeyCode,
    body_flexible: bool,
    response_data: &Bytes,
) -> Result<(CorrelationId, Bytes), ProtocolError> {
    let mut cur = response_data.as_ref();
    if cur.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4 - cur.remaining(),
        });
    }
    let correlation_id = cur.get_i32();
    if crate::network::response_header_v1(api_key, body_flexible) {
        read_tagged_fields(&mut cur, |_, _| Ok(false))?;
    }
    Ok((correlation_id, response_data.slice_ref(cur)))
}

/// Decode an `EnvelopeRequest` at `version`.
///
/// # Errors
/// [`EnvelopeError::InvalidRequest`] when the body is not a well-formed
/// `EnvelopeRequest`, which is how Kafka reports a malformed envelope.
pub(crate) fn decode_request(body: &[u8], version: i16) -> Result<EnvelopeRequest, EnvelopeError> {
    let mut cur = body;
    EnvelopeRequest::decode(&mut cur, version).map_err(|_| EnvelopeError::InvalidRequest)
}

/// Split an `EnvelopeRequest.request_data` into its embedded request header
/// and body.
///
/// `flexible_for` is the broker's own flexibility oracle for the embedded api
/// key and version, because the embedded header is v2 exactly when the
/// embedded body is flexible — the same rule the client-listener dispatch
/// loop applies.
///
/// The checks run in the order of `EnvelopeUtils.handleEnvelopeRequest`:
/// the header, then the forwardable test, then the body.
///
/// # Errors
/// [`EnvelopeError::UnsupportedVersion`] when the header does not parse, which
/// covers an api key `unstable` leaves unknown, and when the body does not
/// decode: `EnvelopeUtils` turns the `InvalidRequestException` of either parse
/// into `UnsupportedVersionException`, so the forwarder can tell this from a
/// malformed envelope. [`EnvelopeError::InvalidRequest`] when the embedded api
/// key is a known one that Kafka does not forward.
pub(crate) fn unwrap_request<F>(
    request_data: &Bytes,
    flexible_for: F,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> Result<ForwardedRequest, EnvelopeError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    let parsed = crate::network::request::parse_request(request_data, flexible_for)
        .map_err(|_| EnvelopeError::UnsupportedVersion)?;
    if !is_known_api_key(parsed.api_key, unstable) {
        return Err(EnvelopeError::UnsupportedVersion);
    }
    if !is_forwardable(parsed.api_key) {
        return Err(EnvelopeError::InvalidRequest);
    }
    if !embedded_body_decodes(parsed.api_key, parsed.api_version, parsed.body) {
        return Err(EnvelopeError::UnsupportedVersion);
    }
    Ok(ForwardedRequest {
        api_key: parsed.api_key,
        api_version: parsed.api_version,
        correlation_id: parsed.correlation_id,
        client_id: parsed.client_id.map(ToOwned::to_owned),
        body: request_data.slice_ref(parsed.body),
        body_flexible: parsed.body_flexible,
    })
}

/// Build the `EnvelopeResponse.response_data` for a served embedded request:
/// the embedded response header in front of the handler's body.
///
/// The embedded response header is v1 — that is, it carries a trailing
/// tagged-fields byte — exactly when the embedded *body* is flexible, which is
/// the same rule [`crate::network::response_header_v1`] applies on the client
/// listener. The correlation id is the client's own, because it rode inside
/// the forwarded header, so the bytes this returns are byte-identical to what
/// the client would have received had it reached the controller itself.
pub(crate) fn wrap_response(forwarded: &ForwardedRequest, body: &[u8]) -> Bytes {
    let header_v1 = crate::network::response_header_v1(forwarded.api_key, forwarded.body_flexible);
    let mut out = BytesMut::with_capacity(4 + usize::from(header_v1) + body.len());
    out.put_i32(forwarded.correlation_id);
    if header_v1 {
        out.put_u8(0);
    }
    out.put_slice(body);
    out.freeze()
}

/// Encode an `EnvelopeResponse` carrying a served `response_data`.
///
/// # Errors
/// Returns an error if the generated codec rejects the response.
pub(crate) fn encode_success(
    response_data: Bytes,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    encode_response(
        &EnvelopeResponse {
            response_data: Some(response_data),
            error_code: codes::NONE,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    )
}

/// Encode the `EnvelopeResponse` for a failure.
///
/// Kafka sends a null `response_data` alongside the error code, so a
/// forwarding broker has nothing to relay and surfaces the code itself.
///
/// # Errors
/// Returns an error if the generated codec rejects the response.
pub(crate) fn encode_failure(
    error: EnvelopeError,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    encode_response(
        &EnvelopeResponse {
            response_data: None,
            error_code: error.code(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    )
}

fn encode_response(
    response: &EnvelopeResponse,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    let mut buf = BytesMut::with_capacity(response.encoded_len(version));
    response.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
    /// "alice"))` in `mirror.gcr.io/apache/kafka:4.3.1`, captured verbatim:
    /// int16 version 0, compact string "User", compact string "alice",
    /// `token_authenticated = false`, empty tagged fields.
    const JVM_USER_ALICE: &[u8] = krabka_macros::jvm_principal_golden!(alice, false);
    /// The same call for `new KafkaPrincipal("User", "bob", true)`, whose
    /// `token_authenticated` byte is `0x01`.
    const JVM_USER_BOB_TOKEN: &[u8] = krabka_macros::jvm_principal_golden!(bob, true);

    fn forwarded_principal(name: &str, token_authenticated: bool) -> ForwardedPrincipal {
        ForwardedPrincipal {
            name: name.to_owned(),
            token_authenticated,
        }
    }

    /// The bytes of a default request of type `R` at `version`, which the
    /// embedded-body check decodes.
    fn body_of<R: krabka_protocol::Encode + Default>(version: i16) -> Vec<u8> {
        let mut body = BytesMut::new();
        R::default()
            .encode(&mut body, version)
            .expect("encode a default request");
        body.to_vec()
    }

    const UNSTABLE_OFF: crate::api_catalog::UnstableApiVersions =
        crate::api_catalog::UnstableApiVersions::Disabled;

    /// The bytes a real JVM forwarder puts in `request_principal` decode into
    /// the identity that JVM meant. These buffers are the oracle: they were
    /// produced by `DefaultKafkaPrincipalBuilder` in the pinned image, not by
    /// this module, so a field-order or length-prefix slip fails here.
    #[test]
    fn a_jvm_serialized_principal_decodes_to_the_identity_it_names() {
        let cases = [
            (
                "User:alice, not token authenticated",
                JVM_USER_ALICE,
                forwarded_principal("alice", false),
            ),
            (
                "User:bob, token authenticated",
                JVM_USER_BOB_TOKEN,
                forwarded_principal("bob", true),
            ),
        ];

        for (case, bytes, want) in cases {
            check!(
                deserialize_principal(Some(bytes)) == Ok(want),
                "case: {case}"
            );
        }
    }

    /// Every shape Kafka's `deserialize` refuses. A missing principal is
    /// included because `request_principal` is nullable on the wire: KIP-590
    /// always populates it, and a null one leaves the controller with no
    /// identity to authorize the embedded request against.
    #[test]
    fn a_principal_kafka_would_refuse_is_a_deserialization_failure() {
        let valid = JVM_USER_ALICE;
        let mut wrong_version = valid.to_vec();
        wrong_version[1] = 1;
        let mut truncated_body = valid.to_vec();
        truncated_body.truncate(4);

        let cases: [(&str, Option<Vec<u8>>); 5] = [
            ("null principal", None),
            ("empty principal", Some(Vec::new())),
            ("version prefix only", Some(vec![0x00])),
            (
                "schema version above the supported range",
                Some(wrong_version),
            ),
            ("body truncated mid-string", Some(truncated_body)),
        ];

        for (case, bytes) in cases {
            check!(
                deserialize_principal(bytes.as_deref())
                    == Err(EnvelopeError::PrincipalDeserializationFailure),
                "case: {case}"
            );
        }
    }

    /// The `EnvelopeResponse.error_code` for each failure, as
    /// `kafka.server.EnvelopeUtils` assigns them. A substitution here is
    /// invisible to a Rust caller and changes what a JVM forwarder reports to
    /// its own client.
    #[test]
    fn each_failure_carries_the_error_code_kafka_assigns_it() {
        let cases = [
            (EnvelopeError::ClusterAuthorizationFailed, 31),
            (EnvelopeError::PrincipalDeserializationFailure, 97),
            (EnvelopeError::UnsupportedVersion, 35),
            (EnvelopeError::InvalidRequest, 42),
        ];

        for (error, want) in cases {
            check!(error.code() == want, "{error:?}");
        }
    }

    /// `client_host_address` is exactly what `InetAddress.getByAddress`
    /// accepts: four octets or sixteen, with an IPv4-mapped sixteen folded
    /// back to the IPv4 address it names. Every other length is the
    /// `UnknownHostException` `EnvelopeUtils` reports as an invalid request.
    #[test]
    fn a_client_host_address_decodes_the_way_inet_address_get_by_address_does() {
        let mapped_v4 = {
            let mut octets = [0u8; 16];
            octets[10] = 0xff;
            octets[11] = 0xff;
            octets[12..].copy_from_slice(&[10, 1, 2, 3]);
            octets
        };
        let cases: [(&str, &[u8], Result<std::net::IpAddr, EnvelopeError>); 6] = [
            ("four octets are an IPv4 address", &[10, 1, 2, 3], {
                Ok(std::net::IpAddr::from([10, 1, 2, 3]))
            }),
            (
                "sixteen octets are an IPv6 address",
                &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                Ok("2001:db8::1".parse().expect("literal IPv6 address")),
            ),
            (
                "an IPv4-mapped IPv6 address folds to its IPv4 form",
                &mapped_v4,
                Ok(std::net::IpAddr::from([10, 1, 2, 3])),
            ),
            ("an empty address", &[], Err(EnvelopeError::InvalidRequest)),
            (
                "three octets",
                &[10, 1, 2],
                Err(EnvelopeError::InvalidRequest),
            ),
            (
                "five octets",
                &[10, 1, 2, 3, 4],
                Err(EnvelopeError::InvalidRequest),
            ),
        ];

        for (case, bytes, want) in cases {
            check!(
                deserialize_client_host_address(bytes) == want,
                "case: {case}"
            );
        }
    }

    /// The send side writes the bytes a JVM forwarder writes, and the receive
    /// side reads them back into the identity that went in.
    #[test]
    fn a_serialized_principal_matches_the_jvm_bytes_and_decodes_back() {
        let cases = [
            (
                "User:alice, not token authenticated",
                forwarded_principal("alice", false),
                JVM_USER_ALICE,
            ),
            (
                "User:bob, token authenticated",
                forwarded_principal("bob", true),
                JVM_USER_BOB_TOKEN,
            ),
        ];

        for (case, principal, jvm) in cases {
            assert!(let Ok(bytes) = serialize_principal(&principal), "case: {case}");

            check!(bytes.as_ref() == jvm, "case: {case}");
            check!(
                deserialize_principal(Some(&bytes)) == Ok(principal),
                "case: {case}"
            );
        }
    }

    /// `client_host_address` goes out as `InetAddress.getAddress()` writes
    /// it, and the receive side reads the same address back. An IPv4-mapped
    /// peer goes out as the four octets of the IPv4 address it names, which
    /// is what a JVM socket reports for such a peer.
    #[test]
    fn a_client_host_address_round_trips_as_inet_address_get_address_writes_it() {
        let cases: [(&str, std::net::IpAddr, &[u8], std::net::IpAddr); 3] = [
            (
                "an IPv4 address is four octets",
                std::net::IpAddr::from([10, 1, 2, 3]),
                &[10, 1, 2, 3],
                std::net::IpAddr::from([10, 1, 2, 3]),
            ),
            (
                "an IPv6 address is sixteen octets",
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            ),
            (
                "an IPv4-mapped IPv6 address is the four IPv4 octets",
                "::ffff:10.1.2.3".parse().expect("literal IPv6 address"),
                &[10, 1, 2, 3],
                std::net::IpAddr::from([10, 1, 2, 3]),
            ),
        ];

        for (case, ip, octets, decoded) in cases {
            let bytes = serialize_client_host_address(ip);

            check!(bytes.as_ref() == octets, "case: {case}");
            check!(
                deserialize_client_host_address(&bytes) == Ok(decoded),
                "case: {case}"
            );
        }
    }

    /// `wrap_request` writes the header `unwrap_request` reads, so an embedded
    /// request round-trips whole, with and without a flexible body and a
    /// client id. The flexible case writes the same bytes as the hand-built
    /// frame the receive-side tests use.
    #[test]
    fn a_wrapped_request_unwraps_to_the_same_request() {
        let create_topics =
            body_of::<krabka_protocol::owned::create_topics_request::CreateTopicsRequest>(7);
        let alter_configs =
            body_of::<krabka_protocol::owned::alter_configs_request::AlterConfigsRequest>(0);
        let cases = [
            (
                "flexible CreateTopics with a client id",
                ForwardedRequest {
                    api_key: 19,
                    api_version: 7,
                    correlation_id: 5,
                    client_id: Some("c".to_owned()),
                    body: Bytes::from(create_topics.clone()),
                    body_flexible: true,
                },
                request_frame(EmbeddedFrameSetup {
                    correlation_id: EmbeddedCorrelationId(5),
                    client_id: Some("c"),
                    body: &create_topics,
                    ..Default::default()
                }),
            ),
            (
                "non-flexible AlterConfigs with a null client id",
                ForwardedRequest {
                    api_key: 33,
                    api_version: 0,
                    correlation_id: -3,
                    client_id: None,
                    body: Bytes::from(alter_configs.clone()),
                    body_flexible: false,
                },
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(33),
                    api_version: krabka_ids::ApiVersion(0),
                    correlation_id: EmbeddedCorrelationId(-3),
                    header: EmbeddedHeader::Legacy,
                    body: &alter_configs,
                    ..Default::default()
                }),
            ),
        ];

        for (case, forwarded, frame) in cases {
            let wrapped = wrap_request(&forwarded);

            check!(wrapped == frame, "case: {case}");
            check!(
                unwrap_request(&wrapped, |_, _| forwarded.body_flexible, UNSTABLE_OFF)
                    == Ok(forwarded),
                "case: {case}"
            );
        }
    }

    #[derive(Clone, Copy, Default)]
    enum ForwardedBodyEncoding {
        Legacy,
        #[default]
        Flexible,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ForwardedResponseSetup {
        #[default(krabka_ids::ApiKey(19))]
        api_key: krabka_ids::ApiKey,
        api_version: krabka_ids::ApiVersion,
        #[default(EmbeddedCorrelationId(0x0102_0304))]
        correlation_id: EmbeddedCorrelationId,
        encoding: ForwardedBodyEncoding,
    }

    fn forwarded(setup: ForwardedResponseSetup) -> ForwardedRequest {
        ForwardedRequest {
            api_key: setup.api_key.0,
            api_version: setup.api_version.0,
            correlation_id: setup.correlation_id.0,
            client_id: None,
            body: Bytes::new(),
            body_flexible: matches!(setup.encoding, ForwardedBodyEncoding::Flexible),
        }
    }

    /// `unwrap_response` strips the header `wrap_response` writes and hands
    /// back the correlation id and the body. `ApiVersions` keeps a v0 header
    /// even when its body is flexible.
    #[test]
    fn a_wrapped_response_unwraps_to_its_correlation_id_and_body() {
        let cases = [
            (
                "flexible body takes a v1 header",
                forwarded(ForwardedResponseSetup::default()),
            ),
            (
                "non-flexible body takes a v0 header",
                forwarded(ForwardedResponseSetup {
                    encoding: ForwardedBodyEncoding::Legacy,
                    ..Default::default()
                }),
            ),
            (
                "flexible ApiVersions takes a v0 header",
                forwarded(ForwardedResponseSetup {
                    api_key: krabka_ids::ApiKey(18),
                    ..Default::default()
                }),
            ),
        ];

        for (case, forwarded) in cases {
            let wrapped = wrap_response(&forwarded, b"body");

            check!(
                unwrap_response(forwarded.api_key, forwarded.body_flexible, &wrapped)
                    .map_err(|error| error.to_string())
                    == Ok((0x0102_0304, Bytes::from_static(b"body"))),
                "case: {case}"
            );
        }
    }

    /// Data too short to hold the correlation id, or a flexible header whose
    /// tagged-fields section is cut off, is refused.
    #[test]
    fn a_response_too_short_for_its_header_is_refused() {
        let cases: [(&str, &[u8], bool); 2] = [
            ("three bytes", &[0, 0, 1], false),
            (
                "a tagged-fields count with no fields after it",
                &[0, 0, 0, 1, 1],
                true,
            ),
        ];

        for (case, data, flexible) in cases {
            check!(
                unwrap_response(19, flexible, &Bytes::copy_from_slice(data)).is_err(),
                "case: {case}"
            );
        }
    }

    /// The Envelope that the send side builds carries the three pieces the
    /// receive side reads, and it survives the v0 codec.
    #[test]
    fn a_built_envelope_request_carries_what_the_receive_side_reads() {
        let request_data = request_frame(EmbeddedFrameSetup {
            correlation_id: EmbeddedCorrelationId(5),
            client_id: Some("c"),
            body: b"topics",
            ..Default::default()
        });
        let client = std::net::IpAddr::from([127, 0, 0, 1]);

        assert!(let Ok(request) = envelope_request(
            request_data.clone(),
            &forwarded_principal("alice", false),
            client,
        ));
        let mut encoded = BytesMut::new();
        request.encode(&mut encoded, 0).expect("encode");

        let want = EnvelopeRequest {
            request_data,
            request_principal: Some(Bytes::from_static(JVM_USER_ALICE)),
            client_host_address: Bytes::from_static(&[127, 0, 0, 1]),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(decode_request(&encoded, 0) == Ok(want));
    }

    /// The forwardable set is `ApiKeys.forwardable` from
    /// `kafka-clients-4.3.1.jar`. The five write APIs issue #82 names are in
    /// it, and so is `UpdateFeatures`, which `kafka-features.sh` drives.
    #[test]
    fn the_forwardable_set_is_the_one_kafka_publishes() {
        let want: std::collections::BTreeSet<i16> = maplit::btreeset! {
            19, 20, 30, 31, 33, 37, 38, 39, 40, 43, 44, 45, 46, 49, 51, 55, 57, 64, 67, 80, 81,
            94,
        };
        let declared: std::collections::BTreeSet<i16> =
            FORWARDABLE_API_KEYS.iter().copied().collect();

        check!(declared == want);
        // Envelope itself is not forwardable, so an envelope cannot nest.
        check!(!is_forwardable(58));
        // Produce is a data-plane api the controller never serves.
        check!(!is_forwardable(0));
    }

    krabka_macros::request_frame_fixture!(wire_request_frame);

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct EmbeddedCorrelationId(i32);

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum EmbeddedHeader {
        Legacy,
        Flexible,
    }

    #[derive(Clone, Copy)]
    struct EmbeddedFrameSetup<'a> {
        api_key: krabka_ids::ApiKey,
        api_version: krabka_ids::ApiVersion,
        correlation_id: EmbeddedCorrelationId,
        client_id: Option<&'a str>,
        header: EmbeddedHeader,
        body: &'a [u8],
    }

    impl Default for EmbeddedFrameSetup<'_> {
        fn default() -> Self {
            Self {
                api_key: krabka_ids::ApiKey(19),
                api_version: krabka_ids::ApiVersion(7),
                correlation_id: EmbeddedCorrelationId(1),
                client_id: None,
                header: EmbeddedHeader::Flexible,
                body: &[],
            }
        }
    }

    fn request_frame(setup: EmbeddedFrameSetup<'_>) -> Bytes {
        let EmbeddedFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id,
            header,
            body,
        } = setup;
        wire_request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(api_key.0),
            api_version: krabka_ids::ApiVersion(api_version.0),
            correlation_id: FrameCorrelationId(correlation_id.0),
            client_id: client_id.map(str::as_bytes),
            tagged: (header == EmbeddedHeader::Flexible).then_some(&[0][..]),
            body,
        })
        .freeze()
    }

    /// The embedded header is split off whole, and the body that remains is
    /// the handler's input. The flexible case also proves the tagged-fields
    /// byte is consumed rather than left at the head of the body.
    #[test]
    fn an_embedded_request_is_split_into_its_header_and_body() {
        let incremental = body_of::<
            krabka_protocol::owned::incremental_alter_configs_request::IncrementalAlterConfigsRequest,
        >(1);
        let alter_configs =
            body_of::<krabka_protocol::owned::alter_configs_request::AlterConfigsRequest>(0);
        let cases = [
            (
                "flexible IncrementalAlterConfigs",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(44),
                    api_version: krabka_ids::ApiVersion(1),
                    correlation_id: EmbeddedCorrelationId(77),
                    client_id: Some("adminclient-1"),
                    body: &incremental,
                    ..Default::default()
                }),
                true,
                ForwardedRequest {
                    api_key: 44,
                    api_version: 1,
                    correlation_id: 77,
                    client_id: Some("adminclient-1".to_owned()),
                    body: Bytes::from(incremental),
                    body_flexible: true,
                },
            ),
            (
                "non-flexible AlterConfigs with a null client id",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(33),
                    api_version: krabka_ids::ApiVersion(0),
                    correlation_id: EmbeddedCorrelationId(3),
                    header: EmbeddedHeader::Legacy,
                    body: &alter_configs,
                    ..Default::default()
                }),
                false,
                ForwardedRequest {
                    api_key: 33,
                    api_version: 0,
                    correlation_id: 3,
                    client_id: None,
                    body: Bytes::from(alter_configs),
                    body_flexible: false,
                },
            ),
        ];

        for (case, frame, flexible, want) in cases {
            assert!(let Ok(forwarded) = unwrap_request(&frame, |_, _| flexible, UNSTABLE_OFF),
                "case: {case}"
            );

            check!(forwarded == want, "case: {case}");
        }
    }

    /// `EnvelopeUtils.handleEnvelopeRequest` answers each way an embedded
    /// request can fail with the code its own throw site gives.
    ///
    /// - A header `RequestHeader.parse` refuses is `UNSUPPORTED_VERSION`: a
    ///   frame too short for one, an api key the release does not have (93 and
    ///   up on 4.3.1, trunk's own keys until `unstable.api.versions.enable`),
    ///   and the removed keys 4 to 7.
    /// - A known key outside `ApiKeys.forwardable` is `INVALID_REQUEST`, and
    ///   the forwardable test runs before the body is read.
    /// - A forwardable key whose body does not decode is
    ///   `UNSUPPORTED_VERSION`, since `parseForwardedRequest` rewraps the
    ///   `InvalidRequestException` of `RequestContext.parseRequest`.
    #[test]
    fn an_embedded_request_kafka_would_refuse_gets_the_code_kafka_gives() {
        use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

        let create_topics =
            body_of::<krabka_protocol::owned::create_topics_request::CreateTopicsRequest>(7);
        let unregister_controller = body_of::<
            krabka_protocol::owned::unregister_controller_request::UnregisterControllerRequest,
        >(0);
        let truncated = &create_topics[..create_topics.len() - 1];
        let cases: [(&str, Bytes, _, Result<(), EnvelopeError>); 14] = [
            (
                "a frame too short to hold a header",
                Bytes::from_static(&[0, 19, 0]),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "Produce is not forwardable",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(0),
                    api_version: krabka_ids::ApiVersion(9),
                    body: b"",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::InvalidRequest),
            ),
            (
                "a nested Envelope is not forwardable",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(58),
                    api_version: krabka_ids::ApiVersion(0),
                    body: b"",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::InvalidRequest),
            ),
            (
                "a non-forwardable key is refused before its body is read",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(3),
                    api_version: krabka_ids::ApiVersion(12),
                    body: b"garbage",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::InvalidRequest),
            ),
            (
                "the removed LeaderAndIsr has no valid version",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(4),
                    api_version: krabka_ids::ApiVersion(0),
                    body: b"",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "the removed ControlledShutdown has no valid version",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(7),
                    api_version: krabka_ids::ApiVersion(0),
                    body: b"",
                    ..Default::default()
                }),
                Enabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "an api key past the release's last is unknown",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(93),
                    api_version: krabka_ids::ApiVersion(0),
                    body: b"",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "a negative api key is unknown",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(-1),
                    api_version: krabka_ids::ApiVersion(0),
                    body: b"",
                    ..Default::default()
                }),
                Enabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "trunk's UnregisterController is unknown to 4.3.1",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(94),
                    api_version: krabka_ids::ApiVersion(0),
                    body: &unregister_controller,
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "trunk's UnregisterController is known under the flag",
                request_frame(EmbeddedFrameSetup {
                    api_key: krabka_ids::ApiKey(94),
                    api_version: krabka_ids::ApiVersion(0),
                    body: &unregister_controller,
                    ..Default::default()
                }),
                Enabled,
                Ok(()),
            ),
            (
                "a body that does not decode",
                request_frame(EmbeddedFrameSetup {
                    body: b"garbage",
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "a body cut short",
                request_frame(EmbeddedFrameSetup {
                    body: truncated,
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "a version the request type does not have",
                request_frame(EmbeddedFrameSetup {
                    api_version: krabka_ids::ApiVersion(99),
                    body: &create_topics,
                    ..Default::default()
                }),
                Disabled,
                Err(EnvelopeError::UnsupportedVersion),
            ),
            (
                "a body that decodes",
                request_frame(EmbeddedFrameSetup {
                    body: &create_topics,
                    ..Default::default()
                }),
                Disabled,
                Ok(()),
            ),
        ];

        for (case, frame, unstable, want) in cases {
            check!(
                unwrap_request(&frame, |_, _| true, unstable).map(|_| ()) == want,
                "case: {case}"
            );
        }
    }

    /// `response_data` is the embedded response header followed by the
    /// handler's body, and the header is v1 exactly when the embedded body is
    /// flexible. The correlation id is the *client's*, carried in from the
    /// forwarded header, so the forwarder can relay these bytes untouched.
    #[test]
    fn a_served_response_is_wrapped_with_the_clients_own_response_header() {
        let cases = [
            (
                "flexible body takes a v1 header",
                forwarded(ForwardedResponseSetup {
                    api_key: krabka_ids::ApiKey(44),
                    ..Default::default()
                }),
                vec![0x01, 0x02, 0x03, 0x04, 0x00, b'x'],
            ),
            (
                "non-flexible body takes a v0 header",
                forwarded(ForwardedResponseSetup {
                    api_key: krabka_ids::ApiKey(33),
                    encoding: ForwardedBodyEncoding::Legacy,
                    ..Default::default()
                }),
                vec![0x01, 0x02, 0x03, 0x04, b'x'],
            ),
        ];

        for (case, forwarded, want) in cases {
            check!(
                wrap_response(&forwarded, b"x").as_ref() == want.as_slice(),
                "case: {case}"
            );
        }
    }

    /// A success and a failure round-trip through the generated v0 codec as
    /// whole structs, so a field-order or nullability slip shows up here
    /// rather than at a JVM forwarder.
    #[test]
    fn an_envelope_response_round_trips_through_the_v0_codec() {
        let cases = [
            (
                "served",
                encode_success(Bytes::from_static(b"payload"), 0).expect("encode"),
                EnvelopeResponse {
                    response_data: Some(Bytes::from_static(b"payload")),
                    error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
            (
                "refused",
                encode_failure(EnvelopeError::ClusterAuthorizationFailed, 0).expect("encode"),
                EnvelopeResponse {
                    response_data: None,
                    error_code: 31,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
        ];

        for (case, encoded, want) in cases {
            let mut cur = encoded.as_ref();
            assert!(let Ok(decoded) = EnvelopeResponse::decode(&mut cur, 0),
                "case: {case}"
            );

            check!(decoded == want, "case: {case}");
            check!(cur.is_empty(), "case: {case}");
        }
    }

    /// An `EnvelopeRequest` decodes back into the same struct, and a body that
    /// is not an envelope at all is refused rather than mistaken for one.
    #[test]
    fn an_envelope_request_round_trips_and_a_malformed_one_is_refused() {
        let request = EnvelopeRequest {
            request_data: request_frame(EmbeddedFrameSetup {
                correlation_id: EmbeddedCorrelationId(5),
                client_id: Some("c"),
                body: b"topics",
                ..Default::default()
            }),
            request_principal: Some(Bytes::from_static(JVM_USER_ALICE)),
            client_host_address: Bytes::from_static(&[127, 0, 0, 1]),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut encoded = BytesMut::new();
        request.encode(&mut encoded, 0).expect("encode");

        check!(decode_request(&encoded, 0) == Ok(request));
        // A single byte cannot hold three length-prefixed fields.
        check!(decode_request(&[0x00], 0) == Err(EnvelopeError::InvalidRequest));
    }
}
