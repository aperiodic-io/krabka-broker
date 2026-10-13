//! KIP-919 Admin RPC routing for the controller listener.

use {
    crate::{
        api_catalog::is_disabled_version,
        authorizer::{AuthorizationRequest, AuthorizationResult},
        broker::Broker,
        envelope::{self, EnvelopeError, ForwardedRequest},
        error::BrokerError,
        handlers::{
            ApiKeyCode, ApiVersion, CorrelationId, DispatchKind, RequestContext,
            acl_wire::CLUSTER_RESOURCE_NAME,
        },
        network::auth::ConnectionAuth,
    },
    bytes::Bytes,
    krabka_raft::{
        ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
        ControllerAdminRouter, ControllerApiVersion, RaftError,
        controller_api_version as api_version,
    },
    std::sync::{Arc, OnceLock, Weak},
};

/// `Envelope`'s api key. KIP-590 forwarding is the one controller-listener API
/// whose payload is another API's request, so it does not go through the
/// registry lookup the rest of [`SUPPORTED_APIS`] shares.
const ENVELOPE_API_KEY: ApiKeyCode = krabka_protocol::owned::envelope_request::API_KEY;

/// The `RequestContext::connection_id` of every request this router hands a
/// broker handler, whether it arrived on the controller listener directly or
/// inside an `Envelope`. A handler that answers differently on the controller
/// listener, as Kafka's `ControllerApis` does, reads it.
pub(crate) const CONTROLLER_ADMIN_CONNECTION_ID: &str = "controller-admin";

/// The Kafka 4.x controller-listener surface, in `api_key` order.
///
/// Apache Kafka marks each request schema with the listeners that accept it,
/// and a controller's `ApiVersionManager` advertises exactly the ones tagged
/// `controller`. A live `mirror.gcr.io/apache/kafka:4.3.1` controller answers
/// `ApiVersions` with 1, 17-20, 29-33, 36-41, 43-46, 49-60, 62-64, 67, 70, 73
/// and 80-82; 4.0.0's schemas carry the same tags. Kafka trunk adds
/// `UnregisterController` (94, KIP-1312), which krabka-protocol vendors, so it
/// is routed here too.
///
/// This table is that set minus the RPCs the controller listener already
/// answers without a broker handler: `Fetch`, `SaslHandshake`, `ApiVersions`,
/// `SaslAuthenticate`, the KIP-595 quorum RPCs, `FetchSnapshot`,
/// `DescribeCluster`, controller registration, and the KIP-853 voter RPCs. What remains is the subset that
/// reuses a broker handler, which is what this router bridges to.
///
/// `BrokerRegistration` and `BrokerHeartbeat` are not Admin APIs, and they
/// belong for the same reason the Admin subset does: KIP-919 puts them on the
/// controller listener, and only the broker crate holds what answering them
/// takes. Registration asks the heartbeat registry whether the previous
/// incarnation still holds a session, and it withdraws the ISR and ELR seats of
/// a broker that cannot prove a clean restart. The heartbeat drives the
/// registry that decides fencing, the KIP-112 offline-dir failover, and the
/// controlled-shutdown drain. Routing them here is what keeps a broker that
/// registers or heartbeats to a controller-only node from being answered by a
/// handler that does none of that.
///
/// `Envelope` is the other entry that is not an Admin API, and it is the one
/// whose payload is another API's request rather than a body a handler reads,
/// so [`serve_envelope`] answers it instead of the registry lookup the rest
/// share. `ApiKeys.ENVELOPE.messageType.listeners()` in
/// `kafka-clients-4.3.1.jar` is exactly `[CONTROLLER]`, so it is advertised
/// here and never on the client listener.
///
/// `AlterPartition` and `AllocateProducerIds` are inter-broker RPCs that a
/// Kafka broker sends to the active controller over its controller listener,
/// so they are routed here as well. A broker that does not find them in this
/// listener's `ApiVersions` table fails them with `UnsupportedVersionException`.
///
/// `DescribeClientQuotas` is deliberately absent for a different reason: its
/// schema is tagged `broker` only, so a Kafka controller neither advertises
/// nor answers it, and neither does this listener.
const SUPPORTED_APIS: &[ControllerApiVersion] = &[
    api_version!(create_topics_request),
    api_version!(delete_topics_request),
    api_version!(describe_acls_request),
    api_version!(create_acls_request),
    api_version!(delete_acls_request),
    api_version!(describe_configs_request),
    api_version!(alter_configs_request),
    api_version!(create_partitions_request),
    api_version!(create_delegation_token_request),
    api_version!(renew_delegation_token_request),
    api_version!(expire_delegation_token_request),
    api_version!(describe_delegation_token_request),
    api_version!(elect_leaders_request),
    api_version!(alter_partition_request),
    api_version!(incremental_alter_configs_request),
    api_version!(alter_partition_reassignments_request),
    api_version!(list_partition_reassignments_request),
    api_version!(alter_client_quotas_request),
    api_version!(describe_user_scram_credentials_request),
    api_version!(alter_user_scram_credentials_request),
    api_version!(update_features_request),
    api_version!(envelope_request),
    api_version!(broker_registration_request),
    api_version!(broker_heartbeat_request),
    api_version!(unregister_broker_request),
    api_version!(allocate_producer_ids_request),
    api_version!(assign_replicas_to_dirs_request),
    api_version!(unregister_controller_request),
];

/// Late-bound bridge from the controller, which starts before the broker, to
/// the broker's single Admin handler registry. The weak pointer avoids a
/// controller -> router -> broker -> controller ownership cycle.
pub(crate) struct BrokerControllerAdminRouter {
    broker: OnceLock<Weak<Broker>>,
}

impl BrokerControllerAdminRouter {
    pub(crate) const fn new() -> Self {
        Self {
            broker: OnceLock::new(),
        }
    }

    pub(crate) fn bind(&self, broker: &Arc<Broker>) -> Result<(), &'static str> {
        self.broker
            .set(Arc::downgrade(broker))
            .map_err(|_| "controller Admin router already bound")
    }
}

impl ControllerAdminRouter for BrokerControllerAdminRouter {
    fn api_versions(&self) -> &[ControllerApiVersion] { SUPPORTED_APIS }

    fn route(&self, request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
        Box::pin(async move {
            let Some(version) = SUPPORTED_APIS
                .iter()
                .find(|version| version.api_key == request.api_key)
                .copied()
            else {
                // Kafka's AdminClient rejects APIs outside the KIP-919
                // controller surface locally. A raw disabled API is rejected
                // by the controller listener before dispatch, which closes the
                // connection rather than inventing a response body. Send a
                // `Metadata` request to a live
                // `mirror.gcr.io/apache/kafka:4.3.1` controller listener and
                // it half-closes too: no error frame comes back.
                return Ok(None);
            };
            if !(version.min_version..=version.max_version).contains(&request.api_version) {
                // The listener likewise closes raw requests whose version is
                // outside the advertised range. The client normally prevents
                // these through ApiVersions negotiation.
                return Ok(None);
            }
            let broker =
                self.broker.get().and_then(Weak::upgrade).ok_or_else(|| {
                    RaftError::ControllerAdmin("broker startup is incomplete".into())
                })?;

            let principal = outer_principal(&request);
            if request.api_key == ENVELOPE_API_KEY {
                return Ok(Some(ControllerAdminResponse {
                    body: serve_envelope(&broker, &request, &principal).await?,
                    // `EnvelopeResponse` is flexible at every valid version.
                    flexible: request.api_version >= version.flexible_min,
                }));
            }

            let body = invoke_registered_handler(&broker, Invocation {
                api_key: request.api_key,
                api_version: request.api_version,
                correlation_id: request.correlation_id,
                body: &request.body,
                client_id: request.client_id.as_deref(),
                peer: &request.peer,
                principal: &principal,
                authenticated_via_token: request.authenticated_via_token,
                forwarded: false,
            })
            .await
            .map_err(|error| RaftError::ControllerAdmin(error.to_string()))?;

            Ok(Some(ControllerAdminResponse {
                body,
                flexible: request.api_version >= version.flexible_min,
            }))
        })
    }
}

/// The identity of the peer that opened the controller-listener connection.
///
/// The SASL principal, or the mTLS principal of an `SSL` connection. A peer
/// with neither runs as `ANONYMOUS`, the same default a broker listener
/// applies.
fn outer_principal(request: &ControllerAdminRequest) -> krabka_security::Principal {
    request
        .principal
        .clone()
        .unwrap_or_else(|| krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: Vec::new(),
        })
}

/// One request handed to a registered broker handler, whether it arrived on
/// the controller listener directly or inside a KIP-590 `Envelope`.
struct Invocation<'a> {
    api_key: ApiKeyCode,
    api_version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    client_id: Option<&'a str>,
    /// The address the handler authorizes and audits against. For a forwarded
    /// request this is the *client's* address, out of
    /// `EnvelopeRequest.client_host_address`, not the forwarding hop's.
    peer: &'a std::net::SocketAddr,
    /// The identity the handler authorizes against. For a forwarded request
    /// this is the *client's* principal, not the forwarding hop's.
    principal: &'a krabka_security::Principal,
    authenticated_via_token: bool,
    forwarded: bool,
}

/// Dispatch `invocation` through the broker's Admin handler registry.
///
/// Shared by the direct controller-listener path and the `Envelope` path so a
/// forwarded `IncrementalAlterConfigs` runs the same handler, under the same
/// `RequestContext`, as one that arrived unwrapped.
async fn invoke_registered_handler(
    broker: &std::sync::Arc<Broker>,
    invocation: Invocation<'_>,
) -> Result<Bytes, BrokerError> {
    let Invocation {
        api_key,
        api_version,
        correlation_id,
        body,
        client_id,
        peer,
        principal,
        authenticated_via_token,
        forwarded,
    } = invocation;
    let entry = broker
        .handlers()
        .get(api_key)
        .ok_or_else(|| BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        })?;
    match entry.kind() {
        // Every krabka-registered controller-listener handler is a context
        // dispatch, so it receives a principal to authorize: `AssignReplicasToDirs`
        // (73) checks `ClusterAction` against the outer connection's identity
        // straight off the listener, and `AllocateProducerIds` (67), which a
        // JVM broker forwards in an `Envelope`, checks it against the
        // principal the envelope names.
        DispatchKind::Context(handler) => {
            let mut context = RequestContext::new(
                principal,
                peer,
                client_id,
                CONTROLLER_ADMIN_CONNECTION_ID,
                false,
                "CONTROLLER",
            )
            .with_correlation_id(correlation_id);
            context.forwarded = forwarded;
            handler(broker, api_version, body, &context).await
        }
        DispatchKind::Auth(handler) => {
            let auth = ConnectionAuth::Authenticated {
                principal: principal.clone(),
                mechanism: krabka_security::SaslMechanism::Plain,
                expires_at_ms: None,
                authenticated_via_token,
            };
            handler(broker, api_version, correlation_id, body, &auth, peer).await
        }
        _ => Err(BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        }),
    }
}

/// Serve one KIP-590 `Envelope` and return the encoded `EnvelopeResponse`
/// body.
///
/// A refusal is reported inside the envelope, as an `error_code` beside a null
/// `response_data`, which is how `kafka.server.EnvelopeUtils` answers: the
/// forwarding hop needs a well-formed envelope back so it can translate the
/// code for its own client. Only a failure of the embedded *handler* — which
/// is a broker fault, not a protocol one — escapes as a `RaftError`, and that
/// closes the connection, matching every other controller-admin API here.
async fn serve_envelope(
    broker: &std::sync::Arc<Broker>,
    request: &ControllerAdminRequest,
    outer: &krabka_security::Principal,
) -> Result<Bytes, RaftError> {
    let version = request.api_version;
    let encoded = match unwrap_envelope(broker, request, outer) {
        Ok(Unwrapped {
            forwarded,
            principal,
            client_host,
            token_authenticated,
        }) => {
            let body = invoke_registered_handler(broker, Invocation {
                api_key: forwarded.api_key,
                api_version: forwarded.api_version,
                correlation_id: forwarded.correlation_id,
                body: &forwarded.body,
                client_id: forwarded.client_id.as_deref(),
                // The *client's* address, not this connection's: the peer
                // here is the forwarding broker, and authorizing or
                // auditing the embedded request against that address both
                // denies clients a host ACL allows and allows clients it
                // denies. `EnvelopeUtils` builds its inner
                // `RequestContext` from `client_host_address` for exactly
                // this reason.
                peer: &client_host,
                principal: &principal,
                // The *client's* flag, out of `request_principal`, not the
                // forwarding hop's: KIP-48 forbids a token-authenticated
                // caller from minting or renewing another token, and that
                // rule has to follow the identity it belongs to.
                authenticated_via_token: token_authenticated,
                forwarded: true,
            })
            .await
            .map_err(|error| RaftError::ControllerAdmin(error.to_string()))?;
            tracing::debug!(
                api_key = forwarded.api_key,
                api_version = forwarded.api_version,
                principal = %principal.name,
                "served a forwarded request from an Envelope"
            );
            envelope::encode_success(envelope::wrap_response(&forwarded, &body), version)
        }
        Err(error) => {
            tracing::warn!(?error, "refused an Envelope");
            envelope::encode_failure(error, version)
        }
    };
    encoded.map_err(|error| RaftError::ControllerAdmin(error.to_string()))
}

/// One `Envelope` that passed the `ClusterAction` gate and decoded cleanly:
/// the session the embedded request runs under, and the request itself.
struct Unwrapped {
    forwarded: ForwardedRequest,
    /// The client identity out of `request_principal`, not the forwarding
    /// hop's.
    principal: krabka_security::Principal,
    /// The client address out of `client_host_address`, not the forwarding
    /// hop's. KIP-590 carries the bare address octets and no port — Kafka's
    /// inner `RequestContext` holds an `InetAddress` — so the port is zero
    /// and only [`std::net::SocketAddr::ip`] is meaningful, which is all any
    /// host ACL or audit record reads.
    client_host: std::net::SocketAddr,
    /// Whether that client authenticated with a delegation token.
    token_authenticated: bool,
}

/// Authorize, decode and validate one `Envelope`.
///
/// The order of the checks is `EnvelopeUtils.handleEnvelopeRequest`'s own:
/// principal, then client address, then the embedded header, then the
/// forwardable test. A malformed envelope that fails two of them reports the
/// code Kafka's first failure would.
fn unwrap_envelope(
    broker: &Broker,
    request: &ControllerAdminRequest,
    outer: &krabka_security::Principal,
) -> Result<Unwrapped, EnvelopeError> {
    // `ApiKeys.ENVELOPE.clusterAction` is true: only a peer that holds
    // `ClusterAction` on the cluster resource may speak for another identity.
    let image = broker.controller.current_image();
    if AuthorizationResult::Deny
        == broker
            .config
            .authorizer
            .authorize(image.as_ref(), &AuthorizationRequest {
                principal: outer,
                host: &request.peer,
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
                operation: krabka_metadata::AclOperation::ClusterAction,
            })
    {
        return Err(EnvelopeError::ClusterAuthorizationFailed);
    }

    let envelope = envelope::decode_request(&request.body, request.api_version)?;
    let forwarded_principal =
        envelope::deserialize_principal(envelope.request_principal.as_deref())?;
    let client_host = envelope::deserialize_client_host_address(&envelope.client_host_address)?;
    let forwarded = envelope::unwrap_request(
        &envelope.request_data,
        |api_key, api_version| broker.handlers().body_flexible(api_key, api_version),
        broker.config.features.unstable_api_versions,
    )?;
    // The embedded key is forwardable in Kafka's table; it still has to be one
    // this broker serves, at a version it serves, before a handler sees it.
    let entry = broker
        .handlers()
        .get(forwarded.api_key)
        .ok_or(EnvelopeError::InvalidRequest)?;
    if !entry.supports_version(forwarded.api_version) {
        return Err(EnvelopeError::UnsupportedVersion);
    }
    // An api key Kafka 4.3.1 does not have -- trunk's `UnregisterController`
    // (94) while `unstable.api.versions.enable` is off -- fails
    // `RequestHeader.parse` on a 4.3.1 controller, and `EnvelopeUtils` reports
    // an unparseable embedded header as UNSUPPORTED_VERSION. The same
    // answer covers a version the listener's gates disable.
    if is_disabled_version(
        forwarded.api_key,
        forwarded.api_version,
        broker.config.features.version_gates(),
    ) {
        return Err(EnvelopeError::UnsupportedVersion);
    }

    Ok(Unwrapped {
        forwarded,
        principal: krabka_security::Principal {
            name: forwarded_principal.name,
            // `KafkaPrincipal` carries no mechanism and no groups, so the
            // reconstructed session keeps the forwarding hop's own auth method
            // and an empty group list. Only the admin audit trail reads the
            // method, and no authorizer reads the groups.
            auth_method: outer.auth_method,
            groups: Vec::new(),
        },
        client_host: std::net::SocketAddr::new(client_host, 0),
        token_authenticated: forwarded_principal.token_authenticated,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::test_support::start_controller, assert2::check, bytes::Bytes,
        std::collections::BTreeSet,
    };

    /// The Kafka 4.x controller-listener set, as a live
    /// `mirror.gcr.io/apache/kafka:4.3.1` controller advertises it (the same
    /// set the 4.0.0 request schemas tag `controller`), minus the keys the
    /// controller listener answers without this router.
    #[test]
    fn supported_set_matches_the_kafka_controller_listener_surface() {
        let keys: BTreeSet<_> = SUPPORTED_APIS.iter().map(|api| api.api_key).collect();

        check!(keys.len() == SUPPORTED_APIS.len());
        check!(
            keys == maplit::btreeset! {
                19, 20, 29, 30, 31, 32, 33, 37, 38, 39, 40, 41, 43, 44, 45, 46, 49, 50, 51, 56, 57,
                58, 62, 63, 64, 67, 73, 94,
            }
        );
    }

    /// `Envelope` is advertised at the versions `kafka-clients-4.3.1.jar`
    /// publishes: `ApiKeys.ENVELOPE` is `0..=0`, flexible from 0. The
    /// controller listener merges this row into its `ApiVersions` table and
    /// frames the request header from `flexible_min`, so a wrong minimum here
    /// eats the first byte of the envelope body.
    #[test]
    fn envelope_is_advertised_at_the_versions_kafka_publishes() {
        let envelope = SUPPORTED_APIS
            .iter()
            .find(|api| api.api_key == ENVELOPE_API_KEY)
            .copied();

        check!(
            envelope
                == Some(ControllerApiVersion {
                    api_key: 58,
                    min_version: 0,
                    max_version: 0,
                    released_max: Some(0),
                    flexible_min: 0,
                })
        );
    }
    fn request(api_key: i16, api_version: i16) -> ControllerAdminRequest {
        ControllerAdminRequest {
            api_key,
            api_version,
            correlation_id: 1,
            client_id: None,
            body: Bytes::new(),
            peer: "127.0.0.1:9093".parse().unwrap(),
            principal: None,
            authenticated_via_token: false,
        }
    }

    #[tokio::test]
    async fn unsupported_api_falls_through_before_broker_binding() {
        let router = BrokerControllerAdminRouter::new();

        check!(router.route(request(2, 0)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn unsupported_version_falls_through_before_broker_binding() {
        let router = BrokerControllerAdminRouter::new();
        let update_features = SUPPORTED_APIS.iter().find(|api| api.api_key == 57).unwrap();

        check!(
            router
                .route(request(57, update_features.max_version + 1))
                .await
                .unwrap()
                .is_none()
        );
    }

    /// The controller listener serves before the broker binds itself to this
    /// router -- the weak pointer exists because the controller starts first --
    /// so a supported key can arrive inside that window. It has to come back as
    /// an error the listener can report, not as an unwrap on an absent broker.
    #[tokio::test]
    async fn a_supported_key_before_the_broker_binds_is_reported_as_an_error() {
        let router = BrokerControllerAdminRouter::new();
        let update_features = SUPPORTED_APIS.iter().find(|api| api.api_key == 57).unwrap();

        let error = router
            .route(request(57, update_features.max_version))
            .await
            .expect_err("an unbound router cannot serve a supported key");

        check!(error.to_string() == "controller admin dispatch: broker startup is incomplete");
    }

    /// `AssignReplicasToDirs` is a context dispatch that reaches the broker
    /// registry straight off the listener, rather than through an
    /// `Envelope`. Route it, with the reporting broker's actual epoch,
    /// against a bound broker and decode what comes back, which is what
    /// proves the arm hands the body to the registry -- authorized by the
    /// outer connection's `ANONYMOUS` principal, which the default
    /// `AllowAllAuthorizer` admits -- rather than falling through to the
    /// incompatible-kind error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_context_kind_api_key_routes_to_the_broker_registry() {
        use krabka_protocol::{Decode as _, Encode as _};

        let (controller, _controller_addr, _dir) = start_controller().await;
        let handle = controller;

        let router = BrokerControllerAdminRouter::new();
        router.bind(handle.broker_for_test()).expect("bind once");

        let api_key = krabka_protocol::owned::assign_replicas_to_dirs_request::API_KEY;
        let api_version = krabka_protocol::owned::assign_replicas_to_dirs_request::MAX_VERSION;
        let broker_epoch = handle
            .broker_arc_for_test()
            .controller
            .current_image()
            .broker_epoch(krabka_raft::NodeId(1))
            .expect("broker 1 is self-registered");
        let mut body = bytes::BytesMut::new();
        krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch,
            directories: Vec::new(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }
        .encode(&mut body, api_version)
        .expect("encode the request body");

        let answer = router
            .route(ControllerAdminRequest {
                body: body.freeze(),
                ..request(api_key, api_version)
            })
            .await
            .expect("the context arm routes without error")
            .expect("a routed key answers with a body");

        let mut cursor = answer.body.clone();
        let decoded = krabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse::decode(
            &mut cursor,
            api_version,
        )
        .expect("decode the routed response");

        check!(answer.flexible);
        check!(
            decoded
                == krabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse::default()
        );
        handle.shutdown().await;
    }
}
