//! Adapters for the apis that authorize against the connection itself: they
//! receive the `ConnectionAuth` and the peer address rather than a
//! [`RequestContext`].
//!
//! `AlterReplicaLogDirs` needs the principal to build a per-partition
//! authorization-failure response, and the four delegation-token apis create,
//! renew, expire and describe tokens for the authenticated principal.
//!
//! [`RequestContext`]: crate::handlers::RequestContext

use {
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult},
        broker::Broker,
        codes::{CLUSTER_AUTHORIZATION_FAILED, NONE},
        error::BrokerError,
        handlers::{
            ApiVersion, CorrelationId, acl_wire::CLUSTER_RESOURCE_NAME,
            alter_replica_log_dirs::handle, audit_admin_for, encode_response,
        },
        network::auth::ConnectionAuth,
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_units::convert::TimeExt as _,
};

pub(super) fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::{
            Decode,
            owned::{
                alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
                alter_replica_log_dirs_response::{
                    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                    AlterReplicaLogDirsResponse,
                },
            },
        };

        let anonymous;
        let principal = if let Some(principal) = auth.principal() {
            principal
        } else {
            anonymous = krabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: krabka_security::AuthMethod::Anonymous,
                groups: vec![],
            };
            &anonymous
        };

        let image = broker.controller.current_image();
        let authorized = broker
            .config
            .authorizer
            .authorize(&*image, &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
                operation: krabka_metadata::AclOperation::Alter,
            })
            == AuthorizationResult::Allow;

        if !authorized {
            let mut cur = body;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            // Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` walks the
            // request as sent: one topic result per (directory, topic) pair,
            // in request order, with every listed partition set to the same
            // error code. It never merges or sorts entries, so a topic named
            // under two directories gets two results here too.
            let results = req
                .dirs
                .into_iter()
                .flat_map(|dir| dir.topics)
                .map(|topic| AlterReplicaLogDirTopicResult {
                    topic_name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition_index| AlterReplicaLogDirPartitionResult {
                            partition_index,
                            error_code: CLUSTER_AUTHORIZATION_FAILED,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            return encode_response(&resp, version);
        }

        handle(broker, version, correlation_id, body).await
    })
}

/// Token adapters all decode first and encode the resulting response at the
/// negotiated version. The supplied body retains each operation's auth and audit order.
macro_rules! token_adapter {
    (
        $name:ident,
        $request_ty:path,($broker:ident, $request:ident, $auth:ident, $peer:ident),
        $body:block
    ) => {
        pub(super) fn $name<'a>(
            $broker: &'a Broker,
            version: ApiVersion,
            _correlation_id: CorrelationId,
            body: &'a [u8],
            $auth: &'a crate::network::auth::ConnectionAuth,
            $peer: &'a std::net::SocketAddr,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(async move {
                use krabka_protocol::Decode;
                let mut cursor = body;
                let $request = <$request_ty>::decode(&mut cursor, version)?;
                let response = $body;
                crate::handlers::encode_response(&response, version)
            })
        }
    };
}

token_adapter!(
    create_delegation_token_adapter,
    krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest,
    (broker, req, auth, peer),
    {
        let resp = crate::handlers::create_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_max_lifetime.millis_i64(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
            crate::handlers::create_delegation_token::TokenAcl {
                authorizer: broker.config.authorizer.as_ref(),
                peer,
            },
        )
        .await;
        if resp.error_code == crate::codes::NONE {
            audit_token_operation(broker, auth, peer, "CreateDelegationToken", &resp.token_id);
        }
        resp
    }
);

token_adapter!(
    renew_delegation_token_adapter,
    krabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest,
    (broker, req, auth, peer),
    {
        // Resolve the token id before the mutation: the response carries only
        // the new expiry, and a delete leaves nothing to look up afterwards.
        let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
        let resp = crate::handlers::renew_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
        )
        .await;
        audit_token_mutation(
            broker,
            auth,
            peer,
            "RenewDelegationToken",
            token_id.as_deref(),
            resp.error_code,
        );
        resp
    }
);

token_adapter!(
    expire_delegation_token_adapter,
    krabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest,
    (broker, req, auth, peer),
    {
        let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
        let resp = crate::handlers::expire_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
        )
        .await;
        audit_token_mutation(
            broker,
            auth,
            peer,
            "ExpireDelegationToken",
            token_id.as_deref(),
            resp.error_code,
        );
        resp
    }
);

token_adapter!(
    describe_delegation_token_adapter,
    krabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest,
    (broker, req, auth, peer),
    {
        crate::handlers::describe_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            peer,
            broker.config.authorizer.as_ref(),
        )
    }
);

fn audit_token_mutation(
    broker: &Broker,
    auth: &ConnectionAuth,
    peer: &std::net::SocketAddr,
    operation: &str,
    token_id: Option<&str>,
    error_code: i16,
) {
    if error_code == NONE
        && let Some(token_id) = token_id
    {
        audit_token_operation(broker, auth, peer, operation, token_id);
    }
}

/// The token id behind an `hmac`, read from the current metadata image.
///
/// `RenewDelegationToken` and `ExpireDelegationToken` address a token by its
/// HMAC, and neither response echoes an id back. The audit record names the
/// id, never the HMAC: the HMAC *is* the token's password equivalent, and an
/// audit topic an auditor can read is not a place to keep one.
fn token_id_for_hmac(broker: &Broker, hmac: &[u8]) -> Option<String> {
    let secret_key = broker.config.delegation_token_secret_key.as_ref()?;
    broker
        .controller
        .current_image()
        .delegation_token_by_hmac(secret_key.as_bytes(), hmac)
        .map(|token| token.token_id.clone())
}

/// Emits the `AdminOperation` record for a delegation-token mutation.
///
/// These apis authorize against the connection, so the principal comes from
/// the `ConnectionAuth` rather than from a `RequestContext`. An unauthenticated
/// connection never reaches a success path here, so there is nothing to audit
/// for one.
fn audit_token_operation(
    broker: &Broker,
    auth: &ConnectionAuth,
    peer: &std::net::SocketAddr,
    operation: &str,
    token_id: &str,
) {
    if let ConnectionAuth::Authenticated { principal, .. } = auth {
        audit_admin_for(
            broker.audit_log.as_ref(),
            principal,
            peer,
            operation,
            krabka_audit::AuditOutcome::Success,
            vec![crate::handlers::audit_resource("DelegationToken", token_id)],
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::CLUSTER_AUTHORIZATION_FAILED,
            network::auth::ConnectionAuth,
            test_support::{DenyAll, codec_helpers, peer, principal, start_broker_with_authorizer},
        },
        krabka_protocol::owned::{
            alter_replica_log_dirs_request::{
                AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
            },
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
        },
        krabka_security::SaslMechanism,
        std::sync::Arc,
    };

    codec_helpers!(AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse);

    fn authed(name: &str) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: principal(name),
            mechanism: SaslMechanism::Plain,
            expires_at_ms: None,
            authenticated_via_token: false,
        }
    }

    fn topic_result(name: &str, partitions: &[i32]) -> AlterReplicaLogDirTopicResult {
        AlterReplicaLogDirTopicResult {
            topic_name: name.to_string(),
            partitions: partitions
                .iter()
                .map(|&partition_index| AlterReplicaLogDirPartitionResult {
                    partition_index,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` produces one
    /// topic result per (directory, topic) pair, in request order, never
    /// merging or sorting them by topic name. A denied `Cluster.Alter` must
    /// match that shape exactly, including when the same topic is named
    /// under two directories or when topics are listed out of name order.
    #[tokio::test]
    async fn denial_emits_one_row_per_dir_and_topic_in_request_order() {
        let version = 2;
        for (case, dirs, expected_results) in [
            (
                "one dir, one topic",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![AlterReplicaLogDirTopic {
                        name: "orders".into(),
                        partitions: vec![0, 1],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                vec![topic_result("orders", &[0, 1])],
            ),
            (
                "same topic under two dirs stays two rows",
                vec![
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data0".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data1".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                vec![topic_result("orders", &[0]), topic_result("orders", &[1])],
            ),
            (
                "topics in reverse name order are not sorted",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![
                        AlterReplicaLogDirTopic {
                            name: "widgets".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                        AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                vec![topic_result("widgets", &[0]), topic_result("orders", &[0])],
            ),
        ] {
            broker_fixture!(
                (broker_handle, _dir, broker),
                start_broker_with_authorizer(Arc::new(DenyAll))
            );
            let auth = authed("alice");
            let peer = peer();
            let req = AlterReplicaLogDirsRequest {
                dirs,
                ..Default::default()
            };
            let body = encode_request(&req, version);

            let bytes = alter_replica_log_dirs_adapter(&broker, version, 0, &body, &auth, &peer)
                .await
                .expect("adapter");
            let resp = decode_response(&bytes, version);

            let expected = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results: expected_results,
                ..Default::default()
            };
            assert2::check!(resp == expected, "case {case}");
            broker_handle.shutdown().await;
        }
    }
}
