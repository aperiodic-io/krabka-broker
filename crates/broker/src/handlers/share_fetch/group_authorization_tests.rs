//! Handler tests for the group `Read` gate on `ShareFetch` and
//! `ShareAcknowledge`.
//!
//! Kafka's `KafkaApis.handleShareFetchRequest` and
//! `KafkaApis.handleShareAcknowledgeRequest` check `Read` on the share group
//! after the feature gate and before the member id, the share session and the
//! topic `Read` checks. A denial answers `getErrorResponse` with
//! `GROUP_AUTHORIZATION_FAILED`: a top-level error code, no topic rows, and an
//! acquisition lock timeout of 0.
//!
//! Each table crosses the group grant with the topic grant. One broker serves
//! every case, and the principal name selects the grants.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::AuthorizationResult,
        broker::{Broker, BrokerHandle},
        codes,
        handlers::test_support::{StartedBroker, create_topic as create_topic_from_test_support},
        share_partition::session::FetchPartitions,
        test_support::{
            decode_response, encode_request, initialize_share_state, peer, principal,
            request_context, start_broker_no_audit_with, try_dispatch_context,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            share_acknowledge_request::{
                AcknowledgePartition, AcknowledgeTopic, ShareAcknowledgeRequest,
            },
            share_acknowledge_response::{
                self, ShareAcknowledgeResponse, ShareAcknowledgeTopicResponse,
            },
            share_fetch_response::{
                self, PartitionData, ShareFetchResponse, ShareFetchableTopicResponse,
            },
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordsPayload,
    },
    std::{net::SocketAddr, sync::Arc},
};

/// The share group that every request names.
const GROUP: &str = "authorized-group";

/// The acquisition lock timeout of the test broker's share-group config.
const LOCK_TIMEOUT_MS: i32 = 30_000;

/// The grants of the principal that sends a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Grants {
    group_read: bool,
    topic_read: bool,
}

impl Grants {
    /// Every combination of the two grants.
    const ALL: [Self; 4] = [
        Self {
            group_read: true,
            topic_read: true,
        },
        Self {
            group_read: true,
            topic_read: false,
        },
        Self {
            group_read: false,
            topic_read: true,
        },
        Self {
            group_read: false,
            topic_read: false,
        },
    ];

    /// The principal name that carries these grants to [`GrantsByName`].
    fn principal_name(self) -> String {
        format!("group-{}-topic-{}", self.group_read, self.topic_read)
    }
}

/// Reads the grants from the principal name that [`Grants::principal_name`]
/// builds. It allows every other request, so that topic creation works.
#[derive(Debug)]
struct GrantsByName;

krabka_macros::test_authorizer!(GrantsByName, request; {
    let Some(grants) = Grants::ALL
        .into_iter()
        .find(|grants| grants.principal_name() == request.principal.name)
    else {
        return AuthorizationResult::Allow;
    };
    let allowed = match (request.resource_type, request.operation) {
        (ResourceType::Group, AclOperation::Read) => {
            grants.group_read && request.resource_name == GROUP
        }
        (ResourceType::Topic, AclOperation::Read) => grants.topic_read,
        _ => false,
    };
    if allowed {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
});

async fn start() -> StartedBroker {
    let HandlerStartedBroker { broker, dir } =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(GrantsByName)).await;
    StartedBroker { broker, dir }
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("share-group-authorization-test")
        .name(name)
        .call()
        .await
}

/// The versions that both RPCs serve.
fn versions() -> [i16; 2] {
    [
        share_fetch_response::MIN_VERSION,
        share_fetch_response::MAX_VERSION,
    ]
}

/// What one case sent and what it got back.
#[derive(Debug, PartialEq)]
struct Outcome<T> {
    version: i16,
    grants: Grants,
    response: T,
}

/// Shared topic, share state, runtime and peer retained through each authorization table.
struct AuthorizationFixture {
    address: SocketAddr,
    shared: Arc<Broker>,
    topic_id: WireUuid,
    _directory: tempfile::TempDir,
    handle: BrokerHandle,
}

async fn authorization_fixture() -> AuthorizationFixture {
    let StartedBroker {
        broker: handle,
        dir: directory,
    } = start().await;
    let topic_id = create_topic(&handle, "share-authorization").await;
    initialize_share_state(&handle, GROUP, uuid::Uuid::from_bytes(topic_id.0), 0).await;
    let shared = handle.broker_arc_for_test();
    let address = peer();
    AuthorizationFixture {
        address,
        shared,
        topic_id,
        _directory: directory,
        handle,
    }
}
#[tokio::test]
async fn share_fetch_checks_group_read_before_topic_read() {
    let fixture = authorization_fixture().await;

    let mut actual: Vec<Outcome<ShareFetchResponse>> = Vec::new();
    let mut expected: Vec<Outcome<ShareFetchResponse>> = Vec::new();
    for version in versions() {
        for grants in Grants::ALL {
            let user = principal(&grants.principal_name());
            let ctx = request_context(&user, &fixture.address, "share-client");
            let request = super::test_fetch_request()
                .group_id(GROUP.into())
                .member_id(format!("member-{version}-{}", grants.principal_name()))
                .topic_id(fixture.topic_id.0)
                .call();
            let response = try_dispatch_context(
                &fixture.shared,
                krabka_protocol::owned::share_fetch_request::API_KEY,
                version,
                &encode_request(&request, version),
                &ctx,
            )
            .await
            .expect("handle share fetch");
            actual.push(Outcome {
                version,
                grants,
                response: decode_response(&response, version),
            });

            let response = if grants.group_read {
                ShareFetchResponse {
                    acquisition_lock_timeout_ms: LOCK_TIMEOUT_MS,
                    responses: vec![ShareFetchableTopicResponse {
                        topic_id: fixture.topic_id,
                        partitions: vec![PartitionData {
                            partition_index: 0,
                            error_code: if grants.topic_read {
                                codes::NONE
                            } else {
                                codes::TOPIC_AUTHORIZATION_FAILED
                            },
                            records: Some(RecordsPayload::Legacy(Bytes::new())),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            } else {
                // `ShareFetchResponse.of(error, throttleTimeMs, empty, List.of(), 0)`.
                krabka_macros::share_group_denial!(ShareFetchResponse)
            };
            expected.push(Outcome {
                version,
                grants,
                response,
            });
        }
    }
    fixture.handle.shutdown().await;

    assert!(actual == expected);
}

#[tokio::test]
async fn share_acknowledge_checks_group_read_before_topic_read() {
    let fixture = authorization_fixture().await;
    let topic = uuid::Uuid::from_bytes(fixture.topic_id.0);

    let mut actual: Vec<Outcome<ShareAcknowledgeResponse>> = Vec::new();
    let mut expected: Vec<Outcome<ShareAcknowledgeResponse>> = Vec::new();
    for version in [
        share_acknowledge_response::MIN_VERSION,
        share_acknowledge_response::MAX_VERSION,
    ] {
        for grants in Grants::ALL {
            let user = principal(&grants.principal_name());
            let ctx = request_context(&user, &fixture.address, "share-client");
            let member = format!("member-{version}-{}", grants.principal_name());
            // Open the share session directly, so that every case acknowledges
            // at epoch 1 in a session that exists.
            fixture
                .shared
                .share_partition_leaders
                .update_fetch_session(
                    (GROUP, &member),
                    ctx.connection_id,
                    0,
                    FetchPartitions {
                        requested: &[(topic, 0)],
                        forgotten: &std::collections::HashSet::new(),
                    },
                    false,
                )
                .expect("open share session");
            let request = ShareAcknowledgeRequest {
                group_id: Some(GROUP.into()),
                member_id: Some(member),
                share_session_epoch: 1,
                topics: vec![AcknowledgeTopic {
                    topic_id: fixture.topic_id,
                    partitions: vec![AcknowledgePartition {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let response = try_dispatch_context(
                &fixture.shared,
                krabka_protocol::owned::share_acknowledge_request::API_KEY,
                version,
                &encode_request(&request, version),
                &ctx,
            )
            .await
            .expect("handle share acknowledge");
            actual.push(Outcome {
                version,
                grants,
                response: decode_response(&response, version),
            });

            let response = if grants.group_read {
                ShareAcknowledgeResponse {
                    // The wire carries the lock timeout from v2 on. An older
                    // version decodes the field's default, 0.
                    acquisition_lock_timeout_ms: if version >= 2 { LOCK_TIMEOUT_MS } else { 0 },
                    responses: vec![ShareAcknowledgeTopicResponse {
                        topic_id: fixture.topic_id,
                        partitions: vec![share_acknowledge_response::PartitionData {
                            partition_index: 0,
                            error_code: if grants.topic_read {
                                codes::NONE
                            } else {
                                codes::TOPIC_AUTHORIZATION_FAILED
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            } else {
                // `ShareAcknowledgeRequest.getErrorResponse` sets the throttle
                // time and the error code, and nothing else.
                krabka_macros::share_group_denial!(ShareAcknowledgeResponse)
            };
            expected.push(Outcome {
                version,
                grants,
                response,
            });
        }
    }
    fixture.handle.shutdown().await;

    assert!(actual == expected);
}
