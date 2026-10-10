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

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    owned::{
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, ShareAcknowledgeRequest,
        },
        share_acknowledge_response::{
            self, ShareAcknowledgeResponse, ShareAcknowledgeTopicResponse,
        },
        share_fetch_request::{FetchPartition, FetchTopic, ShareFetchRequest},
        share_fetch_response::{
            self, PartitionData, ShareFetchResponse, ShareFetchableTopicResponse,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use crate::{
    authorizer::AuthorizationResult,
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, peer, principal, request_context,
        start_broker_no_audit_with,
    },
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

test_authorizer!(GrantsByName, (self, _source, request), {
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

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(GrantsByName)).await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    crate::handlers::test_support::create_topic(
        broker,
        crate::handlers::test_support::ClientTopicSetup {
            client_id: "share-group-authorization-test",
            name,
            ..Default::default()
        },
    )
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
type Outcome<T> = (i16, Grants, T);

/// Shared topic, share state, runtime and peer; locals remain in each test scope.
macro_rules! authorization_fixture {
    (($broker:ident, $dir:ident, $topic_id:ident, $shared:ident, $address:ident)) => {
        let ($broker, $dir) = start().await;
        let $topic_id = create_topic(&$broker, "share-authorization").await;
        crate::test_support::initialize_share_state(
            &$broker,
            GROUP,
            uuid::Uuid::from_bytes($topic_id.0),
            0,
        )
        .await;
        let $shared = $broker.broker_arc_for_test();
        let $address = peer();
    };
}

/// Each API's independent error model pins all fields left empty by a group denial.
macro_rules! expected_group_denial {
    ($response:ident) => {
        $response {
            throttle_time_ms: 0,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            error_message: None,
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            ..Default::default()
        }
    };
}

#[tokio::test]
async fn share_fetch_checks_group_read_before_topic_read() {
    authorization_fixture!((broker, _dir, topic_id, shared, address));

    let mut actual: Vec<Outcome<ShareFetchResponse>> = Vec::new();
    let mut expected: Vec<Outcome<ShareFetchResponse>> = Vec::new();
    for version in versions() {
        for grants in Grants::ALL {
            let user = principal(&grants.principal_name());
            let ctx = request_context(&user, &address, "share-client");
            let request = ShareFetchRequest {
                group_id: Some(GROUP.into()),
                member_id: Some(format!("member-{version}-{}", grants.principal_name())),
                share_session_epoch: 0,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 1_048_576,
                max_records: 10,
                batch_size: 10,
                topics: vec![FetchTopic {
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition_index: 0,
                        partition_max_bytes: 1_048_576,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let response = crate::test_support::try_dispatch_context(
                &shared,
                krabka_protocol::owned::share_fetch_request::API_KEY,
                version,
                &encode_request(&request, version),
                &ctx,
            )
            .await
            .expect("handle share fetch");
            actual.push((version, grants, decode_response(&response, version)));

            let response = if grants.group_read {
                ShareFetchResponse {
                    acquisition_lock_timeout_ms: LOCK_TIMEOUT_MS,
                    responses: vec![ShareFetchableTopicResponse {
                        topic_id,
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
                expected_group_denial!(ShareFetchResponse)
            };
            expected.push((version, grants, response));
        }
    }
    broker.shutdown().await;

    assert!(actual == expected);
}

#[tokio::test]
async fn share_acknowledge_checks_group_read_before_topic_read() {
    authorization_fixture!((broker, _dir, topic_id, shared, address));
    let topic = uuid::Uuid::from_bytes(topic_id.0);

    let mut actual: Vec<Outcome<ShareAcknowledgeResponse>> = Vec::new();
    let mut expected: Vec<Outcome<ShareAcknowledgeResponse>> = Vec::new();
    for version in [
        share_acknowledge_response::MIN_VERSION,
        share_acknowledge_response::MAX_VERSION,
    ] {
        for grants in Grants::ALL {
            let user = principal(&grants.principal_name());
            let ctx = request_context(&user, &address, "share-client");
            let member = format!("member-{version}-{}", grants.principal_name());
            // Open the share session directly, so that every case acknowledges
            // at epoch 1 in a session that exists.
            shared
                .share_partition_leaders
                .update_fetch_session(
                    (GROUP, &member),
                    ctx.connection_id,
                    0,
                    crate::share_partition::session::FetchPartitions {
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
                    topic_id,
                    partitions: vec![AcknowledgePartition {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let response = crate::test_support::try_dispatch_context(
                &shared,
                krabka_protocol::owned::share_acknowledge_request::API_KEY,
                version,
                &encode_request(&request, version),
                &ctx,
            )
            .await
            .expect("handle share acknowledge");
            actual.push((version, grants, decode_response(&response, version)));

            let response = if grants.group_read {
                ShareAcknowledgeResponse {
                    // The wire carries the lock timeout from v2 on. An older
                    // version decodes the field's default, 0.
                    acquisition_lock_timeout_ms: if version >= 2 { LOCK_TIMEOUT_MS } else { 0 },
                    responses: vec![ShareAcknowledgeTopicResponse {
                        topic_id,
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
                expected_group_denial!(ShareAcknowledgeResponse)
            };
            expected.push((version, grants, response));
        }
    }
    broker.shutdown().await;

    assert!(actual == expected);
}
