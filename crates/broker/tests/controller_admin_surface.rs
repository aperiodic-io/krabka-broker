//! KIP-919: the Admin surface the controller listener answers.
//!
//! Apache Kafka tags every request schema with the listeners that accept it,
//! and `ControllerApis` answers exactly the ones tagged `controller`. Krabka
//! bridges that set onto the broker's own handler registry, so these cases
//! drive each api key the bridge routes over a raw controller connection and
//! assert the decoded response, plus the `ApiVersions` surface the same
//! listener advertises.
//!
//! The cases here cover the keys the bridge did not route before: the topic
//! lifecycle (`CreateTopics`, `CreatePartitions`, `DeleteTopics`), the three
//! writing delegation-token RPCs, the SCRAM write path, and
//! `AssignReplicasToDirs`. Of the keys the bridge already routed,
//! `client_admin_controller_bootstrap` drives `DescribeConfigs`, and the
//! official Kafka tools in `jvm_bootstrap_controller` drive `DescribeConfigs`,
//! `IncrementalAlterConfigs`, the three ACL RPCs and
//! `ListPartitionReassignments`.

mod support;

use {
    crate::support::{
        configs::{controller_registration, feature_update},
        discovery::api_versions_request_for,
        sasl::{ScramAlgorithm, scram_upsertion},
        start_with_bound_listeners,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId, config::NodeRole},
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_protocol::{
        ProtocolRequest, UnknownTaggedFields,
        owned::{
            allocate_producer_ids_request::AllocateProducerIdsRequest,
            allocate_producer_ids_response::AllocateProducerIdsResponse,
            alter_client_quotas_request::AlterClientQuotasRequest,
            alter_configs_request::AlterConfigsRequest,
            alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
            alter_partition_request::{
                AlterPartitionRequest, BrokerState, PartitionData as AlterPartitionPartitionData,
                TopicData as AlterPartitionTopicData,
            },
            alter_partition_response::{
                AlterPartitionResponse, PartitionData as AlterPartitionResultPartition,
                TopicData as AlterPartitionResultTopic,
            },
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            alter_user_scram_credentials_response::{
                AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
            },
            api_versions_request::ApiVersionsRequest,
            api_versions_response::ApiVersionsResponse,
            assign_replicas_to_dirs_request::{
                AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
            },
            assign_replicas_to_dirs_response::{
                AssignReplicasToDirsResponse, DirectoryData as RespDirectoryData,
                PartitionData as RespPartitionData, TopicData as RespTopicData,
            },
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_registration_request::{
                BrokerRegistrationRequest, Feature as RegistrationFeature,
                Listener as RegistrationListener,
            },
            create_acls_request::CreateAclsRequest,
            create_delegation_token_request::CreateDelegationTokenRequest,
            create_delegation_token_response::CreateDelegationTokenResponse,
            create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
            create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
            create_topics_request::CreateTopicsRequest,
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            delete_acls_request::DeleteAclsRequest,
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
            describe_acls_request::DescribeAclsRequest,
            describe_configs_request::DescribeConfigsRequest,
            describe_delegation_token_request::DescribeDelegationTokenRequest,
            describe_user_scram_credentials_request::{
                DescribeUserScramCredentialsRequest, UserName,
            },
            describe_user_scram_credentials_response::{
                CredentialInfo, DescribeUserScramCredentialsResponse,
                DescribeUserScramCredentialsResult,
            },
            elect_leaders_request::ElectLeadersRequest,
            envelope_request::EnvelopeRequest,
            expire_delegation_token_request::ExpireDelegationTokenRequest,
            expire_delegation_token_response::ExpireDelegationTokenResponse,
            incremental_alter_configs_request::IncrementalAlterConfigsRequest,
            list_partition_reassignments_request::ListPartitionReassignmentsRequest,
            renew_delegation_token_request::RenewDelegationTokenRequest,
            renew_delegation_token_response::RenewDelegationTokenResponse,
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
            unregister_broker_request::UnregisterBrokerRequest,
            unregister_controller_request::UnregisterControllerRequest,
            update_features_request::UpdateFeaturesRequest,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::time::Duration,
};

/// Kafka's `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, which
/// `ControllerApis.allowTokenRequests` answers for a token RPC that arrives
/// over a PLAINTEXT connection without forwarding.
const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;

/// `DelegationTokenManager.ERROR_TIMESTAMP`, the timestamp Kafka writes into
/// every refused token response.
const TOKEN_ERROR_TIMESTAMP: i64 = -1;

/// `SCRAM-SHA-256` as KIP-554 numbers the mechanisms on the wire.
const SCRAM_SHA_256: i8 = 1;

/// Kafka's `INVALID_REPLICATION_FACTOR`, which `CreateTopics` answers when the
/// registered broker set cannot carry the requested replication factor.
const INVALID_REPLICATION_FACTOR: i16 = 38;

/// Every api key a Kafka 4.x controller listener accepts, in key order.
///
/// Read off a live `mirror.gcr.io/apache/kafka:4.3.1` controller with a raw
/// `ApiVersions` v0 request, and identical to the set the `listeners` tag on
/// the request schemas in `kafka-clients-4.3.1.jar` marks `controller`.
///
/// Kafka trunk adds `UnregisterController` (94, KIP-1312), tagged `broker` and
/// `controller`. No released image advertises it, and krabka serves it
/// because krabka-protocol vendors the trunk schema.
const KAFKA_CONTROLLER_LISTENER_KEYS: [i16; 41] = [
    1, 17, 18, 19, 20, 29, 30, 31, 32, 33, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 62, 63, 64, 67, 70, 73, 80, 81, 82,
];

/// What Kafka trunk's controller listener adds to 4.3.1's, and krabka's
/// under `unstable.api.versions.enable`: `UnregisterController` (94,
/// KIP-1312).
const KAFKA_TRUNK_CONTROLLER_LISTENER_KEYS: [i16; 1] = [94];

/// A running node and the directory that backs it.
struct ControllerNode {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

/// Start a one-node broker whose controller listener is reachable on its own
/// port, and return the handle. Both listeners are bound before the broker
/// starts so the test knows the ports without racing the bind.
async fn start_broker() -> ControllerNode {
    start_node(&[NodeRole::Controller, NodeRole::Broker], |_| {}).await
}

/// [`start_broker`], serving what krabka implements from Kafka trunk:
/// `unstable.api.versions.enable` and `unstable.feature.versions.enable` on.
async fn start_trunk_broker() -> ControllerNode {
    start_node(&[NodeRole::Controller, NodeRole::Broker], enable_trunk).await
}

async fn start_surface(trunk: bool) -> ControllerNode {
    if trunk {
        start_trunk_broker().await
    } else {
        start_broker().await
    }
}

async fn advertised_controller_versions(broker: &BrokerHandle) -> ApiVersionsResponse {
    let connection = dial_controller(broker).await;
    let response = connection
        .send(api_versions_request())
        .await
        .expect("ApiVersions over the controller listener");
    connection.close();
    response
}

fn enable_trunk(config: &mut BrokerConfig) {
    config.features.unstable_api_versions =
        krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
}

/// The same node, with the `process.roles` the case needs and `configure`
/// applied last.
async fn start_node(roles: &[NodeRole], configure: fn(&mut BrokerConfig)) -> ControllerNode {
    let (broker, dir) = start_with_bound_listeners(|config| {
        config.roles = roles.to_vec();
        configure(config);
    })
    .await;
    ControllerNode { broker, dir }
}

/// An `ApiVersions` request that passes Kafka's `ApiVersionsRequest.isValid`:
/// from v3 the KIP-511 client software name and version must be set.
fn api_versions_request() -> ApiVersionsRequest { api_versions_request_for("krabka-test", "1.0") }

/// Dial the controller listener. `Connection::connect` runs the `ApiVersions`
/// bootstrap, so every later `send` negotiates against what this listener
/// advertises rather than against the client's own codec range.
async fn dial_controller(broker: &BrokerHandle) -> Connection {
    Connection::connect(broker.controller_addr(), ConnectionOptions {
        client_id: "controller-admin-surface".to_owned(),
        ..ConnectionOptions::default()
    })
    .await
    .expect("dial the controller listener")
}

/// The versions of one api key that a listener advertises, inclusive.
#[derive(Debug, PartialEq, Eq)]
struct VersionRange {
    min: i16,
    max: i16,
}

/// The Admin api keys a Kafka 4.x controller listener accepts, paired with the
/// version range krabka speaks for each.
///
/// The keys are the ones a live `mirror.gcr.io/apache/kafka:4.3.1` controller
/// advertises in `ApiVersions` (the same set 4.0.0's request schemas tag
/// `controller`), minus the RPCs the controller listener serves without the
/// Admin bridge.
/// `DescribeClientQuotas` (48) is tagged `broker` only, so it is absent there,
/// absent here, and asserted absent below.
///
/// Every one of them is at the version range Kafka 4.3.1 serves, which is the
/// vendored range, so the default and the trunk tables agree on them; trunk
/// adds `UnregisterController`.
fn expected_admin_versions(trunk: bool) -> std::collections::BTreeMap<i16, VersionRange> {
    let mut versions = std::collections::BTreeMap::from(
        [
            admin_range::<CreateTopicsRequest>(),
            admin_range::<DeleteTopicsRequest>(),
            admin_range::<DescribeAclsRequest>(),
            admin_range::<CreateAclsRequest>(),
            admin_range::<DeleteAclsRequest>(),
            admin_range::<DescribeConfigsRequest>(),
            admin_range::<AlterConfigsRequest>(),
            admin_range::<CreatePartitionsRequest>(),
            admin_range::<CreateDelegationTokenRequest>(),
            admin_range::<RenewDelegationTokenRequest>(),
            admin_range::<ExpireDelegationTokenRequest>(),
            admin_range::<DescribeDelegationTokenRequest>(),
            admin_range::<ElectLeadersRequest>(),
            admin_range::<IncrementalAlterConfigsRequest>(),
            admin_range::<AlterPartitionRequest>(),
            admin_range::<AllocateProducerIdsRequest>(),
            admin_range::<AlterPartitionReassignmentsRequest>(),
            admin_range::<ListPartitionReassignmentsRequest>(),
            admin_range::<AlterClientQuotasRequest>(),
            admin_range::<DescribeUserScramCredentialsRequest>(),
            admin_range::<AlterUserScramCredentialsRequest>(),
            admin_range::<UpdateFeaturesRequest>(),
            admin_range::<EnvelopeRequest>(),
            admin_range::<UnregisterBrokerRequest>(),
            admin_range::<AssignReplicasToDirsRequest>(),
        ]
        .map(|entry| (entry.key, entry.range)),
    );
    if trunk {
        let entry = admin_range::<UnregisterControllerRequest>();
        versions.insert(entry.key, entry.range);
    }
    versions
}

struct AdminVersionEntry {
    key: i16,
    range: VersionRange,
}

fn admin_range<R: ProtocolRequest>() -> AdminVersionEntry {
    AdminVersionEntry {
        key: R::API_KEY,
        range: VersionRange {
            min: R::MIN_VERSION,
            max: R::MAX_VERSION,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_api_versions_advertises_the_kafka_controller_admin_surface() {
    for trunk in [false, true] {
        let ControllerNode { broker, dir: _dir } = start_surface(trunk).await;
        let response = advertised_controller_versions(&broker).await;

        let expected = expected_admin_versions(true);
        let advertised: std::collections::BTreeMap<i16, VersionRange> = response
            .api_keys
            .iter()
            .filter(|api| expected.contains_key(&api.api_key))
            .map(|api| {
                (api.api_key, VersionRange {
                    min: api.min_version,
                    max: api.max_version,
                })
            })
            .collect();

        check!(
            advertised == expected_admin_versions(trunk),
            "trunk {trunk}"
        );
        // Tagged `broker` only in Kafka, so a Kafka controller does not offer it.
        check!(
            !response.api_keys.iter().any(|api| api.api_key
                == krabka_protocol::owned::describe_client_quotas_request::API_KEY),
            "trunk {trunk}"
        );
        broker.shutdown().await;
    }
}

/// The whole key set the controller listener advertises, measured against the
/// Kafka oracle rather than against krabka's own tables.
///
/// [`controller_api_versions_advertises_the_kafka_controller_admin_surface`]
/// only inspects the keys the Admin bridge routes, so it cannot see a key
/// krabka offers that no Kafka controller does, or a key of Kafka's that the
/// listener does not offer. This case pins both directions: the advertised set
/// is exactly [`KAFKA_CONTROLLER_LISTENER_KEYS`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_advertises_no_key_kafka_does_not() {
    for trunk in [false, true] {
        let ControllerNode { broker, dir: _dir } = start_surface(trunk).await;
        let response = advertised_controller_versions(&broker).await;

        let advertised: std::collections::BTreeSet<i16> =
            response.api_keys.iter().map(|api| api.api_key).collect();
        let mut kafka: std::collections::BTreeSet<i16> =
            KAFKA_CONTROLLER_LISTENER_KEYS.iter().copied().collect();
        if trunk {
            kafka.extend(KAFKA_TRUNK_CONTROLLER_LISTENER_KEYS);
        }

        check!(
            advertised.difference(&kafka).copied().collect::<Vec<_>>() == Vec::<i16>::new(),
            "trunk {trunk}"
        );
        check!(
            kafka.difference(&advertised).copied().collect::<Vec<_>>() == Vec::<i16>::new(),
            "trunk {trunk}"
        );
        broker.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_topic_lifecycle() {
    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;

    let created = connection
        .send(create_topic_request(
            creatable_topic()
                .name(("controller-lifecycle").into())
                .call(),
        ))
        .await
        .expect("CreateTopics over the controller listener");

    assert!(let [created_topic] = &created.topics[..]);
    // The topic id is minted per create, so the expectation borrows it back;
    // the check below pins it as non-nil, which is what the create promises.
    check!(created_topic.topic_id != WireUuid([0; 16]));
    // KIP-525 fills the row with the topic's whole effective configuration,
    // which the controller listener answers exactly as the broker listener
    // does. `admin_create_topics.rs` is what pins that list against
    // `DescribeConfigs`; here it is borrowed back like the topic id, so this
    // case stays about the lifecycle the controller listener serves.
    check!(
        created_topic
            .configs
            .as_ref()
            .is_some_and(|configs| !configs.is_empty())
    );
    check!(
        created
            == CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: "controller-lifecycle".into(),
                    topic_id: created_topic.topic_id,
                    error_code: 0,
                    error_message: None,
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: created_topic.configs.clone(),
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let grown = connection
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: "controller-lifecycle".into(),
                count: 3,
                // No manual assignment: Kafka's admin client sends null.
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreatePartitions over the controller listener");

    check!(
        grown
            == CreatePartitionsResponse {
                throttle_time_ms: 0,
                results: vec![CreatePartitionsTopicResult {
                    name: "controller-lifecycle".into(),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let deleted = connection
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some("controller-lifecycle".into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics over the controller listener");
    connection.close();

    let expected_delete = DeleteTopicsResponse {
        throttle_time_ms: 0,
        responses: vec![DeletableTopicResult {
            name: Some("controller-lifecycle".into()),
            // A deleted topic's row carries its topic id, as Kafka's
            // `ControllerApis.deleteTopics` answers it (#634).
            topic_id: created_topic.topic_id,
            error_code: 0,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    check!(deleted == expected_delete);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_writing_delegation_token_apis() {
    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;

    // The controller listener in `for_tests` is PLAINTEXT and the requests are
    // not forwarded, so Kafka's `ControllerApis.allowTokenRequests` refuses each
    // RPC before it looks at the token or the secret key.
    let created = connection
        .send(CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        })
        .await
        .expect("CreateDelegationToken over the controller listener");
    let renewed = connection
        .send(RenewDelegationTokenRequest {
            hmac: Bytes::from_static(b"not-a-token"),
            renew_period_ms: -1,
            ..Default::default()
        })
        .await
        .expect("RenewDelegationToken over the controller listener");
    let expired = connection
        .send(ExpireDelegationTokenRequest {
            hmac: Bytes::from_static(b"not-a-token"),
            expiry_time_period_ms: -1,
            ..Default::default()
        })
        .await
        .expect("ExpireDelegationToken over the controller listener");
    connection.close();

    check!(
        created
            == CreateDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                principal_type: "User".into(),
                principal_name: "ANONYMOUS".into(),
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "ANONYMOUS".into(),
                issue_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                max_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    check!(
        renewed
            == RenewDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    check!(
        expired
            == ExpireDelegationTokenResponse {
                error_code: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                expiry_timestamp_ms: TOKEN_ERROR_TIMESTAMP,
                ..Default::default()
            }
    );
    broker.shutdown().await;
}

/// `kafka-configs --bootstrap-controller --entity-type users --alter` is the
/// KIP-919 flow this covers: the SCRAM write lands through the controller
/// listener and the matching read on the same connection sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_scram_write_path() {
    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;

    let altered = connection
        .send(AlterUserScramCredentialsRequest {
            upsertions: vec![
                scram_upsertion()
                    .mechanism(ScramAlgorithm::Sha256)
                    .iterations(8_192)
                    .salt(Bytes::from_static(b"salt-bytes"))
                    .salted_password(Bytes::from_static(b"salted-password-bytes"))
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("AlterUserScramCredentials over the controller listener");

    check!(
        altered
            == AlterUserScramCredentialsResponse {
                throttle_time_ms: 0,
                results: vec![AlterUserScramCredentialsResult {
                    user: "alice".into(),
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let described = connection
        .send(DescribeUserScramCredentialsRequest {
            users: Some(vec![UserName {
                name: "alice".into(),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("DescribeUserScramCredentials over the controller listener");
    connection.close();

    check!(described == alice_scram_credentials());
    broker.shutdown().await;
}

/// Independent expected stored SCRAM response, including each empty tag collection.
fn alice_scram_credentials() -> DescribeUserScramCredentialsResponse {
    DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        results: vec![DescribeUserScramCredentialsResult {
            user: "alice".into(),
            error_code: 0,
            error_message: None,
            credential_infos: vec![CredentialInfo {
                mechanism: SCRAM_SHA_256,
                iterations: 8_192,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// `AssignReplicasToDirs` is a context dispatch, so it also covers the
/// bridge's `ClusterAction`-authorized inter-broker arm; the default
/// `AllowAllAuthorizer` admits the connection's `ANONYMOUS` principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_assign_replicas_to_dirs() {
    const LOG_DIR_ID: WireUuid = WireUuid([7; 16]);

    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;
    let broker_epoch = broker
        .controller_image_for_test()
        .broker(NodeId(1))
        .expect("registered broker")
        .broker_epoch;

    let created = connection
        .send(create_topic_request(
            creatable_topic().name(("controller-dirs").into()).call(),
        ))
        .await
        .expect("CreateTopics over the controller listener");
    assert!(let [created_topic] = &created.topics[..]);
    let topic_id = created_topic.topic_id;

    let req = AssignReplicasToDirsRequest {
        broker_id: 1,
        broker_epoch,
        directories: vec![DirectoryData {
            id: LOG_DIR_ID,
            topics: vec![TopicData {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let assigned = connection
        .send(req)
        .await
        .expect("AssignReplicasToDirs over the controller listener");
    connection.close();

    check!(
        assigned
            == AssignReplicasToDirsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                directories: vec![RespDirectoryData {
                    id: LOG_DIR_ID,
                    topics: vec![RespTopicData {
                        topic_id,
                        partitions: vec![RespPartitionData {
                            partition_index: 0,
                            error_code: 0,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}

/// Kafka trunk's `UnregisterController` (94) over the controller listener: a
/// registered controller id is dropped from the image, and an unknown one is
/// `CONTROLLER_ID_NOT_REGISTERED`. The cluster is first moved to trunk's
/// 4.4-IV2 (33), the level the RPC needs, through `UpdateFeatures` on the
/// same listener. Both are trunk's, so the node runs with Kafka's two
/// `unstable.*.enable` settings on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_unregister_controller() {
    let ControllerNode { broker, dir: _dir } = start_trunk_broker().await;
    broker
        .submit_metadata_record_for_test(controller_registration(NodeId(7)))
        .await
        .expect("seed the registration");
    let connection = dial_controller(&broker).await;
    let upgraded = connection
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update(
                "metadata.version",
                krabka_metadata::metadata_version::CONTROLLER_UNREGISTRATION_MIN_LEVEL,
                1,
            )],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures over the controller listener");
    check!(upgraded.error_code == 0, "{upgraded:?}");

    let mut answers = Vec::new();
    for controller_id in [7, 9] {
        answers.push(
            connection
                .send(UnregisterControllerRequest {
                    controller_id,
                    ..Default::default()
                })
                .await
                .expect("UnregisterController over the controller listener"),
        );
    }
    connection.close();

    check!(
        answers
            == crate::support::admin::unregister_controller_answers()
                .controller_id(9)
                .call()
    );
    check!(
        broker
            .controller_image_for_test()
            .controller(NodeId(7))
            .is_none()
    );
    broker.shutdown().await;
}

/// KIP-919 puts `CreateTopics` and `CreatePartitions` on the controller
/// listener, so a controller-only node answers both -- and it hosts no
/// replicas. `process.roles` without `broker` means `register_broker` skips it,
/// so its image holds no broker at all, and placement has nowhere to put a
/// replica.
///
/// Kafka answers that with `INVALID_REPLICATION_FACTOR`: its
/// `StripedReplicaPlacer` finds no unfenced broker before it counts the
/// registered ones. Substituting the local node instead would create a topic
/// whose only replica lives on a node that serves no partition, leaving
/// metadata nothing can ever serve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_only_node_places_no_replica_on_itself() {
    let ControllerNode { broker, dir: _dir } = start_node(&[NodeRole::Controller], |_| {}).await;
    let connection = dial_controller(&broker).await;

    let created = connection
        .send(create_topic_request(
            creatable_topic()
                .name(("controller-only-placement").into())
                .call(),
        ))
        .await
        .expect("CreateTopics over a controller-only listener");
    connection.close();

    check!(
        created
            == CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: "controller-only-placement".into(),
                    topic_id: WireUuid([0; 16]),
                    error_code: INVALID_REPLICATION_FACTOR,
                    error_message: Some(
                        "Unable to replicate the partition 1 time(s): All brokers are currently \
                         fenced, or have all their log directories cordoned."
                            .into(),
                    ),
                    num_partitions: -1,
                    replication_factor: -1,
                    // Kafka never sets `Configs` on an error row, so it keeps
                    // the generated default, an empty list.
                    configs: Some(Vec::new()),
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
    broker.shutdown().await;
}

/// Kafka's `DUPLICATE_BROKER_REGISTRATION`.
const DUPLICATE_BROKER_REGISTRATION: i16 = 101;

/// Kafka's `INVALID_REGISTRATION`.
const INVALID_REGISTRATION: i16 = 119;

/// One `BrokerRegistration` a Kafka broker sends over the controller listener.
struct Registration {
    broker_id: i32,
    incarnation: u128,
    port: u16,
    log_dirs: &'static [u128],
    with_metadata_version: bool,
}

impl Registration {
    const fn broker_7(incarnation: u128, port: u16) -> Self {
        Self {
            broker_id: 7,
            incarnation,
            port,
            log_dirs: &[7000],
            with_metadata_version: true,
        }
    }

    const fn broker_8() -> Self {
        Self {
            broker_id: 8,
            incarnation: 0x8a,
            port: 19_100,
            log_dirs: &[8000],
            with_metadata_version: true,
        }
    }

    fn request(&self, image: &krabka_metadata::MetadataImage) -> BrokerRegistrationRequest {
        BrokerRegistrationRequest {
            broker_id: self.broker_id,
            cluster_id: image.cluster_id().to_string(),
            incarnation_id: WireUuid(*uuid::Uuid::from_u128(self.incarnation).as_bytes()),
            // A broker with no `broker.rack` registers a null rack.
            rack: None,
            listeners: vec![RegistrationListener {
                name: "PLAINTEXT".into(),
                host: "127.0.0.1".into(),
                port: self.port,
                security_protocol: 0,
                ..Default::default()
            }],
            // What a real broker's `SupportedFeatures` covers: every level
            // the cluster finalized.
            features: image
                .finalized_features()
                .iter()
                .filter(|(name, _)| {
                    self.with_metadata_version
                        || name.as_str()
                            != krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                })
                .map(|(name, level)| RegistrationFeature {
                    name: name.clone(),
                    min_supported_version: 0,
                    max_supported_version: *level,
                    ..Default::default()
                })
                .collect(),
            log_dirs: self
                .log_dirs
                .iter()
                .map(|id| WireUuid(*uuid::Uuid::from_u128(*id).as_bytes()))
                .collect(),
            ..Default::default()
        }
    }
}

/// How the broker epoch in a registration answer relates to the one broker 7
/// held before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EpochAnswer {
    Refused,
    New,
    Kept,
}

/// The port and fence the image holds for a registered broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegisteredBroker {
    port: u16,
    fenced: bool,
}

/// What one registration step came to: the error code, the epoch, and the
/// port and fence the image then holds for the broker the step registered,
/// if the step was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    error_code: i16,
    epoch: EpochAnswer,
    registered: Option<RegisteredBroker>,
}

const fn refused(error_code: i16) -> Outcome {
    Outcome {
        error_code,
        epoch: EpochAnswer::Refused,
        registered: None,
    }
}

const fn accepted(epoch: EpochAnswer, port: u16, fenced: bool) -> Outcome {
    Outcome {
        error_code: 0,
        epoch,
        registered: Some(RegisteredBroker { port, fenced }),
    }
}

/// One registration step of
/// [`controller_listener_registers_a_restarted_broker_as_kafka_does`]: what it
/// does, the registration it sends, how long to wait first, and the outcome
/// Kafka gives it.
struct Step {
    what: &'static str,
    registration: Registration,
    wait: Option<Duration>,
    expected: Outcome,
}

/// krabka-io/krabka-broker#822: a Kafka broker that restarts rejoins over the
/// controller listener.
///
/// A Kafka broker picks a new incarnation id in every process. Kafka's
/// `ClusterControlManager.registerBroker` refuses that id with
/// `DUPLICATE_BROKER_REGISTRATION` only while the previous incarnation still
/// holds a heartbeat session, and registers it with a new broker epoch once
/// the session expires. A new registration is fenced (`RegisterBrokerRecord`
/// defaults `Fenced` to true). It rewrites the record for the same incarnation
/// and keeps the epoch and the fence, and it validates `metadata.version` and
/// the log directories. Each step runs against the image the steps before it left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_registers_a_restarted_broker_as_kafka_does() {
    let steps = vec![
        Step {
            what: "a first registration",
            registration: Registration::broker_7(0xa, 19_092),
            wait: None,
            expected: accepted(EpochAnswer::New, 19_092, true),
        },
        Step {
            what: "the same incarnation again, on a new port, after it unfenced",
            registration: Registration::broker_7(0xa, 19_093),
            wait: None,
            expected: accepted(EpochAnswer::Kept, 19_093, false),
        },
        Step {
            what: "a new incarnation while the previous one heartbeats",
            registration: Registration::broker_7(0xb, 19_094),
            wait: None,
            expected: refused(DUPLICATE_BROKER_REGISTRATION),
        },
        Step {
            what: "the new incarnation once the session expired",
            registration: Registration::broker_7(0xb, 19_094),
            // Longer than the two-second `heartbeat_timeout` of the test
            // configuration.
            wait: Some(Duration::from_millis(2_500)),
            expected: accepted(EpochAnswer::New, 19_094, true),
        },
        Step {
            what: "no metadata.version feature",
            registration: Registration {
                with_metadata_version: false,
                ..Registration::broker_8()
            },
            wait: None,
            expected: refused(INVALID_REGISTRATION),
        },
        Step {
            what: "no log directory",
            registration: Registration {
                log_dirs: &[],
                ..Registration::broker_8()
            },
            wait: None,
            expected: refused(INVALID_REGISTRATION),
        },
        Step {
            what: "a log directory broker 7 registered",
            registration: Registration {
                log_dirs: &[8000, 7000],
                ..Registration::broker_8()
            },
            wait: None,
            expected: refused(INVALID_REGISTRATION),
        },
    ];

    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;
    let mut epoch_of_7 = -1;
    let mut expected = Vec::new();
    let mut outcomes = Vec::new();
    for Step {
        what,
        registration,
        wait,
        expected: outcome,
    } in steps
    {
        expected.push((what, outcome));
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
        let image = broker.controller_image_for_test();
        let answer = connection
            .send(registration.request(&image))
            .await
            .expect("BrokerRegistration over the controller listener");
        let node = NodeId(u64::try_from(registration.broker_id).expect("a broker id"));
        let epoch = match answer.broker_epoch {
            -1 => EpochAnswer::Refused,
            epoch if node == NodeId(7) && epoch == epoch_of_7 => EpochAnswer::Kept,
            _ => EpochAnswer::New,
        };
        let registered = if answer.error_code == 0 {
            broker
                .wait_for_image(|image| {
                    image
                        .broker(node)
                        .is_some_and(|registered| registered.broker_epoch == answer.broker_epoch)
                })
                .await;
            broker
                .controller_image_for_test()
                .broker(node)
                .map(|registered| RegisteredBroker {
                    port: registered.port,
                    fenced: registered.fenced,
                })
        } else {
            None
        };
        outcomes.push((what, Outcome {
            error_code: answer.error_code,
            epoch,
            registered,
        }));
        if answer.error_code != 0 || node != NodeId(7) {
            continue;
        }
        epoch_of_7 = answer.broker_epoch;
        if registration.incarnation == 0xa {
            // The first incarnation heartbeats, so it holds a session, and it
            // has caught up to its registration, so it unfences.
            let heartbeat = connection
                .send(BrokerHeartbeatRequest {
                    broker_id: 7,
                    broker_epoch: answer.broker_epoch,
                    current_metadata_offset: answer.broker_epoch,
                    ..Default::default()
                })
                .await
                .expect("BrokerHeartbeat over the controller listener");
            assert!(heartbeat.error_code == 0, "{what}: {heartbeat:?}");
        }
    }
    connection.close();
    broker.shutdown().await;

    check!(outcomes == expected);
}

/// Kafka's `UNKNOWN_TOPIC_ID`.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// Kafka's `STALE_BROKER_EPOCH`: `ClusterControlManager.checkBrokerEpoch`
/// answers it for a broker with no registration, too.
const STALE_BROKER_EPOCH: i16 = 77;

/// Kafka's `ILLEGAL_SASL_STATE`.
const ILLEGAL_SASL_STATE: i16 = 34;

/// A Kafka broker sends `AlterPartition` and `AllocateProducerIds` to the
/// active controller over its controller listener, and `ControllerApis`
/// answers `SaslHandshake` and `SaslAuthenticate` there with
/// `ILLEGAL_SASL_STATE`. `Connection::send` negotiates each one off this
/// listener's `ApiVersions` table, so an unadvertised key fails before it is
/// sent. The requests go to the highest version both sides speak:
/// `AlterPartition` v3, `AllocateProducerIds` v0, `SaslHandshake` v1 and
/// `SaslAuthenticate` v2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_listener_serves_the_inter_broker_and_sasl_apis() {
    let ControllerNode { broker, dir: _dir } = start_broker().await;
    let connection = dial_controller(&broker).await;
    let unknown_topic = WireUuid([7; 16]);
    // The controller checks the sender's broker epoch before any row, as
    // Kafka's `checkBrokerEpoch` does, so the request carries the real one.
    broker.wait_until_brokers_registered(1).await;
    let broker_epoch = broker
        .controller_image_for_test()
        .broker_epoch(krabka_metadata::NodeId(1))
        .expect("broker 1 registered");

    let alter_partition = connection
        .send(AlterPartitionRequest {
            broker_id: 1,
            broker_epoch,
            topics: vec![AlterPartitionTopicData {
                topic_id: unknown_topic,
                partitions: vec![AlterPartitionPartitionData {
                    partition_index: 0,
                    leader_epoch: 0,
                    new_isr_with_epochs: vec![BrokerState {
                        broker_id: 1,
                        broker_epoch: -1,
                        ..Default::default()
                    }],
                    partition_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterPartition over the controller listener");
    check!(
        alter_partition
            == AlterPartitionResponse {
                topics: vec![AlterPartitionResultTopic {
                    topic_id: unknown_topic,
                    partitions: vec![AlterPartitionResultPartition {
                        partition_index: 0,
                        error_code: UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );

    let allocate = connection
        .send(AllocateProducerIdsRequest {
            broker_id: 99,
            broker_epoch: 0,
            ..Default::default()
        })
        .await
        .expect("AllocateProducerIds over the controller listener");
    check!(
        allocate
            == AllocateProducerIdsResponse {
                error_code: STALE_BROKER_EPOCH,
                producer_id_start: -1,
                producer_id_len: 0,
                ..Default::default()
            }
    );

    let handshake = connection
        .send(SaslHandshakeRequest {
            mechanism: "PLAIN".into(),
            ..Default::default()
        })
        .await
        .expect("SaslHandshake over the controller listener");
    check!(
        handshake
            == SaslHandshakeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }
    );

    let authenticate = connection
        .send(SaslAuthenticateRequest {
            auth_bytes: Bytes::from_static(b"\0broker\0secret"),
            ..Default::default()
        })
        .await
        .expect("SaslAuthenticate over the controller listener");
    check!(
        authenticate
            == SaslAuthenticateResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(
                    "SaslAuthenticate request received after successful authentication".into()
                ),
                ..Default::default()
            }
    );

    connection.close();
    broker.shutdown().await;
}
