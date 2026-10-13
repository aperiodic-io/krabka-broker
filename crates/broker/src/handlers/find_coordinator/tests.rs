//! Live-broker tests for the `FindCoordinator` handler.
//!
//! These drive `handle` against a running broker, which is what covers the
//! create-on-first-use path of the coordinator topics: the first lookup asks
//! for the topic and answers `COORDINATOR_NOT_AVAILABLE`, and the configured
//! partition count and replication factor shape the topic that is created and
//! the partition a key is routed to.

use {
    super::*,
    crate::{
        authorizer::AuthorizationResult,
        broker::{Broker, BrokerHandle},
        coordinator::bootstrap::OFFSETS_TOPIC,
        handlers::test_support::single_replica_partition,
        share_coordinator::bootstrap::TOPIC,
        test_support::{
            DenyAll, KafkaErrorCode, broker_registration, dispatch_wire, peer, principal,
            start_broker_no_audit, start_broker_no_audit_with,
        },
        txn::bootstrap::TOPIC as TXN_TOPIC,
    },
    assert2::{assert, check},
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse,
};

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct LookupKeyType(i8);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct LookupSetup<'a> {
    #[default("g")]
    key: &'a str,
    key_type: LookupKeyType,
    #[default(&["g"])]
    keys: &'a [&'a str],
}

/// The response that a lookup at `version` must produce.
struct VersionedResponse {
    version: krabka_ids::ApiVersion,
    response: FindCoordinatorResponse,
}

#[bon::builder]
async fn check_lookup_versions(
    broker: &Broker,
    #[builder(default = "g")] key: &'_ str,
    #[builder(into, default)] key_type: LookupKeyType,
    #[builder(default = & ["g"])] keys: &'_ [&'_ str],
    rows: Vec<VersionedResponse>,
) {
    let setup = LookupSetup {
        key,
        key_type,
        keys,
    };
    for VersionedResponse {
        version,
        response: expected,
    } in rows
    {
        let request = FindCoordinatorRequest {
            key: setup.key.into(),
            key_type: setup.key_type.0,
            coordinator_keys: if version.0 >= 4 {
                setup.keys.iter().map(|key| (*key).into()).collect()
            } else {
                vec![]
            },
            ..Default::default()
        };
        let response = find(broker, &request, version.0, "alice").await;
        assert!(response == expected, "v{}", version.0);
    }
}

fn legacy_coordinator_error(
    code: KafkaErrorCode,
    message: Option<String>,
) -> FindCoordinatorResponse {
    FindCoordinatorResponse {
        error_code: code.0,
        error_message: message,
        node_id: -1,
        host: String::new(),
        port: -1,
        ..Default::default()
    }
}

const KAFKA_TOPIC_ID: &str = "BQUFBQUFBQUFBQUFBQUFBQ";

#[derive(Debug)]
struct DenyOneTransaction;

test_authorizer!(DenyOneTransaction, (self, _source, request), {
    if request.resource_type == ResourceType::TransactionalId
        && request.operation == AclOperation::Describe
        && request.resource_name == "denied"
    {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// A malformed share lookup returns one refusal and never starts share-state bootstrap.
fn check_invalid_share_lookup(handle: &BrokerHandle, response: &FindCoordinatorResponse) {
    assert!(response.coordinators.len() == 1);
    assert!(response.coordinators[0].error_code == codes::INVALID_REQUEST);
    assert!(handle.controller_image_for_test().topic(TOPIC).is_none());
}

#[tokio::test]
async fn configured_partition_count_controls_txn_topic_and_routing() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.transaction_state_num_partitions = 7;
        config.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "admin-client"
    );
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let tid = "my-tid"; // hashes to partition 43 with the old fixed count of 50
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![tid.to_string()],
        ..Default::default()
    };

    let first = find(&broker, &request, version, "admin").await;
    check!(first.coordinators == vec![row(tid, codes::COORDINATOR_NOT_AVAILABLE, None)]);
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;

    let response = handle(&broker, request, version, &context)
        .await
        .expect("find transaction coordinator");

    let image = broker_handle.controller_image_for_test();
    let topic = image.topic(TXN_TOPIC).expect("transaction-state topic");
    assert!(topic.partitions == 7);
    assert!(topic.replication_factor == 1);
    assert!(image.partitions_of(TXN_TOPIC).count() == 7);
    assert!(response.coordinators.len() == 1);
    assert!(response.coordinators[0].error_code == codes::NONE);
    assert!(response.coordinators[0].node_id == broker.config.broker_id);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn share_key_type_before_v6_is_invalid_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!(
        (principal, peer, context),
        principal("alice"),
        client_id = "share-client"
    );
    let version = 5;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_SHARE,
        coordinator_keys: vec![format!("share-group:{KAFKA_TOPIC_ID}:0")],
        ..Default::default()
    };

    let response = handle(&broker, request, version, &context)
        .await
        .expect("reject pre-v6 share coordinator lookup");

    check_invalid_share_lookup(&broker_handle, &response);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn v4_empty_key_array_stays_empty_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!(
        (principal, peer, context),
        principal("alice"),
        client_id = "txn-client"
    );
    let version = 4;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![],
        ..Default::default()
    };

    let response = handle(&broker, request, version, &context)
        .await
        .expect("empty batched coordinator lookup");

    assert!(response.coordinators.is_empty());
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(TXN_TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn mixed_rejection_and_resolution_preserve_key_order_and_errors() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.authorizer = std::sync::Arc::new(DenyOneTransaction);
        config.transaction_state_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    request_identity!(
        (principal, peer, context),
        principal("alice"),
        client_id = "txn-client"
    );
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_TRANSACTION,
        coordinator_keys: vec![
            "allowed-first".into(),
            "denied".into(),
            "allowed-last".into(),
        ],
        ..Default::default()
    };

    let response = handle(&broker, request, version, &context)
        .await
        .expect("mixed coordinator lookup");

    assert!(
        response
            .coordinators
            .iter()
            .map(|row| (row.key.as_str(), row.error_code))
            .collect::<Vec<_>>()
            == vec![
                ("allowed-first", codes::NONE),
                ("denied", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                ("allowed-last", codes::NONE),
            ]
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn malformed_share_key_is_invalid_without_bootstrap() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.share_coordinator.state_topic_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!(
        (principal, peer, context),
        principal("alice"),
        client_id = "share-client"
    );
    let version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_SHARE,
        coordinator_keys: vec!["malformed".into()],
        ..Default::default()
    };

    let response = handle(&broker, request, version, &context)
        .await
        .expect("reject malformed share coordinator key");

    check_invalid_share_lookup(&broker_handle, &response);
    broker_handle.shutdown().await;
}

/// Serve `request` at `version` through the dispatch registry and decode the
/// answer: below v4 the wire carries the one `key` and the top-level answer.
async fn find(
    broker: &Broker,
    request: &FindCoordinatorRequest,
    version: i16,
    principal_name: &str,
) -> FindCoordinatorResponse {
    request_identity!(
        (principal, peer, context),
        principal(principal_name),
        client_id = "find-client"
    );
    dispatch_wire(
        broker,
        krabka_protocol::owned::find_coordinator_request::API_KEY,
        version,
        request,
        &context,
    )
    .await
}

fn row(key: &str, error_code: i16, error_message: Option<String>) -> Coordinator {
    Coordinator {
        key: key.into(),
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code,
        error_message,
        ..Default::default()
    }
}

macro_rules! mixed_share_keys {
    (($valid:ident, $request:ident)) => {
        let $valid = format!("share-group:{KAFKA_TOPIC_ID}:0");
        let $request = FindCoordinatorRequest {
            key_type: KEY_TYPE_SHARE,
            coordinator_keys: vec!["malformed".into(), $valid.clone()],
            ..Default::default()
        };
    };
}

/// A principal without `ClusterAction` that sends one malformed and one valid
/// share key gets `CLUSTER_AUTHORIZATION_FAILED` with Kafka's message on both:
/// `authorizeClusterOperation` throws before `SharePartitionKey.validate`, and
/// `getErrorResponse` stamps every key. Nothing is bootstrapped.
#[tokio::test]
async fn denied_share_request_answers_cluster_authorization_failed_on_every_key() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.authorizer = std::sync::Arc::new(DenyAll);
        config.share_coordinator.state_topic_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    mixed_share_keys!((valid, request));

    let response = find(&broker, &request, 6, "alice").await;

    let message = Some("Cluster authorization failed.".to_string());
    assert!(
        response
            == FindCoordinatorResponse {
                coordinators: vec![
                    row(
                        "malformed",
                        codes::CLUSTER_AUTHORIZATION_FAILED,
                        message.clone()
                    ),
                    row(&valid, codes::CLUSTER_AUTHORIZATION_FAILED, message),
                ],
                ..Default::default()
            }
    );
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic(TOPIC)
            .is_none()
    );
    broker_handle.shutdown().await;
}

/// A denied group id keeps its own row. At v4+ Kafka's row has no message; at
/// v0-v3 the answer goes through `getErrorResponse`, which carries the default
/// message of the code and `Node.noNode()`.
#[tokio::test]
async fn denied_group_key_answers_kafka_row_shape_per_version() {
    let (broker_handle, _dir) =
        start_broker_no_audit_with(|config| config.authorizer = std::sync::Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let rows = vec![
        VersionedResponse {
            version: krabka_ids::ApiVersion(4),
            response: FindCoordinatorResponse {
                coordinators: vec![row("g", codes::GROUP_AUTHORIZATION_FAILED, None)],
                ..Default::default()
            },
        },
        VersionedResponse {
            version: krabka_ids::ApiVersion(3),
            response: legacy_coordinator_error(
                KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED),
                Some("Group authorization failed.".into()),
            ),
        },
    ];
    check_lookup_versions()
        .broker(&broker)
        .rows(rows)
        .call()
        .await;
    broker_handle.shutdown().await;
}

/// With `ClusterAction` granted, a malformed share key gets its own
/// `INVALID_REQUEST` row with no message and a valid key resolves.
#[tokio::test]
async fn granted_share_request_validates_each_key() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.share_coordinator.state_topic_replication_factor = 1;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    broker_handle.wait_until_share_coordinator_ready().await;
    mixed_share_keys!((valid, request));

    let response = find(&broker, &request, 6, "admin").await;

    let resolved = &response.coordinators[1];
    assert!(
        response
            == FindCoordinatorResponse {
                coordinators: vec![
                    row("malformed", codes::INVALID_REQUEST, None),
                    Coordinator {
                        key: valid,
                        node_id: broker.config.broker_id,
                        host: resolved.host.clone(),
                        port: resolved.port,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
    );
    assert!(resolved.port > 0);
    broker_handle.shutdown().await;
}

/// `CoordinatorType.forId` throws for an unknown key type, so the whole
/// request fails with `INVALID_REQUEST` and its default message, and nothing is
/// bootstrapped.
#[tokio::test]
async fn unknown_key_type_fails_the_whole_request() {
    let (broker_handle, _dir) = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    let message = response::error_message(codes::INVALID_REQUEST);
    let rows = vec![
        VersionedResponse {
            version: krabka_ids::ApiVersion(
                krabka_protocol::owned::find_coordinator_response::MAX_VERSION,
            ),
            response: FindCoordinatorResponse {
                coordinators: vec![
                    row("a", codes::INVALID_REQUEST, message.clone()),
                    row("b", codes::INVALID_REQUEST, message.clone()),
                ],
                ..Default::default()
            },
        },
        VersionedResponse {
            version: krabka_ids::ApiVersion(3),
            response: legacy_coordinator_error(
                KafkaErrorCode(codes::INVALID_REQUEST),
                message.clone(),
            ),
        },
    ];
    check_lookup_versions()
        .broker(&broker)
        .key("a")
        .key_type(i8::MAX)
        .keys(&["a", "b"])
        .rows(rows)
        .call()
        .await;
    let image = broker_handle.controller_image_for_test();
    assert!(image.topic(TXN_TOPIC).is_none());
    assert!(image.topic(TOPIC).is_none());
    broker_handle.shutdown().await;
}

#[test]
fn bootstrap_failure_is_shaped_per_admitted_key_without_losing_rejections() {
    let slots = vec![
        KeySlot::Resolve("allowed-first".into()),
        KeySlot::Rejected(row(
            "denied",
            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            None,
        )),
        KeySlot::Resolve("allowed-last".into()),
    ];
    let unavailable = unavailable_for_keys(vec!["allowed-first".into(), "allowed-last".into()]);

    assert!(
        merge_key_slots(slots, unavailable)
            == vec![
                row("allowed-first", codes::COORDINATOR_NOT_AVAILABLE, None),
                row("denied", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED, None),
                row("allowed-last", codes::COORDINATOR_NOT_AVAILABLE, None),
            ]
    );
}

/// An image with `__consumer_offsets` partitions 0 to 3 led by nodes 1 to 4.
/// Node 1 is this broker; nodes 2 and 3 register an `EXTERNAL` endpoint and
/// node 2 an `INTERNAL` one; node 4 has no registration.
fn resolve_image() -> krabka_metadata::MetadataImage {
    use krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId, TopicRecord,
    };
    let endpoint = |name: &str, host: &str| BrokerEndpoint {
        name: name.into(),
        host: host.into(),
        port: 9093,
        protocol: krabka_security::ListenerProtocol::Plaintext,
    };
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    let topic = OFFSETS_TOPIC;
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: uuid::Uuid::from_u128(7),
        partitions: 4,
        replication_factor: 1,
    }));
    for (node, endpoints) in [
        (1, vec![endpoint("EXTERNAL", "one")]),
        (2, vec![
            endpoint("INTERNAL", "two-in"),
            endpoint("EXTERNAL", "two"),
        ]),
        (3, vec![endpoint("INTERNAL", "three-in")]),
    ] {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: "legacy".into(),
                port: 1000,
                endpoints,
                ..broker_registration(krabka_raft::NodeId(node))
            },
        ));
    }
    for (partition, leader) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
        image.apply(&MetadataRecord::V1Partition(single_replica_partition(
            topic,
            krabka_ids::PartitionIndex(partition),
            NodeId(leader),
        )));
    }
    image
}

/// Kafka resolves the coordinator with `getAliveBrokerNode(leaderId,
/// listenerName)`: a fenced leader, a leader without an endpoint on the
/// request's listener, an unregistered leader and a missing partition all
/// answer `COORDINATOR_NOT_AVAILABLE` with `Node.noNode()`.
#[test]
fn resolve_answers_only_an_alive_leader_on_the_request_listener() {
    let image = resolve_image();
    let resolved = |node_id: i32, host: &str, port: i32| Coordinator {
        key: "k".into(),
        node_id,
        host: host.into(),
        port,
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    };
    let unavailable = row("k", codes::COORDINATOR_NOT_AVAILABLE, None);
    // (label, partition, fenced nodes, expected row)
    let rows = [
        ("local leader", 0, vec![], resolved(1, "local", 9092)),
        ("fenced local leader", 0, vec![1], unavailable.clone()),
        (
            "remote leader on listener",
            1,
            vec![],
            resolved(2, "two", 9093),
        ),
        ("fenced remote leader", 1, vec![2], unavailable.clone()),
        ("no endpoint on listener", 2, vec![], unavailable.clone()),
        ("unregistered leader", 3, vec![], unavailable.clone()),
        ("missing partition", 9, vec![], unavailable.clone()),
    ];
    for (label, partition, fenced, expected) in rows {
        let fenced: std::collections::HashSet<u64> = fenced.into_iter().collect();
        let target = resolve::ResolveTarget {
            image: &image,
            unavailable: &fenced,
            local_node: krabka_metadata::NodeId(1),
            advertised: "local:9092",
            listener: "EXTERNAL",
        };
        let got = resolve_partition_coordinator(&target, OFFSETS_TOPIC, partition, "k".into());
        assert!(got == expected, "{label}");
    }
}

/// v0-v3 carry the single `key` field and read the answer out of the
/// top-level `node_id` / `host` / `port`, not out of the `coordinators` array.
/// Kafka's `handleFindCoordinatorRequestLessThanV4` sets `error.message()`,
/// which for `NONE` is the enum name.
#[tokio::test]
async fn a_legacy_group_lookup_answers_in_the_top_level_fields() {
    let (broker_handle, _dir) =
        start_broker_no_audit_with(|config| config.offsets_topic_replication_factor = 1).await;
    let broker = broker_handle.broker_arc_for_test();
    broker_handle.wait_until_group_coordinator_ready().await;
    let request = FindCoordinatorRequest {
        key: "legacy-group".into(),
        key_type: KEY_TYPE_GROUP,
        ..Default::default()
    };

    let response = find(&broker, &request, 3, "alice").await;

    // v0-v3 has no `coordinators` array on the wire, so the whole answer is
    // the top-level row.
    assert!(
        response
            == unthrottled_wire!(FindCoordinatorResponse {
                error_code: codes::NONE,
                error_message: Some("NONE".into()),
                node_id: broker.config.broker_id,
                host: response.host.clone(),
                port: response.port,
                coordinators: vec![],
            })
    );
    assert!(response.port > 0);
    broker_handle.shutdown().await;
}

/// Waits until no creation of `topic` is in flight on `broker`.
use crate::handlers::test_support::wait_until_creation_ends;

/// Kafka's `KafkaApis.getCoordinator`: before the state topic exists, every
/// lookup answers `COORDINATOR_NOT_AVAILABLE` and asks for the topic. Many
/// concurrent lookups start one creation, as the in-flight set of
/// `DefaultAutoTopicCreationManager` allows. Once the topic has a leader,
/// the lookup names this broker, and nothing more is created.
#[tokio::test]
async fn the_first_lookup_creates_the_state_topic_once_and_a_later_one_names_the_coordinator() {
    const LOOKUPS: usize = 8;
    let share_key = format!("share-group:{KAFKA_TOPIC_ID}:0");
    let cases = [
        (KEY_TYPE_GROUP, "group", OFFSETS_TOPIC),
        (KEY_TYPE_TRANSACTION, "tid", TXN_TOPIC),
        (KEY_TYPE_SHARE, share_key.as_str(), TOPIC),
    ];
    for (key_type, key, topic) in cases {
        let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
            config.offsets_topic_num_partitions = 3;
            config.transaction_state_num_partitions = 3;
            config.share_coordinator.state_topic_num_partitions = 3;
        })
        .await;
        broker_handle.wait_until_brokers_registered(1).await;
        let broker = broker_handle.broker_arc_for_test();
        let request = FindCoordinatorRequest {
            key_type,
            coordinator_keys: vec![key.to_owned()],
            ..Default::default()
        };

        let first = futures_util::future::join_all(
            (0..LOOKUPS).map(|_| find(&broker, &request, 6, "admin")),
        )
        .await;

        let unavailable = FindCoordinatorResponse {
            coordinators: vec![row(key, codes::COORDINATOR_NOT_AVAILABLE, None)],
            ..Default::default()
        };
        check!(first == vec![unavailable; LOOKUPS], "{topic}");
        check!(broker.auto_topic_creation.started() == 1, "{topic}");

        wait_until_creation_ends(&broker, topic).await;
        match key_type {
            KEY_TYPE_GROUP => broker_handle.wait_until_group_coordinator_ready().await,
            KEY_TYPE_TRANSACTION => {
                broker_handle
                    .wait_until_transaction_coordinator_ready()
                    .await;
            }
            _ => broker_handle.wait_until_share_coordinator_ready().await,
        }
        let resolved = find(&broker, &request, 6, "admin").await;
        let row = &resolved.coordinators[0];
        check!(
            resolved
                == FindCoordinatorResponse {
                    coordinators: vec![Coordinator {
                        key: key.to_owned(),
                        node_id: broker.config.broker_id,
                        host: row.host.clone(),
                        port: row.port,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            "{topic}"
        );
        check!(row.port > 0, "{topic}");
        check!(broker.auto_topic_creation.started() == 1, "{topic}");
        broker_handle.shutdown().await;
    }
}

/// The replication factor is never lowered to fit the cluster. With fewer
/// registered brokers than `offsets.topic.replication.factor`, the placement
/// refuses the creation, the lookup answers `COORDINATOR_NOT_AVAILABLE`, and no
/// topic exists. Once a second broker registers, the next lookup creates the
/// topic with a replica on each broker.
#[tokio::test]
async fn a_lookup_with_too_few_brokers_creates_nothing_until_enough_register() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
        config.offsets_topic_num_partitions = 3;
        config.offsets_topic_replication_factor = 2;
    })
    .await;
    broker_handle.wait_until_controller_leader().await;
    broker_handle.wait_until_brokers_registered(1).await;
    let broker = broker_handle.broker_arc_for_test();
    let topic = OFFSETS_TOPIC;
    let request = FindCoordinatorRequest {
        key_type: KEY_TYPE_GROUP,
        coordinator_keys: vec!["group".into()],
        ..Default::default()
    };
    let unavailable = FindCoordinatorResponse {
        coordinators: vec![row("group", codes::COORDINATOR_NOT_AVAILABLE, None)],
        ..Default::default()
    };

    for _ in 0..2 {
        check!(find(&broker, &request, 6, "admin").await == unavailable);
        wait_until_creation_ends(&broker, topic).await;
        check!(broker.controller.current_image().topic(topic).is_none());
    }
    check!(broker.auto_topic_creation.started() == 2);

    let other = krabka_metadata::NodeId(broker.config.node_id.0 + 1);
    broker
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: -1,
                incarnation_id: uuid::Uuid::from_u128(2),
                port: 9094,
                ..broker_registration(other)
            },
        )])
        .await
        .expect("register a second broker");

    check!(find(&broker, &request, 6, "admin").await == unavailable);
    wait_until_creation_ends(&broker, topic).await;
    let image = broker.controller.current_image();
    let created = image.topic(topic).expect("the topic is created");
    check!(created.replication_factor == 2);
    let mut expected = vec![broker.config.node_id, other];
    expected.sort();
    for record in image.partitions_of(topic) {
        let mut replicas = record.replicas.clone();
        replicas.sort();
        check!(replicas == expected, "partition {}", record.partition);
    }
    check!(image.partitions_of(topic).count() == 3);
    broker_handle.shutdown().await;
}
