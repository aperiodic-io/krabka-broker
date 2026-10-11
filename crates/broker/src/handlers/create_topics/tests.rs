//! End-to-end tests of the `CreateTopics` handler, driven over the wire
//! encoding against a running broker: the authorization gate, the per-topic
//! error rows, a successful create, and the KIP-599 mutation quota.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_metadata::{AclEntry, AclOperation, MetadataRecord, PatternType};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::{CreatableTopicConfigs, CreateTopicsResponse},
    },
};

use super::*;
use crate::{
    api_catalog::UnstableApiVersions,
    authorizer::{AllowAllAuthorizer, AuthorizationResult},
    broker::BrokerHandle,
    config::{BrokerConfig, RemoteStorageBackend},
    config_keys::{DISKLESS, serves_topic_key},
    handlers::{
        RequestContext,
        describe_configs::effective_topic_configs,
        test_support::{
            AclSetup, TopicPartitionCount, TopicReplicationFactor, replica_availability,
            seed_controller_quota as seed_controller_quota_helper,
        },
    },
    test_support::{
        AllowAclSetup, DenyAll, DescribedCreationSetup, KafkaErrorCode, allow_acl,
        begin_controlled_shutdown, broker_registration, described_creation_configs, dispatch_wire,
        fence_remote_broker, make_witness, peer, principal, seed_remote_broker,
        start_broker_no_audit_with,
    },
    throttle::LEADER_THROTTLED_REPLICAS_KEY,
};

/// Live broker and request identity for the wire-driven creation cases.
macro_rules! creation_fixture {
    (($handle:ident, $directory:ident, $broker:ident), ($principal:ident, $peer:ident), $authorizer:ident, $user:expr) => {
        broker_fixture!(($handle, $directory, $broker), $authorizer);
        request_identity!(($principal, $peer), principal($user));
    };
}

const VERSION: i16 = 7;

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`, the source a value the create request
/// carried reports.
const DYNAMIC_TOPIC_CONFIG: i8 = 1;

/// `ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG`, the source of a cluster-level
/// broker config.
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;

/// `ConfigSource.DEFAULT_CONFIG`, the source an untouched key reports.
const DEFAULT_CONFIG: i8 = 5;

/// The cluster-level `min.insync.replicas` of the test broker. Its bootstrap
/// records enable ELR, so its controller writes the static value, Kafka's
/// default of 1, as the cluster default when it seeds the log, as Kafka's
/// `ActivationRecordsGenerator.recordsForEmptyLog` does.
const CLUSTER_MIN_INSYNC_REPLICAS: (&str, &str) = ("min.insync.replicas", "1");

/// The KIP-525 configs list a v5+ row carries for a topic created with
/// `overrides` on a cluster whose only dynamic default is the cluster-level
/// `min.insync.replicas`.
///
/// Every topic-scope key is in it, so spelling the list out row by row would
/// transcribe the registry rather than say anything about the handler. What
/// this states instead is the layering the response must show: a key the
/// request set reads its value at `DYNAMIC_TOPIC_CONFIG`, the cluster default
/// reads at `DYNAMIC_DEFAULT_BROKER_CONFIG`, every other key reads the
/// built-in default at `DEFAULT_CONFIG`, a sensitive key's value is withheld,
/// and the list is sorted by name.
fn expected_configs(overrides: &[(&str, &str)]) -> Vec<CreatableTopicConfigs> {
    use crate::config_keys::registry::{self, ConfigScope};

    let mut configs: Vec<CreatableTopicConfigs> = registry::keys_in(ConfigScope::Topic)
        .filter(|row| {
            serves_topic_key(row.name, UnstableApiVersions::Disabled)
                && (!row.internal || overrides.iter().any(|(key, _)| *key == row.name))
        })
        .map(|row| {
            let stored = overrides
                .iter()
                .find(|(key, _)| *key == row.name)
                .map(|(_, value)| *value);
            let (cluster_key, cluster_value) = CLUSTER_MIN_INSYNC_REPLICAS;
            let unset = if row.name == cluster_key {
                (Some(cluster_value), DYNAMIC_DEFAULT_BROKER_CONFIG)
            } else {
                (row.default, DEFAULT_CONFIG)
            };
            let (value, config_source) =
                stored.map_or(unset, |value| (Some(value), DYNAMIC_TOPIC_CONFIG));
            tagged_wire!(CreatableTopicConfigs {
                name: row.name.to_owned(),
                value: (!row.is_sensitive())
                    .then_some(value)
                    .flatten()
                    .map(str::to_owned),
                read_only: row.read_only,
                config_source,
                is_sensitive: row.is_sensitive(),
            })
        })
        .collect();
    configs.sort_by(|left, right| left.name.cmp(&right.name));
    configs
}

/// Independent expected response fields, including the config-disclosure outcome.
#[derive(krabka_macros::FieldDefaults)]
struct ExpectedTopicSetup<'a> {
    #[default("orders")]
    name: &'a str,
    topic_id: ProtoUuid,
    error_code: KafkaErrorCode,
    error_message: Option<String>,
    #[default(TopicPartitionCount(-1))]
    num_partitions: TopicPartitionCount,
    #[default(TopicReplicationFactor(-1))]
    replication_factor: TopicReplicationFactor,
    configs: Vec<CreatableTopicConfigs>,
    topic_config_error_code: KafkaErrorCode,
}

fn expected_topic(setup: ExpectedTopicSetup<'_>) -> CreatableTopicResult {
    let ExpectedTopicSetup {
        name,
        topic_id,
        error_code,
        error_message,
        num_partitions,
        replication_factor,
        configs,
        topic_config_error_code,
    } = setup;
    tagged_wire!(CreatableTopicResult {
        name: name.into(),
        topic_id,
        error_code: error_code.0,
        error_message,
        num_partitions: num_partitions.0,
        replication_factor: replication_factor.0,
        configs: Some(configs),
        topic_config_error_code: topic_config_error_code.0,
    })
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct CreatedTopicExpectation<'a> {
    #[default("retries")]
    name: &'a str,
    #[default(TopicPartitionCount(1))]
    partitions: TopicPartitionCount,
    configs: &'a [(&'a str, &'a str)],
}

fn check_created_topic_response(
    response: &CreateTopicsResponse,
    setup: CreatedTopicExpectation<'_>,
) {
    assert!(response.topics.len() == 1);
    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![expected_topic(ExpectedTopicSetup {
            name: setup.name,
            topic_id: response.topics[0].topic_id,
            num_partitions: setup.partitions,
            replication_factor: TopicReplicationFactor(1),
            configs: expected_configs(setup.configs),
            ..Default::default()
        })],
    });
    assert!(*response == expected);
}

fn expected_empty_topic(
    name: &str,
    error_code: i16,
    error_message: Option<String>,
) -> CreatableTopicResult {
    expected_topic(ExpectedTopicSetup {
        name,
        error_code: KafkaErrorCode(error_code),
        error_message,
        ..Default::default()
    })
}

fn topic(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
    CreatableTopic {
        name: name.into(),
        num_partitions: partitions,
        replication_factor: rf,
        ..Default::default()
    }
}

fn topic_with_config(name: &str) -> CreatableTopic {
    CreatableTopic {
        configs: vec![CreatableTopicConfig {
            name: "retention.ms".into(),
            value: Some("60000".into()),
            ..Default::default()
        }],
        ..topic(name, 2, 1)
    }
}

fn topic_with_configs(name: &str, configs: &[(&str, &str)]) -> CreatableTopic {
    CreatableTopic {
        configs: configs
            .iter()
            .map(|(key, value)| CreatableTopicConfig {
                name: (*key).into(),
                value: Some((*value).into()),
                ..Default::default()
            })
            .collect(),
        ..topic(name, 1, 1)
    }
}

fn request(topics: Vec<CreatableTopic>) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics,
        timeout_ms: 5_000,
        ..Default::default()
    }
}

crate::test_support::context_helper!(client_id = "admin-client");

use crate::test_support::{start_broker_with_authorizer_no_audit as start_broker, test_ctx};

async fn drive(
    broker: &Broker,
    req: &CreateTopicsRequest,
    ctx: &RequestContext<'_>,
) -> CreateTopicsResponse {
    handle(broker, req.clone(), VERSION, ctx)
        .await
        .expect("handle")
}

async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    seed_controller_quota_helper(handle, rate).await;
}

/// A denial on cluster `Create` is a shortcut only. `DenyAll` also denies the
/// per-topic `Create` fallback, so the request still ends every row in
/// `TOPIC_AUTHORIZATION_FAILED` -- but never `CLUSTER_AUTHORIZATION_FAILED`,
/// which Kafka's `ControllerApis.createTopics` never answers for a topic row.
macro_rules! invalid_config_answer {
    (($response:ident, $message:ident), $broker:ident, $request:ident) => {
        let $response = drive(
            &$broker,
            &$request,
            &test_context(&principal("admin"), &peer()),
        )
        .await;
        assert!($response.topics[0].error_code == codes::INVALID_CONFIG);
        let $message = $response.topics[0]
            .error_message
            .as_deref()
            .unwrap_or_default();
    };
}

#[tokio::test]
async fn handle_falls_back_to_topic_authorization_failed_for_each_topic() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), deny_all, "alice");
    let req = request(vec![topic("orders", 1, 1), topic("payments", 1, 1)]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![
            expected_empty_topic(
                "orders",
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some("Authorization failed.".into())
            ),
            expected_empty_topic(
                "payments",
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some("Authorization failed.".into())
            ),
        ],
    });
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("orders")
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_invalid_partition_count_and_replication_factor() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic("bad-count", 0, 1), topic("bad-rf", 1, 2)]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let expected =
        unthrottled_wire!(CreateTopicsResponse {
            topics: vec![
            expected_empty_topic("bad-count", codes::INVALID_PARTITIONS, Some(
                    "Number of partitions was set to an invalid non-positive value.".into(),
                )),
            expected_empty_topic("bad-rf", codes::INVALID_REPLICATION_FACTOR, Some(
                    "Unable to replicate the partition 2 time(s): The target replication factor \
                     of 2 cannot be reached because only 1 broker(s) are registered or some \
                     brokers have all their log directories cordoned."
                        .into(),
                )),
        ],
        });
    assert!(resp == expected);
    for name in ["bad-count", "bad-rf"] {
        let image = broker_handle.controller_image_for_test();
        assert!(image.topic(name).is_none(), "topic {name} not committed");
    }
    broker_handle.shutdown().await;
}

/// KIP-464: `num_partitions = -1` and `replication_factor = -1` take the
/// broker's `num.partitions` and `default.replication.factor` (#728). That is
/// what `kafka-topics --create` sends without `--partitions` and
/// `--replication-factor`. Kafka's `ReplicationControlManager.createTopic`
/// refuses a replication factor of 0 or below -1 first, then a partition count
/// of 0 or below -1.
#[tokio::test]
async fn minus_one_takes_the_broker_topic_creation_defaults() {
    const BAD_RF: &str =
        "Replication factor must be larger than 0, or -1 to use the default value.";
    const BAD_COUNT: &str = "Number of partitions was set to an invalid non-positive value.";
    /// (requested partitions, requested replication factor, error code,
    /// error message, created partitions, created replication factor)
    type Row = (i32, i16, i16, Option<&'static str>, i32, i16);

    let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.num_partitions = 4;
        cfg.default_replication_factor = 2;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    for node_id in [2, 3, 4] {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    broker_epoch: -1,
                    ..broker_registration(krabka_raft::NodeId(node_id))
                },
            )])
            .await
            .expect("seed broker registration");
    }

    let rows: [Row; 8] = [
        (-1, -1, codes::NONE, None, 4, 2),
        (-1, 3, codes::NONE, None, 4, 3),
        (6, -1, codes::NONE, None, 6, 2),
        (0, 1, codes::INVALID_PARTITIONS, Some(BAD_COUNT), -1, -1),
        (-2, 1, codes::INVALID_PARTITIONS, Some(BAD_COUNT), -1, -1),
        (
            1,
            0,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
        (
            0,
            0,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
        (
            1,
            -2,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
    ];
    for (row, (partitions, rf, error_code, error_message, created, created_rf)) in
        rows.into_iter().enumerate()
    {
        let name = format!("defaults-{row}");
        let resp = drive(
            &broker,
            &request(vec![topic(&name, partitions, rf)]),
            &test_context(&principal("admin"), &peer()),
        )
        .await;

        let image = broker_handle.controller_image_for_test();
        let created_ok = error_code == codes::NONE;
        let expected = unthrottled_wire!(CreateTopicsResponse {
            topics: vec![expected_topic(ExpectedTopicSetup {
                name: &name,
                topic_id: image.topic(&name).map_or(ProtoUuid([0; 16]), |topic| {
                    ProtoUuid(topic.topic_id.into_bytes())
                }),
                error_code: KafkaErrorCode(error_code),
                error_message: error_message.map(str::to_owned),
                num_partitions: TopicPartitionCount(created),
                replication_factor: TopicReplicationFactor(created_rf),
                configs: if created_ok {
                    expected_configs(&[])
                } else {
                    Vec::new()
                },
                ..Default::default()
            })],
        });
        check!(resp == expected, "requested ({partitions}, {rf})");

        let committed = image
            .partitions_of(&name)
            .map(|partition| i16::try_from(partition.replicas.len()).expect("replication factor"))
            .collect::<Vec<_>>();
        let expected_committed =
            vec![created_rf; usize::try_from(created.max(0)).expect("partition count")];
        check!(
            committed == expected_committed,
            "requested ({partitions}, {rf})"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_success_persists_topic_config_and_success_fields() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_config("configured")]);

    let resp = drive_single_topic(&broker, &req, &test_context(&p, &peer)).await;
    assert!(resp.topics[0].topic_id != ProtoUuid([0; 16]));
    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![
            // Randomly generated per create; the non-nil assertion above pins it.
            expected_topic(ExpectedTopicSetup {
                name: "configured",
                topic_id: resp.topics[0].topic_id,
                num_partitions: TopicPartitionCount(2),
                replication_factor: TopicReplicationFactor(1),
                configs: expected_configs(&[("retention.ms", "60000")]),
                ..Default::default()
            })
        ],
    });
    assert!(resp == expected);

    let image = broker_handle.controller_image_for_test();
    let topic = image.topic("configured").expect("topic in image");
    assert!(topic.partitions == 2);
    let configs = image.topic_config("configured").expect("topic configs");
    assert!(configs.get("retention.ms").map(String::as_str) == Some("60000"));
    broker_handle.shutdown().await;
}

/// Kafka Streams creates every windowed-store changelog topic with
/// `cleanup.policy=compact,delete`, so a broker that refuses the list value
/// cannot host a Streams application with a windowed store. The value is
/// stored as the client sent it, which is what `DescribeConfigs` echoes back.
#[tokio::test]
async fn handle_creates_a_topic_whose_cleanup_policy_names_both_halves() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_configs(
        "windowed-changelog",
        &[
            ("cleanup.policy", "compact,delete"),
            ("min.compaction.lag.ms", "0"),
            ("message.timestamp.type", "CreateTime"),
        ],
    )]);

    let resp = drive_single_topic(&broker, &req, &test_context(&p, &peer)).await;
    assert!(resp.topics[0].error_code == codes::NONE);
    let image = broker_handle.controller_image_for_test();
    let configs = image
        .topic_config("windowed-changelog")
        .expect("topic configs");
    assert!(configs.get("cleanup.policy").map(String::as_str) == Some("compact,delete"));
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_invalid_topic_configs_before_creating_the_topic() {
    /// One rejection case: the row's label, a config map that must never
    /// reach the metadata quorum, and the substrings the operator needs to
    /// see in the rejection.
    type RejectedConfig<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a [&'a str]);

    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");

    let cases: [RejectedConfig<'_>; 10] = [
        (
            "unknown-key",
            &[("not.a.topic.config", "1000")],
            &["not.a.topic.config"],
        ),
        (
            "compacted-and-tiered",
            &[
                ("cleanup.policy", "compact"),
                ("remote.storage.enable", "true"),
            ],
            &["Tiered Storage functionality is disabled in the broker"],
        ),
        (
            "compact-and-delete-and-tiered",
            &[
                ("cleanup.policy", "compact,delete"),
                ("remote.storage.enable", "true"),
            ],
            &["Tiered Storage functionality is disabled in the broker"],
        ),
        (
            "bad-delivery-mode",
            &[("delivery.mode", "later")],
            &["delivery.mode"],
        ),
        (
            "bad-delivery-delay",
            &[("delivery.max.delay.ms", "-2")],
            &["-2"],
        ),
        (
            "compacted-schedule",
            &[
                ("cleanup.policy", "compact"),
                ("delivery.mode", "scheduled"),
            ],
            &["cleanup.policy", "delivery.mode"],
        ),
        (
            "bad-diskless",
            &[("krabka.diskless", "yes")],
            &["krabka.diskless"],
        ),
        (
            "diskless-and-tiered",
            &[
                ("krabka.diskless", "true"),
                ("remote.storage.enable", "true"),
            ],
            &["krabka.diskless", "remote.storage.enable"],
        ),
        (
            "diskless-and-scheduled",
            &[("krabka.diskless", "true"), ("delivery.mode", "scheduled")],
            &["krabka.diskless", "delivery.mode"],
        ),
        // `BrokerConfig::for_tests` configures no object store, and a diskless
        // topic without one could never flush or trim: the broker starts no
        // WAL index projection and no object flusher, so the local logs would
        // grow without bound behind a flag that advertises the opposite. This
        // is the one case here that the pure key/value validator cannot catch,
        // because it depends on the broker's own configuration.
        (
            "diskless-without-an-object-tier",
            &[(DISKLESS, "true")],
            &[DISKLESS, "remote_storage_backend"],
        ),
    ];

    for (name, configs, needles) in cases {
        let request = request(vec![topic_with_configs(name, configs)]);

        let resp = drive(&broker, &request, &test_context(&p, &peer)).await;

        assert!(resp.topics.len() == 1, "topic {name}");
        check!(
            resp.topics[0].error_code == codes::INVALID_CONFIG,
            "topic {name}"
        );
        let message = resp.topics[0].error_message.clone().unwrap_or_default();
        for needle in needles {
            check!(message.contains(needle), "topic {name}: {message}");
        }
        check!(
            broker_handle
                .controller_image_for_test()
                .topic(name)
                .is_none(),
            "topic {name} must not be created"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_creates_a_scheduled_topic_and_persists_its_delivery_configs() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_configs(
        "retries",
        &[
            ("delivery.mode", "scheduled"),
            ("delivery.max.delay.ms", "-1"),
            ("delivery.schedule.monotonic", "true"),
        ],
    )]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    check_created_topic_response(
        &resp,
        CreatedTopicExpectation {
            configs: &[
                ("delivery.mode", "scheduled"),
                ("delivery.max.delay.ms", "-1"),
                ("delivery.schedule.monotonic", "true"),
            ],
            ..Default::default()
        },
    );

    let image = broker_handle.controller_image_for_test();
    let configs = image.topic_config("retries").expect("topic configs");
    let expected_configs = maplit::btreemap! {
    "delivery.mode".to_string() => "scheduled".to_string(),
    "delivery.max.delay.ms".to_string() => "-1".to_string(),
    "delivery.schedule.monotonic".to_string() => "true".to_string()};
    assert!(*configs == expected_configs);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_creates_a_diskless_topic_and_opens_its_partitions_on_the_wal_path() {
    // A diskless topic needs an object-store tier: without one the broker
    // starts no WAL index projection and no object flusher, and the handler
    // refuses the opt-in rather than create a topic that could never flush or
    // trim. Configure the tier this test's topic depends on.
    let object_store = tempfile::TempDir::new().expect("object store dir");
    let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.rack = Some("rack-a".into());
        cfg.diskless_wal_local_replica_count = 1;
        cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
            dir: object_store.path().to_path_buf(),
        });
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!((p, peer), principal("admin"));
    let req = request(vec![topic_with_configs(
        "events",
        &[("krabka.diskless", "true")],
    )]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    check_created_topic_response(
        &resp,
        CreatedTopicExpectation {
            name: "events",
            configs: &[("krabka.diskless", "true")],
            ..Default::default()
        },
    );

    // The override reaches the metadata log unchanged, ...
    let image = broker_handle.controller_image_for_test();
    let configs = image.topic_config("events").expect("topic configs");
    let expected_configs = maplit::btreemap! {"krabka.diskless".to_string() => "true".to_string()};
    assert!(*configs == expected_configs);
    // ... and the partition the handler materialized is on the diskless
    // runtime, which is the whole point of the key.
    let partition = broker
        .partitions
        .get("events", krabka_ids::PartitionIndex(0))
        .expect("partition materialized locally");
    assert!(partition.diskless);

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_diskless_topic_without_a_rack_safe_wal_quorum() {
    let object_store = tempfile::TempDir::new().expect("object store dir");
    broker_fixture!(
        (broker_handle, _dir, broker),
        local_object_store(object_store)
    );
    let req = request(vec![topic_with_configs(
        "unplaceable-diskless",
        &[("krabka.diskless", "true")],
    )]);

    invalid_config_answer!((resp, message), broker, req);
    for needle in [
        "partition 0",
        "leader 1",
        "0 eligible",
        "3 are required",
        "broker.rack",
    ] {
        check!(message.contains(needle), "{message}");
    }
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("unplaceable-diskless")
            .is_none()
    );
    broker_handle.shutdown().await;
}

/// The diskless WAL placement check names the leader the partition will
/// have, which is the first active replica of a manual assignment (#741), not
/// the first listed one.
#[tokio::test]
async fn diskless_wal_validation_names_the_active_leader_of_a_manual_assignment() {
    let object_store = tempfile::TempDir::new().expect("object store dir");
    broker_fixture!(
        (broker_handle, _dir, broker),
        local_object_store(object_store)
    );
    for node_id in [2, 3] {
        seed_remote_broker(&broker_handle, krabka_raft::NodeId(node_id)).await;
    }
    fence_remote_broker(&broker_handle, 2).await;
    let req = request(vec![CreatableTopic {
        name: "manual-diskless".into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            krabka_protocol::owned::create_topics_request::CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![2, 3],
                ..Default::default()
            },
        ],
        configs: vec![CreatableTopicConfig {
            name: "krabka.diskless".into(),
            value: Some("true".into()),
            ..Default::default()
        }],
        ..Default::default()
    }]);

    invalid_config_answer!((resp, message), broker, req);
    check!(message.contains("partition 0 leader 3 "), "{message}");
    broker_handle.shutdown().await;
}

#[test]
fn diskless_wal_validation_uses_the_local_registration_fallback() {
    let dir = tempfile::TempDir::new().expect("log dir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.rack = Some("rack-a".into());
    config.diskless_wal_local_replica_count = 1;

    assert!(
        diskless_wal_placement_error(
            &krabka_metadata::MetadataImage::default(),
            &config,
            0,
            &[InitialLeadership {
                leader: config.node_id,
                isr: vec![config.node_id],
            }],
        )
        .is_none()
    );
}

#[tokio::test]
async fn a_created_topic_with_the_key_off_stays_on_the_local_log_path() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_configs(
        "plain",
        &[("krabka.diskless", "false")],
    )]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    assert!(resp.topics[0].error_code == codes::NONE);
    let partition = broker
        .partitions
        .get("plain", krabka_ids::PartitionIndex(0))
        .expect("partition materialized locally");
    assert!(!partition.diskless);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn duplicate_topic_reports_error_without_success_fields() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic("dupe", 1, 1)]);
    let first = drive(&broker, &req, &test_context(&p, &peer)).await;
    assert!(first.topics[0].error_code == codes::NONE);

    let second = drive(&broker, &req, &test_context(&p, &peer)).await;

    assert!(second.topics.len() == 1);
    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![expected_empty_topic(
            "dupe",
            codes::TOPIC_ALREADY_EXISTS,
            Some("Topic 'dupe' already exists.".into())
        )],
    });
    assert!(second == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn validate_only_answers_the_verdict_and_commits_nothing() {
    /// One dry run: the topic name, and the row it has to answer with.
    type DryRun<'a> = (&'a str, CreatableTopicResult);

    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let committed = drive(
        &broker,
        &request(vec![topic("existing", 1, 1)]),
        &test_context(&p, &peer),
    )
    .await;
    assert!(committed.topics[0].error_code == codes::NONE);

    let cases: [DryRun<'_>; 2] = [
        (
            // Kafka's `validateOnly` reports `TopicExistsException` for a name
            // that is already taken, with the committing path's message.
            "existing",
            CreatableTopicResult {
                name: "existing".into(),
                error_code: codes::TOPIC_ALREADY_EXISTS,
                error_message: Some("Topic 'existing' already exists.".into()),
                num_partitions: -1,
                replication_factor: -1,
                configs: Some(Vec::new()),
                ..Default::default()
            },
        ),
        (
            // KIP-525 answers a dry run with the configuration the topic
            // *would* be created with: Kafka builds the row in `createTopic`
            // from `computeEffectiveTopicConfigs(creationConfigs)`, and
            // `validateOnly` discards the records alone. The list is filled in
            // below, against the same image the handler resolved it from.
            "fresh",
            CreatableTopicResult {
                name: "fresh".into(),
                error_code: codes::NONE,
                error_message: None,
                num_partitions: 1,
                replication_factor: 1,
                configs: None,
                ..Default::default()
            },
        ),
    ];

    for (name, expected_row) in cases {
        let req = CreateTopicsRequest {
            validate_only: true,
            ..request(vec![topic(name, 1, 1)])
        };

        let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

        assert!(resp.topics.len() == 1, "topic {name}");
        // A refused row carries an empty configuration list -- Kafka's
        // generated default for `Configs`, which the refusal never sets; the
        // one that passed carries what the topic would have been created
        // with. The dry run committed nothing, so that is the empty override map resolved
        // against the image.
        let configs = if expected_row.error_code == codes::NONE {
            Some(creatable_topic_configs(effective_topic_configs(
                &broker_handle.controller_image_for_test(),
                broker.config.node_id,
                name,
                &std::collections::BTreeMap::new(),
                UnstableApiVersions::Disabled,
                &std::collections::BTreeMap::new(),
            )))
        } else {
            expected_row.configs.clone()
        };
        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            // A fresh topic_id is generated before the verdict on every row.
            topics: vec![CreatableTopicResult {
                topic_id: resp.topics[0].topic_id,
                configs,
                ..expected_row
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "topic {name}");
    }

    // The dry run for "fresh" passed every check and still committed nothing.
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("fresh")
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn strict_create_topics_rejects_after_quota_exhaustion() {
    let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!((p, peer), principal("admin"));
    let req = request(vec![topic("throttled", 5, 1)]);

    let charged_no_earlier_than = std::time::Instant::now();
    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    check_created_topic_response(
        &resp,
        CreatedTopicExpectation {
            name: "throttled",
            partitions: TopicPartitionCount(5),
            ..Default::default()
        },
    );

    let rejected = drive(
        &broker,
        &request(vec![topic("rejected", 1, 1)]),
        &test_context(&p, &peer),
    )
    .await;
    // Five mutations against a two-mutation burst at 2/sec leave three of
    // debt: 1.5 s to refill, less what refilled since the first request was
    // charged. Kafka reports the whole refill time, with no cap (#709). The
    // refill since the charge is at most the time since just before it, plus
    // the millisecond that truncating both durations can lose.
    let refilled_at_most = i32::try_from(charged_no_earlier_than.elapsed().as_millis())
        .expect("the test runs for well under i32::MAX ms")
        + 1;
    check!(
        (1_500 - refilled_at_most..=1_500).contains(&rejected.throttle_time_ms),
        "{rejected:?} after {refilled_at_most} ms"
    );
    let expected = CreateTopicsResponse {
        throttle_time_ms: rejected.throttle_time_ms,
        topics: vec![CreatableTopicResult {
            name: "rejected".into(),
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            error_message: Some("The throttling quota has been exceeded.".into()),
            configs: Some(Vec::new()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(rejected == expected);
    broker_handle.shutdown().await;
}

/// An authorizer that allows everything but `DescribeConfigs`, the operation
/// KIP-525 hangs the configs disclosure on.
#[derive(Debug)]
struct DenyDescribeConfigs;

test_authorizer!(DenyDescribeConfigs, (self, _source, req), {
    if req.operation == krabka_metadata::AclOperation::DescribeConfigs {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// KIP-525: the two layers a created topic's configs list distinguishes. The
/// whole-list expectations elsewhere in this file are built from the registry,
/// so this case names the two rows the layering turns on and their values.
#[tokio::test]
async fn created_topic_configs_separate_a_request_value_from_an_inherited_default() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_config("effective")]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let configs = resp.topics[0].configs.clone().expect("v5+ configs list");
    let entry = |name: &str| {
        configs
            .iter()
            .find(|entry| entry.name == name)
            .cloned()
            .unwrap_or_else(|| panic!("{name} in the configs list"))
    };
    // The value this very request carried, at DYNAMIC_TOPIC_CONFIG (1).
    let expected_retention = tagged_wire!(CreatableTopicConfigs {
        name: "retention.ms".into(),
        value: Some("60000".into()),
        read_only: false,
        config_source: DYNAMIC_TOPIC_CONFIG,
        is_sensitive: false,
    });
    check!(entry("retention.ms") == expected_retention);
    // A key the request never mentioned, at DEFAULT_CONFIG (5).
    let expected_cleanup = tagged_wire!(CreatableTopicConfigs {
        name: "cleanup.policy".into(),
        value: Some("delete".into()),
        read_only: false,
        config_source: DEFAULT_CONFIG,
        is_sensitive: false,
    });
    check!(entry("cleanup.policy") == expected_cleanup);
    check!(resp.topics[0].topic_config_error_code == 0);
    broker_handle.shutdown().await;
}

/// The list a `CreateTopics` row carries is the list `DescribeConfigs`
/// answers for the same topic. A client that reads
/// `createTopics(...).config(topic)` instead of issuing a follow-up
/// `DescribeConfigs` -- Terraform's `kafka_topic`, Connect's `TopicAdmin`,
/// Streams' `InternalTopicManager` -- must see no difference.
#[tokio::test]
async fn created_topic_configs_match_describe_configs_for_the_same_topic() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let req = request(vec![topic_with_config("mirrored")]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let image = broker_handle.controller_image_for_test();
    let described = described_creation_configs(
        &image,
        DescribedCreationSetup {
            node: broker.config.node_id,
            topic: "mirrored",
            overrides: image.topic_config("mirrored").expect("stored overrides"),
        },
    );
    assert!(resp.topics[0].configs.clone().expect("configs") == described);
    broker_handle.shutdown().await;
}

/// KIP-525: a principal that may create a topic but may not describe its
/// configs still gets the topic. Kafka withholds only the disclosure -- an
/// empty list, `TOPIC_AUTHORIZATION_FAILED` on `topicConfigErrorCode`, and
/// neither the partition count nor the replication factor, because
/// `AdminClient` fails every accessor on the row once the code is set.
#[tokio::test]
async fn create_without_describe_configs_withholds_the_configs_but_creates_the_topic() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker(Arc::new(DenyDescribeConfigs))
    );
    request_identity!((p, peer), principal("admin"));
    let req = request(vec![topic_with_config("undescribable")]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![expected_topic(ExpectedTopicSetup {
            name: "undescribable",
            topic_id: resp.topics[0].topic_id,
            topic_config_error_code: KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED),
            ..Default::default()
        })],
    });
    assert!(resp == expected);
    // The create itself went through: only the disclosure was withheld.
    let image = broker_handle.controller_image_for_test();
    assert!(image.topic("undescribable").is_some());
    broker_handle.shutdown().await;
}

/// `configs` and `topicConfigErrorCode` arrived in v5, so a v4 response
/// carries neither and encodes as it always did. The handler skips the
/// `DescribeConfigs` check there too: nothing it decides can reach the wire.
#[tokio::test]
async fn v4_response_encodes_without_the_kip_525_fields() {
    const V4: i16 = 4;

    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker(Arc::new(DenyDescribeConfigs))
    );
    let req = request(vec![topic_with_config("legacy")]);
    test_ctx!(ctx, "admin");

    let resp: CreateTopicsResponse = dispatch_wire(
        &broker,
        krabka_protocol::api_key::ApiKey::CreateTopics as i16,
        V4,
        &req,
        &ctx,
    )
    .await;
    let expected = unthrottled_wire!(CreateTopicsResponse {
        topics: vec![expected_empty_topic("legacy", codes::NONE, None)],
    });
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("legacy")
            .is_some()
    );
    broker_handle.shutdown().await;
}

/// Every file and directory under `root`, as paths relative to it, sorted.
fn tree(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            out.push(path.strip_prefix(root).expect("under root").to_path_buf());
            if path.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Kafka's topic-name rules: `ControllerApis` refuses `__cluster_metadata`,
/// and `ReplicationControlManager.validateNewTopicNames` refuses a name that
/// `Topic.validate` refuses or that collides with an existing topic. A refused
/// name commits nothing and creates no directory, inside the log directory or
/// outside it.
#[tokio::test]
async fn handle_refuses_invalid_and_colliding_topic_names() {
    /// One row: the requested name, and the error code and message it gets.
    type NameCase = (String, i16, Option<String>);

    let (broker_handle, dir) = start_broker_no_audit_with(|cfg| {
        cfg.log_dir = cfg.log_dir.join("logs");
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    request_identity!((p, peer), principal("admin"));
    let existing = drive(
        &broker,
        &request(vec![topic("a_b", 1, 1)]),
        &test_context(&p, &peer),
    )
    .await;
    assert!(existing.topics[0].error_code == codes::NONE);

    let absolute = format!("{}/escape", dir.path().display());
    let too_long = "a".repeat(250);
    let illegal = |name: &str| {
        Some(format!(
            "Topic name is invalid: '{name}' contains one or more characters other than ASCII \
             alphanumerics, '.', '_' and '-'"
        ))
    };
    let cases: Vec<NameCase> = vec![
        (
            String::new(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: the empty string is not allowed".into()),
        ),
        (
            ".".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: '.' is not allowed".into()),
        ),
        (
            "..".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: '..' is not allowed".into()),
        ),
        (
            too_long.clone(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some(format!(
                "Topic name is invalid: the length of '{too_long}' is longer than the max \
                 allowed length 249"
            )),
        ),
        ("a/b".into(), codes::INVALID_TOPIC_EXCEPTION, illegal("a/b")),
        (
            "../x".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            illegal("../x"),
        ),
        (
            absolute.clone(),
            codes::INVALID_TOPIC_EXCEPTION,
            illegal(&absolute),
        ),
        ("a b".into(), codes::INVALID_TOPIC_EXCEPTION, illegal("a b")),
        (
            "a.b".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic 'a.b' collides with existing topic: a_b".into()),
        ),
        (
            "__cluster_metadata".into(),
            codes::INVALID_REQUEST,
            Some("Creation of internal topic __cluster_metadata is prohibited.".into()),
        ),
    ];

    let before = tree(dir.path());
    for validate_only in [true, false] {
        for (name, error_code, error_message) in &cases {
            let req = CreateTopicsRequest {
                validate_only,
                ..request(vec![topic(name, 1, 1)])
            };

            let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

            let expected = unthrottled_wire!(CreateTopicsResponse {
                topics: vec![expected_empty_topic(
                    name,
                    *error_code,
                    error_message.clone()
                )],
            });
            check!(resp == expected, "{name:?} validate_only={validate_only}");
            check!(
                broker_handle
                    .controller_image_for_test()
                    .topic(name)
                    .is_none(),
                "{name:?}"
            );
        }
    }
    check!(tree(dir.path()) == before);

    // The longest legal name is accepted.
    let longest = "a".repeat(249);
    let created = drive(
        &broker,
        &request(vec![topic(&longest, 1, 1)]),
        &test_context(&p, &peer),
    )
    .await;
    check!(created.topics[0].error_code == codes::NONE);
    broker_handle.shutdown().await;
}

/// A manual assignment keeps its replica list, but its ISR holds only the
/// listed brokers that are active, and its leader is the first of them
/// (#741). Kafka's `ReplicationControlManager.createTopic` filters the ISR
/// with `ClusterControlManager.isActive`, and answers
/// `INVALID_REPLICA_ASSIGNMENT` when no listed broker is active.
///
/// Brokers 2, 3 and 4 are remote registrations. A fenced heartbeat on the
/// controller makes a broker unavailable, as a real fenced broker is.
#[tokio::test]
async fn manual_assignment_leaves_unavailable_brokers_out_of_the_isr() {
    const V4: i16 = 4;

    /// One row: the fenced brokers, the witness brokers, the replica list of
    /// each partition, and the expected error code, error message and
    /// `(leader, isr)` per partition.
    use crate::handlers::test_support::ManualAssignmentRow as Row;
    let n = krabka_raft::NodeId;
    let rows: [Row; 7] = [
        (
            &[],
            &[],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(2), vec![n(2), n(3), n(4)])],
        ),
        (
            &[2],
            &[],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(3), vec![n(3), n(4)])],
        ),
        (
            &[2, 3],
            &[],
            &[&[3, 2, 4], &[4, 3, 2]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(4)]), (n(4), vec![n(4)])],
        ),
        (
            &[2],
            &[],
            &[&[2]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 0 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[3],
            &[],
            &[&[2], &[3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 1 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(3), n(4)])],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All active brokers specified in the manual partition assignment for partition \
                 0 are witnesses, and a witness cannot lead.",
            ),
            vec![],
        ),
    ];

    for (fenced, witnesses, lists, error_code, error_message, partitions) in rows {
        broker_fixture!((broker_handle, _dir, broker), allow_all);
        for node_id in [2, 3, 4] {
            seed_remote_broker(&broker_handle, krabka_raft::NodeId(node_id)).await;
        }
        for &node_id in witnesses {
            make_witness(&broker_handle, node_id).await;
        }
        for &node_id in fenced {
            fence_remote_broker(&broker_handle, node_id).await;
        }
        let topic = CreatableTopic {
            name: "manual".into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: lists
                .iter()
                .enumerate()
                .map(|(index, broker_ids)| {
                    krabka_protocol::owned::create_topics_request::CreatableReplicaAssignment {
                        partition_index: i32::try_from(index).expect("index"),
                        broker_ids: broker_ids.to_vec(),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        };
        test_ctx!(ctx, "admin");

        let resp: CreateTopicsResponse = dispatch_wire(
            &broker,
            krabka_protocol::api_key::ApiKey::CreateTopics as i16,
            V4,
            &request(vec![topic]),
            &ctx,
        )
        .await;

        let expected = unthrottled_wire!(CreateTopicsResponse {
            topics: vec![expected_empty_topic(
                "manual",
                error_code,
                error_message.map(str::to_owned)
            )],
        });
        check!(resp == expected, "fenced {fenced:?}, assignment {lists:?}");

        let image = broker_handle.controller_image_for_test();
        let committed = (0..)
            .map_while(|index| image.partition("manual", index).cloned())
            .collect::<Vec<_>>();
        let expected_records = partitions
            .into_iter()
            .zip(lists)
            .enumerate()
            .map(
                |(index, ((leader, isr), replicas))| krabka_metadata::PartitionRecord {
                    topic: "manual".into(),
                    partition: i32::try_from(index).expect("index"),
                    leader,
                    replicas: replicas
                        .iter()
                        .map(|&id| n(u64::try_from(id).expect("broker id")))
                        .collect(),
                    isr,
                    leader_epoch: krabka_metadata::LeaderEpoch(INITIAL_LEADER_EPOCH),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                },
            )
            .collect::<Vec<_>>();
        check!(
            committed == expected_records,
            "fenced {fenced:?}, assignment {lists:?}"
        );
        broker_handle.shutdown().await;
    }
}

/// #1201: Kafka's `ClusterControlManager.usableBrokers` hands the placer every
/// registered broker that is not in controlled shutdown, fenced ones included.
/// `StripedReplicaPlacer` takes a fenced broker last and never first, and
/// refuses only when the replication factor exceeds that count, or when no
/// broker is unfenced. `createTopic` then sets the ISR to the replicas that
/// pass `isActive`, and the first of them leads.
///
/// The cluster is the local broker 1 and the remote brokers 2 and 3.
#[tokio::test]
async fn automatic_placement_takes_fenced_brokers_last_and_shrinks_the_isr() {
    /// One row: the fenced brokers, the brokers in controlled shutdown, the
    /// replication factor, and either the brokers every replica list holds or
    /// the refusal message.
    type Row = (
        &'static [u64],
        &'static [u64],
        i16,
        Result<&'static [u64], &'static str>,
    );
    let rows: [Row; 6] = [
        (&[3], &[], 3, Ok(&[1, 2, 3])),
        (&[2, 3], &[], 3, Ok(&[1, 2, 3])),
        (&[2, 3], &[], 1, Ok(&[1])),
        (&[2], &[3], 2, Ok(&[1, 2])),
        (
            &[3],
            &[],
            4,
            Err(
                "Unable to replicate the partition 4 time(s): The target replication factor of 4 \
                 cannot be reached because only 3 broker(s) are registered or some brokers have \
                 all their log directories cordoned.",
            ),
        ),
        (
            &[],
            &[3],
            3,
            Err(
                "Unable to replicate the partition 3 time(s): The target replication factor of 3 \
                 cannot be reached because only 2 broker(s) are registered or some brokers have \
                 all their log directories cordoned.",
            ),
        ),
    ];

    for (fenced, shutting_down, rf, outcome) in rows {
        broker_fixture!((broker_handle, _dir, broker), allow_all);
        for node_id in [2, 3] {
            seed_remote_broker(&broker_handle, krabka_raft::NodeId(node_id)).await;
        }
        for &node_id in fenced {
            fence_remote_broker(&broker_handle, node_id).await;
        }
        for &node_id in shutting_down {
            begin_controlled_shutdown(&broker_handle, node_id).await;
        }
        request_identity!((p, peer), principal("admin"));

        let resp = drive(
            &broker,
            &request(vec![topic("auto", 4, rf)]),
            &test_context(&p, &peer),
        )
        .await;

        let image = broker_handle.controller_image_for_test();
        let committed = (0..4)
            .filter_map(|index| image.partition("auto", index).cloned())
            .collect::<Vec<_>>();
        let label = format!("fenced {fenced:?}, shutting down {shutting_down:?}, rf {rf}");
        match outcome {
            Err(message) => {
                let expected = expected_empty_topic(
                    "auto",
                    codes::INVALID_REPLICATION_FACTOR,
                    Some(message.into()),
                );
                check!(resp.topics == vec![expected], "{label}");
                check!(committed.is_empty(), "{label}");
            }
            Ok(brokers) => {
                check!(resp.topics[0].error_code == codes::NONE, "{label}");
                check!(committed.len() == 4, "{label}");
                for (index, record) in committed.iter().enumerate() {
                    let replicas = &record.replicas;
                    let mut held = replicas.iter().map(|node| node.0).collect::<Vec<_>>();
                    held.sort_unstable();
                    // A fenced replica comes after every unfenced one.
                    let (fenced_flags, isr) = replica_availability(replicas, fenced);
                    check!(held == brokers, "{label}, partition {index}");
                    check!(fenced_flags.is_sorted(), "{label}, partition {index}");
                    check!(
                        *record
                            == krabka_metadata::PartitionRecord {
                                topic: "auto".into(),
                                partition: i32::try_from(index).expect("index"),
                                leader: isr[0],
                                replicas: replicas.clone(),
                                isr,
                                leader_epoch: krabka_metadata::LeaderEpoch(INITIAL_LEADER_EPOCH),
                                adding_replicas: vec![],
                                removing_replicas: vec![],
                                directories: vec![],
                                partition_epoch: 0,
                            },
                        "{label}, partition {index}"
                    );
                }
            }
        }
        broker_handle.shutdown().await;
    }
}

/// #1235: Kafka's `Uuid.randomUuid` never returns an id whose base64url form
/// starts with a dash, and `createTopic` draws the topic id with it. One id in
/// 64 would start with one otherwise, so a request of 2500 topics misses a
/// missing check with a chance of e^-39. A validate-only request runs the same
/// checks and mints the same ids without committing.
#[tokio::test]
async fn topic_ids_never_start_with_a_dash() {
    creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
    let mut req = request(
        (0..2500)
            .map(|n| topic(&format!("topic-{n}"), 1, 1))
            .collect(),
    );
    req.validate_only = true;

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    check!(resp.topics.len() == 2500);
    for row in &resp.topics {
        check!(row.error_code == codes::NONE, "{}", row.name);
        let id = uuid::Uuid::from_bytes(row.topic_id.0);
        check!(!id.is_nil() && id.as_u128() != 1);
        check!(
            !krabka_format::ClusterId(id).to_string().starts_with('-'),
            "{}",
            row.name
        );
    }
    broker_handle.shutdown().await;
}

/// #698 / #1058: Kafka's `Create` decision for a `CreateTopics` request,
/// table-driven over which ACL `alice` holds.
///
/// `ControllerApis.handleCreateTopics`/`createTopics` filters duplicate names
/// and the protected `__cluster_metadata` name out before authorizing either
/// way, then checks cluster `Create` once as a shortcut, and falls back to
/// `Create` on each surviving `Topic(name)` when that shortcut is denied. A
/// literal ACL on `a` never covers `app-x`, and a prefixed ACL on `app-`
/// never covers `a` -- exactly the Kafka Streams/Connect application-id ACL
/// shape #698 reported as unusable. The request repeats `b` and asks for
/// `__cluster_metadata`; both are refused under every ACL shape.
#[tokio::test]
async fn handle_authorizes_create_per_topic_when_cluster_create_is_denied() {
    struct Case {
        acls: Vec<AclEntry>,
        // Which of "a" and "app-x" this ACL shape lets `alice` create.
        // "b" (duplicate) and "__cluster_metadata" (protected) are refused
        // under every shape and are not repeated here.
        created: &'static [&'static str],
    }

    use crate::handlers::test_support::acl;

    let cluster_create = acl(AclSetup::cluster(AclOperation::Create));
    let literal_a = acl(AclSetup {
        resource_name: "a",
        operation: AclOperation::Create,
        ..Default::default()
    });
    let prefixed_app = acl(AclSetup {
        resource_name: "app-",
        pattern_type: PatternType::Prefixed,
        operation: AclOperation::Create,
        ..Default::default()
    });

    let cases = [
        (
            "cluster Create authorizes every survivor",
            Case {
                acls: vec![cluster_create.clone()],
                created: &["a", "app-x"],
            },
        ),
        (
            "a literal ACL authorizes only its exact name",
            Case {
                acls: vec![literal_a.clone()],
                created: &["a"],
            },
        ),
        (
            "an app- prefixed ACL authorizes only its prefix",
            Case {
                acls: vec![prefixed_app.clone()],
                created: &["app-x"],
            },
        ),
        (
            "no ACL authorizes nothing",
            Case {
                acls: vec![],
                created: &[],
            },
        ),
    ];

    for (label, case) in cases {
        broker_fixture!((broker_handle, _dir, broker), controller_peer_acls);
        crate::handlers::acl_test_support::seed_case_acls!(broker, case.acls);

        request_identity!((p, peer), principal("alice"));
        let req = request(vec![
            topic("a", 1, 1),
            topic("app-x", 1, 1),
            topic("b", 1, 1),
            topic("b", 1, 1),
            topic("__cluster_metadata", 1, 1),
        ]);

        let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

        let id_of = |name: &str| {
            resp.topics
                .iter()
                .find(|row| row.name == name)
                .map(|row| row.topic_id)
                .expect("a row per name")
        };
        let row = |name: &str| -> CreatableTopicResult {
            if case.created.contains(&name) {
                expected_topic(ExpectedTopicSetup {
                    name,
                    topic_id: id_of(name),
                    topic_config_error_code: KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED),
                    ..Default::default()
                })
            } else {
                expected_empty_topic(
                    name,
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    Some("Authorization failed.".into()),
                )
            }
        };
        let duplicate_row = |name: &str| {
            expected_empty_topic(
                name,
                codes::INVALID_REQUEST,
                Some("Duplicate topic name.".into()),
            )
        };
        let protected_row = expected_empty_topic(
            "__cluster_metadata",
            codes::INVALID_REQUEST,
            Some("Creation of internal topic __cluster_metadata is prohibited.".into()),
        );

        // Kafka answers the controller's rows first, then one row per
        // duplicated name ("b" appears twice but gets exactly one row), then
        // the protected topic and the topics denied `Create`.
        let (created, denied): (Vec<&str>, Vec<&str>) = ["a", "app-x"]
            .into_iter()
            .partition(|name| case.created.contains(name));
        let topics = created
            .into_iter()
            .map(row)
            .chain([duplicate_row("b")])
            .chain(denied.into_iter().map(row))
            .chain([protected_row])
            .collect();
        let expected = unthrottled_wire!(CreateTopicsResponse { topics });
        check!(resp == expected, "case: {label}");

        let image = broker_handle.controller_image_for_test();
        check!(
            image.topic("a").is_some() == case.created.contains(&"a"),
            "case: {label}, topic a"
        );
        check!(
            image.topic("app-x").is_some() == case.created.contains(&"app-x"),
            "case: {label}, topic app-x"
        );
        check!(image.topic("b").is_none(), "case: {label}, topic b");
        check!(
            image.topic("__cluster_metadata").is_none(),
            "case: {label}, topic __cluster_metadata"
        );

        broker_handle.shutdown().await;
    }
}

// ── #730, #739, #740, #742: Kafka's check order, rows and messages ──

/// A `CreatableTopic` whose configs may carry a null value.
fn topic_with_nullable_configs(name: &str, configs: &[(&str, Option<&str>)]) -> CreatableTopic {
    CreatableTopic {
        configs: configs
            .iter()
            .map(|(key, value)| CreatableTopicConfig {
                name: (*key).into(),
                value: value.map(str::to_owned),
                ..Default::default()
            })
            .collect(),
        ..topic(name, 1, 1)
    }
}

/// #1235: Kafka's `ControllerApis.handleCreateTopics` checks cluster `Create`
/// and the `DescribeConfigs` disclosure with `logIfDenied = false`, so a
/// principal that holds only a topic-scoped `Create` ACL (the standard Streams
/// and Connect setup) creates its topic without a Deny in the audit trail or
/// in `authorization_denied_total`. A real refusal, `Create` denied on the
/// topic, is still counted.
#[tokio::test]
async fn cluster_create_and_describe_configs_probes_leave_no_denial_behind() {
    use crate::metrics::AuthorizationDeniedLabel;

    let literal_a = allow_acl(AllowAclSetup {
        resource_name: "a",
        operation: AclOperation::Create,
        ..Default::default()
    });
    let denied = |operation: &str, resource_type: &str| AuthorizationDeniedLabel {
        operation: operation.into(),
        resource_type: resource_type.into(),
    };
    // (label, ACLs alice holds, the topic's error code, and the denials
    // counted for cluster Create, topic DescribeConfigs and topic Create)
    let cases = [
        (
            "a topic-scoped Create ACL creates the topic quietly",
            vec![literal_a],
            codes::NONE,
            (0, 0, 0),
        ),
        (
            "a real refusal is counted, and the cluster probe is not",
            vec![],
            codes::TOPIC_AUTHORIZATION_FAILED,
            (0, 0, 1),
        ),
    ];

    for (label, acls, error_code, (cluster_create, describe_configs, topic_create)) in cases {
        broker_fixture!((broker_handle, _dir, broker), controller_peer_acls);
        if !acls.is_empty() {
            broker
                .controller
                .submit_change(
                    acls.into_iter()
                        .map(MetadataRecord::V1AccessControlEntry)
                        .collect(),
                )
                .await
                .expect("seed acls");
        }

        let resp = drive(
            &broker,
            &request(vec![topic("a", 1, 1)]),
            &test_context(&principal("alice"), &peer()),
        )
        .await;

        let count = |key| {
            broker
                .metrics
                .authorization_denied
                .get_or_create(&key)
                .get()
        };
        check!(resp.topics[0].error_code == error_code, "{label}");
        check!(
            (
                count(denied("Create", "Cluster")),
                count(denied("DescribeConfigs", "Topic")),
                count(denied("Create", "Topic")),
            ) == (cluster_create, describe_configs, topic_create),
            "{label}"
        );
        broker_handle.shutdown().await;
    }
}

/// An error row, as Kafka builds it: no topic id, and the KIP-525 fields at
/// the Java defaults (-1 counts, an empty config list).
fn error_row(name: &str, error_code: i16, message: &str) -> CreatableTopicResult {
    CreatableTopicResult {
        name: name.into(),
        error_code,
        error_message: Some(message.into()),
        configs: Some(Vec::new()),
        ..Default::default()
    }
}

/// The request rows of one scenario, whether it is a dry run, the rows the
/// response must carry (`None` for a created topic, whose id and configs are
/// minted per create), and the topics that must exist afterwards.
struct CheckOrderCase {
    label: &'static str,
    topics: Vec<CreatableTopic>,
    validate_only: bool,
    rows: Vec<(&'static str, Option<CreatableTopicResult>)>,
    exist: &'static [&'static str],
}

/// Kafka's `ControllerApis.createTopics` and
/// `ReplicationControlManager.createTopics` on a single broker that already
/// holds topic `t`: duplicate names answer one `INVALID_REQUEST` row after the
/// controller's rows, an existing topic answers `TOPIC_ALREADY_EXISTS` ahead
/// of every other check, a null config answers `INVALID_CONFIG`, and a request
/// for more than 10000 partitions answers `POLICY_VIOLATION` on every row.
#[tokio::test]
async fn rows_follow_kafkas_check_order_and_messages() {
    const EXISTS: &str = "Topic 't' already exists.";
    // Kafka 4.3.1's text. Trunk rewords it, and krabka serves that only with
    // `unstable.api.versions.enable`: see `too_many_partitions_message_follows_the_unstable_flag`.
    const TOO_MANY: &str = "Excessively large number of partitions per request.";
    let duplicate = |name| error_row(name, codes::INVALID_REQUEST, "Duplicate topic name.");
    let exists = || Some(error_row("t", codes::TOPIC_ALREADY_EXISTS, EXISTS));
    let manual_rf_2 = CreatableTopic {
        assignments: vec![
            krabka_protocol::owned::create_topics_request::CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![1],
                ..Default::default()
            },
        ],
        ..topic("m", -1, 2)
    };
    let cases = vec![
        CheckOrderCase {
            label: "a name twice creates nothing",
            topics: vec![topic("n", 1, 1), topic("n", 1, 1)],
            validate_only: false,
            rows: vec![("n", Some(duplicate("n")))],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "a name twice in a dry run",
            topics: vec![topic("n", 1, 1), topic("n", 1, 1)],
            validate_only: true,
            rows: vec![("n", Some(duplicate("n")))],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "a duplicate answers after the controller's rows",
            topics: vec![topic("n", 1, 1), topic("u", 1, 1), topic("n", 1, 1)],
            validate_only: false,
            rows: vec![("u", None), ("n", Some(duplicate("n")))],
            exist: &["t", "u"],
        },
        CheckOrderCase {
            label: "an existing topic with an invalid config",
            topics: vec![topic_with_configs("t", &[("retention.ms", "abc")])],
            validate_only: false,
            rows: vec![("t", exists())],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "an existing topic with no partitions",
            topics: vec![topic("t", 0, 1)],
            validate_only: false,
            rows: vec![("t", exists())],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "an existing topic with an unplaceable replication factor",
            topics: vec![topic("t", 1, 3)],
            validate_only: false,
            rows: vec![("t", exists())],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "an existing topic with an invalid config in a dry run",
            topics: vec![topic_with_configs("t", &[("retention.ms", "abc")])],
            validate_only: true,
            rows: vec![("t", exists())],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "a null config",
            topics: vec![topic_with_nullable_configs("n", &[("retention.ms", None)])],
            validate_only: false,
            rows: vec![(
                "n",
                Some(error_row(
                    "n",
                    codes::INVALID_CONFIG,
                    "Null value not supported for topic configs: retention.ms",
                )),
            )],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "two null configs in a dry run",
            topics: vec![topic_with_nullable_configs(
                "n",
                &[
                    ("retention.ms", None),
                    ("segment.ms", Some("60000")),
                    ("cleanup.policy", None),
                ],
            )],
            validate_only: true,
            rows: vec![(
                "n",
                Some(error_row(
                    "n",
                    codes::INVALID_CONFIG,
                    "Null value not supported for topic configs: retention.ms,cleanup.policy",
                )),
            )],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "a manual assignment with a replication factor",
            topics: vec![manual_rf_2],
            validate_only: false,
            rows: vec![(
                "m",
                Some(error_row(
                    "m",
                    codes::INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication factor was not \
                     set to -1.",
                )),
            )],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "one topic above the partition limit",
            topics: vec![topic("a", 10_001, 1)],
            validate_only: false,
            rows: vec![("a", Some(error_row("a", codes::POLICY_VIOLATION, TOO_MANY)))],
            exist: &["t"],
        },
        CheckOrderCase {
            label: "two topics above the limit together answer every requested row",
            topics: vec![
                topic("a", 6_000, 1),
                topic("d", 1, 1),
                topic("b", 6_000, 1),
                topic("d", 1, 1),
            ],
            validate_only: false,
            rows: vec![
                ("a", Some(error_row("a", codes::POLICY_VIOLATION, TOO_MANY))),
                ("d", Some(error_row("d", codes::POLICY_VIOLATION, TOO_MANY))),
                ("b", Some(error_row("b", codes::POLICY_VIOLATION, TOO_MANY))),
                ("d", Some(error_row("d", codes::POLICY_VIOLATION, TOO_MANY))),
            ],
            exist: &["t"],
        },
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for case in cases {
        creation_fixture!((broker_handle, _dir, broker), (p, peer), allow_all, "admin");
        let seeded = drive(
            &broker,
            &request(vec![topic("t", 1, 1)]),
            &test_context(&p, &peer),
        )
        .await;
        assert!(seeded.topics[0].error_code == codes::NONE);

        let req = CreateTopicsRequest {
            validate_only: case.validate_only,
            ..request(case.topics)
        };
        let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

        // A created topic's row carries a minted id and the KIP-525 fields,
        // so its expectation is built from the row the response carries.
        let topics = case
            .rows
            .into_iter()
            .zip(&resp.topics)
            .map(|((name, row), actual_row)| {
                row.unwrap_or_else(|| CreatableTopicResult {
                    name: name.into(),
                    topic_id: actual_row.topic_id,
                    error_message: None,
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: Some(expected_configs(&[])),
                    ..Default::default()
                })
            })
            .collect();
        let image = broker_handle.controller_image_for_test();
        let mut exist: Vec<String> = image
            .topics()
            .map(|topic| topic.name.clone())
            .filter(|name| !name.starts_with("__"))
            .collect();
        exist.sort();
        actual.push((case.label, resp, exist));
        expected.push((
            case.label,
            unthrottled_wire!(CreateTopicsResponse { topics }),
            case.exist.iter().map(|name| (*name).to_owned()).collect(),
        ));
        broker_handle.shutdown().await;
    }
    assert!(actual == expected);
}

/// Kafka's `createTopics` reaches `ConfigurationControlManager.validateAlterConfig`
/// through `incrementalAlterConfig`, which refuses a config value longer than
/// `Short.MAX_VALUE` with `INVALID_CONFIG` before it checks any key. A throttled
/// replicas list of 8000 entries (31999 characters) is valid, and one of 8200
/// entries (32799 characters) is not.
#[tokio::test]
async fn a_config_value_over_short_max_value_answers_invalid_config() {
    let key = LEADER_THROTTLED_REPLICAS_KEY;
    let fits = vec!["0:1"; 8_000].join(",");
    let too_long = vec!["0:1"; 8_200].join(",");
    broker_fixture!((broker_handle, _dir, broker), allow_all);

    let resp = drive(
        &broker,
        &request(vec![
            topic_with_configs("fits", &[(key, &fits)]),
            topic_with_configs("long", &[(key, &too_long)]),
        ]),
        &test_context(&principal("admin"), &peer()),
    )
    .await;

    check!(resp.topics[0].error_code == codes::NONE);
    check!(
        resp.topics[1]
            == error_row(
                "long",
                codes::INVALID_CONFIG,
                "The configuration value cannot be added because it exceeds the maximum value \
                 size of 32767 bytes.",
            )
    );
    let image = broker_handle.controller_image_for_test();
    check!(image.topic("fits").is_some());
    check!(image.topic("long").is_none());
    broker_handle.shutdown().await;
}

/// The whole-request refusal for more than 10000 partitions answers
/// `POLICY_VIOLATION` on every row in both modes, with 4.3.1's text by default
/// ("Excessively large number of partitions per request.") and trunk's text
/// only with `unstable.api.versions.enable` ("Too many partitions in request.").
#[tokio::test]
async fn too_many_partitions_message_follows_the_unstable_flag() {
    use crate::api_catalog::UnstableApiVersions;

    let cases = [
        (
            UnstableApiVersions::Disabled,
            "Excessively large number of partitions per request.",
        ),
        (
            UnstableApiVersions::Enabled,
            "Too many partitions in request.",
        ),
    ];

    for (unstable, message) in cases {
        let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
            cfg.features.unstable_api_versions = unstable;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(
            &broker,
            &request(vec![topic("a", 10_001, 1), topic("b", 1, 1)]),
            &test_context(&principal("admin"), &peer()),
        )
        .await;

        check!(
            resp.topics
                == vec![
                    error_row("a", codes::POLICY_VIOLATION, message),
                    error_row("b", codes::POLICY_VIOLATION, message),
                ],
            "{unstable:?}"
        );
        broker_handle.shutdown().await;
    }
}

/// Single-topic cases must receive exactly one row before inspecting its fields.
async fn drive_single_topic(
    broker: &Broker,
    request: &CreateTopicsRequest,
    context: &RequestContext<'_>,
) -> CreateTopicsResponse {
    let response = drive(broker, request, context).await;
    assert!(response.topics.len() == 1);
    response
}
