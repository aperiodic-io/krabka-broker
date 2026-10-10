use std::path::PathBuf;

use assert2::{assert, check};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig},
        create_topics_response::CreatableTopicResult,
    },
    primitives::uuid::Uuid as ProtoUuid,
};

use super::*;
use crate::test_support::{PartitionCount, ReplicationFactor};

fn configs(pairs: &[(&str, &str)]) -> Vec<CreatableTopicConfig> {
    pairs
        .iter()
        .map(|(name, value)| CreatableTopicConfig {
            name: (*name).to_owned(),
            value: Some((*value).to_owned()),
            ..Default::default()
        })
        .collect()
}

struct AutoTopicSetup<'a> {
    name: &'a str,
    num_partitions: PartitionCount,
    replication_factor: ReplicationFactor,
    configs: Vec<CreatableTopicConfig>,
}

impl Default for AutoTopicSetup<'_> {
    fn default() -> Self {
        Self {
            name: "orders",
            num_partitions: PartitionCount(-1),
            replication_factor: ReplicationFactor(-1),
            configs: vec![],
        }
    }
}

fn topic(setup: AutoTopicSetup<'_>) -> CreatableTopic {
    let AutoTopicSetup {
        name,
        num_partitions,
        replication_factor,
        configs,
    } = setup;
    CreatableTopic {
        name: name.to_owned(),
        num_partitions: num_partitions.0,
        replication_factor: replication_factor.0,
        configs,
        ..Default::default()
    }
}

/// Kafka's `DefaultAutoTopicCreationManager.creatableTopic` gives each
/// coordinator topic its configured partition count, replication factor and
/// topic configs. The values below differ per topic, so a mix-up of two
/// settings shows. Any other name gets the broker defaults only when the
/// operator supplied them, and -1 else.
#[test]
fn creatable_topic_follows_kafkas_creatable_topic() {
    let mut config = BrokerConfig::for_tests(PathBuf::from("/tmp"));
    config.offsets_topic_num_partitions = 11;
    config.offsets_topic_replication_factor = 3;
    config.transaction_state_num_partitions = 12;
    config.transaction_state_replication_factor = 2;
    config.transaction_state_min_isr = 2;
    config.share_coordinator.state_topic_num_partitions = 13;
    config.share_coordinator.state_topic_replication_factor = 4;
    config.share_coordinator.state_topic_min_isr = 3;
    config.barrier_state_num_partitions = 14;
    config.barrier_state_replication_factor = 5;
    config.num_partitions = 6;
    config.default_replication_factor = 7;
    let mut partitions_supplied = config.clone();
    partitions_supplied
        .static_config_origins
        .topic_creation
        .num_partitions = true;
    let mut both_supplied = partitions_supplied.clone();
    both_supplied
        .static_config_origins
        .topic_creation
        .default_replication_factor = true;

    let cases = [
        (
            "__consumer_offsets",
            &config,
            crate::coordinator::bootstrap::OFFSETS_TOPIC,
            topic(AutoTopicSetup {
                name: crate::coordinator::bootstrap::OFFSETS_TOPIC,
                num_partitions: PartitionCount(11),
                replication_factor: ReplicationFactor(3),
                configs: configs(&[
                    ("cleanup.policy", "compact"),
                    ("compression.type", "producer"),
                    ("segment.bytes", "104857600"),
                ]),
            }),
        ),
        (
            "__transaction_state",
            &config,
            crate::txn::bootstrap::TOPIC,
            topic(AutoTopicSetup {
                name: crate::txn::bootstrap::TOPIC,
                num_partitions: PartitionCount(12),
                replication_factor: ReplicationFactor(2),
                configs: configs(&[
                    ("cleanup.policy", "compact"),
                    ("compression.type", "uncompressed"),
                    ("min.insync.replicas", "2"),
                    ("segment.bytes", "104857600"),
                    ("unclean.leader.election.enable", "false"),
                ]),
            }),
        ),
        (
            "__share_group_state",
            &config,
            crate::share_coordinator::bootstrap::TOPIC,
            topic(AutoTopicSetup {
                name: crate::share_coordinator::bootstrap::TOPIC,
                num_partitions: PartitionCount(13),
                replication_factor: ReplicationFactor(4),
                configs: configs(&[
                    ("cleanup.policy", "delete"),
                    ("compression.type", "producer"),
                    ("min.insync.replicas", "3"),
                    ("retention.ms", "-1"),
                    ("segment.bytes", "104857600"),
                ]),
            }),
        ),
        (
            "__barrier_state",
            &config,
            crate::barrier::STATE_TOPIC,
            topic(AutoTopicSetup {
                name: crate::barrier::STATE_TOPIC,
                num_partitions: PartitionCount(14),
                replication_factor: ReplicationFactor(5),
                configs: configs(&[("cleanup.policy", "compact")]),
            }),
        ),
        (
            "no default supplied",
            &config,
            "orders",
            topic(AutoTopicSetup::default()),
        ),
        (
            "an internal name that is not a coordinator topic",
            &config,
            "__remote_log_metadata",
            topic(AutoTopicSetup {
                name: "__remote_log_metadata",
                ..Default::default()
            }),
        ),
        (
            "num.partitions supplied",
            &partitions_supplied,
            "orders",
            topic(AutoTopicSetup {
                num_partitions: PartitionCount(6),
                ..Default::default()
            }),
        ),
        (
            "both defaults supplied",
            &both_supplied,
            "orders",
            topic(AutoTopicSetup {
                num_partitions: PartitionCount(6),
                replication_factor: ReplicationFactor(7),
                ..Default::default()
            }),
        ),
    ];
    for (case, config, name, expected) in cases {
        check!(creatable_topic(config, name) == expected, "{case}");
    }
}

/// Before the broker binds itself, a request creates nothing, and its caller
/// retries as it does for any other `COORDINATOR_NOT_AVAILABLE`.
#[tokio::test]
async fn an_unbound_request_creates_nothing() {
    let creation = Arc::new(AutoTopicCreation::default());
    creation.request(crate::coordinator::bootstrap::OFFSETS_TOPIC);
    check!(creation.started() == 0);
    check!(!creation.is_in_flight(crate::coordinator::bootstrap::OFFSETS_TOPIC));
}

/// KIP-1191: the dead-letter topic asks for the cluster's default partition
/// count and replication factor, opts itself in, and pins `CreateTime`.
#[test]
fn the_dead_letter_topic_is_created_as_kafka_creates_it() {
    check!(
        dead_letter_topic("dlq.orders")
            == topic(AutoTopicSetup {
                name: "dlq.orders",
                configs: configs(&[
                    ("errors.deadletterqueue.group.enable", "true"),
                    ("message.timestamp.type", "CreateTime"),
                ]),
                ..Default::default()
            })
    );
}

/// An unbound component cannot create the dead-letter topic: the write
/// fails, and the leader archives the record regardless.
#[tokio::test]
async fn an_unbound_component_cannot_create_the_dead_letter_topic() {
    let creation = AutoTopicCreation::default();

    check!(
        creation
            .create_dead_letter_topic("dlq.orders")
            .await
            .is_err()
    );
}

/// Kafka's `ExpiringErrorCache`: an entry counts until its expiry time, a new
/// `put` replaces the entry of the same topic, and a full cache drops the
/// entry that expires first, not the one written first.
#[test]
fn the_error_cache_expires_and_evicts_by_expiry_time() {
    let cache = ExpiringErrorCache::new(2);
    cache.put("a", "first".into(), 1_000, 0);
    cache.put("b", "second".into(), 500, 0);

    check!(cache.has_error("a", 999));
    check!(!cache.has_error("a", 1_000));
    check!(!cache.has_error("unknown", 0));
    check!(
        cache.errors_for_topics(["a", "b", "unknown"], 499)
            == BTreeMap::from([
                ("a".to_owned(), "first".to_owned()),
                ("b".to_owned(), "second".to_owned()),
            ])
    );
    check!(
        cache.errors_for_topics(["a", "b"], 500)
            == BTreeMap::from([("a".to_owned(), "first".to_owned())])
    );

    // A third entry goes over the capacity. `b` expires first, so it goes,
    // though `a` was written before it.
    cache.put("c", "third".into(), 2_000, 100);
    check!(cache.len() == 2);
    check!(
        cache.errors_for_topics(["a", "b", "c"], 100)
            == BTreeMap::from([
                ("a".to_owned(), "first".to_owned()),
                ("c".to_owned(), "third".to_owned()),
            ])
    );

    // A new entry for `a` replaces the old one. The stale heap entry of `a`
    // expires at 1000 and must not remove the new entry.
    cache.put("a", "again".into(), 5_000, 200);
    cache.put("d", "fourth".into(), 10, 1_500);
    check!(
        cache.errors_for_topics(["a", "c", "d"], 1_500)
            == BTreeMap::from([
                ("a".to_owned(), "again".to_owned()),
                ("c".to_owned(), "third".to_owned()),
            ])
    );
}

/// A put drops every entry that has expired, and the cache keeps at most its
/// capacity: `ERROR_CACHE_CAPACITY` by default, as in Kafka.
#[test]
fn the_error_cache_is_bounded() {
    let cache = ExpiringErrorCache::default();
    for index in 0..=ERROR_CACHE_CAPACITY {
        cache.put(&format!("t{index}"), "no brokers".into(), 1_000, 100);
    }
    check!(cache.len() == ERROR_CACHE_CAPACITY);

    cache.put("late", "no brokers".into(), 1_000, 2_000);
    check!(cache.len() == 1);
}

fn result_row(name: &str, error_code: i16, error_message: Option<&str>) -> CreatableTopicResult {
    CreatableTopicResult {
        name: name.to_owned(),
        topic_id: ProtoUuid([0; 16]),
        error_code,
        error_message: error_message.map(str::to_owned),
        num_partitions: -1,
        replication_factor: -1,
        configs: None,
        topic_config_error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// The completion of Kafka's `sendCreateTopicRequestWithErrorCaching`. The
/// in-flight marks clear. A request that got no answer caches its message
/// for every topic. An answer caches each row that is not `NONE`,
/// `TOPIC_ALREADY_EXISTS` included, with the default message of its code
/// when the row carries a null or empty one.
#[test]
fn a_finished_streams_creation_caches_kafkas_errors() {
    let names = ["a", "b", "c", "d"].map(str::to_owned);
    let response = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![
            result_row("a", codes::NONE, None),
            result_row(
                "b",
                codes::TOPIC_ALREADY_EXISTS,
                Some("Topic 'b' already exists."),
            ),
            result_row("c", codes::INVALID_CONFIG, None),
            result_row("d", codes::INVALID_REPLICATION_FACTOR, Some("")),
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let message = |text: &str| Some(text.to_owned());
    let cases = [
        (
            "an answer",
            Ok(response),
            [
                None,
                message("Topic 'b' already exists."),
                message("Configuration is invalid."),
                message(
                    "Replication factor is below 1 or larger than the number of available \
                     brokers.",
                ),
            ],
        ),
        (
            "a timeout",
            Err(TopicCreatorError::Timeout),
            [(); 4].map(|()| message("CreateTopicsRequest to controller timed out")),
        ),
        (
            "a refused envelope",
            Err(TopicCreatorError::Envelope(
                codes::CLUSTER_AUTHORIZATION_FAILED,
            )),
            [(); 4].map(|()| message("Cluster authorization failed.")),
        ),
    ];
    for (case, result, expected) in cases {
        let creation = AutoTopicCreation::default();
        for name in &names {
            assert!(creation.begin(name));
        }
        creation.finish_streams_creation(&names, &result, 1_000, 100);
        for name in &names {
            check!(!creation.is_in_flight(name), "{case}: {name}");
        }
        let expected: BTreeMap<String, String> = names
            .iter()
            .zip(expected)
            .filter_map(|(name, message)| message.map(|message| (name.clone(), message)))
            .collect();
        check!(
            creation
                .streams_internal_topic_creation_errors(names.iter().map(String::as_str), 1_099)
                == expected,
            "{case}"
        );
        check!(
            creation
                .streams_internal_topic_creation_errors(names.iter().map(String::as_str), 1_100)
                == BTreeMap::new(),
            "{case}"
        );
    }
}

/// Kafka's `createStreamsInternalTopics` skips a topic whose last failure has
/// not expired and a topic whose creation is in flight. It sends a request
/// only when some topic is left.
#[tokio::test]
async fn a_streams_creation_skips_a_backed_off_or_in_flight_topic() {
    let (handle, _dir) = crate::test_support::start_broker_no_audit().await;
    let broker = handle.broker_arc_for_test();
    let creation = &broker.auto_topic_creation;
    let identity = ForwardedIdentity {
        principal_name: "alice".to_owned(),
        client_address: std::net::IpAddr::from([127, 0, 0, 1]),
        client_id: "streams-client".to_owned(),
        correlation_id: 1,
    };
    creation.errors.put(
        "backed-off",
        "no brokers".into(),
        60_000,
        crate::time_util::now_ms(),
    );
    assert!(creation.hold_for_test("in-flight"));

    creation.create_streams_internal_topics(
        &broker,
        vec![
            topic(AutoTopicSetup {
                name: "backed-off",
                num_partitions: PartitionCount(1),
                ..Default::default()
            }),
            topic(AutoTopicSetup {
                name: "in-flight",
                num_partitions: PartitionCount(1),
                ..Default::default()
            }),
        ],
        identity.clone(),
        60_000,
    );
    check!(creation.started() == 0);

    creation.create_streams_internal_topics(
        &broker,
        vec![
            topic(AutoTopicSetup {
                name: "backed-off",
                num_partitions: PartitionCount(1),
                ..Default::default()
            }),
            topic(AutoTopicSetup {
                name: "fresh",
                num_partitions: PartitionCount(1),
                ..Default::default()
            }),
        ],
        identity,
        60_000,
    );
    check!(creation.started() == 1);
    handle.wait_until_partition_present("fresh", 0).await;
    check!(
        broker
            .controller
            .current_image()
            .topic("backed-off")
            .is_none()
    );
    handle.shutdown().await;
}
