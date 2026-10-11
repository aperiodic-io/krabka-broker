//! What `describe_one` reports for each resource type: the effective value,
//! the layer it came from, the synonym chain beneath it, and the typed
//! metadata the JVM `AdminClient` reads.

use assert2::{assert, check};
use krabka_metadata::{
    BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
    TopicConfigRecord,
};
use krabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
};
use uuid::Uuid;

use super::{
    super::{
        entry::EntryOptions,
        wire::{
            CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_BROKER,
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_GROUP,
            CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER,
        },
    },
    *,
};
use crate::{
    api_catalog::UnstableApiVersions,
    client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS},
    codes::{INVALID_REQUEST, INVALID_TOPIC_EXCEPTION, UNKNOWN_TOPIC_OR_PARTITION},
    config::BrokerConfig,
    config_keys::{
        KAFKA_TRUNK_TOPIC_KEYS,
        broker_dynamic::TOPIC_DEFAULT_SYNONYMS,
        group::KAFKA_GROUP_KEYS,
        kafka_broker::{KAFKA_BROKER_CONFIGS, lookup},
        registry::ConfigType,
    },
    coordinator::unified::streams::config::StreamsGroupConfig,
    file_config::FileConfig,
    handlers::describe_configs::test_support::{TopicConfigSetup, set_topic_config},
    test_support::string_pairs,
    throttle::{FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY},
};

fn seed_metrics_subscription(image: &mut MetadataImage) {
    image.apply(&MetadataRecord::V1ClientMetricsConfig(
        krabka_metadata::ClientMetricsConfigRecord {
            name: "sub-1".to_owned(),
            configs: maplit::btreemap! {
            crate::client_metrics::config::KEY_METRICS.to_string() => "org.apache.kafka".to_string()},
        },
    ));
}

/// A request that asks for everything, the way `kafka-configs --describe
/// --all` does.
pub(super) const EVERYTHING: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: true,
};

/// A request that asks for values alone.
const VALUES_ONLY: EntryOptions = EntryOptions {
    include_synonyms: false,
    include_documentation: false,
};

/// The node that serves a request no test routes anywhere in particular.
const SERVING_NODE: krabka_metadata::NodeId = krabka_metadata::NodeId(1);

/// A process that named none of the four static broker keys, which is what
/// every case but the ones that tune one runs as.
fn untuned() -> StaticBrokerConfigs<'static> {
    super::static_broker::kafka_default_static_broker()
}

/// The node a request reaches: a broker resource is served by the node it
/// names, because the JVM `AdminClient` routes it there and the broker
/// refuses to answer for anyone else.
fn serving_node_for(resource_type: i8, resource_name: &str) -> krabka_metadata::NodeId {
    if resource_type == RESOURCE_TYPE_BROKER {
        resource_name
            .parse::<u64>()
            .map_or(SERVING_NODE, krabka_metadata::NodeId)
    } else {
        SERVING_NODE
    }
}

/// A named broker's entries without the topic-default broker keys and the
/// other `KafkaConfig` keys it reports at their defaults, which
/// `a_broker_reports_its_topic_default_keys_typed` and
/// `a_named_broker_reports_every_kafka_config_key` cover, so the static-layer
/// tests read only the keys they are about.
fn static_view(result: &DescribeConfigsResult) -> Vec<DescribeConfigsResourceResult> {
    result
        .configs
        .iter()
        .filter(|entry| {
            !TOPIC_DEFAULT_SYNONYMS
                .iter()
                .any(|(broker, _)| *broker == entry.name)
                && (EMITTED_ELSEWHERE.contains(&entry.name.as_str())
                    || lookup(&entry.name).is_none())
        })
        .cloned()
        .collect()
}

/// `image` with the topic a `TOPIC` resource names registered in it.
fn with_topic(image: &MetadataImage, resource_type: i8, name: &str) -> MetadataImage {
    let mut image = image.clone();
    if resource_type == RESOURCE_TYPE_TOPIC && image.topic(name).is_none() {
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: name.into(),
            topic_id: Uuid::from_u128(0x70),
            partitions: 1,
            replication_factor: 1,
        }));
    }
    image
}

/// Describe one resource with the supplied node and static settings,
/// while every logger runs at `info`.
fn describe_at(
    (serving_node, static_broker): (krabka_metadata::NodeId, StaticBrokerConfigs<'_>),
    image: &MetadataImage,
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
    options: EntryOptions,
) -> DescribeConfigsResult {
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info");
    describe_one(
        image,
        &resource_request(resource_type, resource_name, configuration_keys),
        ServingBroker {
            node: serving_node,
            static_broker,
            loggers: BrokerLoggers {
                node_id: 1,
                levels: &levels,
            },
            unstable_api_versions: UnstableApiVersions::Enabled,
        },
        300_000,
        options,
    )
}

/// The request and static settings are input fixtures; expected entry chains stay independent.
fn resource_request(
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
) -> krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
    krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
        resource_type,
        resource_name: resource_name.to_owned(),
        configuration_keys,
        ..Default::default()
    }
}

fn settings_for<'a>(
    (resource_type, resource_name): (i8, &str),
    settings: &'a std::collections::BTreeMap<&'static str, String>,
) -> (krabka_metadata::NodeId, StaticBrokerConfigs<'a>) {
    (
        serving_node_for(resource_type, resource_name),
        StaticBrokerConfigs {
            settings,
            ..untuned()
        },
    )
}

/// Describe one resource the way a client reaches it.
///
/// The one test that probes the wrong-node refusal drives [`describe_at`]
/// instead.
fn describe(
    image: &MetadataImage,
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
    options: EntryOptions,
) -> DescribeConfigsResult {
    // A topic resource describes a topic that exists; the tests that probe a
    // missing or invalid name drive [`describe_at`] directly.
    let image = &with_topic(image, resource_type, resource_name);
    describe_at(
        (serving_node_for(resource_type, resource_name), untuned()),
        image,
        resource_type,
        resource_name,
        configuration_keys,
        options,
    )
}

/// The same, with the node id and live filter a `BROKER_LOGGER` resource is
/// resolved against.
fn describe_with_loggers(
    image: &MetadataImage,
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
    options: EntryOptions,
    loggers: BrokerLoggers<'_>,
) -> DescribeConfigsResult {
    describe_one(
        image,
        &resource_request(resource_type, resource_name, configuration_keys),
        ServingBroker {
            node: serving_node_for(resource_type, resource_name),
            static_broker: untuned(),
            loggers,
            unstable_api_versions: UnstableApiVersions::Enabled,
        },
        300_000,
        options,
    )
}

/// A `BROKER_LOGGER` describe against a node whose id is 7.
#[test]
fn broker_logger_resource_names_this_node_or_is_refused() {
    let image = MetadataImage::new(Uuid::nil());
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info,krabka_broker=debug");

    let result = describe_with_loggers(
        &image,
        RESOURCE_TYPE_BROKER_LOGGER,
        "7",
        None,
        VALUES_ONLY,
        BrokerLoggers {
            node_id: 7,
            levels: &levels,
        },
    );
    check!(result.error_code == crate::codes::NONE);
    check!(
        result
            .configs
            .iter()
            .any(|c| c.name == "krabka_broker" && c.value.as_deref() == Some("DEBUG"))
    );

    let refused = describe_with_loggers(
        &image,
        RESOURCE_TYPE_BROKER_LOGGER,
        "8",
        None,
        VALUES_ONLY,
        BrokerLoggers {
            node_id: 7,
            levels: &levels,
        },
    );
    check!(refused.error_code == crate::codes::INVALID_REQUEST);
    check!(
        refused.error_message.as_deref() == Some("Unexpected broker id, expected 7 but received 8")
    );
    check!(refused.configs.is_empty());
}

/// Describe one topic the way `kafka-configs --describe --all` does.
pub(super) fn describe_topic(
    image: &MetadataImage,
    topic: &str,
    configuration_keys: Option<Vec<String>>,
) -> DescribeConfigsResult {
    describe(
        image,
        RESOURCE_TYPE_TOPIC,
        topic,
        configuration_keys,
        EVERYTHING,
    )
}

/// The one entry a result holds for `name`.
pub(super) fn entry_named<'a>(
    result: &'a DescribeConfigsResult,
    name: &str,
) -> &'a DescribeConfigsResourceResult {
    result
        .configs
        .iter()
        .find(|entry| entry.name == name)
        .unwrap_or_else(|| panic!("no `{name}` entry in {:?}", result.configs))
}

type ExpectedSynonymChain = (Option<String>, i8, Vec<DescribeConfigsSynonym>);

fn expected_default_chain(key: &str, default: &str) -> ExpectedSynonymChain {
    (
        Some(default.to_owned()),
        CONFIG_SOURCE_DEFAULT,
        vec![synonym(key, default, CONFIG_SOURCE_DEFAULT)],
    )
}

fn expected_named_static_chain(key: &str, value: &str, default: &str) -> ExpectedSynonymChain {
    (
        Some(value.to_owned()),
        CONFIG_SOURCE_STATIC_BROKER,
        vec![
            synonym(key, value, CONFIG_SOURCE_STATIC_BROKER),
            synonym(key, default, CONFIG_SOURCE_DEFAULT),
        ],
    )
}

pub(super) use crate::handlers::describe_configs::test_support::synonym;

fn image_with_broker_config(
    node_id: krabka_metadata::NodeId,
    pairs: &[(&str, &str)],
) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for (name, value) in pairs {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: (*name).to_owned(),
            config_value: Some((*value).to_owned()),
        }));
    }
    image
}

/// The case the whole change exists for: a topic override sitting above a
/// cluster default, described in full.
///
/// The two entries are compared whole, because every field of them is the
/// answer: the effective value, the layer it came from, the chain beneath it
/// in Kafka's precedence order, the `ConfigDef` type the JVM `AdminClient`
/// parses the value with, and the documentation `include_documentation` asked
/// for. Verified against `apache/kafka:4.3.1`, which answers the same shape
/// for `retention.ms` over `log.retention.ms`.
#[test]
fn a_topic_reports_its_override_above_the_cluster_default_with_the_whole_chain() {
    let mut image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
    );
    set_topic_config(&mut image, TopicConfigSetup::default());

    let result = describe_topic(
        &image,
        "orders",
        Some(vec![
            config_keys::RETENTION_MS.to_owned(),
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
            config_keys::CLEANUP_POLICY.to_owned(),
        ]),
    );

    let retention =
        registry::lookup(ConfigScope::Topic, config_keys::RETENTION_MS).expect("retention.ms");
    let unclean = registry::lookup(
        ConfigScope::Topic,
        config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
    )
    .expect("unclean.leader.election.enable");
    let policy =
        registry::lookup(ConfigScope::Topic, config_keys::CLEANUP_POLICY).expect("cleanup.policy");

    assert!(
        result
            == tagged_wire!(DescribeConfigsResult {
                error_code: crate::codes::NONE,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".to_owned(),
                configs: vec![
                    // Set nowhere, so the key reports its default beneath
                    // the broker synonym Kafka names, `log.cleanup.policy`.
                    expected_config_entry(ExpectedConfigSetup {
                        name: config_keys::CLEANUP_POLICY,
                        value: Some("delete"),
                        synonyms: vec![synonym(
                            "log.cleanup.policy",
                            "delete",
                            CONFIG_SOURCE_DEFAULT
                        )],
                        config_type: ConfigTypeCode(ConfigType::List.wire()),
                        documentation: Some(policy.doc.to_owned()),
                        ..Default::default()
                    }),
                    expected_config_entry(ExpectedConfigSetup {
                        name: config_keys::RETENTION_MS,
                        value: Some("60000"),
                        source: ConfigSourceCode(CONFIG_SOURCE_DYNAMIC_TOPIC),
                        synonyms: vec![
                            synonym(
                                config_keys::RETENTION_MS,
                                "60000",
                                CONFIG_SOURCE_DYNAMIC_TOPIC
                            ),
                            synonym("log.retention.hours", "168", CONFIG_SOURCE_DEFAULT),
                        ],
                        documentation: Some(retention.doc.to_owned()),
                        ..Default::default()
                    }),
                    // Not set on the topic, so the cluster default wins and
                    // the built-in default sits below it.
                    expected_config_entry(ExpectedConfigSetup {
                        name: config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                        value: Some("true"),
                        source: ConfigSourceCode(CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER),
                        synonyms: vec![
                            synonym(
                                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                                "true",
                                CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                            ),
                            synonym(
                                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                                "false",
                                CONFIG_SOURCE_DEFAULT
                            ),
                        ],
                        config_type: ConfigTypeCode(ConfigType::Boolean.wire()),
                        documentation: Some(unclean.doc.to_owned()),
                        ..Default::default()
                    }),
                ],
            })
    );
}

#[test]
fn a_topic_override_stays_at_the_head_of_the_chain_above_the_cluster_default() {
    // The other half of the precedence rule: when the topic *does* set the
    // key, the cluster default drops to a synonym and the source says
    // DYNAMIC_TOPIC_CONFIG. An operator reads the override and the value it
    // displaced in one response.
    let mut image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
    );
    set_topic_config(
        &mut image,
        TopicConfigSetup {
            key: config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
            value: "false",
            ..Default::default()
        },
    );

    let result = describe_topic(
        &image,
        "orders",
        Some(vec![config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned()]),
    );
    let entry = entry_named(&result, config_keys::UNCLEAN_LEADER_ELECTION_ENABLE);

    check!(entry.value == Some("false".to_owned()));
    check!(entry.config_source == CONFIG_SOURCE_DYNAMIC_TOPIC);
    check!(
        entry.synonyms
            == vec![
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "false",
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                ),
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "true",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                ),
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "false",
                    CONFIG_SOURCE_DEFAULT
                ),
            ]
    );
}

#[test]
fn a_topic_with_no_overrides_reports_every_key_at_its_default() {
    // `kafka-configs --describe --all` shows effective configuration, so a
    // topic that overrides nothing still answers with every key it has.
    let image = MetadataImage::new(Uuid::nil());
    let result = describe_topic(&image, "orders", None);

    let reported: Vec<&str> = result.configs.iter().map(|e| e.name.as_str()).collect();
    let mut expected: Vec<&str> = registry::keys_in(ConfigScope::Topic)
        .filter(|row| !row.internal)
        .map(|row| row.name)
        .collect();
    expected.sort_unstable();

    check!(reported == expected);
    check!(
        result
            .configs
            .iter()
            .all(|entry| entry.config_source == CONFIG_SOURCE_DEFAULT)
    );
    check!(result.configs.iter().all(|entry| entry.config_type != 0));
    check!(result.configs.iter().all(|entry| !entry.is_sensitive));
    check!(
        result
            .configs
            .iter()
            .all(|entry| entry.documentation.is_some())
    );
}

/// Kafka's `KafkaConfigSchema.resolveEffectiveTopicConfigs` skips a key
/// defined with `defineInternal` unless the topic sets it, so
/// `internal.segment.bytes` is on neither a describe nor a KIP-525 config
/// list of a topic that leaves it alone.
#[test]
fn an_internal_topic_key_is_reported_only_when_the_topic_sets_it() {
    let untouched = describe_topic(&MetadataImage::new(Uuid::nil()), "orders", None);
    check!(
        !untouched
            .configs
            .iter()
            .any(|entry| entry.name == config_keys::INTERNAL_SEGMENT_BYTES)
    );

    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: maplit::btreemap! {
            config_keys::INTERNAL_SEGMENT_BYTES.to_string() => "4096".to_string(),
        },
    }));
    let set = describe_topic(&image, "orders", None);
    let entry = set
        .configs
        .iter()
        .find(|entry| entry.name == config_keys::INTERNAL_SEGMENT_BYTES)
        .expect("a topic that sets the internal key reports it");
    check!(entry.value.as_deref() == Some("4096"));
    check!(entry.config_source == CONFIG_SOURCE_DYNAMIC_TOPIC);

    // The KIP-525 list is the same computation.
    let created = effective_topic_configs(
        &MetadataImage::new(Uuid::nil()),
        SERVING_NODE,
        "orders",
        &std::collections::BTreeMap::new(),
        UnstableApiVersions::Enabled,
        &std::collections::BTreeMap::new(),
    );
    check!(
        !created
            .iter()
            .any(|entry| entry.name == config_keys::INTERNAL_SEGMENT_BYTES)
    );
    let created = effective_topic_configs(
        &MetadataImage::new(Uuid::nil()),
        SERVING_NODE,
        "orders",
        &maplit::btreemap! {
            config_keys::INTERNAL_SEGMENT_BYTES.to_string() => "4096".to_string(),
        },
        UnstableApiVersions::Enabled,
        &std::collections::BTreeMap::new(),
    );
    check!(
        created
            .iter()
            .any(|entry| entry.name == config_keys::INTERNAL_SEGMENT_BYTES)
    );
}

#[test]
fn the_fixed_data_path_key_is_read_only_and_typed() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "events".into(),
        overrides: maplit::btreemap! {
        config_keys::DISKLESS.to_string() => "true".to_string()},
    }));

    let result = describe_topic(
        &image,
        "events",
        Some(vec![config_keys::DISKLESS.to_owned()]),
    );

    assert!(
        result.configs
            == vec![expected_config_entry(ExpectedConfigSetup {
                name: config_keys::DISKLESS,
                value: Some("true"),
                mutability: ConfigMutability::ReadOnly,
                source: ConfigSourceCode(CONFIG_SOURCE_DYNAMIC_TOPIC),
                synonyms: vec![synonym(
                    config_keys::DISKLESS,
                    "true",
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                )],
                config_type: ConfigTypeCode(ConfigType::Boolean.wire()),
                documentation: Some(
                    registry::lookup(ConfigScope::Topic, config_keys::DISKLESS)
                        .expect("krabka.diskless")
                        .doc
                        .to_owned()
                )
            })]
    );
}

#[test]
fn a_broker_reports_its_per_node_override_above_the_cluster_default() {
    let mut image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: LEADER_THROTTLED_RATE_KEY.to_owned(),
        config_value: Some("512".to_owned()),
    }));

    let result = describe(
        &image,
        RESOURCE_TYPE_BROKER,
        "2",
        Some(vec![
            crate::throttle::LEADER_THROTTLED_RATE_KEY.to_owned(),
            NODE_ID.to_owned(),
        ]),
        EVERYTHING,
    );

    assert!(
        result.configs
            == vec![
                expected_config_entry(ExpectedConfigSetup {
                    name: crate::throttle::LEADER_THROTTLED_RATE_KEY,
                    value: Some("1024"),
                    source: ConfigSourceCode(CONFIG_SOURCE_DYNAMIC_BROKER),
                    synonyms: vec![
                        synonym(
                            crate::throttle::LEADER_THROTTLED_RATE_KEY,
                            "1024",
                            CONFIG_SOURCE_DYNAMIC_BROKER
                        ),
                        synonym(
                            crate::throttle::LEADER_THROTTLED_RATE_KEY,
                            "512",
                            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                        ),
                    ],
                    documentation: Some(
                        registry::lookup(
                            ConfigScope::Broker,
                            crate::throttle::LEADER_THROTTLED_RATE_KEY
                        )
                        .expect("leader.replication.throttled.rate")
                        .doc
                        .to_owned()
                    ),
                    ..Default::default()
                }),
                expected_config_entry(ExpectedConfigSetup {
                    name: NODE_ID,
                    value: Some("2"),
                    mutability: ConfigMutability::ReadOnly,
                    source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
                    synonyms: vec![synonym(NODE_ID, "2", CONFIG_SOURCE_STATIC_BROKER)],
                    config_type: ConfigTypeCode(ConfigType::Int.wire()),
                    documentation: Some(
                        registry::lookup(ConfigScope::Broker, NODE_ID)
                            .expect("node.id")
                            .doc
                            .to_owned()
                    )
                }),
            ]
    );
}

#[test]
fn the_cluster_default_resource_reports_the_defaults_and_no_node_id() {
    // An empty resource name is Kafka's cluster-wide default broker
    // resource. It holds no per-node layer, and no node runs it, so the
    // static `node.id` entry a numeric name carries has no place in it.
    let image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );

    let result = describe(&image, RESOURCE_TYPE_BROKER, "", None, EVERYTHING);

    assert!(
        result.configs
            == vec![expected_config_entry(ExpectedConfigSetup {
                name: crate::throttle::LEADER_THROTTLED_RATE_KEY,
                value: Some("1024"),
                source: ConfigSourceCode(CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER),
                synonyms: vec![synonym(
                    crate::throttle::LEADER_THROTTLED_RATE_KEY,
                    "1024",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                )],
                documentation: Some(
                    registry::lookup(
                        ConfigScope::Broker,
                        crate::throttle::LEADER_THROTTLED_RATE_KEY
                    )
                    .expect("leader.replication.throttled.rate")
                    .doc
                    .to_owned()
                ),
                ..Default::default()
            })]
    );
}

#[test]
fn a_broker_that_overrides_nothing_still_reports_its_static_node_id() {
    // `node.id` never reaches the metadata image, so a node with no dynamic
    // override at all is the case where the static layer is the whole
    // response. `apache/kafka:4.3.1` answers `node.id` the same way: value
    // from the static configuration, `STATIC_BROKER_CONFIG`, read-only.
    let result = describe(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "7",
        Some(vec![NODE_ID.to_owned()]),
        VALUES_ONLY,
    );

    assert!(
        result.configs
            == vec![expected_config_entry(ExpectedConfigSetup {
                name: NODE_ID,
                value: Some("7"),
                mutability: ConfigMutability::ReadOnly,
                source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
                config_type: ConfigTypeCode(ConfigType::Int.wire()),
                ..Default::default()
            })]
    );
}

#[test]
fn a_broker_that_overrides_nothing_still_reports_its_static_configuration() {
    // None of these keys reaches the metadata image, so a node with no
    // dynamic override at all is the case where the static layer is the
    // whole response. `apache/kafka:4.3.1` answers the same way: values from
    // the node's own configuration, read-only, and the KIP-98 expiry pair and
    // the KIP-211 retention pair at `DEFAULT_CONFIG` because this node never
    // moved them off Kafka's built-in defaults. The KIP-464 topic-creation
    // pair reports the same way, as do `auto.create.topics.enable`, which sorts
    // to the head of the response, and the idle window after it.
    let result = describe(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "7",
        None,
        VALUES_ONLY,
    );

    assert!(
        static_view(&result)
            == expected_plain_configs(&[
                (
                    config_keys::AUTO_CREATE_TOPICS_ENABLE,
                    "true",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Boolean
                ),
                (
                    config_keys::CONNECTIONS_MAX_IDLE_MS,
                    "600000",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Long
                ),
                (
                    config_keys::DEFAULT_REPLICATION_FACTOR,
                    "1",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Int
                ),
                (
                    config_keys::DELETE_TOPIC_ENABLE,
                    "true",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Boolean
                ),
                (NODE_ID, "7", CONFIG_SOURCE_STATIC_BROKER, ConfigType::Int),
                (
                    config_keys::NUM_PARTITIONS,
                    "1",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Int
                ),
                (
                    config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                    "600000",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Long
                ),
                (
                    config_keys::OFFSETS_RETENTION_MINUTES,
                    "10080",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Int
                ),
                (
                    config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                    "3600000",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Int
                ),
                (
                    config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
                    "604800000",
                    CONFIG_SOURCE_DEFAULT,
                    ConfigType::Int
                )
            ])
    );
}

/// The cluster-default resource carries dynamic defaults alone, in Kafka and
/// here, so the static expiry keys belong to a named node and to no other
/// resource.
#[test]
fn the_static_expiry_keys_belong_to_a_named_broker_alone() {
    let image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );

    for (label, resource_name, expected) in [
        (
            "a named node reports both static expiry keys",
            "1",
            vec![
                config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            ],
        ),
        (
            "the cluster-default resource reports neither",
            "",
            Vec::new(),
        ),
    ] {
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            resource_name,
            Some(vec![
                config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS.to_owned(),
            ]),
            VALUES_ONLY,
        );
        let names: Vec<&str> = result.configs.iter().map(|e| e.name.as_str()).collect();

        check!(names == expected, "{label}");
    }
}

#[test]
fn the_key_filter_decides_what_a_broker_resource_reports() {
    let image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[
            (LEADER_THROTTLED_RATE_KEY, "1024"),
            (FOLLOWER_THROTTLED_RATE_KEY, "512"),
        ],
    );

    for (label, filter, expected) in [
        (
            "no filter reports every stored key beside the static ones",
            None,
            vec![
                config_keys::AUTO_CREATE_TOPICS_ENABLE,
                config_keys::CONNECTIONS_MAX_IDLE_MS,
                config_keys::DEFAULT_REPLICATION_FACTOR,
                config_keys::DELETE_TOPIC_ENABLE,
                crate::throttle::FOLLOWER_THROTTLED_RATE_KEY,
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                NODE_ID,
                config_keys::NUM_PARTITIONS,
                config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                config_keys::OFFSETS_RETENTION_MINUTES,
                config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            ],
        ),
        (
            "a filter narrows the response to the keys it names",
            Some(vec![crate::throttle::LEADER_THROTTLED_RATE_KEY]),
            vec![crate::throttle::LEADER_THROTTLED_RATE_KEY],
        ),
        (
            "a filter that names no key this broker holds reports nothing",
            Some(vec!["no.such.key"]),
            Vec::new(),
        ),
    ] {
        let configuration_keys =
            filter.map(|keys| keys.iter().map(|key| (*key).to_owned()).collect());
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "2",
            configuration_keys,
            VALUES_ONLY,
        );
        let view = static_view(&result);
        let names: Vec<&str> = view.iter().map(|e| e.name.as_str()).collect();

        check!(names == expected, "{label}");
    }
}

#[test]
fn an_empty_key_filter_asks_for_everything_the_way_a_null_filter_does() {
    // Kafka filters with `keys == null || keys.isEmpty() || keys.contains(name)`
    // in `ConfigHelperUtils.toDescribeConfigsResult`, so an empty list asks for
    // every key rather than for none. One closure carries the filter into every
    // resource type, so every type it reaches is checked here.
    let mut image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );
    set_topic_config(&mut image, TopicConfigSetup::default());
    seed_metrics_subscription(&mut image);
    image.apply(&MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-1".to_owned(),
            configs: maplit::btreemap! {
            crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS.to_string()
                => "2".to_string()},
        },
    ));

    for (resource_type, resource_name) in [
        (RESOURCE_TYPE_TOPIC, "orders"),
        (RESOURCE_TYPE_BROKER, "2"),
        (RESOURCE_TYPE_CLIENT_METRICS, "sub-1"),
        (RESOURCE_TYPE_GROUP, "streams-1"),
    ] {
        let unfiltered = describe(&image, resource_type, resource_name, None, EVERYTHING);
        let empty_filter = describe(
            &image,
            resource_type,
            resource_name,
            Some(Vec::new()),
            EVERYTHING,
        );

        check!(
            !unfiltered.configs.is_empty(),
            "resource type {resource_type}"
        );
        check!(empty_filter == unfiltered, "resource type {resource_type}");
    }
}

#[test]
fn every_key_an_alter_can_store_on_a_broker_comes_back_with_its_value() {
    // A stored key with no row at all is a name Kafka does not define, which
    // the entry builder withholds, as Kafka does. A key krabka runs with
    // therefore has to have a row, its own or the `KafkaConfig` roster's (see
    // `a_stored_kafka_config_key_comes_back_typed_and_disclosed`).
    for row in registry::keys_in(ConfigScope::Broker) {
        if row.read_only {
            continue;
        }
        let image = image_with_broker_config(krabka_metadata::NodeId(1), &[(row.name, "1")]);
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![row.name.to_owned()]),
            VALUES_ONLY,
        );
        let entry = entry_named(&result, row.name);

        check!(entry.value == Some("1".to_owned()), "{}", row.name);
        check!(!entry.is_sensitive, "{}", row.name);
        check!(entry.config_type == row.config_type.wire(), "{}", row.name);
    }
}

#[test]
fn a_controller_managed_broker_key_is_read_only_wherever_it_is_reported() {
    // The registry and `is_controller_managed_broker_config` have to agree:
    // the alter paths refuse the key by the second, and `kafka-configs` must
    // say so by the first.
    for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
        let image = image_with_broker_config(krabka_metadata::NodeId(1), &[(key, "true")]);
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![key.to_owned()]),
            VALUES_ONLY,
        );
        check!(entry_named(&result, key).read_only, "{key}");
        check!(
            config_keys::is_controller_managed_broker_config(key),
            "{key}"
        );
    }
}

/// Kafka answers a broker resource that names another node with
/// `InvalidRequestException`, not with that node's configuration:
/// `ConfigHelper` in the pinned image's `kafka_2.13-4.3.1.jar` carries the
/// message "Unexpected broker id, expected <id> or empty string, but received
/// <name>". It has to, because everything a named broker resource reports
/// beyond the dynamic overrides -- `node.id` and the static expiry settings --
/// is read out of the serving process. Answering would label one broker's
/// static configuration as another broker's.
#[test]
fn a_broker_resource_that_names_another_node_is_refused() {
    let image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );

    let result = describe_at(
        (krabka_metadata::NodeId(1), untuned()),
        &image,
        RESOURCE_TYPE_BROKER,
        "2",
        None,
        EVERYTHING,
    );

    assert!(
        result
            == tagged_wire!(DescribeConfigsResult {
                error_code: crate::codes::INVALID_REQUEST,
                error_message: Some(
                    "Unexpected broker id, expected 1 or empty string, but received 2".to_owned()
                ),
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "2".to_owned(),
                configs: Vec::new(),
            })
    );
}

/// The cluster-default resource has no node in it, so the serving node never
/// refuses it.
#[test]
fn the_cluster_default_broker_resource_is_served_by_any_node() {
    let image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(LEADER_THROTTLED_RATE_KEY, "1024")],
    );

    let result = describe_at(
        (krabka_metadata::NodeId(9), untuned()),
        &image,
        RESOURCE_TYPE_BROKER,
        "",
        None,
        VALUES_ONLY,
    );

    check!(result.error_code == crate::codes::NONE);
    let names: Vec<&str> = result.configs.iter().map(|e| e.name.as_str()).collect();
    check!(names == vec![crate::throttle::LEADER_THROTTLED_RATE_KEY]);
}

#[test]
fn a_resource_name_kafka_refuses_is_refused_with_kafkas_error() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: "orders".into(),
        topic_id: Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 1,
    }));
    let cases = [
        (
            RESOURCE_TYPE_TOPIC,
            "missing",
            UNKNOWN_TOPIC_OR_PARTITION,
            None,
        ),
        (
            RESOURCE_TYPE_TOPIC,
            "",
            INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: the empty string is not allowed"),
        ),
        (
            RESOURCE_TYPE_TOPIC,
            "bad/name",
            INVALID_TOPIC_EXCEPTION,
            Some(
                "Topic name is invalid: 'bad/name' contains one or more characters other than \
                 ASCII alphanumerics, '.', '_' and '-'",
            ),
        ),
        (
            RESOURCE_TYPE_CLIENT_METRICS,
            "",
            INVALID_REQUEST,
            Some("Client metrics subscription name must not be empty"),
        ),
        (
            RESOURCE_TYPE_GROUP,
            "",
            INVALID_REQUEST,
            Some("Group name must not be empty"),
        ),
        (
            RESOURCE_TYPE_BROKER,
            "abc",
            INVALID_REQUEST,
            Some("Broker id must be an integer, but it is: abc"),
        ),
        (
            RESOURCE_TYPE_BROKER,
            "-1",
            INVALID_REQUEST,
            Some("Unexpected broker id, expected 1 or empty string, but received -1"),
        ),
    ];
    for (resource_type, name, error_code, message) in cases {
        let result = describe_at(
            (krabka_metadata::NodeId(1), untuned()),
            &image,
            resource_type,
            name,
            None,
            EVERYTHING,
        );
        check!(
            result
                == tagged_wire!(DescribeConfigsResult {
                    error_code,
                    error_message: message.map(str::to_owned),
                    resource_type,
                    resource_name: name.to_owned(),
                    configs: Vec::new(),
                }),
            "{resource_type} {name:?}"
        );
    }
    let found = describe_at(
        (krabka_metadata::NodeId(1), untuned()),
        &image,
        RESOURCE_TYPE_TOPIC,
        "orders",
        None,
        EVERYTHING,
    );
    check!(found.error_code == crate::codes::NONE);
    check!(!found.configs.is_empty());
}

#[test]
fn a_client_metrics_subscription_reports_all_three_keys_typed() {
    let mut image = MetadataImage::new(Uuid::nil());
    seed_metrics_subscription(&mut image);

    let result = describe(
        &image,
        RESOURCE_TYPE_CLIENT_METRICS,
        "sub-1",
        None,
        EVERYTHING,
    );
    let metrics = entry_named(&result, KEY_METRICS);
    let interval = entry_named(&result, KEY_INTERVAL_MS);
    let matcher = entry_named(&result, KEY_MATCH);

    check!(metrics.value == Some("org.apache.kafka".to_owned()));
    check!(metrics.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    check!(metrics.config_type == ConfigType::List.wire());
    // Unset keys report the broker's effective default, not a blank.
    check!(interval.value == Some("300000".to_owned()));
    check!(interval.config_source == CONFIG_SOURCE_DEFAULT);
    check!(interval.config_type == ConfigType::Int.wire());
    check!(matcher.value == Some(String::new()));
    check!(matcher.config_type == ConfigType::List.wire());
}

#[test]
fn a_group_reports_its_override_above_the_streams_default() {
    use crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS;

    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-1".to_owned(),
            configs: maplit::btreemap! {
            KEY_NUM_STANDBY_REPLICAS.to_string() => "2".to_string()},
        },
    ));

    let defaults = StreamsGroupConfig::default().group_config_values();
    let fallback = defaults
        .iter()
        .find(|(key, _)| *key == KEY_NUM_STANDBY_REPLICAS)
        .map(|(_, value)| value.clone())
        .expect("streams.num.standby.replicas has a default");

    let result = describe(&image, RESOURCE_TYPE_GROUP, "streams-1", None, EVERYTHING);
    let entry = entry_named(&result, KEY_NUM_STANDBY_REPLICAS);

    check!(entry.value == Some("2".to_owned()));
    check!(entry.config_source == CONFIG_SOURCE_DYNAMIC_GROUP);
    check!(entry.config_type == ConfigType::Int.wire());
    check!(
        entry.synonyms
            == vec![
                synonym(KEY_NUM_STANDBY_REPLICAS, "2", CONFIG_SOURCE_DYNAMIC_GROUP),
                synonym(
                    "group.streams.num.standby.replicas",
                    &fallback,
                    CONFIG_SOURCE_DEFAULT
                ),
            ]
    );
}

#[test]
fn every_key_a_group_or_a_subscription_answers_with_is_typed_and_disclosed() {
    // The same hazard as the broker resource, over the two key sets the
    // broker itself supplies: Kafka's `GroupConfig` decides which group keys a
    // response holds, and each one must come back typed and with a value.
    let image = MetadataImage::new(Uuid::nil());
    // A running broker states the streams assignor it runs, which Kafka's
    // `GroupConfig` has no default for.
    let mut config = BrokerConfig::default();
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    let settings = static_settings(&config);
    let group = describe_at(
        settings_for((RESOURCE_TYPE_GROUP, "streams-1"), &settings),
        &image,
        RESOURCE_TYPE_GROUP,
        "streams-1",
        None,
        EVERYTHING,
    );
    let subscription = describe(
        &image,
        RESOURCE_TYPE_CLIENT_METRICS,
        "sub-1",
        None,
        EVERYTHING,
    );

    let reported: Vec<&str> = group.configs.iter().map(|e| e.name.as_str()).collect();
    let expected: Vec<&str> = KAFKA_GROUP_KEYS.iter().map(|key| key.name).collect();

    check!(reported == expected);
    for entry in group.configs.iter().chain(&subscription.configs) {
        check!(entry.config_type != 0i8, "{} is untyped", entry.name);
        check!(!entry.is_sensitive, "{} is withheld", entry.name);
        check!(entry.value.is_some(), "{} has no value", entry.name);
        check!(entry.documentation.is_some(), "{} has no doc", entry.name);
    }
}

/// #784: with `unstable.api.versions.enable` off a group resource lists
/// exactly Kafka 4.3.1's 17 `GroupConfig` keys, none of trunk's.
#[test]
fn a_group_lists_kafka_4_3_1s_keys_unless_unstable_api_versions_are_enabled() {
    let image = MetadataImage::new(Uuid::nil());
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info");
    let group = describe_one(
        &image,
        &resource_request(RESOURCE_TYPE_GROUP, "streams-1", None),
        ServingBroker {
            node: krabka_metadata::NodeId(1),
            static_broker: untuned(),
            loggers: BrokerLoggers {
                node_id: 1,
                levels: &levels,
            },
            unstable_api_versions: UnstableApiVersions::Disabled,
        },
        300_000,
        EVERYTHING,
    );
    let reported: Vec<&str> = group.configs.iter().map(|e| e.name.as_str()).collect();
    check!(
        reported
            == vec![
                "consumer.assignment.interval.ms",
                "consumer.heartbeat.interval.ms",
                "consumer.session.timeout.ms",
                "share.assignment.interval.ms",
                "share.auto.offset.reset",
                "share.delivery.count.limit",
                "share.heartbeat.interval.ms",
                "share.isolation.level",
                "share.partition.max.record.locks",
                "share.record.lock.duration.ms",
                "share.renew.acknowledge.enable",
                "share.session.timeout.ms",
                "streams.assignment.interval.ms",
                "streams.heartbeat.interval.ms",
                "streams.initial.rebalance.delay.ms",
                "streams.num.standby.replicas",
                "streams.session.timeout.ms",
            ]
    );
}

/// #907's companion: with `unstable.api.versions.enable` off a topic
/// resource carries none of Kafka trunk's four newest topic keys, and a named
/// broker none of their broker synonyms, as Kafka 4.3.1's `LogConfig` and
/// `KafkaConfig` define neither. With it on, both carry every one.
#[test]
fn trunk_topic_keys_are_described_only_under_unstable_api_versions() {
    use crate::api_catalog::UnstableApiVersions;

    let image = with_topic(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_TOPIC,
        "orders",
    );
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info");
    let names = |resource_type: i8, resource_name: &str, unstable| -> Vec<String> {
        describe_one(
            &image,
            &resource_request(resource_type, resource_name, None),
            ServingBroker {
                node: krabka_metadata::NodeId(1),
                static_broker: untuned(),
                loggers: BrokerLoggers {
                    node_id: 1,
                    levels: &levels,
                },
                unstable_api_versions: unstable,
            },
            300_000,
            EVERYTHING,
        )
        .configs
        .into_iter()
        .map(|entry| entry.name)
        .collect()
    };
    let trunk_topic_keys = KAFKA_TRUNK_TOPIC_KEYS;
    let trunk_broker_keys = [
        "log.remote.copy.lag.bytes",
        "log.remote.copy.lag.ms",
        "max.decompressed.message.bytes",
    ];
    for (unstable, served) in [
        (UnstableApiVersions::Disabled, false),
        (UnstableApiVersions::Enabled, true),
    ] {
        let topic = names(RESOURCE_TYPE_TOPIC, "orders", unstable);
        for key in trunk_topic_keys {
            check!(
                topic.iter().any(|name| name == key) == served,
                "{key} {unstable:?}"
            );
        }
        let broker = names(RESOURCE_TYPE_BROKER, "1", unstable);
        for key in trunk_broker_keys {
            check!(
                broker.iter().any(|name| name == key) == served,
                "{key} {unstable:?}"
            );
        }
    }
    let strict = names(RESOURCE_TYPE_TOPIC, "orders", UnstableApiVersions::Disabled);
    let trunk = names(RESOURCE_TYPE_TOPIC, "orders", UnstableApiVersions::Enabled);
    check!(trunk.len() == strict.len() + trunk_topic_keys.len());
}

#[test]
fn an_unhandled_resource_type_is_refused() {
    let image = MetadataImage::new(Uuid::nil());
    let result = describe(&image, 99, "whatever", None, EVERYTHING);

    assert!(
        result
            == tagged_wire!(DescribeConfigsResult {
                error_code: crate::codes::INVALID_REQUEST,
                error_message: Some("Unsupported resource type: 99".to_owned()),
                resource_type: 99,
                resource_name: "whatever".to_owned(),
                configs: Vec::new(),
            })
    );
}

/// KIP-211: a broker that runs the built-in retention reports both keys at
/// `DEFAULT_CONFIG`, read-only, with the default on the synonym chain.
///
/// Verified against `apache/kafka:4.3.1`, where `kafka-configs --entity-type
/// brokers --entity-name 1 --describe --all` reports
/// `offsets.retention.minutes=10080 sensitive=false
/// synonyms={DEFAULT_CONFIG:offsets.retention.minutes=10080}` on a broker
/// whose properties name neither key.
#[test]
fn an_untuned_broker_reports_both_retention_keys_at_their_default() {
    let result = describe(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "1",
        Some(vec![
            config_keys::OFFSETS_RETENTION_MINUTES.to_owned(),
            config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS.to_owned(),
        ]),
        EVERYTHING,
    );

    assert!(
        result.configs
            == vec![
                expected_config_entry(ExpectedConfigSetup {
                    name: config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                    value: Some("600000"),
                    mutability: ConfigMutability::ReadOnly,
                    synonyms: vec![synonym(
                        config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                        "600000",
                        CONFIG_SOURCE_DEFAULT
                    )],
                    documentation: Some(
                        registry::lookup(
                            ConfigScope::Broker,
                            config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS
                        )
                        .expect("offsets.retention.check.interval.ms")
                        .doc
                        .to_owned()
                    ),
                    ..Default::default()
                }),
                expected_config_entry(ExpectedConfigSetup {
                    name: config_keys::OFFSETS_RETENTION_MINUTES,
                    value: Some("10080"),
                    mutability: ConfigMutability::ReadOnly,
                    synonyms: vec![synonym(
                        config_keys::OFFSETS_RETENTION_MINUTES,
                        "10080",
                        CONFIG_SOURCE_DEFAULT
                    )],
                    config_type: ConfigTypeCode(ConfigType::Int.wire()),
                    documentation: Some(
                        registry::lookup(
                            ConfigScope::Broker,
                            config_keys::OFFSETS_RETENTION_MINUTES
                        )
                        .expect("offsets.retention.minutes")
                        .doc
                        .to_owned()
                    ),
                    ..Default::default()
                }),
            ]
    );
}

/// A retuned knob reports the process's own value at `STATIC_BROKER_CONFIG`,
/// above the default it displaced. It stays read-only either way:
/// `apache/kafka:4.3.1` answers `kafka-configs --alter --add-config
/// offsets.retention.minutes=100` with `InvalidRequestException: Cannot update
/// these configs dynamically`.
#[test]
fn a_retuned_retention_knob_reports_the_static_layer_above_the_default() {
    let result = describe_at(
        (
            serving_node_for(RESOURCE_TYPE_BROKER, "1"),
            StaticBrokerConfigs {
                offsets_retention: Some(krabka_units::minutes(60)),
                offsets_retention_check_interval: None,
                ..untuned()
            },
        ),
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "1",
        Some(vec![config_keys::OFFSETS_RETENTION_MINUTES.to_owned()]),
        EVERYTHING,
    );

    assert!(
        result.configs
            == vec![expected_config_entry(ExpectedConfigSetup {
                name: config_keys::OFFSETS_RETENTION_MINUTES,
                value: Some("60"),
                mutability: ConfigMutability::ReadOnly,
                source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
                synonyms: vec![
                    synonym(
                        config_keys::OFFSETS_RETENTION_MINUTES,
                        "60",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                    synonym(
                        config_keys::OFFSETS_RETENTION_MINUTES,
                        "10080",
                        CONFIG_SOURCE_DEFAULT
                    ),
                ],
                config_type: ConfigTypeCode(ConfigType::Int.wire()),
                documentation: Some(
                    registry::lookup(ConfigScope::Broker, config_keys::OFFSETS_RETENTION_MINUTES)
                        .expect("offsets.retention.minutes")
                        .doc
                        .to_owned()
                )
            })]
    );
}

/// Source is provenance, not a comparison. A key the operator wrote down at
/// Kafka's own default value still reports `STATIC_BROKER_CONFIG`, above the
/// `DEFAULT_CONFIG` synonym that carries the same number.
///
/// Verified against `apache/kafka:4.3.1` with `offsets.retention.minutes=10080`
/// in the broker's properties: `kafka-configs --entity-type brokers
/// --entity-name 1 --describe --all` answers
/// `synonyms={STATIC_BROKER_CONFIG:offsets.retention.minutes=10080,
/// DEFAULT_CONFIG:offsets.retention.minutes=10080}`.
#[test]
fn a_knob_set_to_its_own_default_still_reports_the_static_source() {
    let described = |static_broker| {
        describe_at(
            (serving_node_for(RESOURCE_TYPE_BROKER, "1"), static_broker),
            &MetadataImage::new(Uuid::nil()),
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![config_keys::OFFSETS_RETENTION_MINUTES.to_owned()]),
            EVERYTHING,
        )
        .configs
    };
    let default_synonym = synonym(
        config_keys::OFFSETS_RETENTION_MINUTES,
        "10080",
        CONFIG_SOURCE_DEFAULT,
    );
    let entry = |config_source, synonyms| {
        expected_config_entry(ExpectedConfigSetup {
            name: config_keys::OFFSETS_RETENTION_MINUTES,
            value: Some("10080"),
            mutability: ConfigMutability::ReadOnly,
            source: ConfigSourceCode(config_source),
            synonyms,
            config_type: ConfigTypeCode(ConfigType::Int.wire()),
            documentation: Some(
                registry::lookup(ConfigScope::Broker, config_keys::OFFSETS_RETENTION_MINUTES)
                    .expect("offsets.retention.minutes")
                    .doc
                    .to_owned(),
            ),
        })
    };

    check!(
        described(untuned()) == vec![entry(CONFIG_SOURCE_DEFAULT, vec![default_synonym.clone()])],
        "a broker that names neither key"
    );
    check!(
        described(StaticBrokerConfigs {
            offsets_retention: Some(krabka_units::minutes(10_080)),
            offsets_retention_check_interval: None,
            ..untuned()
        }) == vec![entry(
            CONFIG_SOURCE_STATIC_BROKER,
            vec![
                synonym(
                    config_keys::OFFSETS_RETENTION_MINUTES,
                    "10080",
                    CONFIG_SOURCE_STATIC_BROKER
                ),
                default_synonym,
            ]
        )],
        "a broker whose properties name the key at that same value"
    );
}

/// The idle-window doc string, which both the broker-wide key and every
/// per-listener override report.
fn idle_documentation() -> String {
    registry::lookup(ConfigScope::Broker, config_keys::CONNECTIONS_MAX_IDLE_MS)
        .expect("connections.max.idle.ms")
        .doc
        .to_owned()
}

/// A named broker resource reports the idle window beside the static
/// `node.id`, because both describe the node this process is rather than the
/// cluster it belongs to. A process that names no window reports Kafka's
/// 600000 at `DEFAULT_CONFIG`.
#[test]
fn a_broker_reports_its_idle_window_beside_the_static_node_id() {
    let result = describe(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "7",
        None,
        VALUES_ONLY,
    );

    let view = static_view(&result);
    let names: Vec<&str> = view.iter().map(|entry| entry.name.as_str()).collect();
    assert!(
        names
            == vec![
                config_keys::AUTO_CREATE_TOPICS_ENABLE,
                config_keys::CONNECTIONS_MAX_IDLE_MS,
                config_keys::DEFAULT_REPLICATION_FACTOR,
                config_keys::DELETE_TOPIC_ENABLE,
                NODE_ID,
                config_keys::NUM_PARTITIONS,
                config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                config_keys::OFFSETS_RETENTION_MINUTES,
                config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            ]
    );
    assert!(
        *entry_named(&result, config_keys::CONNECTIONS_MAX_IDLE_MS)
            == expected_config_entry(ExpectedConfigSetup {
                name: config_keys::CONNECTIONS_MAX_IDLE_MS,
                value: Some("600000"),
                mutability: ConfigMutability::ReadOnly,
                ..Default::default()
            })
    );
}

/// A window the operator set, and the per-listener override beside it, both
/// report at `STATIC_BROKER_CONFIG` with the chain `kafka-configs --all`
/// renders after the value.
#[test]
fn a_configured_idle_window_and_its_listener_override_report_as_static() {
    let listener_key = "listener.name.external.connections.max.idle.ms";
    let overrides = std::iter::once(("EXTERNAL".to_owned(), krabka_units::secs(5))).collect();

    let result = describe_at(
        (
            serving_node_for(RESOURCE_TYPE_BROKER, "7"),
            StaticBrokerConfigs {
                connections_max_idle: Some(krabka_units::secs(30)),
                connections_max_idle_overrides: &overrides,
                ..untuned()
            },
        ),
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "7",
        Some(vec![
            config_keys::CONNECTIONS_MAX_IDLE_MS.to_owned(),
            listener_key.to_owned(),
        ]),
        EVERYTHING,
    );

    let broker_wide_static = synonym(
        config_keys::CONNECTIONS_MAX_IDLE_MS,
        "30000",
        CONFIG_SOURCE_STATIC_BROKER,
    );
    let broker_wide_default = synonym(
        config_keys::CONNECTIONS_MAX_IDLE_MS,
        "600000",
        CONFIG_SOURCE_DEFAULT,
    );
    assert!(
        result.configs
            == vec![
                expected_config_entry(ExpectedConfigSetup {
                    name: config_keys::CONNECTIONS_MAX_IDLE_MS,
                    value: Some("30000"),
                    mutability: ConfigMutability::ReadOnly,
                    source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
                    synonyms: vec![broker_wide_static.clone(), broker_wide_default.clone()],
                    documentation: Some(idle_documentation()),
                    ..Default::default()
                }),
                expected_config_entry(ExpectedConfigSetup {
                    name: listener_key,
                    value: Some("5000"),
                    mutability: ConfigMutability::ReadOnly,
                    source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
                    synonyms: vec![
                        synonym(listener_key, "5000", CONFIG_SOURCE_STATIC_BROKER),
                        broker_wide_static,
                        broker_wide_default,
                    ],
                    documentation: Some(idle_documentation()),
                    ..Default::default()
                }),
            ]
    );
}

/// The cluster-default broker resource describes the cluster, not a node, so
/// the idle window has no place in it — the same rule that keeps `node.id`
/// and the retention keys out.
#[test]
fn the_cluster_default_resource_reports_no_idle_window() {
    let overrides = std::iter::once(("EXTERNAL".to_owned(), krabka_units::secs(5))).collect();

    let result = describe_at(
        (
            serving_node_for(RESOURCE_TYPE_BROKER, ""),
            StaticBrokerConfigs {
                connections_max_idle: Some(krabka_units::secs(30)),
                connections_max_idle_overrides: &overrides,
                ..untuned()
            },
        ),
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "",
        None,
        EVERYTHING,
    );

    assert!(result.configs == Vec::new());
}

/// A topic with a stored config map, for the synonym-chain rows below.
fn image_with_topic(overrides: &[(&str, &str)], cluster: &[(&str, &str)]) -> MetadataImage {
    let mut image = image_with_broker_config(DEFAULT_BROKER_CONFIG_NODE_ID, cluster);
    image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(7),
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "t".into(),
        overrides: string_pairs(overrides),
    }));
    image
}

/// Kafka's `createTopicConfigEntry` chain: the topic override, then every
/// broker synonym of the key under the broker key's name. Each row is the
/// stored state, the key, and the entry's value, source and synonyms.
#[test]
fn a_topic_key_reports_its_broker_synonyms_under_the_broker_names() {
    let cases = [
        (
            vec![],
            vec![("min.insync.replicas", "2")],
            "min.insync.replicas",
            "2",
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            vec![
                synonym(
                    "min.insync.replicas",
                    "2",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                ),
                synonym("min.insync.replicas", "1", CONFIG_SOURCE_DEFAULT),
            ],
        ),
        (
            vec![("retention.ms", "1000")],
            vec![],
            "retention.ms",
            "1000",
            CONFIG_SOURCE_DYNAMIC_TOPIC,
            vec![
                synonym("retention.ms", "1000", CONFIG_SOURCE_DYNAMIC_TOPIC),
                synonym("log.retention.hours", "168", CONFIG_SOURCE_DEFAULT),
            ],
        ),
        (
            vec![],
            vec![],
            "retention.ms",
            "604800000",
            CONFIG_SOURCE_DEFAULT,
            vec![synonym("log.retention.hours", "168", CONFIG_SOURCE_DEFAULT)],
        ),
        (
            vec![],
            vec![],
            "max.message.bytes",
            "1048588",
            CONFIG_SOURCE_DEFAULT,
            vec![synonym(
                "message.max.bytes",
                "1048588",
                CONFIG_SOURCE_DEFAULT,
            )],
        ),
        (
            vec![],
            vec![("log.cleanup.policy", "compact")],
            "cleanup.policy",
            "compact",
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            vec![
                synonym(
                    "log.cleanup.policy",
                    "compact",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                ),
                synonym("log.cleanup.policy", "delete", CONFIG_SOURCE_DEFAULT),
            ],
        ),
    ];
    for (overrides, cluster, key, value, source, synonyms) in cases {
        let image = image_with_topic(&overrides, &cluster);
        let result = describe_topic(&image, "t", Some(vec![key.to_owned()]));
        let entry = entry_named(&result, key);
        check!(
            (entry.value.as_deref(), entry.config_source, &entry.synonyms)
                == (Some(value), source, &synonyms),
            "{key} {overrides:?} {cluster:?}"
        );
    }
}

/// A broker's `min.insync.replicas` is typed and disclosed, on the cluster
/// default resource and on a named broker, and a named broker reports each
/// topic-default broker key it runs with.
#[test]
fn a_broker_reports_its_topic_default_keys_typed() {
    let image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[("min.insync.replicas", "2")],
    );
    let cluster = describe(&image, RESOURCE_TYPE_BROKER, "", None, EVERYTHING);
    let entry = entry_named(&cluster, "min.insync.replicas");
    check!(
        (
            entry.value.as_deref(),
            entry.config_source,
            entry.is_sensitive,
            entry.config_type,
            entry.synonyms.clone(),
        ) == (
            Some("2"),
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            false,
            ConfigType::Int.wire(),
            vec![synonym(
                "min.insync.replicas",
                "2",
                CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            )],
        )
    );

    let named = describe(&image, RESOURCE_TYPE_BROKER, "1", None, EVERYTHING);
    let entry = entry_named(&named, "message.max.bytes");
    check!(
        (entry.value.as_deref(), entry.config_source, entry.read_only)
            == (Some("1048588"), CONFIG_SOURCE_DEFAULT, false)
    );
    for (key, _) in TOPIC_DEFAULT_SYNONYMS {
        check!(
            named.configs.iter().any(|entry| entry.name == *key),
            "{key}"
        );
    }
}

/// Kafka's `createBrokerConfigEntry` types a stored key by `KafkaConfig`'s own
/// `ConfigDef`, and withholds a value only for a `PASSWORD` key or a name Kafka
/// does not define (`KafkaConfig.maybeSensitive`). `readOnly` is
/// `!ALL_DYNAMIC_CONFIGS.contains(name)`, so a listener override and a name
/// Kafka does not define both read back read-only.
#[test]
fn a_stored_kafka_config_key_comes_back_typed_and_disclosed() {
    // The key, its stored value, and the value, sensitivity, wire type and
    // read-only flag the entry carries.
    type Case = (
        &'static str,
        &'static str,
        Option<&'static str>,
        bool,
        i8,
        bool,
    );
    let cases: [Case; 7] = [
        ("num.io.threads", "8", Some("8"), false, 3, false),
        ("max.connections", "100", Some("100"), false, 3, false),
        (
            "follower.fetch.last.tiered.offset.enable",
            "true",
            Some("true"),
            false,
            1,
            false,
        ),
        (
            "listener.name.external.ssl.keystore.location",
            "/keys/external.jks",
            Some("/keys/external.jks"),
            false,
            2,
            true,
        ),
        (
            "listener.name.external.ssl.keystore.password",
            "hunter2",
            None,
            true,
            9,
            true,
        ),
        // The mechanism-prefixed JAAS config is typed by the key it ends in.
        (
            "listener.name.external.plain.sasl.jaas.config",
            "secret",
            None,
            true,
            9,
            true,
        ),
        // A name `KafkaConfig` does not define: untyped, so withheld.
        ("plugin.custom.key", "x", None, true, 0, true),
    ];
    for (name, stored, value, sensitive, wire_type, read_only) in cases {
        for (resource_name, node) in [("", DEFAULT_BROKER_CONFIG_NODE_ID), ("1", SERVING_NODE)] {
            let image = image_with_broker_config(node, &[(name, stored)]);
            let result = describe(
                &image,
                RESOURCE_TYPE_BROKER,
                resource_name,
                Some(vec![name.to_owned()]),
                VALUES_ONLY,
            );
            let entry = entry_named(&result, name);
            check!(
                (
                    entry.value.as_deref(),
                    entry.is_sensitive,
                    entry.config_type,
                    entry.read_only,
                ) == (value, sensitive, wire_type, read_only),
                "{name} on {resource_name:?}"
            );
        }
    }
}

/// Kafka's `ConfigHelperUtils.createResponseConfig` walks
/// `config.nonInternalValues()` for a named broker, so tools that read
/// `KafkaConfig` through `DescribeConfigs` find every non-internal key, at its
/// default unless the process holds a value.
#[test]
fn a_named_broker_reports_every_kafka_config_key() {
    let image = MetadataImage::new(Uuid::nil());
    let result = describe(&image, RESOURCE_TYPE_BROKER, "1", None, EVERYTHING);

    for row in KAFKA_BROKER_CONFIGS {
        let count = result
            .configs
            .iter()
            .filter(|entry| entry.name == row.name)
            .count();
        check!(count == usize::from(!row.internal), "{}", row.name);
    }

    // `num.network.threads` is not one krabka holds a static value of: it is
    // the built-in default, typed, at `DEFAULT_CONFIG`.
    let entry = entry_named(&result, "num.network.threads");
    check!(
        (
            entry.value.as_deref(),
            entry.config_source,
            entry.config_type,
            entry.read_only,
            entry.synonyms.clone(),
        ) == (
            Some("3"),
            CONFIG_SOURCE_DEFAULT,
            ConfigType::Int.wire(),
            false,
            vec![synonym("num.network.threads", "3", CONFIG_SOURCE_DEFAULT)],
        )
    );
    // A `PASSWORD` key is typed and withheld, and a read-only key says so.
    let entry = entry_named(&result, "ssl.keystore.password");
    check!((entry.value.clone(), entry.is_sensitive, entry.config_type) == (None, true, 9));
    check!(entry_named(&result, "auto.leader.rebalance.enable").read_only);
    // An internal key is not listed.
    check!(
        !result
            .configs
            .iter()
            .any(|entry| entry.name == "unstable.api.versions.enable")
    );
}

/// A key this process holds a static value of reports it at
/// `STATIC_BROKER_CONFIG`, with the built-in default beneath it, as Kafka
/// reports a key that `server.properties` names.
#[test]
fn a_named_broker_reports_the_static_values_it_holds() {
    let settings = maplit::btreemap! {
        "log.dirs" => "/data/a,/data/b".to_owned(),
        "message.max.bytes" => "2097152".to_owned(),
        "broker.rack" => "rack-1".to_owned(),
    };
    let result = describe_at(
        settings_for((RESOURCE_TYPE_BROKER, "1"), &settings),
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "1",
        None,
        EVERYTHING,
    );

    let entry = entry_named(&result, "log.dirs");
    check!(
        (
            entry.value.as_deref(),
            entry.config_source,
            entry.synonyms.clone(),
        ) == (
            Some("/data/a,/data/b"),
            CONFIG_SOURCE_STATIC_BROKER,
            vec![synonym(
                "log.dirs",
                "/data/a,/data/b",
                CONFIG_SOURCE_STATIC_BROKER
            )],
        )
    );
    let entry = entry_named(&result, "message.max.bytes");
    check!(
        (
            entry.value.as_deref(),
            entry.config_source,
            entry.synonyms.clone(),
        ) == (
            Some("2097152"),
            CONFIG_SOURCE_STATIC_BROKER,
            vec![
                synonym("message.max.bytes", "2097152", CONFIG_SOURCE_STATIC_BROKER),
                synonym("message.max.bytes", "1048588", CONFIG_SOURCE_DEFAULT),
            ],
        )
    );
    let entry = entry_named(&result, "broker.rack");
    check!(
        (entry.value.as_deref(), entry.config_source)
            == (Some("rack-1"), CONFIG_SOURCE_STATIC_BROKER)
    );
}

/// `sasl.server.max.receive.size` and `connection.failed.authentication.delay.ms`
/// are not dynamic, so a named broker reports each as a read-only `INT`, at
/// its Kafka default unless the operator named it and at
/// `STATIC_BROKER_CONFIG` with that default beneath it when they did, even
/// for a value equal to the default.
#[test]
fn a_named_broker_reports_an_authentication_limit_the_operator_named() {
    let sasl = "sasl.server.max.receive.size";
    let delay = "connection.failed.authentication.delay.ms";
    let default_only = expected_default_chain;
    let named = expected_named_static_chain;
    for (label, source, want_sasl, want_delay) in [
        (
            "neither named",
            "[runtime]\n",
            default_only(sasl, "524288"),
            default_only(delay, "100"),
        ),
        (
            "both named",
            "[runtime]\nsasl_server_max_receive = \"1MiB\"\n\
             connection_failed_authentication_delay = \"0ms\"\n",
            named(sasl, "1048576", "524288"),
            named(delay, "0", "100"),
        ),
        (
            "both named at Kafka's own default",
            "[runtime]\nsasl_server_max_receive = \"524288B\"\n\
             connection_failed_authentication_delay = \"100ms\"\n",
            named(sasl, "524288", "524288"),
            named(delay, "100", "100"),
        ),
    ] {
        let file: FileConfig = toml::from_str(source).expect("parse runtime config");
        let mut config = BrokerConfig::default();
        file.apply_to(&mut config).expect("apply runtime config");
        let settings = static_settings(&config);
        let result = describe_at(
            settings_for((RESOURCE_TYPE_BROKER, "1"), &settings),
            &MetadataImage::new(Uuid::nil()),
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![sasl.to_owned(), delay.to_owned()]),
            EVERYTHING,
        );

        for (key, want) in [(sasl, want_sasl), (delay, want_delay)] {
            let entry = entry_named(&result, key);
            check!(
                (
                    entry.value.clone(),
                    entry.config_source,
                    entry.synonyms.clone(),
                ) == want,
                "{label}: {key}"
            );
            check!(
                entry.read_only && entry.config_type == ConfigType::Int.wire(),
                "{label}: {key} is a read-only INT"
            );
        }
    }
}

/// Kafka's `KafkaConfigSchema.resolveEffectiveTopicConfig` reports the static
/// layer whenever `server.properties` names a synonym of the key, at the
/// default value too, so `message.max.bytes`, `log.segment.bytes` and
/// `min.insync.replicas` set on the broker reach a topic that overrides none
/// of them, ahead of the built-in default. A topic override still wins.
#[test]
fn a_topic_reports_the_static_synonyms_the_broker_was_started_with() {
    let settings = maplit::btreemap! {
        "message.max.bytes" => "2097152".to_owned(),
        "log.segment.bytes" => "536870912".to_owned(),
        // Kafka's own default, spelled out: still the static layer.
        "min.insync.replicas" => "1".to_owned(),
    };
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: maplit::btreemap! {
            "segment.bytes".to_string() => "1048576".to_string(),
        },
    }));
    let result = describe_at(
        settings_for((RESOURCE_TYPE_TOPIC, "orders"), &settings),
        &with_topic(&image, RESOURCE_TYPE_TOPIC, "orders"),
        RESOURCE_TYPE_TOPIC,
        "orders",
        None,
        EVERYTHING,
    );

    let chain = |key: &str| {
        let entry = entry_named(&result, key);
        (
            entry.value.clone(),
            entry.config_source,
            entry.synonyms.clone(),
        )
    };
    check!(
        chain("max.message.bytes")
            == (
                Some("2097152".to_owned()),
                CONFIG_SOURCE_STATIC_BROKER,
                vec![
                    synonym("message.max.bytes", "2097152", CONFIG_SOURCE_STATIC_BROKER),
                    synonym("message.max.bytes", "1048588", CONFIG_SOURCE_DEFAULT),
                ],
            )
    );
    check!(
        chain("min.insync.replicas")
            == (
                Some("1".to_owned()),
                CONFIG_SOURCE_STATIC_BROKER,
                vec![
                    synonym("min.insync.replicas", "1", CONFIG_SOURCE_STATIC_BROKER),
                    synonym("min.insync.replicas", "1", CONFIG_SOURCE_DEFAULT),
                ],
            )
    );
    check!(
        chain("segment.bytes")
            == (
                Some("1048576".to_owned()),
                CONFIG_SOURCE_DYNAMIC_TOPIC,
                vec![
                    synonym("segment.bytes", "1048576", CONFIG_SOURCE_DYNAMIC_TOPIC),
                    synonym(
                        "log.segment.bytes",
                        "536870912",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                    synonym("log.segment.bytes", "1073741824", CONFIG_SOURCE_DEFAULT),
                ],
            )
    );

    // The KIP-525 list `CreateTopics` v5+ carries is the same computation.
    let created = effective_topic_configs(
        &MetadataImage::new(Uuid::nil()),
        SERVING_NODE,
        "orders",
        &std::collections::BTreeMap::new(),
        UnstableApiVersions::Disabled,
        &settings,
    );
    let created_entry = |key: &str| {
        let entry = created
            .iter()
            .find(|entry| entry.name == key)
            .expect("a topic key");
        (entry.value.clone(), entry.config_source)
    };
    check!(
        created_entry("max.message.bytes")
            == (Some("2097152".to_owned()), CONFIG_SOURCE_STATIC_BROKER)
    );
    check!(
        created_entry("segment.bytes")
            == (Some("536870912".to_owned()), CONFIG_SOURCE_STATIC_BROKER)
    );
}

/// A node started with `log.roll.ms` in its `server.properties` keys rolls
/// every topic that sets no `segment.ms` at that interval, so the topic's
/// `segment.ms` and the named broker's `log.roll.ms` both report it at
/// `STATIC_BROKER_CONFIG`. A node that names neither key reports Kafka's
/// seven days through the `log.roll.hours` default.
#[test]
fn a_topic_and_a_named_broker_report_the_log_roll_ms_the_node_runs() {
    /// The value, the source and the synonyms of one entry.
    type Chain = (Option<String>, i8, Vec<DescribeConfigsSynonym>);
    let cases: [(&str, &str, Chain, Chain); 2] = [
        (
            "not named",
            "broker_id = 1\n",
            (
                Some("604800000".to_owned()),
                CONFIG_SOURCE_DEFAULT,
                vec![synonym("log.roll.hours", "168", CONFIG_SOURCE_DEFAULT)],
            ),
            (None, CONFIG_SOURCE_DEFAULT, vec![]),
        ),
        (
            "named",
            "[server_properties]\n\"log.roll.ms\" = \"3000\"\n",
            (
                Some("3000".to_owned()),
                CONFIG_SOURCE_STATIC_BROKER,
                vec![
                    synonym("log.roll.ms", "3000", CONFIG_SOURCE_STATIC_BROKER),
                    synonym("log.roll.hours", "168", CONFIG_SOURCE_DEFAULT),
                ],
            ),
            (
                Some("3000".to_owned()),
                CONFIG_SOURCE_STATIC_BROKER,
                vec![synonym("log.roll.ms", "3000", CONFIG_SOURCE_STATIC_BROKER)],
            ),
        ),
    ];
    let image = MetadataImage::new(Uuid::nil());
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, src, topic_chain, broker_chain) in cases {
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut config = BrokerConfig::default();
        file.apply_to(&mut config).expect("apply");
        let settings = static_settings(&config);
        let chain = |resource_type, name: &str, key: &str| {
            let result = describe_at(
                settings_for((resource_type, name), &settings),
                &with_topic(&image, resource_type, name),
                resource_type,
                name,
                None,
                EVERYTHING,
            );
            let entry = entry_named(&result, key);
            (
                entry.value.clone(),
                entry.config_source,
                entry.synonyms.clone(),
            )
        };
        actual.push((
            label,
            chain(RESOURCE_TYPE_TOPIC, "orders", "segment.ms"),
            chain(RESOURCE_TYPE_BROKER, "1", "log.roll.ms"),
        ));
        expected.push((label, topic_chain, broker_chain));
    }
    assert!(actual == expected);
}

/// A topic's `min.insync.replicas` resolves, for the node that computes it,
/// as the topic override, then that node's own dynamic broker config, then
/// the cluster-wide default (`KafkaConfigSchema.resolveEffectiveTopicConfig`).
/// `DescribeConfigs` computes it on the serving broker and `CreateTopics`
/// (`computeEffectiveTopicConfigs`) on the controller, so both reach the
/// node's own value at `DYNAMIC_BROKER_CONFIG` and never another node's.
#[test]
fn a_topic_reports_the_computing_nodes_own_min_insync_replicas() {
    let mut image = MetadataImage::new(Uuid::nil());
    for (node, value) in [
        (krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID, "3"),
        (krabka_metadata::NodeId(1), "2"),
    ] {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: node,
            config_name: "min.insync.replicas".into(),
            config_value: Some(value.into()),
        }));
    }
    let image = with_topic(&image, RESOURCE_TYPE_TOPIC, "orders");
    let keys = Some(vec!["min.insync.replicas".to_owned()]);
    let expected = |node_value: &str, node_source, synonyms| {
        (Some(node_value.to_owned()), node_source, synonyms)
    };

    for (node, want) in [
        (
            krabka_metadata::NodeId(1),
            expected(
                "2",
                CONFIG_SOURCE_DYNAMIC_BROKER,
                vec![
                    synonym("min.insync.replicas", "2", CONFIG_SOURCE_DYNAMIC_BROKER),
                    synonym(
                        "min.insync.replicas",
                        "3",
                        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    ),
                    synonym("min.insync.replicas", "1", CONFIG_SOURCE_DEFAULT),
                ],
            ),
        ),
        (
            krabka_metadata::NodeId(2),
            expected(
                "3",
                CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                vec![
                    synonym(
                        "min.insync.replicas",
                        "3",
                        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    ),
                    synonym("min.insync.replicas", "1", CONFIG_SOURCE_DEFAULT),
                ],
            ),
        ),
    ] {
        let described = describe_at(
            (node, untuned()),
            &image,
            RESOURCE_TYPE_TOPIC,
            "orders",
            keys.clone(),
            EVERYTHING,
        );
        let entry = entry_named(&described, "min.insync.replicas");
        check!(
            (
                entry.value.clone(),
                entry.config_source,
                entry.synonyms.clone()
            ) == want,
            "DescribeConfigs served by {node:?}"
        );

        let created = effective_topic_configs(
            &image,
            node,
            "orders",
            &std::collections::BTreeMap::new(),
            UnstableApiVersions::Disabled,
            &std::collections::BTreeMap::new(),
        );
        let entry = created
            .iter()
            .find(|entry| entry.name == "min.insync.replicas")
            .expect("a topic key");
        check!(
            (entry.value.clone(), entry.config_source) == (want.0, want.1),
            "CreateTopics computed by {node:?}"
        );
    }
}

/// Kafka's `extractGroupConfigMap` and `createGroupConfigEntry`: a group
/// reports, beneath its own override, the value the coordinators run with, at
/// `STATIC_BROKER_CONFIG` when the operator named the broker synonym (even at
/// Kafka's default) or the coordinator runs another value, and the same key on
/// a named broker reports the same value with the same source. A node that
/// named nothing reports Kafka's defaults.
#[test]
fn a_group_and_a_named_broker_report_the_values_the_coordinators_run_with() {
    let file: FileConfig = toml::from_str(
        "[runtime]\nconsumer_group_session_timeout = \"60s\"\n\
         consumer_group_heartbeat_interval = \"5s\"\n\
         share_group_record_lock_duration = \"45s\"\n\
         streams_group_num_standby_replicas = 1\n\
         socket_send_buffer = \"1MiB\"\n",
    )
    .expect("parse runtime config");
    let mut config = BrokerConfig::default();
    file.apply_to(&mut config).expect("apply runtime config");
    let settings = static_settings(&config);
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "g".into(),
            configs: maplit::btreemap! {
                "share.record.lock.duration.ms".to_owned() => "20000".to_owned(),
            },
        },
    ));
    let described = |resource_type, name: &str| {
        describe_at(
            settings_for((resource_type, name), &settings),
            &with_topic(&image, resource_type, name),
            resource_type,
            name,
            None,
            EVERYTHING,
        )
    };
    let group = described(RESOURCE_TYPE_GROUP, "g");
    let broker = described(RESOURCE_TYPE_BROKER, "1");
    let chain = |result: &DescribeConfigsResult, key: &str| {
        let entry = entry_named(result, key);
        (
            entry.value.clone(),
            entry.config_source,
            entry.synonyms.clone(),
        )
    };
    let named = expected_named_static_chain;
    let untouched = expected_default_chain;

    // A group's own key, over the broker synonym that the coordinator's
    // settings state.
    check!(
        chain(&group, "consumer.session.timeout.ms")
            == named("group.consumer.session.timeout.ms", "60000", "45000")
    );
    // Named at Kafka's own default: still the static layer.
    check!(
        chain(&group, "consumer.heartbeat.interval.ms")
            == named("group.consumer.heartbeat.interval.ms", "5000", "5000")
    );
    check!(
        chain(&group, "consumer.assignment.interval.ms")
            == untouched("group.consumer.assignment.interval.ms", "1000")
    );
    check!(
        chain(&group, "streams.num.standby.replicas")
            == named("group.streams.num.standby.replicas", "1", "0")
    );
    // The group's override is above the value the coordinator runs.
    check!(
        chain(&group, "share.record.lock.duration.ms")
            == (
                Some("20000".to_owned()),
                CONFIG_SOURCE_DYNAMIC_GROUP,
                vec![
                    synonym(
                        "share.record.lock.duration.ms",
                        "20000",
                        CONFIG_SOURCE_DYNAMIC_GROUP
                    ),
                    synonym(
                        "group.share.record.lock.duration.ms",
                        "45000",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                    synonym(
                        "group.share.record.lock.duration.ms",
                        "30000",
                        CONFIG_SOURCE_DEFAULT
                    ),
                ],
            )
    );

    // The broker resource names the same keys and agrees with the group.
    check!(
        chain(&broker, "group.consumer.session.timeout.ms")
            == named("group.consumer.session.timeout.ms", "60000", "45000")
    );
    check!(
        chain(&broker, "group.consumer.heartbeat.interval.ms")
            == named("group.consumer.heartbeat.interval.ms", "5000", "5000")
    );
    check!(
        chain(&broker, "group.share.record.lock.duration.ms")
            == named("group.share.record.lock.duration.ms", "45000", "30000")
    );
    check!(
        chain(&broker, "group.consumer.assignment.interval.ms")
            == untouched("group.consumer.assignment.interval.ms", "1000")
    );
    // Kafka's `socket.send.buffer.bytes` is 100 KiB and this node runs 1 MiB,
    // which the operator named, and a key the operator left alone stays at
    // Kafka's default.
    check!(
        chain(&broker, "socket.send.buffer.bytes")
            == named("socket.send.buffer.bytes", "1048576", "102400")
    );
    check!(chain(&broker, "queued.max.requests") == untouched("queued.max.requests", "500"));
}

/// Kafka's `createGroupConfigEntry`: every `GroupConfig` key, with the group
/// override above the broker synonym, or a `DEFAULT_CONFIG` entry under the
/// group key's own name for a key with no broker synonym.
#[test]
fn a_group_reports_every_kafka_group_key_with_its_broker_synonym() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "g".into(),
            configs: maplit::btreemap! {
                "streams.session.timeout.ms".to_owned() => "60000".to_owned(),
            },
        },
    ));
    let result = describe(&image, RESOURCE_TYPE_GROUP, "g", None, EVERYTHING);
    let names: Vec<&str> = result
        .configs
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    let kafka: Vec<&str> = KAFKA_GROUP_KEYS.iter().map(|key| key.name).collect();
    check!(names == kafka);

    let cases = [
        (
            "consumer.session.timeout.ms",
            "45000",
            CONFIG_SOURCE_DEFAULT,
            vec![synonym(
                "group.consumer.session.timeout.ms",
                "45000",
                CONFIG_SOURCE_DEFAULT,
            )],
        ),
        (
            "streams.session.timeout.ms",
            "60000",
            CONFIG_SOURCE_DYNAMIC_GROUP,
            vec![
                synonym(
                    "streams.session.timeout.ms",
                    "60000",
                    CONFIG_SOURCE_DYNAMIC_GROUP,
                ),
                synonym(
                    "group.streams.session.timeout.ms",
                    "45000",
                    CONFIG_SOURCE_DEFAULT,
                ),
            ],
        ),
        (
            "share.isolation.level",
            "read_uncommitted",
            CONFIG_SOURCE_DEFAULT,
            vec![synonym(
                "share.isolation.level",
                "read_uncommitted",
                CONFIG_SOURCE_DEFAULT,
            )],
        ),
    ];
    for (key, value, source, synonyms) in cases {
        let entry = entry_named(&result, key);
        check!(
            (
                entry.value.as_deref(),
                entry.config_source,
                entry.is_sensitive,
                &entry.synonyms
            ) == (Some(value), source, false, &synonyms),
            "{key}"
        );
    }
}

/// `transaction.partition.verification.enable` is a dynamic broker config with
/// a static layer. A named broker reports the static value beneath its
/// dynamic overrides, at `STATIC_BROKER_CONFIG`, when the operator named the
/// key in the `[runtime]` table or in `server_properties` (even at Kafka's
/// default) or the node runs another value, and Kafka's default alone
/// otherwise.
#[test]
fn a_named_broker_reports_the_static_layer_of_the_partition_verification_key() {
    const KEY: &str = "transaction.partition.verification.enable";
    const RUNTIME_FALSE: &str = "[runtime]\ntransaction_partition_verification_enable = false\n";
    const PROPERTY_FALSE: &str =
        "[server_properties]\n\"transaction.partition.verification.enable\" = \"false\"\n";
    /// The dynamic values by node, then the chain: a value and its source,
    /// the head first.
    type Layers = (
        Vec<(krabka_metadata::NodeId, &'static str)>,
        Vec<(&'static str, i8)>,
    );
    let default = ("true", CONFIG_SOURCE_DEFAULT);
    let static_false = ("false", CONFIG_SOURCE_STATIC_BROKER);
    let cases: [(&str, &str, Layers); 8] = [
        ("nothing named", "broker_id = 0\n", (vec![], vec![default])),
        (
            "runtime table at Kafka's default",
            "[runtime]\ntransaction_partition_verification_enable = true\n",
            (vec![], vec![("true", CONFIG_SOURCE_STATIC_BROKER), default]),
        ),
        (
            "runtime table off",
            RUNTIME_FALSE,
            (vec![], vec![static_false, default]),
        ),
        (
            "server property off",
            PROPERTY_FALSE,
            (vec![], vec![static_false, default]),
        ),
        (
            "cluster-wide dynamic over static",
            RUNTIME_FALSE,
            (
                vec![(DEFAULT_BROKER_CONFIG_NODE_ID, "true")],
                vec![
                    ("true", CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER),
                    static_false,
                    default,
                ],
            ),
        ),
        (
            "per-broker dynamic over static",
            PROPERTY_FALSE,
            (
                vec![(SERVING_NODE, "true")],
                vec![
                    ("true", CONFIG_SOURCE_DYNAMIC_BROKER),
                    static_false,
                    default,
                ],
            ),
        ),
        (
            "per-broker dynamic over cluster-wide and static",
            RUNTIME_FALSE,
            (
                vec![
                    (DEFAULT_BROKER_CONFIG_NODE_ID, "true"),
                    (SERVING_NODE, "false"),
                ],
                vec![
                    ("false", CONFIG_SOURCE_DYNAMIC_BROKER),
                    ("true", CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER),
                    static_false,
                    default,
                ],
            ),
        ),
        (
            "dynamic over the default",
            "broker_id = 0\n",
            (
                vec![(SERVING_NODE, "false")],
                vec![("false", CONFIG_SOURCE_DYNAMIC_BROKER), default],
            ),
        ),
    ];
    for (label, source, (dynamic, chain)) in cases {
        let file: FileConfig = toml::from_str(source).expect("parse the file");
        let mut config = BrokerConfig::default();
        file.apply_to(&mut config).expect("apply the file");
        let settings = static_settings(&config);
        let mut image = MetadataImage::new(Uuid::nil());
        for (node_id, value) in dynamic {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: KEY.to_owned(),
                config_value: Some(value.to_owned()),
            }));
        }

        let described = describe_at(
            settings_for((RESOURCE_TYPE_BROKER, "1"), &settings),
            &image,
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![KEY.to_owned()]),
            EVERYTHING,
        );

        let entry = entry_named(&described, KEY);
        let (head_value, head_source) = chain[0];
        let synonyms: Vec<DescribeConfigsSynonym> = chain
            .iter()
            .map(|(value, source)| synonym(KEY, value, *source))
            .collect();
        check!(
            (entry.value.as_deref(), entry.config_source, &entry.synonyms)
                == (Some(head_value), head_source, &synonyms),
            "{label}"
        );
    }
}

mod listeners;

/// Independent wire expectations: callers pin each value, source, type,
/// synonym chain and documentation separately from production entry shaping.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::From, derive_more::Into,
)]
pub(super) struct ConfigSourceCode(pub i8);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::From, derive_more::Into,
)]
pub(super) struct ConfigTypeCode(pub i8);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum ConfigMutability {
    #[default]
    Writable,
    ReadOnly,
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ExpectedConfigSetup<'a> {
    #[default("retention.ms")]
    pub name: &'a str,
    #[default(Some("604800000"))]
    pub value: Option<&'a str>,
    pub mutability: ConfigMutability,
    #[default(ConfigSourceCode(CONFIG_SOURCE_DEFAULT))]
    pub source: ConfigSourceCode,
    pub synonyms: Vec<DescribeConfigsSynonym>,
    #[default(ConfigTypeCode(ConfigType::Long.wire()))]
    pub config_type: ConfigTypeCode,
    pub documentation: Option<String>,
}

pub(super) fn expected_config_entry(
    setup: ExpectedConfigSetup<'_>,
) -> DescribeConfigsResourceResult {
    let ExpectedConfigSetup {
        name,
        value,
        mutability,
        source,
        synonyms,
        config_type,
        documentation,
    } = setup;
    tagged_wire!(DescribeConfigsResourceResult {
        name: name.to_owned(),
        value: value.map(str::to_owned),
        read_only: mutability == ConfigMutability::ReadOnly,
        config_source: source.0,
        is_sensitive: false,
        synonyms,
        config_type: config_type.0,
        documentation,
    })
}

fn expected_plain_configs(
    rows: &[(&str, &str, i8, ConfigType)],
) -> Vec<DescribeConfigsResourceResult> {
    rows.iter()
        .map(|&(name, value, source, kind)| {
            expected_config_entry(ExpectedConfigSetup {
                name,
                value: Some(value),
                mutability: ConfigMutability::ReadOnly,
                source: ConfigSourceCode(source),
                config_type: ConfigTypeCode(kind.wire()),
                ..Default::default()
            })
        })
        .collect()
}
