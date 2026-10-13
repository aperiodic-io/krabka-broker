use {
    super::*,
    assert2::check,
    krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord},
    krabka_units::bytes,
    uuid::Uuid,
};

const NODE: NodeId = NodeId(1);

/// A broker config as stored: the node it is set for, its name and its value.
struct StoredConfig<'a> {
    node_id: NodeId,
    name: &'a str,
    value: &'a str,
}

fn image(records: &[StoredConfig<'_>]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for StoredConfig {
        node_id,
        name,
        value,
    } in records
    {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: *node_id,
            config_name: (*name).to_owned(),
            config_value: Some((*value).to_owned()),
        }));
    }
    image
}

fn defaults(records: &[StoredConfig<'_>]) -> BrokerLogDefaults {
    BrokerLogDefaults::resolve(&image(records), NODE, &LogConfig::default(), (None, None))
}

/// The static defaults of a broker whose image holds no dynamic broker
/// config: the `LogConfig` it started with, and the windows it was given.
#[test]
fn without_dynamic_configs_the_static_base_stands() {
    let base = LogConfig {
        max_message_size: bytes(4_096),
        ..LogConfig::default()
    };
    let resolved =
        BrokerLogDefaults::resolve(&image(&[]), NODE, &base, (Some(1_000), Some(3_600_000)));
    check!(
        resolved
            == BrokerLogDefaults {
                max_message_size: bytes(4_096),
                message_timestamp_type: base.message_timestamp_type,
                message_timestamp_before_max_ms: Some(1_000),
                message_timestamp_after_max_ms: Some(3_600_000),
                cleanup_policy: base.cleanup_policy,
                compression_type: base.compression_type,
            }
    );
}

/// Kafka's `DynamicLogConfig` layers: this node's own value over the cluster
/// default over the static one, each under the topic key it is the default of.
#[test]
fn a_node_override_beats_the_cluster_default_beats_the_static_base() {
    let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
    let cases = [
        ("static only", vec![], 1_048_588),
        (
            "cluster default",
            vec![StoredConfig {
                node_id: cluster,
                name: "message.max.bytes",
                value: "2097152",
            }],
            2_097_152,
        ),
        (
            "node override alone",
            vec![StoredConfig {
                node_id: NODE,
                name: "message.max.bytes",
                value: "4194304",
            }],
            4_194_304,
        ),
        (
            "node override over cluster default",
            vec![
                StoredConfig {
                    node_id: cluster,
                    name: "message.max.bytes",
                    value: "2097152",
                },
                StoredConfig {
                    node_id: NODE,
                    name: "message.max.bytes",
                    value: "4194304",
                },
            ],
            4_194_304,
        ),
        (
            "another node's override is not this node's",
            vec![StoredConfig {
                node_id: NodeId(2),
                name: "message.max.bytes",
                value: "4194304",
            }],
            1_048_588,
        ),
    ];
    for (label, records, want) in cases {
        check!(
            defaults(&records).max_message_size == bytes(want),
            "{label}"
        );
    }
}

/// Every default the produce path checks a batch against, from one stored
/// cluster default each.
#[test]
fn each_produce_checked_default_follows_its_broker_key() {
    let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
    let resolved = defaults(&[
        StoredConfig {
            node_id: cluster,
            name: "log.message.timestamp.type",
            value: "LogAppendTime",
        },
        StoredConfig {
            node_id: cluster,
            name: "log.message.timestamp.before.max.ms",
            value: "1000",
        },
        StoredConfig {
            node_id: cluster,
            name: "log.message.timestamp.after.max.ms",
            value: "9223372036854775807",
        },
        StoredConfig {
            node_id: cluster,
            name: "log.cleanup.policy",
            value: "compact",
        },
        StoredConfig {
            node_id: cluster,
            name: "compression.type",
            value: "zstd",
        },
    ]);
    check!(resolved.message_timestamp_type == TimestampType::LogAppendTime);
    check!(resolved.message_timestamp_before_max_ms == Some(1_000));
    // `Long.MAX_VALUE` spells no bound, over the static one-hour window too.
    let with_static = BrokerLogDefaults::resolve(
        &image(&[StoredConfig {
            node_id: cluster,
            name: "log.message.timestamp.after.max.ms",
            value: "9223372036854775807",
        }]),
        NODE,
        &LogConfig::default(),
        (None, Some(3_600_000)),
    );
    check!(with_static.message_timestamp_after_max_ms == None);
    check!(resolved.cleanup_policy == CleanupPolicy::Compact);
    check!(resolved.compression_type == Some(CompressionType::Zstd));
}

/// The `LogConfig` a partition falls back to is the same layering.
#[test]
fn the_partition_base_carries_the_dynamic_defaults() {
    let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
    let base = LogConfig::default();
    let image = image(&[
        StoredConfig {
            node_id: cluster,
            name: "log.segment.bytes",
            value: "2097152",
        },
        StoredConfig {
            node_id: NODE,
            name: "message.max.bytes",
            value: "8192",
        },
    ]);
    let layered = dynamic_log_base(&image, NODE, &base);
    check!(layered.segment_size == bytes(2_097_152));
    check!(layered.max_message_size == bytes(8_192));
    // A key no dynamic default names keeps its static value.
    check!(layered.retention == base.retention);
}
