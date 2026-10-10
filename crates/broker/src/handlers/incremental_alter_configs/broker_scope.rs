//! Broker-scoped resources for `IncrementalAlterConfigs`: Kafka's
//! `ConfigAdminManager.validateBrokerConfigChange` over the resource's stored
//! dynamic configs with the operations applied, then one `V1BrokerConfig`
//! record per altered key.
//!
//! The key rules themselves -- which keys are dynamic, which need a named
//! broker, and each key's value check -- are shared with `AlterConfigs` and
//! live in [`crate::config_keys::broker_dynamic`].

use std::collections::BTreeMap;

use krabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord, NodeId};
use krabka_protocol::owned::{
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{OP_APPEND, OP_DELETE, OP_SET, OP_SUBTRACT};
use crate::{
    api_catalog::UnstableApiVersions,
    codes,
    config_keys::{
        self,
        broker_dynamic::{
            BrokerConfigContext, BrokerKeyKind, CLUSTER_DEFAULT_ONLY, broker_key_kind,
            cordoned_log_dirs_disabled_error, cordoned_log_dirs_error, elr_min_isr_error,
            validate_dynamic_broker_configs,
        },
    },
};

/// The default a `LIST` broker key starts from when the resource does not
/// hold it, as Kafka's `prepareIncrementalConfigs` reads it from
/// `ConfigKey.defaultValue`. Only a `LIST` key reaches here, and every
/// topic key typed `LIST` is one Kafka 4.3.1 has.
fn list_default(name: &str) -> &'static str {
    config_keys::broker_dynamic::broker_key_row(name, UnstableApiVersions::Disabled)
        .and_then(|row| row.default)
        .unwrap_or("")
}

/// Kafka's `prepareIncrementalConfigs`: apply one resource's operations to its
/// stored dynamic configs.
fn apply_operations(
    resource: &AlterConfigsResource,
    props: &mut BTreeMap<String, String>,
    unstable: UnstableApiVersions,
) -> Result<(), (i16, String)> {
    for cfg in &resource.configs {
        let name = cfg.name.as_str();
        let value = cfg.value.as_deref().unwrap_or_default();
        match cfg.config_operation {
            OP_SET => {
                props.insert(name.to_owned(), value.to_owned());
            }
            OP_DELETE => {
                props.remove(name);
            }
            operation => {
                let verb = if operation == OP_APPEND {
                    "append"
                } else {
                    "subtract"
                };
                match broker_key_kind(name, unstable) {
                    BrokerKeyKind::Unknown => {
                        return Err((
                            codes::INVALID_CONFIG,
                            format!("Unknown config name: {name}"),
                        ));
                    }
                    BrokerKeyKind::Scalar => {
                        return Err((
                            codes::INVALID_CONFIG,
                            format!("Config value {verb} is not allowed for config key: {name}"),
                        ));
                    }
                    BrokerKeyKind::List => {}
                }
                let old = props
                    .get(name)
                    .map_or_else(|| list_default(name).to_owned(), Clone::clone);
                let mut parts: Vec<&str> = old.split(',').filter(|part| !part.is_empty()).collect();
                let items: Vec<&str> = value.split(',').collect();
                if operation == OP_APPEND {
                    for item in items {
                        if !parts.contains(&item) {
                            parts.push(item);
                        }
                    }
                } else {
                    parts.retain(|part| !items.contains(part));
                }
                props.insert(name.to_owned(), parts.join(","));
            }
        }
    }
    Ok(())
}

/// The records one `BROKER` resource stages, or the error it answers with.
/// `log_dirs` are this node's log directories, which a named-broker
/// `cordoned.log.dirs` must name, and are empty on a node without the broker
/// role.
fn broker_records(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id = context.resource_node(&resource.resource_name)?;
    if let Some(cfg) = resource.configs.iter().find(|cfg| {
        !matches!(
            cfg.config_operation,
            OP_SET | OP_DELETE | OP_APPEND | OP_SUBTRACT
        )
    }) {
        return Err((
            codes::INVALID_REQUEST,
            format!("Unknown operations type {}", cfg.config_operation),
        ));
    }
    for cfg in &resource.configs {
        // krabka's controller-published keys stand outside the alter paths.
        if config_keys::is_controller_managed_broker_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is controller-managed and read-only",
                    cfg.name
                ),
            ));
        }
        if node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
            && CLUSTER_DEFAULT_ONLY.contains(&cfg.name.as_str())
        {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    cfg.name
                ),
            ));
        }
    }
    let mut props: BTreeMap<String, String> = context
        .image
        .broker_config(node_id)
        .into_iter()
        .flatten()
        .filter(|(name, _)| !config_keys::is_controller_managed_broker_config(name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    apply_operations(resource, &mut props, context.unstable)?;
    let per_broker = node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
    // The records carry the client's strings, as Kafka's do: validation
    // parses a value and keeps nothing of the parse.
    validate_dynamic_broker_configs(&props, per_broker, context.unstable)?;
    if per_broker {
        cordoned_log_dirs_error(&props, context.log_dirs)?;
    }

    // Kafka writes a record for every key the request names, changed or not
    // (KAFKA-14136), and checks the ELR rules on each.
    let mut records = Vec::with_capacity(resource.configs.len());
    for cfg in &resource.configs {
        let value = props.get(&cfg.name).cloned();
        if let Some(error) = elr_min_isr_error(context.image, node_id, &cfg.name, value.as_deref())
        {
            return Err(error);
        }
        if let Some(error) = cordoned_log_dirs_disabled_error(context.image, &cfg.name) {
            return Err(error);
        }
        records.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: cfg.name.clone(),
            config_value: value,
        }));
    }
    Ok(records)
}

pub(super) fn handle_broker_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    serving: NodeId,
    (log_dirs, unstable): (&[std::path::PathBuf], UnstableApiVersions),
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    match broker_records(
        resource,
        crate::config_keys::broker_dynamic::BrokerConfigContext {
            image,
            serving,
            log_dirs,
            unstable,
        },
    ) {
        Ok(records) => to_submit.extend(records),
        Err((code, message)) => {
            out.error_code = code;
            out.error_message = Some(message);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;
    use crate::handlers::{
        incremental_alter_configs::test_support::{
            make_del_cfg, make_image_with_broker, make_resource, make_set_cfg,
        },
        test_support::{BrokerConfigMutationSetup, broker_config_context},
    };

    const SERVING: NodeId = NodeId(1);

    fn op(key: &str, operation: i8, value: &str) -> AlterableConfig {
        AlterableConfig {
            name: key.into(),
            config_operation: operation,
            value: Some(value.into()),
            ..Default::default()
        }
    }

    fn record(node: NodeId, key: &str, value: Option<&str>) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: node,
            config_name: key.into(),
            config_value: value.map(str::to_owned),
        })
    }

    fn image_with(configs: &[(NodeId, &str, &str)], elr: bool) -> MetadataImage {
        let mut image = make_image_with_broker(SERVING);
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                port: 9093,
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            },
        ));
        for (node, key, value) in configs {
            image.apply(&record(*node, key, Some(value)));
        }
        if elr {
            image.apply(&MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: crate::features::ELR_VERSION.into(),
                    level: 1,
                },
            ));
        }
        image
    }

    /// One row of the broker-resource table.
    type Case<'a> = (
        &'a str,
        MetadataImage,
        Vec<AlterableConfig>,
        Result<Vec<MetadataRecord>, (i16, &'a str)>,
    );

    fn check_broker_record_cases(cases: Vec<Case<'_>>, setup: BrokerConfigMutationSetup<'_>) {
        for (name, image, configs, want) in cases {
            let resource = make_resource(name, configs.clone());
            let got = broker_records(&resource, broker_config_context(&image, setup));
            let want = want.map_err(|(code, message)| (code, message.to_owned()));
            check!(got == want, "{name:?} {configs:?}");
        }
    }

    /// Each row is a resource name, the stored state, the operations, and the
    /// whole expected outcome: the staged records, or the error.
    #[test]
    fn broker_resources_follow_kafkas_dynamic_config_rules() {
        let cluster = krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
        let rate = crate::throttle::LEADER_THROTTLED_RATE_KEY;
        let min_isr = config_keys::MIN_INSYNC_REPLICAS;
        let cases: Vec<Case<'_>> = vec![
            (
                "1",
                image_with(&[], false),
                vec![make_set_cfg(rate, "2048")],
                Ok(vec![record(SERVING, rate, Some("2048"))]),
            ),
            (
                "2",
                image_with(&[], false),
                vec![make_set_cfg(rate, "2048")],
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received 2",
                )),
            ),
            (
                "x",
                image_with(&[], false),
                vec![make_set_cfg(rate, "2048")],
                Err((
                    codes::INVALID_REQUEST,
                    "Node id must be an integer, but it is: x",
                )),
            ),
            (
                "1",
                image_with(&[], false),
                vec![op(rate, 7, "1")],
                Err((codes::INVALID_REQUEST, "Unknown operations type 7")),
            ),
            (
                "",
                image_with(&[], false),
                vec![make_set_cfg("log.retention.ms", "86400000")],
                Ok(vec![record(cluster, "log.retention.ms", Some("86400000"))]),
            ),
            (
                "",
                image_with(&[], false),
                vec![make_set_cfg("num.io.threads", "16")],
                Ok(vec![record(cluster, "num.io.threads", Some("16"))]),
            ),
            (
                "1",
                image_with(&[], false),
                vec![make_set_cfg(
                    "listener.name.client.ssl.keystore.location",
                    "/k",
                )],
                Ok(vec![record(
                    SERVING,
                    "listener.name.client.ssl.keystore.location",
                    Some("/k"),
                )]),
            ),
            (
                "",
                image_with(&[], false),
                vec![make_set_cfg("log.dirs", "/tmp")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: [log.dirs]",
                )),
            ),
            // `kafka-configs --entity-type brokers --entity-default
            // --add-config auto.leader.rebalance.enable=false` is refused, as
            // is every other `KafkaConfig` key that is not dynamic.
            (
                "",
                image_with(&[], false),
                vec![make_set_cfg("auto.leader.rebalance.enable", "false")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: [auto.leader.rebalance.enable]",
                )),
            ),
            (
                "",
                image_with(&[], false),
                vec![make_set_cfg("max.connections", "-5")],
                Err((
                    codes::INVALID_REQUEST,
                    "Invalid value -5 for configuration max.connections: Value must be at least 0",
                )),
            ),
            (
                "",
                image_with(&[], false),
                vec![op("metric.reporters", OP_APPEND, "com.example.Reporter")],
                Ok(vec![record(
                    krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    "metric.reporters",
                    Some("org.apache.kafka.common.metrics.JmxReporter,com.example.Reporter"),
                )]),
            ),
            (
                "1",
                image_with(&[], false),
                vec![make_set_cfg(rate, "-5")],
                Err((
                    codes::INVALID_REQUEST,
                    "Invalid value -5 for configuration leader.replication.throttled.rate: \
                     Value must be at least 0",
                )),
            ),
            (
                "1",
                image_with(&[], false),
                vec![make_set_cfg(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "true",
                )],
                Ok(vec![record(
                    SERVING,
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    Some("true"),
                )]),
            ),
            (
                "1",
                image_with(&[], false),
                vec![make_set_cfg(min_isr, "2")],
                Ok(vec![record(SERVING, min_isr, Some("2"))]),
            ),
            (
                "1",
                image_with(&[], true),
                vec![make_set_cfg(min_isr, "2")],
                Err((
                    codes::INVALID_CONFIG,
                    "Broker-level min.insync.replicas cannot be altered while ELR is enabled.",
                )),
            ),
            (
                "",
                image_with(&[], true),
                vec![make_del_cfg(min_isr)],
                Err((
                    codes::INVALID_CONFIG,
                    "Cluster-level min.insync.replicas cannot be removed while ELR is enabled.",
                )),
            ),
            (
                "1",
                image_with(&[], false),
                vec![op(rate, OP_APPEND, "1")],
                Err((
                    codes::INVALID_CONFIG,
                    "Config value append is not allowed for config key: \
                     leader.replication.throttled.rate",
                )),
            ),
            (
                "1",
                image_with(&[], false),
                vec![op("not.a.kafka.key", OP_SUBTRACT, "1")],
                Err((
                    codes::INVALID_CONFIG,
                    "Unknown config name: not.a.kafka.key",
                )),
            ),
            (
                "",
                image_with(&[], false),
                vec![op("log.cleanup.policy", OP_APPEND, "compact")],
                Ok(vec![record(
                    cluster,
                    "log.cleanup.policy",
                    Some("delete,compact"),
                )]),
            ),
            (
                "",
                image_with(&[(cluster, "log.cleanup.policy", "compact,delete")], false),
                vec![op("log.cleanup.policy", OP_SUBTRACT, "delete")],
                Ok(vec![record(cluster, "log.cleanup.policy", Some("compact"))]),
            ),
        ];
        check_broker_record_cases(cases, BrokerConfigMutationSetup::default());
    }

    /// Kafka stores the string the client sent
    /// (`ConfigurationControlManager.incrementalAlterConfigResource` takes the
    /// operation's value as it is, and `DynamicConfig.Broker.validate` drops
    /// the parse), so a describe returns what the alter wrote and a tool that
    /// diffs the two does not loop: a `DOUBLE` that `Double.toString` prints as
    /// `1.0`, a `LONG` it prints as `1.048576E8`, a padded `INT`, and a list
    /// with spaces are all kept as they came.
    #[test]
    fn a_broker_resource_stores_the_string_the_client_sent() {
        let cluster = krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
        let sent = [
            ("log.cleaner.min.cleanable.ratio", "1"),
            ("log.cleaner.io.max.bytes.per.second", "104857600"),
            ("num.io.threads", " 16 "),
            ("log.cleanup.policy", "compact , delete"),
        ];
        let resource = make_resource(
            "",
            sent.iter()
                .map(|(key, value)| make_set_cfg(key, value))
                .collect(),
        );

        let records = broker_records(
            &resource,
            broker_config_context(
                &image_with(&[], false),
                BrokerConfigMutationSetup::default(),
            ),
        );

        check!(
            records
                == Ok(sent
                    .iter()
                    .map(|(key, value)| record(cluster, key, Some(value)))
                    .collect())
        );
    }

    #[test]
    fn controller_managed_broker_configs_are_rejected_as_read_only() {
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for cfg in [make_set_cfg(key, "true"), make_del_cfg(key)] {
                let resource = make_resource("1", vec![cfg]);
                check!(
                    broker_records(
                        &resource,
                        broker_config_context(
                            &image_with(&[], false),
                            BrokerConfigMutationSetup::default()
                        )
                    ) == Err((
                        codes::INVALID_CONFIG,
                        format!("broker config {key} is controller-managed and read-only"),
                    )),
                    "key {key}"
                );
            }
        }
    }

    /// KIP-1066 `cordoned.log.dirs`, with each refusal a live
    /// `apache/kafka:4.3.1` gave `kafka-configs --alter` on a broker with
    /// `log.dirs=/tmp/d1,/tmp/d2`: a per-broker key, a `LIST` that APPEND and
    /// SUBTRACT act on, a value checked against this node's `log.dirs`, and a
    /// write the controller refuses below `metadata.version` `4.3-IV0`.
    #[test]
    fn cordoned_log_dirs_follows_kafkas_dynamic_config_rules() {
        const KEY: &str = crate::cordoned_log_dirs::CORDONED_LOG_DIRS;
        let log_dirs = [
            std::path::PathBuf::from("/tmp/d1"),
            std::path::PathBuf::from("/tmp/d2"),
        ];
        let at_level = |level: i16, configs: &[(NodeId, &str, &str)]| {
            let mut image = image_with(configs, false);
            image.apply(&MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: crate::features::METADATA_VERSION.into(),
                    level,
                },
            ));
            image
        };
        let disabled = (
            codes::INVALID_CONFIG,
            "The cordoned.log.dirs configuration value cannot be set because it requires \
             metadata.version >= 4.3-IV0",
        );
        let cases: Vec<Case<'_>> = vec![
            (
                "1",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "/tmp/d1")],
                Ok(vec![record(SERVING, KEY, Some("/tmp/d1"))]),
            ),
            (
                "1",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "*")],
                Ok(vec![record(SERVING, KEY, Some("*"))]),
            ),
            (
                "1",
                at_level(30, &[(SERVING, KEY, "/tmp/d1")]),
                vec![op(KEY, OP_APPEND, "/tmp/d2")],
                Ok(vec![record(SERVING, KEY, Some("/tmp/d1,/tmp/d2"))]),
            ),
            (
                "1",
                at_level(30, &[(SERVING, KEY, "/tmp/d1,/tmp/d2")]),
                vec![op(KEY, OP_SUBTRACT, "/tmp/d1")],
                Ok(vec![record(SERVING, KEY, Some("/tmp/d2"))]),
            ),
            (
                "1",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "/tmp/nope")],
                Err((
                    codes::INVALID_REQUEST,
                    "requirement failed: All entries in cordoned.log.dirs must be present in \
                     log.dirs or log.dir. Missing entries : /tmp/nope",
                )),
            ),
            (
                "1",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "*,/tmp/d1")],
                Err((
                    codes::INVALID_REQUEST,
                    "requirement failed: When cordoned.log.dirs is set to *, it must not \
                     contain other values",
                )),
            ),
            (
                "1",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "/tmp/d1,")],
                Err((
                    codes::INVALID_REQUEST,
                    "Configuration 'cordoned.log.dirs' values must not be empty.",
                )),
            ),
            (
                "",
                at_level(30, &[]),
                vec![make_set_cfg(KEY, "/tmp/d1")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs at default cluster level, broker id must be \
                     specified: [cordoned.log.dirs]",
                )),
            ),
            (
                "1",
                at_level(29, &[]),
                vec![make_set_cfg(KEY, "")],
                Err(disabled),
            ),
            (
                "1",
                at_level(29, &[]),
                vec![make_del_cfg(KEY)],
                Err(disabled),
            ),
        ];
        check_broker_record_cases(
            cases,
            BrokerConfigMutationSetup {
                log_dirs: &log_dirs,
                ..Default::default()
            },
        );
    }
}
