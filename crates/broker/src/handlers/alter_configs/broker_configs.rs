//! The per-key broker config records an `AlterConfigs` broker resource
//! becomes, including the tombstones for overrides the replacement omits.
//!
//! Kafka's `AlterConfigs` is a full replacement, so this module both writes
//! the requested keys and deletes the ones the request left out. Keys that
//! only the controller writes stand outside that replacement: naming one is an
//! error, and the tombstone sweep skips them.
//!
//! The dynamic-config rules are Kafka's `DynamicBrokerConfig.validateConfigs`,
//! shared with `IncrementalAlterConfigs` in
//! [`crate::config_keys::broker_dynamic`].

use std::collections::BTreeMap;

use krabka_metadata::{BrokerConfigRecord, MetadataRecord};
use krabka_protocol::owned::alter_configs_request::AlterConfigsResource;

use crate::{
    codes,
    config_keys::{
        self,
        broker_dynamic::{
            BrokerConfigContext, CLUSTER_DEFAULT_ONLY, cordoned_log_dirs_disabled_error,
            cordoned_log_dirs_error, elr_min_isr_error, validate_dynamic_broker_configs,
        },
    },
    cordoned_log_dirs::CORDONED_LOG_DIRS,
};

/// Check broker identity and directory membership on the client-addressed node.
pub(super) fn preprocess_broker_resource(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
) -> Result<(), (i16, String)> {
    let node = context.resource_node(&resource.resource_name)?;
    if node != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID {
        let values = resource
            .configs
            .iter()
            .filter(|c| c.name == CORDONED_LOG_DIRS)
            .map(|c| (c.name.clone(), c.value.clone().unwrap_or_default()))
            .collect();
        cordoned_log_dirs_error(&values, context.log_dirs)?;
    }
    Ok(())
}

/// The records one `AlterConfigs` `BROKER` resource stages. `log_dirs` are
/// this node's log directories, which a named-broker `cordoned.log.dirs` must
/// name, and are empty on a node without the broker role.
pub(super) fn broker_config_records(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id = context.resource_node(&resource.resource_name)?;
    let per_broker = node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
    let mut replacement = BTreeMap::new();
    for config in &resource.configs {
        if config_keys::is_controller_managed_broker_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is controller-managed and read-only",
                    config.name
                ),
            ));
        }
        if per_broker && CLUSTER_DEFAULT_ONLY.contains(&config.name.as_str()) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    config.name
                ),
            ));
        }
        // `ConfigAdminManager.preprocess` has already refused a null value.
        replacement.insert(
            config.name.clone(),
            config.value.clone().unwrap_or_default(),
        );
    }
    // The records carry the client's strings, as Kafka's do: validation
    // parses a value and keeps nothing of the parse.
    validate_dynamic_broker_configs(&replacement, per_broker, context.unstable)?;
    if per_broker {
        cordoned_log_dirs_error(&replacement, context.log_dirs)?;
    }

    let current = context.image.broker_config(node_id);
    let deleted: Vec<&String> = current
        .into_iter()
        .flat_map(BTreeMap::keys)
        .filter(|name| {
            !replacement.contains_key(*name)
                && !config_keys::is_controller_managed_broker_config(name)
        })
        .collect();
    // Kafka's `ConfigurationControlManager` checks the ELR rules on every
    // record, the written ones and the implicit deletions.
    for (name, value) in replacement
        .iter()
        .map(|(name, value)| (name, Some(value.as_str())))
        .chain(deleted.iter().map(|name| (*name, None)))
    {
        if let Some(error) = elr_min_isr_error(context.image, node_id, name, value) {
            return Err(error);
        }
        if let Some(error) = cordoned_log_dirs_disabled_error(context.image, name) {
            return Err(error);
        }
    }
    let mut records = Vec::with_capacity(replacement.len() + deleted.len());
    records.extend(replacement.iter().map(|(name, value)| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: Some(value.clone()),
        })
    }));
    records.extend(deleted.into_iter().map(|name| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: None,
        })
    }));
    Ok(records)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::NodeId;

    use super::*;
    use crate::{
        config_keys::{BROKER_WITNESS, UNCLEAN_RECOVERY_STRATEGY, WITNESS_TRUE},
        handlers::{
            alter_configs::test_support::{broker_resource, image_with_broker},
            test_support::{BrokerConfigMutationSetup, broker_config_context},
        },
        test_support::{broker_registration, finalize_elr_version},
        throttle::{FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY},
    };

    const SERVING: NodeId = NodeId(1);

    fn record(node_id: NodeId, name: &str, value: Option<&str>) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.into(),
            config_value: value.map(str::to_owned),
        })
    }

    /// Replace this serving broker's dynamic config with stable APIs enabled.
    fn replacement_records(
        image: &krabka_metadata::MetadataImage,
        name: &str,
        configs: &[(&str, &str)],
    ) -> Result<Vec<MetadataRecord>, (i16, String)> {
        broker_config_records(
            &broker_resource(name, configs),
            broker_config_context(image, BrokerConfigMutationSetup::default()),
        )
    }

    fn replace_follower_throttle_with_leader(
        image: &mut krabka_metadata::MetadataImage,
    ) -> Vec<MetadataRecord> {
        image.apply(&record(SERVING, FOLLOWER_THROTTLED_RATE_KEY, Some("512")));

        replacement_records(image, "1", &[(LEADER_THROTTLED_RATE_KEY, "2048")])
            .expect("valid broker replacement")
    }

    #[test]
    fn broker_full_replacement_sets_requested_and_deletes_omitted_configs() {
        let mut image = image_with_broker(1);
        image.apply(&record(SERVING, LEADER_THROTTLED_RATE_KEY, Some("1024")));
        let records = replace_follower_throttle_with_leader(&mut image);

        let expected = vec![
            record(
                SERVING,
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                Some("2048"),
            ),
            record(SERVING, crate::throttle::FOLLOWER_THROTTLED_RATE_KEY, None),
        ];
        assert!(records == expected);
    }

    /// One row of the broker-resource table: the name, the configs, and the
    /// outcome.
    type Case<'a> = (
        &'a str,
        Vec<(&'a str, &'a str)>,
        Result<Vec<MetadataRecord>, (i16, &'a str)>,
    );

    /// Kafka's `ConfigAdminManager` and `DynamicBrokerConfig` rules for a
    /// `BROKER` resource, served by node 1 with node 2 also registered. Each
    /// row is the resource name, the configs, and the whole expected outcome.
    #[test]
    fn broker_resources_follow_kafkas_name_and_dynamic_config_rules() {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                port: 9093,
                ..broker_registration(krabka_raft::NodeId(2))
            },
        ));
        let cluster = krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
        let rate = LEADER_THROTTLED_RATE_KEY;
        let cases: Vec<Case<'_>> = vec![
            (
                "2",
                vec![(rate, "1")],
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received 2",
                )),
            ),
            (
                "7",
                vec![(rate, "1")],
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received 7",
                )),
            ),
            (
                "one",
                vec![(rate, "1")],
                Err((
                    codes::INVALID_REQUEST,
                    "Node id must be an integer, but it is: one",
                )),
            ),
            (
                "",
                vec![("log.retention.ms", "86400000"), ("plugin.key", "x")],
                Ok(vec![
                    record(cluster, "log.retention.ms", Some("86400000")),
                    record(cluster, "plugin.key", Some("x")),
                ]),
            ),
            (
                "1",
                vec![(config_keys::MIN_INSYNC_REPLICAS, "2")],
                Ok(vec![record(
                    SERVING,
                    config_keys::MIN_INSYNC_REPLICAS,
                    Some("2"),
                )]),
            ),
            (
                "",
                vec![("log.dirs", "/tmp")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: [log.dirs]",
                )),
            ),
            // Every `KafkaConfig` key that is not dynamic is refused, not
            // only the ones krabka reads at startup.
            (
                "",
                vec![("auto.leader.rebalance.enable", "false")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: [auto.leader.rebalance.enable]",
                )),
            ),
            (
                "1",
                vec![("sasl.server.max.receive.size", "1048576")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: [sasl.server.max.receive.size]",
                )),
            ),
            (
                "",
                vec![("connection.failed.authentication.delay.ms", "0")],
                Err((
                    codes::INVALID_REQUEST,
                    "Cannot update these configs dynamically: \
                     [connection.failed.authentication.delay.ms]",
                )),
            ),
            // A dynamic key is parsed against its `ConfigDef`.
            (
                "1",
                vec![("num.io.threads", "abc")],
                Err((
                    codes::INVALID_REQUEST,
                    "Invalid value abc for configuration num.io.threads: Not a number of type INT",
                )),
            ),
            // The record carries the string the client sent, as Kafka's does
            // (`ConfigurationControlManager` stores the value it is given and
            // `DynamicConfig.Broker.validate` drops the parse): a padded
            // `INT`, a `DOUBLE` that prints as `1.0`, a `LONG` that
            // `Double.toString` would print as `1.048576E8`, a spaced list.
            (
                "",
                vec![
                    ("max.connections", " 100 "),
                    ("log.cleaner.min.cleanable.ratio", "1"),
                    ("log.cleaner.io.max.bytes.per.second", "104857600"),
                    ("log.cleanup.policy", "compact , delete"),
                ],
                Ok(vec![
                    record(
                        cluster,
                        "log.cleaner.io.max.bytes.per.second",
                        Some("104857600"),
                    ),
                    record(cluster, "log.cleaner.min.cleanable.ratio", Some("1")),
                    record(cluster, "log.cleanup.policy", Some("compact , delete")),
                    record(cluster, "max.connections", Some(" 100 ")),
                ]),
            ),
        ];
        for (name, configs, want) in cases {
            let got = replacement_records(&image, name, &configs);
            let want = want.map_err(|(code, message)| (code, message.to_owned()));
            check!(got == want, "{name:?} {configs:?}");
        }
    }

    #[test]
    fn broker_full_replacement_rejects_controller_managed_configs() {
        let image = image_with_broker(1);
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for resource_name in ["1", ""] {
                check!(
                    replacement_records(&image, resource_name, &[(key, "true")])
                        == Err((
                            codes::INVALID_CONFIG,
                            format!("broker config {key} is controller-managed and read-only"),
                        )),
                    "key {key}"
                );
            }
        }
    }

    #[test]
    fn broker_full_replacement_leaves_controller_managed_configs_alone() {
        let mut image = image_with_broker(1);
        image.apply(&record(SERVING, BROKER_WITNESS, Some(WITNESS_TRUE)));
        let records = replace_follower_throttle_with_leader(&mut image);

        assert!(
            records
                == vec![
                    record(
                        SERVING,
                        crate::throttle::LEADER_THROTTLED_RATE_KEY,
                        Some("2048")
                    ),
                    record(SERVING, crate::throttle::FOLLOWER_THROTTLED_RATE_KEY, None),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_per_broker_recovery_setting() {
        let image = image_with_broker(1);

        let error = replacement_records(&image, "1", &[(UNCLEAN_RECOVERY_STRATEGY, "Balanced")])
            .expect_err("per-broker recovery setting must be rejected");

        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn elr_requires_the_cluster_min_isr_override_to_survive_replacement() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        finalize_elr_version(&mut image);
        image.apply(&record(
            krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_keys::MIN_INSYNC_REPLICAS,
            Some("2"),
        ));

        let error = replacement_records(&image, "", &[]);

        assert!(
            error
                == Err((
                    codes::INVALID_CONFIG,
                    "Cluster-level min.insync.replicas cannot be removed while ELR is enabled."
                        .to_owned()
                ))
        );
    }
}
