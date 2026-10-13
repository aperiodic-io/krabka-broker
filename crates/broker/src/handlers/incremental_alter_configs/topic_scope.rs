//! Topic-scoped resources for `IncrementalAlterConfigs`. The handler merges
//! the per-key operations onto the topic's current override map, validates
//! the resulting map, and returns the `V1TopicConfig` record that carries it.
//!
//! The order is Kafka's `ConfigurationControlManager`: the operations merge
//! first (an APPEND or SUBTRACT of a key that is not a `LIST` is refused
//! there), then `ControllerConfigurationValidator` checks the topic name and
//! the merged map, and only then does the existence check run.
//!
//! `krabka.diskless` is fixed when the topic is created, so the merged map is
//! also compared against the topic's current one: a SET that restates the
//! value the topic already has is a no-op and passes, and any op that would
//! move a partition between the diskless WAL runtime and the local-log
//! runtime is refused. See
//! [`crate::config_keys::validate_diskless_unchanged`].

use {
    super::{OP_APPEND, OP_DELETE, OP_SET},
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::{
            self,
            registry::{self, ConfigScope, ConfigType},
        },
        topic_policy::{TopicPolicy, check as check_topic_policy},
    },
    krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord},
    krabka_protocol::owned::incremental_alter_configs_request::AlterConfigsResource,
};

/// Kafka's `ConfigurationControlManager` APPEND and SUBTRACT: a `LIST` key's
/// current value (or its default) split on commas, with each item of the
/// operation's value added when absent or removed.
pub(in crate::handlers::incremental_alter_configs) fn merge_list_op(
    operation: i8,
    current: Option<&str>,
    default: Option<&str>,
    value: &str,
) -> String {
    let base = current.or(default).unwrap_or_default();
    let mut parts: Vec<&str> = base.split(',').filter(|part| !part.is_empty()).collect();
    // Java's `String.split` drops trailing empty strings.
    let items: Vec<&str> = value.trim_end_matches(',').split(',').collect();
    for item in items {
        if operation == OP_APPEND {
            if !parts.contains(&item) {
                parts.push(item);
            }
        } else if let Some(at) = parts.iter().position(|part| *part == item) {
            parts.remove(at);
        }
    }
    parts.join(",")
}

/// Kafka's refusal of an APPEND or SUBTRACT on a key that is not a `LIST`.
pub(in crate::handlers::incremental_alter_configs) fn not_a_list(
    operation: i8,
    key: &str,
) -> (i16, String) {
    let verb = if operation == OP_APPEND {
        "APPEND"
    } else {
        "SUBTRACT"
    };
    (
        codes::INVALID_CONFIG,
        format!("Can't {verb} to key {key} because its type is not LIST."),
    )
}

pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    policy: &TopicPolicy,
    remote_storage_system_enabled: bool,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    let topic = resource.resource_name.as_str();
    let current = image.topic_config(topic);
    let mut merged = current.cloned().unwrap_or_default();
    for config in &resource.configs {
        // A controller-managed key is refused before the operation is read.
        // A DELETE of it is an attempt to clear the freeze, so every
        // operation gets the same refusal.
        if config_keys::is_controller_managed_topic_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&config.name),
            ));
        }
        let value = config.value.as_deref().unwrap_or_default();
        match config.config_operation {
            OP_SET => {
                merged.insert(config.name.clone(), value.to_owned());
            }
            // A DELETE of a key the topic does not hold changes nothing, and
            // Kafka writes no record for it, whatever the key is.
            OP_DELETE => {
                merged.remove(&config.name);
            }
            operation => {
                let row = registry::lookup(ConfigScope::Topic, &config.name)
                    .filter(|row| row.is_alterable() && row.config_type == ConfigType::List)
                    .ok_or_else(|| not_a_list(operation, &config.name))?;
                let next = merge_list_op(
                    operation,
                    merged.get(&config.name).map(String::as_str),
                    row.default,
                    value,
                );
                merged.insert(config.name.clone(), next);
            }
        }
    }
    // `ControllerConfigurationValidator.validateTopicName`, then the map.
    if topic.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default topic resources are not allowed.".into(),
        ));
    }
    if let Err(invalid) = krabka_log::topic_name::validate_topic_name(topic) {
        return Err((codes::INVALID_TOPIC_EXCEPTION, invalid.to_string()));
    }
    let merged = config_keys::canonical_topic_config_map(
        &merged,
        &config_keys::TopicDefaults::from_image(image, unstable),
        remote_storage_system_enabled,
        unstable,
    )
    .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    config_keys::validate_diskless_unchanged(current, &merged)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    config_keys::validate_remote_storage_disable(current, &merged)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    if image.topic(topic).is_none() {
        return Err((
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            format!("The topic '{topic}' does not exist."),
        ));
    }
    // KIP-133: the operator-declared policy, on the map the topic ends up
    // with. Kafka calls `AlterConfigPolicy.validate` on the same resolved map,
    // and its `RequestMetadata` carries no partition count and no replication
    // factor, so neither is passed here. It runs after the built-in
    // validators, as `ConfigAdminManager` does: a config the broker itself
    // refuses never reaches the policy.
    check_topic_policy(policy, topic, None, None, &merged)
        .map_err(|reason| (codes::POLICY_VIOLATION, reason))?;
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides: merged,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::{
                incremental_alter_configs::test_support::{
                    AlterableConfig, assert, check, image_with_topic_config, make_del_cfg,
                    make_set_cfg, make_topic_resource,
                },
                test_support::check_config_result,
            },
            test_support::string_pairs,
            throttle::LEADER_THROTTLED_REPLICAS_KEY,
        },
    };

    /// The record builder under the empty policy, which is the state of a
    /// broker with no `[topic_policy]` section. It shadows the glob-imported
    /// name so the cases below read as the two-argument call they were
    /// written as; the policy's own cases pass a real one to
    /// [`super::topic_config_record`].
    fn topic_config_record(
        resource: &AlterConfigsResource,
        image: &MetadataImage,
    ) -> Result<MetadataRecord, (i16, String)> {
        super::topic_config_record(
            resource,
            image,
            &TopicPolicy::default(),
            true,
            UnstableApiVersions::Disabled,
        )
    }

    #[test]
    fn topic_throttle_config_value_validated() {
        // Verify ThrottledReplicas::parse rejects malformed input that
        // the validator delegates to.
        assert!(crate::throttle::ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(crate::throttle::ThrottledReplicas::parse("0:bad").is_err());
    }

    #[test]
    fn controller_managed_topic_configs_are_rejected_whatever_the_operation() {
        let img = image_with_topic_config("orders", &[(config_keys::RETENTION_MS, "60000")]);

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS {
            for (label, config) in [
                ("a SET of the key", make_set_cfg(key, "true")),
                (
                    "a DELETE of the key, which asks to clear the freeze",
                    make_del_cfg(key),
                ),
                ("an APPEND of the key", AlterableConfig {
                    name: key.into(),
                    config_operation: 2,
                    value: Some("true".into()),
                    ..Default::default()
                }),
                ("a SUBTRACT of the key", AlterableConfig {
                    name: key.into(),
                    config_operation: 3,
                    value: Some("true".into()),
                    ..Default::default()
                }),
            ] {
                let error = topic_config_record(&make_topic_resource("orders", vec![config]), &img)
                    .expect_err("controller-managed key must be rejected");

                check!(error.0 == codes::INVALID_CONFIG, "{label}, key {key}");
                check!(
                    error.1 == config_keys::controller_managed_topic_config_message(key),
                    "{label}, key {key}"
                );
                check!(!error.1.is_empty(), "{label}, key {key}");
            }
        }
    }

    #[test]
    fn an_ordinary_topic_config_still_merges_onto_the_existing_map() {
        let img = image_with_topic_config("orders", &[(config_keys::RETENTION_MS, "60000")]);

        let record = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::SEGMENT_BYTES,
                "1048576",
            )]),
            &img,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_string() => "60000".to_string(),
            config_keys::SEGMENT_BYTES.to_string() => "1048576".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn compaction_op_conflicts_with_scheduled_delivery_already_on_the_topic() {
        // The ops alone are legal. Only the merge with the topic's existing
        // config shows the conflict.
        let img = image_with_topic_config("orders", &[(config_keys::DELIVERY_MODE, "scheduled")]);

        let (code, message) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::CLEANUP_POLICY,
                "compact",
            )]),
            &img,
        )
        .expect_err("compaction merged onto a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(
            message.contains(config_keys::CLEANUP_POLICY),
            "got: {message}"
        );
        assert!(
            message.contains(config_keys::DELIVERY_MODE),
            "got: {message}"
        );
    }

    #[test]
    fn scheduled_delivery_op_conflicts_with_compaction_already_on_the_topic() {
        let img = image_with_topic_config("orders", &[(config_keys::CLEANUP_POLICY, "compact")]);

        let (code, _) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::DELIVERY_MODE,
                "scheduled",
            )]),
            &img,
        )
        .expect_err("scheduling a compacted topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
    }

    #[test]
    fn deleting_the_delivery_mode_clears_the_conflict_in_the_same_request() {
        let img = image_with_topic_config("orders", &[(config_keys::DELIVERY_MODE, "scheduled")]);

        let record = topic_config_record(
            &make_topic_resource("orders", vec![
                make_del_cfg(config_keys::DELIVERY_MODE),
                make_set_cfg(config_keys::CLEANUP_POLICY, "compact"),
            ]),
            &img,
        )
        .expect("removing the schedule leaves a plain compacted topic");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {config_keys::CLEANUP_POLICY.to_string() => "compact".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn no_operation_can_move_a_topic_between_data_paths() {
        /// The topic's stored overrides, the ops the request carries, and
        /// whether the merged map is accepted.
        type DisklessOps<'a> = (
            &'a str,
            &'a [(&'a str, &'a str)],
            Vec<AlterableConfig>,
            bool,
        );

        let cases: [DisklessOps<'_>; 5] = [
            (
                "a SET that restates the value the topic already has",
                &[(config_keys::DISKLESS, "true")],
                vec![make_set_cfg(config_keys::DISKLESS, "true")],
                true,
            ),
            (
                "an ordinary SET alongside the untouched flag",
                &[(config_keys::DISKLESS, "true")],
                vec![make_set_cfg(config_keys::RETENTION_MS, "60000")],
                true,
            ),
            (
                "a SET that turns the flag off",
                &[(config_keys::DISKLESS, "true")],
                vec![make_set_cfg(config_keys::DISKLESS, "false")],
                false,
            ),
            (
                "a DELETE of the flag",
                &[(config_keys::DISKLESS, "true")],
                vec![make_del_cfg(config_keys::DISKLESS)],
                false,
            ),
            (
                "a SET that turns the flag on for a local-log topic",
                &[(config_keys::RETENTION_MS, "60000")],
                vec![make_set_cfg(config_keys::DISKLESS, "true")],
                false,
            ),
        ];

        for (label, stored, ops, want_ok) in cases {
            let img = image_with_topic_config("orders", stored);

            let result = topic_config_record(&make_topic_resource("orders", ops), &img);

            check_config_result(result, want_ok, label, config_keys::DISKLESS);
        }
    }

    #[test]
    fn tiered_storage_cannot_be_turned_on_for_a_diskless_topic() {
        let img = image_with_topic_config("orders", &[(config_keys::DISKLESS, "true")]);

        let (code, message) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                "remote.storage.enable",
                "true",
            )]),
            &img,
        )
        .expect_err("two object-store data paths on one topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(message.contains(config_keys::DISKLESS), "got: {message}");
        assert!(message.contains("remote.storage.enable"), "got: {message}");
    }

    #[test]
    fn scheduled_delivery_keys_merge_onto_an_existing_topic_config() {
        let img = image_with_topic_config("retries", &[(config_keys::RETENTION_MS, "60000")]);

        let record = topic_config_record(
            &make_topic_resource("retries", vec![
                make_set_cfg(config_keys::DELIVERY_MODE, "scheduled"),
                make_set_cfg(config_keys::DELIVERY_MAX_DELAY_MS, "3600000"),
                make_set_cfg(config_keys::DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ]),
            &img,
        )
        .expect("valid scheduled delivery ops");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "retries".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_string() => "60000".to_string(),
            config_keys::DELIVERY_MODE.to_string() => "scheduled".to_string(),
            config_keys::DELIVERY_MAX_DELAY_MS.to_string() => "3600000".to_string(),
            config_keys::DELIVERY_SCHEDULE_MONOTONIC.to_string() => "true".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn a_merged_map_that_breaks_the_policy_is_a_policy_violation() {
        let img = image_with_topic_config("orders", &[(config_keys::MIN_INSYNC_REPLICAS, "2")]);
        let policy = TopicPolicy {
            min_insync_replicas: Some(2),
            ..TopicPolicy::default()
        };

        let (code, message) = super::topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::MIN_INSYNC_REPLICAS,
                "1",
            )]),
            &img,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect_err("a merged map below the policy floor must be refused");

        check!(code == codes::POLICY_VIOLATION);
        check!(
            message.contains(config_keys::MIN_INSYNC_REPLICAS),
            "{message}"
        );
    }

    #[test]
    fn a_merged_map_that_satisfies_the_policy_still_commits() {
        let img = image_with_topic_config("orders", &[(config_keys::MIN_INSYNC_REPLICAS, "1")]);
        let policy = TopicPolicy {
            min_insync_replicas: Some(2),
            ..TopicPolicy::default()
        };

        let record = super::topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::MIN_INSYNC_REPLICAS,
                "2",
            )]),
            &img,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect("a merge that lifts the topic to the floor is accepted");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::MIN_INSYNC_REPLICAS.to_string() => "2".to_string()},
        });
        check!(record == expected);
    }

    /// Kafka's `ConfigurationControlManager` merge and
    /// `ControllerConfigurationValidator` order for a `TOPIC` resource. Each
    /// row is the stored overrides, the operations, and the whole outcome.
    #[test]
    fn topic_operations_follow_kafkas_merge_rules() {
        let op = |key: &str, operation: i8, value: &str| AlterableConfig {
            name: key.into(),
            config_operation: operation,
            value: Some(value.into()),
            ..Default::default()
        };
        let leader = LEADER_THROTTLED_REPLICAS_KEY;
        let record = |pairs: &[(&str, &str)]| {
            Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".into(),
                overrides: string_pairs(pairs),
            }))
        };
        let cases = [
            (
                "orders",
                vec![],
                vec![make_del_cfg("not.a.topic.config")],
                record(&[]),
            ),
            (
                "orders",
                vec![],
                vec![op(config_keys::CLEANUP_POLICY, OP_APPEND, "compact")],
                record(&[(config_keys::CLEANUP_POLICY, "delete,compact")]),
            ),
            (
                "orders",
                vec![(config_keys::CLEANUP_POLICY, "compact,delete")],
                vec![op(config_keys::CLEANUP_POLICY, 3, "delete")],
                record(&[(config_keys::CLEANUP_POLICY, "compact")]),
            ),
            (
                "orders",
                vec![(leader, "0:1")],
                vec![op(leader, OP_APPEND, "1:2")],
                record(&[(leader, "0:1,1:2")]),
            ),
            (
                "orders",
                vec![(leader, "0:1")],
                vec![op(leader, OP_APPEND, "0:1")],
                record(&[(leader, "0:1")]),
            ),
            (
                "orders",
                vec![],
                vec![op(config_keys::RETENTION_MS, OP_APPEND, "1")],
                Err((
                    codes::INVALID_CONFIG,
                    "Can't APPEND to key retention.ms because its type is not LIST.".to_owned(),
                )),
            ),
            (
                "",
                vec![],
                vec![make_set_cfg(config_keys::RETENTION_MS, "1000")],
                Err((
                    codes::INVALID_REQUEST,
                    "Default topic resources are not allowed.".to_owned(),
                )),
            ),
            (
                "a/b",
                vec![],
                vec![make_set_cfg(config_keys::RETENTION_MS, "1000")],
                Err((
                    codes::INVALID_TOPIC_EXCEPTION,
                    "Topic name is invalid: 'a/b' contains one or more characters other than \
                     ASCII alphanumerics, '.', '_' and '-'"
                        .to_owned(),
                )),
            ),
            (
                "missing",
                vec![],
                vec![make_set_cfg(config_keys::RETENTION_MS, "1000")],
                Err((
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "The topic 'missing' does not exist.".to_owned(),
                )),
            ),
        ];
        for (name, stored, ops, want) in cases {
            let image = image_with_topic_config("orders", &stored);
            check!(
                topic_config_record(&make_topic_resource(name, ops.clone()), &image) == want,
                "{name:?} {stored:?} {ops:?}"
            );
        }
    }
}
