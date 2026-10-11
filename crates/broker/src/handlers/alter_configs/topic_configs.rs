//! The authoritative topic override record an `AlterConfigs` topic resource
//! becomes.
//!
//! `AlterConfigs` replaces a topic's whole override map, so the per-key
//! validation and the cross-key combination rules both apply to the map this
//! module builds, and neither reads the topic's current overrides.
//!
//! Two rules do read them. `krabka.diskless` is fixed when the topic is
//! created, and a replacement that simply omits it would otherwise turn a
//! diskless topic back into a local-log one, so the resulting map is compared
//! against the stored one. See
//! [`crate::config_keys::validate_diskless_unchanged`].
//!
//! ELR is partition metadata, as it is in Kafka, so config replacement cannot
//! erase it.

use krabka_metadata::{MetadataRecord, TopicConfigRecord};
use krabka_protocol::owned::alter_configs_request::AlterConfigsResource;

use crate::{
    api_catalog::UnstableApiVersions,
    codes, config_keys,
    topic_policy::{TopicPolicy, check},
};

/// Build the authoritative `V1TopicConfig` record for a topic resource. The
/// request carries the *complete* set of non-default values, so the map this
/// builds is the whole override map and the cross-key rules apply to it.
pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
    policy: &TopicPolicy,
    remote_storage_system_enabled: bool,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    // `ControllerConfigurationValidator.validateTopicName` runs before the
    // configs are checked, and the existence check runs after them.
    if resource.resource_name.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default topic resources are not allowed.".into(),
        ));
    }
    if let Err(invalid) = krabka_log::topic_name::validate_topic_name(&resource.resource_name) {
        return Err((codes::INVALID_TOPIC_EXCEPTION, invalid.to_string()));
    }
    let current = image.topic_config(&resource.resource_name);
    let mut overrides = std::collections::BTreeMap::new();
    for cfg in &resource.configs {
        // A controller-managed key has no client writer, so it cannot take
        // part in the replacement. The check comes before the whitelist, so
        // the operator reads the refusal that names what does write the key
        // and not `Unknown topic config name`.
        if config_keys::is_controller_managed_topic_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&cfg.name),
            ));
        }
        let value = config_keys::canonical_topic_config(
            &cfg.name,
            cfg.value.as_deref().unwrap_or_default(),
            unstable,
        )
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
        overrides.insert(cfg.name.clone(), value);
    }
    config_keys::validate_config_combination(
        &overrides,
        &config_keys::TopicDefaults::from_image(image, unstable),
        remote_storage_system_enabled,
    )
    .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    config_keys::validate_diskless_unchanged(current, &overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    config_keys::validate_remote_storage_disable(current, &overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    if image.topic(&resource.resource_name).is_none() {
        return Err((
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            format!("The topic '{}' does not exist.", resource.resource_name),
        ));
    }
    // KIP-133: the operator-declared policy, on the map the topic ends up
    // with. Kafka calls `AlterConfigPolicy.validate` on the same resolved map,
    // and its `RequestMetadata` carries no partition count and no replication
    // factor, so neither is passed here. It runs after the built-in
    // validators, as `ConfigAdminManager` does: a config the broker itself
    // refuses never reaches the policy.
    check(policy, &resource.resource_name, None, None, &overrides)
        .map_err(|reason| (codes::POLICY_VIOLATION, reason))?;
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides,
    }))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{
        config_keys::{CLEANUP_POLICY, DELIVERY_MODE, REMOTE_STORAGE_ENABLE, RETENTION_MS},
        handlers::{
            alter_configs::test_support::{
                image_with_topic, image_with_topic_config, topic_resource,
            },
            test_support::check_config_result,
        },
    };

    /// The record builder under the empty policy, which is the state of a
    /// broker with no `[topic_policy]` section. It shadows the glob-imported
    /// name so the cases below read as the two-argument call they were
    /// written as; the policy's own cases pass a real one to
    /// [`super::topic_config_record`].
    fn topic_config_record(
        resource: &AlterConfigsResource,
        image: &krabka_metadata::MetadataImage,
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
    fn topic_replacement_rejects_compaction_on_a_scheduled_topic() {
        let image = image_with_topic("orders");

        let (code, message) = topic_config_record(
            &topic_resource(
                "orders",
                &[(CLEANUP_POLICY, "compact"), (DELIVERY_MODE, "scheduled")],
            ),
            &image,
        )
        .expect_err("compaction on a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(
            message.contains(crate::config_keys::CLEANUP_POLICY),
            "got: {message}"
        );
        assert!(
            message.contains(crate::config_keys::DELIVERY_MODE),
            "got: {message}"
        );
    }

    /// Kafka's `LogConfig.validateRemoteStorageRequiresDeleteCleanupPolicy`
    /// refuses tiered storage on a topic whose policy is anything but `delete`
    /// or the empty list, and `AlterConfigs` is one of the three paths that
    /// surface the `ConfigException` as `INVALID_CONFIG`.
    #[test]
    fn topic_replacement_rejects_tiered_storage_on_a_compacted_topic() {
        let image = image_with_topic("orders");

        for policy in ["compact", "compact,delete", "delete,compact"] {
            let (code, message) = topic_config_record(
                &topic_resource(
                    "orders",
                    &[(CLEANUP_POLICY, policy), (REMOTE_STORAGE_ENABLE, "true")],
                ),
                &image,
            )
            .expect_err("tiered storage on a compacted topic must be rejected");

            check!(code == codes::INVALID_CONFIG, "cleanup.policy={policy}");
            check!(
                message == config_keys::REMOTE_STORAGE_POLICY_MESSAGE,
                "cleanup.policy={policy}"
            );
        }
    }

    /// Independent expected replacement for the ordinary delete policy.
    fn expected_delete_policy_record(extra: (&str, &str)) -> MetadataRecord {
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
                crate::config_keys::CLEANUP_POLICY.to_string() => "delete".to_string(),
                extra.0.to_string() => extra.1.to_string(),
            },
        })
    }

    /// The same replacement without compaction is ordinary, so the refusal
    /// above is the pair and not the tiered-storage key alone.
    #[test]
    fn topic_replacement_accepts_tiered_storage_on_a_delete_policy_topic() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource(
                "orders",
                &[(CLEANUP_POLICY, "delete"), (REMOTE_STORAGE_ENABLE, "true")],
            ),
            &image,
        )
        .expect("a tiered delete-policy topic is valid");

        let expected = expected_delete_policy_record((REMOTE_STORAGE_ENABLE, "true"));
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_accepts_a_scheduled_topic_without_compaction() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource(
                "orders",
                &[(CLEANUP_POLICY, "delete"), (DELIVERY_MODE, "scheduled")],
            ),
            &image,
        )
        .expect("a scheduled delete-policy topic is valid");

        let expected = expected_delete_policy_record((DELIVERY_MODE, "scheduled"));
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_rejects_controller_managed_configs() {
        let image = image_with_topic("orders");

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS {
            // A full replacement carries a value for every key it names, so
            // both a `true` and an empty value are a write of the key.
            for value in ["true", "false", ""] {
                let error = topic_config_record(&topic_resource("orders", &[(key, value)]), &image)
                    .expect_err("controller-managed key must be rejected");

                check!(
                    error.0 == codes::INVALID_CONFIG,
                    "key {key} value {value:?}"
                );
                check!(
                    error.1 == config_keys::controller_managed_topic_config_message(key),
                    "key {key} value {value:?}"
                );
                check!(!error.1.is_empty(), "key {key} value {value:?}");
            }
        }
    }

    #[test]
    fn topic_replacement_cannot_move_a_topic_between_data_paths() {
        /// The topic's stored overrides, the complete replacement the request
        /// carries, and whether the replacement is accepted.
        type Replacement<'a> = (
            &'a str,
            &'a [(&'a str, &'a str)],
            &'a [(&'a str, &'a str)],
            bool,
        );

        let cases: [Replacement<'_>; 6] = [
            (
                "a replacement that restates the diskless flag",
                &[(config_keys::DISKLESS, "true")],
                &[
                    (config_keys::DISKLESS, "true"),
                    (config_keys::RETENTION_MS, "60000"),
                ],
                true,
            ),
            (
                "a replacement that silently drops the diskless flag",
                &[(config_keys::DISKLESS, "true")],
                &[(config_keys::RETENTION_MS, "60000")],
                false,
            ),
            (
                "a replacement that turns the diskless flag off",
                &[(config_keys::DISKLESS, "true")],
                &[(config_keys::DISKLESS, "false")],
                false,
            ),
            (
                "a replacement that turns the diskless flag on",
                &[(config_keys::RETENTION_MS, "60000")],
                &[(config_keys::DISKLESS, "true")],
                false,
            ),
            (
                "a replacement that drops an explicit false, which changes nothing",
                &[(config_keys::DISKLESS, "false")],
                &[(config_keys::RETENTION_MS, "60000")],
                true,
            ),
            (
                "a replacement that adds tiered storage to a diskless topic",
                &[(config_keys::DISKLESS, "true")],
                &[
                    (config_keys::DISKLESS, "true"),
                    ("remote.storage.enable", "true"),
                ],
                false,
            ),
        ];

        for (label, stored, replacement, want_ok) in cases {
            let image = image_with_topic_config("orders", stored);

            let result = topic_config_record(&topic_resource("orders", replacement), &image);

            check_config_result(result, want_ok, label, config_keys::DISKLESS);
        }
    }

    #[test]
    fn topic_replacement_leaves_an_ordinary_config_unaffected() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[(RETENTION_MS, "60000")]),
            &image,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {crate::config_keys::RETENTION_MS.to_string() => "60000".to_string()},
        });
        assert!(record == expected);
    }

    fn forbid_unclean_election() -> TopicPolicy {
        TopicPolicy {
            forbidden: [(
                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
            ..TopicPolicy::default()
        }
    }

    #[test]
    fn a_replacement_that_breaks_the_policy_is_a_policy_violation() {
        let image = image_with_topic("orders");
        let policy = forbid_unclean_election();

        let (code, message) = super::topic_config_record(
            &topic_resource(
                "orders",
                &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
            ),
            &image,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect_err("a forbidden value must be refused");

        check!(code == codes::POLICY_VIOLATION);
        check!(
            message.contains(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE),
            "{message}"
        );
    }

    #[test]
    fn a_replacement_the_policy_allows_still_commits() {
        let image = image_with_topic("orders");
        let policy = forbid_unclean_election();

        let record = super::topic_config_record(
            &topic_resource(
                "orders",
                &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "false")],
            ),
            &image,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect("the other value of a forbidden key is accepted");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_string() => "false".to_string()},
        });
        check!(record == expected);
    }

    /// `ControllerConfigurationValidator.validateTopicName` runs before the
    /// configs, and the existence check after them, each with Kafka's code
    /// and message.
    #[test]
    fn topic_names_are_checked_in_kafkas_order() {
        let image = image_with_topic("orders");
        let cases = [
            (
                "",
                vec![(config_keys::RETENTION_MS, "1000")],
                (
                    codes::INVALID_REQUEST,
                    "Default topic resources are not allowed.".to_owned(),
                ),
            ),
            (
                "a/b",
                vec![(config_keys::RETENTION_MS, "1000")],
                (
                    codes::INVALID_TOPIC_EXCEPTION,
                    "Topic name is invalid: 'a/b' contains one or more characters other than \
                     ASCII alphanumerics, '.', '_' and '-'"
                        .to_owned(),
                ),
            ),
            (
                "missing",
                vec![(config_keys::RETENTION_MS, "1000")],
                (
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "The topic 'missing' does not exist.".to_owned(),
                ),
            ),
            (
                "missing",
                vec![(config_keys::RETENTION_MS, "abc")],
                (
                    codes::INVALID_CONFIG,
                    "Invalid value abc for configuration retention.ms: Not a number of type LONG"
                        .to_owned(),
                ),
            ),
        ];
        for (name, configs, want) in cases {
            check!(
                topic_config_record(&topic_resource(name, &configs), &image) == Err(want),
                "{name:?}"
            );
        }
    }
}
