//! TOML shape of `[topic_policy]` — the KIP-108 / KIP-133 rule set — and the
//! apply step that copies it onto [`crate::config::BrokerConfig`].

use {
    super::{FileConfigError, validate::positive_usize},
    crate::{config::BrokerConfig, topic_policy::TopicPolicy},
    std::collections::BTreeMap,
};

/// TOML shape of `[topic_policy]`. Maps to
/// [`crate::topic_policy::TopicPolicy`].
///
/// The whole table is the KIP-108 / KIP-133 policy Kafka loads from a Java
/// class named by `create.topic.policy.class.name` and
/// `alter.config.policy.class.name`. krabka cannot load a class, so the rules
/// are declared here instead. Omitting the table declares no rule, which is
/// Kafka's default of no policy class: every `CreateTopics` and every topic
/// `AlterConfigs` that passes ordinary config validation is accepted.
///
/// A request that breaks a rule is answered with `POLICY_VIOLATION` (44) and
/// an `error_message` naming the rule, which is what a `PolicyViolationException`
/// from a Java policy class produces. The two replica rules apply to
/// `CreateTopics` alone, because an `AlterConfigs` request carries neither
/// number, exactly as `AlterConfigPolicy.RequestMetadata` does not.
///
/// `deny_unknown_fields` so a misspelled rule name is rejected at parse time
/// rather than leaving the broker enforcing nothing where the operator wrote
/// a rule.
#[krabka_macros::config_table(strict)]
pub struct FileTopicPolicyConfig {
    /// Lowest replication factor `CreateTopics` may be asked for. The
    /// effective factor is checked, so a request that leaves it `-1` is
    /// judged on the factor the placement resolved. Omitted enforces no
    /// floor. Must be at least 1.
    pub min_replication_factor: Option<usize>,
    /// Highest partition count `CreateTopics` may be asked for, checked
    /// against the effective count. Omitted enforces no ceiling. Must be at
    /// least 1.
    pub max_partitions: Option<usize>,
    /// Lowest `min.insync.replicas` a topic may carry. A topic that does not
    /// set the key passes: it runs on the cluster default, which the same
    /// operator sets. Omitted enforces no floor. Must be at least 1.
    pub min_insync_replicas: Option<usize>,
    /// Config keys the topic must set, with the exact value it must set them
    /// to, written as a TOML table:
    /// `required = { "cleanup.policy" = "delete" }`. A topic that omits the
    /// key, or sets it to another value, is refused.
    #[serde(default)]
    pub required: BTreeMap<String, String>,
    /// Config keys the topic must not set to the named value, written as a
    /// TOML table:
    /// `forbidden = { "unclean.leader.election.enable" = "true" }`. Any other
    /// value for that key, and omitting the key, both pass.
    #[serde(default)]
    pub forbidden: BTreeMap<String, String>,
}

/// Apply `[topic_policy]`. Absent leaves the `BrokerConfig` default, which
/// declares no rule.
pub(super) fn apply_topic_policy(
    file: Option<FileTopicPolicyConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(file) = file else {
        return Ok(());
    };
    // A floor or a ceiling of zero is a rule that either means nothing or
    // refuses every topic. Both are the operator having mistyped it.
    let policy = TopicPolicy {
        min_replication_factor: file
            .min_replication_factor
            .map(|value| positive_usize("topic_policy.min_replication_factor", value))
            .transpose()?,
        max_partitions: file
            .max_partitions
            .map(|value| positive_usize("topic_policy.max_partitions", value))
            .transpose()?,
        min_insync_replicas: file
            .min_insync_replicas
            .map(|value| positive_usize("topic_policy.min_insync_replicas", value))
            .transpose()?,
        required: file.required,
        forbidden: file.forbidden,
    };
    cfg.topic_policy = policy;
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{config::BrokerConfig, file_config::FileConfig, topic_policy::TopicPolicy},
        assert2::check,
    };

    fn apply(toml: &str) -> Result<BrokerConfig, FileConfigError> {
        let file: FileConfig = toml::from_str(toml).expect("parse topic_policy section");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg)?;
        Ok(cfg)
    }

    #[test]
    fn every_rule_round_trips_onto_the_broker_config() {
        let cfg = apply(
            r#"
[topic_policy]
min_replication_factor = 3
max_partitions = 64
min_insync_replicas = 2
required = { "cleanup.policy" = "delete" }
forbidden = { "unclean.leader.election.enable" = "true" }
"#,
        )
        .expect("a well-formed policy applies");

        let expected = TopicPolicy {
            min_replication_factor: Some(3),
            max_partitions: Some(64),
            min_insync_replicas: Some(2),
            required: [("cleanup.policy".to_owned(), "delete".to_owned())]
                .into_iter()
                .collect(),
            forbidden: [(
                "unclean.leader.election.enable".to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
        };
        check!(cfg.topic_policy == expected);
    }

    #[test]
    fn an_absent_table_leaves_no_rule() {
        let cfg = apply("").expect("an empty document applies");
        check!(cfg.topic_policy.is_empty());
    }

    #[test]
    fn a_misspelled_rule_name_is_a_parse_error() {
        let parsed: Result<FileConfig, _> =
            toml::from_str("[topic_policy]\nmin_replication_factors = 3\n");
        check!(parsed.is_err());
    }

    #[yare::parameterized(
        minimum_replication_factor = { "[topic_policy]\nmin_replication_factor = 0\n" },
        maximum_partitions = { "[topic_policy]\nmax_partitions = 0\n" },
        minimum_insync_replicas = { "[topic_policy]\nmin_insync_replicas = 0\n" },
    )]
    fn a_zero_bound_is_a_config_error(toml: &str) {
        check!(apply(toml).is_err(), "{toml}");
    }
}
