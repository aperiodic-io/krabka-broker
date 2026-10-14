//! The two unclean-recovery keys, the KIP-841 enable flag and the KIP-966
//! strategy. Both resolve through [`topic_or_cluster_default`].

use super::lookup::topic_or_cluster_default;

/// KIP-841: gates whether the controller may auto-elect an out-of-ISR
/// replica as leader on ISR-empty failover. Default: `false`, which matches
/// Apache Kafka. The partition then stays unavailable until a former ISR
/// member returns. `true` accepts possible data loss in exchange for
/// availability. `crate::leader_election::on_broker_dead` reads the topic
/// override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_LEADER_ELECTION_ENABLE: &str = "unclean.leader.election.enable";
/// KIP-966: topic-level unclean-recovery strategy. It supersedes
/// `unclean.leader.election.enable`. At `Balanced` or `Aggressive` the
/// controller runs offset-aware recovery: it polls surviving replicas for
/// their log offsets and elects the most complete log. Default: `None`,
/// which falls back to the legacy enable-flag behavior.
/// `crate::unclean_recovery` and the failover / `ElectLeaders` paths read the
/// topic override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_RECOVERY_STRATEGY: &str = "unclean.recovery.strategy";

/// Resolved value of `unclean.recovery.strategy` for a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStrategy {
    /// No offset-aware recovery. Defer to `unclean.leader.election.enable`.
    None,
    /// Wait for all currently-alive replicas, then elect the most complete
    /// log.
    Balanced,
    /// Elect the most complete log among the replicas that respond within
    /// a short deadline. This optimizes availability.
    Aggressive,
}

impl RecoveryStrategy {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        // A broker resource stores the client's string, and Kafka's
        // `ConfigDef.parse` trims a `STRING` value before it checks it.
        match super::parse::java_trim(value) {
            "None" => Some(Self::None),
            "Balanced" => Some(Self::Balanced),
            "Aggressive" => Some(Self::Aggressive),
            _ => None,
        }
    }
}

/// Resolve `unclean.recovery.strategy` for `topic`. A topic override takes
/// precedence over the cluster-wide default broker config. The result is
/// [`RecoveryStrategy::None`] when neither value exists or the selected value
/// is unparseable.
pub(crate) fn resolve_recovery_strategy(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> RecoveryStrategy {
    topic_or_cluster_default(image, topic, UNCLEAN_RECOVERY_STRATEGY)
        .and_then(RecoveryStrategy::parse)
        .unwrap_or(RecoveryStrategy::None)
}

/// Resolve `unclean.leader.election.enable` for `topic`. A topic override
/// takes precedence over the cluster-wide default broker config. Missing or
/// invalid values resolve to `false`.
pub(crate) fn resolve_unclean_leader_election_enabled(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> bool {
    topic_or_cluster_default(image, topic, UNCLEAN_LEADER_ELECTION_ENABLE)
        .and_then(super::parse::bool_value)
        == Some(true)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::validation::{is_recognized, validate_topic_config},
            *,
        },
        assert2::assert,
    };

    #[test]
    fn validate_unclean_leader_election_enable_accepts_bools() {
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "true").is_ok());
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "false").is_ok());
    }

    #[test]
    fn validate_unclean_leader_election_enable_rejects_junk() {
        let err = validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "yes").unwrap_err();
        assert!(err.contains("unclean.leader.election.enable"), "got: {err}");
    }

    #[test]
    fn is_recognized_includes_unclean_leader_election_enable() {
        assert!(is_recognized(UNCLEAN_LEADER_ELECTION_ENABLE));
    }

    #[yare::parameterized(
        none = { "None" },
        balanced = { "Balanced" },
        aggressive = { "Aggressive" },
    )]
    fn recovery_strategy_accepts_valid_values(value: &str) {
        assert2::assert!(
            validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, value).is_ok(),
            "{value}"
        );
    }

    #[test]
    fn recovery_strategy_rejects_garbage() {
        assert!(validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, "fast").is_err());
    }

    #[test]
    fn recovery_strategy_recognized() {
        assert!(is_recognized(UNCLEAN_RECOVERY_STRATEGY));
    }

    #[yare::parameterized(
        none = { "None", Some(RecoveryStrategy::None) },
        balanced = { "Balanced", Some(RecoveryStrategy::Balanced) },
        aggressive = { "Aggressive", Some(RecoveryStrategy::Aggressive) },
        trimmed_aggressive = { " Aggressive\t", Some(RecoveryStrategy::Aggressive) },
        unknown = { "bogus", None },
    )]
    fn parse_recovery_strategy_maps_values(input: &str, want: Option<RecoveryStrategy>) {
        assert2::assert!(RecoveryStrategy::parse(input) == want, "input {input:?}");
    }

    #[test]
    fn recovery_settings_resolve_topic_over_cluster_default() {
        use {
            krabka_metadata::{
                BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
                TopicConfigRecord,
            },
            std::collections::BTreeMap,
            uuid::Uuid,
        };
        let mut img = MetadataImage::new(Uuid::nil());
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));

        for (key, value) in [
            (UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
            (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
        ] {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(value.into()),
            }));
        }
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Balanced);
        assert!(resolve_unclean_leader_election_enabled(&img, "t"));

        let mut overrides = BTreeMap::new();
        overrides.insert(UNCLEAN_RECOVERY_STRATEGY.into(), "Aggressive".into());
        overrides.insert(UNCLEAN_LEADER_ELECTION_ENABLE.into(), "false".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        }));
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Aggressive);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));
    }

    #[test]
    fn invalid_topic_recovery_setting_does_not_expose_cluster_default() {
        use {
            krabka_metadata::{
                BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
                TopicConfigRecord,
            },
            uuid::Uuid,
        };

        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: UNCLEAN_RECOVERY_STRATEGY.into(),
            config_value: Some("Balanced".into()),
        }));
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: maplit::btreemap! {UNCLEAN_RECOVERY_STRATEGY.into() => "invalid".into()},
        }));

        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
    }
}
