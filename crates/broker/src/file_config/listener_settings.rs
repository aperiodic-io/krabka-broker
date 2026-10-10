//! Applying the listener-shaped file values to a `BrokerConfig`.
//!
//! [`ListenerSettings`] carries the `[[listeners]]` array together with the
//! top-level keys that only make sense beside it — the inter-broker listener
//! name, the connection ceilings, the raw `server_properties`, and the
//! controller listener's protocol and TLS material. `apply_listener_settings`
//! writes them under the fill-or-replace rules the file config uses.

use krabka_security::ListenerProtocol;
use krabka_units::Time;

use super::{FileClientAuthMode, FileConfigError, FileListener, FileTlsConfig};

pub(super) struct ListenerSettings {
    pub(super) listeners: Vec<FileListener>,
    pub(super) inter_broker_listener_name: Option<String>,
    pub(super) max_connections: Option<usize>,
    pub(super) max_connections_per_ip: Option<usize>,
    pub(super) connections_max_idle: Option<Time>,
    pub(super) connections_max_reauth: Option<Time>,
    pub(super) server_properties: std::collections::BTreeMap<String, String>,
    pub(super) controller_listener_protocol: Option<ListenerProtocol>,
    pub(super) tls_config: Option<FileTlsConfig>,
}

/// KIP-464: `num.partitions` and `default.replication.factor` under their
/// Kafka names. A dedicated `[runtime]` key or CLI flag wins, so a property
/// applies only when no dedicated source named the setting. A value that is
/// not a positive integer refuses the configuration, as Kafka's `ConfigDef`
/// refuses it at startup.
fn apply_topic_creation_properties(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let origins = &mut cfg.static_config_origins.topic_creation;
    if !origins.num_partitions
        && let Some(value) = properties.get(crate::config_keys::NUM_PARTITIONS)
    {
        cfg.num_partitions = parse_positive(crate::config_keys::NUM_PARTITIONS, value)?;
        origins.num_partitions = true;
    }
    if !origins.default_replication_factor
        && let Some(value) = properties.get(crate::config_keys::DEFAULT_REPLICATION_FACTOR)
    {
        cfg.default_replication_factor =
            parse_positive(crate::config_keys::DEFAULT_REPLICATION_FACTOR, value)?;
        origins.default_replication_factor = true;
    }
    Ok(())
}

/// A boolean `server_properties` value under its Kafka name, or `None` when
/// the operator did not name it. Kafka's `ConfigDef` reads a boolean
/// case-insensitively and refuses anything but `true` and `false` at startup,
/// and so does this.
fn boolean_property(
    properties: &std::collections::BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, FileConfigError> {
    let Some(value) = properties.get(key) else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        _ => Err(FileConfigError::InvalidConfig(format!(
            "server_properties `{key}` must be `true` or `false`, got `{value}`"
        ))),
    }
}

/// Kafka's internal `ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG`.
const UNSTABLE_API_VERSIONS_ENABLE: &str = "unstable.api.versions.enable";

/// Kafka's internal `ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG`.
const UNSTABLE_FEATURE_VERSIONS_ENABLE: &str = "unstable.feature.versions.enable";

/// The static boolean broker keys Kafka reads at startup:
/// `delete.topic.enable` and `auto.create.topics.enable`, each recorded as
/// operator-supplied when named, `transaction.partition.verification.enable`,
/// the static layer of a dynamic config, and the internal
/// `unstable.api.versions.enable` and `unstable.feature.versions.enable`.
fn apply_boolean_properties(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    if let Some(enabled) = boolean_property(properties, UNSTABLE_API_VERSIONS_ENABLE)? {
        cfg.features.unstable_api_versions = enabled.into();
    }
    if let Some(enabled) = boolean_property(properties, UNSTABLE_FEATURE_VERSIONS_ENABLE)? {
        cfg.features.unstable_feature_versions = enabled.into();
    }
    if let Some(enabled) = boolean_property(properties, crate::config_keys::DELETE_TOPIC_ENABLE)? {
        cfg.delete_topic_enable = enabled;
        cfg.static_config_origins.topic_admin.delete_topic_enable = true;
    }
    if let Some(enabled) =
        boolean_property(properties, crate::config_keys::AUTO_CREATE_TOPICS_ENABLE)?
    {
        cfg.auto_create_topics_enable = enabled;
        cfg.static_config_origins
            .topic_admin
            .auto_create_topics_enable = true;
    }
    // A dedicated `[runtime]` key or CLI flag wins, and its presence is
    // already recorded as the provenance of the key.
    let verification = crate::txn::coordinator::produce_verification::PARTITION_VERIFICATION_ENABLE;
    if !cfg
        .static_config_origins
        .supplied_kafka_keys
        .contains(verification)
        && let Some(enabled) = boolean_property(properties, verification)?
    {
        cfg.transaction_partition_verification_enable = enabled;
        cfg.static_config_origins
            .supplied_kafka_keys
            .insert(verification);
    }
    Ok(())
}

/// KIP-1066 `cordoned.log.dirs` under its Kafka name. A value already set by
/// another source wins. `crate::BrokerConfig::validate` checks it against the
/// log directories once they are all known.
fn apply_cordoned_log_dirs(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) {
    if cfg.cordoned_log_dirs.is_none()
        && let Some(value) = properties.get(crate::cordoned_log_dirs::CORDONED_LOG_DIRS)
    {
        cfg.cordoned_log_dirs = Some(value.clone());
    }
}

/// Kafka's `log.roll.ms`, the static default of a topic's `segment.ms`. A
/// topic that sets no `segment.ms` rolls its active segment when an append
/// would make the segment span more record time than this. Kafka refuses a
/// value under 1 at startup, and so does this.
fn apply_log_roll_ms(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    use krabka_units::convert::TimeExt as _;

    if let Some(value) = properties.get(crate::config_keys::LOG_ROLL_MS) {
        let millis: i64 = parse_positive(crate::config_keys::LOG_ROLL_MS, value)?;
        cfg.log_config.segment_roll_interval = Time::from_millis(millis);
        cfg.static_config_origins
            .supplied_kafka_keys
            .insert(crate::config_keys::LOG_ROLL_MS);
    }
    Ok(())
}

/// Kafka's `group.consumer.migration.policy`: whether a consumer group may
/// convert between the classic and the consumer protocol while it has
/// members. Kafka reads the value without regard to case, and refuses a value
/// outside its four policies at startup.
fn apply_consumer_group_migration_policy(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    const KEY: &str = "group.consumer.migration.policy";
    if let Some(value) = properties.get(KEY) {
        cfg.next_gen_consumer_group.migration_policy =
            value.trim().to_ascii_lowercase().parse().map_err(|_| {
                FileConfigError::InvalidConfig(format!(
                    "server_properties `{KEY}` must be one of `disabled`, `upgrade`, \
                     `downgrade` or `bidirectional`, got `{value}`"
                ))
            })?;
        cfg.static_config_origins.supplied_kafka_keys.insert(KEY);
    }
    Ok(())
}

/// Kafka trunk's `group.streams.topology.description.plugin.class`
/// (KIP-1331). It names the JVM class that stores the topology descriptions
/// Streams clients push. krabka builds in Kafka's in-memory plugin and runs it
/// when the key names that class, and refuses any other class, as Kafka
/// refuses to start on a class it cannot load.
fn apply_topology_description_plugin(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    use crate::coordinator::unified::streams::description::{
        PLUGIN_CLASS_CONFIG, TopologyDescriptionPlugin,
    };

    if let Some(value) = properties.get(PLUGIN_CLASS_CONFIG) {
        cfg.streams_group.topology_description_plugin =
            TopologyDescriptionPlugin::from_class_name(value)
                .map_err(FileConfigError::InvalidConfig)?;
    }
    Ok(())
}

/// A positive integer `server_properties` value.
fn parse_positive<T: std::str::FromStr + Default + PartialOrd>(
    name: &str,
    value: &str,
) -> Result<T, FileConfigError> {
    value
        .trim()
        .parse::<T>()
        .ok()
        .filter(|parsed| *parsed > T::default())
        .ok_or_else(|| {
            FileConfigError::InvalidConfig(format!(
                "server_properties `{name}` must be a positive integer, got `{value}`"
            ))
        })
}

pub(super) fn apply_listener_settings(
    settings: ListenerSettings,
    cfg: &mut crate::config::BrokerConfig,
    defaults: &crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let had_file_listeners = !settings.listeners.is_empty();
    if had_file_listeners {
        // Read the per-listener idle overrides before `into_spec` consumes
        // each entry. They are keyed by listener name, which is how the
        // dispatch loop and `DescribeConfigs` both look one up.
        cfg.connections_max_idle_overrides = settings
            .listeners
            .iter()
            .filter_map(|listener| {
                listener
                    .connections_max_idle
                    .map(|idle| (listener.name.clone(), idle))
            })
            .collect();
        cfg.connections_max_reauth_overrides = settings
            .listeners
            .iter()
            .filter_map(|listener| {
                listener
                    .connections_max_reauth
                    .map(|reauth| (listener.name.clone(), reauth))
            })
            .collect();
        cfg.listeners = settings
            .listeners
            .into_iter()
            .map(FileListener::into_spec)
            .collect::<Result<_, _>>()?;
    }
    if let Some(name) = settings.inter_broker_listener_name {
        cfg.inter_broker_listener_name = name;
        cfg.static_config_origins
            .supplied_kafka_keys
            .insert(crate::config_keys::INTER_BROKER_LISTENER_NAME);
    }
    if had_file_listeners
        && let Some(advertised) = cfg
            .listeners
            .iter()
            .find(|listener| listener.name == cfg.inter_broker_listener_name)
            .or_else(|| cfg.listeners.first())
            .map(|listener| listener.advertised.clone())
    {
        cfg.advertised_listener = advertised;
    }
    if let Some(maximum) = settings.max_connections
        && cfg.max_connections == defaults.max_connections
    {
        cfg.max_connections = maximum;
    }
    if let Some(maximum) = settings.max_connections_per_ip
        && cfg.max_connections_per_ip == defaults.max_connections_per_ip
    {
        cfg.max_connections_per_ip = maximum;
    }
    if let Some(idle) = settings.connections_max_idle
        && cfg.connections_max_idle.is_none()
    {
        cfg.connections_max_idle = Some(idle);
    }
    if let Some(reauth) = settings.connections_max_reauth
        && cfg.connections_max_reauth.is_none()
    {
        cfg.connections_max_reauth = Some(reauth);
    }
    if cfg.features.transaction_two_phase_commit_enable
        == defaults.features.transaction_two_phase_commit_enable
        && let Some(value) = settings
            .server_properties
            .get("transaction.two.phase.commit.enable")
    {
        cfg.features.transaction_two_phase_commit_enable =
            value.trim().eq_ignore_ascii_case("true");
    }
    apply_topic_creation_properties(&settings.server_properties, cfg)?;
    apply_boolean_properties(&settings.server_properties, cfg)?;
    apply_cordoned_log_dirs(&settings.server_properties, cfg);
    apply_log_roll_ms(&settings.server_properties, cfg)?;
    apply_consumer_group_migration_policy(&settings.server_properties, cfg)?;
    apply_topology_description_plugin(&settings.server_properties, cfg)?;
    let num_val = settings
        .server_properties
        .get("quota.window.num")
        .and_then(|v| v.trim().parse::<u32>().ok());
    let size_val = settings
        .server_properties
        .get("quota.window.size.seconds")
        .and_then(|v| v.trim().parse::<u64>().ok());
    if num_val.is_some() || size_val.is_some() {
        let num = num_val.unwrap_or(11);
        let size_secs = size_val.unwrap_or(1);
        if cfg.quota_throttle_max == defaults.quota_throttle_max {
            // `ClientRequestQuotaManager.maxThrottleTimeMs`: one window.
            let s = u32::try_from(size_secs).unwrap_or(u32::MAX);
            cfg.quota_throttle_max = krabka_units::secs(s);
        }
        if cfg.quota_window == defaults.quota_window {
            let s = u32::try_from(size_secs * u64::from(num)).unwrap_or(u32::MAX);
            cfg.quota_window = krabka_units::secs(s);
        }
    }
    if let Some(protocol) = settings.controller_listener_protocol
        && cfg.controller_listener_protocol == defaults.controller_listener_protocol
    {
        cfg.controller_listener_protocol = protocol;
    }
    if let Some(tls) = settings.tls_config
        && cfg.tls_config.is_none()
    {
        use krabka_security::{ClientAuthMode, TlsConfig};
        cfg.tls_principal_mapper = crate::SslPrincipalMapper::parse(&tls.principal_mapping_rules)
            .map_err(|error| {
            FileConfigError::InvalidConfig(format!(
                "invalid ssl principal mapping rule in tls_config: {error}"
            ))
        })?;
        cfg.tls_config = Some(TlsConfig {
            cert_chain_path: tls.cert_path,
            private_key_path: tls.key_path,
            trust_roots_path: tls.trust_roots_path,
            client_ca_path: tls.client_ca_path,
            client_auth: match tls.client_auth {
                FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                FileClientAuthMode::Optional => ClientAuthMode::Optional,
                FileClientAuthMode::Required => ClientAuthMode::Required,
            },
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use crate::file_config::FileConfig;

    fn file_outcome<T>(
        source: &str,
        read: impl FnOnce(&crate::config::BrokerConfig) -> T,
    ) -> Result<T, String> {
        let file: FileConfig = toml::from_str(source).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg)
            .map(|()| read(&cfg))
            .map_err(|error| error.to_string())
    }

    fn check_config_cases<const N: usize, S: AsRef<str>, T: std::fmt::Debug + PartialEq>(
        cases: [(&str, S, Result<T, String>); N],
        read: impl Fn(&crate::config::BrokerConfig) -> T,
    ) {
        let mut actual = Vec::with_capacity(N);
        let mut expected = Vec::with_capacity(N);
        for (label, source, want) in cases {
            actual.push((label, file_outcome(source.as_ref(), &read)));
            expected.push((label, want));
        }
        assert!(actual == expected);
    }

    #[test]
    fn apply_to_populates_listeners() {
        let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(src).unwrap();

        check!(cfg.listeners.len() == 1);
        check!(cfg.listeners[0].name.as_str() == "PLAIN");
        check!(cfg.listeners[0].advertised.as_str() == "demo-0:9092");
        check!(cfg.inter_broker_listener_name.as_str() == "PLAIN");
    }
    #[test]
    fn apply_to_maps_connection_caps() {
        use crate::config::BrokerConfig;

        let src = r"
max_connections = 100
max_connections_per_ip = 8
";
        let file: FileConfig = toml::from_str(src).unwrap();
        assert!(file.max_connections == Some(100));
        assert!(file.max_connections_per_ip == Some(8));

        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.max_connections == 100);
        assert!(cfg.max_connections_per_ip == 8);
    }
    #[test]
    fn apply_to_omitted_connection_caps_keep_default_unlimited() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.max_connections == None);
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // Omitted → unchanged from the (unlimited) BrokerConfig default.
        assert!(cfg.max_connections == usize::MAX);
        assert!(cfg.max_connections_per_ip == usize::MAX);
    }
    /// The idle window comes in two places: a top-level key for the broker
    /// and a per-listener key that wins for the listener that carries it.
    #[test]
    fn apply_to_maps_the_idle_window_and_its_per_listener_override() {
        use std::time::Duration;

        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "INTERNAL"
connections_max_idle = "45s"

[[listeners]]
name = "INTERNAL"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"
connections_max_idle = "5s"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        check!(cfg.connections_max_idle == Some(krabka_units::secs(45)));
        check!(
            cfg.connections_max_idle_overrides
                == maplit::btreemap! {"EXTERNAL".to_string() => krabka_units::secs(5)}
        );
        check!(cfg.connections_max_idle_for("EXTERNAL") == Some(Duration::from_secs(5)));
        check!(cfg.connections_max_idle_for("INTERNAL") == Some(Duration::from_secs(45)));
    }

    /// KIP-464: `num.partitions` and `default.replication.factor` under their
    /// Kafka names in `[server_properties]`. A dedicated `[runtime]` key wins
    /// over the property, and a value that is not a positive integer refuses
    /// the file.
    #[test]
    fn apply_to_reads_the_topic_creation_defaults_from_server_properties() {
        use crate::config::{BrokerConfig, TopicCreationOrigins};

        for (label, src, expected) in [
            (
                "neither named",
                "broker_id = 0\n",
                Ok((1, 1, TopicCreationOrigins::default())),
            ),
            (
                "both as properties",
                "[server_properties]\n\"num.partitions\" = \"6\"\n\
                 \"default.replication.factor\" = \"3\"\n",
                Ok((
                    6,
                    3,
                    TopicCreationOrigins {
                        num_partitions: true,
                        default_replication_factor: true,
                    },
                )),
            ),
            (
                "a dedicated key wins over the property",
                "[runtime]\nnum_partitions = 4\n\
                 [server_properties]\n\"num.partitions\" = \"6\"\n",
                Ok((
                    4,
                    1,
                    TopicCreationOrigins {
                        num_partitions: true,
                        default_replication_factor: false,
                    },
                )),
            ),
            (
                "a zero is refused",
                "[server_properties]\n\"default.replication.factor\" = \"0\"\n",
                Err(
                    "server_properties `default.replication.factor` must be a positive integer, got `0`",
                ),
            ),
            (
                "a word is refused",
                "[server_properties]\n\"num.partitions\" = \"many\"\n",
                Err("server_properties `num.partitions` must be a positive integer, got `many`"),
            ),
        ] {
            let file: FileConfig = toml::from_str(src).expect("parse");
            let mut cfg = BrokerConfig::default();
            let result = file.apply_to(&mut cfg).map(|()| {
                (
                    cfg.num_partitions,
                    cfg.default_replication_factor,
                    cfg.static_config_origins.topic_creation,
                )
            });
            let result = result.map_err(|error| error.to_string());

            check!(
                result
                    .as_ref()
                    .map_err(|message| message.contains(expected.err().unwrap_or("-")))
                    == expected.as_ref().map_err(|_| true),
                "{label}: {result:?}"
            );
        }
    }

    /// `delete.topic.enable` and `auto.create.topics.enable` under their Kafka
    /// names: booleans Kafka reads case-insensitively, default `true`, and a
    /// value that is neither word refuses the configuration.
    #[test]
    fn boolean_keys_are_read_from_server_properties() {
        /// The value and the operator-supplied flag of one key.
        type Read = fn(&crate::config::BrokerConfig) -> (bool, bool);
        /// A label, the key it reads, the file, and the value, flag or error.
        type Case = (
            &'static str,
            Read,
            &'static str,
            Result<(bool, bool), String>,
        );
        let delete: Read = |cfg| {
            (
                cfg.delete_topic_enable,
                cfg.static_config_origins.topic_admin.delete_topic_enable,
            )
        };
        let auto_create: Read = |cfg| {
            (
                cfg.auto_create_topics_enable,
                cfg.static_config_origins
                    .topic_admin
                    .auto_create_topics_enable,
            )
        };
        let cases: [Case; 6] = [
            (
                "delete not named",
                delete,
                "broker_id = 0\n",
                Ok((true, false)),
            ),
            (
                "delete false",
                delete,
                "[server_properties]\n\"delete.topic.enable\" = \"false\"\n",
                Ok((false, true)),
            ),
            (
                "delete upper case",
                delete,
                "[server_properties]\n\"delete.topic.enable\" = \"TRUE\"\n",
                Ok((true, true)),
            ),
            (
                "delete a word",
                delete,
                "[server_properties]\n\"delete.topic.enable\" = \"maybe\"\n",
                Err(
                    "invalid config: server_properties `delete.topic.enable` must be `true` or \
                     `false`, got `maybe`"
                        .to_owned(),
                ),
            ),
            (
                "auto-create not named",
                auto_create,
                "broker_id = 0\n",
                Ok((true, false)),
            ),
            (
                "auto-create false",
                auto_create,
                "[server_properties]\n\"auto.create.topics.enable\" = \"False\"\n",
                Ok((false, true)),
            ),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (label, read, src, want) in cases {
            let result = file_outcome(src, read);
            actual.push((label, result));
            expected.push((label, want));
        }
        assert!(actual == expected);
    }

    /// Kafka's `log.roll.ms` sets the roll interval of every log whose topic
    /// sets no `segment.ms`, and records that the operator named it. A value
    /// under 1 refuses the configuration, as `KafkaConfig` refuses it. Kafka's
    /// `LogDirFailureTest` sets it to make a broker write into a failed log
    /// directory within seconds.
    #[test]
    fn log_roll_ms_is_read_from_server_properties() {
        use krabka_units::convert::TimeExt as _;

        /// The roll interval in milliseconds and the operator-supplied flag,
        /// or the error.
        type Outcome = Result<(i64, bool), String>;
        let property =
            |value: &str| format!("[server_properties]\n\"log.roll.ms\" = \"{value}\"\n");
        let cases: [(&str, String, Outcome); 5] = [
            (
                "not named",
                "broker_id = 0\n".to_owned(),
                Ok((604_800_000, false)),
            ),
            ("named", property("3000"), Ok((3000, true))),
            (
                "named at the segment.ms default",
                property("604800000"),
                Ok((604_800_000, true)),
            ),
            (
                "zero",
                property("0"),
                Err(
                    "invalid config: server_properties `log.roll.ms` must be a positive integer, \
                     got `0`"
                        .to_owned(),
                ),
            ),
            (
                "a word",
                property("soon"),
                Err(
                    "invalid config: server_properties `log.roll.ms` must be a positive integer, \
                     got `soon`"
                        .to_owned(),
                ),
            ),
        ];
        check_config_cases(cases, |cfg| {
            (
                cfg.log_config.segment_roll_interval.millis_i64(),
                cfg.static_config_origins
                    .supplied_kafka_keys
                    .contains("log.roll.ms"),
            )
        });
    }

    #[test]
    fn the_consumer_group_migration_policy_is_read_from_server_properties() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy as Policy;

        /// The policy and the operator-supplied flag, or the error.
        type Outcome = Result<(Policy, bool), String>;
        let property = |value: &str| {
            format!("[server_properties]\n\"group.consumer.migration.policy\" = \"{value}\"\n")
        };
        let cases: [(&str, String, Outcome); 5] = [
            (
                "not named",
                "broker_id = 0\n".to_owned(),
                Ok((Policy::Bidirectional, false)),
            ),
            (
                "disabled",
                property("disabled"),
                Ok((Policy::Disabled, true)),
            ),
            (
                "upper case",
                property("DOWNGRADE"),
                Ok((Policy::Downgrade, true)),
            ),
            (
                "mixed case",
                property("Upgrade"),
                Ok((Policy::Upgrade, true)),
            ),
            (
                "not a policy",
                property("sideways"),
                Err(
                    "invalid config: server_properties `group.consumer.migration.policy` must be \
                     one of `disabled`, `upgrade`, `downgrade` or `bidirectional`, got `sideways`"
                        .to_owned(),
                ),
            ),
        ];
        check_config_cases(cases, |cfg| {
            (
                cfg.next_gen_consumer_group.migration_policy,
                cfg.static_config_origins
                    .supplied_kafka_keys
                    .contains("group.consumer.migration.policy"),
            )
        });
    }

    /// `transaction.partition.verification.enable`, the static layer of a
    /// dynamic config, comes from the `[runtime]` table or from
    /// `server_properties`. The dedicated `[runtime]` key wins over the
    /// property, and either source records that the operator named the key.
    #[test]
    fn partition_verification_is_read_from_the_runtime_table_and_server_properties() {
        /// The value and the operator-supplied flag, or the error.
        type Outcome = Result<(bool, bool), String>;
        const KEY: &str = "transaction.partition.verification.enable";
        let property = |value: &str| format!("[server_properties]\n\"{KEY}\" = \"{value}\"\n");
        let runtime = |value: &str| {
            format!("[runtime]\ntransaction_partition_verification_enable = {value}\n")
        };
        // A label, the file, and the outcome it loads to.
        let cases: [(&str, String, Outcome); 8] = [
            ("not named", "broker_id = 0\n".to_owned(), Ok((true, false))),
            ("property false", property("false"), Ok((false, true))),
            ("property upper case", property("TRUE"), Ok((true, true))),
            (
                "property a word",
                property("maybe"),
                Err(format!(
                    "invalid config: server_properties `{KEY}` must be `true` or `false`, got \
                     `maybe`"
                )),
            ),
            ("runtime false", runtime("false"), Ok((false, true))),
            ("runtime at the default", runtime("true"), Ok((true, true))),
            (
                "runtime true over a false property",
                format!("{}{}", runtime("true"), property("false")),
                Ok((true, true)),
            ),
            (
                "runtime false over a true property",
                format!("{}{}", runtime("false"), property("true")),
                Ok((false, true)),
            ),
        ];
        check_config_cases(cases, |cfg| {
            (
                cfg.transaction_partition_verification_enable,
                cfg.static_config_origins.supplied_kafka_keys.contains(KEY),
            )
        });
    }

    /// Omitted everywhere, the broker keeps Kafka's 600000 default and no
    /// listener carries an override.
    #[test]
    fn apply_to_omitted_idle_window_keeps_kafkas_default() {
        use crate::config::{BrokerConfig, DEFAULT_CONNECTIONS_MAX_IDLE};

        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.connections_max_idle.is_none());

        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // Unset stays unset -- that is what `DescribeConfigs` reports as
        // DEFAULT_CONFIG -- and the window in force is Kafka's default.
        assert!(cfg.connections_max_idle.is_none());
        assert!(cfg.effective_connections_max_idle() == DEFAULT_CONNECTIONS_MAX_IDLE);
        assert!(cfg.connections_max_idle_overrides.is_empty());
    }

    #[test]
    fn apply_to_reads_two_phase_commit_enable_from_server_properties() {
        use crate::config::BrokerConfig;

        // KIP-939: the `transaction.two.phase.commit.enable` server property
        // flips the cluster 2PC gate on; absent / "false" leaves it off.
        let on: FileConfig = toml::from_str(
            "[server_properties]\n\"transaction.two.phase.commit.enable\" = \"true\"\n",
        )
        .unwrap();
        let mut cfg = BrokerConfig::default();
        assert!(!cfg.features.transaction_two_phase_commit_enable); // default
        on.apply_to(&mut cfg).unwrap();
        assert!(cfg.features.transaction_two_phase_commit_enable);

        // Omitted → unchanged (stays at the default false).
        let absent: FileConfig = toml::from_str("broker_id = 0").unwrap();
        let mut cfg2 = BrokerConfig::default();
        absent.apply_to(&mut cfg2).unwrap();
        assert!(!cfg2.features.transaction_two_phase_commit_enable);
    }

    /// #646: Kafka's internal `unstable.api.versions.enable` under its own
    /// name, off by default, refused unless it is a boolean.
    #[test]
    fn apply_to_reads_unstable_api_versions_enable_from_server_properties() {
        use crate::api_catalog::UnstableApiVersions;

        for (toml, expected) in [
            ("broker_id = 0", Ok(UnstableApiVersions::Disabled)),
            (
                "[server_properties]\n\"unstable.api.versions.enable\" = \"true\"\n",
                Ok(UnstableApiVersions::Enabled),
            ),
            (
                "[server_properties]\n\"unstable.api.versions.enable\" = \"FALSE\"\n",
                Ok(UnstableApiVersions::Disabled),
            ),
            (
                "[server_properties]\n\"unstable.api.versions.enable\" = \"yes\"\n",
                Err(
                    "server_properties `unstable.api.versions.enable` must be `true` or \
                     `false`, got `yes`"
                        .to_string(),
                ),
            ),
        ] {
            let applied = crate::file_config::test_support::configured_feature(toml, |cfg| {
                cfg.features.unstable_api_versions
            });
            assert!(applied == expected, "{toml}");
        }
    }

    /// #784: Kafka's internal `unstable.feature.versions.enable` under its own
    /// name, off by default, refused unless it is a boolean.
    #[test]
    fn apply_to_reads_unstable_feature_versions_enable_from_server_properties() {
        use krabka_raft::UnstableFeatureVersions;

        for (toml, expected) in [
            ("broker_id = 0", Ok(UnstableFeatureVersions::Disabled)),
            (
                "[server_properties]\n\"unstable.feature.versions.enable\" = \"true\"\n",
                Ok(UnstableFeatureVersions::Enabled),
            ),
            (
                "[server_properties]\n\"unstable.feature.versions.enable\" = \"false\"\n",
                Ok(UnstableFeatureVersions::Disabled),
            ),
            (
                "[server_properties]\n\"unstable.feature.versions.enable\" = \"1\"\n",
                Err(
                    "server_properties `unstable.feature.versions.enable` must be `true` or \
                     `false`, got `1`"
                        .to_string(),
                ),
            ),
        ] {
            let applied = crate::file_config::test_support::configured_feature(toml, |cfg| {
                cfg.features.unstable_feature_versions
            });
            assert!(applied == expected, "{toml}");
        }
    }

    /// KIP-1331's `group.streams.topology.description.plugin.class`: unset
    /// by default, Kafka's in-memory plugin when it names that class, and
    /// refused for any class krabka cannot load.
    #[test]
    fn apply_to_reads_the_topology_description_plugin_from_server_properties() {
        use crate::{
            config::BrokerConfig,
            coordinator::unified::streams::description::TopologyDescriptionPlugin,
        };

        const KEY: &str = "group.streams.topology.description.plugin.class";
        let property = |value: &str| format!("[server_properties]\n\"{KEY}\" = \"{value}\"\n");
        for (toml, expected) in [
            (
                "broker_id = 0".to_owned(),
                Ok(TopologyDescriptionPlugin::None),
            ),
            (
                property("org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"),
                Ok(TopologyDescriptionPlugin::InMemory),
            ),
            (
                property("com.example.JdbcTopologyStore"),
                Err(format!(
                    "server_properties `{KEY}` names `com.example.JdbcTopologyStore`, which krabka \
                     cannot load: the only topology description plugin it builds in is \
                     `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`"
                )),
            ),
        ] {
            let file: FileConfig = toml::from_str(&toml).unwrap();
            let mut cfg = BrokerConfig::default();
            let applied = file
                .apply_to(&mut cfg)
                .map(|()| cfg.streams_group.topology_description_plugin)
                .map_err(|error| match error {
                    crate::file_config::FileConfigError::InvalidConfig(message) => message,
                    other => other.to_string(),
                });
            assert!(applied == expected, "{toml}");
        }
    }

    #[test]
    fn apply_to_reads_quota_window_properties() {
        use krabka_units::secs;

        let toml = r#"
[server_properties]
"quota.window.num" = "6"
"quota.window.size.seconds" = "2"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        // The request-quota bound is one window: size = 2s.
        assert!(cfg.quota_throttle_max == secs(2));
        // size * num = 2 * 6 = 12s
        assert!(cfg.quota_window == secs(12));
    }
    #[test]
    fn apply_to_propagates_tls_config() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
        let cfg = crate::file_config::test_support::configured(src, "parse").unwrap();
        assert!(cfg.controller_listener_protocol == krabka_security::ListenerProtocol::Ssl);
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(tls.cert_chain_path == std::path::PathBuf::from("/c"));
    }
    #[test]
    fn apply_to_threads_trust_roots_and_controller_server_name() {
        // The operator renders the cluster CA as the dialer trust root and
        // the shared headless FQDN as the controller SNI so KIP-595 peers can
        // mTLS to each other.
        let src = r#"
controller_server_name = "demo-broker-headless.default.svc.cluster.local"
[tls_config]
cert_path = "/etc/krabka/broker-tls/0.crt"
key_path = "/etc/krabka/broker-tls/0.key"
trust_roots_path = "/etc/krabka/cluster-ca/ca.crt"
client_ca_path = "/etc/krabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg = crate::file_config::test_support::configured(src, "parse").unwrap();
        assert!(
            cfg.controller_server_name.as_deref()
                == Some("demo-broker-headless.default.svc.cluster.local")
        );
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(
            tls.trust_roots_path.as_deref()
                == Some(std::path::Path::new("/etc/krabka/cluster-ca/ca.crt"))
        );
    }
    /// The top-level `[tls_config]` carries the KIP-371 principal mapping
    /// rules that the controller listener applies to a peer certificate.
    #[test]
    fn apply_to_parses_top_level_principal_mapping_rules() {
        let base = r#"
[tls_config]
cert_path = "/etc/krabka/tls/node.crt"
key_path = "/etc/krabka/tls/node.key"
"#;
        let dn = "CN=node-1,OU=brokers,O=krabka";
        let cases = [
            ("no rules", String::new(), Some(dn)),
            (
                "a rule",
                "principal_mapping_rules = [\"RULE:^CN=(.*?),.*$/$1/\"]\n".to_owned(),
                Some("node-1"),
            ),
            (
                "a rule that matches nothing",
                "principal_mapping_rules = [\"RULE:^OU=(.*?)$/$1/\"]\n".to_owned(),
                None,
            ),
        ];
        for (name, rules, expected) in cases {
            let cfg =
                crate::file_config::test_support::configured(&format!("{base}{rules}"), "parse")
                    .unwrap();
            check!(
                cfg.tls_principal_mapper.apply(dn).as_deref() == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn apply_to_empty_listeners_does_not_clear_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = BrokerConfig {
            listeners: vec![crate::config::ListenerSpec {
                name: "X".into(),
                bind_addr: "0.0.0.0:9094".parse().unwrap(),
                advertised: "h:9094".into(),
                protocol: krabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: crate::SslPrincipalMapper::default(),
            }],
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.listeners.len() == 1);
        assert!(cfg.listeners[0].name == "X");
    }
    #[test]
    fn apply_to_syncs_advertised_listener_from_inter_broker_listener() {
        // Two listeners; the inter-broker one ("PLAIN") is NOT declared first.
        // `advertised_listener` (used by FindCoordinator + broker
        // self-registration) must be taken from the inter-broker listener's
        // `advertised` (the pod FQDN), not left at the CLI default
        // 127.0.0.1:9092 and not taken from the first-declared listener.
        let toml = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "ext.example.com:9094"
protocol = "Plaintext"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
protocol = "Plaintext"
"#;
        let cfg = crate::file_config::test_support::configured(toml, "parse").unwrap();

        assert!(
            cfg.advertised_listener == "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
        );
        // The inter-broker listener wins over the first-declared EXTERNAL one.
        assert!(cfg.advertised_listener != "ext.example.com:9094");
    }
}
