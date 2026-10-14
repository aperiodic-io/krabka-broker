//! Applying the listener-shaped file values to a `BrokerConfig`.
//!
//! [`ListenerSettings`] carries the `[[listeners]]` array together with the
//! top-level keys that only make sense beside it — the inter-broker listener
//! name, the connection ceilings, the raw `server_properties`, and the
//! controller listener's protocol and TLS material. `apply_listener_settings`
//! writes them under the fill-or-replace rules the file config uses.

use {
    super::{FileClientAuthMode, FileConfigError, FileListener, FileTlsConfig},
    crate::{
        SslPrincipalMapper,
        config::BrokerConfig,
        config_keys::{
            AUTO_CREATE_TOPICS_ENABLE, DEFAULT_REPLICATION_FACTOR, DELETE_TOPIC_ENABLE,
            INTER_BROKER_LISTENER_NAME, LOG_ROLL_MS, NUM_PARTITIONS,
        },
        cordoned_log_dirs::CORDONED_LOG_DIRS,
        txn::coordinator::produce_verification::PARTITION_VERIFICATION_ENABLE,
    },
    krabka_security::ListenerProtocol,
    krabka_units::Time,
};

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
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    let origins = &mut cfg.static_config_origins.topic_creation;
    if !origins.num_partitions
        && let Some(value) = properties.get(NUM_PARTITIONS)
    {
        cfg.num_partitions = parse_positive(NUM_PARTITIONS, value)?;
        origins.num_partitions = true;
    }
    if !origins.default_replication_factor
        && let Some(value) = properties.get(DEFAULT_REPLICATION_FACTOR)
    {
        cfg.default_replication_factor = parse_positive(DEFAULT_REPLICATION_FACTOR, value)?;
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
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    if let Some(enabled) = boolean_property(properties, UNSTABLE_API_VERSIONS_ENABLE)? {
        cfg.features.unstable_api_versions = enabled.into();
    }
    if let Some(enabled) = boolean_property(properties, UNSTABLE_FEATURE_VERSIONS_ENABLE)? {
        cfg.features.unstable_feature_versions = enabled.into();
    }
    if let Some(enabled) = boolean_property(properties, DELETE_TOPIC_ENABLE)? {
        cfg.delete_topic_enable = enabled;
        cfg.static_config_origins.topic_admin.delete_topic_enable = true;
    }
    if let Some(enabled) = boolean_property(properties, AUTO_CREATE_TOPICS_ENABLE)? {
        cfg.auto_create_topics_enable = enabled;
        cfg.static_config_origins
            .topic_admin
            .auto_create_topics_enable = true;
    }
    // A dedicated `[runtime]` key or CLI flag wins, and its presence is
    // already recorded as the provenance of the key.
    let verification = PARTITION_VERIFICATION_ENABLE;
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
    cfg: &mut BrokerConfig,
) {
    if cfg.cordoned_log_dirs.is_none()
        && let Some(value) = properties.get(CORDONED_LOG_DIRS)
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
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    use krabka_units::convert::TimeExt as _;

    if let Some(value) = properties.get(LOG_ROLL_MS) {
        let millis: i64 = parse_positive(LOG_ROLL_MS, value)?;
        cfg.log_config.segment_roll_interval = Time::from_millis(millis);
        cfg.static_config_origins
            .supplied_kafka_keys
            .insert(LOG_ROLL_MS);
    }
    Ok(())
}

/// Kafka's `group.consumer.migration.policy`: whether a consumer group may
/// convert between the classic and the consumer protocol while it has
/// members. Kafka reads the value without regard to case, and refuses a value
/// outside its four policies at startup.
fn apply_consumer_group_migration_policy(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut BrokerConfig,
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
    cfg: &mut BrokerConfig,
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
    cfg: &mut BrokerConfig,
    defaults: &BrokerConfig,
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
            .insert(INTER_BROKER_LISTENER_NAME);
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
        cfg.tls_principal_mapper = SslPrincipalMapper::parse(&tls.principal_mapping_rules)
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
    use {
        crate::{
            api_catalog::UnstableApiVersions,
            config::{BrokerConfig, TopicCreationOrigins},
            coordinator::unified::{
                config::ConsumerGroupMigrationPolicy,
                streams::description::TopologyDescriptionPlugin,
            },
            file_config::{
                FileConfig, FileConfigError,
                test_support::{configured, configured_feature, configured_unwrap_parse},
            },
        },
        assert2::{assert, check},
    };

    fn file_outcome<T>(source: &str, read: impl FnOnce(&BrokerConfig) -> T) -> Result<T, String> {
        let file: FileConfig = toml::from_str(source).expect("parse");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg)
            .map(|()| read(&cfg))
            .map_err(|error| error.to_string())
    }

    /// A config value, and whether the operator supplied it.
    #[derive(Debug, PartialEq)]
    struct Supplied<T> {
        value: T,
        supplied: bool,
    }

    fn check_config_case<T: std::fmt::Debug + PartialEq, E: AsRef<str>>(
        label: &str,
        result: &Result<T, String>,
        expected: &Result<T, E>,
    ) {
        let expected_error = expected.as_ref().err().map_or("-", |error| error.as_ref());
        let actual = result
            .as_ref()
            .map_err(|message| message.contains(expected_error));
        assert2::assert!(
            actual == expected.as_ref().map_err(|_| true),
            "{label}: {result:?}"
        );
    }

    fn delete_topic_enable(config: &BrokerConfig) -> Supplied<bool> {
        Supplied {
            value: config.delete_topic_enable,
            supplied: config.static_config_origins.topic_admin.delete_topic_enable,
        }
    }

    fn auto_create_topics_enable(config: &BrokerConfig) -> Supplied<bool> {
        Supplied {
            value: config.auto_create_topics_enable,
            supplied: config
                .static_config_origins
                .topic_admin
                .auto_create_topics_enable,
        }
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
        let cfg = configured_unwrap_parse(src).unwrap();

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
        use {crate::config::BrokerConfig, std::time::Duration};

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
    #[yare::parameterized(
        neither_named = {
            "neither named",
            "broker_id = 0\n",
            Ok((1, 1, TopicCreationOrigins::default())),
        },
        both_as_properties = {
            "both as properties",
            "[server_properties]\n\"num.partitions\" = \"6\"\n\
                     \"default.replication.factor\" = \"3\"\n",
            Ok((6, 3, TopicCreationOrigins {
                num_partitions: true,
                default_replication_factor: true,
            })),
        },
        dedicated_key_wins_over_property = {
            "a dedicated key wins over the property",
            "[runtime]\nnum_partitions = 4\n\
                         [server_properties]\n\"num.partitions\" = \"6\"\n",
            Ok((4, 1, TopicCreationOrigins {
                num_partitions: true,
                default_replication_factor: false,
            })),
        },
        zero_refused = {
            "a zero is refused",
            "[server_properties]\n\"default.replication.factor\" = \"0\"\n",
            Err("server_properties `default.replication.factor` must be a positive integer, got `0`"),
        },
        word_refused = {
            "a word is refused",
            "[server_properties]\n\"num.partitions\" = \"many\"\n",
            Err("server_properties `num.partitions` must be a positive integer, got `many`"),
        },
    )]
    fn apply_to_reads_the_topic_creation_defaults_from_server_properties(
        label: &str,
        source: &str,
        expected: Result<(i32, i16, TopicCreationOrigins), &str>,
    ) {
        let file: FileConfig = toml::from_str(source).expect("parse");
        let mut config = BrokerConfig::default();
        let result = file
            .apply_to(&mut config)
            .map(|()| {
                (
                    config.num_partitions,
                    config.default_replication_factor,
                    config.static_config_origins.topic_creation,
                )
            })
            .map_err(|error| error.to_string());
        check_config_case(label, &result, &expected);
    }

    /// `delete.topic.enable` and `auto.create.topics.enable` under their Kafka
    /// names: booleans Kafka reads case-insensitively, default `true`, and a
    /// value that is neither word refuses the configuration.
    #[yare::parameterized(
        delete_not_named = {
            "delete not named",
            delete_topic_enable,
            "broker_id = 0\n",
            Ok(Supplied { value: true, supplied: false }),
        },
        delete_false = {
            "delete false",
            delete_topic_enable,
            "[server_properties]\n\"delete.topic.enable\" = \"false\"\n",
            Ok(Supplied { value: false, supplied: true }),
        },
        delete_upper_case = {
            "delete upper case",
            delete_topic_enable,
            "[server_properties]\n\"delete.topic.enable\" = \"TRUE\"\n",
            Ok(Supplied { value: true, supplied: true }),
        },
        delete_a_word = {
            "delete a word",
            delete_topic_enable,
            "[server_properties]\n\"delete.topic.enable\" = \"maybe\"\n",
            Err("invalid config: server_properties `delete.topic.enable` must be `true` or `false`, got `maybe`".to_owned()),
        },
        auto_create_not_named = {
            "auto-create not named",
            auto_create_topics_enable,
            "broker_id = 0\n",
            Ok(Supplied { value: true, supplied: false }),
        },
        auto_create_false = {
            "auto-create false",
            auto_create_topics_enable,
            "[server_properties]\n\"auto.create.topics.enable\" = \"False\"\n",
            Ok(Supplied { value: false, supplied: true }),
        },
    )]
    fn boolean_keys_are_read_from_server_properties(
        label: &str,
        read: fn(&BrokerConfig) -> Supplied<bool>,
        source: &str,
        expected: Result<Supplied<bool>, String>,
    ) {
        let actual = file_outcome(source, read);
        assert2::assert!(actual == expected, "{label}: {actual:?}");
    }

    /// Kafka's `log.roll.ms` sets the roll interval of every log whose topic
    /// sets no `segment.ms`, and records that the operator named it. A value
    /// under 1 refuses the configuration, as `KafkaConfig` refuses it. Kafka's
    /// `LogDirFailureTest` sets it to make a broker write into a failed log
    /// directory within seconds.
    #[yare::parameterized(
        not_named = {
            "not named",
            "broker_id = 0\n",
            Ok(Supplied { value: 604_800_000, supplied: false }),
        },
        named = {
            "named",
            "[server_properties]\n\"log.roll.ms\" = \"3000\"\n",
            Ok(Supplied { value: 3000, supplied: true }),
        },
        named_at_segment_default = {
            "named at the segment.ms default",
            "[server_properties]\n\"log.roll.ms\" = \"604800000\"\n",
            Ok(Supplied { value: 604_800_000, supplied: true }),
        },
        zero = {
            "zero",
            "[server_properties]\n\"log.roll.ms\" = \"0\"\n",
            Err(
                "invalid config: server_properties `log.roll.ms` must be a positive integer, got `0`"
                    .to_owned(),
            ),
        },
        a_word = {
            "a word",
            "[server_properties]\n\"log.roll.ms\" = \"soon\"\n",
            Err(
                "invalid config: server_properties `log.roll.ms` must be a positive integer, got `soon`"
                    .to_owned(),
            ),
        },
    )]
    fn log_roll_ms_is_read_from_server_properties(
        label: &str,
        source: &str,
        expected: Result<Supplied<i64>, String>,
    ) {
        use krabka_units::convert::TimeExt as _;

        let result = file_outcome(source, |cfg| Supplied {
            value: cfg.log_config.segment_roll_interval.millis_i64(),
            supplied: cfg
                .static_config_origins
                .supplied_kafka_keys
                .contains("log.roll.ms"),
        });
        check_config_case(label, &result, &expected);
    }

    #[yare::parameterized(
        not_named = {
            "not named",
            "broker_id = 0\n",
            Ok(Supplied {
                value: ConsumerGroupMigrationPolicy::Bidirectional,
                supplied: false,
            }),
        },
        disabled = {
            "disabled",
            "[server_properties]\n\"group.consumer.migration.policy\" = \"disabled\"\n",
            Ok(Supplied {
                value: ConsumerGroupMigrationPolicy::Disabled,
                supplied: true,
            }),
        },
        upper_case = {
            "upper case",
            "[server_properties]\n\"group.consumer.migration.policy\" = \"DOWNGRADE\"\n",
            Ok(Supplied {
                value: ConsumerGroupMigrationPolicy::Downgrade,
                supplied: true,
            }),
        },
        mixed_case = {
            "mixed case",
            "[server_properties]\n\"group.consumer.migration.policy\" = \"Upgrade\"\n",
            Ok(Supplied {
                value: ConsumerGroupMigrationPolicy::Upgrade,
                supplied: true,
            }),
        },
        not_a_policy = {
            "not a policy",
            "[server_properties]\n\"group.consumer.migration.policy\" = \"sideways\"\n",
            Err("invalid config: server_properties `group.consumer.migration.policy` must be one of `disabled`, `upgrade`, `downgrade` or `bidirectional`, got `sideways`".to_owned()),
        },
    )]
    fn the_consumer_group_migration_policy_is_read_from_server_properties(
        label: &str,
        source: &str,
        expected: Result<Supplied<ConsumerGroupMigrationPolicy>, String>,
    ) {
        let result = file_outcome(source, |cfg| Supplied {
            value: cfg.next_gen_consumer_group.migration_policy,
            supplied: cfg
                .static_config_origins
                .supplied_kafka_keys
                .contains("group.consumer.migration.policy"),
        });
        check_config_case(label, &result, &expected);
    }

    /// `transaction.partition.verification.enable`, the static layer of a
    /// dynamic config, comes from the `[runtime]` table or from
    /// `server_properties`. The dedicated `[runtime]` key wins over the
    /// property, and either source records that the operator named the key.
    #[yare::parameterized(
        not_named = {
            "not named",
            "broker_id = 0\n",
            Ok(Supplied { value: true, supplied: false }),
        },
        property_false = {
            "property false",
            "[server_properties]\n\"transaction.partition.verification.enable\" = \"false\"\n",
            Ok(Supplied { value: false, supplied: true }),
        },
        property_upper_case = {
            "property upper case",
            "[server_properties]\n\"transaction.partition.verification.enable\" = \"TRUE\"\n",
            Ok(Supplied { value: true, supplied: true }),
        },
        property_a_word = {
            "property a word",
            "[server_properties]\n\"transaction.partition.verification.enable\" = \"maybe\"\n",
            Err("invalid config: server_properties `transaction.partition.verification.enable` must be `true` or `false`, got `maybe`".to_owned()),
        },
        runtime_false = {
            "runtime false",
            "[runtime]\ntransaction_partition_verification_enable = false\n",
            Ok(Supplied { value: false, supplied: true }),
        },
        runtime_at_default = {
            "runtime at the default",
            "[runtime]\ntransaction_partition_verification_enable = true\n",
            Ok(Supplied { value: true, supplied: true }),
        },
        runtime_true_over_false_property = {
            "runtime true over a false property",
            "[runtime]\ntransaction_partition_verification_enable = true\n\
             [server_properties]\n\"transaction.partition.verification.enable\" = \"false\"\n",
            Ok(Supplied { value: true, supplied: true }),
        },
        runtime_false_over_true_property = {
            "runtime false over a true property",
            "[runtime]\ntransaction_partition_verification_enable = false\n\
             [server_properties]\n\"transaction.partition.verification.enable\" = \"true\"\n",
            Ok(Supplied { value: false, supplied: true }),
        },
    )]
    fn partition_verification_is_read_from_the_runtime_table_and_server_properties(
        label: &str,
        source: &str,
        expected: Result<Supplied<bool>, String>,
    ) {
        const KEY: &str = "transaction.partition.verification.enable";
        let result = file_outcome(source, |cfg| Supplied {
            value: cfg.transaction_partition_verification_enable,
            supplied: cfg.static_config_origins.supplied_kafka_keys.contains(KEY),
        });
        check_config_case(label, &result, &expected);
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
    #[yare::parameterized(
        default_disabled = {
            "broker_id = 0",
            Ok(UnstableApiVersions::Disabled),
        },
        enabled = {
            "[server_properties]\n\"unstable.api.versions.enable\" = \"true\"\n",
            Ok(UnstableApiVersions::Enabled),
        },
        uppercase_false = {
            "[server_properties]\n\"unstable.api.versions.enable\" = \"FALSE\"\n",
            Ok(UnstableApiVersions::Disabled),
        },
        invalid_yes = {
            "[server_properties]\n\"unstable.api.versions.enable\" = \"yes\"\n",
            Err(
                "server_properties `unstable.api.versions.enable` must be `true` or `false`, got `yes`"
                    .to_owned(),
            ),
        },
    )]
    fn apply_to_reads_unstable_api_versions_enable_from_server_properties(
        source: &str,
        expected: Result<UnstableApiVersions, String>,
    ) {
        let applied = configured_feature(source, |cfg| cfg.features.unstable_api_versions);
        assert2::assert!(applied == expected, "{source}");
    }

    /// #784: Kafka's internal `unstable.feature.versions.enable` under its own
    /// name, off by default, refused unless it is a boolean.
    #[yare::parameterized(
        default_disabled = {
            "broker_id = 0",
            Ok(krabka_raft::UnstableFeatureVersions::Disabled),
        },
        enabled = {
            "[server_properties]\n\"unstable.feature.versions.enable\" = \"true\"\n",
            Ok(krabka_raft::UnstableFeatureVersions::Enabled),
        },
        disabled = {
            "[server_properties]\n\"unstable.feature.versions.enable\" = \"false\"\n",
            Ok(krabka_raft::UnstableFeatureVersions::Disabled),
        },
        invalid_one = {
            "[server_properties]\n\"unstable.feature.versions.enable\" = \"1\"\n",
            Err(
                "server_properties `unstable.feature.versions.enable` must be `true` or `false`, got `1`"
                    .to_owned(),
            ),
        },
    )]
    fn apply_to_reads_unstable_feature_versions_enable_from_server_properties(
        source: &str,
        expected: Result<krabka_raft::UnstableFeatureVersions, String>,
    ) {
        let applied = configured_feature(source, |cfg| cfg.features.unstable_feature_versions);
        assert2::assert!(applied == expected, "{source}");
    }

    /// KIP-1331's `group.streams.topology.description.plugin.class`: unset
    /// by default, Kafka's in-memory plugin when it names that class, and
    /// refused for any class krabka cannot load.
    #[yare::parameterized(
        default = { "broker_id = 0", Ok(TopologyDescriptionPlugin::None) },
        in_memory = {
            "[server_properties]\n\"group.streams.topology.description.plugin.class\" = \"org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin\"\n",
            Ok(TopologyDescriptionPlugin::InMemory),
        },
        unsupported_plugin = {
            "[server_properties]\n\"group.streams.topology.description.plugin.class\" = \"com.example.JdbcTopologyStore\"\n",
            Err("server_properties `group.streams.topology.description.plugin.class` names `com.example.JdbcTopologyStore`, which krabka cannot load: the only topology description plugin it builds in is `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`".to_owned()),
        },
    )]
    fn apply_to_reads_the_topology_description_plugin_from_server_properties(
        source: &str,
        expected: Result<TopologyDescriptionPlugin, String>,
    ) {
        let file: FileConfig = toml::from_str(source).unwrap();
        let mut config = BrokerConfig::default();
        let actual = file
            .apply_to(&mut config)
            .map(|()| config.streams_group.topology_description_plugin)
            .map_err(|error| match error {
                FileConfigError::InvalidConfig(message) => message,
                other => other.to_string(),
            });
        assert2::assert!(actual == expected, "{source}");
    }

    #[test]
    fn apply_to_reads_quota_window_properties() {
        use krabka_units::secs;

        let toml = r#"
[server_properties]
"quota.window.num" = "6"
"quota.window.size.seconds" = "2"
"#;
        let cfg = configured_unwrap_parse(toml).unwrap();
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
        let cfg = configured(src, "parse").unwrap();
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
        let cfg = configured(src, "parse").unwrap();
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
    #[yare::parameterized(
        no_rules = { "no rules", "", Some("CN=node-1,OU=brokers,O=krabka") },
        rule = {
            "a rule",
            "principal_mapping_rules = [\"RULE:^CN=(.*?),.*$/$1/\"]\n",
            Some("node-1"),
        },
        rule_matches_nothing = {
            "a rule that matches nothing",
            "principal_mapping_rules = [\"RULE:^OU=(.*?)$/$1/\"]\n",
            None,
        },
    )]
    fn apply_to_parses_top_level_principal_mapping_rules(
        name: &str,
        rules: &str,
        expected: Option<&'static str>,
    ) {
        let base = r#"
[tls_config]
cert_path = "/etc/krabka/tls/node.crt"
key_path = "/etc/krabka/tls/node.key"
"#;
        let dn = "CN=node-1,OU=brokers,O=krabka";
        let cfg = configured(&format!("{base}{rules}"), "parse").unwrap();
        assert2::check!(
            cfg.tls_principal_mapper.apply(dn).as_deref() == expected,
            "{name}"
        );
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
        let cfg = configured(toml, "parse").unwrap();

        assert!(
            cfg.advertised_listener == "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
        );
        // The inter-broker listener wins over the first-declared EXTERNAL one.
        assert!(cfg.advertised_listener != "ext.example.com:9094");
    }
}
