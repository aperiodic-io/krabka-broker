//! The static broker configs `DescribeConfigs` reports beside a node's
//! dynamic overrides: the two KIP-98 transactional-id expiry keys and the two
//! KIP-211 offset-retention keys.
//!
//! Kafka answers `kafka-configs --entity-type brokers --entity-name <id>
//! --describe --all` from the node's own `server.properties`, so a key the
//! operator never set still comes back, at its built-in default. The two
//! KIP-98 transactional-id expiry keys behave exactly that way, and neither is
//! dynamically reconfigurable:
//!
//! ```text
//! transaction.remove.expired.transaction.cleanup.interval.ms=3600000 sensitive=false
//!   synonyms={DEFAULT_CONFIG:transaction.remove.expired.transaction.cleanup.interval.ms=3600000}
//! transactional.id.expiration.ms=604800000 sensitive=false
//!   synonyms={DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! That is `apache/kafka:4.3.1` with neither key set. Setting one moves it to
//! the head of the chain and keeps the built-in default beneath it:
//!
//! ```text
//! transactional.id.expiration.ms=120000 sensitive=false
//!   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=120000,
//!             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! Provenance, not a value comparison, is what puts it there. The same image
//! started with `KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS=604800000` -- the
//! built-in default, written out -- still answers
//!
//! ```text
//! transactional.id.expiration.ms=604800000 sensitive=false
//!   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=604800000,
//!             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! while the key it was not given keeps the one-synonym default chain above.
//! Kafka reads that from `KafkaConfig.originals`, the properties the operator
//! wrote; krabka records it as [`crate::config::StaticConfigOrigins`] while it
//! loads the configuration.
//!
//! Altering either one on the same broker answers
//! `Cannot update these configs dynamically: [transactional.id.expiration.ms]`,
//! which is why the registry marks both rows read-only. The cluster-default
//! broker resource reports neither key at all -- the same probe against
//! `--entity-default` returns no row for them -- so these entries belong to a
//! named broker resource alone.
//!
//! Everything else about an entry -- the type byte, the documentation, the
//! read-only flag -- comes from [`crate::config_keys::registry`], the one
//! table every config surface reads. Both keys are `ConfigDef.Type::INT` with
//! an `atLeast(1)` validator there, read out of the image's
//! `kafka-transaction-coordinator-4.3.1.jar`.
//!
//! The KIP-211 pair -- `offsets.retention.minutes` and
//! `offsets.retention.check.interval.ms` -- reaches this module already
//! reduced to what the operator named, as an [`Option`]: `None` is a key the
//! process never saw and so reports its registry default alone, and `Some` is
//! a value that arrived through the file, CLI, or environment overlay, which
//! Kafka reports at `STATIC_BROKER_CONFIG` whatever the value is. Verified
//! against the same image, where a broker whose properties carry
//! `offsets.retention.minutes=10080` -- Kafka's own default -- answers
//! `synonyms={STATIC_BROKER_CONFIG:...=10080, DEFAULT_CONFIG:...=10080}`.

use {
    super::super::{
        entry::{DefaultLayer, EntryOptions, Layer, config_entry},
        wire::CONFIG_SOURCE_STATIC_BROKER,
    },
    crate::{
        config::{BrokerConfig, KAFKA_STATIC_KEYS, NodeRole},
        config_keys::{
            self,
            registry::{self, ConfigScope},
        },
        cordoned_log_dirs::CORDONED_LOG_DIRS,
    },
    krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult,
    krabka_units::{
        Time,
        convert::{ByteSizeExt as _, TimeExt as _},
    },
};

/// One static broker setting as this node holds it: the value it runs with,
/// and whether its own configuration named the key.
///
/// The key is `ConfigDef.Type::INT` in Kafka, so the value is milliseconds in
/// an `i32`: the broker's own config validation refuses a wider value, and a
/// typed client could not parse one back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerSetting {
    /// The effective value, in milliseconds.
    pub(in crate::handlers::describe_configs) value_ms: i32,
    /// Whether the operator supplied the key. See
    /// [`crate::config::StaticConfigOrigins`]: a config source is provenance,
    /// so a supplied setting heads the chain even when it equals the built-in
    /// default.
    pub(in crate::handlers::describe_configs) supplied: bool,
}

/// The static broker configs this node reports. The handler fills it from
/// [`crate::config::BrokerConfig`].
///
/// The idle-window pair borrows from that configuration rather than copying
/// it, because the per-listener overrides are a map whose size is the number
/// of listeners. [`super::super::static_configs`] turns the pair into entries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerConfigs<'a> {
    /// `transactional.id.expiration.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration: StaticBrokerSetting,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_cleanup_interval:
        StaticBrokerSetting,
    /// `offsets.retention.minutes`, as the operator named it. The
    /// configuration refuses a retention that is not a whole number of
    /// minutes, so the reported value is exact.
    pub(in crate::handlers::describe_configs) offsets_retention: Option<Time>,
    /// `offsets.retention.check.interval.ms`, as the operator named it.
    pub(in crate::handlers::describe_configs) offsets_retention_check_interval: Option<Time>,
    /// KIP-464 `num.partitions`, as the operator named it.
    pub(in crate::handlers::describe_configs) num_partitions: Option<i32>,
    /// KIP-464 `default.replication.factor`, as the operator named it.
    pub(in crate::handlers::describe_configs) default_replication_factor: Option<i16>,
    /// `delete.topic.enable`, as the operator named it.
    pub(in crate::handlers::describe_configs) delete_topic_enable: Option<bool>,
    /// `auto.create.topics.enable`, as the operator named it.
    pub(in crate::handlers::describe_configs) auto_create_topics_enable: Option<bool>,
    /// `connections.max.idle.ms`, as the operator named it. `None` is a key
    /// the process never saw, which reports the registry default alone.
    pub(in crate::handlers::describe_configs) connections_max_idle: Option<Time>,
    /// The `listener.name.<name>.connections.max.idle.ms` overrides, keyed by
    /// listener name. Each is a key per listener rather than a registry row.
    pub(in crate::handlers::describe_configs) connections_max_idle_overrides:
        &'a std::collections::BTreeMap<String, Time>,
    /// The other `KafkaConfig` keys this process runs with a static value
    /// for, by Kafka key. See [`static_settings`].
    pub(in crate::handlers::describe_configs) settings:
        &'a std::collections::BTreeMap<&'static str, String>,
}

/// The `KafkaConfig` keys krabka can state a static value of, for the
/// `STATIC_BROKER_CONFIG` layer of a named broker resource and of a topic key's
/// broker synonym.
///
/// Kafka reports a key at `STATIC_BROKER_CONFIG` when `server.properties`
/// names it, whatever the value is. Some keys are always named: `log.dirs` and
/// `process.roles` are how a node is told where it lives, and krabka always
/// holds them. The listener keys name the listeners of the node's roles, as
/// [`listener_settings`] says. The four log defaults
/// `message.max.bytes`, `log.segment.bytes`, `log.roll.ms` and
/// `min.insync.replicas` are named only when the operator supplied them, which
/// [`crate::config::StaticConfigOrigins`] records, so an inherited default
/// stays at `DEFAULT_CONFIG`, and so do `sasl.server.max.receive.size` and
/// `connection.failed.authentication.delay.ms`. `broker.rack`,
/// `metadata.log.dir` and `cordoned.log.dirs` are named when the node has a
/// value. Kafka reports an unset `metadata.log.dir` as null at
/// `DEFAULT_CONFIG`, and the metadata log is then in the first entry of
/// `log.dirs`.
///
/// The keys of [`crate::config::KAFKA_STATIC_KEYS`] -- the coordinators'
/// `group.*` settings, the internal topics' geometry, the socket and queue
/// limits, and the rest that a `[runtime]` field sets -- are here when the
/// operator named them, which the loader records in
/// [`crate::config::StaticConfigOrigins::supplied_kafka_keys`], or when the
/// node runs a value other than Kafka's default, whatever put it there. The
/// value is the one the node runs with, so `group.consumer.session.timeout.ms`
/// answers 60000 on a node that grants 60 s, on the broker resource and, as the
/// synonym of `consumer.session.timeout.ms`, on a group resource.
///
/// A key absent here is reported at its Kafka default, which is what an
/// operator who never wrote it gets from Kafka too.
pub(crate) fn static_settings(
    config: &BrokerConfig,
) -> std::collections::BTreeMap<&'static str, String> {
    let join_paths = |dirs: Vec<std::path::PathBuf>| {
        dirs.iter()
            .map(|dir| dir.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    let roles: Vec<&str> = config
        .roles
        .iter()
        .map(|role| match role {
            NodeRole::Controller => "controller",
            NodeRole::Broker | NodeRole::Witness => "broker",
        })
        .collect();
    let mut settings = std::collections::BTreeMap::from([
        ("log.dirs", join_paths(config.all_log_dirs())),
        ("process.roles", roles.join(",")),
    ]);
    settings.extend(listener_settings(config));
    if let Some(rack) = &config.rack {
        settings.insert("broker.rack", rack.clone());
    }
    if let Some(dir) = &config.metadata_log_dir {
        settings.insert("metadata.log.dir", dir.display().to_string());
    }
    if let Some(cordoned) = &config.cordoned_log_dirs {
        settings.insert(CORDONED_LOG_DIRS, cordoned.clone());
    }
    let supplied = config.static_config_origins.log;
    if supplied.message_max_bytes {
        settings.insert(
            "message.max.bytes",
            config.log_config.max_message_size.bytes_u64().to_string(),
        );
    }
    if supplied.log_segment_bytes {
        settings.insert(
            "log.segment.bytes",
            config.log_config.segment_size.bytes_u64().to_string(),
        );
    }
    if config
        .static_config_origins
        .supplied_kafka_keys
        .contains(config_keys::LOG_ROLL_MS)
    {
        settings.insert(
            config_keys::LOG_ROLL_MS,
            config
                .log_config
                .segment_roll_interval
                .millis_i64()
                .to_string(),
        );
    }
    if supplied.min_insync_replicas {
        settings.insert(
            config_keys::MIN_INSYNC_REPLICAS,
            config.default_min_insync_replicas.to_string(),
        );
    }
    let authentication = config.static_config_origins.authentication;
    if authentication.sasl_server_max_receive {
        settings.insert(
            "sasl.server.max.receive.size",
            config.sasl_server_max_receive.bytes_u64().to_string(),
        );
    }
    if authentication.connection_failed_authentication_delay {
        settings.insert(
            "connection.failed.authentication.delay.ms",
            config
                .connection_failed_authentication_delay
                .millis_i64()
                .to_string(),
        );
    }
    settings.extend(
        KAFKA_STATIC_KEYS
            .iter()
            .filter_map(|key| key.static_value(config).map(|value| (key.name, value))),
    );
    settings
}

/// The listener keys of this node, as the `server.properties` of a Kafka node
/// with the same `process.roles` names them.
///
/// - `listeners` names the listeners that the node opens: the data-plane
///   listeners on a node with the broker role, then the controller listener on
///   a node with the controller role. `KafkaConfig` refuses a controller-only
///   node whose `listeners` name a listener that `controller.listener.names`
///   does not name, and a broker-only node whose `listeners` name one that it
///   does.
/// - `advertised.listeners` names the advertised data-plane listeners, on a
///   node with the broker role only. krabka takes the advertised endpoint of
///   the controller listener from the voter set. Kafka's
///   `effectiveAdvertisedControllerListeners` does the same when
///   `advertised.listeners` does not name the controller listener. Thus a
///   controller-only node names no advertised listener, and Kafka then reports
///   the key as null at `DEFAULT_CONFIG`, with no synonym.
/// - `listener.security.protocol.map` gives the protocol of each listener in
///   `listeners`, and of the controller listener on a broker-only node too.
///   That node connects to the controller listener, and `KafkaConfig` refuses
///   a broker-only node whose map does not name it.
/// - `controller.listener.names` is `CONTROLLER` on each role. `KafkaConfig`
///   refuses a node that does not name it.
/// - `inter.broker.listener.name` is the name that the operator gave. When the
///   operator gave no name, Kafka uses the listener of
///   `security.inter.broker.protocol` and reports the key as null at
///   `DEFAULT_CONFIG`, so the key is not here.
fn listener_settings(config: &BrokerConfig) -> Vec<(&'static str, String)> {
    use crate::controller_endpoint::CONTROLLER_LISTENER_NAME;

    let data_plane = if config.is_broker() {
        config.effective_listeners()
    } else {
        Vec::new()
    };
    let join = |entries: Vec<String>| entries.join(",");
    let opened = data_plane
        .iter()
        .map(|listener| format!("{}://{}", listener.name, listener.bind_addr))
        .chain(config.is_controller().then(|| {
            format!(
                "{CONTROLLER_LISTENER_NAME}://{}",
                config.controller_listen_addr
            )
        }))
        .collect();
    let protocols = data_plane
        .iter()
        .map(|listener| (listener.name.as_str(), listener.protocol))
        .chain([(
            CONTROLLER_LISTENER_NAME,
            config.controller_listener_protocol,
        )])
        .map(|(name, protocol)| format!("{name}:{}", security_protocol_name(protocol)))
        .collect();
    let mut settings = vec![
        ("listeners", join(opened)),
        ("listener.security.protocol.map", join(protocols)),
        (
            "controller.listener.names",
            CONTROLLER_LISTENER_NAME.to_owned(),
        ),
    ];
    if config.is_broker() {
        let advertised = data_plane
            .iter()
            .map(|listener| format!("{}://{}", listener.name, listener.advertised))
            .collect();
        settings.push(("advertised.listeners", join(advertised)));
    }
    if config
        .static_config_origins
        .supplied_kafka_keys
        .contains(config_keys::INTER_BROKER_LISTENER_NAME)
    {
        settings.push((
            config_keys::INTER_BROKER_LISTENER_NAME,
            config.inter_broker_listener_name.clone(),
        ));
    }
    settings
}

/// Kafka's `SecurityProtocol` name of a listener protocol, which is how
/// `listener.security.protocol.map` spells it.
const fn security_protocol_name(protocol: krabka_security::ListenerProtocol) -> &'static str {
    use krabka_security::ListenerProtocol::{Plaintext, SaslPlaintext, SaslSsl, Ssl};

    match protocol {
        Plaintext => "PLAINTEXT",
        Ssl => "SSL",
        SaslPlaintext => "SASL_PLAINTEXT",
        SaslSsl => "SASL_SSL",
    }
}

/// One static broker entry.
///
/// The value the node runs with sits at `STATIC_BROKER_CONFIG`, with the
/// registry default underneath it as the `DEFAULT_CONFIG` synonym, whenever
/// the value did not reach the node through that default alone. That is the
/// case in either of two ways: the operator named the key -- provenance,
/// which is what Kafka reads out of `KafkaConfig.originals` and reports even
/// for a value identical to the default -- or the node runs something the
/// built-in default does not supply, which is how krabka's own test profile
/// moves the sweep cadence. A node inheriting the default reports it alone,
/// which is the first of the two Kafka outputs quoted above.
fn static_entry(
    key: &str,
    setting: StaticBrokerSetting,
    options: EntryOptions,
) -> DescribeConfigsResourceResult {
    let row = registry::lookup(ConfigScope::Broker, key);
    let default = row.and_then(|row| row.default);
    let rendered = setting.value_ms.to_string();
    let layers: Vec<Layer<'_>> = (setting.supplied || default != Some(rendered.as_str()))
        .then_some(Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: key,
            value: &rendered,
        })
        .into_iter()
        .collect();
    config_entry(
        row,
        key,
        &layers,
        DefaultLayer {
            value: default,
            name: Some(key),
        },
        options,
    )
}

/// One static broker entry the caller has already reduced to what the
/// operator named.
///
/// `Some` heads the chain at `STATIC_BROKER_CONFIG` with the registry default
/// beneath it; `None` reports that default alone. This is the same rule
/// [`static_entry`] applies, stated over a provenance the caller resolved
/// rather than over a value the node runs with.
fn named_entry(
    key: &'static str,
    set_by_operator: Option<&str>,
    options: EntryOptions,
) -> DescribeConfigsResourceResult {
    let row = registry::lookup(ConfigScope::Broker, key);
    let layers: Vec<Layer<'_>> = set_by_operator
        .map(|value| Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: key,
            value,
        })
        .into_iter()
        .collect();
    config_entry(
        row,
        key,
        &layers,
        DefaultLayer {
            value: row.and_then(|row| row.default),
            name: Some(key),
        },
        options,
    )
}

/// Every static broker entry for one node, filtered by the request's
/// `configuration_keys`.
pub(super) fn static_broker_entries(
    configs: StaticBrokerConfigs<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let mut entries: Vec<DescribeConfigsResourceResult> = [
        (
            config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            configs.txn_id_expiration,
        ),
        (
            config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            configs.txn_id_expiration_cleanup_interval,
        ),
    ]
    .into_iter()
    .filter(|(key, _)| wanted(key))
    .map(|(key, setting)| static_entry(key, setting, options))
    .collect();
    entries.extend(
        [
            (
                config_keys::OFFSETS_RETENTION_MINUTES,
                configs
                    .offsets_retention
                    .map(|retention| (retention.millis_i64() / 60_000).to_string()),
            ),
            (
                config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                configs
                    .offsets_retention_check_interval
                    .map(|interval| interval.millis_i64().to_string()),
            ),
            (
                config_keys::NUM_PARTITIONS,
                configs.num_partitions.map(|count| count.to_string()),
            ),
            (
                config_keys::DEFAULT_REPLICATION_FACTOR,
                configs
                    .default_replication_factor
                    .map(|factor| factor.to_string()),
            ),
            (
                config_keys::DELETE_TOPIC_ENABLE,
                configs
                    .delete_topic_enable
                    .map(|enabled| enabled.to_string()),
            ),
            (
                config_keys::AUTO_CREATE_TOPICS_ENABLE,
                configs
                    .auto_create_topics_enable
                    .map(|enabled| enabled.to_string()),
            ),
        ]
        .into_iter()
        .filter(|(key, _)| wanted(key))
        .map(|(key, set_by_operator)| named_entry(key, set_by_operator.as_deref(), options)),
    );
    entries
}

/// The values a broker that never touched either key runs with. The
/// `describe_one` tests pass it so their expectations read as Kafka's
/// out-of-the-box `--describe --all` output.
#[cfg(test)]
pub(in crate::handlers::describe_configs) fn kafka_default_static_broker()
-> StaticBrokerConfigs<'static> {
    StaticBrokerConfigs {
        txn_id_expiration: StaticBrokerSetting {
            value_ms: 604_800_000,
            supplied: false,
        },
        txn_id_expiration_cleanup_interval: StaticBrokerSetting {
            value_ms: 3_600_000,
            supplied: false,
        },
        offsets_retention: None,
        offsets_retention_check_interval: None,
        num_partitions: None,
        default_replication_factor: None,
        delete_topic_enable: None,
        auto_create_topics_enable: None,
        connections_max_idle: None,
        connections_max_idle_overrides: NO_IDLE_OVERRIDES.get_or_init(Default::default),
        settings: NO_SETTINGS.get_or_init(Default::default),
    }
}

/// The empty static-settings map [`kafka_default_static_broker`] borrows: a
/// process that names none of the keys [`static_settings`] states.
#[cfg(test)]
static NO_SETTINGS: std::sync::OnceLock<std::collections::BTreeMap<&'static str, String>> =
    std::sync::OnceLock::new();

/// The empty override map [`kafka_default_static_broker`] borrows, so the
/// helper can hand back a `'static` view without each caller owning a map.
#[cfg(test)]
static NO_IDLE_OVERRIDES: std::sync::OnceLock<std::collections::BTreeMap<String, Time>> =
    std::sync::OnceLock::new();

#[cfg(test)]
mod tests;
