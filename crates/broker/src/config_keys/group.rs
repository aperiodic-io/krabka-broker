//! Kafka's `GroupConfig`: the keys a `GROUP` resource carries, their types,
//! defaults and broker synonyms, and `GroupConfig.validate`.
//!
//! Kafka trunk defines 27 group keys, ten of which Kafka 4.3.1 does not
//! have ([`KAFKA_TRUNK_GROUP_KEYS`]). Those ten are reported and accepted
//! only under `unstable.api.versions.enable`; otherwise the group resource is
//! 4.3.1's 17, and naming one in an alter is refused as 4.3.1 refuses an
//! unknown key.
//!
//! An alter accepts and validates every key the broker serves, as Kafka's
//! `ControllerConfigurationValidator` does: it has no filter for the keys a
//! coordinator applies. Each coordinator reads the keys it applies out of the
//! group's stored override map, over the broker's value, and ignores the
//! others: the consumer coordinator applies the three `consumer.*` keys, the
//! share coordinator the `share.*` session, heartbeat and assignment-interval
//! keys, its share partitions the other `share.*` keys, and the streams
//! coordinator the `streams.*` keys. At `share.version` 2 the share
//! partitions also apply the two `errors.deadletterqueue.*` keys (KIP-1191).
//! The assignor offload keys are stored and reported, and not applied
//! (`NOT_APPLIED`, and `docs/KIP_MATRIX.md`).

use std::collections::BTreeMap;

use super::{
    kafka_broker::roster,
    parse::{check_one_of, check_range, int_value, java_trim, parse_bool, parse_int, parse_long},
};
use crate::coordinator::unified::{
    config::NextGenConfig,
    share::config::ShareGroupConfig,
    streams::config::{
        KEY_ACCEPTABLE_RECOVERY_LAG, KEY_ASSIGNOR_NAME, KEY_HEARTBEAT_INTERVAL_MS,
        KEY_NUM_STANDBY_REPLICAS, KEY_NUM_WARMUP_REPLICAS, KEY_RACK_AWARE_ASSIGNMENT_TAGS,
        KEY_SESSION_TIMEOUT_MS, KEY_SHARE_AUTO_OFFSET_RESET, KEY_TASK_OFFSET_INTERVAL_MS,
        ShareAutoOffsetReset, StreamsGroupConfig,
    },
};

/// One key of Kafka's `GroupConfig.CONFIG_DEF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupKey {
    pub(crate) name: &'static str,
    pub(crate) config_type: super::registry::ConfigType,
    /// Kafka's default, `ConfigDef.convertToString` of `defaultValue`.
    pub(crate) default: Option<&'static str>,
    /// `GroupConfig.ALL_GROUP_CONFIG_SYNONYMS`: the broker key that sets the
    /// value for every group that does not override it.
    pub(crate) broker_synonym: Option<&'static str>,
}

const fn group_key(
    name: &'static str,
    config_type: super::registry::ConfigType,
    default: Option<&'static str>,
    broker_synonym: Option<&'static str>,
) -> GroupKey {
    GroupKey {
        name,
        config_type,
        default,
        broker_synonym,
    }
}

/// Every key of Kafka trunk's `GroupConfig.CONFIG_DEF`, in the order
/// `DescribeConfigs` lists them (by name).
pub(crate) const KAFKA_GROUP_KEYS: &[GroupKey] = {
    use super::registry::ConfigType::{Boolean, Int, List, Long, String};
    roster! {
        group_key("consumer.assignment.interval.ms", Int, Some("1000"), Some("group.consumer.assignment.interval.ms")),
        group_key("consumer.assignor.offload.enable", Boolean, Some("true"), Some("group.consumer.assignor.offload.enable")),
        group_key("consumer.heartbeat.interval.ms", Int, Some("5000"), Some("group.consumer.heartbeat.interval.ms")),
        group_key("consumer.session.timeout.ms", Int, Some("45000"), Some("group.consumer.session.timeout.ms")),
        group_key("errors.deadletterqueue.copy.record.enable", Boolean, Some("false"), None),
        group_key("errors.deadletterqueue.topic.name", String, Some(""), None),
        group_key("share.assignment.interval.ms", Int, Some("1000"), Some("group.share.assignment.interval.ms")),
        group_key("share.assignor.offload.enable", Boolean, Some("true"), Some("group.share.assignor.offload.enable")),
        group_key("share.auto.offset.reset", String, Some("latest"), None),
        group_key("share.delivery.count.limit", Int, Some("5"), Some("group.share.delivery.count.limit")),
        group_key("share.heartbeat.interval.ms", Int, Some("5000"), Some("group.share.heartbeat.interval.ms")),
        group_key("share.isolation.level", String, Some("read_uncommitted"), None),
        group_key("share.partition.max.record.locks", Int, Some("2000"), Some("group.share.partition.max.record.locks")),
        group_key("share.record.lock.duration.ms", Int, Some("30000"), Some("group.share.record.lock.duration.ms")),
        group_key("share.renew.acknowledge.enable", Boolean, Some("true"), None),
        group_key("share.session.timeout.ms", Int, Some("45000"), Some("group.share.session.timeout.ms")),
        group_key("streams.acceptable.recovery.lag", Long, Some("10000"), Some("group.streams.acceptable.recovery.lag")),
        group_key("streams.assignment.interval.ms", Int, Some("1000"), Some("group.streams.assignment.interval.ms")),
        group_key("streams.assignor.name", String, None, Some("group.streams.assignors")),
        group_key("streams.assignor.offload.enable", Boolean, Some("true"), Some("group.streams.assignor.offload.enable")),
        group_key("streams.heartbeat.interval.ms", Int, Some("5000"), Some("group.streams.heartbeat.interval.ms")),
        group_key("streams.initial.rebalance.delay.ms", Int, Some("3000"), Some("group.streams.initial.rebalance.delay.ms")),
        group_key("streams.num.standby.replicas", Int, Some("0"), Some("group.streams.num.standby.replicas")),
        group_key("streams.num.warmup.replicas", Int, Some("2"), Some("group.streams.num.warmup.replicas")),
        group_key("streams.rack.aware.assignment.tags", List, Some(""), Some("group.streams.rack.aware.assignment.tags")),
        group_key("streams.session.timeout.ms", Int, Some("45000"), Some("group.streams.session.timeout.ms")),
        group_key("streams.task.offset.interval.ms", Int, Some("60000"), Some("group.streams.task.offset.interval.ms")),
    }
};

/// The group keys Kafka trunk's `GroupConfig` defines and Kafka 4.3.1's does
/// not. Kafka 4.3.1 declares the constants of the three
/// `*.assignor.offload.enable` keys and never `define`s them, so it too
/// answers `Unknown group config name` for them.
pub(crate) const KAFKA_TRUNK_GROUP_KEYS: &[&str] = &[
    "consumer.assignor.offload.enable",
    "errors.deadletterqueue.copy.record.enable",
    "errors.deadletterqueue.topic.name",
    "share.assignor.offload.enable",
    "streams.acceptable.recovery.lag",
    "streams.assignor.name",
    "streams.assignor.offload.enable",
    "streams.num.warmup.replicas",
    "streams.rack.aware.assignment.tags",
    "streams.task.offset.interval.ms",
];

/// The group keys a broker serves under `unstable`: Kafka 4.3.1's
/// `GroupConfig` by default, trunk's with unstable api versions enabled.
pub(crate) fn served_group_keys(
    unstable: crate::api_catalog::UnstableApiVersions,
) -> impl Iterator<Item = &'static GroupKey> {
    KAFKA_GROUP_KEYS.iter().filter(move |key| {
        unstable == crate::api_catalog::UnstableApiVersions::Enabled
            || !KAFKA_TRUNK_GROUP_KEYS.contains(&key.name)
    })
}

/// The row for one group key served under `unstable`.
pub(crate) fn kafka_group_key(
    name: &str,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> Option<&'static GroupKey> {
    served_group_keys(unstable).find(|key| key.name == name)
}

/// The group keys krabka's coordinators accept and store and do not apply:
/// the assignor offload keys, because krabka runs its assignors in the group's
/// own actor. Every other key of Kafka's `GroupConfig` is applied by the
/// coordinator of its protocol; the two `errors.deadletterqueue.*` keys act
/// once `share.version` is 2.
const NOT_APPLIED: &[&str] = &[
    "consumer.assignor.offload.enable",
    "share.assignor.offload.enable",
    "streams.assignor.offload.enable",
];

/// A registry-shaped row for a Kafka group key that has none of its own,
/// which `DescribeConfigs` reports typed and disclosed, at the broker's value.
pub(crate) fn group_row(key: &GroupKey) -> super::registry::ConfigKey {
    let doc = if NOT_APPLIED.contains(&key.name) {
        "A Kafka group config this broker accepts, stores and reports, and does not apply yet."
    } else {
        "A Kafka group config, stored per group and applied by this broker."
    };
    super::registry::ConfigKey {
        name: key.name,
        scope: super::registry::ConfigScope::Group,
        config_type: key.config_type,
        type_note: None,
        default: key.default,
        doc,
        read_only: false,
        sensitive: false,
        internal: false,
        kip: None,
        cluster_default: None,
        check: super::registry::ValueCheck::Parsed,
    }
}

/// Kafka's `group.streams.max.standby.replicas` default.
const MAX_STANDBY_REPLICAS: i32 = 2;
/// Kafka's `group.streams.max.warmup.replicas` default.
const MAX_WARMUP_REPLICAS: i32 = 20;
/// Kafka's `group.streams.min.task.offset.interval.ms` default.
const MIN_TASK_OFFSET_INTERVAL_MS: i32 = 15_000;
/// Kafka's `group.{consumer,share,streams}.min.assignment.interval.ms`
/// default, which krabka does not make configurable.
const MIN_ASSIGNMENT_INTERVAL_MS: i32 = 0;
/// Kafka's `group.{consumer,share,streams}.max.assignment.interval.ms`
/// default, which krabka does not make configurable.
const MAX_ASSIGNMENT_INTERVAL_MS: i32 = 15_000;
/// The task assignors krabka's streams coordinator registers, which is what
/// `group.streams.assignors` names on a Kafka broker.
const REGISTERED_ASSIGNORS: &[&str] = &["auto", "sticky", "highly_available"];

/// The values `share.isolation.level` accepts, which are
/// `IsolationLevel.toString()`.
const ISOLATION_LEVELS: &[&str] = &["read_committed", "read_uncommitted"];

/// `GroupConfig.CONFIG_DEF`'s definition order, trunk's. `ConfigDef.parse`
/// reads the keys in this order, so the first bad value in it is the one the
/// answer names.
const DEFINITION_ORDER: &[&str] = &[
    "consumer.session.timeout.ms",
    "consumer.heartbeat.interval.ms",
    "consumer.assignment.interval.ms",
    "consumer.assignor.offload.enable",
    "share.session.timeout.ms",
    "share.heartbeat.interval.ms",
    "share.record.lock.duration.ms",
    "share.delivery.count.limit",
    "share.partition.max.record.locks",
    "share.auto.offset.reset",
    "share.isolation.level",
    "share.renew.acknowledge.enable",
    "share.assignment.interval.ms",
    "share.assignor.offload.enable",
    "streams.session.timeout.ms",
    "streams.heartbeat.interval.ms",
    "streams.num.standby.replicas",
    "streams.initial.rebalance.delay.ms",
    "streams.assignment.interval.ms",
    "streams.assignor.offload.enable",
    KEY_TASK_OFFSET_INTERVAL_MS,
    KEY_NUM_WARMUP_REPLICAS,
    KEY_RACK_AWARE_ASSIGNMENT_TAGS,
    KEY_ACCEPTABLE_RECOVERY_LAG,
    KEY_ASSIGNOR_NAME,
    "errors.deadletterqueue.topic.name",
    "errors.deadletterqueue.copy.record.enable",
];

/// The broker settings a group override is checked against: each coordinator's
/// defaults and its `group.<protocol>.min.*` / `max.*` bounds, which Kafka's
/// `GroupConfig.validate` reads off `GroupCoordinatorConfig` and
/// `ShareGroupConfig`.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupBounds {
    pub(crate) consumer: NextGenConfig,
    pub(crate) share: ShareGroupConfig,
    pub(crate) streams: StreamsGroupConfig,
}

impl GroupBounds {
    /// The bounds `config` runs its coordinators with.
    #[must_use]
    pub(crate) fn of(config: &crate::config::BrokerConfig) -> Self {
        Self {
            consumer: (*config.next_gen_consumer_group).clone(),
            share: (*config.share_group).clone(),
            streams: (*config.streams_group).clone(),
        }
    }
}

fn millis(duration: std::time::Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

/// The `ConfigDef` validator floor of an `INT` or `LONG` group key
/// (`atLeast(n)`), when it has one.
fn at_least(name: &str) -> Option<i32> {
    match name {
        "consumer.session.timeout.ms"
        | "consumer.heartbeat.interval.ms"
        | "share.session.timeout.ms"
        | "share.heartbeat.interval.ms"
        | "streams.session.timeout.ms"
        | "streams.heartbeat.interval.ms"
        | KEY_TASK_OFFSET_INTERVAL_MS => Some(1),
        "consumer.assignment.interval.ms"
        | "share.assignment.interval.ms"
        | "streams.assignment.interval.ms"
        | "streams.initial.rebalance.delay.ms"
        | "streams.num.standby.replicas"
        | KEY_NUM_WARMUP_REPLICAS
        | KEY_ACCEPTABLE_RECOVERY_LAG => Some(0),
        "share.record.lock.duration.ms" => Some(1_000),
        "share.delivery.count.limit" => Some(2),
        "share.partition.max.record.locks" => Some(100),
        _ => None,
    }
}

/// `ConfigDef.parse` of one group override: its type and its own validator.
fn parse_value(key: &GroupKey, value: &str) -> Result<(), String> {
    use super::registry::ConfigType;

    let name = key.name;
    match key.config_type {
        ConfigType::Int => {
            check_range(name, parse_int(name, value)?, at_least(name), None).map(drop)
        }
        ConfigType::Long => check_range(
            name,
            parse_long(name, value)?,
            at_least(name).map(i64::from),
            None,
        )
        .map(drop),
        ConfigType::Boolean => parse_bool(name, value).map(drop),
        ConfigType::String => match name {
            "share.isolation.level" => check_one_of(name, value, ISOLATION_LEVELS).map(drop),
            KEY_SHARE_AUTO_OFFSET_RESET => ShareAutoOffsetReset::parse(value).map(drop),
            _ => Ok(()),
        },
        // The one `LIST` key is `streams.rack.aware.assignment.tags`, which
        // the streams coordinator's own parser checks.
        _ => Ok(()),
    }
}

/// `GroupConfig.validateValues`' broker-bound checks, over the override map.
///
/// Kafka 4.3.1 refuses a value outside `[group.<protocol>.min.<key>,
/// group.<protocol>.max.<key>]` naming the broker key it broke, as
/// `<key> must be greater than or equal to <min key>` or `... less than or
/// equal to <max key>`. Trunk states the range instead, as `<key> must be in
/// the range <min> to <max> inclusive.`, so that is what a broker serving
/// `unstable.api.versions.enable` answers.
fn check_bounds(
    overrides: &BTreeMap<String, String>,
    bounds: &GroupBounds,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> Result<(), String> {
    let trunk = unstable == crate::api_catalog::UnstableApiVersions::Enabled;
    let int = |name: &str| overrides.get(name).and_then(|value| int_value(value));
    let range = |name: &str, min: (i32, &str), max: (i32, &str)| -> Result<(), String> {
        let Some(value) = int(name) else {
            return Ok(());
        };
        if trunk && (value < min.0 || value > max.0) {
            return Err(format!(
                "{name} must be in the range {} to {} inclusive.",
                min.0, max.0
            ));
        }
        if value < min.0 {
            return Err(format!("{name} must be greater than or equal to {}", min.1));
        }
        if value > max.0 {
            return Err(format!("{name} must be less than or equal to {}", max.1));
        }
        Ok(())
    };
    let assignment = |name: &str, protocol: &str| {
        range(
            name,
            (
                MIN_ASSIGNMENT_INTERVAL_MS,
                format!("group.{protocol}.min.assignment.interval.ms").as_str(),
            ),
            (
                MAX_ASSIGNMENT_INTERVAL_MS,
                format!("group.{protocol}.max.assignment.interval.ms").as_str(),
            ),
        )
    };
    let (consumer, share, streams) = (&bounds.consumer, &bounds.share, &bounds.streams);
    range(
        "consumer.heartbeat.interval.ms",
        (
            millis(consumer.min_heartbeat_interval),
            "group.consumer.min.heartbeat.interval.ms",
        ),
        (
            millis(consumer.max_heartbeat_interval),
            "group.consumer.max.heartbeat.interval.ms",
        ),
    )?;
    range(
        "consumer.session.timeout.ms",
        (
            millis(consumer.min_session_timeout),
            "group.consumer.min.session.timeout.ms",
        ),
        (
            millis(consumer.max_session_timeout),
            "group.consumer.max.session.timeout.ms",
        ),
    )?;
    assignment("consumer.assignment.interval.ms", "consumer")?;
    range(
        "share.heartbeat.interval.ms",
        (
            millis(share.min_heartbeat_interval),
            "group.share.min.heartbeat.interval.ms",
        ),
        (
            millis(share.max_heartbeat_interval),
            "group.share.max.heartbeat.interval.ms",
        ),
    )?;
    range(
        "share.session.timeout.ms",
        (
            millis(share.min_session_timeout),
            "group.share.min.session.timeout.ms",
        ),
        (
            millis(share.max_session_timeout),
            "group.share.max.session.timeout.ms",
        ),
    )?;
    range(
        "share.record.lock.duration.ms",
        (
            millis(share.min_record_lock_duration),
            "group.share.min.record.lock.duration.ms",
        ),
        (
            millis(share.max_record_lock_duration),
            "group.share.max.record.lock.duration.ms",
        ),
    )?;
    range(
        "share.delivery.count.limit",
        (
            i32::from(share.min_delivery_count_limit),
            "group.share.min.delivery.count.limit",
        ),
        (
            i32::from(share.max_delivery_count_limit),
            "group.share.max.delivery.count.limit",
        ),
    )?;
    range(
        "share.partition.max.record.locks",
        (
            share.min_partition_max_record_locks,
            "group.share.min.partition.max.record.locks",
        ),
        (
            share.max_partition_max_record_locks,
            "group.share.max.partition.max.record.locks",
        ),
    )?;
    assignment("share.assignment.interval.ms", "share")?;
    range(
        KEY_HEARTBEAT_INTERVAL_MS,
        (
            millis(streams.min_heartbeat_interval),
            "group.streams.min.heartbeat.interval.ms",
        ),
        (
            millis(streams.max_heartbeat_interval),
            "group.streams.max.heartbeat.interval.ms",
        ),
    )?;
    range(
        KEY_SESSION_TIMEOUT_MS,
        (
            millis(streams.min_session_timeout),
            "group.streams.min.session.timeout.ms",
        ),
        (
            millis(streams.max_session_timeout),
            "group.streams.max.session.timeout.ms",
        ),
    )?;
    if int(KEY_NUM_STANDBY_REPLICAS).is_some_and(|value| value > MAX_STANDBY_REPLICAS) {
        return Err(format!(
            "{KEY_NUM_STANDBY_REPLICAS} must be less than or equal to {}",
            if trunk {
                MAX_STANDBY_REPLICAS.to_string()
            } else {
                "group.streams.max.standby.replicas".to_owned()
            }
        ));
    }
    assignment("streams.assignment.interval.ms", "streams")?;
    if trunk {
        check_trunk_streams_bounds(overrides)?;
    }
    for (session, heartbeat, default_session, default_heartbeat) in [
        (
            "consumer.session.timeout.ms",
            "consumer.heartbeat.interval.ms",
            millis(consumer.session_timeout),
            millis(consumer.heartbeat_interval),
        ),
        (
            "share.session.timeout.ms",
            "share.heartbeat.interval.ms",
            millis(share.session_timeout),
            millis(share.heartbeat_interval),
        ),
        (
            KEY_SESSION_TIMEOUT_MS,
            KEY_HEARTBEAT_INTERVAL_MS,
            millis(streams.session_timeout),
            millis(streams.heartbeat_interval),
        ),
    ] {
        if (int(session).is_some() || int(heartbeat).is_some())
            && int(session).unwrap_or(default_session)
                <= int(heartbeat).unwrap_or(default_heartbeat)
        {
            return Err(format!("{session} must be greater than {heartbeat}"));
        }
    }
    // Trunk keeps the reserved `__` prefix for internal topics.
    if trunk
        && overrides
            .get("errors.deadletterqueue.topic.name")
            .is_some_and(|name| java_trim(name).starts_with("__"))
    {
        return Err(
            "errors.deadletterqueue.topic.name: DLQ topic name must not start with '__'".to_owned(),
        );
    }
    Ok(())
}

/// The trunk-only bounds of `GroupConfig.validateValues`: the task offset
/// interval floor, the warmup replica ceiling, and the registered assignor.
fn check_trunk_streams_bounds(overrides: &BTreeMap<String, String>) -> Result<(), String> {
    let int = |name: &str| overrides.get(name).and_then(|value| int_value(value));
    if let Some(value) = int(KEY_TASK_OFFSET_INTERVAL_MS)
        && value < MIN_TASK_OFFSET_INTERVAL_MS
    {
        return Err(format!(
            "{KEY_TASK_OFFSET_INTERVAL_MS} must be greater than or equal to \
             {MIN_TASK_OFFSET_INTERVAL_MS}"
        ));
    }
    if let Some(value) = int(KEY_NUM_WARMUP_REPLICAS)
        && value > MAX_WARMUP_REPLICAS
    {
        return Err(format!(
            "{KEY_NUM_WARMUP_REPLICAS} must be less than or equal to {MAX_WARMUP_REPLICAS}"
        ));
    }
    if let Some(value) = overrides.get(KEY_ASSIGNOR_NAME)
        && !REGISTERED_ASSIGNORS.contains(&java_trim(value))
    {
        return Err(format!(
            "{KEY_ASSIGNOR_NAME} '{}' is not a registered task assignor. Registered assignors \
             are: [{}].",
            java_trim(value),
            REGISTERED_ASSIGNORS.join(", ")
        ));
    }
    Ok(())
}

/// Kafka's `GroupConfig.validate` over a group's resulting override map,
/// with the broker bounds `bounds` carries. A key the broker does not serve
/// under `unstable` is `Unknown group config name`, Kafka's
/// `GroupConfig.validateNames` text.
///
/// Every key the broker serves is accepted and validated, whether or not a
/// coordinator of krabka applies it yet: Kafka's `ControllerConfigurationValidator`
/// has no such filter. See `docs/KIP_MATRIX.md` for the ones that are stored
/// and not applied.
///
/// # Errors
/// Returns Kafka's `InvalidConfigurationException` or `ConfigException`
/// text, which the alter paths answer with `INVALID_CONFIG`.
pub(crate) fn validate_group_configs(
    overrides: &BTreeMap<String, String>,
    bounds: &GroupBounds,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> Result<(), String> {
    for name in overrides.keys() {
        if kafka_group_key(name, unstable).is_none() {
            return Err(format!("Unknown group config name: {name}"));
        }
    }
    for name in DEFINITION_ORDER {
        if let (Some(value), Some(key)) = (overrides.get(*name), kafka_group_key(name, unstable)) {
            parse_value(key, value)?;
        }
    }
    check_bounds(overrides, bounds, unstable)?;
    // The rack-aware tags, the acceptable recovery lag and the strategy of
    // `share.auto.offset.reset` are the applier's own parsers.
    bounds.streams.with_group_overrides(overrides).map(drop)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        api_catalog::UnstableApiVersions::{self, Disabled, Enabled},
        test_support::config_map as overrides,
    };

    /// One row of a table: a key and value, and the refusal text or `Ok`.
    type Row = ((&'static str, &'static str), Result<(), &'static str>);

    fn validate(pairs: &[(&str, &str)], unstable: UnstableApiVersions) -> Result<(), String> {
        validate_group_configs(&overrides(pairs), &GroupBounds::default(), unstable)
    }

    /// Kafka trunk's `GroupConfig.validate`, under
    /// `unstable.api.versions.enable`: the range text states both bounds.
    #[test]
    fn group_overrides_are_validated_with_trunks_rules_and_messages() {
        let cases: [Row; 9] = [
            (
                ("not.a.group.key", "1"),
                Err("Unknown group config name: not.a.group.key"),
            ),
            (
                (KEY_NUM_STANDBY_REPLICAS, "3"),
                Err("streams.num.standby.replicas must be less than or equal to 2"),
            ),
            (
                (KEY_NUM_WARMUP_REPLICAS, "21"),
                Err("streams.num.warmup.replicas must be less than or equal to 20"),
            ),
            (
                (KEY_TASK_OFFSET_INTERVAL_MS, "14999"),
                Err("streams.task.offset.interval.ms must be greater than or equal to 15000"),
            ),
            (
                (KEY_SESSION_TIMEOUT_MS, "90000"),
                Err("streams.session.timeout.ms must be in the range 45000 to 60000 inclusive."),
            ),
            (
                (KEY_SESSION_TIMEOUT_MS, "abc"),
                Err(
                    "Invalid value abc for configuration streams.session.timeout.ms: Not a \
                     number of type INT",
                ),
            ),
            (
                (KEY_ASSIGNOR_NAME, "range"),
                Err(
                    "streams.assignor.name 'range' is not a registered task assignor. \
                     Registered assignors are: [auto, sticky, highly_available].",
                ),
            ),
            ((KEY_NUM_STANDBY_REPLICAS, "2"), Ok(())),
            ((KEY_SESSION_TIMEOUT_MS, "60000"), Ok(())),
        ];
        for ((key, value), want) in cases {
            check!(
                validate(&[(key, value)], Enabled) == want.map_err(str::to_owned),
                "{key}={value}"
            );
        }
    }

    /// Kafka 4.3.1's `GroupConfig.validateValues` names the broker key a value
    /// broke, and accepts every key of its `CONFIG_DEF`, whatever krabka's
    /// coordinators apply. Each row is a key, a value and the outcome.
    #[test]
    fn group_overrides_are_validated_with_kafka_4_3_1s_rules_and_messages() {
        let cases: [Row; 22] = [
            (("consumer.session.timeout.ms", "50000"), Ok(())),
            (("consumer.heartbeat.interval.ms", "6000"), Ok(())),
            (("consumer.assignment.interval.ms", "500"), Ok(())),
            (("share.session.timeout.ms", "50000"), Ok(())),
            (("share.heartbeat.interval.ms", "6000"), Ok(())),
            (("share.record.lock.duration.ms", "45000"), Ok(())),
            (("share.delivery.count.limit", "7"), Ok(())),
            (("share.partition.max.record.locks", "1000"), Ok(())),
            (("share.isolation.level", "read_committed"), Ok(())),
            (("share.renew.acknowledge.enable", "false"), Ok(())),
            (("share.assignment.interval.ms", "0"), Ok(())),
            (("streams.initial.rebalance.delay.ms", "0"), Ok(())),
            (
                ("consumer.session.timeout.ms", "1000"),
                Err(
                    "consumer.session.timeout.ms must be greater than or equal to \
                     group.consumer.min.session.timeout.ms",
                ),
            ),
            (
                ("consumer.heartbeat.interval.ms", "20000"),
                Err(
                    "consumer.heartbeat.interval.ms must be less than or equal to \
                     group.consumer.max.heartbeat.interval.ms",
                ),
            ),
            (
                ("share.record.lock.duration.ms", "500"),
                Err(
                    "Invalid value 500 for configuration share.record.lock.duration.ms: Value \
                     must be at least 1000",
                ),
            ),
            (
                ("share.record.lock.duration.ms", "3600000"),
                Err(
                    "share.record.lock.duration.ms must be less than or equal to \
                     group.share.max.record.lock.duration.ms",
                ),
            ),
            (
                ("share.delivery.count.limit", "1"),
                Err(
                    "Invalid value 1 for configuration share.delivery.count.limit: Value must \
                     be at least 2",
                ),
            ),
            (
                ("share.isolation.level", "read_everything"),
                Err(
                    "Invalid value read_everything for configuration share.isolation.level: \
                     String must be one of: read_committed, read_uncommitted",
                ),
            ),
            (
                ("share.renew.acknowledge.enable", "maybe"),
                Err(
                    "Invalid value maybe for configuration share.renew.acknowledge.enable: \
                     Expected value to be either true or false",
                ),
            ),
            (
                ("streams.session.timeout.ms", "90000"),
                Err("streams.session.timeout.ms must be less than or equal to \
                     group.streams.max.session.timeout.ms"),
            ),
            (
                ("streams.num.standby.replicas", "3"),
                Err(
                    "streams.num.standby.replicas must be less than or equal to \
                     group.streams.max.standby.replicas",
                ),
            ),
            (
                ("streams.assignment.interval.ms", "20000"),
                Err(
                    "streams.assignment.interval.ms must be less than or equal to \
                     group.streams.max.assignment.interval.ms",
                ),
            ),
        ];
        for ((key, value), want) in cases {
            check!(
                validate(&[(key, value)], Disabled) == want.map_err(str::to_owned),
                "{key}={value}"
            );
        }
    }

    /// The parse runs in `CONFIG_DEF`'s order, so the first bad value in it
    /// is the one named, whatever the names sort to.
    #[test]
    fn the_first_bad_value_in_definition_order_is_the_one_named() {
        check!(
            validate(
                &[
                    ("streams.session.timeout.ms", "x"),
                    ("consumer.session.timeout.ms", "y")
                ],
                Disabled
            ) == Err(
                "Invalid value y for configuration consumer.session.timeout.ms: Not a number of \
                 type INT"
                    .to_owned()
            )
        );
    }

    /// #784: with `unstable.api.versions.enable` off a group resource is
    /// Kafka 4.3.1's, so each Kafka trunk group key is refused with 4.3.1's
    /// `GroupConfig.validateNames` text, and accepted when it is on.
    #[test]
    fn trunk_group_keys_need_unstable_api_versions() {
        for key in KAFKA_TRUNK_GROUP_KEYS {
            let value = match *key {
                KEY_TASK_OFFSET_INTERVAL_MS => "15000",
                KEY_ASSIGNOR_NAME => "sticky",
                KEY_RACK_AWARE_ASSIGNMENT_TAGS => "zone",
                "errors.deadletterqueue.copy.record.enable"
                | "consumer.assignor.offload.enable"
                | "share.assignor.offload.enable"
                | "streams.assignor.offload.enable" => "true",
                "errors.deadletterqueue.topic.name" => "orders-dlq",
                _ => "2",
            };
            check!(
                validate(&[(key, value)], Disabled)
                    == Err(format!("Unknown group config name: {key}")),
                "{key}"
            );
            check!(validate(&[(key, value)], Enabled) == Ok(()), "{key}");
        }
        let strict: Vec<&str> = served_group_keys(Disabled).map(|key| key.name).collect();
        check!(strict.len() == 17);
        check!(
            !strict
                .iter()
                .any(|name| KAFKA_TRUNK_GROUP_KEYS.contains(name))
        );
    }

    /// Trunk's `validateValues` refuses a dead-letter-queue topic that is
    /// internal, and types the offload and copy switches as booleans.
    #[test]
    fn dead_letter_queue_keys_follow_trunks_validation() {
        check!(
            validate(
                &[("errors.deadletterqueue.topic.name", "__internal")],
                Enabled
            ) == Err(
                "errors.deadletterqueue.topic.name: DLQ topic name must not start with '__'"
                    .to_owned()
            )
        );
        check!(
            validate(
                &[("errors.deadletterqueue.copy.record.enable", "yes")],
                Enabled
            ) == Err(
                "Invalid value yes for configuration errors.deadletterqueue.copy.record.\
                     enable: Expected value to be either true or false"
                    .to_owned()
            )
        );
        check!(validate(&[("errors.deadletterqueue.topic.name", "")], Enabled) == Ok(()));
    }

    #[test]
    fn the_session_timeout_must_exceed_the_effective_heartbeat() {
        let bounds = GroupBounds {
            streams: StreamsGroupConfig {
                max_heartbeat_interval: std::time::Duration::from_mins(1),
                ..StreamsGroupConfig::default()
            },
            ..GroupBounds::default()
        };
        let overrides =
            BTreeMap::from([(KEY_HEARTBEAT_INTERVAL_MS.to_owned(), "45000".to_owned())]);
        check!(
            validate_group_configs(&overrides, &bounds, Enabled)
                == Err(
                    "streams.session.timeout.ms must be greater than streams.heartbeat.interval.ms"
                        .to_owned()
                )
        );
        // The same rule for the consumer and share pairs, against their own
        // defaults, under either version's bounds.
        let bounds = GroupBounds {
            consumer: NextGenConfig {
                max_heartbeat_interval: std::time::Duration::from_mins(2),
                ..NextGenConfig::default()
            },
            ..GroupBounds::default()
        };
        check!(
            validate_group_configs(
                &overrides_of("consumer.heartbeat.interval.ms", "45000"),
                &bounds,
                Disabled
            ) == Err(
                "consumer.session.timeout.ms must be greater than consumer.heartbeat.interval.ms"
                    .to_owned()
            )
        );
    }

    fn overrides_of(key: &str, value: &str) -> BTreeMap<String, String> {
        overrides(&[(key, value)])
    }

    #[test]
    fn kafkas_group_key_roster_has_every_trunk_key() {
        check!(KAFKA_GROUP_KEYS.len() == 27);
        let names: Vec<&str> = KAFKA_GROUP_KEYS.iter().map(|key| key.name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        check!(names == sorted);
        // The parse order names each key once, and only keys of the roster.
        let mut ordered = DEFINITION_ORDER.to_vec();
        ordered.sort_unstable();
        check!(ordered == names);
    }
}
