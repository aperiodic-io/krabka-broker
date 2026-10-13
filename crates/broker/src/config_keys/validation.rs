//! The `AlterConfigs` value checks: the per-key validator, the whole-map
//! validator with the cross-key rules, and the whitelist membership test.
//!
//! Every value goes through Kafka's `ConfigDef.parseType` first (see
//! [`super::parse`]), so ` 1000 ` is a valid `retention.ms` and `TRUE` a valid
//! boolean, and each refusal carries Kafka's `ConfigException` text. The
//! validators also return the value in the canonical form Kafka reports it
//! in, `ConfigDef.convertToString` of the parsed value, and the alter paths
//! store that form. Every reader of a stored value therefore sees `true`, not
//! ` TRUE `.

use {
    super::{
        CLEANUP_POLICY, COMPRESSION_GZIP_LEVEL, COMPRESSION_TYPE, LOCAL_RETENTION_BYTES,
        LOCAL_RETENTION_INHERIT, LOCAL_RETENTION_MS, MAX_COMPACTION_LAG_MS, MESSAGE_TIMESTAMP_TYPE,
        MESSAGE_TIMESTAMP_TYPE_LOG_APPEND, MIN_CLEANABLE_DIRTY_RATIO, MIN_COMPACTION_LAG_MS,
        REMOTE_COPY_LAG_BYTES, REMOTE_COPY_LAG_MS, REMOTE_LOG_COPY_DISABLE,
        REMOTE_LOG_DELETE_ON_DISABLE, REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS,
        dedup::{DEDUP_KEY_HEADER, DEDUP_WINDOW_MS, parse_dedup_key_header, parse_dedup_window},
        delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED},
        diskless::validate_diskless_combination,
        parse::{
            bool_value, check_one_of, check_range, check_valid_list, invalid_value, java_double,
            java_trim, list_value, long_value, parse_bool, parse_double, parse_int, parse_long,
        },
        registry::{
            self, CLEANUP_POLICY_VALUES, ConfigKey, ConfigScope, GZIP_DEFAULT_LEVEL,
            GZIP_MAX_LEVEL, GZIP_MIN_LEVEL, ValueCheck,
        },
        serves_topic_key,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        throttle::{
            FOLLOWER_THROTTLED_REPLICAS_KEY, LEADER_THROTTLED_REPLICAS_KEY, ThrottledReplicas,
        },
    },
    krabka_log::CleanupPolicy,
    std::collections::BTreeMap,
};

/// Kafka's `validateRemoteStorageOnlyIfSystemEnabled` refusal: a topic asks
/// for tiered storage on a broker with no remote storage backend.
pub(crate) const REMOTE_STORAGE_DISABLED_MESSAGE: &str = "Tiered Storage functionality is \
                                                          disabled in the broker. Topic cannot \
                                                          be configured with remote log storage.";

/// Kafka's `validateRemoteStorageRequiresDeleteCleanupPolicy` refusal: a
/// tiered topic's policy must be `delete` alone or the empty list.
pub(crate) const REMOTE_STORAGE_POLICY_MESSAGE: &str = "Remote log storage only supports topics \
                                                        with cleanup.policy=delete or \
                                                        cleanup.policy being an empty list.";

/// `true` when `map` holds `key` as a boolean `true`, read the way Kafka
/// parses a `BOOLEAN`.
pub(crate) fn flag(map: &BTreeMap<String, String>, key: &str) -> bool {
    map.get(key).and_then(|value| bool_value(value)) == Some(true)
}

/// Validate a single key/value pair. `Err(reason)` carries Kafka's refusal
/// text, which the handler propagates into the `error_message` field of the
/// response.
///
/// The accepted values come from the key's row in [`super::registry`], so the
/// check an operator meets here is the one the reference page and
/// `DescribeConfigs` describe. The key set is Kafka 4.3.1's, which is what a
/// broker without `unstable.api.versions.enable` serves.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    canonical_topic_config(key, value, UnstableApiVersions::Disabled).map(drop)
}

/// Validate a single key/value pair and return the value in the form Kafka
/// reports it: trimmed, a boolean in lower case, a number as it parses, and a
/// list joined with bare commas.
///
/// A key the broker does not serve under `unstable` (see
/// [`super::serves_topic_key`]) is `Unknown topic config name`, the text of
/// Kafka's `LogConfig.validateNames`.
pub(crate) fn canonical_topic_config(
    key: &str,
    value: &str,
    unstable: UnstableApiVersions,
) -> Result<String, String> {
    check_value_size(value)?;
    let Some(row) = registry::lookup(ConfigScope::Topic, key)
        .filter(|row| row.is_alterable() && serves_topic_key(row.name, unstable))
    else {
        return Err(format!("Unknown topic config name: {key}"));
    };
    canonical_value(row, value)
}

/// Kafka's `Short.MAX_VALUE`: the longest config value that
/// `ConfigurationControlManager.validateAlterConfig` accepts, counted as Java's
/// `String.length()` counts it, in UTF-16 code units.
const MAX_CONFIG_VALUE_LEN: usize = 32_767;

/// Kafka's `DISALLOWED_CONFIG_VALUE_SIZE_ERROR` message.
const CONFIG_VALUE_SIZE_MESSAGE: &str = "The configuration value cannot be added because it \
                                         exceeds the maximum value size of 32767 bytes.";

/// Refuse a config value that is longer than Kafka's `Short.MAX_VALUE`.
///
/// `validateAlterConfig` makes this check on each value a request writes
/// before any per-key validation, and `createTopics` reaches it through
/// `incrementalAlterConfig` for the configs of a new topic. A `KRaft`
/// `ConfigRecord` cannot carry a longer value.
pub(crate) fn check_value_size(value: &str) -> Result<(), String> {
    // UTF-8 never takes fewer bytes than UTF-16 takes code units, so a value
    // that fits in bytes fits in units too and needs no count.
    if value.len() <= MAX_CONFIG_VALUE_LEN || value.encode_utf16().count() <= MAX_CONFIG_VALUE_LEN {
        Ok(())
    } else {
        Err(CONFIG_VALUE_SIZE_MESSAGE.to_owned())
    }
}

/// The value check a row carries, over one value.
pub(crate) fn canonical_value(row: &ConfigKey, value: &str) -> Result<String, String> {
    let key = row.name;
    match row.check {
        ValueCheck::Bool => parse_bool(key, value).map(|parsed| parsed.to_string()),
        ValueCheck::OneOf(accepted) => check_one_of(key, value, accepted).map(str::to_owned),
        ValueCheck::I64AtLeast(min) => {
            check_range(key, parse_long(key, value)?, Some(min), None).map(|v| v.to_string())
        }
        ValueCheck::I32AtLeast(min) => {
            check_range(key, parse_int(key, value)?, Some(min), None).map(|v| v.to_string())
        }
        ValueCheck::I32Between(min, max) => {
            check_range(key, parse_int(key, value)?, Some(min), Some(max)).map(|v| v.to_string())
        }
        ValueCheck::Parsed => match key {
            CLEANUP_POLICY => check_valid_list(key, value, CLEANUP_POLICY_VALUES, true)
                .map(|values| values.join(",")),
            COMPRESSION_TYPE => parse_compression_type(value).map(|_| java_trim(value).to_owned()),
            COMPRESSION_GZIP_LEVEL => parse_gzip_level(value).map(|level| level.to_string()),
            MIN_CLEANABLE_DIRTY_RATIO => parse_ratio_value(value).map(java_double),
            DEDUP_WINDOW_MS => parse_dedup_window(value).map(|millis| millis.to_string()),
            DEDUP_KEY_HEADER => parse_dedup_key_header(value).map(str::to_owned),
            LEADER_THROTTLED_REPLICAS_KEY | FOLLOWER_THROTTLED_REPLICAS_KEY => {
                let values = list_value(value);
                ThrottledReplicas::parse(&values.join(","))
                    .map(|_| values.join(","))
                    .map_err(|_| {
                        invalid_value(
                            key,
                            format!("[{}]", values.join(", ")),
                            &format!(
                                "{key} must be the literal '*' or a list of replicas in the \
                                 following format: [partitionId]:[brokerId],[partitionId]:\
                                 [brokerId],..."
                            ),
                        )
                    })
            }
            other => Err(format!("Unknown topic config name: {other}")),
        },
        // `canonical_topic_config` has already refused every key an alter
        // path may not write, which is every `NotAltered` row.
        ValueCheck::NotAltered => Err(format!("Unknown topic config name: {key}")),
    }
}

/// Validate a topic's complete override map on a broker with a remote storage
/// backend: every key/value pair through [`validate_topic_config`], then the
/// cross-key rules in [`validate_config_combination`].
#[cfg(test)]
pub(crate) fn validate_topic_config_map(
    overrides: &BTreeMap<String, String>,
) -> Result<(), String> {
    canonical_topic_config_map(
        overrides,
        &TopicDefaults::default(),
        true,
        UnstableApiVersions::Disabled,
    )
    .map(drop)
}

/// Validate a topic's complete override map, and return it in the canonical
/// form [`canonical_topic_config`] gives each value. This is the map an alter
/// or create path stores. `CreateTopics` builds the whole map before it
/// commits anything, so it validates in one call.
///
/// `defaults` are the effective broker defaults the cross-key rules read for a
/// key the map leaves unset. `remote_storage_system_enabled` is Kafka's
/// `RemoteLogManagerConfig.isRemoteStorageSystemEnabled`: whether this broker
/// has a remote storage backend at all. `unstable` is Kafka's
/// `unstable.api.versions.enable`, which decides whether Kafka trunk's topic
/// keys are accepted.
pub(crate) fn canonical_topic_config_map(
    overrides: &BTreeMap<String, String>,
    defaults: &TopicDefaults,
    remote_storage_system_enabled: bool,
    unstable: UnstableApiVersions,
) -> Result<BTreeMap<String, String>, String> {
    // Kafka checks every value's size before it validates any key.
    overrides
        .values()
        .try_for_each(|value| check_value_size(value))?;
    let canonical = overrides
        .iter()
        .map(|(key, value)| Ok((key.clone(), canonical_topic_config(key, value, unstable)?)))
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    validate_config_combination(&canonical, defaults, remote_storage_system_enabled)?;
    Ok(canonical)
}

/// Validate the rules that span two keys, over a topic's complete override
/// map. [`validate_topic_config`] takes one pair and cannot see them.
///
/// Kafka's own rules come first, in the order `LogConfig.validate` runs them:
/// `validateValues` (the compaction lags), then, when `remote.storage.enable`
/// is true, the tiered-storage rules in [`validate_remote_storage`]. Kafka
/// runs them over the topic map merged over the broker's effective defaults
/// (`KafkaConfig.extractLogConfigMap`), so a key the topic leaves unset reads
/// `defaults` before the registry default.
///
/// KFC-1 states the first krabka rule: `cleanup.policy=compact` and
/// `delivery.mode=scheduled` exclude each other. Compaction deletes a record
/// once a later record carries the same key, and on a scheduled topic that
/// later record can arrive long before the earlier one comes due. The earlier
/// record would then be deleted without a single delivery, which is the
/// failure scheduled delivery exists to prevent.
///
/// KFC-1 states a second: `message.timestamp.type=LogAppendTime` and
/// `delivery.mode=scheduled` exclude each other. A scheduled topic reads each
/// batch's `max_timestamp` as its activation time, and log-append stamping
/// overwrites exactly that field with the broker's clock at append. The pair
/// would silently deliver every record at once, and the schedule the producer
/// wrote would be unrecoverable, because the overwrite destroys it.
///
/// The other three are the data-path rules in [`validate_diskless_combination`]:
/// `krabka.diskless=true` excludes `remote.storage.enable=true`,
/// `delivery.mode=scheduled`, and a KFC-10 `dedup.window.ms` other than `-1`.
/// `dedup.key.header` alone is accepted on any topic, because it does nothing
/// while the window is off.
pub(crate) fn validate_config_combination(
    overrides: &BTreeMap<String, String>,
    defaults: &TopicDefaults,
    remote_storage_system_enabled: bool,
) -> Result<(), String> {
    validate_compaction_lag_order(overrides, defaults)?;
    // krabka's diskless rule names the diskless flag, which is the key an
    // operator has to change, so it answers before the tier checks do.
    validate_diskless_combination(overrides)?;
    validate_remote_storage(overrides, defaults, remote_storage_system_enabled)?;
    let compacting = overrides.get(CLEANUP_POLICY).is_some_and(|policy| {
        parse_cleanup_policy(policy).is_ok_and(CleanupPolicy::contains_compact)
    });
    let scheduled = overrides
        .get(DELIVERY_MODE)
        .is_some_and(|mode| java_trim(mode) == DELIVERY_MODE_SCHEDULED);
    if compacting && scheduled {
        return Err(format!(
            "{CLEANUP_POLICY}=compact cannot be combined with \
             {DELIVERY_MODE}={DELIVERY_MODE_SCHEDULED}: compaction deletes a record once a \
             later record carries the same key, and on a scheduled topic that later record \
             can arrive long before the earlier one comes due, so the earlier record would \
             be deleted without a single delivery"
        ));
    }
    let log_append_time = overrides
        .get(MESSAGE_TIMESTAMP_TYPE)
        .is_some_and(|value| java_trim(value) == MESSAGE_TIMESTAMP_TYPE_LOG_APPEND);
    if log_append_time && scheduled {
        return Err(format!(
            "{MESSAGE_TIMESTAMP_TYPE}={MESSAGE_TIMESTAMP_TYPE_LOG_APPEND} cannot be combined \
             with {DELIVERY_MODE}={DELIVERY_MODE_SCHEDULED}: a scheduled topic reads a batch's \
             max timestamp as its activation time, and log-append stamping overwrites that \
             field with the broker's clock, so every record would come due at once"
        ));
    }
    Ok(())
}

/// The broker-level defaults a topic key takes when the topic sets no
/// override, keyed by topic key.
///
/// Kafka's `ControllerConfigurationValidator` hands `LogConfig.validate` the
/// broker's `KafkaConfig.extractLogConfigMap`, and the cross-key rules run over
/// the topic's configs merged over it. A cluster that sets
/// `log.cleanup.policy=compact` therefore refuses a tiered topic that leaves
/// `cleanup.policy` unset. Krabka's broker-level topic defaults are the
/// cluster-wide dynamic broker configs; a key the cluster does not set keeps
/// its registry default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TopicDefaults(BTreeMap<&'static str, String>);

impl TopicDefaults {
    /// The effective defaults `image`'s cluster-wide broker configs give each
    /// topic key, through the key's broker synonyms in Kafka's order of
    /// preference. `log.retention.minutes` and `log.retention.hours` are
    /// converted to milliseconds, and a negative retention is `-1`, as
    /// `KafkaConfig.logRetentionTimeMillis` reads them.
    ///
    /// A topic key the broker does not serve under `unstable` has no broker
    /// default: its broker key is not a `KafkaConfig` key there, so Kafka's
    /// `extractLogConfigMap` does not carry it.
    pub(crate) fn from_image(
        image: &krabka_metadata::MetadataImage,
        unstable: UnstableApiVersions,
    ) -> Self {
        let Some(cluster) = image.default_broker_config() else {
            return Self::default();
        };
        Self(
            registry::keys_in(ConfigScope::Topic)
                .filter(|row| serves_topic_key(row.name, unstable))
                .filter_map(|row| {
                    super::broker_dynamic::topic_broker_synonyms(row.name)
                        .iter()
                        .find_map(|synonym| {
                            let value = cluster.get(synonym.name)?;
                            Some((row.name, in_topic_unit(row.name, synonym.name, value)?))
                        })
                })
                .collect(),
        )
    }

    fn get(&self, key: &str) -> Option<&str> { self.0.get(key).map(String::as_str) }
}

/// A broker synonym's `value` in the unit of `topic_key`: minutes and hours
/// become milliseconds, and a negative `retention.ms` is `-1`. `None` when a
/// value that needs converting is not a number.
pub(crate) fn in_topic_unit(topic_key: &str, broker_key: &str, value: &str) -> Option<String> {
    let per_unit = match broker_key {
        "log.retention.hours" | "log.roll.hours" | "log.roll.jitter.hours" => 3_600_000,
        "log.retention.minutes" => 60_000,
        _ if topic_key == RETENTION_MS => 1,
        _ => return Some(value.to_owned()),
    };
    let millis = long_value(value)?.checked_mul(per_unit)?;
    let millis = if topic_key == RETENTION_MS && millis < 0 {
        -1
    } else {
        millis
    };
    Some(millis.to_string())
}

/// A `LONG` key of the resulting map: the topic override, else the effective
/// broker default, else the registry default. That is what Kafka's combined
/// map carries for the key.
fn long_or_default(
    overrides: &BTreeMap<String, String>,
    defaults: &TopicDefaults,
    key: &str,
) -> Option<i64> {
    overrides
        .get(key)
        .map(String::as_str)
        .or_else(|| defaults.get(key))
        .or_else(|| registry::lookup(ConfigScope::Topic, key).and_then(|row| row.default))
        .and_then(long_value)
}

/// Kafka's tiered-storage rules, which `LogConfig.validateTopicLogConfigValues`
/// runs when the resulting map has `remote.storage.enable=true`, in its order:
/// the broker must have a remote storage backend, the cleanup policy must be
/// `delete` alone or empty, the local retention must fit inside the total
/// retention in both size and time, the copy lags must fit inside the
/// effective local retention, and a read-only tier must keep the local and
/// total retention equal.
fn validate_remote_storage(
    overrides: &BTreeMap<String, String>,
    defaults: &TopicDefaults,
    remote_storage_system_enabled: bool,
) -> Result<(), String> {
    if !flag(overrides, REMOTE_STORAGE_ENABLE) {
        return Ok(());
    }
    if !remote_storage_system_enabled {
        return Err(REMOTE_STORAGE_DISABLED_MESSAGE.to_owned());
    }
    let policy = overrides
        .get(CLEANUP_POLICY)
        .map(String::as_str)
        .or_else(|| defaults.get(CLEANUP_POLICY))
        .map_or_else(|| vec!["delete"], list_value);
    if !policy.is_empty() && policy != ["delete"] {
        return Err(REMOTE_STORAGE_POLICY_MESSAGE.to_owned());
    }
    let value = |key: &str| long_or_default(overrides, defaults, key).unwrap_or_default();
    let retention_bytes = value(RETENTION_BYTES);
    let local_retention_bytes = value(LOCAL_RETENTION_BYTES);
    let retention_ms = value(RETENTION_MS);
    let local_retention_ms = value(LOCAL_RETENTION_MS);

    for (total_key, total, local_key, local, applies) in [
        (
            RETENTION_BYTES,
            retention_bytes,
            LOCAL_RETENTION_BYTES,
            local_retention_bytes,
            retention_bytes > -1,
        ),
        (
            RETENTION_MS,
            retention_ms,
            LOCAL_RETENTION_MS,
            local_retention_ms,
            retention_ms != -1,
        ),
    ] {
        if !applies || local == LOCAL_RETENTION_INHERIT {
            continue;
        }
        if local == -1 {
            return Err(invalid_value(
                local_key,
                local,
                &format!("Value must not be -1 as {total_key} value is set as {total}."),
            ));
        }
        if local > total {
            return Err(invalid_value(
                local_key,
                local,
                &format!("Value must not be more than {total_key} property value: {total}"),
            ));
        }
    }

    for (lag_key, local_key, total, local) in [
        (
            REMOTE_COPY_LAG_BYTES,
            LOCAL_RETENTION_BYTES,
            retention_bytes,
            local_retention_bytes,
        ),
        (
            REMOTE_COPY_LAG_MS,
            LOCAL_RETENTION_MS,
            retention_ms,
            local_retention_ms,
        ),
    ] {
        let lag = value(lag_key);
        let effective = if local == LOCAL_RETENTION_INHERIT {
            total
        } else {
            local
        };
        if lag > 0 && effective >= 0 && lag > effective {
            return Err(invalid_value(
                lag_key,
                lag,
                &format!("Value must not exceed {local_key} (effective value: {effective})"),
            ));
        }
    }

    if flag(overrides, REMOTE_LOG_COPY_DISABLE) {
        for (unit, total, local) in [
            ("bytes", retention_bytes, local_retention_bytes),
            ("ms", retention_ms, local_retention_ms),
        ] {
            if local != LOCAL_RETENTION_INHERIT && local != total {
                return Err(format!(
                    "When `remote.log.copy.disable` is set to true, the `local.retention.{unit}` \
                     and `retention.{unit}` must be set to the identical value because there \
                     will be no more logs copied to the remote storage."
                ));
            }
        }
    }
    Ok(())
}

/// Kafka's `LogConfig.validateValues`: the cleaner cannot be told to protect a
/// record for longer than the deadline that forces it to be compacted.
///
/// Each key alone passes its own range check, so the rule belongs here, where
/// the whole map is in hand. A key the request leaves alone is read at its
/// registry default, which is what Kafka's `props` carries for an unset key:
/// `min.compaction.lag.ms` 0 and `max.compaction.lag.ms` `i64::MAX`, so an
/// alter that sets only one of the two is still checked against the other.
fn validate_compaction_lag_order(
    overrides: &BTreeMap<String, String>,
    defaults: &TopicDefaults,
) -> Result<(), String> {
    let lag = |name: &str| long_or_default(overrides, defaults, name);
    let (Some(min), Some(max)) = (lag(MIN_COMPACTION_LAG_MS), lag(MAX_COMPACTION_LAG_MS)) else {
        return Ok(());
    };
    if min > max {
        return Err(format!(
            "conflict topic config setting {MIN_COMPACTION_LAG_MS} ({min}) > \
             {MAX_COMPACTION_LAG_MS} ({max})"
        ));
    }
    Ok(())
}

/// Kafka's KIP-950 `LogConfig.validateTurningOffRemoteStorageWithDelete`:
/// turning tiered storage off is refused unless the operator has said what
/// should happen to the segments already in the tier.
///
/// `remote.storage.enable` going `true -> false` erases the topic's remote
/// copies and raises its log start offset to the local log start, so Kafka
/// makes the operator ask for that explicitly with
/// `remote.log.delete.on.disable=true`. The alternative it names in the same
/// message is the read-only tier: keep `remote.storage.enable=true` and set
/// `remote.log.copy.disable=true`, which stops new copies while the history
/// stays readable.
///
/// `current` is the topic's stored override map and `next` the map the alter
/// installs, so this is the only rule here that reads both: the others decide
/// a map on its own.
///
/// # Errors
/// Returns the refusal message when the alter turns tiered storage off
/// without `remote.log.delete.on.disable=true` in the resulting map.
pub(crate) fn validate_remote_storage_disable(
    current: Option<&BTreeMap<String, String>>,
    next: &BTreeMap<String, String>,
) -> Result<(), String> {
    let was_enabled = current.is_some_and(|map| flag(map, REMOTE_STORAGE_ENABLE));
    if !was_enabled || flag(next, REMOTE_STORAGE_ENABLE) || flag(next, REMOTE_LOG_DELETE_ON_DISABLE)
    {
        return Ok(());
    }
    Err(format!(
        "It is invalid to disable remote storage without deleting remote data. If you want to \
         keep the remote data and turn to read only, please set \
         `{REMOTE_STORAGE_ENABLE}=true,{REMOTE_LOG_COPY_DISABLE}=true`. If you want to disable \
         remote storage and delete all remote data, please set \
         `{REMOTE_STORAGE_ENABLE}=false,{REMOTE_LOG_DELETE_ON_DISABLE}=true`."
    ))
}

/// Parse Kafka's `cleanup.policy` list into the policy a partition runs under.
///
/// The value is a comma-separated list, and Kafka derives two independent
/// booleans from it: `compact` when the list names `compact`, `delete` when it
/// names `delete`. Either order and either name alone is accepted, and so is
/// `compact,delete`, which Kafka Streams writes on every windowed-store
/// changelog topic. The empty list is accepted too, and means no cleanup at
/// all. A repeated name, an empty element and an unknown name are refused, as
/// Kafka's `ValidList.in("compact", "delete")` refuses them.
pub(crate) fn parse_cleanup_policy(value: &str) -> Result<CleanupPolicy, String> {
    let values = check_valid_list(CLEANUP_POLICY, value, CLEANUP_POLICY_VALUES, true)?;
    let compact = values.contains(&"compact");
    let delete = values.contains(&"delete");
    Ok(match (compact, delete) {
        (true, true) => CleanupPolicy::CompactAndDelete,
        (true, false) => CleanupPolicy::Compact,
        (false, true) => CleanupPolicy::Delete,
        (false, false) => CleanupPolicy::NoCleanup,
    })
}

/// Kafka's `min.cleanable.dirty.ratio`, a `DOUBLE` validated with
/// `between(0, 1)`. `apache/kafka:4.3.1` refuses `2` with `Invalid value 2.0
/// for configuration min.cleanable.dirty.ratio: Value must be no more than
/// 1`: the value it prints is the parsed `double`.
fn parse_ratio_value(value: &str) -> Result<f64, String> {
    let key = MIN_CLEANABLE_DIRTY_RATIO;
    let parsed = parse_double(key, value)?;
    // Kafka's `Range` compares with `<` and `>`, which a NaN passes; krabka's
    // ratio type holds no NaN, so a NaN is refused as the non-number it is.
    if parsed.is_nan() {
        return Err(invalid_value(key, value, "Not a number of type DOUBLE"));
    }
    if parsed < 0.0 {
        return Err(invalid_value(
            key,
            java_double(parsed),
            "Value must be at least 0",
        ));
    }
    if parsed > 1.0 {
        return Err(invalid_value(
            key,
            java_double(parsed),
            "Value must be no more than 1",
        ));
    }
    Ok(parsed)
}

/// Parse Kafka's `min.cleanable.dirty.ratio` into the ratio the cleaner reads.
pub(crate) fn parse_dirty_ratio(value: &str) -> Result<krabka_units::Ratio, String> {
    parse_ratio_value(value).map(krabka_units::fraction)
}

/// Map the wire-side `compression.type` value to the matching
/// [`krabka_log::LogConfig::compression_type`]. This function returns `Ok(None)` for the
/// special `producer` value, which is the Kafka default and does no
/// broker-side re-encoding. It returns `Ok(Some(_))` for any concrete codec.
/// It returns `Err` for an unknown name.
///
/// The accepted set is Kafka's own and nothing else. `LogConfig` validates the
/// key with `ValidString.in(BrokerCompressionType.names())`, and that enum has
/// six members -- `uncompressed`, `zstd`, `lz4`, `snappy`, `gzip`, `producer`.
/// `none` is a *producer*-side codec name, not a broker-side one, and
/// `apache/kafka:4.3.1` refuses it with `Invalid value none for configuration
/// compression.type: String must be one of: uncompressed, zstd, lz4, snappy,
/// gzip, producer`. Accepting it here would let a topic be created against
/// krabka that no Kafka broker would accept the config of, which is the
/// direction of divergence that breaks a migration back.
pub(crate) fn parse_compression_type(
    value: &str,
) -> Result<Option<krabka_compression::CompressionType>, String> {
    use krabka_compression::CompressionType;
    let name = check_one_of(COMPRESSION_TYPE, value, &[
        "uncompressed",
        "zstd",
        "lz4",
        "snappy",
        "gzip",
        "producer",
    ])?;
    Ok(match name {
        "uncompressed" => Some(CompressionType::None),
        "gzip" => Some(CompressionType::Gzip),
        "snappy" => Some(CompressionType::Snappy),
        "lz4" => Some(CompressionType::Lz4),
        "zstd" => Some(CompressionType::Zstd),
        _ => None,
    })
}

/// `compression.gzip.level`. Kafka's validator for the key is neither a floor
/// nor a plain range: `apache/kafka:4.3.1` answers `Invalid value 0 for
/// configuration compression.gzip.level: Value must be between 1 and 9 or
/// equal to -1`, so `0` is refused while `-1`, the
/// `Deflater.DEFAULT_COMPRESSION` this key defaults to, is not.
fn parse_gzip_level(value: &str) -> Result<i32, String> {
    let parsed = parse_int(COMPRESSION_GZIP_LEVEL, value)?;
    if parsed == GZIP_DEFAULT_LEVEL || (GZIP_MIN_LEVEL..=GZIP_MAX_LEVEL).contains(&parsed) {
        return Ok(parsed);
    }
    Err(invalid_value(
        COMPRESSION_GZIP_LEVEL,
        parsed,
        &format!(
            "Value must be between {GZIP_MIN_LEVEL} and {GZIP_MAX_LEVEL} or equal to \
             {GZIP_DEFAULT_LEVEL}"
        ),
    ))
}

/// Returns `true` if `key` is one of the recognized topic-config keys.
/// This helps `IncrementalAlterConfigs` DELETE-op validation, which then
/// needs no sentinel probe value. A controller-written key such as
/// [`super::WRITE_FREEZE`] is not recognized: no alter path may write it.
#[cfg(test)]
pub(crate) fn is_recognized(key: &str) -> bool {
    registry::lookup(ConfigScope::Topic, key).is_some_and(registry::ConfigKey::is_alterable)
}

#[cfg(test)]
mod tests;
