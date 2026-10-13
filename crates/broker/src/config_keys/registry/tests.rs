//! Behaviour of the typed key registry: the rows every surface reads, the
//! wire bytes they carry, and the defaults they must agree with.

use {
    super::*,
    crate::throttle::{
        ALTER_LOG_DIRS_THROTTLED_RATE_KEY, FOLLOWER_THROTTLED_RATE_KEY,
        FOLLOWER_THROTTLED_REPLICAS_KEY, LEADER_THROTTLED_RATE_KEY, LEADER_THROTTLED_REPLICAS_KEY,
    },
    assert2::{assert, check},
    krabka_units::convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
};

#[test]
fn config_type_wire_bytes_match_kafka() {
    // `DescribeConfigsResponse.ConfigType`, verified against
    // `apache/kafka:4.3.1` over the wire.
    let observed: Vec<(&str, i8)> = [
        ConfigType::Boolean,
        ConfigType::String,
        ConfigType::Int,
        ConfigType::Long,
        ConfigType::Double,
        ConfigType::List,
    ]
    .into_iter()
    .map(|config_type| (config_type.label(), config_type.wire()))
    .collect();

    assert!(
        observed
            == vec![
                ("boolean", 1),
                ("string", 2),
                ("int", 3),
                ("long", 5),
                ("double", 6),
                ("list", 7),
            ]
    );
}

#[test]
fn topic_key_types_match_apache_kafka() {
    // Read off `apache/kafka:4.3.1` with a DescribeConfigs v3 probe that asked
    // for synonyms and documentation. Every row here is a key Kafka also has,
    // so the byte the JVM AdminClient reads must be the same one.
    for (name, expected) in [
        (RETENTION_MS, ConfigType::Long),
        (RETENTION_BYTES, ConfigType::Long),
        (SEGMENT_BYTES, ConfigType::Int),
        (CLEANUP_POLICY, ConfigType::List),
        (COMPRESSION_TYPE, ConfigType::String),
        (MIN_INSYNC_REPLICAS, ConfigType::Int),
        (UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean),
        (REMOTE_STORAGE_ENABLE, ConfigType::Boolean),
        (LOCAL_RETENTION_MS, ConfigType::Long),
        (LOCAL_RETENTION_BYTES, ConfigType::Long),
        (DELETE_RETENTION_MS, ConfigType::Long),
        (LEADER_THROTTLED_REPLICAS_KEY, ConfigType::List),
        (FOLLOWER_THROTTLED_REPLICAS_KEY, ConfigType::List),
        (SEGMENT_MS, ConfigType::Long),
        (SEGMENT_INDEX_BYTES, ConfigType::Int),
        (SEGMENT_JITTER_MS, ConfigType::Long),
        (MIN_COMPACTION_LAG_MS, ConfigType::Long),
        (MAX_COMPACTION_LAG_MS, ConfigType::Long),
        (MIN_CLEANABLE_DIRTY_RATIO, ConfigType::Double),
        (FILE_DELETE_DELAY_MS, ConfigType::Long),
        (FLUSH_MESSAGES, ConfigType::Long),
        (FLUSH_MS, ConfigType::Long),
        (INDEX_INTERVAL_BYTES, ConfigType::Int),
        (PREALLOCATE, ConfigType::Boolean),
        (MESSAGE_TIMESTAMP_TYPE, ConfigType::String),
        (MESSAGE_TIMESTAMP_AFTER_MAX_MS, ConfigType::Long),
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, ConfigType::Long),
    ] {
        let row = lookup(ConfigScope::Topic, name).expect(name);
        check!(row.config_type == expected, "{name}");
    }
}

#[test]
fn broker_key_types_match_apache_kafka() {
    for (name, expected) in [
        (LEADER_THROTTLED_RATE_KEY, ConfigType::Long),
        (FOLLOWER_THROTTLED_RATE_KEY, ConfigType::Long),
        (ALTER_LOG_DIRS_THROTTLED_RATE_KEY, ConfigType::Long),
        (UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean),
        (REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, ConfigType::Long),
        // `GroupCoordinatorConfig`: `offsets.retention.minutes` is `INT` and
        // `offsets.retention.check.interval.ms` is `LONG`.
        (OFFSETS_RETENTION_MINUTES, ConfigType::Int),
        (OFFSETS_RETENTION_CHECK_INTERVAL_MS, ConfigType::Long),
        // `SocketServerConfigs`: `connections.max.idle.ms` is `LONG`.
        (CONNECTIONS_MAX_IDLE_MS, ConfigType::Long),
        // KIP-368: `connections.max.reauth.ms` is `LONG`.
        (CONNECTIONS_MAX_REAUTH_MS, ConfigType::Long),
        (NODE_ID, ConfigType::Int),
        (TRANSACTIONAL_ID_EXPIRATION_MS, ConfigType::Int),
        (
            TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            ConfigType::Int,
        ),
    ] {
        let row = lookup(ConfigScope::Broker, name).expect(name);
        check!(row.config_type == expected, "{name}");
    }
}

#[test]
fn topic_key_defaults_match_apache_kafka() {
    // The same probe's `DEFAULT_CONFIG` values on a topic with no overrides.
    for (name, expected) in [
        (RETENTION_MS, "604800000"),
        (RETENTION_BYTES, "-1"),
        (SEGMENT_BYTES, "1073741824"),
        (CLEANUP_POLICY, "delete"),
        (COMPRESSION_TYPE, "producer"),
        (MIN_INSYNC_REPLICAS, "1"),
        (UNCLEAN_LEADER_ELECTION_ENABLE, "false"),
        (REMOTE_STORAGE_ENABLE, "false"),
        (LOCAL_RETENTION_MS, "-2"),
        (LOCAL_RETENTION_BYTES, "-2"),
        (DELETE_RETENTION_MS, "86400000"),
        (LEADER_THROTTLED_REPLICAS_KEY, ""),
        (FOLLOWER_THROTTLED_REPLICAS_KEY, ""),
        (SEGMENT_MS, "604800000"),
        (SEGMENT_INDEX_BYTES, "10485760"),
        (SEGMENT_JITTER_MS, "0"),
        (MIN_COMPACTION_LAG_MS, "0"),
        (MAX_COMPACTION_LAG_MS, "9223372036854775807"),
        (MIN_CLEANABLE_DIRTY_RATIO, "0.5"),
        (FILE_DELETE_DELAY_MS, "60000"),
        (FLUSH_MESSAGES, "9223372036854775807"),
        (FLUSH_MS, "9223372036854775807"),
        (INDEX_INTERVAL_BYTES, "4096"),
        (PREALLOCATE, "false"),
        (MESSAGE_TIMESTAMP_TYPE, "CreateTime"),
        // Kafka's two timestamp windows do not share a default: the future
        // one is `LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT`, one hour, and
        // the past one is `Long.MAX_VALUE`.
        (MESSAGE_TIMESTAMP_AFTER_MAX_MS, "3600000"),
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "9223372036854775807"),
    ] {
        let row = lookup(ConfigScope::Topic, name).expect(name);
        check!(row.default == Some(expected), "{name}");
    }
}

#[test]
fn topic_defaults_agree_with_the_log_config_a_partition_runs_on() {
    // The registry states the default a topic reports; `LogConfig::default()`
    // states the value a partition actually runs with. A partition running on
    // a value `DescribeConfigs` does not report is the bug this pins.
    let log = krabka_log::LogConfig::default();
    let default_of = |name| lookup(ConfigScope::Topic, name).expect(name).default;

    check!(
        default_of(RETENTION_MS)
            == Some(
                log.retention
                    .map_or_else(|| "-1".to_owned(), |window| window.millis_i64().to_string())
                    .as_str()
            )
    );
    check!(default_of(SEGMENT_BYTES) == Some(log.segment_size.bytes_u64().to_string().as_str()));
    check!(
        default_of(DELETE_RETENTION_MS)
            == Some(log.delete_retention.millis_i64().to_string().as_str())
    );
    check!(default_of(CLEANUP_POLICY) == Some(log.cleanup_policy.as_str()));
    check!(
        default_of(SEGMENT_MS) == Some(log.segment_roll_interval.millis_i64().to_string().as_str())
    );
    check!(
        default_of(INDEX_INTERVAL_BYTES)
            == Some(log.index_interval.bytes_u64().to_string().as_str())
    );
    check!(
        default_of(MIN_COMPACTION_LAG_MS)
            == Some(log.min_compaction_lag.millis_i64().to_string().as_str())
    );
    // Kafka spells "no bound" as `Long.MAX_VALUE`; the log carries it as
    // `None`, and the registry has to report the number an operator reads.
    check!(
        log.max_compaction_lag == None
            && default_of(MAX_COMPACTION_LAG_MS) == Some("9223372036854775807")
    );
    check!(
        default_of(MIN_CLEANABLE_DIRTY_RATIO)
            == Some(log.min_cleanable_dirty_ratio.as_f64().to_string().as_str())
    );
    check!(
        log.message_timestamp_type == krabka_protocol::records::TimestampType::CreateTime
            && default_of(MESSAGE_TIMESTAMP_TYPE) == Some("CreateTime")
    );
    check!(log.compression_type.is_none() && default_of(COMPRESSION_TYPE) == Some("producer"));
    check!(
        default_of(REMOTE_STORAGE_ENABLE)
            == Some(if log.remote_storage_enable {
                "true"
            } else {
                "false"
            })
    );
}

#[test]
fn every_row_is_unique_in_its_scope_and_carries_documentation() {
    let mut seen = std::collections::HashSet::new();
    for row in CONFIG_KEYS {
        check!(
            seen.insert((row.scope, row.name)),
            "duplicate row for {:?} {}",
            row.scope,
            row.name
        );
        check!(!row.doc.is_empty(), "{} has no documentation", row.name);
    }
}

#[test]
fn value_type_renders_the_type_and_its_note() {
    let retention = lookup(ConfigScope::Topic, RETENTION_MS).expect(RETENTION_MS);
    let policy = lookup(ConfigScope::Topic, CLEANUP_POLICY).expect(CLEANUP_POLICY);

    check!(retention.value_type() == "long (ms)");
    check!(policy.value_type() == "list");
}

#[test]
fn the_synthesised_keys_are_the_only_unstored_rows() {
    let unstored: Vec<&str> = CONFIG_KEYS
        .iter()
        .filter(|row| !row.is_stored())
        .map(|row| row.name)
        .collect();

    assert!(
        unstored
            == vec![
                WRITE_FREEZE,
                OFFSETS_RETENTION_MINUTES,
                OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                CONNECTIONS_MAX_IDLE_MS,
                CONNECTIONS_MAX_REAUTH_MS,
                NUM_PARTITIONS,
                DEFAULT_REPLICATION_FACTOR,
                DELETE_TOPIC_ENABLE,
                AUTO_CREATE_TOPICS_ENABLE,
                TRANSACTIONAL_ID_EXPIRATION_MS,
                TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
                NODE_ID,
            ]
    );
}

/// The two KIP-98 keys report Kafka's own defaults, so a broker that never
/// touched either one answers `--describe --all` the way a Kafka broker does.
#[test]
fn transactional_id_expiry_defaults_match_apache_kafka() {
    for (name, expected) in [
        (TRANSACTIONAL_ID_EXPIRATION_MS, "604800000"),
        (TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, "3600000"),
    ] {
        let row = lookup(ConfigScope::Broker, name).expect(name);
        check!(row.default == Some(expected), "{name}");
        // `kafka-configs --alter` answers `Cannot update these configs
        // dynamically`, so `DescribeConfigs` must report them read-only.
        check!(row.read_only, "{name}");
    }
}

/// The defaults the registry reports are the values the broker actually runs
/// with. A broker sweeping on a cadence `DescribeConfigs` does not report is
/// the drift this pins.
#[test]
fn transactional_id_expiry_defaults_agree_with_the_broker_config() {
    let default_of = |name| lookup(ConfigScope::Broker, name).expect(name).default;

    check!(
        default_of(TRANSACTIONAL_ID_EXPIRATION_MS)
            == Some(
                crate::config::DEFAULT_TXN_ID_EXPIRATION
                    .millis_i64()
                    .to_string()
                    .as_str()
            )
    );
    check!(
        default_of(TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS)
            == Some(
                crate::config::DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL
                    .millis_i64()
                    .to_string()
                    .as_str()
            )
    );
}

#[test]
fn a_numeric_check_is_as_wide_as_the_type_the_row_advertises() {
    // The width a row's check enforces is the width the JVM `AdminClient`
    // parses the value with. A `ConfigType::Int` row that accepted an `i64`
    // would tell a typed client that a value it cannot represent is valid, so
    // no `Int` row may carry a 64-bit floor and no `Long` row a 32-bit one.
    let mismatched: Vec<&str> = CONFIG_KEYS
        .iter()
        .filter(|row| {
            matches!(
                (row.config_type, row.check),
                (ConfigType::Int, ValueCheck::I64AtLeast(_))
                    | (
                        ConfigType::Long,
                        ValueCheck::I32AtLeast(_) | ValueCheck::I32Between(..)
                    )
            )
        })
        .map(|row| row.name)
        .collect();

    assert!(mismatched == Vec::<&str>::new());
}

#[test]
fn no_key_krabka_has_today_is_sensitive() {
    // The redaction path still has to hold before a secret-valued key exists,
    // which `handlers::describe_configs::entry` tests directly.
    assert!(!CONFIG_KEYS.iter().any(ConfigKey::is_sensitive));
}

#[test]
fn a_scope_reports_only_its_own_rows() {
    // `unclean.leader.election.enable` is both a topic key and a
    // cluster-default broker key, and the two rows differ.
    let topic = lookup(ConfigScope::Topic, UNCLEAN_LEADER_ELECTION_ENABLE)
        .expect("topic unclean.leader.election.enable");
    let broker = lookup(ConfigScope::Broker, UNCLEAN_LEADER_ELECTION_ENABLE)
        .expect("broker unclean.leader.election.enable");

    check!(topic.cluster_default == Some(UNCLEAN_LEADER_ELECTION_ENABLE));
    check!(broker.cluster_default == None);
    check!(lookup(ConfigScope::Group, UNCLEAN_LEADER_ELECTION_ENABLE).is_none());
    check!(keys_in(ConfigScope::ClientMetrics).count() == 3);
}
