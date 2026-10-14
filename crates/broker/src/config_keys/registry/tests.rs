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
    #[derive(Debug, PartialEq, Eq)]
    struct ConfigTypeBytes {
        label: &'static str,
        wire: i8,
    }
    let observed: Vec<ConfigTypeBytes> = [
        ConfigType::Boolean,
        ConfigType::String,
        ConfigType::Int,
        ConfigType::Long,
        ConfigType::Double,
        ConfigType::List,
    ]
    .into_iter()
    .map(|config_type| ConfigTypeBytes {
        label: config_type.label(),
        wire: config_type.wire(),
    })
    .collect();

    assert!(
        observed
            == [
                ("boolean", 1),
                ("string", 2),
                ("int", 3),
                ("long", 5),
                ("double", 6),
                ("list", 7),
            ]
            .map(|(label, wire)| ConfigTypeBytes { label, wire })
    );
}

#[yare::parameterized(
    retention_ms = { RETENTION_MS, ConfigType::Long },
    retention_bytes = { RETENTION_BYTES, ConfigType::Long },
    segment_bytes = { SEGMENT_BYTES, ConfigType::Int },
    cleanup_policy = { CLEANUP_POLICY, ConfigType::List },
    compression_type = { COMPRESSION_TYPE, ConfigType::String },
    min_insync_replicas = { MIN_INSYNC_REPLICAS, ConfigType::Int },
    unclean_leader_election_enable = { UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean },
    remote_storage_enable = { REMOTE_STORAGE_ENABLE, ConfigType::Boolean },
    local_retention_ms = { LOCAL_RETENTION_MS, ConfigType::Long },
    local_retention_bytes = { LOCAL_RETENTION_BYTES, ConfigType::Long },
    delete_retention_ms = { DELETE_RETENTION_MS, ConfigType::Long },
    leader_throttled_replicas = { LEADER_THROTTLED_REPLICAS_KEY, ConfigType::List },
    follower_throttled_replicas = { FOLLOWER_THROTTLED_REPLICAS_KEY, ConfigType::List },
    segment_ms = { SEGMENT_MS, ConfigType::Long },
    segment_index_bytes = { SEGMENT_INDEX_BYTES, ConfigType::Int },
    segment_jitter_ms = { SEGMENT_JITTER_MS, ConfigType::Long },
    min_compaction_lag_ms = { MIN_COMPACTION_LAG_MS, ConfigType::Long },
    max_compaction_lag_ms = { MAX_COMPACTION_LAG_MS, ConfigType::Long },
    min_cleanable_dirty_ratio = { MIN_CLEANABLE_DIRTY_RATIO, ConfigType::Double },
    file_delete_delay_ms = { FILE_DELETE_DELAY_MS, ConfigType::Long },
    flush_messages = { FLUSH_MESSAGES, ConfigType::Long },
    flush_ms = { FLUSH_MS, ConfigType::Long },
    index_interval_bytes = { INDEX_INTERVAL_BYTES, ConfigType::Int },
    preallocate = { PREALLOCATE, ConfigType::Boolean },
    message_timestamp_type = { MESSAGE_TIMESTAMP_TYPE, ConfigType::String },
    message_timestamp_after_max_ms = { MESSAGE_TIMESTAMP_AFTER_MAX_MS, ConfigType::Long },
    message_timestamp_before_max_ms = { MESSAGE_TIMESTAMP_BEFORE_MAX_MS, ConfigType::Long },
)]
fn topic_key_types_match_apache_kafka(name: &str, expected: ConfigType) {
    // Read off `apache/kafka:4.3.1` with a DescribeConfigs v3 probe that asked
    // for synonyms and documentation. Every row here is a key Kafka also has,
    // so the byte the JVM AdminClient reads must be the same one.
    let row = lookup(ConfigScope::Topic, name).expect(name);
    check!(row.config_type == expected, "{name}");
}

#[yare::parameterized(
    leader_throttled_rate = { LEADER_THROTTLED_RATE_KEY, ConfigType::Long },
    follower_throttled_rate = { FOLLOWER_THROTTLED_RATE_KEY, ConfigType::Long },
    alter_log_dirs_throttled_rate = { ALTER_LOG_DIRS_THROTTLED_RATE_KEY, ConfigType::Long },
    unclean_leader_election_enable = { UNCLEAN_LEADER_ELECTION_ENABLE, ConfigType::Boolean },
    remote_list_offsets_request_timeout_ms = {
        REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
        ConfigType::Long,
    },
    offsets_retention_minutes = { OFFSETS_RETENTION_MINUTES, ConfigType::Int },
    offsets_retention_check_interval_ms = { OFFSETS_RETENTION_CHECK_INTERVAL_MS, ConfigType::Long },
    connections_max_idle_ms = { CONNECTIONS_MAX_IDLE_MS, ConfigType::Long },
    connections_max_reauth_ms = { CONNECTIONS_MAX_REAUTH_MS, ConfigType::Long },
    node_id = { NODE_ID, ConfigType::Int },
    transactional_id_expiration_ms = { TRANSACTIONAL_ID_EXPIRATION_MS, ConfigType::Int },
    transaction_remove_expired_cleanup_interval_ms = {
        TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
        ConfigType::Int,
    },
)]
fn broker_key_types_match_apache_kafka(name: &str, expected: ConfigType) {
    let row = lookup(ConfigScope::Broker, name).expect(name);
    check!(row.config_type == expected, "{name}");
}

#[yare::parameterized(
    retention_ms = { RETENTION_MS, "604800000" },
    retention_bytes = { RETENTION_BYTES, "-1" },
    segment_bytes = { SEGMENT_BYTES, "1073741824" },
    cleanup_policy = { CLEANUP_POLICY, "delete" },
    compression_type = { COMPRESSION_TYPE, "producer" },
    min_insync_replicas = { MIN_INSYNC_REPLICAS, "1" },
    unclean_leader_election_enable = { UNCLEAN_LEADER_ELECTION_ENABLE, "false" },
    remote_storage_enable = { REMOTE_STORAGE_ENABLE, "false" },
    local_retention_ms = { LOCAL_RETENTION_MS, "-2" },
    local_retention_bytes = { LOCAL_RETENTION_BYTES, "-2" },
    delete_retention_ms = { DELETE_RETENTION_MS, "86400000" },
    leader_throttled_replicas = { LEADER_THROTTLED_REPLICAS_KEY, "" },
    follower_throttled_replicas = { FOLLOWER_THROTTLED_REPLICAS_KEY, "" },
    segment_ms = { SEGMENT_MS, "604800000" },
    segment_index_bytes = { SEGMENT_INDEX_BYTES, "10485760" },
    segment_jitter_ms = { SEGMENT_JITTER_MS, "0" },
    min_compaction_lag_ms = { MIN_COMPACTION_LAG_MS, "0" },
    max_compaction_lag_ms = { MAX_COMPACTION_LAG_MS, "9223372036854775807" },
    min_cleanable_dirty_ratio = { MIN_CLEANABLE_DIRTY_RATIO, "0.5" },
    file_delete_delay_ms = { FILE_DELETE_DELAY_MS, "60000" },
    flush_messages = { FLUSH_MESSAGES, "9223372036854775807" },
    flush_ms = { FLUSH_MS, "9223372036854775807" },
    index_interval_bytes = { INDEX_INTERVAL_BYTES, "4096" },
    preallocate = { PREALLOCATE, "false" },
    message_timestamp_type = { MESSAGE_TIMESTAMP_TYPE, "CreateTime" },
    message_timestamp_after_max_ms = { MESSAGE_TIMESTAMP_AFTER_MAX_MS, "3600000" },
    message_timestamp_before_max_ms = { MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "9223372036854775807" },
)]
fn topic_key_defaults_match_apache_kafka(name: &str, expected: &str) {
    // The same probe's `DEFAULT_CONFIG` values on a topic with no overrides.
    let row = lookup(ConfigScope::Topic, name).expect(name);
    check!(row.default == Some(expected), "{name}");
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
#[yare::parameterized(
    transactional_id_expiration = { TRANSACTIONAL_ID_EXPIRATION_MS, "604800000" },
    transaction_remove_expired_cleanup_interval = {
        TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
        "3600000",
    },
)]
fn transactional_id_expiry_defaults_match_apache_kafka(name: &str, expected: &str) {
    let row = lookup(ConfigScope::Broker, name).expect(name);
    check!(row.default == Some(expected), "{name}");
    // `kafka-configs --alter` answers `Cannot update these configs
    // dynamically`, so `DescribeConfigs` must report them read-only.
    check!(row.read_only, "{name}");
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
