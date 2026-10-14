//! Tests for the per-key and whole-map topic-config validators.

use {
    super::{
        super::{
            DELETE_RETENTION_MS, ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, FILE_DELETE_DELAY_MS,
            FLUSH_MESSAGES, FLUSH_MS, INDEX_INTERVAL_BYTES, INTERNAL_SEGMENT_BYTES,
            LOCAL_RETENTION_BYTES, LOCAL_RETENTION_MS, MAX_COMPACTION_LAG_MS,
            MAX_DECOMPRESSED_MESSAGE_BYTES, MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE, MIN_COMPACTION_LAG_MS,
            MIN_INSYNC_REPLICAS, PREALLOCATE, REMOTE_LOG_COPY_DISABLE,
            REMOTE_LOG_DELETE_ON_DISABLE, REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS,
            SEGMENT_BYTES, SEGMENT_INDEX_BYTES, SEGMENT_JITTER_MS, SEGMENT_MS,
            delivery::DELIVERY_MODE_IMMEDIATE,
        },
        *,
    },
    crate::throttle::{FOLLOWER_THROTTLED_REPLICAS_KEY, LEADER_THROTTLED_REPLICAS_KEY},
    assert2::check,
    krabka_log::CleanupPolicy,
};

/// A config key and its value.
#[derive(Debug, Clone, Copy)]
struct ConfigPair<'a> {
    key: &'a str,
    value: &'a str,
}

/// The owned map of `pairs`.
fn config_map(pairs: &[ConfigPair<'_>]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|ConfigPair { key, value }| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// A name Kafka has no `TopicConfig` entry for, which its own `LogConfig`
/// refuses with `Unknown topic config name`. The probe every unknown-key test
/// uses, so a key krabka later registers cannot quietly turn one of them into
/// a test of nothing.
const UNKNOWN_KEY: &str = "not.a.topic.config";

#[yare::parameterized(
    positive = { "60000", true },
    unlimited = { "-1", true },
    below_unlimited = { "-5", false },
    non_integer = { "abc", false },
)]
fn validate_retention_ms_boundary_cases(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(RETENTION_MS, value).is_ok() == want_ok,
        "retention.ms={value}"
    );
}

/// Kafka floors `segment.bytes` at one mebibyte: `LogConfig` validates the key
/// with `atLeast(1024 * 1024)`, and `apache/kafka:4.3.1` refuses anything below
/// it with `Invalid value 1048575 for configuration segment.bytes: Value must
/// be at least 1048576`. A coordinator that wants a smaller segment reaches
/// past the floor through `internal.segment.bytes`, which `defineInternal`
/// gives no validator at all.
#[yare::parameterized(
    zero = { "0", false },
    one = { "1", false },
    fourteen = { "14", false },
    just_below_floor = { "1048575", false },
    floor = { "1048576", true },
    above_floor = { "1073741824", true },
    non_integer = { "not-a-number", false },
)]
fn validate_segment_bytes_floors_at_kafkas_one_mebibyte(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(SEGMENT_BYTES, value).is_ok() == want_ok,
        "segment.bytes={value}"
    );
}

#[test]
fn internal_segment_bytes_is_not_subject_to_the_topic_floor() {
    // The floor is the topic key's alone. `internal.segment.bytes` is
    // `defineInternal`'d with a null validator, so it still reaches below it.
    assert2::assert!(validate_topic_config(INTERNAL_SEGMENT_BYTES, "1").is_ok());
}

/// `retention.bytes` is one of the few keys Kafka defines with no validator at
/// all: `LogConfig` declares it as a bare `LONG` with a default, so every value
/// the type holds is accepted and `LogManager`'s retention test is the plain
/// `retentionSize < 0`. Refusing `-2` here would lose a `MirrorMaker` replay of
/// a source topic that carries one.
#[yare::parameterized(
    minus_one = { "-1", true },
    minus_two = { "-2", true },
    zero = { "0", true },
    positive = { "1073741824", true },
    i64_min = { "-9223372036854775808", true },
    i64_max = { "9223372036854775807", true },
    above_i64_max = { "9223372036854775808", false },
    non_integer = { "abc", false },
)]
fn validate_retention_bytes_accepts_every_long_kafka_accepts(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(RETENTION_BYTES, value).is_ok() == want_ok,
        "retention.bytes={value}"
    );
}

/// Kafka types `cleanup.policy` as a LIST and derives `compact` and `delete`
/// from it by membership, so either name alone and both names in either order
/// are all valid. Kafka Streams sends `compact,delete` on every windowed-store
/// changelog topic. `ValidList.in` allows the empty list, which runs no
/// cleanup, and refuses a repeated or an empty element.
#[yare::parameterized(
    delete = { "delete", Some(CleanupPolicy::Delete) },
    compact = { "compact", Some(CleanupPolicy::Compact) },
    compact_then_delete = { "compact,delete", Some(CleanupPolicy::CompactAndDelete) },
    delete_then_compact = { "delete,compact", Some(CleanupPolicy::CompactAndDelete) },
    compact_then_spaced_delete = { "compact, delete", Some(CleanupPolicy::CompactAndDelete) },
    junk = { "junk", None },
    compact_then_junk = { "compact,junk", None },
    trailing_compact_separator = { "compact,", None },
    trailing_delete_separator = { "delete,", None },
    duplicate_delete = { "delete,delete", None },
    duplicate_compact = { "compact, compact", None },
    empty = { "", Some(CleanupPolicy::NoCleanup) },
    whitespace = { " ", Some(CleanupPolicy::NoCleanup) },
)]
fn validate_cleanup_policy_follows_kafkas_valid_list(value: &str, expected: Option<CleanupPolicy>) {
    assert2::assert!(parse_cleanup_policy(value).ok() == expected, "{value}");
    assert2::assert!(
        validate_topic_config(CLEANUP_POLICY, value).is_ok() == expected.is_some(),
        "{value}"
    );
}

/// `LogConfig` validates `compression.type` with
/// `ValidString.in(BrokerCompressionType.names())`, and that enum has exactly
/// six members. `none` is a producer-side codec name and is not one of them:
/// `apache/kafka:4.3.1` refuses it with `Invalid value none for configuration
/// compression.type: String must be one of: uncompressed, zstd, lz4, snappy,
/// gzip, producer`.
#[yare::parameterized(
    producer = { "producer", true },
    uncompressed = { "uncompressed", true },
    gzip = { "gzip", true },
    snappy = { "snappy", true },
    lz4 = { "lz4", true },
    zstd = { "zstd", true },
    none = { "none", false },
    empty = { "", false },
    uppercase_gzip = { "GZIP", false },
    leading_space = { " gzip", true },
)]
fn validate_compression_accepts_kafkas_six_names_and_no_others(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(COMPRESSION_TYPE, value).is_ok() == want_ok,
        "compression.type={value}"
    );
}

#[test]
fn validate_compression_bogus_rejected() {
    let err = validate_topic_config(COMPRESSION_TYPE, "bzip3").unwrap_err();
    assert2::assert!(err.contains("compression.type"), "got: {err}");
}

#[test]
fn parse_compression_type_maps_producer_to_none() {
    assert2::assert!(parse_compression_type("producer") == Ok(None));
}

#[yare::parameterized(
    gzip = { "gzip", krabka_compression::CompressionType::Gzip },
    snappy = { "snappy", krabka_compression::CompressionType::Snappy },
    lz4 = { "lz4", krabka_compression::CompressionType::Lz4 },
    zstd = { "zstd", krabka_compression::CompressionType::Zstd },
    uncompressed = { "uncompressed", krabka_compression::CompressionType::None },
)]
fn parse_compression_type_maps_codecs(input: &str, want: krabka_compression::CompressionType) {
    assert2::assert!(
        parse_compression_type(input) == Ok(Some(want)),
        "compression.type={input}"
    );
}

#[test]
fn parse_compression_type_rejects_none() {
    assert2::assert!(parse_compression_type("none").is_err());
}

#[test]
fn validate_min_isr_positive_accepted() {
    assert2::assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
}

/// `DescribeConfigs` reports both keys as `ConfigType::Int`, and
/// `apache/kafka:4.3.1` refuses a value past `i32::MAX` on both:
/// `Invalid value 2147483648 for configuration segment.bytes: Not a number
/// of type INT`. A value the broker accepts must fit the type it advertises,
/// so the largest accepted value is `i32::MAX`.
#[yare::parameterized(
    segment_bytes_max = { SEGMENT_BYTES, "2147483647", true },
    segment_bytes_above_max = { SEGMENT_BYTES, "2147483648", false },
    segment_bytes_i64_max = { SEGMENT_BYTES, "9223372036854775807", false },
    min_isr_max = { MIN_INSYNC_REPLICAS, "2147483647", true },
    min_isr_above_max = { MIN_INSYNC_REPLICAS, "2147483648", false },
    min_isr_zero = { MIN_INSYNC_REPLICAS, "0", false },
    min_isr_negative = { MIN_INSYNC_REPLICAS, "-1", false },
)]
fn an_int_key_refuses_what_kafkas_int_cannot_hold(key: &str, value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(key, value).is_ok() == want_ok,
        "{key}={value}"
    );
}

#[test]
fn validate_unknown_key_rejected() {
    assert2::assert!(
        validate_topic_config(UNKNOWN_KEY, "1000")
            == Err(format!("Unknown topic config name: {UNKNOWN_KEY}"))
    );
}

/// The Kafka `TopicConfig` names krabka registers, with the value Kafka's own
/// `ConfigDef` accepts and one it refuses.
/// The Kafka `TopicConfig` names krabka registers, with the value Kafka's own
/// `ConfigDef` accepts and one it refuses.
#[yare::parameterized(
    segment_ms = { SEGMENT_MS, "60000", "0" },
    segment_index_bytes = { SEGMENT_INDEX_BYTES, "10485760", "3" },
    segment_jitter_ms = { SEGMENT_JITTER_MS, "0", "-1" },
    min_compaction_lag_ms = { MIN_COMPACTION_LAG_MS, "0", "-1" },
    max_compaction_lag_ms = { MAX_COMPACTION_LAG_MS, "9223372036854775807", "0" },
    min_cleanable_dirty_ratio = { MIN_CLEANABLE_DIRTY_RATIO, "0.5", "2.0" },
    file_delete_delay_ms = { FILE_DELETE_DELAY_MS, "60000", "-1" },
    flush_messages = { FLUSH_MESSAGES, "10000", "0" },
    flush_ms = { FLUSH_MS, "1000", "-1" },
    index_interval_bytes = { INDEX_INTERVAL_BYTES, "4096", "-1" },
    preallocate = { PREALLOCATE, "true", "yes" },
    message_timestamp_type = { MESSAGE_TIMESTAMP_TYPE, "LogAppendTime", "WallClock" },
    message_timestamp_after_max_ms = { MESSAGE_TIMESTAMP_AFTER_MAX_MS, "3600000", "-1" },
    message_timestamp_before_max_ms = { MESSAGE_TIMESTAMP_BEFORE_MAX_MS, "3600000", "-1" },
)]
fn the_remaining_kafka_topic_config_names_are_accepted(key: &str, accepted: &str, refused: &str) {
    check!(is_recognized(key), "{key}");
    check!(
        validate_topic_config(key, accepted) == Ok(()),
        "{key}={accepted}"
    );
    check!(
        validate_topic_config(key, refused).is_err(),
        "{key}={refused}"
    );
}

/// Streams applications send both override sets on their internal topics.
#[yare::parameterized(
    repartition_topic = {
        maplit::btreemap! {
            CLEANUP_POLICY.to_string() => "delete".to_string(),
            SEGMENT_BYTES.to_string() => "52428800".to_string(),
            RETENTION_MS.to_string() => "-1".to_string(),
            MESSAGE_TIMESTAMP_TYPE.to_string() => "CreateTime".to_string(),
        },
    },
    windowed_changelog = {
        maplit::btreemap! {
            CLEANUP_POLICY.to_string() => "compact,delete".to_string(),
            RETENTION_MS.to_string() => "86400000".to_string(),
            MIN_COMPACTION_LAG_MS.to_string() => "0".to_string(),
            MESSAGE_TIMESTAMP_TYPE.to_string() => "CreateTime".to_string(),
        },
    },
)]
fn streams_topic_override_set_is_accepted(overrides: std::collections::BTreeMap<String, String>) {
    assert2::assert!(validate_topic_config_map(&overrides) == Ok(()));
}

#[yare::parameterized(
    zero = { "0", true },
    midpoint = { "0.5", true },
    one = { "1", true },
    above_one = { "1.0001", false },
    below_zero = { "-0.1", false },
    nan = { "NaN", false },
    non_numeric = { "half", false },
)]
fn validate_dirty_ratio_accepts_the_closed_unit_interval(value: &str, want_ok: bool) {
    check!(
        validate_topic_config(MIN_CLEANABLE_DIRTY_RATIO, value).is_ok() == want_ok,
        "min.cleanable.dirty.ratio={value}"
    );
}

#[test]
fn validate_remote_storage_enable_accepts_bools() {
    assert2::assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "true").is_ok());
    assert2::assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "false").is_ok());
}

#[test]
fn validate_remote_storage_enable_rejects_junk() {
    let err = validate_topic_config(REMOTE_STORAGE_ENABLE, "yes").unwrap_err();
    assert2::assert!(err.contains("remote.storage.enable"), "got: {err}");
}

#[test]
fn is_recognized_includes_remote_storage_enable() {
    assert2::assert!(is_recognized(REMOTE_STORAGE_ENABLE));
}

#[yare::parameterized(
    retention_ms = { RETENTION_MS, true },
    retention_bytes = { RETENTION_BYTES, true },
    segment_bytes = { SEGMENT_BYTES, true },
    cleanup_policy = { CLEANUP_POLICY, true },
    compression_type = { COMPRESSION_TYPE, true },
    min_insync_replicas = { MIN_INSYNC_REPLICAS, true },
    unknown = { UNKNOWN_KEY, false },
    empty = { "", false },
)]
fn is_recognized_matches_whitelist(key: &str, want: bool) {
    assert2::assert!(is_recognized(key) == want, "key {key:?}");
}

#[yare::parameterized(
    minus_two = { "-2" },
    minus_one = { "-1" },
    positive = { "60000" },
)]
fn validate_local_retention_ms_accepts_minus_one_minus_two_and_positive(value: &str) {
    assert2::assert!(
        validate_topic_config(LOCAL_RETENTION_MS, value) == Ok(()),
        "local.retention.ms={value}"
    );
}

#[test]
fn validate_local_retention_ms_rejects_below_minus_two() {
    assert2::assert!(validate_topic_config(LOCAL_RETENTION_MS, "-3").is_err());
}

#[test]
fn is_recognized_includes_local_retention_keys() {
    assert2::assert!(is_recognized(LOCAL_RETENTION_MS));
    assert2::assert!(is_recognized(LOCAL_RETENTION_BYTES));
}

#[yare::parameterized(
    zero = { "0", true },
    positive = { "86400000", true },
    negative = { "-1", false },
)]
fn validate_delete_retention_ms_accepts_nonneg_rejects_negative(value: &str, want_ok: bool) {
    assert2::assert!(
        validate_topic_config(DELETE_RETENTION_MS, value).is_ok() == want_ok,
        "delete.retention.ms={value}"
    );
}

#[test]
fn is_recognized_includes_delete_retention_ms() {
    assert2::assert!(is_recognized(DELETE_RETENTION_MS));
}

#[test]
fn compact_and_scheduled_delivery_exclude_each_other() {
    let cases = [
        (Some("compact"), Some(DELIVERY_MODE_SCHEDULED), false),
        (Some("compact"), Some(DELIVERY_MODE_IMMEDIATE), true),
        (Some("compact"), None, true),
        (Some("delete"), Some(DELIVERY_MODE_SCHEDULED), true),
        (None, Some(DELIVERY_MODE_SCHEDULED), true),
        (None, None, true),
    ];
    for (policy, mode, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        if let Some(policy) = policy {
            overrides.insert(CLEANUP_POLICY.to_string(), policy.to_string());
        }
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_string(), mode.to_string());
        }
        assert2::assert!(
            validate_config_combination(&overrides, &TopicDefaults::default(), true).is_ok()
                == want_ok,
            "overrides {overrides:?}"
        );
    }
}

/// Kafka's `LogConfig.validateValues`: a `min.compaction.lag.ms` above
/// `max.compaction.lag.ms` is refused, and an alter that names only one of the
/// two is checked against the other's stored default, which is what Kafka's
/// fully-defaulted property map carries.
#[test]
fn a_min_compaction_lag_above_the_max_is_refused() {
    let cases = [
        ("both, min below max", Some("1000"), Some("60000"), true),
        ("both, equal", Some("60000"), Some("60000"), true),
        ("both, min above max", Some("60000"), Some("1000"), false),
        (
            "min alone against the unbounded default",
            Some("60000"),
            None,
            true,
        ),
        ("max alone against the zero default", None, Some("1"), true),
        ("neither", None, None, true),
    ];
    for (case, min, max, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        if let Some(min) = min {
            overrides.insert(MIN_COMPACTION_LAG_MS.to_string(), min.to_string());
        }
        if let Some(max) = max {
            overrides.insert(MAX_COMPACTION_LAG_MS.to_string(), max.to_string());
        }
        assert2::assert!(
            validate_config_combination(&overrides, &TopicDefaults::default(), true).is_ok()
                == want_ok,
            "{case}: {overrides:?}"
        );
    }
}

/// The refusal carries Kafka's wording, which `kafka-configs` prints verbatim.
#[test]
fn the_compaction_lag_conflict_carries_kafkas_message() {
    let overrides = BTreeMap::from([
        (MIN_COMPACTION_LAG_MS.to_string(), "60000".to_string()),
        (MAX_COMPACTION_LAG_MS.to_string(), "1".to_string()),
    ]);
    assert2::assert!(
        validate_config_combination(&overrides, &TopicDefaults::default(), true)
            == Err(
                "conflict topic config setting min.compaction.lag.ms (60000) > \
                 max.compaction.lag.ms (1)"
                    .to_string()
            )
    );
}

/// A tiered topic's config map: `remote.storage.enable=true` followed by `rest`.
fn tiered_pairs<const N: usize>(rest: [ConfigPair<'static>; N]) -> Vec<ConfigPair<'static>> {
    let mut pairs = vec![ConfigPair {
        key: REMOTE_STORAGE_ENABLE,
        value: "true",
    }];
    pairs.extend(rest);
    pairs
}

/// One row of the tiered-storage table.
struct TierCase<'a> {
    /// The broker's tier state.
    tier_on: bool,
    /// The topic's config map.
    pairs: Vec<ConfigPair<'a>>,
    /// Kafka's result.
    want: Result<(), String>,
}

/// The tiered-storage rows: the broker's tier state, the map, and Kafka's result.
fn tier_cases_a() -> Vec<TierCase<'static>> {
    vec![
        TierCase {
            tier_on: false,
            pairs: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "true",
            }],
            want: Err(REMOTE_STORAGE_DISABLED_MESSAGE.to_owned()),
        },
        TierCase {
            tier_on: false,
            pairs: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "false",
            }],
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: RETENTION_BYTES,
                    value: "100",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "-1",
                },
            ]),
            want: Err(
                "Invalid value -1 for configuration local.retention.bytes: Value must not be -1 \
                 as retention.bytes value is set as 100."
                    .to_owned(),
            ),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: RETENTION_BYTES,
                    value: "100",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "200",
                },
            ]),
            want: Err(
                "Invalid value 200 for configuration local.retention.bytes: Value must not be \
                 more than retention.bytes property value: 100"
                    .to_owned(),
            ),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: RETENTION_BYTES,
                    value: "-1",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "-1",
                },
            ]),
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([ConfigPair {
                key: LOCAL_RETENTION_MS,
                value: "-1",
            }]),
            want: Err(
                "Invalid value -1 for configuration local.retention.ms: Value must not be -1 as \
                 retention.ms value is set as 604800000."
                    .to_owned(),
            ),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: RETENTION_MS,
                    value: "-1",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "5000",
                },
            ]),
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: REMOTE_LOG_COPY_DISABLE,
                    value: "true",
                },
                ConfigPair {
                    key: RETENTION_MS,
                    value: "1000",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "500",
                },
            ]),
            want: Err(
                "When `remote.log.copy.disable` is set to true, the `local.retention.ms` and \
                 `retention.ms` must be set to the identical value because there will be no more \
                 logs copied to the remote storage."
                    .to_owned(),
            ),
        },
    ]
}

/// The tiered-storage rows: the broker's tier state, the map, and Kafka's result.
fn tier_cases_b() -> Vec<TierCase<'static>> {
    let policy_message = Err(REMOTE_STORAGE_POLICY_MESSAGE.to_owned());
    vec![
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: REMOTE_LOG_COPY_DISABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "-2",
                },
            ]),
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([ConfigPair {
                key: CLEANUP_POLICY,
                value: "compact",
            }]),
            want: policy_message.clone(),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([ConfigPair {
                key: CLEANUP_POLICY,
                value: "compact,delete",
            }]),
            want: policy_message,
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([ConfigPair {
                key: CLEANUP_POLICY,
                value: "",
            }]),
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "false",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "500",
                },
                ConfigPair {
                    key: RETENTION_BYTES,
                    value: "100",
                },
            ],
            want: Ok(()),
        },
        // Kafka trunk's copy-lag rules against the effective local retention.
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "1000",
                },
                ConfigPair {
                    key: REMOTE_COPY_LAG_MS,
                    value: "2000",
                },
            ]),
            want: Err(
                "Invalid value 2000 for configuration remote.copy.lag.ms: Value must not exceed \
                 local.retention.ms (effective value: 1000)"
                    .to_owned(),
            ),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "-1",
                },
                ConfigPair {
                    key: RETENTION_MS,
                    value: "-1",
                },
                ConfigPair {
                    key: REMOTE_COPY_LAG_MS,
                    value: "2000",
                },
            ]),
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: tiered_pairs([
                ConfigPair {
                    key: RETENTION_BYTES,
                    value: "100",
                },
                ConfigPair {
                    key: REMOTE_COPY_LAG_BYTES,
                    value: "200",
                },
            ]),
            want: Err(
                "Invalid value 200 for configuration remote.copy.lag.bytes: Value must not \
                 exceed local.retention.bytes (effective value: 100)"
                    .to_owned(),
            ),
        },
    ]
}

/// The tiered-storage rows: the broker's tier state, the map, and Kafka's result.
fn tier_cases_c() -> Vec<TierCase<'static>> {
    vec![
        TierCase {
            tier_on: true,
            pairs: vec![ConfigPair {
                key: REMOTE_COPY_LAG_MS,
                value: "-1",
            }],
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: vec![ConfigPair {
                key: MAX_DECOMPRESSED_MESSAGE_BYTES,
                value: "2147483639",
            }],
            want: Ok(()),
        },
        TierCase {
            tier_on: true,
            pairs: vec![ConfigPair {
                key: ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
                value: "true",
            }],
            want: Ok(()),
        },
    ]
}

/// The tiered-storage rows: the broker's tier state, the map, and Kafka's result.
fn tier_cases() -> Vec<TierCase<'static>> {
    let mut cases = tier_cases_a();
    cases.extend(tier_cases_b());
    cases.extend(tier_cases_c());
    cases
}

/// Kafka's `LogConfig.validateTopicLogConfigValues` tiered-storage rules, run
/// on the resulting map when `remote.storage.enable` is true. Each row is the
/// broker's tier state, the map, and Kafka's result.
#[test]
fn tiered_storage_rules_follow_kafkas_log_config() {
    let cases = tier_cases();
    for TierCase {
        tier_on,
        pairs,
        want,
    } in cases
    {
        let map = config_map(&pairs);
        check!(
            canonical_topic_config_map(
                &map,
                &TopicDefaults::default(),
                tier_on,
                UnstableApiVersions::Enabled,
            )
            .map(drop)
                == want,
            "tier={tier_on} {pairs:?}"
        );
    }
}

/// One row of the cross-key table.
struct CrossKeyCase {
    /// The cluster-wide broker configs.
    cluster: Vec<ConfigPair<'static>>,
    /// The topic's overrides.
    topic: Vec<ConfigPair<'static>>,
    want: Result<(), String>,
}

fn cross_key_cases() -> Vec<CrossKeyCase> {
    let local_ms_over = |value: i64, total: i64| {
        Err(format!(
            "Invalid value {value} for configuration local.retention.ms: Value must not be \
             more than retention.ms property value: {total}"
        ))
    };
    let cases: Vec<CrossKeyCase> = vec![
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "true",
            }],
            want: Err(REMOTE_STORAGE_POLICY_MESSAGE.to_owned()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: CLEANUP_POLICY,
                    value: "delete",
                },
            ],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleanup.policy",
                value: "compact",
            }],
            topic: vec![ConfigPair {
                key: REMOTE_STORAGE_ENABLE,
                value: "false",
            }],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.ms",
                value: "1000",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "2000",
                },
            ],
            want: local_ms_over(2000, 1000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.hours",
                value: "1",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "3600001",
                },
            ],
            want: local_ms_over(3_600_001, 3_600_000),
        },
        CrossKeyCase {
            cluster: vec![
                ConfigPair {
                    key: "log.retention.hours",
                    value: "1",
                },
                ConfigPair {
                    key: "log.retention.minutes",
                    value: "1",
                },
            ],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "60001",
                },
            ],
            want: local_ms_over(60_001, 60_000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.ms",
                value: "-5",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_MS,
                    value: "60001",
                },
            ],
            want: Ok(()),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.local.retention.ms",
                value: "5000",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: RETENTION_MS,
                    value: "1000",
                },
            ],
            want: local_ms_over(5000, 1000),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.retention.bytes",
                value: "100",
            }],
            topic: vec![
                ConfigPair {
                    key: REMOTE_STORAGE_ENABLE,
                    value: "true",
                },
                ConfigPair {
                    key: LOCAL_RETENTION_BYTES,
                    value: "200",
                },
            ],
            want: Err(
                "Invalid value 200 for configuration local.retention.bytes: Value must not be \
                 more than retention.bytes property value: 100"
                    .to_owned(),
            ),
        },
        CrossKeyCase {
            cluster: vec![ConfigPair {
                key: "log.cleaner.min.compaction.lag.ms",
                value: "100",
            }],
            topic: vec![ConfigPair {
                key: MAX_COMPACTION_LAG_MS,
                value: "50",
            }],
            want: Err(
                "conflict topic config setting min.compaction.lag.ms (100) > \
                 max.compaction.lag.ms (50)"
                    .to_owned(),
            ),
        },
    ];
    cases
}

/// Kafka validates a topic's map merged over the broker's effective defaults
/// (`KafkaConfig.extractLogConfigMap`), so a key the topic leaves unset takes
/// the cluster-wide broker default, in the topic key's unit, and not the
/// registry default.
#[test]
fn cross_key_rules_read_the_cluster_broker_defaults() {
    let cases = cross_key_cases();
    for CrossKeyCase {
        cluster,
        topic,
        want,
    } in cases
    {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for ConfigPair { key: name, value } in &cluster {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    config_name: (*name).to_owned(),
                    config_value: Some((*value).to_owned()),
                },
            ));
        }
        let map = config_map(&topic);
        check!(
            canonical_topic_config_map(
                &map,
                &TopicDefaults::from_image(&image, UnstableApiVersions::Enabled),
                true,
                UnstableApiVersions::Enabled,
            )
            .map(drop)
                == want,
            "{cluster:?} {topic:?}"
        );
    }
}

/// The per-key refusals of Kafka trunk's four newest topic keys, on a broker
/// serving trunk.
#[yare::parameterized(
    remote_copy_lag_ms = { REMOTE_COPY_LAG_MS, "-2" },
    remote_copy_lag_bytes = { REMOTE_COPY_LAG_BYTES, "-2" },
    max_decompressed_message_bytes_zero = { MAX_DECOMPRESSED_MESSAGE_BYTES, "0" },
    max_decompressed_message_bytes_too_large = {
        MAX_DECOMPRESSED_MESSAGE_BYTES,
        "2147483640",
    },
    errors_deadletterqueue_group_enable = { ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, "yes" },
)]
fn kafka_trunks_newest_topic_keys_refuse_what_kafka_refuses(key: &str, value: &str) {
    check!(
        canonical_topic_config(key, value, UnstableApiVersions::Enabled).is_err(),
        "{key}={value}"
    );
}

/// #907's companion: Kafka 4.3.1's `LogConfig` does not define Kafka trunk's
/// four newest topic keys, so `LogConfig.validateNames` refuses each with
/// `Unknown topic config name`, whatever the value, on every path that
/// validates a topic's map. `unstable.api.versions.enable` accepts them.
#[yare::parameterized(
    remote_copy_lag_ms = { REMOTE_COPY_LAG_MS, " 1000 ", "1000" },
    remote_copy_lag_bytes = { REMOTE_COPY_LAG_BYTES, "-1", "-1" },
    max_decompressed_message_bytes = {
        MAX_DECOMPRESSED_MESSAGE_BYTES,
        "2147483639",
        "2147483639",
    },
    errors_deadletterqueue_group_enable = {
        ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
        "TRUE",
        "true",
    },
)]
fn kafka_trunks_newest_topic_keys_need_unstable_api_versions(
    key: &str,
    value: &str,
    canonical: &str,
) {
    let unknown = format!("Unknown topic config name: {key}");
    check!(
        canonical_topic_config(key, value, UnstableApiVersions::Disabled) == Err(unknown.clone()),
        "{key}"
    );
    check!(
        canonical_topic_config(key, value, UnstableApiVersions::Enabled)
            == Ok(canonical.to_owned()),
        "{key}"
    );
    let map = BTreeMap::from([(key.to_owned(), value.to_owned())]);
    check!(
        canonical_topic_config_map(
            &map,
            &TopicDefaults::default(),
            true,
            UnstableApiVersions::Disabled
        )
        .map(drop)
            == Err(unknown),
        "{key}"
    );
    check!(
        canonical_topic_config_map(
            &map,
            &TopicDefaults::default(),
            true,
            UnstableApiVersions::Enabled
        ) == Ok(BTreeMap::from([(key.to_owned(), canonical.to_owned())])),
        "{key}"
    );
}

/// Under Kafka 4.3.1 the broker keys `log.remote.copy.lag.ms` and
/// `log.remote.copy.lag.bytes` are not `KafkaConfig` keys, so
/// `extractLogConfigMap` carries no default for the topic keys from them, and
/// a cluster value of either reaches no topic's effective defaults.
#[test]
fn trunk_topic_keys_take_no_cluster_default_under_kafka_4_3_1() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
        krabka_metadata::BrokerConfigRecord {
            node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: "log.remote.copy.lag.ms".to_owned(),
            config_value: Some("5000".to_owned()),
        },
    ));
    check!(
        TopicDefaults::from_image(&image, UnstableApiVersions::Disabled).get(REMOTE_COPY_LAG_MS)
            == None
    );
    check!(
        TopicDefaults::from_image(&image, UnstableApiVersions::Enabled).get(REMOTE_COPY_LAG_MS)
            == Some("5000")
    );
}

/// A `compact,delete` topic is a compacted topic for every cross-key rule, so
/// KFC-1's exclusion covers it too.
#[test]
fn compact_and_delete_is_compaction_for_the_scheduled_delivery_rule() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact,delete".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};

    assert2::assert!(
        validate_config_combination(&overrides, &TopicDefaults::default(), true).is_err()
    );
}

/// KFC-1's third exclusion: a scheduled topic reads each batch's
/// `max_timestamp` as its activation time, and log-append stamping overwrites
/// exactly that field, so the pair would deliver every record at once.
#[test]
fn log_append_time_and_scheduled_delivery_exclude_each_other() {
    let cases = [
        ("LogAppendTime", Some(DELIVERY_MODE_SCHEDULED), false),
        ("LogAppendTime", Some(DELIVERY_MODE_IMMEDIATE), true),
        ("LogAppendTime", None, true),
        ("CreateTime", Some(DELIVERY_MODE_SCHEDULED), true),
        ("CreateTime", None, true),
    ];
    for (timestamp_type, mode, want_ok) in cases {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            MESSAGE_TIMESTAMP_TYPE.to_string(),
            timestamp_type.to_string(),
        );
        if let Some(mode) = mode {
            overrides.insert(DELIVERY_MODE.to_string(), mode.to_string());
        }
        let outcome = validate_config_combination(&overrides, &TopicDefaults::default(), true);
        check!(
            outcome.is_ok() == want_ok,
            "message.timestamp.type={timestamp_type} delivery.mode={mode:?}"
        );
        if !want_ok {
            let error = outcome.unwrap_err();
            check!(error.contains(MESSAGE_TIMESTAMP_TYPE), "got: {error}");
            check!(error.contains(DELIVERY_MODE), "got: {error}");
        }
    }
}

/// The whole-map entry point applies the rule too, which is the path
/// `CreateTopics` takes.
#[test]
fn validate_topic_config_map_refuses_log_append_time_on_a_scheduled_topic() {
    let overrides = maplit::btreemap! {
    MESSAGE_TIMESTAMP_TYPE.to_string() => "LogAppendTime".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};

    assert2::assert!(validate_topic_config_map(&overrides).is_err());
}

#[test]
fn compact_plus_scheduled_rejection_names_both_keys() {
    let overrides = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    let error =
        validate_config_combination(&overrides, &TopicDefaults::default(), true).unwrap_err();
    assert2::assert!(error.contains(CLEANUP_POLICY), "got: {error}");
    assert2::assert!(error.contains(DELIVERY_MODE), "got: {error}");
}

#[test]
fn validate_topic_config_map_checks_pairs_and_then_combinations() {
    let accepted = maplit::btreemap! {
    RETENTION_MS.to_string() => "60000".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    assert2::assert!(validate_topic_config_map(&accepted) == Ok(()));

    let bad_pair = maplit::btreemap! {DELIVERY_MODE.to_string() => "later".to_string()};
    assert2::assert!(validate_topic_config_map(&bad_pair).is_err());

    let unknown_key = maplit::btreemap! {UNKNOWN_KEY.to_string() => "1000".to_string()};
    assert2::assert!(validate_topic_config_map(&unknown_key).is_err());

    let bad_combination = maplit::btreemap! {
    CLEANUP_POLICY.to_string() => "compact".to_string(),
    DELIVERY_MODE.to_string() => DELIVERY_MODE_SCHEDULED.to_string()};
    assert2::assert!(validate_topic_config_map(&bad_combination).is_err());
}

/// KIP-950 `LogConfig.validateRemoteStorageConfigs`: tiered storage cannot be
/// turned off without saying what happens to the segments already in the
/// tier.
#[test]
fn disabling_tiered_storage_needs_the_delete_flag() {
    let stored =
        |enabled: bool| BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_string(), enabled.to_string())]);
    let cases = [
        (
            "the bare flip is refused",
            Some(stored(true)),
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            false,
        ),
        (
            "the flip with delete-on-disable is accepted",
            Some(stored(true)),
            vec![
                (REMOTE_STORAGE_ENABLE, "false"),
                (REMOTE_LOG_DELETE_ON_DISABLE, "true"),
            ],
            true,
        ),
        (
            "the read-only tier keeps the flag on, so nothing is refused",
            Some(stored(true)),
            vec![
                (REMOTE_STORAGE_ENABLE, "true"),
                (REMOTE_LOG_COPY_DISABLE, "true"),
            ],
            true,
        ),
        (
            "a topic that was never tiered is not flipping anything off",
            Some(stored(false)),
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            true,
        ),
        (
            "a topic with no stored config at all",
            None,
            vec![(REMOTE_STORAGE_ENABLE, "false")],
            true,
        ),
        (
            "turning it on is never refused",
            Some(stored(false)),
            vec![(REMOTE_STORAGE_ENABLE, "true")],
            true,
        ),
    ];
    for (case, current, next, want_ok) in cases {
        let next: BTreeMap<String, String> = next
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        assert2::assert!(
            validate_remote_storage_disable(current.as_ref(), &next).is_ok() == want_ok,
            "{case}: {next:?}"
        );
    }
}

/// The refusal names both of Kafka's ways out, because an operator reads it
/// out of `kafka-configs` and has to pick one.
#[test]
fn the_disable_refusal_names_both_ways_out() {
    let current = BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_string(), "true".to_string())]);
    let next = BTreeMap::from([(REMOTE_STORAGE_ENABLE.to_string(), "false".to_string())]);
    let message = validate_remote_storage_disable(Some(&current), &next)
        .expect_err("the bare flip is refused");
    check!(
        message
            == "It is invalid to disable remote storage without deleting remote data. If you \
                want to keep the remote data and turn to read only, please set \
                `remote.storage.enable=true,remote.log.copy.disable=true`. If you want to \
                disable remote storage and delete all remote data, please set \
                `remote.storage.enable=false,remote.log.delete.on.disable=true`."
    );
}

/// Kafka's `ConfigDef.parseType` trims a value, reads a boolean in any case,
/// reads a `DOUBLE` with `Double.parseDouble`, and checks a throttled replica
/// list with `ThrottledReplicaListValidator`'s regex. Each row is Kafka's
/// result, and an accepted row carries the canonical value krabka stores,
/// which is `ConfigDef.convertToString` of the parsed value.
#[yare::parameterized(
    uppercase_boolean = { PREALLOCATE, "TRUE", Some("true") },
    whitespace_boolean = { REMOTE_LOG_COPY_DISABLE, " false ", Some("false") },
    invalid_boolean = { PREALLOCATE, "yes", None },
    trimmed_long = { RETENTION_MS, " 1000 ", Some("1000") },
    trimmed_int = { SEGMENT_BYTES, "1048576 ", Some("1048576") },
    trimmed_enum = { COMPRESSION_TYPE, " gzip", Some("gzip") },
    double_suffix = { MIN_CLEANABLE_DIRTY_RATIO, "0.5d", Some("0.5") },
    integral_double = { MIN_CLEANABLE_DIRTY_RATIO, "1", Some("1.0") },
    spaced_cleanup_list = { CLEANUP_POLICY, " compact , delete ", Some("compact,delete") },
    empty_cleanup_list = { CLEANUP_POLICY, " ", Some("") },
    blank_leader_throttle = { LEADER_THROTTLED_REPLICAS_KEY, " ", Some("") },
    wildcard_leader_throttle = { LEADER_THROTTLED_REPLICAS_KEY, " * ", Some("*") },
    leader_throttle_list = { LEADER_THROTTLED_REPLICAS_KEY, "0:1,,1:2", Some("0:1,,1:2") },
    negative_leader_throttle = { LEADER_THROTTLED_REPLICAS_KEY, "-1:1", None },
    signed_leader_throttle = { LEADER_THROTTLED_REPLICAS_KEY, "+0:1", None },
    spaced_follower_throttle = { FOLLOWER_THROTTLED_REPLICAS_KEY, "0 : 1", None },
)]
fn values_are_parsed_the_way_kafkas_config_def_parses_them(
    key: &str,
    value: &str,
    canonical: Option<&str>,
) {
    check!(
        canonical_topic_config(key, value, UnstableApiVersions::Disabled)
            .ok()
            .as_deref()
            == canonical,
        "{key}={value:?}"
    );
}

/// `apply_to_log_config` reads a stored value with the parser validation uses,
/// so a value validation accepts is never read back as its default.
#[test]
fn the_log_config_reader_parses_what_validation_accepts() {
    let overrides = BTreeMap::from([
        (REMOTE_STORAGE_ENABLE.to_owned(), "TRUE".to_owned()),
        (RETENTION_MS.to_owned(), " 1000 ".to_owned()),
        (CLEANUP_POLICY.to_owned(), String::new()),
    ]);
    let applied = super::super::log_config::apply_to_log_config(
        &overrides,
        &krabka_log::LogConfig::default(),
    );
    check!(applied.remote_storage_enable);
    check!(applied.retention == Some(krabka_units::millis(1000)));
    check!(applied.cleanup_policy == CleanupPolicy::NoCleanup);
}

/// Each refusal is Kafka's own `ConfigException` text: `Unknown topic config
/// name: <key>`, or `Invalid value <parsed value> for configuration <key>:
/// <validator message>`.
#[yare::parameterized(
    unknown_key = { "foo", "1", "Unknown topic config name: foo" },
    negative_max_message_bytes = {
        "max.message.bytes",
        "-1",
        "Invalid value -1 for configuration max.message.bytes: Value must be at least 0",
    },
    lz4_level_too_high = {
        "compression.lz4.level",
        "18",
        "Invalid value 18 for configuration compression.lz4.level: Value must be no more than 17",
    },
    gzip_level_zero = {
        "compression.gzip.level",
        "0",
        "Invalid value 0 for configuration compression.gzip.level: Value must be between 1 and 9 or equal to -1",
    },
    non_integer_retention_ms = {
        RETENTION_MS,
        "abc",
        "Invalid value abc for configuration retention.ms: Not a number of type LONG",
    },
    segment_bytes_int_overflow = {
        SEGMENT_BYTES,
        "2147483648",
        "Invalid value 2147483648 for configuration segment.bytes: Not a number of type INT",
    },
    invalid_boolean = {
        PREALLOCATE,
        "yes",
        "Invalid value yes for configuration preallocate: Expected value to be either true or false",
    },
    invalid_compression_type = {
        COMPRESSION_TYPE,
        "none",
        "Invalid value none for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer",
    },
    invalid_timestamp_type = {
        MESSAGE_TIMESTAMP_TYPE,
        "x",
        "Invalid value x for configuration message.timestamp.type: String must be one of: CreateTime, LogAppendTime",
    },
    invalid_cleanup_policy = {
        CLEANUP_POLICY,
        "foo",
        "Invalid value foo for configuration cleanup.policy: String must be one of: compact, delete",
    },
    duplicate_cleanup_policy = {
        CLEANUP_POLICY,
        "delete,delete",
        "Configuration 'cleanup.policy' values must not be duplicated.",
    },
    empty_cleanup_policy_item = {
        CLEANUP_POLICY,
        "delete,",
        "Configuration 'cleanup.policy' values must not be empty.",
    },
    dirty_ratio_too_high = {
        MIN_CLEANABLE_DIRTY_RATIO,
        "2",
        "Invalid value 2.0 for configuration min.cleanable.dirty.ratio: Value must be no more than 1",
    },
    invalid_follower_throttle = {
        FOLLOWER_THROTTLED_REPLICAS_KEY,
        "0 : 1",
        "Invalid value [0 : 1] for configuration follower.replication.throttled.replicas: follower.replication.throttled.replicas must be the literal '*' or a list of replicas in the following format: [partitionId]:[brokerId],[partitionId]:[brokerId],...",
    },
)]
fn refusals_carry_kafkas_config_exception_text(key: &str, value: &str, message: &str) {
    check!(
        validate_topic_config(key, value) == Err(message.to_owned()),
        "{key}={value}"
    );
}

/// Kafka's `ConfigurationControlManager.validateAlterConfig` refuses a value
/// longer than `Short.MAX_VALUE` with `INVALID_CONFIG` before it validates any
/// key, and counts the length as Java's `String.length()` does, in UTF-16 code
/// units: a value of 32767 units fits, and one of 32768 does not, whatever the
/// bytes it takes.
#[yare::parameterized(
    ascii_fits = { "a".repeat(32_767), true },
    ascii_one_too_long = { "a".repeat(32_768), false },
    two_byte_fits = { "é".repeat(32_767), true },
    two_byte_too_long = { "é".repeat(32_768), false },
    astral_fits = { "😀".repeat(16_383), true },
    astral_too_long = { "😀".repeat(16_384), false },
)]
fn a_config_value_size_matches_java_string_length(value: String, want_ok: bool) {
    const MESSAGE: &str = "The configuration value cannot be added because it exceeds the \
                           maximum value size of 32767 bytes.";
    let expected = if want_ok {
        Ok(())
    } else {
        Err(MESSAGE.to_owned())
    };
    check!(check_value_size(&value) == expected);
}

/// The aggregate config-map path checks a value's size before validating its
/// key, and the per-key path uses the same limit for long list values.
#[test]
fn a_config_value_size_is_checked_before_other_validation() {
    const MESSAGE: &str = "The configuration value cannot be added because it exceeds the \
                           maximum value size of 32767 bytes.";
    // A valid throttled-replicas list: 8000 entries of `0:1` and 7999 commas.
    let fits = vec!["0:1"; 8_000].join(",");
    let too_long = vec!["0:1"; 8_200].join(",");
    // The whole-map check answers the size before a key it would refuse: the
    // key here is unknown, and the other value is not an integer, yet the size
    // wins.
    let mut overrides = BTreeMap::new();
    overrides.insert(UNKNOWN_KEY.to_owned(), "a".repeat(32_768));
    overrides.insert(RETENTION_MS.to_owned(), "abc".to_owned());
    check!(
        canonical_topic_config_map(
            &overrides,
            &TopicDefaults::default(),
            false,
            UnstableApiVersions::Disabled,
        ) == Err(MESSAGE.to_owned())
    );
    // A list that fits validates as its own format says, and one that does not
    // fit is refused for its size on the per-key path too.
    let key = LEADER_THROTTLED_REPLICAS_KEY;
    check!(validate_topic_config(key, &fits) == Ok(()));
    check!(validate_topic_config(key, &too_long) == Err(MESSAGE.to_owned()));
}
