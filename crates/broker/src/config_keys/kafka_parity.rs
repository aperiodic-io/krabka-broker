//! Parity between krabka's topic-config surface and Apache Kafka 4.3.1's.
//!
//! `MirrorMaker` 2 reads a source topic's config set with `DescribeConfigs` and
//! replays it onto the target in **one** `IncrementalAlterConfigs` call. Kafka
//! refuses an unknown topic config, and so does krabka, so a single key krabka
//! does not carry fails the whole set: `retention.ms` and everything beside it
//! never propagate, and the operator sees one WARN in a JVM log. That is how
//! `compression.lz4.level` was found -- one CI run at a time -- and this module
//! exists so the next gap fails here instead.
//!
//! # Where the roster comes from
//!
//! [`KAFKA_TOPIC_CONFIGS`] is `LogConfig.configKeys()` of the released Apache
//! Kafka 4.3.1 binary distribution, which is the artifact set the
//! `mirror.gcr.io/apache/kafka:4.3.1` image ships. It was captured by running
//! that jar, not by reading a wiki page and not from memory:
//!
//! ```text
//! curl -O https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
//! # sha512 matches the published .sha512:
//! # c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e
//! # 8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c
//! tar xzf kafka_2.13-4.3.1.tgz
//! # then, against kafka_2.13-4.3.1/libs/*:
//! #   LogConfig.configKeys()  -> name, ConfigDef.Type, default, validator
//! #   LogConfig.validate(map) -> what an alter accepts or refuses
//! ```
//!
//! `LogConfig`'s `ConfigDef`, not `TopicConfig`'s constants, is the authority:
//! `TopicConfig` still declares `message.downconversion.enable`, and
//! `LogConfig` no longer defines it, so `LogConfig.validate` answers `Unknown
//! topic config name: message.downconversion.enable`. A roster built from
//! `TopicConfig` would have carried a key Kafka 4.3.1 rejects.
//!
//! The same run pinned the refusals in [`kafka_refuses_what_krabka_refuses`]
//! and confirmed that Kafka rejects an unrecognised key rather than ignoring
//! it, which is why krabka stays strict here and closes the gap by carrying
//! Kafka's keys rather than by becoming permissive.
//!
//! # The other direction
//!
//! [`KRABKA_TOPIC_CONFIGS`] is every topic key krabka has that Kafka 4.3.1 does
//! not. A key added to the registry without a row here fails
//! [`the_topic_key_sets_differ_only_by_the_rosters_below`], which is the point:
//! a krabka-private key is a deliberate divergence and has to be named as one.
//!
//! # Kafka trunk
//!
//! [`KAFKA_TRUNK_TOPIC_CONFIGS`] is the four topic keys Kafka trunk's
//! `LogConfig` defines beyond 4.3.1. A broker without
//! `unstable.api.versions.enable` serves Kafka 4.3.1's topic resource, so it
//! refuses each of them as 4.3.1 does (`Unknown topic config name`) and does
//! not describe them. A `MirrorMaker` 2 replay from a trunk cluster that sets
//! one of them therefore fails against krabka exactly as it fails against a
//! stock 4.3.1 broker, until the target opts in to trunk behaviour.

use {
    super::{
        dedup::{DEDUP_KEY_HEADER, DEDUP_WINDOW_MS},
        delivery::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE, DELIVERY_SCHEDULE_MONOTONIC},
        diskless::DISKLESS,
        docs::topic_config_docs,
        kafka_broker::roster,
        recovery::UNCLEAN_RECOVERY_STRATEGY,
        registry::{self, ConfigScope, ConfigType},
        schema::{SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_VALUE},
        topic_scope::WRITE_FREEZE,
        validation::{
            TopicDefaults, canonical_topic_config, canonical_topic_config_map,
            validate_topic_config, validate_topic_config_map,
        },
    },
    crate::api_catalog::UnstableApiVersions,
    assert2::{assert, check},
    std::collections::BTreeMap,
};

/// One row of Kafka 4.3.1's `LogConfig.configKeys()`.
struct KafkaTopicConfig {
    name: &'static str,
    /// `ConfigDef.Type`, as the JVM `AdminClient` parses the value with.
    config_type: ConfigType,
    /// The `ConfigKey.defaultValue`, spelled the way `DescribeConfigs` renders
    /// it: an empty LIST is `""`, and `None` is Kafka's `null`.
    default: Option<&'static str>,
    /// A value Kafka's own validator for the key accepts, used to drive the
    /// alter path. Chosen so the whole set is also a legal combination.
    sample: &'static str,
}

const fn kafka(
    name: &'static str,
    config_type: ConfigType,
    default: Option<&'static str>,
    sample: &'static str,
) -> KafkaTopicConfig {
    KafkaTopicConfig {
        name,
        config_type,
        default,
        sample,
    }
}

/// Every topic config Apache Kafka 4.3.1 defines, in the order
/// `LogConfig.configKeys()` sorts them.
///
/// 34 rows: the 33 `LogConfig.nonInternalConfigNames()` plus
/// `internal.segment.bytes`, which `ConfigDef` marks internal. Internal only
/// hides a key from `kafka-configs --help`; `LogConfig.validate` accepts it,
/// so an alter carrying it succeeds against a real broker and must here too.
const KAFKA_TOPIC_CONFIGS: &[KafkaTopicConfig] = roster! {
    kafka("cleanup.policy", ConfigType::List, Some("delete"), "delete"),
    kafka("compression.gzip.level", ConfigType::Int, Some("-1"), "6"),
    kafka("compression.lz4.level", ConfigType::Int, Some("9"), "9"),
    kafka("compression.type", ConfigType::String, Some("producer"), "producer"),
    kafka("compression.zstd.level", ConfigType::Int, Some("3"), "3"),
    kafka("delete.retention.ms", ConfigType::Long, Some("86400000"), "86400000"),
    kafka("file.delete.delay.ms", ConfigType::Long, Some("60000"), "60000"),
    kafka("flush.messages", ConfigType::Long, Some("9223372036854775807"), "10000"),
    kafka("flush.ms", ConfigType::Long, Some("9223372036854775807"), "1000"),
    kafka("follower.replication.throttled.replicas", ConfigType::List, Some(""), "0:1"),
    kafka("index.interval.bytes", ConfigType::Int, Some("4096"), "4096"),
    kafka("internal.segment.bytes", ConfigType::Int, None, "1048576"),
    kafka("leader.replication.throttled.replicas", ConfigType::List, Some(""), "0:1"),
    kafka("local.retention.bytes", ConfigType::Long, Some("-2"), "-2"),
    kafka("local.retention.ms", ConfigType::Long, Some("-2"), "-2"),
    kafka("max.compaction.lag.ms", ConfigType::Long, Some("9223372036854775807"), "86400000"),
    kafka("max.message.bytes", ConfigType::Int, Some("1048588"), "1048588"),
    kafka("message.timestamp.after.max.ms", ConfigType::Long, Some("3600000"), "3600000"),
    kafka("message.timestamp.before.max.ms", ConfigType::Long, Some("9223372036854775807"), "3600000"),
    kafka("message.timestamp.type", ConfigType::String, Some("CreateTime"), "CreateTime"),
    kafka("min.cleanable.dirty.ratio", ConfigType::Double, Some("0.5"), "0.5"),
    kafka("min.compaction.lag.ms", ConfigType::Long, Some("0"), "0"),
    kafka("min.insync.replicas", ConfigType::Int, Some("1"), "1"),
    kafka("preallocate", ConfigType::Boolean, Some("false"), "false"),
    kafka("remote.log.copy.disable", ConfigType::Boolean, Some("false"), "false"),
    kafka("remote.log.delete.on.disable", ConfigType::Boolean, Some("false"), "false"),
    kafka("remote.storage.enable", ConfigType::Boolean, Some("false"), "false"),
    kafka("retention.bytes", ConfigType::Long, Some("-1"), "-1"),
    kafka("retention.ms", ConfigType::Long, Some("604800000"), "604800000"),
    kafka("segment.bytes", ConfigType::Int, Some("1073741824"), "1073741824"),
    kafka("segment.index.bytes", ConfigType::Int, Some("10485760"), "10485760"),
    kafka("segment.jitter.ms", ConfigType::Long, Some("0"), "0"),
    kafka("segment.ms", ConfigType::Long, Some("604800000"), "604800000"),
    kafka("unclean.leader.election.enable", ConfigType::Boolean, Some("false"), "false"),
};

/// The topic configs Apache Kafka trunk defines beyond 4.3.1, from
/// `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java`
/// at commit `4338e13ea3864a51d9ead14db23a3e0b76ca9fb7`. They arrive with the
/// next Kafka release. krabka serves them only under
/// `unstable.api.versions.enable`; without it, as on Kafka 4.3.1, each is an
/// unknown topic config.
const KAFKA_TRUNK_TOPIC_CONFIGS: &[KafkaTopicConfig] = roster! {
    kafka("errors.deadletterqueue.group.enable", ConfigType::Boolean, Some("false"), "false"),
    kafka("max.decompressed.message.bytes", ConfigType::Int, Some("2147483639"), "2147483639"),
    kafka("remote.copy.lag.bytes", ConfigType::Long, Some("-1"), "-1"),
    kafka("remote.copy.lag.ms", ConfigType::Long, Some("0"), "0"),
};

/// Every Kafka topic config, released and trunk.
fn kafka_rosters() -> impl Iterator<Item = &'static KafkaTopicConfig> {
    KAFKA_TOPIC_CONFIGS
        .iter()
        .chain(KAFKA_TRUNK_TOPIC_CONFIGS.iter())
}

/// The topic keys krabka has and Apache Kafka 4.3.1 does not.
///
/// Every one is a deliberate krabka extension. `unclean.recovery.strategy` is
/// KIP-966, which Kafka has accepted but not shipped as a topic config in
/// 4.3.1; the rest are krabka's own.
const KRABKA_TOPIC_CONFIGS: &[&str] = &[
    UNCLEAN_RECOVERY_STRATEGY,
    DISKLESS,
    DELIVERY_MODE,
    DELIVERY_MAX_DELAY_MS,
    DELIVERY_SCHEDULE_MONOTONIC,
    SCHEMA_VALIDATION_KEY,
    SCHEMA_VALIDATION_VALUE,
    SCHEMA_VALIDATION_MODE,
    DEDUP_WINDOW_MS,
    DEDUP_KEY_HEADER,
    WRITE_FREEZE,
];

/// Kafka keys whose default krabka reports differently, each with the reason.
///
/// This is not a place to park a fresh divergence. A row here is a behaviour
/// krabka has not implemented, stated so that
/// [`kafka_topic_key_types_and_defaults_match`] still pins every other key.
/// The table is empty, and
/// [`every_recorded_divergence_is_still_a_divergence`] is what keeps a row
/// from outliving the gap it names.
const DEFAULT_DIVERGENCES: &[Divergence] = &[];

/// A Kafka key and the default krabka reports for it.
struct Divergence {
    name: &'static str,
    krabka_default: &'static str,
}

fn krabka_topic_keys() -> Vec<&'static str> {
    registry::keys_in(ConfigScope::Topic)
        .map(|row| row.name)
        .collect()
}

/// The regression test for the `MirrorMaker` 2 defect: every key Kafka 4.3.1
/// carries has to survive krabka's alter validator with a value Kafka's own
/// validator accepts.
///
/// Before `compression.gzip.level`, `compression.lz4.level`,
/// `compression.zstd.level` and `internal.segment.bytes` were added, this
/// failed with an unknown-key refusal on each of them -- the same refusal
/// `MirrorMaker` surfaced as `InvalidConfigurationException: unrecognized config
/// key`.
#[test]
fn every_kafka_4_3_1_topic_config_is_accepted_by_the_alter_path() {
    for row in KAFKA_TOPIC_CONFIGS {
        check!(
            validate_topic_config(row.name, row.sample) == Ok(()),
            "{}={}",
            row.name,
            row.sample
        );
    }
}

/// Kafka trunk's topic keys follow `unstable.api.versions.enable`: refused
/// with Kafka 4.3.1's `LogConfig.validateNames` text without it, accepted with
/// it.
#[test]
fn kafka_trunk_topic_configs_are_accepted_only_under_unstable_api_versions() {
    for row in KAFKA_TRUNK_TOPIC_CONFIGS {
        check!(
            canonical_topic_config(row.name, row.sample, UnstableApiVersions::Disabled)
                == Err(format!("Unknown topic config name: {}", row.name)),
            "{}",
            row.name
        );
        check!(
            canonical_topic_config(row.name, row.sample, UnstableApiVersions::Enabled)
                == Ok(row.sample.to_owned()),
            "{}",
            row.name
        );
    }
}

/// `MirrorMaker` sends the source topic's whole config set as one
/// `IncrementalAlterConfigs`, so one unknown key loses every other key with
/// it. The whole-map validator is the surface that call lands on: a 4.3.1
/// set replays onto a default broker, and a trunk set only onto one with
/// `unstable.api.versions.enable`.
#[test]
fn a_whole_kafka_config_set_replays_in_one_alter() {
    let released: BTreeMap<String, String> = KAFKA_TOPIC_CONFIGS
        .iter()
        .map(|row| (row.name.to_owned(), row.sample.to_owned()))
        .collect();
    assert!(validate_topic_config_map(&released) == Ok(()));

    let trunk: BTreeMap<String, String> = kafka_rosters()
        .map(|row| (row.name.to_owned(), row.sample.to_owned()))
        .collect();
    assert!(
        canonical_topic_config_map(
            &trunk,
            &TopicDefaults::default(),
            true,
            UnstableApiVersions::Enabled
        )
        .map(drop)
            == Ok(())
    );
    assert!(validate_topic_config_map(&trunk).is_err());
}

/// Both directions of the diff at once. A Kafka key krabka lacks fails on the
/// left; a krabka key nobody classified fails on the right.
#[test]
fn the_topic_key_sets_differ_only_by_the_rosters_below() {
    let krabka: Vec<&str> = krabka_topic_keys();
    let kafka: Vec<&str> = kafka_rosters().map(|row| row.name).collect();

    let missing: Vec<&str> = kafka
        .iter()
        .copied()
        .filter(|name| !krabka.contains(name))
        .collect();
    let extra: Vec<&str> = krabka
        .iter()
        .copied()
        .filter(|name| !kafka.contains(name))
        .collect();

    check!(missing == Vec::<&str>::new(), "Kafka keys krabka rejects");
    check!(
        extra == KRABKA_TOPIC_CONFIGS.to_vec(),
        "krabka keys Kafka 4.3.1 has no row for"
    );
}

/// The type byte and default a Kafka key reports have to be Kafka's own: the
/// JVM `AdminClient` parses `ConfigEntry.value()` with the type the broker
/// hands back, and `kafka-configs --describe --all` prints the default.
#[test]
fn kafka_topic_key_types_and_defaults_match() {
    for row in kafka_rosters() {
        let Some(krabka_row) = registry::lookup(ConfigScope::Topic, row.name) else {
            // `the_topic_key_sets_differ_only_by_the_rosters_below` reports a
            // missing key; nothing to compare here.
            continue;
        };
        let expected_default = DEFAULT_DIVERGENCES
            .iter()
            .find(|divergence| divergence.name == row.name)
            .map_or(row.default, |divergence| Some(divergence.krabka_default));

        check!(
            (krabka_row.config_type, krabka_row.default) == (row.config_type, expected_default),
            "{}",
            row.name
        );
    }
}

/// A divergence stops being one once it is closed, and the row here has to go
/// with it. Without this, a fixed default would leave a row claiming a
/// difference that no longer exists.
#[test]
fn every_recorded_divergence_is_still_a_divergence() {
    for Divergence {
        name,
        krabka_default,
    } in DEFAULT_DIVERGENCES
    {
        let kafka_default = kafka_rosters()
            .find(|row| row.name == *name)
            .unwrap_or_else(|| panic!("{name} is not a Kafka 4.3.1 topic config"))
            .default;
        check!(kafka_default != Some(*krabka_default), "{name}");
    }
}

/// Every Kafka key is on the generated reference page and in
/// `DescribeConfigs`' typed metadata, which is the same projection of the
/// registry. A key the validator accepts but the page never mentions would
/// leave an operator reading `kafka-configs --describe` one story and the
/// alter refusal another.
#[test]
fn every_kafka_topic_key_is_documented() {
    let documented: Vec<&str> = topic_config_docs().iter().map(|doc| doc.key).collect();

    for row in kafka_rosters() {
        check!(documented.contains(&row.name), "{}", row.name);
    }
}

/// Matching Kafka's key set is not the same as accepting anything. Each value
/// below was refused by `LogConfig.validate` on the 4.3.1 jar, with the
/// message quoted beside it, and krabka refuses it too.
#[test]
fn kafka_refuses_what_krabka_refuses() {
    for (name, value) in [
        // `Unknown topic config name: message.downconversion.enable` --
        // `TopicConfig` still declares the constant, `LogConfig` does not
        // define the key, and 4.0 made down-conversion impossible.
        ("message.downconversion.enable", "true"),
        // `Unknown topic config name: totally.made.up`.
        ("totally.made.up", "1"),
        // `Invalid value 0 for configuration compression.gzip.level: Value
        // must be between 1 and 9 or equal to -1`.
        ("compression.gzip.level", "0"),
        // `Invalid value 18 for configuration compression.lz4.level: Value
        // must be no more than 17`.
        ("compression.lz4.level", "18"),
        ("compression.lz4.level", "0"),
        // `Invalid value -131073 for configuration compression.zstd.level:
        // Value must be at least -131072`.
        ("compression.zstd.level", "-131073"),
        ("compression.zstd.level", "23"),
        // `Invalid value -1 for configuration max.message.bytes: Value must
        // be at least 0`.
        ("max.message.bytes", "-1"),
        // `Invalid value 1048575 for configuration segment.bytes: Value must
        // be at least 1048576`. `LogConfig` validates the key with
        // `atLeast(1024 * 1024)`; `internal.segment.bytes` is the key that
        // reaches below the floor, and `defineInternal` gives it no validator.
        ("segment.bytes", "1048575"),
        ("segment.bytes", "0"),
        // `Invalid value none for configuration compression.type: String must
        // be one of: uncompressed, zstd, lz4, snappy, gzip, producer`.
        // `LogConfig` validates the key with
        // `ValidString.in(BrokerCompressionType.names())`, and `none` is a
        // producer-side codec name that enum does not carry.
        ("compression.type", "none"),
    ] {
        check!(
            validate_topic_config(name, value).is_err(),
            "{name}={value} must be refused"
        );
    }
}

/// Not every Kafka key carries a validator. `LogConfig` declares
/// `retention.bytes` as a bare `LONG` with a default and nothing else, so it
/// accepts every value the type holds -- `-2` included, which `retention.ms`
/// next to it refuses. A `MirrorMaker` replay of a source topic carrying one
/// has to survive here, so krabka may not invent a floor Kafka does not have.
#[test]
fn a_key_kafka_gives_no_validator_accepts_every_value_of_its_type() {
    for value in [
        "-9223372036854775808",
        "-2",
        "-1",
        "0",
        "9223372036854775807",
    ] {
        check!(
            validate_topic_config("retention.bytes", value) == Ok(()),
            "retention.bytes={value}"
        );
    }
    check!(
        validate_topic_config("retention.bytes", "9223372036854775808").is_err(),
        "a value Kafka's LONG cannot hold is still refused"
    );
}

/// The levels Kafka's own validators accept at each end of their range.
#[test]
fn codec_levels_accept_the_bounds_kafka_accepts() {
    for (name, value) in [
        ("compression.gzip.level", "-1"),
        ("compression.gzip.level", "1"),
        ("compression.gzip.level", "9"),
        ("compression.lz4.level", "1"),
        ("compression.lz4.level", "17"),
        ("compression.zstd.level", "-131072"),
        ("compression.zstd.level", "22"),
    ] {
        check!(
            validate_topic_config(name, value) == Ok(()),
            "{name}={value}"
        );
    }
}
