//! The `kafka-configs` tool: an `--alter --add-config` and `--describe`
//! round-trip over a topic configuration.

use {
    crate::{
        jvm_acceptance::{
            alter_console_topic_config, broker0_advertised, docker_run_kafka_tool,
            docker_run_kafka_tool_allowing_failure, nc_check_connectivity, start_console_broker,
            start_host_broker, tool_output,
        },
        oracle::OracleComparison,
    },
    assert2::assert,
};

/// `kafka-configs --alter --add-config retention.ms=60000 --topic t` then
/// `--describe` round-trips through `V1TopicConfig` and the supervisor
/// reconcile push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trip() {
    const TOPIC: &str = "krabka-cfg-alter-itest";

    let (_broker, _dir) = start_console_broker(TOPIC, 1).await;

    alter_console_topic_config(TOPIC, "retention.ms=60000");

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("retention.ms=60000"),
        "describe output missing retention.ms=60000: {s}"
    );
}

/// `kafka-configs --describe --all` renders the effective configuration and
/// the provenance of every value in it (KIP-226).
///
/// This is the surface an operator uses to tell a dynamic override from an
/// inherited default, and it only works if the broker types each key and
/// returns its synonym chain. The JVM tool prints
/// `name=value sensitive=… synonyms={SOURCE:name=value, …}`, so the three
/// assertions below are the three cases the chain has: a topic override, a
/// value inherited from the cluster-wide default, and a key nobody has set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_describe_all_shows_effective_values_and_their_sources() {
    const TOPIC: &str = "krabka-cfg-describe-all-itest";

    let (_broker, _dir) = start_console_broker(TOPIC, 1).await;
    alter_console_topic_config(TOPIC, "retention.ms=60000");
    // The cluster-wide default the topic will inherit.
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-default",
        "--add-config",
        "unclean.leader.election.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--all",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let rendered = String::from_utf8_lossy(&out.stdout);

    for (label, expected) in [
        (
            "the topic override sits at the head of its broker synonym chain",
            "retention.ms=60000 sensitive=false \
             synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=60000, \
             DEFAULT_CONFIG:log.retention.hours=168}",
        ),
        (
            "an inherited value names the cluster default above the built-in one",
            "unclean.leader.election.enable=true sensitive=false \
             synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:unclean.leader.election.enable=true, \
             DEFAULT_CONFIG:unclean.leader.election.enable=false}",
        ),
        (
            "a key nobody set reports its effective value and the broker default behind it",
            "cleanup.policy=delete sensitive=false \
             synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}",
        ),
    ] {
        assert!(
            rendered.contains(expected),
            "{label}: expected {expected:?} in {rendered}"
        );
    }
}

/// `kafka-configs --alter --add-config` over every Kafka `TopicConfig` key
/// krabka registers, then `--describe` back.
///
/// The JVM tool parses each value with the type the broker reports for the
/// key, so a key krabka typed differently from Kafka fails here rather than
/// in a client months later. One `--alter` carries the whole set, as an
/// operator's own command would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trips_every_registered_topic_key() {
    const TOPIC: &str = "krabka-cfg-every-key-itest";
    /// Each key with a value Kafka's own `ConfigDef` accepts for it.
    const SETTINGS: &[(&str, &str)] = &[
        ("segment.ms", "60000"),
        ("segment.index.bytes", "10485760"),
        ("segment.jitter.ms", "0"),
        ("min.compaction.lag.ms", "0"),
        ("max.compaction.lag.ms", "86400000"),
        ("min.cleanable.dirty.ratio", "0.25"),
        ("file.delete.delay.ms", "60000"),
        ("flush.messages", "10000"),
        ("flush.ms", "1000"),
        ("index.interval.bytes", "8192"),
        ("preallocate", "false"),
        ("message.timestamp.type", "CreateTime"),
        ("message.timestamp.after.max.ms", "3600000"),
        ("message.timestamp.before.max.ms", "3600000"),
    ];

    let (_broker, _dir) = start_console_broker(TOPIC, 1).await;

    let added = SETTINGS
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        &added,
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let rendered = String::from_utf8_lossy(&out.stdout);
    for (key, value) in SETTINGS {
        assert!(
            rendered.contains(&format!("{key}={value}")),
            "describe output missing {key}={value}: {rendered}"
        );
    }
}

/// `kafka-topics --create` with the override set Kafka Streams'
/// `RepartitionTopicConfig` sends on every internal repartition topic, and
/// the `WindowedChangelogTopicConfig` set whose `cleanup.policy` is exactly
/// `compact,delete`. A broker that refuses either cannot host a Streams
/// application.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_create_accepts_the_streams_internal_topic_configs() {
    /// (topic, the `--config` arguments Streams sends, the `--describe`
    /// substring that proves the broker stored them).
    const CASES: &[(&str, &[&str], &str)] = &[
        (
            "krabka-streams-repartition-itest",
            &[
                "cleanup.policy=delete",
                "segment.bytes=52428800",
                "retention.ms=-1",
                "message.timestamp.type=CreateTime",
            ],
            "cleanup.policy=delete",
        ),
        (
            "krabka-streams-windowed-changelog-itest",
            &[
                "cleanup.policy=compact,delete",
                "retention.ms=86400000",
                "min.compaction.lag.ms=0",
                "message.timestamp.type=CreateTime",
            ],
            "cleanup.policy=compact,delete",
        ),
    ];

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    for (topic, configs, expected) in CASES {
        let mut create = vec![
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
        ];
        for config in *configs {
            create.push("--config");
            create.push(config);
        }
        docker_run_kafka_tool(&create);

        let out = docker_run_kafka_tool(&[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
        ]);
        let rendered = String::from_utf8_lossy(&out.stdout);
        assert!(
            rendered.contains(expected),
            "{topic}: describe output missing {expected}: {rendered}"
        );
    }
}

/// Kafka's `LogConfig.validate` refuses tiered storage on a compacted topic,
/// and the JVM tool surfaces that as a `ConfigException` on the alter. The
/// refusal covers `compact,delete` as well, because Kafka tests the policy
/// list for membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_refuses_tiered_storage_on_a_compacted_topic() {
    const TOPIC: &str = "krabka-cfg-compact-tiered-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--config",
        "cleanup.policy=compact,delete",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // The alter is the case, so the tool must be allowed to fail: it exits
    // non-zero and prints the broker's `InvalidConfigurationException`, which
    // is exactly the evidence this test wants.
    let out = docker_run_kafka_tool_allowing_failure(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "remote.storage.enable=true",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let rendered = tool_output(&out);
    assert!(
        !out.status.success(),
        "the alter must be refused, not applied: {rendered}"
    );
    assert!(
        rendered.contains(
            "Remote log storage only supports topics with cleanup.policy=delete or \
             cleanup.policy being an empty list."
        ) || rendered.contains("Tiered Storage functionality is disabled in the broker."),
        "alter should be refused with Kafka's message: {rendered}"
    );
}

// ── the entity types `kafka-configs` reaches through the other two APIs ─────
//
// `topics` and `brokers`, above, are `IncrementalAlterConfigs` and
// `DescribeConfigs`. The four entity types below are not: `groups` and
// `client-metrics` are the same pair over two more resource types, and
// `clients` and the `users`+`clients` tuple are `AlterClientQuotas` and
// `DescribeClientQuotas` instead. `ConfigCommand` chooses which pair to send
// from the `--entity-type` alone, so an entity type that reaches the wrong
// handler -- or no handler -- is invisible to any test that only drives
// `topics`.
//
// Each case runs the same `kafka-configs` binary, from the same
// `apache/kafka:4.3.1` image, against krabka and against a stock broker of that
// release, and compares the parsed answer.

use crate::config_output::{Configs, parse_describe, quota_entities};

/// One entity type, addressed and altered the way an operator addresses it.
///
/// `entity` is spliced into both the `--alter` and the `--describe`, so the
/// tuple case is expressible: it is two `--entity-type`/`--entity-name` pairs
/// in one entity, and nothing but the argument list says so.
struct EntityCase {
    /// What a failure names.
    label: &'static str,
    /// The `--entity-type`/`--entity-name` arguments that address it.
    entity: &'static [&'static str],
    /// The `--add-config` argument.
    add_config: &'static str,
    /// The keys the describe must report back, whatever values it gives them.
    ///
    /// The values are not written down here: the quota path renders every
    /// number as a Java `double`, so `1024` comes back as `1024.0`, and a
    /// suite that pinned one spelling of that would be asserting Java's
    /// `Double.toString` rather than anything about a broker. The oracle
    /// settles the values instead.
    keys: &'static [&'static str],
    /// What the quota path calls this entity, or `None` for the entity types
    /// that go through `DescribeConfigs` and print no such line.
    quota_entity: Option<&'static str>,
}

/// The four entity types this file did not previously reach.
///
/// `groups` carries `share.auto.offset.reset` rather than one of the
/// `consumer.*` group configs. That is the key both releases agree is a group
/// config; the `consumer.*` ones are Kafka's and krabka does not register
/// them, so a case built on those would be a report of that gap rather than a
/// test of the `groups` entity type, which is what this table is for.
const ENTITY_CASES: &[EntityCase] = &[
    EntityCase {
        label: "clients",
        entity: &["--entity-type", "clients", "--entity-name", "krabka-client"],
        add_config: "consumer_byte_rate=1024,producer_byte_rate=2048",
        keys: &["consumer_byte_rate", "producer_byte_rate"],
        quota_entity: Some("client-id 'krabka-client'"),
    },
    EntityCase {
        label: "groups",
        entity: &["--entity-type", "groups", "--entity-name", "krabka-cfg-grp"],
        add_config: "share.auto.offset.reset=earliest",
        keys: &["share.auto.offset.reset"],
        quota_entity: None,
    },
    EntityCase {
        label: "client-metrics",
        entity: &[
            "--entity-type",
            "client-metrics",
            "--entity-name",
            "krabka-cfg-metrics",
        ],
        add_config: "interval.ms=60000,metrics=org.apache.kafka.",
        keys: &["interval.ms", "metrics"],
        quota_entity: None,
    },
    EntityCase {
        label: "the users+clients tuple",
        entity: &[
            "--entity-type",
            "users",
            "--entity-name",
            "krabka-cfg-user",
            "--entity-type",
            "clients",
            "--entity-name",
            "krabka-client",
        ],
        add_config: "request_percentage=42",
        keys: &["request_percentage"],
        // Both components, in the order `ConfigCommand` renders a tuple. A
        // broker that dropped the client half would answer for the user alone,
        // and every value above would still match.
        quota_entity: Some("user-principal 'krabka-cfg-user', client-id 'krabka-client'"),
    },
];

/// `--alter --add-config` then `--describe` over each entity type, compared
/// with Apache Kafka.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn kafka_configs_entity_types_round_trip_as_apache_kafka_does() {
    // Kafka first: a key one release calls a group config and the other does
    // not fails here, on the stock broker, rather than being read as a krabka
    // bug.
    let comparison = OracleComparison::start("configs-entities").await;
    let [oracle_side, krabka_side] = comparison.sides();

    for case in ENTITY_CASES {
        let mut answers: Vec<Configs> = Vec::new();
        for side in [&oracle_side, &krabka_side] {
            let mut alter = vec!["--bootstrap-server", side.bootstrap(), "--alter"];
            alter.extend_from_slice(case.entity);
            alter.extend_from_slice(&["--add-config", case.add_config]);
            let altered = side.run("kafka-configs", &alter);
            assert!(
                altered.succeeded(),
                "{}: --alter over {} was refused:\n{}",
                side.label(),
                case.label,
                altered.text(),
            );

            let mut describe = vec!["--bootstrap-server", side.bootstrap(), "--describe"];
            describe.extend_from_slice(case.entity);
            let described = side.run("kafka-configs", &describe).expect_success();
            let configs = parse_describe(&described.stdout);
            for key in case.keys {
                assert!(
                    configs.contains_key(*key),
                    "{}: --describe over {} lost {key}: got {configs:?}\n{}",
                    side.label(),
                    case.label,
                    described.stdout,
                );
            }
            let entities = quota_entities(&described.stdout);
            let expected: Vec<String> = case.quota_entity.into_iter().map(str::to_owned).collect();
            assert!(
                entities == expected,
                "{}: --describe over {} named {entities:?}, expected {expected:?}\n{}",
                side.label(),
                case.label,
                described.stdout,
            );
            answers.push(configs);
        }
        assert!(
            answers[0] == answers[1],
            "{}: krabka and Apache Kafka disagreed: {answers:?}",
            case.label,
        );
    }

    comparison.broker.shutdown().await;
}
