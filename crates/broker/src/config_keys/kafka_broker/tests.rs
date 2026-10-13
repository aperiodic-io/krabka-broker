use {super::*, crate::config_keys::registry::ConfigType, assert2::check};

fn dynamic_names(dynamic: Dynamic) -> Vec<&'static str> {
    KAFKA_BROKER_CONFIGS
        .iter()
        .filter(|row| row.dynamic == dynamic)
        .map(|row| row.name)
        .collect()
}

/// `lookup` is a binary search, so the table has to stay sorted, and a name
/// may appear once.
#[test]
fn the_roster_is_sorted_by_name_without_repeats() {
    let names: Vec<&str> = KAFKA_BROKER_CONFIGS.iter().map(|row| row.name).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    check!(names == sorted);
}

/// The shape of Kafka 4.3.1's `AbstractKafkaConfig.CONFIG_DEF`: 360 keys, 20
/// of them internal, and 99 in `DynamicBrokerConfig.ALL_DYNAMIC_CONFIGS` (the
/// other three of its 102 are the `QuotaConfig` keys, which `CONFIG_DEF` does
/// not hold).
#[test]
fn the_roster_has_the_shape_of_kafka_4_3_1s_config_def() {
    check!(KAFKA_BROKER_CONFIGS.len() == 360);
    check!(
        KAFKA_BROKER_CONFIGS
            .iter()
            .filter(|row| row.internal)
            .count()
            == 20
    );
    check!(dynamic_names(Dynamic::Cluster).len() + dynamic_names(Dynamic::PerBroker).len() == 99);
}

/// `DynamicBrokerConfig.PER_BROKER_CONFIGS`: the SSL keys, `DynamicListenerConfig`
/// and `cordoned.log.dirs`, less the three cluster-level listener keys.
#[test]
fn the_per_broker_keys_are_kafkas() {
    let mut want = vec![
        "cordoned.log.dirs",
        "listener.security.protocol.map",
        "listeners",
        "principal.builder.class",
        "sasl.enabled.mechanisms",
        "sasl.jaas.config",
        "sasl.kerberos.kinit.cmd",
        "sasl.kerberos.min.time.before.relogin",
        "sasl.kerberos.principal.to.local.rules",
        "sasl.kerberos.service.name",
        "sasl.kerberos.ticket.renew.jitter",
        "sasl.kerberos.ticket.renew.window.factor",
        "sasl.login.refresh.buffer.seconds",
        "sasl.login.refresh.min.period.seconds",
        "sasl.login.refresh.window.factor",
        "sasl.login.refresh.window.jitter",
        "sasl.mechanism.inter.broker.protocol",
        "ssl.cipher.suites",
        "ssl.client.auth",
        "ssl.enabled.protocols",
        "ssl.endpoint.identification.algorithm",
        "ssl.engine.factory.class",
        "ssl.key.password",
        "ssl.keymanager.algorithm",
        "ssl.keystore.certificate.chain",
        "ssl.keystore.key",
        "ssl.keystore.location",
        "ssl.keystore.password",
        "ssl.keystore.type",
        "ssl.protocol",
        "ssl.provider",
        "ssl.secure.random.implementation",
        "ssl.trustmanager.algorithm",
        "ssl.truststore.certificates",
        "ssl.truststore.location",
        "ssl.truststore.password",
        "ssl.truststore.type",
    ];
    want.sort_unstable();
    check!(dynamic_names(Dynamic::PerBroker) == want);
}

/// The keys the issue names, which a Kafka broker refuses to alter, are in
/// the roster and not dynamic.
#[test]
fn keys_kafka_refuses_to_alter_are_read_only() {
    for name in [
        "auto.leader.rebalance.enable",
        "queued.max.requests",
        "log.cleaner.enable",
        "authorizer.class.name",
        "offsets.topic.replication.factor",
        "log.dirs",
        "node.id",
        "unstable.api.versions.enable",
    ] {
        check!(
            lookup(name).map(|row| row.dynamic) == Some(Dynamic::ReadOnly),
            "{name}"
        );
    }
}

/// `DynamicConfig.Broker.validate` parses each value against the key's
/// `ConfigDef` type and runs its validator. Each row is a key, a value and
/// the value Kafka reports, or its `ConfigException` text.
#[test]
fn dynamic_keys_are_parsed_and_range_checked_the_way_config_def_does() {
    struct Case {
        key: &'static str,
        value: &'static str,
        want: Result<&'static str, &'static str>,
    }
    let cases: [Case; 22] = [
        Case {
            key: "num.io.threads",
            value: " 16 ",
            want: Ok("16"),
        },
        Case {
            key: "num.io.threads",
            value: "abc",
            want: Err(
                "Invalid value abc for configuration num.io.threads: Not a number of type INT",
            ),
        },
        Case {
            key: "num.io.threads",
            value: "0",
            want: Err("Invalid value 0 for configuration num.io.threads: Value must be at least 1"),
        },
        Case {
            key: "num.io.threads",
            value: "2147483648",
            want: Err(
                "Invalid value 2147483648 for configuration num.io.threads: Not a number of type \
                 INT",
            ),
        },
        Case {
            key: "max.connections",
            value: "-5",
            want: Err(
                "Invalid value -5 for configuration max.connections: Value must be at least 0",
            ),
        },
        // `log.cleaner.threads` is `atLeast(0)`: zero is a value Kafka takes.
        Case {
            key: "log.cleaner.threads",
            value: "0",
            want: Ok("0"),
        },
        Case {
            key: "log.cleaner.threads",
            value: "-1",
            want: Err(
                "Invalid value -1 for configuration log.cleaner.threads: Value must be at least 0",
            ),
        },
        Case {
            key: "log.cleaner.io.buffer.load.factor",
            value: "0.75",
            want: Ok("0.75"),
        },
        Case {
            key: "log.cleaner.io.buffer.load.factor",
            value: "x",
            want: Err(
                "Invalid value x for configuration log.cleaner.io.buffer.load.factor: Not a \
                 number of type DOUBLE",
            ),
        },
        Case {
            key: "sasl.login.refresh.buffer.seconds",
            value: "300",
            want: Ok("300"),
        },
        Case {
            key: "sasl.login.refresh.buffer.seconds",
            value: "40000",
            want: Err(
                "Invalid value 40000 for configuration sasl.login.refresh.buffer.seconds: Not a \
                 number of type SHORT",
            ),
        },
        Case {
            key: "follower.fetch.last.tiered.offset.enable",
            value: "TRUE",
            want: Ok("true"),
        },
        Case {
            key: "follower.fetch.last.tiered.offset.enable",
            value: "yes",
            want: Err(
                "Invalid value yes for configuration follower.fetch.last.tiered.offset.enable: \
                 Expected value to be either true or false",
            ),
        },
        Case {
            key: "metric.reporters",
            value: "a, b",
            want: Ok("a,b"),
        },
        Case {
            key: "metric.reporters",
            value: "",
            want: Ok(""),
        },
        Case {
            key: "metric.reporters",
            value: "a,a",
            want: Err("Configuration 'metric.reporters' values must not be duplicated."),
        },
        Case {
            key: "listeners",
            value: " ",
            want: Err(
                "Configuration 'listeners' must not be empty. Valid values include: any \
                 non-empty value",
            ),
        },
        Case {
            key: "ssl.client.auth",
            value: "maybe",
            want: Err(
                "Invalid value maybe for configuration ssl.client.auth: String must be one of: \
                 required, requested, none",
            ),
        },
        Case {
            key: "ssl.client.auth",
            value: " required ",
            want: Ok("required"),
        },
        // A password is trimmed, and a class name is not loaded.
        Case {
            key: "ssl.keystore.password",
            value: " secret ",
            want: Ok("secret"),
        },
        Case {
            key: "principal.builder.class",
            value: "no.such.Class",
            want: Ok("no.such.Class"),
        },
        Case {
            key: "group.coordinator.cached.buffer.max.bytes",
            value: "1",
            want: Err(
                "Invalid value 1 for configuration group.coordinator.cached.buffer.max.bytes: \
                 Value must be at least 524288",
            ),
        },
    ];
    for Case {
        key: name,
        value,
        want,
    } in cases
    {
        let row = lookup(name).expect("a KafkaConfig key");
        check!(
            row.canonical(value) == want.map(str::to_owned).map_err(str::to_owned),
            "{name}={value:?}"
        );
    }
}

/// `KafkaConfig.configType` types a `listener.name.<listener>.` override as
/// the key it overrides, and a mechanism override as the key it ends in.
#[test]
fn a_listener_override_resolves_to_the_key_it_overrides() {
    let cases = [
        ("num.io.threads", Some(("num.io.threads", ConfigType::Int))),
        (
            "listener.name.external.ssl.keystore.location",
            Some(("ssl.keystore.location", ConfigType::String)),
        ),
        (
            "listener.name.external.ssl.keystore.password",
            Some(("ssl.keystore.password", ConfigType::Password)),
        ),
        (
            "listener.name.external.plain.sasl.jaas.config",
            Some(("sasl.jaas.config", ConfigType::Password)),
        ),
        ("listener.name.external.no.such.key", None),
        ("no.such.key", None),
    ];
    for (name, want) in cases {
        check!(
            resolve(name).map(|row| (row.name, row.config_type)) == want,
            "{name}"
        );
    }
}

/// A password key is sensitive, and a key the roster row types is read-only
/// exactly when it is not dynamic.
#[test]
fn the_row_of_a_key_states_its_sensitivity_and_mode() {
    let sensitive = lookup("ssl.keystore.password").expect("a key").row();
    check!(sensitive.sensitive);
    check!(!sensitive.read_only);
    check!(sensitive.config_type.wire() == 9);

    let read_only = lookup("log.dirs").expect("a key").row();
    check!(!read_only.sensitive);
    check!(read_only.read_only);
    check!(read_only.config_type.wire() == 7);
}
