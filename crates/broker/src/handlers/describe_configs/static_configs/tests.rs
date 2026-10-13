//! What the idle-window entries report: the value, the provenance behind it,
//! the chain a request can ask for, and the typed metadata the registry row
//! supplies.

use {
    super::{super::wire::CONFIG_SOURCE_DEFAULT, *},
    assert2::assert,
    krabka_protocol::owned::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsSynonym,
    },
    krabka_units::{Time, millis, secs},
};

/// A request that asks for values alone, the way a plain `--describe` does.
const VALUES_ONLY: EntryOptions = EntryOptions {
    include_synonyms: false,
    include_documentation: false,
};

/// A request that asks for the chain too, the way `--all` does.
const WITH_SYNONYMS: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: false,
};

/// No `configuration_keys` filter: every key the broker offers.
fn everything(_: &str) -> bool { true }

/// One case's static configs, owning the override map that the borrowed
/// [`StaticBrokerConfigs`] view points at.
struct IdleCase {
    idle: Option<Time>,
    overrides: std::collections::BTreeMap<String, Time>,
}

impl IdleCase {
    /// The node's static configs, with every key but the idle window left at
    /// Kafka's built-in default.
    fn statics(&self) -> StaticBrokerConfigs<'_> {
        StaticBrokerConfigs {
            connections_max_idle: self.idle,
            connections_max_idle_overrides: &self.overrides,
            ..super::super::resources::kafka_default_static_broker()
        }
    }
}

/// The idle window of one named listener.
struct ListenerWindow<'a> {
    name: &'a str,
    window: Time,
}

/// A broker-wide window of `idle` with the named per-listener overrides.
fn config_with(idle: Option<Time>, overrides: &[ListenerWindow<'_>]) -> IdleCase {
    IdleCase {
        idle,
        overrides: overrides
            .iter()
            .map(|listener| (listener.name.to_string(), listener.window))
            .collect(),
    }
}

/// `ConfigDef.Type::LONG`, which is what the registry row advertises.
const LONG: i8 = 5;

fn expected(name: &str, value: &str, source: i8) -> DescribeConfigsResourceResult {
    tagged_wire!(DescribeConfigsResourceResult {
        name: name.to_string(),
        value: Some(value.to_string()),
        read_only: true,
        config_source: source,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: LONG,
        documentation: None,
    })
}

fn synonym(name: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym {
        name: name.to_string(),
        value: Some(value.to_string()),
        source,
        ..Default::default()
    }
}

/// The window the process runs when nobody configures one has to be the
/// default the describe reports, or `kafka-configs` would name a value no
/// connection is held to.
#[test]
fn the_registry_default_is_the_window_the_broker_runs() {
    let row = registry::lookup(ConfigScope::Broker, CONNECTIONS_MAX_IDLE_MS)
        .expect("connections.max.idle.ms has a registry row");

    assert!(
        row.default
            == Some(
                DEFAULT_CONNECTIONS_MAX_IDLE
                    .millis_i64()
                    .to_string()
                    .as_str()
            )
    );
    assert!(row.read_only);
}

/// The broker-wide key's source follows where the value came from, not what
/// it is: the two configurations below run the same ten-minute window and
/// report different sources, exactly as the container does.
#[test]
fn the_idle_windows_source_is_its_provenance_and_not_its_value() {
    let cases = [
        ("unset", None, CONFIG_SOURCE_DEFAULT),
        (
            "set to Kafka's own default",
            Some(DEFAULT_CONNECTIONS_MAX_IDLE),
            CONFIG_SOURCE_STATIC_BROKER,
        ),
    ];
    for (case, idle, source) in cases {
        assert!(
            idle_window_entries(config_with(idle, &[]).statics(), &everything, VALUES_ONLY)
                == vec![expected(CONNECTIONS_MAX_IDLE_MS, "600000", source)],
            "{case}"
        );
    }
}

#[test]
fn a_set_idle_window_and_its_listener_overrides_report_as_static() {
    let entries = idle_window_entries(
        config_with(Some(secs(30)), &[
            ListenerWindow {
                name: "PLAINTEXT",
                window: millis(15_000),
            },
            ListenerWindow {
                name: "EXTERNAL",
                window: secs(45),
            },
        ])
        .statics(),
        &everything,
        VALUES_ONLY,
    );

    assert!(
        entries
            == vec![
                expected(
                    CONNECTIONS_MAX_IDLE_MS,
                    "30000",
                    CONFIG_SOURCE_STATIC_BROKER
                ),
                expected(
                    "listener.name.external.connections.max.idle.ms",
                    "45000",
                    CONFIG_SOURCE_STATIC_BROKER
                ),
                expected(
                    "listener.name.plaintext.connections.max.idle.ms",
                    "15000",
                    CONFIG_SOURCE_STATIC_BROKER
                ),
            ]
    );
}

/// The chains the container answers with, key by key, for the two
/// configurations that differ in whether the broker-wide key was set.
#[test]
fn requested_synonyms_carry_the_chain_the_container_reports() {
    let listener_key = "listener.name.plaintext.connections.max.idle.ms";
    let broker_wide_default = synonym(CONNECTIONS_MAX_IDLE_MS, "600000", CONFIG_SOURCE_DEFAULT);
    let broker_wide_static = synonym(
        CONNECTIONS_MAX_IDLE_MS,
        "30000",
        CONFIG_SOURCE_STATIC_BROKER,
    );
    let listener_static = synonym(listener_key, "15000", CONFIG_SOURCE_STATIC_BROKER);

    let cases = [
        ("broker-wide unset", None, vec![
            (
                CONNECTIONS_MAX_IDLE_MS,
                "600000",
                CONFIG_SOURCE_DEFAULT,
                vec![broker_wide_default.clone()],
            ),
            (listener_key, "15000", CONFIG_SOURCE_STATIC_BROKER, vec![
                listener_static.clone(),
                broker_wide_default.clone(),
            ]),
        ]),
        ("broker-wide set", Some(secs(30)), vec![
            (
                CONNECTIONS_MAX_IDLE_MS,
                "30000",
                CONFIG_SOURCE_STATIC_BROKER,
                vec![broker_wide_static.clone(), broker_wide_default.clone()],
            ),
            (listener_key, "15000", CONFIG_SOURCE_STATIC_BROKER, vec![
                listener_static.clone(),
                broker_wide_static.clone(),
                broker_wide_default.clone(),
            ]),
        ]),
    ];

    for (case, idle, want) in cases {
        let config = config_with(idle, &[ListenerWindow {
            name: "PLAINTEXT",
            window: millis(15_000),
        }]);
        let want: Vec<DescribeConfigsResourceResult> = want
            .into_iter()
            .map(
                |(name, value, source, synonyms)| DescribeConfigsResourceResult {
                    synonyms,
                    ..expected(name, value, source)
                },
            )
            .collect();
        assert!(
            idle_window_entries(config.statics(), &everything, WITH_SYNONYMS) == want,
            "{case}"
        );
    }
}

/// A request that does not ask for synonyms gets none, on every entry.
#[test]
fn unrequested_synonyms_are_never_synthesised() {
    let config = config_with(Some(secs(30)), &[ListenerWindow {
        name: "PLAINTEXT",
        window: millis(15_000),
    }]);
    let entries = idle_window_entries(config.statics(), &everything, VALUES_ONLY);

    assert!(entries.len() == 2);
    assert!(entries.iter().all(|entry| entry.synonyms.is_empty()));
}

/// The `configuration_keys` filter reaches both the broker-wide key and the
/// per-listener ones, each of which is a key the client can name.
#[test]
fn a_key_filter_selects_among_the_idle_entries() {
    let config = config_with(Some(secs(30)), &[ListenerWindow {
        name: "PLAINTEXT",
        window: millis(15_000),
    }]);
    let listener_key = "listener.name.plaintext.connections.max.idle.ms";

    let broker_wide_only = idle_window_entries(
        config.statics(),
        &|key| key == CONNECTIONS_MAX_IDLE_MS,
        VALUES_ONLY,
    );
    let listener_only =
        idle_window_entries(config.statics(), &|key| key == listener_key, VALUES_ONLY);
    let neither = idle_window_entries(config.statics(), &|key| key == "node.id", VALUES_ONLY);

    assert!(
        broker_wide_only
            == vec![expected(
                CONNECTIONS_MAX_IDLE_MS,
                "30000",
                CONFIG_SOURCE_STATIC_BROKER
            )]
    );
    assert!(listener_only == vec![expected(listener_key, "15000", CONFIG_SOURCE_STATIC_BROKER)]);
    assert!(neither == Vec::<DescribeConfigsResourceResult>::new());
}

/// A per-listener key has no registry row of its own, so it borrows the
/// broker-wide row's documentation — the same text `kafka-configs
/// --describe --all` prints above both.
#[test]
fn a_listener_override_documents_itself_from_the_broker_wide_row() {
    let row = registry::lookup(ConfigScope::Broker, CONNECTIONS_MAX_IDLE_MS)
        .expect("connections.max.idle.ms has a registry row");
    let documented = EntryOptions {
        include_synonyms: false,
        include_documentation: true,
    };

    let entries = idle_window_entries(
        config_with(None, &[ListenerWindow {
            name: "PLAINTEXT",
            window: millis(15_000),
        }])
        .statics(),
        &everything,
        documented,
    );

    assert!(
        entries
            .iter()
            .all(|entry| entry.documentation.as_deref() == Some(row.doc))
    );
}
