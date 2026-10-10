//! What one `DescribeConfigsResourceResult` says: the value and source the
//! precedence chain resolves to, the synonyms the chain becomes, the typed
//! metadata the registry supplies, and the redaction a sensitive key gets.

use assert2::{assert, check};
use krabka_protocol::UnknownTaggedFields;

use super::{
    super::wire::{
        CONFIG_SOURCE_DYNAMIC_BROKER, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
        CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER,
    },
    *,
};
use crate::config_keys::registry::{self, ConfigScope, ConfigType, ValueCheck};

const BOTH: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: true,
};
const NEITHER: EntryOptions = EntryOptions {
    include_synonyms: false,
    include_documentation: false,
};

fn retention_row() -> &'static registry::ConfigKey {
    registry::lookup(ConfigScope::Topic, crate::config_keys::RETENTION_MS)
        .expect("retention.ms is a registered topic key")
}

/// A secret-valued key, of the kind krabka does not have yet. The redaction
/// rule has to hold before such a key exists, not after.
const SECRET: registry::ConfigKey = registry::ConfigKey {
    name: "listener.name.external.ssl.keystore.password",
    scope: ConfigScope::Broker,
    config_type: ConfigType::String,
    type_note: None,
    default: Some("hunter2"),
    doc: "The store password for the key store file.",
    read_only: false,
    sensitive: true,
    internal: false,
    kip: None,
    cluster_default: None,
    check: ValueCheck::Parsed,
};

#[test]
fn the_chain_head_decides_the_value_and_the_source() {
    // The whole chain in one entry: a per-broker override over a cluster
    // default over the built-in default. `ConfigEntry.source()` is the head,
    // and the synonyms are the chain in the same order.
    let entry = config_entry(
        Some(&SECRET_FREE),
        "log.retention.ms",
        &[
            Layer {
                source: CONFIG_SOURCE_DYNAMIC_BROKER,
                name: "log.retention.ms",
                value: "90000",
            },
            Layer {
                source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                name: "log.retention.ms",
                value: "120000",
            },
        ],
        DefaultLayer {
            value: Some("604800000"),
            name: Some("log.retention.ms"),
        },
        BOTH,
    );

    assert!(
        entry
            == tagged_wire!(DescribeConfigsResourceResult {
                name: "log.retention.ms".to_owned(),
                value: Some("90000".to_owned()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_BROKER,
                is_sensitive: false,
                synonyms: vec![
                    synonym("log.retention.ms", "90000", CONFIG_SOURCE_DYNAMIC_BROKER),
                    synonym(
                        "log.retention.ms",
                        "120000",
                        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                    ),
                    synonym("log.retention.ms", "604800000", CONFIG_SOURCE_DEFAULT),
                ],
                config_type: ConfigType::Long.wire(),
                documentation: Some(SECRET_FREE.doc.to_owned()),
            })
    );
}

/// A name Kafka does not define has no row: `KafkaConfig.configType` is empty,
/// so the entry is `UNKNOWN`-typed, its value is withheld (`maybeSensitive`
/// treats an untyped key as sensitive), and, being in no dynamic set, it is
/// read-only.
#[test]
fn a_name_kafka_does_not_define_is_untyped_withheld_and_read_only() {
    let entry = config_entry(
        None,
        "plugin.custom.key",
        &[Layer {
            source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            name: "plugin.custom.key",
            value: "x",
        }],
        DefaultLayer::default(),
        BOTH,
    );

    assert!(
        entry
            == DescribeConfigsResourceResult {
                name: "plugin.custom.key".to_owned(),
                value: None,
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                is_sensitive: true,
                synonyms: vec![DescribeConfigsSynonym {
                    name: "plugin.custom.key".to_owned(),
                    value: None,
                    source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    ..Default::default()
                }],
                config_type: 0,
                documentation: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
}

/// The same row as `log.retention.ms` would carry, so the chain test does not
/// depend on a key krabka happens to have.
const SECRET_FREE: registry::ConfigKey = registry::ConfigKey {
    name: "log.retention.ms",
    scope: ConfigScope::Broker,
    config_type: ConfigType::Long,
    type_note: Some("ms"),
    default: Some("604800000"),
    doc: "How long a log file is kept before it is deleted.",
    read_only: false,
    sensitive: false,
    internal: false,
    kip: None,
    cluster_default: None,
    check: ValueCheck::Parsed,
};

use crate::handlers::describe_configs::test_support::synonym;

#[test]
fn an_empty_chain_reports_the_default_at_default_config() {
    let row = retention_row();

    for (label, default, expected_value, expected_synonyms) in [
        (
            "a key that names the broker config its default comes from",
            DefaultLayer {
                value: Some("604800000"),
                name: Some("log.retention.ms"),
            },
            Some("604800000".to_owned()),
            vec![synonym(
                "log.retention.ms",
                "604800000",
                CONFIG_SOURCE_DEFAULT,
            )],
        ),
        (
            "a key with no broker-level config, which Kafka gives no synonym",
            DefaultLayer {
                value: Some("604800000"),
                name: None,
            },
            Some("604800000".to_owned()),
            Vec::new(),
        ),
        (
            "a key with no default at all",
            DefaultLayer::default(),
            None,
            Vec::new(),
        ),
    ] {
        let entry = config_entry(Some(row), row.name, &[], default, BOTH);
        check!(entry.value == expected_value, "{label}");
        check!(entry.config_source == CONFIG_SOURCE_DEFAULT, "{label}");
        check!(entry.synonyms == expected_synonyms, "{label}");
    }
}

#[test]
fn the_request_flags_gate_the_synonyms_and_the_documentation() {
    let row = retention_row();
    let layers = [Layer {
        source: CONFIG_SOURCE_DYNAMIC_TOPIC,
        name: row.name,
        value: "60000",
    }];
    let default = DefaultLayer {
        value: row.default,
        name: None,
    };

    let full = config_entry(Some(row), row.name, &layers, default, BOTH);
    let bare = config_entry(Some(row), row.name, &layers, default, NEITHER);

    check!(full.synonyms.len() == 1);
    check!(full.documentation == Some(row.doc.to_owned()));
    // The source still names the layer the value came from. A client that
    // asks for no synonyms still has to be able to tell an override from an
    // inherited default.
    check!(bare.config_source == CONFIG_SOURCE_DYNAMIC_TOPIC);
    check!(bare.value == Some("60000".to_owned()));
    check!(bare.synonyms == Vec::new());
    check!(bare.documentation == None);
}

#[test]
fn a_sensitive_key_reports_no_value_anywhere_in_the_chain() {
    let entry = config_entry(
        Some(&SECRET),
        SECRET.name,
        &[Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: SECRET.name,
            value: "correct-horse-battery-staple",
        }],
        DefaultLayer {
            value: SECRET.default,
            name: Some(SECRET.name),
        },
        BOTH,
    );

    assert!(
        entry
            == tagged_wire!(DescribeConfigsResourceResult {
                name: SECRET.name.to_owned(),
                value: None,
                read_only: false,
                config_source: CONFIG_SOURCE_STATIC_BROKER,
                is_sensitive: true,
                synonyms: vec![
                    tagged_wire!(DescribeConfigsSynonym {
                        name: SECRET.name.to_owned(),
                        value: None,
                        source: CONFIG_SOURCE_STATIC_BROKER,
                    }),
                    tagged_wire!(DescribeConfigsSynonym {
                        name: SECRET.name.to_owned(),
                        value: None,
                        source: CONFIG_SOURCE_DEFAULT,
                    }),
                ],
                config_type: ConfigType::String.wire(),
                documentation: Some(SECRET.doc.to_owned()),
            })
    );
}

#[test]
fn a_key_with_no_registry_row_is_treated_as_one_the_broker_may_not_disclose() {
    // Kafka's `ConfigHelper.maybeSensitive` reads an absent type the same way:
    // a value the broker cannot describe is a value it does not echo.
    let entry = config_entry(
        None,
        "some.key.no.row.covers",
        &[Layer {
            source: CONFIG_SOURCE_DYNAMIC_BROKER,
            name: "some.key.no.row.covers",
            value: "a-value",
        }],
        DefaultLayer::default(),
        BOTH,
    );

    check!(entry.value == None);
    check!(entry.is_sensitive);
    check!(entry.config_type == 0i8);
    check!(entry.documentation == None);
    check!(
        entry.synonyms
            == vec![tagged_wire!(DescribeConfigsSynonym {
                name: "some.key.no.row.covers".to_owned(),
                value: None,
                source: CONFIG_SOURCE_DYNAMIC_BROKER,
            })]
    );
}

#[test]
fn the_read_only_flag_comes_from_the_registry_row() {
    for (scope, name, expected) in [
        (ConfigScope::Topic, crate::config_keys::DISKLESS, true),
        (ConfigScope::Topic, crate::config_keys::WRITE_FREEZE, true),
        (ConfigScope::Topic, crate::config_keys::RETENTION_MS, false),
        (
            ConfigScope::Broker,
            crate::config_keys::BROKER_WITNESS,
            true,
        ),
        (
            ConfigScope::Broker,
            crate::config_keys::registry::NODE_ID,
            true,
        ),
    ] {
        let row = registry::lookup(scope, name).expect(name);
        let entry = config_entry(Some(row), name, &[], DefaultLayer::default(), NEITHER);
        check!(entry.read_only == expected, "{name}");
    }
}
