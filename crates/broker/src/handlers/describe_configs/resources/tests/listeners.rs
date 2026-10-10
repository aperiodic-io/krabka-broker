//! The listener keys that a named broker resource reports on each
//! `process.roles`: `listeners`, `advertised.listeners`,
//! `listener.security.protocol.map`, `controller.listener.names` and
//! `inter.broker.listener.name`.

use krabka_security::ListenerProtocol::{self, Plaintext, SaslPlaintext, SaslSsl, Ssl};

use super::*;
use crate::config::{
    BrokerConfig, ListenerSpec,
    NodeRole::{self, Broker, Controller},
};

/// The five keys, in the name order that `describe_one` reports them in.
const LISTENER_KEYS: [&str; 5] = [
    "advertised.listeners",
    "controller.listener.names",
    "inter.broker.listener.name",
    "listener.security.protocol.map",
    "listeners",
];

const SYNONYMS_ONLY: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: false,
};

/// Kafka 4.3.1's built-in defaults of the two keys that have one.
const DEFAULT_LISTENERS: &str = "PLAINTEXT://:9092";
const DEFAULT_PROTOCOL_MAP: &str =
    "SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT";

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ListenerSetup<'a> {
    #[default("CLIENT")]
    name: &'a str,
    #[default("0.0.0.0:9092")]
    bind: &'a str,
    #[default("broker-1.example:9092")]
    advertised: &'a str,
    #[default(SaslSsl)]
    protocol: ListenerProtocol,
}

fn listener(setup: ListenerSetup<'_>) -> ListenerSpec {
    let ListenerSetup {
        name,
        bind,
        advertised,
        protocol,
    } = setup;
    ListenerSpec {
        name: name.to_owned(),
        bind_addr: bind.parse().expect("a socket address"),
        advertised: advertised.to_owned(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: crate::SslPrincipalMapper::default(),
    }
}

// A node with two data-plane listeners and an SSL controller listener. A
// named inter-broker listener comes through the file config, which is where
// the loader records that the operator named it.
fn two_listener_node(roles: &[NodeRole], inter_broker: Option<&str>) -> BrokerConfig {
    let mut config = BrokerConfig {
        roles: roles.to_vec(),
        listeners: vec![
            listener(ListenerSetup::default()),
            listener(ListenerSetup {
                name: "INTERNAL",
                bind: "10.0.0.1:9094",
                advertised: "broker-1.internal:9094",
                protocol: SaslPlaintext,
            }),
        ],
        controller_listen_addr: "0.0.0.0:9093".parse().expect("a socket address"),
        controller_listener_protocol: Ssl,
        ..BrokerConfig::default()
    };
    if let Some(name) = inter_broker {
        let file: crate::file_config::FileConfig =
            toml::from_str(&format!("inter_broker_listener_name = \"{name}\"\n"))
                .expect("parse the file");
        file.apply_to(&mut config).expect("apply the file");
    }
    config
}

// A combined node on the single PLAINTEXT listener that `listen_addr` and
// `advertised_listener` describe, with nothing named about the inter-broker
// listener.
fn single_listener_node() -> BrokerConfig {
    BrokerConfig {
        roles: vec![Controller, Broker],
        listeners: Vec::new(),
        listen_addr: "127.0.0.1:9092".parse().expect("a socket address"),
        advertised_listener: "localhost:9092".to_owned(),
        controller_listen_addr: "127.0.0.1:9093".parse().expect("a socket address"),
        controller_listener_protocol: Plaintext,
        ..BrokerConfig::default()
    }
}

// A key that `server.properties` names: the value at `STATIC_BROKER_CONFIG`,
// with the built-in default beneath it when the key has one.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct NamedConfigSetup<'a> {
    #[default("listeners")]
    name: &'a str,
    value: &'a str,
    mutability: ConfigMutability,
    #[default(ConfigType::List)]
    config_type: ConfigType,
    builtin_default: Option<&'a str>,
}

fn named(setup: NamedConfigSetup<'_>) -> DescribeConfigsResourceResult {
    expected_config_entry(ExpectedConfigSetup {
        name: setup.name,
        value: Some(setup.value),
        mutability: setup.mutability,
        source: ConfigSourceCode(CONFIG_SOURCE_STATIC_BROKER),
        synonyms: std::iter::once(synonym(
            setup.name,
            setup.value,
            CONFIG_SOURCE_STATIC_BROKER,
        ))
        .chain(
            setup
                .builtin_default
                .map(|default| synonym(setup.name, default, CONFIG_SOURCE_DEFAULT)),
        )
        .collect(),
        config_type: ConfigTypeCode(setup.config_type.wire()),
        ..Default::default()
    })
}

// A key that `server.properties` does not name and that has no built-in
// default: a null value at `DEFAULT_CONFIG`, with no synonym.
fn unset(name: &str, config_type: ConfigType) -> DescribeConfigsResourceResult {
    tagged_wire!(DescribeConfigsResourceResult {
        name: name.to_owned(),
        value: None,
        read_only: true,
        config_source: CONFIG_SOURCE_DEFAULT,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: config_type.wire(),
        documentation: None,
    })
}

fn listeners(value: &str) -> DescribeConfigsResourceResult {
    named(NamedConfigSetup {
        value,
        builtin_default: Some(DEFAULT_LISTENERS),
        ..Default::default()
    })
}

fn advertised(value: &str) -> DescribeConfigsResourceResult {
    named(NamedConfigSetup {
        name: "advertised.listeners",
        value,
        mutability: ConfigMutability::ReadOnly,
        ..Default::default()
    })
}

fn protocol_map(value: &str) -> DescribeConfigsResourceResult {
    named(NamedConfigSetup {
        name: "listener.security.protocol.map",
        value,
        config_type: ConfigType::String,
        builtin_default: Some(DEFAULT_PROTOCOL_MAP),
        ..Default::default()
    })
}

fn controller_listener_names() -> DescribeConfigsResourceResult {
    named(NamedConfigSetup {
        name: "controller.listener.names",
        value: "CONTROLLER",
        mutability: ConfigMutability::ReadOnly,
        ..Default::default()
    })
}

fn inter_broker(value: &str) -> DescribeConfigsResourceResult {
    named(NamedConfigSetup {
        name: "inter.broker.listener.name",
        value,
        mutability: ConfigMutability::ReadOnly,
        config_type: ConfigType::String,
        ..Default::default()
    })
}

// Kafka's `listeners` is what the node opens: the controller listener alone on
// a controller-only node, the broker listeners alone on a broker-only node, and
// both on a combined node. Every KRaft node names `controller.listener.names`,
// and `listener.security.protocol.map` maps the controller listener on a
// broker-only node too, because that node dials it. `advertised.listeners`
// names the broker listeners: krabka takes the advertised controller endpoint
// from the voter set, as Kafka does when `advertised.listeners` does not name
// the controller listener, so a controller-only node leaves the key unset. And
// `inter.broker.listener.name` is at `STATIC_BROKER_CONFIG` only when the
// operator named it.
#[test]
fn a_named_broker_reports_the_listener_keys_of_its_roles() {
    let advertised_brokers = "CLIENT://broker-1.example:9092,INTERNAL://broker-1.internal:9094";
    let broker_map = "CLIENT:SASL_SSL,INTERNAL:SASL_PLAINTEXT,CONTROLLER:SSL";
    let cases = [
        (
            "controller-only",
            two_listener_node(&[Controller], None),
            vec![
                unset("advertised.listeners", ConfigType::List),
                controller_listener_names(),
                unset("inter.broker.listener.name", ConfigType::String),
                protocol_map("CONTROLLER:SSL"),
                listeners("CONTROLLER://0.0.0.0:9093"),
            ],
        ),
        (
            "broker-only",
            two_listener_node(&[Broker], Some("INTERNAL")),
            vec![
                advertised(advertised_brokers),
                controller_listener_names(),
                inter_broker("INTERNAL"),
                protocol_map(broker_map),
                listeners("CLIENT://0.0.0.0:9092,INTERNAL://10.0.0.1:9094"),
            ],
        ),
        (
            "combined",
            two_listener_node(&[Controller, Broker], Some("INTERNAL")),
            vec![
                advertised(advertised_brokers),
                controller_listener_names(),
                inter_broker("INTERNAL"),
                protocol_map(broker_map),
                listeners(
                    "CLIENT://0.0.0.0:9092,INTERNAL://10.0.0.1:9094,CONTROLLER://0.0.0.0:9093",
                ),
            ],
        ),
        (
            "combined, on one PLAINTEXT listener",
            single_listener_node(),
            vec![
                advertised("PLAINTEXT://localhost:9092"),
                controller_listener_names(),
                unset("inter.broker.listener.name", ConfigType::String),
                protocol_map("PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                listeners("PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093"),
            ],
        ),
    ];

    for (label, config, expected) in cases {
        let settings = static_settings(&config);
        let described = describe_at(
            settings_for((RESOURCE_TYPE_BROKER, "1"), &settings),
            &MetadataImage::new(Uuid::nil()),
            RESOURCE_TYPE_BROKER,
            "1",
            Some(LISTENER_KEYS.map(str::to_owned).to_vec()),
            SYNONYMS_ONLY,
        );

        check!(described.configs == expected, "{label}");
    }
}
