//! What `DescribeConfigs` reports for a node's listener keys on each
//! `process.roles`, read through the listener that a client uses to reach that
//! node.
//!
//! A controller-only node opens only its controller listener, so
//! `kafka-configs --bootstrap-controller` is the one way to it. A broker-only
//! node opens only its data-plane listener, which `--bootstrap-server` dials. A
//! combined node opens both, and both answer the same.

mod support;

use std::net::SocketAddr;

use assert2::check;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, config::NodeRole};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
        },
    },
};
use tempfile::TempDir;

use crate::support::configs::describe_resource;

/// Kafka's `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;

/// Kafka's `ConfigSource.STATIC_BROKER_CONFIG` and `DEFAULT_CONFIG`.
const STATIC_BROKER_CONFIG: i8 = 4;
const DEFAULT_CONFIG: i8 = 5;

/// The five keys, in the name order that the broker reports them in.
const LISTENER_KEYS: [&str; 5] = [
    "advertised.listeners",
    "controller.listener.names",
    "inter.broker.listener.name",
    "listener.security.protocol.map",
    "listeners",
];

/// Kafka 4.3.1's built-in defaults of the two keys that have one.
const DEFAULT_LISTENERS: &str = "PLAINTEXT://:9092";
const DEFAULT_PROTOCOL_MAP: &str =
    "SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT";

struct Node {
    handle: BrokerHandle,
    /// The address that the data-plane listener was given, which is also the
    /// address that it advertises.
    data_addr: SocketAddr,
    _dir: TempDir,
}

// Starts node `node` with `roles` and one PLAINTEXT listener of each kind. A
// node with the controller role is the only voter of its own quorum. A
// broker-only node joins the quorum of `controller`.
async fn start_node(node: NodeId, roles: &[NodeRole], controller: Option<&BrokerHandle>) -> Node {
    let dir = TempDir::new().expect("tempdir");
    let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the data-plane listener");
    let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the controller listener");
    let data_addr = data_listener.local_addr().expect("data-plane address");
    let controller_addr = controller_listener
        .local_addr()
        .expect("controller address");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.broker_id = i32::try_from(node.0).expect("a small node id");
    config.node_id = node;
    config.listen_addr = data_addr;
    config.advertised_listener = data_addr.to_string();
    config.controller_listen_addr = controller_addr;
    config.roles = roles.to_vec();
    match controller {
        None => {
            config.controller_quorum_voters = vec![(node, controller_addr.to_string())];
        }
        Some(controller) => {
            config.controller_quorum_voters = vec![(
                NodeId(controller.node_id()),
                controller.controller_addr().to_string(),
            )];
            config.bootstrap_mode = BootstrapMode::Join;
        }
    }
    let handle =
        Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
            .await
            .expect("start the node");
    Node {
        handle,
        data_addr,
        _dir: dir,
    }
}

// Sends `DescribeConfigs` for the listener keys of `node` to `address`, with
// the synonyms, the way `kafka-configs --describe --all` asks.
async fn describe_listener_keys(address: SocketAddr, node: u64) -> DescribeConfigsResult {
    let connection = Connection::connect(
        address,
        ConnectionOptions {
            client_id: "describe-configs-listeners".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("connect");
    let response = connection
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                node.to_string(),
                Some(LISTENER_KEYS.map(str::to_owned).to_vec()),
            )],
            include_synonyms: true,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    connection.close();
    response
        .results
        .into_iter()
        .next()
        .expect("one result for the one resource")
}

fn synonym(name: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        source,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

#[derive(Clone, Copy, Default)]
enum ConfigMutability {
    #[default]
    Writable,
    ReadOnly,
}

#[derive(Clone, Copy, Default)]
enum ConfigKind {
    String,
    #[default]
    List,
}

impl ConfigKind {
    const fn wire(self) -> i8 {
        match self {
            Self::String => 2,
            Self::List => 7,
        }
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct NamedConfigSetup<'a> {
    #[default("listeners")]
    name: &'a str,
    value: &'a str,
    mutability: ConfigMutability,
    kind: ConfigKind,
    builtin_default: Option<&'a str>,
}

fn named(setup: NamedConfigSetup<'_>) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: setup.name.to_owned(),
        value: Some(setup.value.to_owned()),
        read_only: matches!(setup.mutability, ConfigMutability::ReadOnly),
        config_source: STATIC_BROKER_CONFIG,
        is_sensitive: false,
        synonyms: std::iter::once(synonym(setup.name, setup.value, STATIC_BROKER_CONFIG))
            .chain(
                setup
                    .builtin_default
                    .map(|default| synonym(setup.name, default, DEFAULT_CONFIG)),
            )
            .collect(),
        config_type: setup.kind.wire(),
        documentation: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn unset(name: &str, kind: ConfigKind) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.to_owned(),
        value: None,
        read_only: true,
        config_source: DEFAULT_CONFIG,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: kind.wire(),
        documentation: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

// The listener keys that a node reports. `advertised` is `None` on a node that
// names no advertised listener.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ExpectedListenersSetup<'a> {
    #[default(NodeId(1))]
    node: NodeId,
    #[default(DEFAULT_LISTENERS)]
    listeners: &'a str,
    advertised: Option<&'a str>,
    #[default(DEFAULT_PROTOCOL_MAP)]
    protocol_map: &'a str,
}

fn expected(setup: ExpectedListenersSetup<'_>) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code: 0,
        error_message: None,
        resource_type: RESOURCE_TYPE_BROKER,
        resource_name: setup.node.0.to_string(),
        configs: vec![
            setup.advertised.map_or_else(
                || unset("advertised.listeners", ConfigKind::List),
                |advertised| {
                    named(NamedConfigSetup {
                        name: "advertised.listeners",
                        value: advertised,
                        mutability: ConfigMutability::ReadOnly,
                        ..Default::default()
                    })
                },
            ),
            named(NamedConfigSetup {
                name: "controller.listener.names",
                value: "CONTROLLER",
                mutability: ConfigMutability::ReadOnly,
                ..Default::default()
            }),
            unset("inter.broker.listener.name", ConfigKind::String),
            named(NamedConfigSetup {
                name: "listener.security.protocol.map",
                value: setup.protocol_map,
                kind: ConfigKind::String,
                builtin_default: Some(DEFAULT_PROTOCOL_MAP),
                ..Default::default()
            }),
            named(NamedConfigSetup {
                value: setup.listeners,
                builtin_default: Some(DEFAULT_LISTENERS),
                ..Default::default()
            }),
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

#[derive(Debug, Clone, Copy)]
enum Roles {
    ControllerOnly,
    BrokerOnly,
    Combined,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_role_reports_the_listener_keys_through_the_listener_a_client_uses() {
    for roles in [Roles::ControllerOnly, Roles::BrokerOnly, Roles::Combined] {
        // The nodes of the cluster, the node described, the listeners a client
        // reaches it on, and what it reports.
        let (nodes, described, dialed, want) = match roles {
            Roles::ControllerOnly => {
                let node = start_node(NodeId(1), &[NodeRole::Controller], None).await;
                let controller = node.handle.controller_addr();
                let want = expected(ExpectedListenersSetup {
                    listeners: &format!("CONTROLLER://{controller}"),
                    protocol_map: "CONTROLLER:PLAINTEXT",
                    ..Default::default()
                });
                (vec![node], 1, vec![("controller", controller)], want)
            }
            Roles::BrokerOnly => {
                let controller = start_node(NodeId(1), &[NodeRole::Controller], None).await;
                let broker =
                    start_node(NodeId(2), &[NodeRole::Broker], Some(&controller.handle)).await;
                let data = broker.data_addr;
                let want = expected(ExpectedListenersSetup {
                    node: NodeId(2),
                    listeners: &format!("PLAINTEXT://{data}"),
                    advertised: Some(&format!("PLAINTEXT://{data}")),
                    protocol_map: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                });
                (
                    vec![broker, controller],
                    2,
                    vec![("data-plane", data)],
                    want,
                )
            }
            Roles::Combined => {
                let node =
                    start_node(NodeId(1), &[NodeRole::Controller, NodeRole::Broker], None).await;
                let data = node.data_addr;
                let controller = node.handle.controller_addr();
                let want = expected(ExpectedListenersSetup {
                    listeners: &format!("PLAINTEXT://{data},CONTROLLER://{controller}"),
                    advertised: Some(&format!("PLAINTEXT://{data}")),
                    protocol_map: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                    ..Default::default()
                });
                (
                    vec![node],
                    1,
                    vec![("data-plane", data), ("controller", controller)],
                    want,
                )
            }
        };

        for (listener, address) in dialed {
            check!(
                describe_listener_keys(address, described).await == want,
                "{roles:?} through the {listener} listener"
            );
        }
        for node in nodes {
            node.handle.shutdown().await;
        }
    }
}
