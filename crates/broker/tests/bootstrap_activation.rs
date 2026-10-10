//! The bootstrap records of a new cluster reach the metadata log once, from
//! the active controller, as Kafka's `QuorumController` writes them on its
//! activation with `ActivationRecordsGenerator.recordsForEmptyLog`. A broker
//! never writes them, and waits until it reads them from the log.
//!
//! Each case formats every node with `krabka-format` in process, as Kafka's
//! system tests run `kafka-storage format` before they start a node. It starts
//! the controllers and then the brokers, and reads the committed metadata log
//! back off the controller listener of the leader.
//!
//! When the bootstrap records enable ELR, the controller also writes the
//! cluster-level `min.insync.replicas`, at its static value, in the same
//! batch, and `DescribeConfigs` reports it as Kafka reports it.
//!
//! The dynamic case is the topology of Kafka's `quorum_reconfiguration_test.py`.
//! Its standalone controller holds the bootstrap records in its bootstrap
//! checkpoint, and its brokers hold none. A controller that kept those records
//! in its image and out of the log left the brokers without a
//! `metadata.version`, and each broker then wrote its own copy.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use assert2::{assert, check};
use futures_util::future::join_all;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, config::NodeRole};
use krabka_metadata::{
    BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
    metadata_version::ELR_VERSION_FEATURE,
};
use krabka_protocol::owned::{
    describe_configs_request::DescribeConfigsRequest,
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    },
};
use tempfile::TempDir;
use tokio::net::TcpListener;

use crate::support::{client::connect_owned, configs::describe_resource};

mod support;

/// The cluster id `kafka-storage format` is given in the system tests.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node gets to finish starting.
const DEADLINE: Duration = Duration::from_secs(60);

/// Kafka's `TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG`.
const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// Kafka's `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;

/// Kafka's `DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG`.
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;

/// Kafka's `DescribeConfigsResponse.ConfigType.INT`.
const CONFIG_TYPE_INT: i8 = 3;

/// How a case's nodes find the controller quorum.
#[derive(Debug, Clone, Copy)]
enum Quorum {
    /// KIP-853: the first controller is formatted with `--standalone`, and
    /// every node names the controllers through
    /// `controller.quorum.bootstrap.servers` alone.
    Dynamic,
    /// KIP-595: every node names the controllers through
    /// `controller.quorum.voters`, and no format names a quorum.
    Static,
}

/// One node of a case: its node id and its roles.
type NodeSpec = (NodeId, &'static [NodeRole]);

const CONTROLLER: &[NodeRole] = &[NodeRole::Controller];
const BROKER: &[NodeRole] = &[NodeRole::Broker];
const COMBINED: &[NodeRole] = &[NodeRole::Broker, NodeRole::Controller];

/// A node that is formatted and not yet started.
struct Node {
    id: NodeId,
    roles: &'static [NodeRole],
    client: TcpListener,
    controller: TcpListener,
    client_addr: SocketAddr,
    controller_addr: SocketAddr,
    dir: TempDir,
    log_dir: PathBuf,
}

/// Formats `log_dir` for node `id` with `krabka-format` and the quorum and
/// feature flags in `flags`, in process.
async fn format(log_dir: &std::path::Path, id: NodeId, flags: &[String]) {
    let node_id = id.to_string();
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.to_str().expect("utf-8 temp path").to_owned(),
        "--cluster-id".to_owned(),
        CLUSTER_ID.to_owned(),
        "--node-id".to_owned(),
        node_id,
    ];
    argv.extend_from_slice(flags);
    let code = krabka_format::run_from_args(argv).await;
    assert!(code == 0, "krabka-format exited {code} for node {id}");
}

/// The config the broker binary builds for `node` from its formatted
/// directory: the ids of its `meta.properties`, `Bootstrap` mode for a fresh
/// directory, and the static `min.insync.replicas` of the case.
#[derive(Clone, Copy, derive_more::Display)]
struct MinimumInsyncReplicas(i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ActivationSetup<'a> {
    #[default(Quorum::Dynamic)]
    quorum: Quorum,
    controllers: &'a [(NodeId, SocketAddr)],
    #[default(MinimumInsyncReplicas(1))]
    min_insync_replicas: MinimumInsyncReplicas,
}

fn config(node: &Node, setup: ActivationSetup<'_>) -> BrokerConfig {
    let meta = krabka_broker::bootstrap::initialize_log_dirs(
        &node.log_dir,
        std::slice::from_ref(&node.log_dir),
        node.id,
        None,
    )
    .expect("krabka-format wrote meta.properties");
    let mut config = crate::support::addressed_node_config(
        &node.log_dir,
        crate::support::AddressedNodeSetup {
            node: node.id,
            client: node.client_addr,
            controller: node.controller_addr,
        },
    );
    config.roles = node.roles.to_vec();
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.cluster_id = Some(meta.cluster_id);
    config.directory_id = meta.directory_id;
    config.default_min_insync_replicas = setup.min_insync_replicas.0;
    match setup.quorum {
        Quorum::Dynamic => {
            config.controller_quorum_voters = vec![];
            config.bootstrap_servers = setup
                .controllers
                .iter()
                .map(|(_, addr)| addr.to_string())
                .collect();
        }
        Quorum::Static => {
            config.controller_quorum_voters = setup
                .controllers
                .iter()
                .map(|(id, addr)| (*id, addr.to_string()))
                .collect();
        }
    }
    config
}

/// Starts every node of `nodes` at the same time, and fails the test when one
/// does not finish starting.
async fn start_all(
    nodes: Vec<Node>,
    setup: ActivationSetup<'_>,
) -> Vec<(NodeId, BrokerHandle, TempDir)> {
    join_all(nodes.into_iter().map(|node| async move {
        let config = config(&node, setup);
        let id = node.id;
        let handle = tokio::time::timeout(
            DEADLINE,
            Broker::start_with_listeners(config, Some(node.controller), Some(node.client)),
        )
        .await
        .unwrap_or_else(|_| panic!("node {id} did not finish starting in {DEADLINE:?}"))
        .unwrap_or_else(|error| panic!("node {id} failed to start: {error}"));
        (id, handle, node.dir)
    }))
    .await
}

/// One batch of the committed metadata log.
#[derive(Debug, Clone, PartialEq)]
enum Batch {
    /// A control batch: a `LeaderChange` marker and the KIP-853 controls.
    Control,
    /// A batch of metadata records.
    Metadata(Vec<MetadataRecord>),
}

/// Reads the committed metadata log from its start off the controller
/// listener at `controller`, with each value decoded against the image that
/// the values before it produce.
async fn committed_batches(controller: SocketAddr) -> Vec<Batch> {
    let response =
        crate::support::client::metadata_fetch(controller, "bootstrap-activation-test", 0).await;

    let mut image = MetadataImage::new(uuid::Uuid::nil());
    let mut batches = Vec::new();
    for batch in crate::support::records::metadata_batches(&response.records) {
        if batch.attributes.is_control_batch() {
            batches.push(Batch::Control);
            continue;
        }
        let mut records = Vec::new();
        for value in batch
            .records
            .iter()
            .filter_map(|record| record.value.as_ref())
        {
            if krabka_raft::is_kip835_noop(value) {
                continue;
            }
            let record =
                krabka_metadata::from_kraft_value(value, &image).expect("decode a metadata value");
            image.apply(&record);
            records.push(record);
        }
        batches.push(Batch::Metadata(records));
    }
    batches
}

/// What the committed log says about the bootstrap records: whether the
/// first metadata batch follows the leader's control batch, that batch, and
/// every batch that finalizes a feature level.
type Seeding = (bool, Option<Batch>, Vec<Vec<MetadataRecord>>);

fn seeding(batches: &[Batch]) -> Seeding {
    let leading_controls = batches
        .iter()
        .take_while(|batch| **batch == Batch::Control)
        .count();
    let finalizing = batches
        .iter()
        .filter_map(|batch| match batch {
            Batch::Metadata(records)
                if records
                    .iter()
                    .any(|record| matches!(record, MetadataRecord::V1FeatureLevel(_))) =>
            {
                Some(records.clone())
            }
            _ => None,
        })
        .collect();
    (
        leading_controls > 0,
        batches.get(leading_controls).cloned(),
        finalizing,
    )
}

/// `DescribeConfigs` of the cluster-level `min.insync.replicas` through the
/// client listener at `broker`, as `kafka-configs --describe --entity-type
/// brokers --entity-default` sends it, with the synonyms.
async fn describe_cluster_min_insync_replicas(broker: SocketAddr) -> DescribeConfigsResult {
    let client = connect_owned(broker.to_string(), "bootstrap-activation-test", "client").await;
    let response = client
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                String::new(),
                Some(vec![MIN_INSYNC_REPLICAS.to_owned()]),
            )],
            include_synonyms: true,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let mut results = response.results;
    assert!(results.len() == 1, "one result per resource: {results:?}");
    results.remove(0)
}

/// What Kafka's `DescribeConfigs` answers for the cluster-level
/// `min.insync.replicas` at `value`: a `DYNAMIC_DEFAULT_BROKER_CONFIG` entry,
/// writable, of type `INT`, whose only synonym is the cluster default itself.
/// Without a value there is no entry.
fn kafkas_cluster_min_insync_replicas(
    value: Option<MinimumInsyncReplicas>,
) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code: 0,
        error_message: None,
        resource_type: RESOURCE_TYPE_BROKER,
        resource_name: String::new(),
        configs: value
            .map(|value| DescribeConfigsResourceResult {
                name: MIN_INSYNC_REPLICAS.to_owned(),
                value: Some(value.to_string()),
                read_only: false,
                config_source: DYNAMIC_DEFAULT_BROKER_CONFIG,
                is_sensitive: false,
                synonyms: vec![DescribeConfigsSynonym {
                    name: MIN_INSYNC_REPLICAS.to_owned(),
                    value: Some(value.to_string()),
                    source: DYNAMIC_DEFAULT_BROKER_CONFIG,
                    ..Default::default()
                }],
                config_type: CONFIG_TYPE_INT,
                documentation: None,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

/// One case: its topology, whether its format enables ELR, and the static
/// `min.insync.replicas` of every node.
#[derive(Clone, Copy)]
enum EligibleLeaderReplicas {
    Enabled,
    Disabled,
}

struct Case {
    what: &'static str,
    quorum: Quorum,
    controllers: &'static [NodeSpec],
    brokers: &'static [NodeSpec],
    eligible_leader_replicas: EligibleLeaderReplicas,
    min_insync_replicas: MinimumInsyncReplicas,
}

/// The active controller writes the bootstrap records once, as the first
/// metadata batch of the log, on a dynamic quorum, on a static quorum of
/// isolated controllers, and on a combined node. When the bootstrap records
/// enable ELR, the same batch ends with the cluster-level
/// `min.insync.replicas` at the controller's static value, as Kafka's
/// `ActivationRecordsGenerator.recordsForEmptyLog` writes it, and
/// `DescribeConfigs` reports it as Kafka does. No broker writes a second copy,
/// and every node's image takes the records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_active_controller_writes_the_bootstrap_records_once() {
    support::init_tracing();
    let cases = [
        Case {
            what: "a dynamic quorum of a standalone controller and two brokers",
            quorum: Quorum::Dynamic,
            controllers: &[(NodeId(3001), CONTROLLER)],
            brokers: &[(NodeId(1), BROKER), (NodeId(2), BROKER)],
            eligible_leader_replicas: EligibleLeaderReplicas::Enabled,
            min_insync_replicas: MinimumInsyncReplicas(2),
        },
        Case {
            what: "a static quorum of three isolated controllers and two brokers",
            quorum: Quorum::Static,
            controllers: &[
                (NodeId(3001), CONTROLLER),
                (NodeId(3002), CONTROLLER),
                (NodeId(3003), CONTROLLER),
            ],
            brokers: &[(NodeId(1), BROKER), (NodeId(2), BROKER)],
            eligible_leader_replicas: EligibleLeaderReplicas::Enabled,
            min_insync_replicas: MinimumInsyncReplicas(1),
        },
        Case {
            what: "one combined node",
            quorum: Quorum::Static,
            controllers: &[(NodeId(1), COMBINED)],
            brokers: &[],
            eligible_leader_replicas: EligibleLeaderReplicas::Enabled,
            min_insync_replicas: MinimumInsyncReplicas(2),
        },
        Case {
            what: "one combined node formatted without ELR",
            quorum: Quorum::Static,
            controllers: &[(NodeId(1), COMBINED)],
            brokers: &[],
            eligible_leader_replicas: EligibleLeaderReplicas::Disabled,
            min_insync_replicas: MinimumInsyncReplicas(2),
        },
    ];
    for case in cases {
        run_case(case).await;
    }
}

async fn run_case(case: Case) {
    let Case {
        what,
        quorum,
        controllers: controller_specs,
        brokers: broker_specs,
        eligible_leader_replicas,
        min_insync_replicas,
    } = case;
    // A default format finalizes every feature at the default level of the
    // latest production release, `metadata.version` first. That enables ELR.
    // `--feature eligible.leader.replicas.version=0` leaves it out.
    let disabled: std::collections::BTreeMap<String, i16> =
        if matches!(eligible_leader_replicas, EligibleLeaderReplicas::Enabled) {
            std::collections::BTreeMap::new()
        } else {
            [(ELR_VERSION_FEATURE.to_owned(), 0)].into()
        };
    let bootstrap = krabka_metadata::bootstrap_feature_records_with_overrides(
        krabka_format::LATEST_PRODUCTION_METADATA_VERSION,
        &disabled,
    );
    let cluster_min_isr = matches!(eligible_leader_replicas, EligibleLeaderReplicas::Enabled)
        .then_some(min_insync_replicas);
    let mut activation = bootstrap.clone();
    activation.extend(cluster_min_isr.map(|value| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: MIN_INSYNC_REPLICAS.to_owned(),
            config_value: Some(value.to_string()),
        })
    }));
    let expected_levels: std::collections::BTreeMap<String, i16> = bootstrap
        .iter()
        .filter_map(|record| match record {
            MetadataRecord::V1FeatureLevel(feature) => Some((feature.name.clone(), feature.level)),
            _ => None,
        })
        .collect();

    let count = controller_specs.len() + broker_specs.len();
    let (client_addrs, controller_addrs, clients, controller_listeners) =
        support::bind_and_hold_ports(count).await;
    let mut nodes: Vec<Node> = controller_specs
        .iter()
        .chain(broker_specs)
        .zip(clients.into_iter().zip(controller_listeners))
        .zip(client_addrs.into_iter().zip(controller_addrs))
        .map(
            |((&(id, roles), (client, controller)), (client_addr, controller_addr))| {
                let dir = TempDir::new().expect("tempdir");
                let log_dir = dir.path().join("log");
                Node {
                    id,
                    roles,
                    client,
                    controller,
                    client_addr,
                    controller_addr,
                    dir,
                    log_dir,
                }
            },
        )
        .collect();
    let controllers: Vec<(NodeId, SocketAddr)> = nodes[..controller_specs.len()]
        .iter()
        .map(|node| (node.id, node.controller_addr))
        .collect();
    for (index, node) in nodes.iter().enumerate() {
        let mut flags = match quorum {
            Quorum::Dynamic if index == 0 => vec![
                "--standalone".to_owned(),
                "--controller-listener".to_owned(),
                node.controller_addr.to_string(),
            ],
            Quorum::Dynamic | Quorum::Static => vec![],
        };
        if matches!(eligible_leader_replicas, EligibleLeaderReplicas::Disabled) {
            flags.extend(["--feature".to_owned(), format!("{ELR_VERSION_FEATURE}=0")]);
        }
        format(&node.log_dir, node.id, &flags).await;
    }

    let brokers = nodes.split_off(controller_specs.len());
    let started_controllers = start_all(
        nodes,
        ActivationSetup {
            quorum,
            controllers: &controllers,
            min_insync_replicas,
        },
    )
    .await;
    let leader_id = started_controllers[0]
        .1
        .wait_until_controller_leader()
        .await;
    let started_brokers = start_all(
        brokers,
        ActivationSetup {
            quorum,
            controllers: &controllers,
            min_insync_replicas,
        },
    )
    .await;
    let leader = started_controllers
        .iter()
        .find(|(id, ..)| *id == leader_id)
        .map(|(_, handle, _)| handle)
        .expect("the leader is one of the controllers");
    let broker_role_count = controller_specs
        .iter()
        .chain(broker_specs)
        .filter(|(_, roles)| roles.contains(&NodeRole::Broker))
        .count();
    leader
        .wait_until_brokers_registered(broker_role_count)
        .await;

    let committed = committed_batches(leader.controller_addr()).await;
    check!(
        seeding(&committed)
            == (
                true,
                Some(Batch::Metadata(activation.clone())),
                vec![activation]
            ),
        "{what}: {committed:#?}"
    );
    // The wait fails the test when a node's image does not take the feature
    // levels and the cluster-level `min.insync.replicas` of the activation.
    let expected_min_isr = cluster_min_isr.map(|value| value.to_string());
    for (_, handle, _) in started_controllers.iter().chain(&started_brokers) {
        handle
            .wait_for_image(|image| {
                *image.finalized_features() == expected_levels
                    && image
                        .default_broker_config()
                        .and_then(|configs| configs.get(MIN_INSYNC_REPLICAS))
                        == expected_min_isr.as_ref()
            })
            .await;
    }
    let broker = started_brokers
        .iter()
        .chain(&started_controllers)
        .map(|(_, handle, _)| handle)
        .find(|handle| handle.listen_addr().port() != 0)
        .expect("a node with the broker role");
    check!(
        describe_cluster_min_insync_replicas(broker.listen_addr()).await
            == kafkas_cluster_min_insync_replicas(cluster_min_isr),
        "{what}"
    );

    for (_, handle, dir) in started_brokers.into_iter().chain(started_controllers) {
        handle.shutdown().await;
        drop(dir);
    }
}
