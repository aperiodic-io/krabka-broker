//! Cross-broker transaction-marker fan-out over a *secured* inter-broker
//! listener.
//!
//! `EndTxn` fans `WriteTxnMarkers` out to every partition leader in the
//! transaction. When a *remote* broker leads a partition, the marker travels
//! over the inter-broker listener. The fan-out must run the same TLS and SASL
//! handshakes that the listener demands. It dials through the shared
//! `InterBrokerClient`, which carries the inter-broker TLS connector and the
//! SASL credentials. It does not use a bare one-shot
//! `krabka_client_core::Client`, which carries neither and can only reach a
//! PLAINTEXT inter-broker listener.
//!
//! The test boots a two-broker cluster whose inter-broker listener is
//! `SASL_PLAINTEXT`. It creates a topic whose two partitions round-robin onto
//! different brokers: P0 → node 1, P1 → node 2. It then drives the transaction
//! control plane *directly* with SASL-authenticated low-level clients:
//! `FindCoordinator → InitProducerId → AddPartitionsToTxn → EndTxn`. The
//! partition added to the transaction is deliberately the one led by the broker
//! that is *not* the coordinator, so `EndTxn` must fan a marker to a remote
//! leader over the SASL listener. With the earlier one-shot dial that handshake
//! fails and `EndTxn` returns a retriable error. With the pooled
//! `InterBrokerClient` it returns `NONE`.
//!
//! The test drives the control plane by hand and not with the high-level
//! `Producer`. This avoids a separate, earlier gap where the producer's
//! transaction-coordinator connection ignores client security. The test stays
//! focused on the broker-to-broker fan-out that this change fixes.
//!
//! Windows-gated like the other multi-node transactional tests, because
//! openraft and tokio scheduling race on the hosted Windows runner.

use {
    crate::support::{
        acl::use_simple_acl_authorizer,
        controller_voters,
        discovery::named_topic_metadata,
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        shutdown_cluster,
        topics::{creatable_topic, create_topic_request},
        transactions::{ProducerIdentity, end_transaction_request, init_producer_request},
    },
    assert2::assert,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle,
        config::{InterBrokerCredentials, ListenerSpec},
    },
    krabka_client_core::{
        Client,
        security::{ClientSecurity, SaslCredentials},
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            add_partitions_to_txn_request::{
                AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction,
            },
            common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
            find_coordinator_request::FindCoordinatorRequest,
            init_producer_id_response::InitProducerIdResponse,
            produce_request::ProduceRequest,
            produce_response::PartitionProduceResponse,
        },
        records::{Attributes, RecordBatch},
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

mod support;

const USER: &str = "broker";
const PASS: &str = "secret";
const IB_LISTENER: &str = "SASL_PLAINTEXT";
const TID: &str = "remote-fanout-tid";
const TOPIC: &str = "t";

/// A single `SASL_PLAINTEXT` data listener, bound and advertised at `addr`.
///
/// This listener is also the inter-broker listener. The advertised port must be
/// concrete: self-registration records `ListenerSpec::advertised` *before* the
/// listener is bound, so a `:0` would register port 0 and break the
/// inter-broker dial.
fn sasl_listener(addr: SocketAddr) -> Vec<ListenerSpec> {
    vec![crate::support::listeners::listener(
        IB_LISTENER,
        addr,
        ListenerProtocol::SaslPlaintext,
    )]
}

/// Add the `SASL_PLAINTEXT` inter-broker listener and its `SASL/PLAIN`
/// credentials to a base config whose listener binds `addr`.
fn apply_sasl(cfg: &mut BrokerConfig, addr: SocketAddr) {
    cfg.listen_addr = addr;
    cfg.advertised_listener = addr.to_string();
    cfg.listeners = sasl_listener(addr);
    cfg.inter_broker_listener_name = IB_LISTENER.to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert(USER.to_string(), PASS.to_string());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: USER.to_string(),
        password: PASS.to_string(),
    });
}

/// Client-side `SASL/PLAIN` so the test's low-level clients authenticate
/// against the brokers' `SASL_PLAINTEXT` listener.
fn client_security((username, password): (&str, &str)) -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: username.to_string(),
            password: password.to_string(),
        }),
        sasl_host: None,
    }
}

/// Open a client to `addr` that authenticates as `credentials`.
async fn sasl_client_as(addr: &str, credentials: (&str, &str)) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("krabka-txn-fanout-test")
        .security(client_security(credentials))
        .build()
        .await
        .expect("sasl client connect")
}

/// Open a client to `addr` that authenticates as the broker user.
async fn sasl_client(addr: &str) -> Client { sasl_client_as(addr, (USER, PASS)).await }

/// Boot a two-broker KIP-853 auto-join cluster on a `SASL_PLAINTEXT` listener.
///
/// The same listener carries data and inter-broker traffic. This function
/// mirrors the concrete-port handling in `support::start_n_node`. The marker
/// fan-out resolves the leader's advertised inter-broker endpoint, which must be
/// a real reachable port.
async fn start_two_sasl(
    configure: fn(&mut BrokerConfig),
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    support::init_tracing();

    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(2).await;

    // KIP-595 static-quorum bootstrap: both brokers boot in `Bootstrap` mode
    // with the same static 2-voter set (concrete controller ports) and elect
    // among themselves over the SASL controller wire — no KIP-853 dynamic voter
    // reconfiguration or auto-join.
    let voters: Vec<(u64, SocketAddr)> = vec![(1, controller_addrs[0]), (2, controller_addrs[1])];

    let dir0 = TempDir::new().unwrap();
    let mut cfg0 = BrokerConfig::for_tests(dir0.path().to_path_buf());
    cfg0.broker_id = 1;
    cfg0.node_id = krabka_broker::NodeId(1);
    cfg0.directory_id = uuid::Uuid::from_u128(1);
    cfg0.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg0.controller_listen_addr = controller_addrs[0];
    cfg0.controller_quorum_voters = controller_voters(&voters);
    cfg0.auto_join = false;
    cfg0.bootstrap_servers = vec![];
    apply_sasl(&mut cfg0, client_addrs[0]);
    configure(&mut cfg0);

    let dir1 = TempDir::new().unwrap();
    let mut cfg1 = BrokerConfig::for_tests(dir1.path().to_path_buf());
    cfg1.broker_id = 2;
    cfg1.node_id = krabka_broker::NodeId(2);
    cfg1.directory_id = uuid::Uuid::from_u128(2);
    cfg1.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg1.controller_listen_addr = controller_addrs[1];
    cfg1.controller_quorum_voters = controller_voters(&voters);
    cfg1.auto_join = false;
    cfg1.bootstrap_servers = vec![];
    apply_sasl(&mut cfg1, client_addrs[1]);
    configure(&mut cfg1);

    // Pull held listeners before the spawns so each spawn owns its pair.
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (data0, controller0) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (data1, controller1) = (data_ls.next().unwrap(), ctrl_ls.next().unwrap());

    // Start both concurrently: `Broker::start` blocks until a leader is
    // committed, which needs a voter majority up, so a sequential
    // `start().await` on broker0 alone would deadlock.
    let cfg0_for_spawn = cfg0.clone();
    let cfg1_for_spawn = cfg1.clone();
    let join0 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg0_for_spawn, Some(controller0), Some(data0)).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_for_spawn, Some(controller1), Some(data1)).await
    });
    let broker0 = join0
        .await
        .map_err(|e| BrokerError::Startup(format!("broker0 task panicked: {e}")))??;
    let broker1 = join1
        .await
        .map_err(|e| BrokerError::Startup(format!("broker1 task panicked: {e}")))??;

    // Block until the static set converges on a 2-voter quorum.
    // `voter_count_for_test` reads the committed metadata image's voter set, so
    // `wait_for_image` observes the same convergence event-driven (image watch
    // channel) rather than polling on a fixed cadence. Both nodes are static
    // voters, so broker0's image reflects the full set once the quorum forms.
    broker0.wait_for_image(|img| img.voters().len() >= 2).await;

    Ok(vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)])
}

/// Retry the cluster boot a few times.
///
/// Short raft timings sometimes split-vote on busy runners. This function
/// mirrors `support::start_n_node_with_retry`.
async fn start_two_sasl_with_retry(
    configure: fn(&mut BrokerConfig),
) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last = None;
    for attempt in 1..=3 {
        match start_two_sasl(configure).await {
            Ok(c) => return c,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "SASL cluster boot failed; retrying");
                last = Some(e);
                // intentional: backoff before re-booting the whole cluster after
                // a failed boot attempt (lets stray raft timings settle) — not a
                // wait on any observable krabka broker/image/metric state.
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("SASL cluster boot failed after 3 attempts: {last:?}");
}

/// Wait until every broker's metadata image lists both peers, so `CreateTopics`
/// round-robins partition leadership across the two brokers.
async fn wait_both_registered(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    // Each broker's `broker_count` reads its committed metadata image;
    // `wait_until_brokers_registered` observes that same `img.brokers().count()`
    // via the image watch channel, so wait per-broker instead of polling.
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(2).await;
    }
}

/// Resolve `TOPIC`'s partition → leader-node map with Metadata.
///
/// The function waits until both partitions have an elected leader in
/// `handle`'s metadata image. The broker serves Metadata to the connected admin
/// client from that same image.
/// Waits until every broker's image holds both partitions of `topic`. The
/// coordinator answers the leader's `AddPartitionsToTxn` with
/// `UNKNOWN_TOPIC_OR_PARTITION` until its own image holds the partition, as
/// Kafka's `handleAddPartitionsToTxnRequest` does.
async fn wait_all_hold_topic<'a>(handles: impl Iterator<Item = &'a BrokerHandle>, topic: &str) {
    for handle in handles {
        handle
            .wait_for_image(|img| (0..2).all(|p| img.partition(topic, p).is_some()))
            .await;
    }
}

async fn partition_leaders(client: &Client, handle: &BrokerHandle, topic: &str) -> Vec<(i32, i32)> {
    // A non-zero `leader` in the image is exactly the wire condition the old
    // loop polled for (`leader_id >= 0`); await both partitions' elections
    // event-driven via the image watch channel, then take one Metadata snapshot.
    handle
        .wait_for_image(|img| {
            (0..2).all(|p| img.partition(topic, p).is_some_and(|pr| pr.leader != 0))
        })
        .await;
    let resp = client
        .send(named_topic_metadata(topic.to_string()))
        .await
        .expect("metadata");
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .expect("topic present in metadata after leader election");
    topic
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.leader_id))
        .collect()
}

/// Find the transaction coordinator for `transactional_id`: its node id, host
/// and port.
///
/// The transaction coordinator partition (`__transaction_state[hash(id)]`) is
/// auto-created and its leader elected lazily on first access, so the first
/// `FindCoordinator` can race ahead of that election and briefly return
/// `COORDINATOR_NOT_AVAILABLE`. The function retries until a deadline, so a
/// coordinator that never becomes available still fails the test.
async fn find_coordinator(client: &Client, transactional_id: &str) -> (i32, String, i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let fc = client
            .send(FindCoordinatorRequest {
                key: transactional_id.into(),
                key_type: 1, // TRANSACTION
                coordinator_keys: vec![transactional_id.into()],
                ..Default::default()
            })
            .await
            .expect("find coordinator");
        let (node, host, port) = fc.coordinators.first().map_or_else(
            || (fc.node_id, fc.host.clone(), fc.port),
            |c| (c.node_id, c.host.clone(), c.port),
        );
        if node >= 0 {
            return (node, host, port);
        }
        assert!(
            Instant::now() <= deadline,
            "txn coordinator never became available: {fc:?}"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_marker_fanout_to_remote_leader_over_sasl() {
    let cluster = start_two_sasl_with_retry(|_| {}).await;
    wait_both_registered(&cluster).await;

    let bootstrap = cluster[0].1.listen_addr.to_string();
    let admin = sasl_client(&bootstrap).await;

    // Topic with 2 partitions, RF=1. Round-robin places P0 on node 1 and P1 on
    // node 2.
    let cr = admin
        .send(create_topic_request(
            creatable_topic().name((TOPIC).into()).partitions(2).call(),
        ))
        .await
        .expect("create topic");
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic: error_code={}",
        cr.topics[0].error_code
    );

    let leaders = partition_leaders(&admin, &cluster[0].0, TOPIC).await;
    let distinct: std::collections::BTreeSet<i32> = leaders.iter().map(|&(_, l)| l).collect();
    assert!(
        distinct.len() == 2,
        "expected partition leadership split across both brokers, got {leaders:?}"
    );

    let (coord_node, coord_host, coord_port) = find_coordinator(&admin, TID).await;

    // Pick the partition led by the broker that is NOT the coordinator, so
    // EndTxn must fan a marker to a *remote* leader over the SASL listener.
    let remote_partition = leaders
        .iter()
        .find(|&&(_, leader)| leader != coord_node)
        .map(|&(p, _)| p)
        .expect("a partition led by a non-coordinator broker");

    // Connect to the coordinator and run the transaction control plane.
    let coord = sasl_client(&format!("{coord_host}:{coord_port}")).await;

    let init = coord
        .send(
            init_producer_request()
                .maybe_transactional_id(Some(TID.into()))
                .call(),
        )
        .await
        .expect("init producer id");
    assert!(init.error_code == 0, "InitProducerId failed: {init:?}");
    let (pid, epoch) = (init.producer_id, init.producer_epoch);

    // AddPartitionsToTxn for the remote-led partition. Fill both the v4+
    // `transactions` array and the v3-and-below flat fields so the request is
    // correct whatever version negotiates.
    let topic = AddPartitionsToTxnTopic {
        name: TOPIC.into(),
        partitions: vec![remote_partition],
        ..Default::default()
    };
    let add = coord
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![topic],
            ..Default::default()
        })
        .await
        .expect("add partitions to txn");
    assert!(
        add.error_code == 0,
        "AddPartitionsToTxn top-level error: {add:?}"
    );

    // EndTxn(commit): the coordinator fans a WriteTxnMarkers to the remote
    // partition's leader over the SASL_PLAINTEXT inter-broker listener. This is
    // the path the fix repairs — the pre-fix one-shot client could not
    // authenticate and EndTxn would surface a retriable UNKNOWN_SERVER_ERROR.
    let end = coord
        .send(
            end_transaction_request()
                .transactional_id(TID)
                .producer(ProducerIdentity::from_wire((pid, epoch)))
                .call(),
        )
        .await
        .expect("end txn");
    assert!(
        end.error_code == 0,
        "EndTxn must succeed: remote marker fan-out over SASL inter-broker (error_code={})",
        end.error_code
    );

    admin.close();
    coord.close();
    shutdown_cluster(cluster).await;
}

const ADMIN_USER: &str = "admin";
const ADMIN_PASS: &str = "admin-secret";
const CLIENT_USER: &str = "client";
const CLIENT_PASS: &str = "client-secret";

/// Run the brokers with an ACL authorizer. `admin` and `ANONYMOUS` are the
/// super users. The broker user and the client user get their ACLs from the
/// test.
///
/// The controller listener of this cluster is `PLAINTEXT`, so every peer on it
/// is `ANONYMOUS`, and each controller request is authorized for that
/// principal (#684). Kafka's answer for that setup is
/// `super.users=User:ANONYMOUS`. Without it the raft peers refuse each other
/// and the cluster never registers.
fn apply_acls(cfg: &mut BrokerConfig) {
    for (user, pass) in [(ADMIN_USER, ADMIN_PASS), (CLIENT_USER, CLIENT_PASS)] {
        cfg.plain_credentials
            .insert(user.to_string(), pass.to_string());
    }
    cfg.super_users = [ADMIN_USER.to_string(), "ANONYMOUS".to_string()]
        .into_iter()
        .collect();
    use_simple_acl_authorizer(cfg);
}

// An Allow ACL on a literal resource.
use crate::support::{acl::literal_allow_acl as allow, wire::At};

/// One transactional `Produce` whose partition leader is not the transaction
/// coordinator.
struct VerificationCase {
    name: &'static str,
    /// `Produce` v11 only verifies the partition, so the client adds it first
    /// with its own `AddPartitionsToTxn` (v3). `Produce` v12 lets the leader
    /// add the partition.
    produce_version: i16,
}

async fn ready_leader_addr(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    topic: &str,
    partition: i32,
    leader: i32,
) -> String {
    let (handle, cfg, _) = cluster
        .iter()
        .find(|(_, cfg, _)| i64::from(leader) == i64::try_from(cfg.node_id.0).unwrap())
        .expect("the leader is in the cluster");
    // Metadata can precede the local epoch and ISR installation.
    // Wait for the selected leader's Produce gate before sending the batch.
    handle
        .wait_until_local_partition_leader(topic, partition, cfg.node_id)
        .await;
    cfg.listen_addr.to_string()
}

/// KIP-890 verification of a transactional `Produce` when the transaction
/// coordinator is on another broker, in a cluster with ACLs.
///
/// The partition leader sends `AddPartitionsToTxn` v4 or later to the
/// coordinator as the broker principal. Kafka's
/// `KafkaApis.handleAddPartitionsToTxnRequest` authorizes that request with
/// `ClusterAction` on the cluster alone, and checks no transactional id or
/// topic ACL. The broker user here holds only `ClusterAction`, so the produce
/// succeeds only if the coordinator follows Kafka. The commit then fans the
/// marker out to the remote leader as the same broker principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_coordinator_verifies_a_produce_for_a_broker_with_only_cluster_action() {
    let cluster = start_two_sasl_with_retry(apply_acls).await;
    wait_both_registered(&cluster).await;

    let grants = [
        allow()
            .resource_type(ResourceType::Cluster)
            .resource_name("kafka-cluster")
            .principal(&format!("User:{USER}"))
            .operation(AclOperation::ClusterAction)
            .call(),
        allow()
            .resource_type(ResourceType::TransactionalId)
            .resource_name("*")
            .principal(&format!("User:{CLIENT_USER}"))
            .operation(AclOperation::Write)
            .call(),
        allow()
            .resource_name("*")
            .principal(&format!("User:{CLIENT_USER}"))
            .operation(AclOperation::Write)
            .call(),
    ];
    for grant in grants {
        cluster[0]
            .0
            .submit_metadata_record_for_test(grant)
            .await
            .expect("seed ACL");
    }
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|img| {
                img.matching_acls(ResourceType::Cluster, "kafka-cluster")
                    .count()
                    == 1
                    && img
                        .matching_acls(ResourceType::TransactionalId, "any")
                        .count()
                        == 1
                    && img.matching_acls(ResourceType::Topic, "any").count() == 1
            })
            .await;
    }

    let admin = sasl_client_as(
        &cluster[0].1.listen_addr.to_string(),
        (ADMIN_USER, ADMIN_PASS),
    )
    .await;
    let client_bootstrap = sasl_client_as(
        &cluster[0].1.listen_addr.to_string(),
        (CLIENT_USER, CLIENT_PASS),
    )
    .await;

    let cases = [
        VerificationCase {
            name: "verify-only-produce-v11",
            produce_version: 11,
        },
        VerificationCase {
            name: "add-produce-v12",
            produce_version: 12,
        },
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let created = admin
            .send(create_topic_request(
                creatable_topic()
                    .name((case.name).into())
                    .partitions(2)
                    .call(),
            ))
            .await
            .expect("create topic");
        assert!(
            created.topics[0].error_code == 0,
            "{}: {created:?}",
            case.name
        );
        let leaders = partition_leaders(&admin, &cluster[0].0, case.name).await;
        wait_all_hold_topic(cluster.iter().map(|(handle, _, _)| handle), case.name).await;

        let (coordinator, coordinator_host, coordinator_port) =
            find_coordinator(&client_bootstrap, case.name).await;
        let (partition, leader) = leaders
            .iter()
            .copied()
            .find(|&(_, leader)| leader != coordinator)
            .expect("a partition led by a broker that is not the coordinator");
        let leader_addr = ready_leader_addr(&cluster, case.name, partition, leader).await;
        let to_coordinator = sasl_client_as(
            &format!("{coordinator_host}:{coordinator_port}"),
            (CLIENT_USER, CLIENT_PASS),
        )
        .await;
        let to_leader = sasl_client_as(&leader_addr, (CLIENT_USER, CLIENT_PASS)).await;

        let init = init_producer(&to_coordinator, case.name).await;
        let topic = AddPartitionsToTxnTopic {
            name: case.name.into(),
            partitions: vec![partition],
            ..Default::default()
        };
        if case.produce_version < 12 {
            let added = to_coordinator
                .send(At::<_, 3>(AddPartitionsToTxnRequest {
                    v3_and_below_transactional_id: case.name.into(),
                    v3_and_below_producer_id: init.producer_id,
                    v3_and_below_producer_epoch: init.producer_epoch,
                    v3_and_below_topics: vec![topic],
                    ..Default::default()
                }))
                .await
                .expect("add partitions to txn");
            assert!(
                added.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code
                    == 0,
                "{}: {added:?}",
                case.name
            );
        }

        let batch = RecordBatch {
            attributes: Attributes::default().with_transactional(true),
            producer_id: init.producer_id,
            producer_epoch: init.producer_epoch,
            base_sequence: 0,
            last_offset_delta: 0,
            max_timestamp: 1,
            ..batch_from_records(vec![value_record(0, Some(bytes::Bytes::from_static(b"v")))])
        };
        let request = ProduceRequest {
            transactional_id: Some(case.name.into()),
            ..batch_request(batch, SinglePartitionProduceSetup {
                topic: (case.name).into(),
                partition: krabka_ids::PartitionIndex(partition),
                ..SinglePartitionProduceSetup::replicated()
            })
        };
        let produced = if case.produce_version < 12 {
            to_leader.send(At::<_, 11>(request)).await
        } else {
            to_leader.send(At::<_, 12>(request)).await
        }
        .expect("produce");

        let ended = to_coordinator
            .send(
                end_transaction_request()
                    .transactional_id(case.name)
                    .producer(ProducerIdentity::from_wire((
                        init.producer_id,
                        init.producer_epoch,
                    )))
                    .call(),
            )
            .await
            .expect("end txn");

        actual.push((
            case.name,
            produced.responses[0].partition_responses[0].clone(),
            ended.error_code,
        ));
        expected.push((
            case.name,
            PartitionProduceResponse {
                index: partition,
                error_code: 0,
                base_offset: 0,
                log_append_time_ms: -1,
                log_start_offset: 0,
                ..Default::default()
            },
            0,
        ));
        to_coordinator.close();
        to_leader.close();
    }

    admin.close();
    client_bootstrap.close();
    shutdown_cluster(cluster).await;
    assert!(actual == expected);
}

/// `InitProducerId` for `transactional_id`, retried while the coordinator
/// loads its partition.
async fn init_producer(client: &Client, transactional_id: &str) -> InitProducerIdResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let init = client
            .send(
                init_producer_request()
                    .maybe_transactional_id(Some(transactional_id.into()))
                    .call(),
            )
            .await
            .expect("init producer id");
        // COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR and
        // CONCURRENT_TRANSACTIONS mean the coordinator is still loading.
        if !matches!(init.error_code, 15 | 16 | 51) || Instant::now() > deadline {
            assert!(init.error_code == 0, "InitProducerId: {init:?}");
            return init;
        }
        // intentional: coordinator load has no awaiter reachable from this
        // client; the coordinator answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
