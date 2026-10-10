//! `InterBrokerClient` wired into the replicator and the heartbeat.
//!
//! A two-broker cluster whose inter-broker listener is `SASL_PLAINTEXT`
//! authenticates its outbound fetch and heartbeat traffic and replicates
//! records end-to-end.
//!
//! Gated to non-Windows (openraft `debug_assert!` race on the hosted Windows
//! runner -- the same gate as `tests/replication.rs`).

use std::net::SocketAddr;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::InterBrokerCredentials};
use krabka_protocol::{owned::create_topics_request::CreateTopicsRequest, records::RecordBatch};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::{
    harness::admin_plain_password,
    support::{
        client::connect_client,
        records::{batch_from_records, value_record},
    },
};

/// Reserve `n` ephemeral loopback ports and keep their listeners open.
async fn reserve_listeners(n: usize) -> (Vec<SocketAddr>, Vec<tokio::net::TcpListener>) {
    let mut addrs = Vec::with_capacity(n);
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap());
        listeners.push(l);
    }
    (addrs, listeners)
}

/// Build a SASL-enabled broker config with two listeners.
///
/// The first listener is a PLAINTEXT data-plane listener on
/// `listen_addr`. The test clients use it, because they do not speak SASL
/// yet. The second listener is a `SASL_PLAINTEXT` inter-broker listener.
/// The replicator and the heartbeat use it against the peer broker.
const DEFAULT_SASL_ENDPOINTS: [SocketAddr; 1] = [SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    0,
))];

#[derive(krabka_macros::FieldDefaults)]
struct SaslNodeSetup<'a> {
    cluster: crate::support::ClusterNodeSetup<'a>,
    #[default(&DEFAULT_SASL_ENDPOINTS)]
    sasl_addrs: &'a [SocketAddr],
}

fn sasl_two_listener_config(log_dir: &std::path::Path, setup: SaslNodeSetup<'_>) -> BrokerConfig {
    let listen = setup.cluster.client_addrs[setup.cluster.index.0];
    let sasl = setup.sasl_addrs[setup.cluster.index.0];
    let mut cfg = crate::support::broker_config(log_dir, setup.cluster);
    cfg.listeners = vec![
        crate::support::listeners::listener("PLAINTEXT", listen, ListenerProtocol::Plaintext),
        crate::support::listeners::listener(
            "SASL_PLAINTEXT",
            sasl,
            ListenerProtocol::SaslPlaintext,
        ),
    ];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), admin_plain_password());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "broker".to_string(),
        password: admin_plain_password(),
    });
    cfg
}

/// Start a 2-broker cluster whose inter-broker listener is
/// `SASL_PLAINTEXT`.
///
/// This helper is a copy of `support::start_n_node`, but it uses the
/// two-listener config above. It returns `(handle, config, tempdir)`
/// triples in broker id order.
async fn start_two_node_sasl() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    crate::support::init_tracing_with("warn");

    let (plaintext_addrs, plaintext_listeners) = reserve_listeners(2).await;
    let (sasl_addrs, sasl_listeners) = reserve_listeners(2).await;
    let (controller_addrs, controller_listeners) = reserve_listeners(2).await;
    let voters: Vec<(u64, SocketAddr)> = (0..2_u64)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    let dir0 = TempDir::new().unwrap();
    let cfg0 = sasl_two_listener_config(
        dir0.path(),
        SaslNodeSetup {
            cluster: crate::support::ClusterNodeSetup {
                client_addrs: &plaintext_addrs,
                controller_addrs: &controller_addrs,
                voters: crate::support::controller_voters(&voters),
                ..Default::default()
            },
            sasl_addrs: &sasl_addrs,
        },
    );
    let dir1 = TempDir::new().unwrap();
    let cfg1 = sasl_two_listener_config(
        dir1.path(),
        SaslNodeSetup {
            cluster: crate::support::ClusterNodeSetup {
                index: crate::support::NodeIndex(1),
                client_addrs: &plaintext_addrs,
                controller_addrs: &controller_addrs,
                voters: crate::support::controller_voters(&voters),
                ..Default::default()
            },
            sasl_addrs: &sasl_addrs,
        },
    );
    // KIP-595 static-quorum bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the SASL controller
    // wire — no add_learner / change_membership (KIP-853 dynamic voter reconfiguration). Start
    // them concurrently: `Broker::start` blocks until a leader is committed,
    // which needs a voter majority up, so a sequential `start().await` on
    // broker0 alone would deadlock.
    let mut listeners = plaintext_listeners
        .into_iter()
        .zip(sasl_listeners)
        .zip(controller_listeners);
    let ((plaintext0, sasl0), controller0) = listeners.next().unwrap();
    let ((plaintext1, sasl1), controller1) = listeners.next().unwrap();
    let cfg0_for_spawn = cfg0.clone();
    let cfg1_for_spawn = cfg1.clone();
    let join0 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg0_for_spawn, Some(controller0), [plaintext0, sasl0]).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_for_spawn, Some(controller1), [plaintext1, sasl1]).await
    });
    let broker0 = join0.await.expect("join0 spawn").expect("broker0 start");
    let broker1 = join1.await.expect("join1 spawn").expect("broker1 start");
    vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)]
}

/// Start two brokers with a `SASL_PLAINTEXT` inter-broker listener,
/// create a topic with rf=2, produce, and check that the follower
/// converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_broker_sasl_plaintext_replication() {
    let cluster = start_two_node_sasl().await;

    // Wait for both brokers to register in each other's image.
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(2).await;
    }

    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(leader_addr.clone(), None).await;
    let resp = admin
        .send(CreateTopicsRequest {
            // Node 1, which the produce below goes to, leads the partition.
            topics: vec![crate::support::topic_on("sasl-repl", &[&[1, 2]])],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);
    let topic_id = resp.topics[0].topic_id;

    // Wait for the topic to propagate to every broker's image.
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("sasl-repl", 0).await;
    }

    // Produce 10 records to the leader.
    let producer = connect_client(leader_addr, None).await;
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 9,
        ..batch_from_records(
            (0..10)
                .map(|i| value_record(i, Some(bytes::Bytes::from(format!("v{i}")))))
                .collect(),
        )
    };
    let prod = producer
        .send(crate::support::produce::batch_request(
            batch,
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("sasl-repl").into(),
                topic_id,
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        ))
        .await
        .unwrap();
    assert!(prod.responses[0].partition_responses[0].error_code == 0);

    // Wait until every broker's local log reaches >= 10. The SASL
    // inter-broker handshake on each follower-fetch round trip is the
    // critical path here — a misconfigured replicator would never
    // commit a record and this awaiter would time out.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("sasl-repl", 0, 10).await;
    }

    crate::support::shutdown_cluster(cluster).await;
}
