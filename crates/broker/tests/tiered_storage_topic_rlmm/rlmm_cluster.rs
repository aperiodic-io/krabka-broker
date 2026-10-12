//! Broker boot and client-connection helpers for the topic-backed
//! `RemoteLogMetadataManager` suite.
//!
//! Every test in this binary needs a single broker whose `RlmmKind` is
//! `TopicBacked` and whose bootstrap address is the broker's own advertised
//! listener. This module holds the two flavours of that boot, PLAINTEXT and
//! `SASL_PLAINTEXT`, the client builders that dial them, and the two waits the
//! tests gate on: activation of the swapped-in manager and propagation of a
//! tiered-storage topic config into the partition's `LogConfig`.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::{
    BrokerConfig, BrokerHandle, KafkaRlmmConfig, RemoteStorageBackend, RlmmKind,
    config::{InterBrokerCredentials, ListenerSpec},
};
use krabka_client_core::Client;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::support;

krabka_macros::bound_start_fixture!(config, bare_config, ::krabka_broker);

/// Boot a single broker with the `Local` tiered-storage backend and the
/// topic-backed RLMM pointed at its own loopback listener. Returns the
/// handle plus the log + remote tempdirs. The caller keeps them alive.
pub(crate) async fn start_broker_with_topic_rlmm() -> (BrokerHandle, TempDir, TempDir) {
    start_configured_topic_rlmm(|_, _| {}).await
}

/// Keep held listeners and tempdirs alive while applying a scenario-specific configuration.
pub(crate) async fn start_configured_topic_rlmm(
    configure: impl FnOnce(&mut BrokerConfig, &std::path::Path),
) -> (BrokerHandle, TempDir, TempDir) {
    support::init_tracing();

    // Pin a loopback port so the RLMM bootstrap can dial the broker's own
    // listener: `KafkaRlmmConfig::bootstrap` is resolved before the
    // listener binds, so an ephemeral `:0` wouldn't be knowable in time.
    // Held listeners eliminate the bind-and-drop TOCTOU race under parallel
    // nextest (`AddrInUse` flakes).
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];

    let log_dir = TempDir::new().expect("log tempdir");
    let remote_dir = TempDir::new().expect("remote tempdir");

    let mut cfg = bare_config(log_dir.path(), listen, controller_addrs[0]);
    cfg.remote_log_manager_interval = krabka_units::secs(1);
    cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().into(),
    });
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: format!("127.0.0.1:{}", listen.port()),
        num_partitions: 1,
        replication: 1,
        min_isr: 1,
        snapshot_interval: krabka_units::hours(1),
        snapshot_dir: log_dir.path().join("remote-log-metadata"),
        security: None,
        ..KafkaRlmmConfig::default()
    });

    configure(&mut cfg, log_dir.path());

    let broker =
        support::start_first_held(cfg, client_listeners, controller_listeners, "broker start")
            .await;
    (broker, log_dir, remote_dir)
}

pub(crate) async fn await_tiered_config(broker: &BrokerHandle, topic: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if broker
            .partition_log_config_for_test(topic, 0)
            .is_some_and(|config| {
                config.remote_storage_enable
                    && config.segment_size == krabka_units::kibibytes(1)
                    && config.local_retention_size == Some(krabka_units::bytes(1))
            })
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "tiered-storage topic config never propagated; saw {:?}",
            broker.partition_log_config_for_test(topic, 0)
        );
        tokio::task::yield_now().await;
    }
}

pub(crate) async fn build_client(broker: &BrokerHandle) -> Client {
    build_client_secured(broker, None).await
}

/// Build a test client, optionally negotiating TLS/SASL. `None` is the
/// plaintext path used by the loopback tests. `Some(..)` authenticates
/// against a SASL listener.
pub(crate) async fn build_client_secured(
    broker: &BrokerHandle,
    security: Option<krabka_client_core::security::ClientSecurity>,
) -> Client {
    Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("tiered-topic-rlmm-test")
        .maybe_security(security)
        .build()
        .await
        .expect("client build")
}

/// Wait for the topic-backed remote-log-metadata-manager bootstrap to swap the topic-backed manager in. The
/// `tiered_storage_rlmm_topic_backed` gauge flips to 1 to signal the swap.
pub(crate) async fn await_activation(broker: &BrokerHandle) {
    broker
        .wait_for_metrics("rlmm topic-backed", |m| {
            m.tiered_storage_rlmm_topic_backed.get() == 1
        })
        .await;
}

/// Boot a single broker whose only listener, which is also the inter-broker
/// listener, is `SASL_PLAINTEXT/PLAIN`. The topic-backed RLMM points at it. The
/// RLMM authenticates as the inter-broker PLAIN principal.
pub(crate) async fn start_sasl_broker_with_topic_rlmm() -> (BrokerHandle, TempDir, TempDir) {
    start_configured_topic_rlmm(|cfg, _| {
        cfg.listeners = vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: cfg.listen_addr,
            advertised: format!("127.0.0.1:{}", cfg.listen_addr.port()),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        }];
        cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
        cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        cfg.plain_credentials
            .insert("rlmm".to_string(), "rlmm-secret".to_string());
        cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: "rlmm".to_string(),
            password: "rlmm-secret".to_string(),
        });
    })
    .await
}
