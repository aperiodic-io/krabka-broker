//! Shared fixtures for the transactional suite: broker boot, topic creation,
//! transactional-producer initialisation, and record construction.
//!
//! `init_transaction` drives `FindCoordinator` and then retries
//! `InitProducerId` until the transaction coordinator is loaded, so a test does
//! not have to encode that readiness race itself.
//!
//! No broker creates `__transaction_state` or `__consumer_offsets` when it
//! starts. The boot helpers bring both coordinators up before they return.
//! The krabka producer does not retry `COORDINATOR_NOT_AVAILABLE` from
//! `FindCoordinator`, which Kafka's `TransactionManager` does.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_core::security::{ClientSecurity, SaslCredentials};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::{
    support,
    support::{client::connect_client, transactions::new_producer_request},
};

pub async fn boot_single() -> (BrokerHandle, String, TempDir) {
    boot_single_with(|_| {}).await
}

/// [`boot_single`] under Kafka's `unstable.api.versions.enable`, for the
/// cases that drive Kafka trunk's `TxnOffsetCommit` v6 (KIP-1319).
pub async fn boot_single_trunk() -> (BrokerHandle, String, TempDir) {
    boot_single_with(|config| {
        config.features.unstable_api_versions =
            krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    })
    .await
}

async fn boot_single_with(configure: fn(&mut BrokerConfig)) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut config);
    let broker = Broker::start(config).await.unwrap();
    wait_until_coordinators_ready(&broker).await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Creates and loads `__transaction_state` and `__consumer_offsets`, as the
/// first client lookup of each does.
async fn wait_until_coordinators_ready(broker: &BrokerHandle) {
    broker.wait_until_transaction_coordinator_ready().await;
    broker.wait_until_group_coordinator_ready().await;
}

pub async fn create_topic(bootstrap: &str, name: &str) {
    create_topic_with_configs(bootstrap, name, Vec::new()).await;
}

pub async fn create_topic_with_segment_bytes(bootstrap: &str, name: &str, bytes: u64) {
    create_topic_with_configs(
        bootstrap,
        name,
        vec![CreatableTopicConfig {
            name: "internal.segment.bytes".into(),
            value: Some(bytes.to_string()),
            ..Default::default()
        }],
    )
    .await;
}

async fn create_topic_with_configs(
    bootstrap: &str,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let client = connect_client(bootstrap, None).await;
    crate::support::transaction_wire::create_topic(
        &client,
        crate::support::transaction_wire::TransactionTopicSetup {
            name,
            configs,
            context: "create_topic",
            ..Default::default()
        },
    )
    .await;
}

pub async fn init_transaction(
    client: &krabka_client_core::Client,
    transactional_id: &str,
) -> (i64, i16) {
    support::find_coordinator(client, support::KEY_TYPE_TRANSACTION, transactional_id).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(new_producer_request(
                crate::support::transactions::InitProducerSetup {
                    transactional_id: Some(transactional_id.into()),
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        if response.error_code == 0 {
            return (response.producer_id, response.producer_epoch);
        }
        // COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE and
        // NOT_COORDINATOR are the answers of a coordinator that is not ready.
        assert!(
            matches!(response.error_code, 14..=16),
            "InitProducerId: {response:?}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "InitProducerId coordinator did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Boots a single-broker cluster whose only listener is `SASL_PLAINTEXT`, with
/// `PLAIN` enabled and the given users provisioned. Returns the same
/// `(handle, bootstrap, dir)` triple as [`boot_single`].
pub fn boot_single_sasl(
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, String, TempDir)> {
    let dir = TempDir::new().unwrap();
    let mut cfg = crate::support::sasl::sasl_plaintext_mechanisms(
        dir.path().to_path_buf(),
        vec![SaslMechanism::Plain],
    );
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    Box::pin(async move {
        let broker = Broker::start(cfg).await.unwrap();
        wait_until_coordinators_ready(&broker).await;
        let bootstrap = broker.listen_addr().to_string();
        (broker, bootstrap, dir)
    })
}

/// Client-side `SASL_PLAINTEXT` and `PLAIN` security for `(user, pass)`.
pub fn sasl_plain_security(user: &str, pass: &str) -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: user.to_string(),
            password: pass.to_string(),
        }),
        sasl_host: None,
    }
}

/// Creates the topic `name` with 1 partition over a SASL-authenticated admin
/// connection.
pub async fn create_topic_sasl(bootstrap: &str, name: &str, security: ClientSecurity) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .maybe_security(Some(security))
        .build()
        .await
        .unwrap();
    crate::support::transaction_wire::create_topic(
        &client,
        crate::support::transaction_wire::TransactionTopicSetup {
            name,
            context: "create_topic_sasl",
            ..Default::default()
        },
    )
    .await;
}

pub use crate::support::producer::string_record as rec;

pub async fn send_ok(producer: &Producer, record: ProducerRecord) {
    producer.send(record).await.expect("produce acknowledged");
}

/// Bring up all three voters and the group coordinator before a transaction failover scenario.
pub(crate) async fn registered_transaction_cluster(
    configure: impl Fn(usize, &mut BrokerConfig),
) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let cluster = support::start_n_node_with(3, configure)
        .await
        .expect("start the cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    // __consumer_offsets needs three replicas, including when a later reader outlives one broker.
    for (handle, _, _) in &cluster {
        handle.wait_until_group_coordinator_ready().await;
    }
    cluster
}
