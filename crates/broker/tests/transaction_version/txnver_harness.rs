//! Shared fixtures for the `transaction.version` suite: broker boot, an admin
//! client, topic creation, and the `UpdateFeatures` downgrade that moves the
//! cluster onto a lower `transaction.version` level.
//!
//! `downgrade_transaction_version` is what every case in this binary is built
//! on, because the in-process broker self-bootstraps at `TV_2` and each level
//! below it has to be reached through a live feature downgrade.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};

use crate::support::client::connect_client;

// Kafka error codes asserted below.
pub const NONE: i16 = 0;
pub const TRANSACTION_ABORTABLE: i16 = 120;

pub use crate::support::boot_single;

pub async fn admin_client(bootstrap: &str) -> Client {
    connect_client(bootstrap, Some("krabka-txnv-test")).await
}

pub async fn create_topic(
    client: &Client,
    name: &str,
    partitions: crate::support::topics::TopicPartitionCount,
) {
    crate::support::transaction_wire::create_topic(
        client,
        crate::support::transaction_wire::TransactionTopicSetup {
            name,
            partitions,
            context: "create_topic",
            ..Default::default()
        },
    )
    .await;
}

/// Downgrade the finalized `transaction.version` to `level` with a
/// `SAFE_DOWNGRADE` (`upgrade_type = 2`) `UpdateFeatures` request. Level 1
/// finalizes the Flexible level. Level 0 tombstones the feature (→ absent →
/// Classic). `resolve_txn_version` reads the live image per request, so a new
/// transaction started after this call returns picks up the downgraded level.
pub async fn downgrade_transaction_version(client: &Client, level: i16) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "transaction.version".into(),
                max_version_level: level,
                upgrade_type: 2, // SAFE_DOWNGRADE
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "UpdateFeatures top-level: {resp:?}");
    if let Some(row) = resp
        .results
        .iter()
        .find(|r| r.feature == "transaction.version")
    {
        assert!(
            row.error_code == 0,
            "transaction.version downgrade to {level} rejected: {resp:?}"
        );
    }
}

/// A single materialized transaction-state partition, optionally reopened on the same directory.
pub fn config(
    log_dir: std::path::PathBuf,
    bootstrap_mode: Option<krabka_broker::BootstrapMode>,
) -> krabka_broker::BrokerConfig {
    let mut config = krabka_broker::BrokerConfig::for_tests(log_dir);
    config.transaction_state_num_partitions = 1;
    config.transaction_state_replication_factor = 1;
    if let Some(mode) = bootstrap_mode {
        config.bootstrap_mode = mode;
    }
    config
}

/// Locate the named topic in its own metadata response, retaining its diagnostic.
pub async fn topic_id(client: &Client, topic: &str) -> krabka_protocol::primitives::uuid::Uuid {
    client
        .send(crate::support::discovery::topic_metadata_request(Some(
            vec![crate::support::topics::metadata_topic(
                Some(topic.into()),
                krabka_protocol::primitives::uuid::Uuid::default(),
            )],
        )))
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(topic))
        .map(|row| row.topic_id)
        .expect("topic in metadata")
}

/// Trigger loading of the transaction coordinator without asserting on the lookup response.
pub async fn find_coordinator(client: &Client, transactional_id: &str) {
    let _ = client
        .send(crate::support::discovery::coordinator_lookup_request(
            transactional_id,
            1,
            vec![transactional_id.into()],
        ))
        .await
        .expect("FindCoordinator");
}

/// A literal expected response table; no field is copied from an actual response.
pub fn expected_partitions(
    transactional_id: &str,
    topic: &str,
    partitions: &[(i32, i16)],
) -> krabka_protocol::owned::add_partitions_to_txn_response::AddPartitionsToTxnResponse {
    use krabka_protocol::owned::{
        add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
        common::add_partitions_to_txn_response::{
            add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
            add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
        },
    };
    AddPartitionsToTxnResponse {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: transactional_id.into(),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: topic.into(),
                results_by_partition: partitions
                    .iter()
                    .map(|&(partition_index, partition_error_code)| {
                        AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}
