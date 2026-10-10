//! Cluster boot, topic setup, and offset helpers shared by every test in this
//! suite.
//!
//! The module holds the one-broker in-process cluster and its client, the
//! `CreateTopics` and `UpdateFeatures` calls that a streams group needs before
//! it can form, the `Metadata` lookup that resolves a topic id, the
//! simple-consumer `OffsetCommit` that seeds the offset a downgrade must
//! preserve, and the `BrokerConfig` that restarts a broker on an existing log
//! directory.

use std::sync::Arc;

use assert2::assert;
use krabka_broker::{BootstrapMode, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    ERR_NONE,
    support::offsets::{offset_commit_partition, offset_commit_topic},
};

pub(crate) async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    crate::support::streams::boot(crate::support::streams::ElectionReadiness::BrokerElectable).await
}

pub(crate) async fn connect(bootstrap: &str) -> Arc<Client> {
    crate::support::client::connect(bootstrap, "c1").await
}

pub(crate) async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    crate::support::client::create_topic(client, topic, partitions).await;
}

/// Finalizes `streams.version` at level 1, so that the heartbeat and describe
/// handlers stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is
/// UPGRADE.
pub(crate) async fn finalize_streams_version(client: &Client) {
    crate::support::streams::finalize_streams_version(client).await;
}

pub(crate) use crate::support::topic_id_for;

/// Commits an offset as the streams member `member_id` at `member_epoch`, as
/// a Streams client does. Kafka's `StreamsGroup.validateOffsetCommit` refuses
/// a commit with no member on a group that has members.
pub(crate) async fn commit_offset_as_member(
    client: &Client,
    group_id: &str,
    (member_id, member_epoch): (&str, i32),
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
) {
    let cr = client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch: member_epoch,
            member_id: member_id.into(),
            topics: vec![offset_commit_topic(
                topic,
                topic_id,
                vec![OffsetCommitRequestPartition {
                    committed_leader_epoch: 0,
                    ..offset_commit_partition(partition, offset, Some(String::new()))
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit (streams member) failed: {cr:?}"
    );
}

pub(crate) fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}
