//! Shared cluster and share-consume helpers for the KIP-932 admin offset suite.
//!
//! The helpers start a single-broker test cluster, create a topic, produce
//! records, drive `ShareGroupHeartbeat` membership, and reuse the shared
//! `ShareFetch` and `ShareAcknowledge` fixtures. The error-code and
//! acknowledgement-type constants live here as well, since every admin surface
//! in this suite asserts on them.

pub use crate::support::share::{
    acquired_count, bootstrap_share_state, broker_config, broker_test_permit, connect,
    create_topic, produce_n,
};
use {
    crate::support::share::{
        ConsumptionFixture, ConsumptionSetup, MemberSession, ShareRecordCount, ShareSessionSetup,
        consumption_fixture, join as join_share_group, permitted_topic_fixture, start_topic,
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    std::time::Duration,
};

pub const NONE: i16 = 0;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const NON_EMPTY_GROUP: i16 = 68;

pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    // Delegates to the broker-handle awaiter (30s timeout, 25ms poll interval).
    // `join()` drives the steady-state heartbeats that trigger the lifecycle hook
    // before this is called, so no repeated heartbeats are needed here.
    broker
        .wait_for_share_state_summary(group, tid, partition)
        .await;
}

/// Joins `group` with a subscription to `topic`.
///
/// The function drives steady-state heartbeats, so the lifecycle hook
/// initializes the share state of the subscribed partitions. It returns
/// its member id and epoch, so the caller can leave with the live epoch.
pub async fn join(client: &Client, group: &str, topic: &str) -> MemberSession {
    let MemberSession {
        member: member_id,
        mut epoch,
    } = join_share_group(client, group, topic).await;

    for _ in 0..3 {
        let hb = client
            .send(ShareGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.clone(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec![topic.into()]),
                ..Default::default()
            })
            .await
            .expect("ShareGroupHeartbeat steady-state");
        epoch = hb.member_epoch;
        // intentional: paces steady-state heartbeats to drive the membership
        // reconciliation / lifecycle hook forward; this drives the protocol rather
        // than waiting on a single observable state (share init is awaited separately).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    MemberSession {
        member: member_id,
        epoch,
    }
}

/// Leaves the group with `member_epoch == -1`.
///
/// The broker keeps the group but reports zero members, in state "Empty".
pub async fn leave(client: &Client, group: &str, member_id: &str) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat leave");
    assert!(resp.error_code == 0, "leave failed: {:?}", resp.error_code);
}

krabka_macros::share_first_fetch!(fetch_until_acquired, Initial);

krabka_macros::share_consumption_driver!(
    initialize_consumption,
    join,
    |broker, client, member, epoch, tid| [wait_for_share_init(broker, "g1", tid, 0)]
);

/// Initialize the persisted topic/group fixture before a restart scenario.
#[bon::builder]
pub async fn initialized_topic(
    log_dir: std::path::PathBuf,
    #[builder(into, default = ShareRecordCount(3))] count: ShareRecordCount,
) -> (
    krabka_broker::BrokerHandle,
    std::sync::Arc<Client>,
    uuid::Uuid,
    String,
) {
    let (broker, client, tid) = start_topic().cfg(broker_config(log_dir)).call().await;
    let MemberSession { member, .. } = initialize_consumption(&broker, &client, ConsumptionSetup {
        topic_id: tid,
        count,
    })
    .await;
    (broker, client, tid, member)
}

/// Prepare g1's persister and data without joining a member to the group.
#[bon::builder]
pub async fn seeded_empty_group(
    #[builder(into, default = ShareRecordCount(3))] count: ShareRecordCount,
) -> (
    tokio::sync::OwnedSemaphorePermit,
    krabka_broker::BrokerHandle,
    std::sync::Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
) {
    let (permit, broker, client, dir, tid) = Box::pin(permitted_topic_fixture().call()).await;
    bootstrap_share_state(&broker, &client, "g1").await;
    produce_n()
        .client(&client)
        .topic_id(tid)
        .count(count.0)
        .call()
        .await;
    (permit, broker, client, dir, tid)
}

/// Owners for a restart fixture whose caller reopens the same data directory.
pub struct RestartDirectory {
    pub permit: tokio::sync::OwnedSemaphorePermit,
    pub dir: tempfile::TempDir,
    pub path: std::path::PathBuf,
}

pub async fn restart_directory() -> RestartDirectory {
    let permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    RestartDirectory { permit, dir, path }
}

/// Join g1, seed three records, and acquire its first partition.
pub async fn acquired_topic() -> (
    tokio::sync::OwnedSemaphorePermit,
    krabka_broker::BrokerHandle,
    std::sync::Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
    String,
    krabka_protocol::owned::share_fetch_response::PartitionData,
) {
    let ConsumptionFixture {
        _permit: permit,
        broker,
        client,
        _dir: dir,
        tid,
        member,
    } = consumption_fixture()
        .initialize(initialize_consumption)
        .call()
        .await;
    let row = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
    (permit, broker, client, dir, tid, member, row)
}

/// Retain the directory and permit across both boots of an initialized-topic test.
pub struct InitializedRestartFixture {
    pub _permit: tokio::sync::OwnedSemaphorePermit,
    pub _dir: tempfile::TempDir,
    pub log_dir: std::path::PathBuf,
    pub topic: (
        krabka_broker::BrokerHandle,
        std::sync::Arc<Client>,
        uuid::Uuid,
        String,
    ),
}

#[bon::builder]
pub async fn initialized_restart_fixture(
    #[builder(into, default = ShareRecordCount(3))] count: ShareRecordCount,
) -> InitializedRestartFixture {
    let RestartDirectory {
        permit,
        dir,
        path: log_dir,
    } = restart_directory().await;
    let topic = initialized_topic()
        .log_dir(log_dir.clone())
        .count(count.0)
        .call()
        .await;
    InitializedRestartFixture {
        _permit: permit,
        _dir: dir,
        log_dir,
        topic,
    }
}
