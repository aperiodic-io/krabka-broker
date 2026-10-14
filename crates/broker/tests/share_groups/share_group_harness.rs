//! Broker boot, client connection and request-building helpers shared by the
//! KIP-932 share-group scenarios in this suite.
//!
//! Every scenario starts a single-node broker whose share-state topic has one
//! partition, connects a typed client to it, and then drives
//! `ShareGroupHeartbeat` and `ShareGroupDescribe` over the wire. Those steps
//! live here so each scenario module holds only its own assertions.

use {
    crate::support::share::broker_config,
    assert2::assert,
    krabka_broker::Broker,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        share_group_describe_request::ShareGroupDescribeRequest,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
};

/// Starts a broker on `log_dir` whose group and share coordinators serve
/// requests.
///
/// No broker creates `__consumer_offsets` or `__share_group_state` when it
/// starts. `ShareGroupHeartbeat` needs the group coordinator, and the
/// heartbeat initializes share state through the share coordinator. The
/// handle helpers ask for each topic as a client's first lookup does, and wait
/// until this broker serves it.
pub async fn start(log_dir: std::path::PathBuf) -> krabka_broker::BrokerHandle {
    let broker = Broker::start(broker_config(log_dir)).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    broker.wait_until_share_coordinator_ready().await;
    broker
}

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = start(dir.path().to_path_buf()).await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Reopen the caller's existing directory without adding startup-readiness waits.
pub async fn rejoin(
    log_dir: std::path::PathBuf,
) -> (krabka_broker::BrokerHandle, std::sync::Arc<Client>) {
    let mut config = broker_config(log_dir);
    config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let broker = Box::pin(Broker::start(config)).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    (broker, client)
}

pub use crate::support::client::{connect_c1 as connect, create_topic};

pub fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        ..Default::default()
    }
}

pub async fn join_share_group(
    client: &Client,
    group: &str,
    member_id: &str,
    topic: &str,
) -> String {
    let mut request = heartbeat(group, member_id, 0);
    request.subscribed_topic_names = Some(vec![topic.into()]);
    let response = client.send(request).await.unwrap();
    assert!(
        response.error_code == 0,
        "join failed: {:?}",
        response.error_code
    );
    response
        .member_id
        .expect("join response includes a member id")
}

pub fn total_assigned(
    resp: &krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
) -> usize {
    resp.assignment.as_ref().map_or(0, |a| {
        a.topic_partitions.iter().map(|t| t.partitions.len()).sum()
    })
}

pub async fn describe(
    client: &Client,
    group: &str,
) -> krabka_protocol::owned::share_group_describe_response::ShareGroupDescribeResponse {
    client
        .send(ShareGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap()
}

/// Resolves the id of a created topic from this broker's metadata image.
pub use crate::support::share::topic_id;
