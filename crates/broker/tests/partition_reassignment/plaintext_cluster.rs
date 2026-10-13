//! Cluster setup and image-polling helpers for the partition-reassignment
//! suite.
//!
//! It holds the broker-id conversions between `NodeId` and the wire `i32`, the
//! 3-broker PLAINTEXT cluster boot, and the lookup that resolves the listen
//! address of the current raft controller leader. `AlterPartitionReassignments`
//! must reach that leader, so every test starts from this address.

use {
    crate::support, assert2::assert, krabka_broker::BrokerHandle, std::net::SocketAddr,
    tempfile::TempDir,
};

pub fn node_id(id: i32) -> krabka_metadata::NodeId {
    krabka_metadata::NodeId(u64::try_from(id).expect("broker IDs are non-negative"))
}

pub fn broker_id(node: krabka_metadata::NodeId) -> i32 {
    i32::try_from(node.0).expect("test broker ID fits in i32")
}

/// Waits until `handle` sees `(topic, partition)` in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    // Event-driven: subscribes to the image watch channel via the awaiter.
    handle.wait_until_partition_present(topic, partition).await;
}

/// A 3-broker PLAINTEXT cluster: the broker handles, the data directory
/// behind each, and the listen address of broker 1.
pub struct ThreeBrokerCluster {
    pub h1: BrokerHandle,
    pub h2: BrokerHandle,
    pub h3: BrokerHandle,
    pub d1: TempDir,
    pub d2: TempDir,
    pub d3: TempDir,
    pub addr1: SocketAddr,
}

/// Starts a 3-broker PLAINTEXT cluster. It waits until all 3 brokers see each
/// other registered before it returns.
pub async fn start_three_broker_plaintext_cluster() -> ThreeBrokerCluster {
    let cluster = support::start_n_node_with_retry(3).await;
    three_broker_handles(cluster).await
}

pub async fn start_three_broker_plaintext_cluster_with_log_dirs(
    extra_log_dirs: &[std::path::PathBuf; 3],
) -> ThreeBrokerCluster {
    let cluster = support::start_n_node_with(3, |index, config| {
        config.extra_log_dirs = vec![extra_log_dirs[index].clone()];
    })
    .await
    .expect("start three-broker JBOD cluster");
    three_broker_handles(cluster).await
}

/// Polls until the raft controller leader is stable, then returns its listen
/// address. It tries each handle in `handles` to find the one whose `node_id`
/// matches the reported raft leader.
pub async fn controller_leader_addr(handles: &[&BrokerHandle]) -> SocketAddr {
    // Event-driven: await a non-zero elected controller leader on the first
    // handle's leader watch channel instead of polling `controller_leader_id`.
    let leader_id = handles[0].wait_until_controller_leader().await;
    // We identify the leader by the node_id — it is the raft node id (u64)
    // which equals (broker_index + 1). The handles slice is ordered
    // [broker1, broker2, broker3], so handle[i] has node_id = i+1.
    let idx = usize::try_from(leader_id.0).unwrap().saturating_sub(1);
    assert!(
        idx < handles.len(),
        "raft leader id {leader_id:?} out of range for {} handles",
        handles.len()
    );
    handles[idx].listen_addr()
}

async fn three_broker_handles(
    cluster: Vec<(BrokerHandle, krabka_broker::BrokerConfig, TempDir)>,
) -> ThreeBrokerCluster {
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let mut it = cluster.into_iter();
    let (h1, _cfg1, d1) = it.next().unwrap();
    let (h2, _cfg2, d2) = it.next().unwrap();
    let (h3, _cfg3, d3) = it.next().unwrap();
    let addr1 = h1.listen_addr();
    ThreeBrokerCluster {
        h1,
        h2,
        h3,
        d1,
        d2,
        d3,
        addr1,
    }
}
