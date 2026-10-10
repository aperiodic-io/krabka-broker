//! The three-site cluster a site is *stopped* in: it boots two data sites and
//! a witness site, hands out handles and addresses, and takes a handle away
//! when its site is lost.
//!
//! The relayed variant that leaves every broker running and cuts the network
//! instead is `LinkedCluster`, in the `linked` module.

use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use tempfile::TempDir;

use crate::{
    NODE_A,
    profile::{apply_stretch_config, wait_for_stretch_metadata},
    support, within,
};

/// A running three-site cluster. Handles are taken out as sites are stopped,
/// so `shutdown` can still drain whatever is left.
pub struct Cluster {
    handles: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Boot the three-site cluster: two data sites, one witness site,
    /// `min.insync.replicas=2` (the only value a stretch profile accepts at
    /// rf=3 over three sites), and the witness role on the `site-c` node.
    pub async fn start() -> Self {
        let cluster =
            support::start_n_node_customized_with_retry(3, apply_stretch_config, "stretch cluster")
                .await;
        support::wait_for_all_brokers_registered(&cluster, 3).await;
        for (handle, _, _) in &cluster {
            wait_for_stretch_metadata(handle).await;
        }
        let (handles, configs, dirs) = cluster.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut handles, mut configs, mut dirs), (handle, config, dir)| {
                handles.push(Some(handle));
                configs.push(config);
                dirs.push(dir);
                (handles, configs, dirs)
            },
        );
        Self {
            handles,
            configs,
            _dirs: dirs,
        }
    }

    pub fn handle(&self, index: usize) -> &BrokerHandle {
        self.handles[index]
            .as_ref()
            .unwrap_or_else(|| panic!("broker {index} is still running"))
    }

    pub fn addr(&self, index: usize) -> String {
        self.configs[index].listen_addr.to_string()
    }

    /// Lose a site: stop its broker and let it leave the cluster.
    pub async fn stop(&mut self, index: usize) {
        let handle = self.handles[index]
            .take()
            .unwrap_or_else(|| panic!("broker {index} was already stopped"));
        within("stopping a site", handle.shutdown()).await;
    }

    pub async fn shutdown(mut self) {
        for handle in self.handles.drain(..).flatten() {
            within("cluster shutdown", handle.shutdown()).await;
        }
    }
}

/// Bring the cluster up with a topic and every replica in the ISR.
pub async fn cluster_with_topic() -> (Cluster, WireUuid) {
    let cluster = Cluster::start().await;
    let topic_id =
        crate::produce::initialize_sites(cluster.addr(NODE_A), |node| cluster.handle(node)).await;
    (cluster, topic_id)
}
