//! The same three-site cluster reached through one relay per site, so a site
//! can be taken off the network with every broker still running.
//!
//! A stopped site and an unreachable site are different failures, and only the
//! second one leaves a *live* site that could misbehave. The cut is one-way,
//! for the reasons the crate documentation gives.

use {
    crate::{
        NODE_A,
        produce::initialize_sites,
        profile::{apply_stretch_config, wait_for_stretch_metadata},
        support::linked_cluster::{NODES, RelayedCluster, SiteLinks},
    },
    krabka_broker::BrokerHandle,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
};

/// A three-site cluster whose peers reach each site only through a relay.
///
/// Each site advertises its relay's addresses -- the controller one in the
/// voter set, the client one as its inter-broker endpoint -- while binding its
/// real listeners. Cutting a link therefore takes that site off the network
/// *as its peers see it*, with every broker still running. The test's own
/// clients bootstrap at the real listen addresses, so a client can always talk
/// to the broker in "its" site, the way a client in a partitioned data centre
/// still reaches the local broker.
pub struct LinkedCluster(RelayedCluster<SiteLinks>);

impl LinkedCluster {
    pub async fn start() -> Self {
        let cluster = RelayedCluster::start(apply_stretch_config).await;
        for index in 0..NODES {
            wait_for_stretch_metadata(cluster.handle(index)).await;
        }
        Self(cluster)
    }

    pub fn handle(&self, index: usize) -> &BrokerHandle { self.0.handle(index) }

    pub fn addr(&self, index: usize) -> String { self.0.addr(index).to_string() }

    /// Take a site off the network as its peers see it, including the
    /// connections they already hold open.
    pub fn cut(&self, index: usize) { self.0.network().cut(index); }

    /// Put it back.
    pub fn heal(&self, index: usize) { self.0.network().heal(index); }

    pub async fn shutdown(self) { self.0.shutdown().await; }
}

/// Bring a relayed cluster up with the topic and all three replicas in sync.
pub async fn linked_cluster_with_topic() -> (LinkedCluster, WireUuid) {
    let cluster = LinkedCluster::start().await;
    let topic_id = initialize_sites(cluster.addr(NODE_A), |node| cluster.handle(node)).await;
    (cluster, topic_id)
}
