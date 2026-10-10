//! The same three-site cluster reached through one relay per site, so a site
//! can be taken off the network with every broker still running.
//!
//! A stopped site and an unreachable site are different failures, and only the
//! second one leaves a *live* site that could misbehave. The cut is one-way,
//! for the reasons the crate documentation gives.

use std::{net::SocketAddr, time::Duration};

use krabka_broker::{Broker, BrokerConfig, BrokerError, BrokerHandle};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use tempfile::TempDir;

use crate::{
    NODE_A,
    profile::{apply_stretch_config, wait_for_stretch_metadata},
    support,
    support::relay::SiteLink,
    within,
};

/// A three-site cluster whose peers reach each site only through a relay.
///
/// Each site advertises its [`SiteLink`]'s addresses — the controller one in
/// the voter set, the client one as its inter-broker endpoint — while binding
/// its real listeners. Cutting a link therefore takes that site off the network
/// *as its peers see it*, with every broker still running. The test's own
/// clients bootstrap at the real listen addresses, so a client can always talk
/// to the broker in "its" site, the way a client in a partitioned data centre
/// still reaches the local broker.
pub struct LinkedCluster {
    handles: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    links: Vec<SiteLink>,
    _dirs: Vec<TempDir>,
}

impl LinkedCluster {
    pub async fn start() -> Self {
        let mut last_err = None;
        for attempt in 1..=3 {
            match Self::try_start().await {
                Ok(cluster) => return cluster,
                Err(error) => {
                    tracing::warn!(attempt, %error, "relayed stretch cluster start failed");
                    last_err = Some(error);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        panic!("relayed stretch cluster start failed after 3 attempts: {last_err:?}");
    }

    async fn try_start() -> Result<Self, BrokerError> {
        support::init_tracing();
        let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
            support::bind_and_hold_ports(3).await;

        // One relay pair per site, in front of the listeners that site already
        // holds. The brokers adopt the real listeners; only the *advertised*
        // addresses go through the relays.
        let mut links = Vec::with_capacity(3);
        for index in 0..3 {
            links.push(SiteLink::start(controller_addrs[index], client_addrs[index]).await);
        }
        let voters: Vec<(u64, SocketAddr)> = (0..3)
            .map(|index| {
                (
                    u64::try_from(index + 1).unwrap(),
                    links[index].controller_addr(),
                )
            })
            .collect();

        let topology = support::RoleTopology::new(&client_addrs, &controller_addrs, &voters);

        let mut starts = Vec::with_capacity(3);
        let mut metas: Vec<(BrokerConfig, TempDir)> = Vec::with_capacity(3);
        for (index, (data, controller)) in
            support::listener_pairs(client_listeners, controller_listeners)
        {
            let dir = TempDir::new().unwrap();
            let mut cfg = support::broker_config(
                dir.path(),
                topology.node_setup(crate::support::ClusterBootstrapSetup {
                    index: crate::support::NodeIndex(index),
                    ..Default::default()
                }),
            );
            // Peers and clients learn this site through its relay; the broker
            // itself binds the real port behind it.
            cfg.advertised_listener = links[index].client_addr().to_string();
            cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id.0));
            cfg.auto_join = false;
            cfg.bootstrap_servers = vec![];
            apply_stretch_config(index, &mut cfg);
            let spawn_cfg = cfg.clone();
            starts.push(tokio::spawn(async move {
                Broker::start_with_listeners(spawn_cfg, Some(controller), Some(data)).await
            }));
            metas.push((cfg, dir));
        }

        let mut handles = Vec::with_capacity(3);
        let mut configs = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for (start, (cfg, dir)) in starts.into_iter().zip(metas) {
            let handle = support::await_broker_start(start).await?;
            handles.push(Some(handle));
            configs.push(cfg);
            dirs.push(dir);
        }

        for handle in handles.iter().flatten() {
            handle.wait_until_brokers_registered(3).await;
            wait_for_stretch_metadata(handle).await;
        }

        Ok(Self {
            handles,
            configs,
            links,
            _dirs: dirs,
        })
    }

    pub fn handle(&self, index: usize) -> &BrokerHandle {
        self.handles[index].as_ref().expect("broker is running")
    }

    pub fn addr(&self, index: usize) -> String {
        self.configs[index].listen_addr.to_string()
    }

    /// Take a site off the network as its peers see it, including the
    /// connections they already hold open.
    pub fn cut(&self, index: usize) {
        self.links[index].cut();
    }

    /// Put it back.
    pub fn heal(&self, index: usize) {
        self.links[index].heal();
    }

    pub async fn shutdown(mut self) {
        for handle in self.handles.drain(..).flatten() {
            within("cluster shutdown", handle.shutdown()).await;
        }
        for link in self.links.drain(..) {
            within("relay shutdown", link.shutdown()).await;
        }
    }
}

/// Bring a relayed cluster up with the topic and all three replicas in sync.
pub async fn linked_cluster_with_topic() -> (LinkedCluster, WireUuid) {
    let cluster = LinkedCluster::start().await;
    let topic_id =
        crate::produce::initialize_sites(cluster.addr(NODE_A), |node| cluster.handle(node)).await;
    (cluster, topic_id)
}
