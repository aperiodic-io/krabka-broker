//! Booting the three-site stretch cluster the witness tests run on, and the
//! client and shutdown helpers that bracket each of them.
//!
//! The cluster is what makes the role observable at all: two data sites, one
//! witness site carrying `NodeRole::Witness`, `min.insync.replicas=2`, and the
//! rack-aware replica selector that the KIP-392 redirect check needs. Every
//! test in this suite starts here, so the boot lives in its own file rather
//! than beside any one of them.

use krabka_broker::{
    BrokerConfig, BrokerHandle, NodeId,
    config::{NodeRole, StretchProfile},
    replica_selector::ReplicaSelectorKind,
};
use krabka_client_core::Client;
use tempfile::TempDir;

use crate::{
    BROKER_WITNESS, SITE_A, SITE_C, SITES, STRETCH_PREFERRED_LEADER_SITE, support,
    support::client::connect_owned, within,
};

fn stretch_profile() -> StretchProfile {
    StretchProfile {
        sites: SITES.iter().map(|site| (*site).to_string()).collect(),
        witness_site: SITE_C.to_string(),
        preferred_leader_site: SITE_A.to_string(),
    }
}

/// Boot the three-site cluster: two data sites and one witness site, with
/// `min.insync.replicas=2` (the only value a stretch profile accepts) and the
/// rack-aware replica selector, which is what makes the KIP-392 redirect check
/// meaningful.
pub(crate) async fn start_stretch_cluster() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let cluster = support::start_n_node_customized_with_retry(
        3,
        |i, cfg| {
            cfg.rack = Some(SITES[i].to_string());
            cfg.stretch = Some(stretch_profile());
            cfg.default_min_insync_replicas = 2;
            cfg.default_replication_factor = 3;
            cfg.replica_selector = ReplicaSelectorKind::RackAware;
            if SITES[i] == SITE_C {
                cfg.roles.push(NodeRole::Witness);
            }
        },
        "stretch cluster",
    )
    .await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    // Placement and the produce / fetch gates read the role and the
    // preferred site out of the metadata image, so wait until both
    // records have reached every node before a topic is created.
    for (handle, _, _) in &cluster {
        within(
            "witness role and preferred site in the image",
            handle.wait_for_image(|img| {
                img.broker_config(NodeId(3))
                    .and_then(|configs| configs.get(BROKER_WITNESS))
                    .map(String::as_str)
                    == Some("true")
                    && img
                        .default_broker_config()
                        .and_then(|configs| configs.get(STRETCH_PREFERRED_LEADER_SITE))
                        .map(String::as_str)
                        == Some(SITE_A)
            }),
        )
        .await;
    }
    cluster
}

pub(crate) async fn client_at(addr: &str) -> Client {
    connect_owned(addr.to_string(), "witness-role-test", "client build").await
}

pub(crate) async fn shutdown(cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>) {
    crate::support::shutdown_cluster(cluster).await;
}
