//! The stretch profile itself: what every broker of the three-site deployment
//! is configured with, and the controller-managed records the rest of the
//! cluster's behaviour keys on.
//!
//! Both cluster harnesses in this suite apply the same configuration and then
//! wait for the same two records, so the pair lives here rather than in either
//! harness.

use {
    crate::{SITE_A, SITE_C, SITES, WITNESS, within},
    krabka_broker::{
        BrokerConfig, BrokerHandle, NodeId,
        config::{NodeRole, StretchProfile},
    },
};

pub fn stretch_profile() -> StretchProfile {
    StretchProfile {
        sites: SITES.iter().map(|site| (*site).to_string()).collect(),
        witness_site: SITE_C.to_string(),
        preferred_leader_site: SITE_A.to_string(),
    }
}

/// Put broker `index` in its site: the rack that names it, the profile every
/// node of the cluster shares, `min.insync.replicas=2` (the only value a
/// stretch profile accepts at rf=3 over three sites), the topic-creation
/// default replication factor of 3 that the profile also checks, the
/// coordinator topics at replication factor 3, which the profile checks for
/// `__consumer_offsets`, and the witness role for the node in the witness
/// site.
pub fn apply_stretch_config(index: usize, cfg: &mut BrokerConfig) {
    *cfg = cfg.clone().with_internal_topics_for(3);
    cfg.rack = Some(SITES[index].to_string());
    cfg.stretch = Some(stretch_profile());
    cfg.default_min_insync_replicas = 2;
    cfg.default_replication_factor = 3;
    if SITES[index] == SITE_C {
        cfg.roles.push(NodeRole::Witness);
    }
}

/// Await the two controller-managed records the rest of the cluster's
/// behaviour keys on: the witness role of the `site-c` node, and the preferred
/// leader site. Placement, the leader picks and the produce gate all read them
/// out of the metadata image, so nothing may be created before they land.
pub async fn wait_for_stretch_metadata(handle: &BrokerHandle) {
    within(
        "the witness role and the preferred site reach the image",
        handle.wait_for_image(|img| {
            img.broker_config(NodeId(WITNESS))
                .and_then(|configs| configs.get("broker.witness"))
                .map(String::as_str)
                == Some("true")
                && img
                    .default_broker_config()
                    .and_then(|configs| configs.get("stretch.preferred.leader.site"))
                    .map(String::as_str)
                    == Some(SITE_A)
        }),
    )
    .await;
}
