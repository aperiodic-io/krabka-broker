//! The bounded cluster shape that the model checks: the sites, the brokers,
//! the replica list that the real placement produced, and the proved
//! site-loss verdicts that go with it.
//!
//! This module is separate because the shape is fixed before the search
//! begins. Everything here is decided once, in [`StretchModel::new`], and the
//! transition modules then only read it.

use {
    crate::{
        config_keys::RecoveryStrategy,
        elr::state::PartitionElr,
        leader_election::FailoverDecision,
        site_placement::{PlacementRng, SiteBrokerView, stretch_replicas},
    },
    assert2::assert,
    krabka_metadata::PartitionRecord,
    krabka_raft::NodeId,
    krabka_verified::stretch::{
        min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
    },
    std::collections::{BTreeMap, HashSet},
};

/// The one topic of the model. Every action works on partition 0 of it.
pub const TOPIC: &str = "stretch";

/// The largest count of sites that the model impairs at once. Two of three
/// sites is already a full loss of the metadata quorum, and a third impaired
/// site adds no new decision.
const MAX_IMPAIRED_SITES: usize = 2;

/// The controller failover decision, as a function pointer. The model runs the
/// real [`failover_one`](crate::leader_election::failover_one) under it. The
/// RED witness runs [`legacy_elect`](super::red_witness::legacy_elect).
///
/// The [`PartitionElr`] is the partition's published eligible and last-known
/// replicas. This model carries no ELR state, so it is always empty here: what
/// it checks is the witness rule, and the KIP-966 rungs are exercised in
/// `leader_election::policy`.
pub type ElectFn = fn(
    &PartitionRecord,
    NodeId,
    &HashSet<NodeId>,
    &HashSet<NodeId>,
    &PartitionElr,
    RecoveryStrategy,
    bool,
) -> FailoverDecision;

/// One site of the stretch cluster.
pub struct SiteConfig {
    /// The `broker.rack` value that names the site.
    pub name: &'static str,
    /// The count of `KRaft` voters that the site holds.
    pub voters: i64,
}

/// One broker of the model cluster: node id, site index, and witness role.
type BrokerSpec = (u64, usize, bool);

/// Bounded config for the stretch-cluster model.
pub struct StretchModel {
    /// The sites, in model index order. Index `k` is site `k`.
    pub sites: Vec<SiteConfig>,
    /// The brokers of the cluster, in node-id order.
    pub brokers: Vec<SiteBrokerView>,
    /// The site index of each broker.
    pub site_of: BTreeMap<NodeId, u8>,
    /// The replicas of partition 0, from the real [`stretch_replicas`]. Order
    /// is significant: `replicas[0]` is the preferred leader.
    pub replicas: Vec<NodeId>,
    /// The witness nodes among `replicas`.
    pub witnesses: HashSet<NodeId>,
    /// The site that holds partition leadership in a healthy cluster.
    pub preferred_site: u8,
    /// The topic's `min.insync.replicas`.
    pub min_insync: i32,
    /// The sum of `voters` over every site.
    pub total_voters: i64,
    /// The replica count that survives the loss of any one site, from the
    /// proved [`site_loss_survivors`].
    pub survivors: i64,
    /// `min.insync.replicas` keeps `acks=all` durable and writable through the
    /// loss of any one site, from the proved [`min_insync_is_site_loss_safe`].
    pub min_insync_safe: bool,
    /// The `KRaft` voter split keeps a majority through the loss of any one
    /// site, from the proved [`quorum_survives_any_single_site_loss`].
    pub quorum_tolerates_one_loss: bool,
    /// The largest count of sites that the model impairs at once.
    pub max_impaired: usize,
    /// The leader-epoch cap that bounds the search.
    pub max_epoch: i32,
    /// The controller failover decision under test.
    pub elect: ElectFn,
}

impl StretchModel {
    /// Three sites with one broker each. Node 3 sits in the cheap third site
    /// and carries the witness role. `replication.factor=3` with
    /// `min.insync.replicas=2` is the supported stretch shape.
    pub fn three_sites(min_insync: i32, elect: ElectFn) -> Self {
        Self::new(
            vec![
                SiteConfig {
                    name: "east",
                    voters: 1,
                },
                SiteConfig {
                    name: "west",
                    voters: 1,
                },
                SiteConfig {
                    name: "witness",
                    voters: 1,
                },
            ],
            &[(1, 0, false), (2, 1, false), (3, 2, true)],
            3,
            min_insync,
            elect,
        )
    }

    fn new(
        sites: Vec<SiteConfig>,
        brokers: &[BrokerSpec],
        replication_factor: i16,
        min_insync: i32,
        elect: ElectFn,
    ) -> Self {
        let preferred_site = 0_u8;
        let views: Vec<SiteBrokerView> = brokers
            .iter()
            .map(|&(node_id, site, is_witness)| SiteBrokerView {
                node_id: NodeId(node_id),
                site: Some(sites[site].name.to_string()),
                is_witness,
                fenced: false,
            })
            .collect();
        let site_of: BTreeMap<NodeId, u8> = brokers
            .iter()
            .map(|&(node_id, site, _)| {
                (
                    NodeId(node_id),
                    u8::try_from(site).expect("site index fits in u8"),
                )
            })
            .collect();
        let witnesses: HashSet<NodeId> = brokers
            .iter()
            .filter(|&&(_, _, is_witness)| is_witness)
            .map(|&(node_id, _, _)| NodeId(node_id))
            .collect();

        // The real placement decides the replica list, including which site
        // holds `replicas[0]`.
        let placement = stretch_replicas(
            &views,
            1,
            replication_factor,
            Some(sites[preferred_site as usize].name),
            // A fixed seed keeps the model check reproducible.
            &mut PlacementRng::seeded(0),
        );
        assert!(
            placement.len() == 1,
            "stretch_replicas refused the model configuration"
        );
        let replicas = placement[0].clone();
        let preferred = *replicas.first().expect("a placed partition has replicas");
        assert!(
            !witnesses.contains(&preferred),
            "stretch_replicas put witness {preferred} first in {replicas:?}"
        );
        assert!(
            site_of[&preferred] == preferred_site,
            "stretch_replicas put {preferred} outside the preferred site in {replicas:?}"
        );

        let voters: Vec<i64> = sites.iter().map(|site| site.voters).collect();
        let total_voters = voters.iter().sum();
        let site_count = i64::try_from(sites.len()).expect("site count fits in i64");
        let rf = i64::from(replication_factor);
        Self {
            survivors: site_loss_survivors(rf, site_count),
            min_insync_safe: min_insync_is_site_loss_safe(rf, site_count, i64::from(min_insync)),
            quorum_tolerates_one_loss: quorum_survives_any_single_site_loss(&voters),
            max_impaired: MAX_IMPAIRED_SITES.min(sites.len() - 1),
            sites,
            brokers: views,
            site_of,
            replicas,
            witnesses,
            preferred_site,
            min_insync,
            total_voters,
            max_epoch: 6,
            elect,
        }
    }
}
