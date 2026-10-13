//! What the controller can reach. These queries turn the down and isolated
//! site sets of a [`StretchState`] into the alive broker set and the network
//! components that the failover, produce, and property code all read.
//!
//! The queries are pure reads of the state and the fixed cluster shape, so
//! they sit apart from the transitions that mutate the state.

use {
    super::{config::StretchModel, state::StretchState},
    krabka_raft::NodeId,
    std::collections::{BTreeSet, HashSet},
};

impl StretchModel {
    pub fn site_of(&self, node: NodeId) -> u8 { self.site_of[&node] }

    pub fn site_count(&self) -> u8 {
        u8::try_from(self.sites.len()).expect("site count fits in u8")
    }

    /// The alive set as the controller sees it. The controller reaches every
    /// broker of a site that runs and that no network partition cut off.
    pub fn alive(&self, state: &StretchState) -> HashSet<NodeId> {
        self.brokers
            .iter()
            .map(|broker| broker.node_id)
            .filter(|&node| {
                let site = self.site_of(node);
                !(state.down.contains(&site) || state.isolated.contains(&site))
            })
            .collect()
    }

    /// The sites of the network component that holds `site`. A down site holds
    /// no component. An isolated site is alone. Every other running site is in
    /// the one large component.
    pub fn component_of(&self, state: &StretchState, site: u8) -> BTreeSet<u8> {
        if state.down.contains(&site) {
            return BTreeSet::new();
        }
        if state.isolated.contains(&site) {
            return maplit::btreeset! {site};
        }
        (0..self.site_count())
            .filter(|k| !(state.down.contains(k) || state.isolated.contains(k)))
            .collect()
    }

    /// `true` when the network component that holds the leader also holds a
    /// strict majority of the `KRaft` voters.
    pub fn leader_holds_majority(&self, state: &StretchState) -> bool {
        let component = self.component_of(state, self.site_of(state.leader));
        let voters: i64 = component
            .iter()
            .map(|&site| self.sites[site as usize].voters)
            .sum();
        2 * voters > self.total_voters
    }
}
