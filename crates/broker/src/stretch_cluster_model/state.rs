//! The state that the search enumerates, the actions that move between two
//! states, and the small pure predicates over a state that every transition
//! module shares.
//!
//! The types live apart from the transitions so that a reader can see the
//! whole search space, including the sticky violation flags, on one screen.

use {krabka_raft::NodeId, std::collections::BTreeSet};

/// The outcome of one `acks=all` produce.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WriteOutcome {
    /// The leader refused the write, or the high watermark never covered it.
    Rejected,
    /// Every in-sync replica took the record, and the high watermark advanced.
    Committed,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StretchState {
    /// The sites that are powered off.
    pub down: BTreeSet<u8>,
    /// The sites that run, and that a network partition cut off from the rest.
    pub isolated: BTreeSet<u8>,
    pub leader: NodeId,
    /// The in-sync replica set, in replica order.
    pub isr: Vec<NodeId>,
    pub leader_epoch: i32,
    /// The outcome of the last `acks=all` produce.
    pub last_write: Option<WriteOutcome>,
    /// Sticky. A write committed inside a set of sites that holds no voter
    /// majority.
    pub commit_in_minority: bool,
    /// Sticky. A leader change reused a leader epoch.
    pub epoch_reused: bool,
    /// Sticky. A preferred election left the leader outside the preferred
    /// site while that site held an electable replica.
    pub preferred_pinning_broken: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum StretchAction {
    /// The site loses power. Every broker of the site stops.
    SiteDown(u8),
    /// The site comes back. Its replicas catch up and rejoin the in-sync set.
    SiteUp(u8),
    /// A network partition cuts the site off from the other sites.
    SitePartition(u8),
    /// The network partition heals.
    SiteHeal(u8),
    /// The controller runs its failover decision for a broker it cannot reach.
    Failover(NodeId),
    /// An operator, or the KIP-460 auto-rebalance, asks for a preferred
    /// election on partition 0.
    PreferredElection,
    /// A producer sends one `acks=all` record to the partition leader.
    ProduceAcksAll,
}

/// The count of sites that are down or isolated. The two sets are disjoint.
pub fn impaired(state: &StretchState) -> usize { state.down.len() + state.isolated.len() }

/// `true` when sites `left` and `right` both run and can reach each other. An
/// isolated site reaches only itself.
pub fn same_component(state: &StretchState, left: u8, right: u8) -> bool {
    if state.down.contains(&left) || state.down.contains(&right) {
        return false;
    }
    left == right || !(state.isolated.contains(&left) || state.isolated.contains(&right))
}

/// Record a leader change that reused an epoch. A new leader must always carry
/// a strictly greater leader epoch, which is what makes an epoch name at most
/// one leader.
pub fn check_epoch(last: &StretchState, next: &mut StretchState) {
    let reused = next.leader_epoch < last.leader_epoch
        || (next.leader != last.leader && next.leader_epoch <= last.leader_epoch);
    if reused {
        next.epoch_reused = true;
    }
}
