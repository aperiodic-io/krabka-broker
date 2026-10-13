//! Property-based coverage of KIP-101/279/320 reconciliation at a scale the
//! exhaustive `leader_epoch_model` cannot reach. It drives the model's own
//! step function -- the production lookup and truncation underneath -- over
//! random schedules of elections, writes and follower fetches.

use {
    super::leader_epoch_model::{Action, Cluster, Violations},
    proptest::prelude::*,
};

const REPLICAS: usize = 4;
const MAX_LOG: usize = 40;

/// Decode one random `(kind, replica)` pair into an action that is legal in
/// `cluster`.
fn action(cluster: &Cluster, kind: u8, replica: usize) -> Action {
    let replica = replica % REPLICAS;
    match kind % 8 {
        0 => Action::Elect(replica),
        1..=3 if cluster.replicas[cluster.leader].log.len() < MAX_LOG => Action::Write,
        _ if replica == cluster.leader => Action::Fetch((replica + 1) % REPLICAS),
        _ => Action::Fetch(replica),
    }
}

proptest! {
    /// After every step of a random schedule: a follower the leader accepted
    /// is a prefix of the leader's log, no truncation dropped an agreed record,
    /// every divergence made progress, the leader always placed the
    /// follower's epoch, and every checkpoint stays strictly increasing.
    #[test]
    fn reconciliation_holds_on_random_schedules(
        schedule in proptest::collection::vec((0u8..8, 0usize..REPLICAS), 0..400usize),
    ) {
        let mut cluster = Cluster::new(REPLICAS, true);
        for (kind, replica) in schedule {
            let step = action(&cluster, kind, replica);
            cluster.step(step, true);
            prop_assert!(cluster.follower_prefix_holds(), "{step:?}: {cluster:?}");
            prop_assert_eq!(cluster.violations, Violations::default(), "{:?}: {:?}", step, cluster);
            prop_assert!(cluster.checkpoints_hold(), "{step:?}: {cluster:?}");
        }
    }
}
