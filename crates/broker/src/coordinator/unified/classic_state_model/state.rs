//! The state the search enumerates, its canonical fingerprint, and the actions
//! that move between two states.
//!
//! The state is a real `ClassicGroup` plus the logical clock, so a transition
//! drives production code rather than a restatement of it. `ClassicGroup` holds
//! `HashMap`s, which have no stable ordering, so equality and hashing go
//! through one sorted projection and stateright sees a canonical fingerprint.

use {
    crate::coordinator::unified::classic_state::{ClassicGroup, GroupState},
    std::time::Instant,
};

/// Model state: a real `ClassicGroup` plus the logical clock. `Hash` and `Eq`
/// are manual over a canonical projection, because `ClassicGroup` holds
/// `HashMap`s.
#[derive(Clone, Debug)]
pub(super) struct GrpState {
    pub(super) g: ClassicGroup,
    pub(super) clock: i64,
}

// NOTE: `generation_id` is deliberately EXCLUDED from the fingerprint. It is a
// monotonic counter bumped on every rebalance and read by no transition, so
// including it would make the rebalance cycle (join→complete→sync→…) an
// unbounded state generator (the DPM-A1 monotonic-counter lesson). States that
// differ only in generation are behaviorally equivalent for every invariant.
type Proj = (
    GroupState,
    Option<String>,
    Option<String>,
    bool,
    i64,
    Vec<(String, Option<String>, bool, Instant)>,
    Vec<(String, String)>,
    Vec<String>,
);

impl GrpState {
    fn proj(&self) -> Proj {
        let mut members: Vec<(String, Option<String>, bool, Instant)> = self
            .g
            .members
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    m.group_instance_id.clone(),
                    m.assignment.is_some(),
                    m.last_heartbeat,
                )
            })
            .collect();
        members.sort();
        let mut idx: Vec<(String, String)> = self
            .g
            .static_members
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        idx.sort();
        let mut joined: Vec<String> = self.g.joined_this_round.iter().cloned().collect();
        joined.sort();
        (
            self.g.state,
            self.g.leader_id.clone(),
            self.g.protocol_name.clone(),
            self.g.rebalance_from_empty,
            self.clock,
            members,
            idx,
            joined,
        )
    }
}

krabka_macros::projection_identity!(GrpState, proj);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum Act {
    JoinDynamic(&'static str),
    JoinStatic(&'static str, &'static str), // (instance_id, member_id)
    Heartbeat(&'static str),
    Leave(&'static str),
    CompleteRebalance,
    Sync,
    ExpireTick,
}
