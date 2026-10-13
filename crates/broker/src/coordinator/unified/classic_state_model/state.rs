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
#[derive(PartialEq, Eq, Hash)]
struct Proj {
    state: GroupState,
    leader_id: Option<String>,
    protocol_name: Option<String>,
    rebalance_from_empty: bool,
    clock: i64,
    members: Vec<MemberProj>,
    static_index: Vec<StaticMemberProj>,
    joined: Vec<String>,
}

/// One member of the projection: its identity, whether it holds an
/// assignment, and its last heartbeat.
#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MemberProj {
    member_id: String,
    group_instance_id: Option<String>,
    has_assignment: bool,
    last_heartbeat: Instant,
}

/// One `instance_id` to `member_id` entry of the static membership index.
#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
struct StaticMemberProj {
    instance_id: String,
    member_id: String,
}

impl GrpState {
    fn proj(&self) -> Proj {
        let mut members: Vec<MemberProj> = self
            .g
            .members
            .iter()
            .map(|(id, m)| MemberProj {
                member_id: id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                has_assignment: m.assignment.is_some(),
                last_heartbeat: m.last_heartbeat,
            })
            .collect();
        members.sort();
        let mut static_index: Vec<StaticMemberProj> = self
            .g
            .static_members
            .iter()
            .map(|(k, v)| StaticMemberProj {
                instance_id: k.clone(),
                member_id: v.clone(),
            })
            .collect();
        static_index.sort();
        let mut joined: Vec<String> = self.g.joined_this_round.iter().cloned().collect();
        joined.sort();
        Proj {
            state: self.g.state,
            leader_id: self.g.leader_id.clone(),
            protocol_name: self.g.protocol_name.clone(),
            rebalance_from_empty: self.g.rebalance_from_empty,
            clock: self.clock,
            members,
            static_index,
            joined,
        }
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
