//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`).
//!
//! The model state holds the REAL `ReplicaState` and drives the production
//! `install_isr` / `update_follower_leo` / `recompute_hw_for_leader_append`.
//! The BFS checker explores every interleaving of leader append, follower
//! fetch, and ISR shrink/expand. It asserts that the partition-replication
//! safety invariants never break, above all no-committed-data-loss. Design:
//! `crates/broker/docs/replication-isr-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! this module fences each run with `within_boundary` + `target_state_count`.
//! You MUST run each config under the host memory watchdog while you tune the
//! bounds.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    time::Instant,
};

use krabka_log::Offset;
use krabka_raft::NodeId;
use stateright::{Model, Property};

use super::ReplicaState;
use crate::model_check::check_model;

/// Hard backstop on generated states. It bounds host memory even if
/// `within_boundary` is looser than intended.
const MAX_STATES: usize = 200_000;
/// Depth backstop; must exceed each config's reachable-graph diameter.
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_SAFETY: usize = 174;
const PINNED_UNIQUE_STATES_OVERSHOOT: usize = 174;

/// Bounded model config. It is held here, not in the fingerprinted state.
struct IsrModel {
    /// Constant injected `now`. The model does not model wall-clock time.
    /// ISR shrink/expand is an explicit action, not a time-based decision.
    t0: Instant,
    /// `replicas[0]` is the fixed leader; the rest are followers.
    replicas: Vec<NodeId>,
    /// Leader-LEO / follower-LEO cap.
    max_offset: i64,
    /// When set, followers may report a LEO above `leader_leo`. This is the
    /// clamp test.
    test_overshoot: bool,
}

impl IsrModel {
    fn safety(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            max_offset,
            test_overshoot: false,
        }
    }

    fn overshoot(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(2),
                krabka_audit::NodeId(3),
            ],
            max_offset,
            test_overshoot: true,
        }
    }

    fn leader(&self) -> NodeId {
        self.replicas[0]
    }

    fn followers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas[1..].iter().copied()
    }
}

/// The fingerprinted model state: the REAL core + the leader's own LEO.
#[derive(Clone, Debug)]
struct IsrState {
    rs: ReplicaState,
    leader_leo: Offset,
}

impl IsrState {
    /// Normalized, timestamp-free projection for Eq/Hash. The real state holds
    /// non-`Hash` `HashMap`/`HashSet` and non-deterministic timestamps. Neither
    /// of them is safety-relevant here.
    fn project(&self) -> (Vec<NodeId>, Vec<(NodeId, Offset)>, Offset, i32, Offset) {
        let mut isr: Vec<NodeId> = self.rs.isr.iter().copied().collect();
        isr.sort_unstable();
        let mut pf: Vec<(NodeId, Offset)> = self
            .rs
            .per_follower
            .iter()
            .map(|(k, v)| (*k, v.leo))
            .collect();
        pf.sort_unstable();
        (
            isr,
            pf,
            self.rs.hw,
            self.rs.current_leader_epoch.0,
            self.leader_leo,
        )
    }
}

impl PartialEq for IsrState {
    fn eq(&self, other: &Self) -> bool {
        self.project() == other.project()
    }
}
impl Eq for IsrState {}
impl Hash for IsrState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project().hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum IsrAction {
    /// The leader appends one record and recomputes the HW. `leader_leo`
    /// increases by 1.
    LeaderAppend,
    /// A follower reports `leo` in a fetch.
    FollowerFetch { follower: NodeId, leo: Offset },
    /// The controller installs a new committed ISR.
    InstallIsr { isr: Vec<NodeId> },
}

impl Model for IsrModel {
    type State = IsrState;
    type Action = IsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Fresh leader: full replica set in the ISR, followers seeded at 0.
        let mut rs = ReplicaState::new();
        rs.install_isr(&self.replicas, &self.replicas, self.leader(), self.t0);
        vec![IsrState {
            rs,
            leader_leo: Offset(0),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let leader = self.leader();

        if state.leader_leo < self.max_offset {
            actions.push(IsrAction::LeaderAppend);
        }

        // Follower fetches: advance by one or jump to the leader's LEO. Targets
        // are monotonic (never below the follower's current LEO) — a real
        // follower's reported LEO never regresses, which is what keeps HW
        // monotone. `test_overshoot` additionally probes the defensive clamp.
        for f in self.followers() {
            let cur = state.rs.per_follower.get(&f).map_or(Offset(0), |s| s.leo);
            let mut targets: Vec<Offset> = Vec::new();
            if cur < state.leader_leo {
                targets.push(cur + 1);
                targets.push(state.leader_leo);
            }
            if self.test_overshoot {
                targets.push(state.leader_leo + 1);
            }
            targets.sort_unstable();
            targets.dedup();
            for leo in targets {
                actions.push(IsrAction::FollowerFetch { follower: f, leo });
            }
        }

        // ISR changes: every subset of replicas that contains the leader and
        // differs from the current ISR. Expansion only admits followers whose
        // log end reached the HW (per_follower.leo >= hw) — the high-watermark
        // half of the leader's rule, Kafka's `Partition.isFollowerInSync`;
        // without it the model would report a false data-loss violation.
        let cur_isr: HashSet<NodeId> = state.rs.isr.clone();
        let follower_vec: Vec<NodeId> = self.followers().collect();
        for mask in 0u32..(1u32 << follower_vec.len()) {
            let mut isr: Vec<NodeId> = vec![leader];
            for (i, &f) in follower_vec.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    isr.push(f);
                }
            }
            let isr_set: HashSet<NodeId> = isr.iter().copied().collect();
            if isr_set == cur_isr {
                continue;
            }
            let expansion_ok = isr
                .iter()
                .filter(|&&n| n != leader && !cur_isr.contains(&n))
                .all(|f| state.rs.per_follower.get(f).map_or(Offset(0), |s| s.leo) >= state.rs.hw);
            if !expansion_ok {
                continue;
            }
            isr.sort_unstable();
            actions.push(IsrAction::InstallIsr { isr });
        }
    }

    krabka_macros::model_transition!(last, action, state; {
            match action {
                IsrAction::LeaderAppend => {
                    if state.leader_leo >= self.max_offset {
                        return None;
                    }
                    state.leader_leo += 1;
                    state.rs.recompute_hw_for_leader_append(state.leader_leo);
                }
                IsrAction::FollowerFetch { follower, leo } => {
                    state
                        .rs
                        .update_follower_leo(follower, leo, state.leader_leo, self.t0);
                }
                IsrAction::InstallIsr { isr } => {
                    state
                        .rs
                        .install_isr(&isr, &self.replicas, self.leader(), self.t0);
                }
            }
            // Transition invariant (kept out of the fingerprinted state): the
            // high-watermark never regresses.
            assert2::assert!(
                state.rs.hw >= last.rs.hw,
                "HWM regressed: {} -> {}",
                last.rs.hw,
                state.rs.hw
            );
            Some(state)

    });

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hw_within_leader", |_, s: &IsrState| {
                s.rs.hw <= s.leader_leo
            }),
            // No-committed-data-loss: every ISR member holds every committed
            // record. A missing per_follower entry for an ISR member counts as a
            // violation, although compute_hw reads one as log end -1 and holds
            // the HW for it.
            Property::always("no_data_loss", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                s.rs.isr
                    .iter()
                    .filter(|&&f| f != leader)
                    .all(|f| s.rs.per_follower.get(f).is_some_and(|st| st.leo >= s.rs.hw))
            }),
            Property::always("leo_clamped", |_, s: &IsrState| {
                s.rs.per_follower.values().all(|st| st.leo <= s.leader_leo)
            }),
            Property::always("hw_nonneg", |_, s: &IsrState| s.rs.hw >= 0),
            Property::always("leader_in_isr", |m: &IsrModel, s: &IsrState| {
                s.rs.isr.contains(&m.leader())
            }),
            Property::sometimes("can_advance_hw", |_, s: &IsrState| s.rs.hw > 0),
            Property::sometimes("can_reach_leader_leo", |_, s: &IsrState| {
                s.leader_leo > 0 && s.rs.hw == s.leader_leo
            }),
            Property::sometimes("can_pin_below_leader", |_, s: &IsrState| {
                s.rs.hw > 0 && s.rs.hw < s.leader_leo
            }),
            Property::sometimes("can_shrink_isr", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                m.replicas.iter().any(|&r| {
                    r != leader && !s.rs.isr.contains(&r) && s.rs.per_follower.contains_key(&r)
                })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_leo <= self.max_offset
            && state.rs.hw <= self.max_offset
            && state
                .rs
                .per_follower
                .values()
                .all(|s| s.leo <= self.max_offset)
    }
}

/// Runs one bounded config to completion. Asserts that the run was exhaustive,
/// that the cap or the depth did not truncate it, and that all properties hold.
fn run(model: IsrModel, label: &str, pinned_unique_states: usize) {
    check_model(
        model,
        label,
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        pinned_unique_states,
    );
}

#[test]
fn isr_safety() {
    run(
        IsrModel::safety(3),
        "isr_safety",
        PINNED_UNIQUE_STATES_SAFETY,
    );
}

#[test]
fn isr_overshoot() {
    run(
        IsrModel::overshoot(3),
        "isr_overshoot",
        PINNED_UNIQUE_STATES_OVERSHOOT,
    );
}
