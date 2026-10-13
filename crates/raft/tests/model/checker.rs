//! The `stateright::Model` implementation: the initial states, the enabled
//! action set, the transition relation, the safety and witness properties, and
//! the state-space boundary. The trait demands one `impl` block, so the whole
//! checker interface lives in this one file.

use {
    super::{
        commit::settle_committed,
        config::ConsensusModel,
        log::ModelLog,
        spec::{APPENDER_COUNT, AppenderId, ClientId, KraftLogSpec, LogOp},
        state::{
            CommitPoint, ModelAction, ModelState, NodeModel, StepWitness, is_leader,
            live_authority, node_high_watermark,
        },
    },
    krabka_raft::kraft::{
        QuorumStateMachine,
        action::TimerKind,
        event::Event,
        role::Role,
        types::{Epoch, LogView, NodeId, QuorumState},
    },
    stateright::{
        Model, Property,
        semantics::{ConsistencyTester, LinearizabilityTester},
    },
    std::collections::{BTreeMap, BTreeSet},
};

fn append_to_leader(state: &mut ModelState, leader: NodeId, client: ClientId, value: u64) -> i64 {
    let epoch = state.nodes[&leader].machine.quorum_state().leader_epoch;
    let offset = state.nodes[&leader].log.end_offset();
    let _ = state
        .linz
        .on_invoke(client, LogOp::Append(value))
        .expect("fresh client id has no in-flight op");
    state
        .nodes
        .get_mut(&leader)
        .expect("leader exists")
        .log
        .append_in_epoch(epoch, 1);
    offset
}

impl Model for ConsensusModel {
    type Action = ModelAction;
    type State = ModelState;

    fn init_states(&self) -> Vec<Self::State> {
        let voters = self.voter_set();
        let mut nodes = BTreeMap::new();
        for &id in &self.voter_ids {
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                Self::election_timeout_of(id),
            );
            nodes.insert(id, NodeModel {
                machine,
                log: ModelLog::default(),
                high_watermark: 0,
            });
        }
        vec![ModelState {
            nodes,
            network: BTreeSet::new(),
            linz: LinearizabilityTester::new(KraftLogSpec::default()),
            pending: BTreeMap::new(),
            wal_frontiers: self.voter_ids.iter().map(|&id| (id, 0)).collect(),
            appenders_seen: BTreeSet::new(),
            committed: Vec::new(),
            committed_epochs: Vec::new(),
            appends_issued: 0,
            crashed: BTreeSet::new(),
            check_quorum_violation: false,
            leader_resigned: false,
            step_witness: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every in-flight message is independently deliverable (unordered net).
        // Loss/duplication, when enabled, offer a drop and a duplicate-deliver
        // for each in-flight message.
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
            if self.enable_loss_dup {
                actions.push(ModelAction::DropMsg(env.clone()));
                actions.push(ModelAction::DuplicateDeliver(env.clone()));
            }
        }
        // Any voter that is not currently leader may suffer an election timeout;
        // any follower/observer may suffer a fetch timeout. The core ignores
        // inapplicable ones, so over-offering is sound (it only adds interleavings).
        // Crashed nodes are unreachable: offered no timeouts.
        for (&id, node) in &state.nodes {
            if state.crashed.contains(&id) {
                continue;
            }
            match node.machine.role() {
                // A leader runs neither watchdog; its one timer is the
                // check-quorum window that ends the epoch it has lost.
                Role::Leader { .. } => {
                    if self.enable_check_quorum {
                        actions.push(ModelAction::Timeout(id, TimerKind::CheckQuorum));
                    }
                }
                Role::Follower { .. } | Role::Observer { .. } => {
                    actions.push(ModelAction::Timeout(id, TimerKind::Fetch));
                    actions.push(ModelAction::Timeout(id, TimerKind::Election));
                }
                _ => actions.push(ModelAction::Timeout(id, TimerKind::Election)),
            }
        }
        // Crash/recover, capped at `max_crashes` concurrently crashed.
        if state.crashed.len() < self.max_crashes {
            for &id in &self.voter_ids {
                if !state.crashed.contains(&id) {
                    actions.push(ModelAction::Crash(id));
                }
            }
        }
        for &id in &state.crashed {
            actions.push(ModelAction::Recover(id));
        }
        if self.enable_append_via
            && let Some((&offset, _)) = state
                .pending
                .iter()
                .find(|(_, (_, _, point))| *point == CommitPoint::WalQuorumDurable)
        {
            let end_offset = offset + 1;
            // WAL members are exchangeable here: this focused config has no
            // WAL-node crash action, and only the majority-th frontier is
            // observed. Advance the first eligible label as a symmetry normal
            // form instead of exploring every permutation of identical fsyncs.
            if let Some(&id) = self.voter_ids.iter().find(|id| {
                !state.crashed.contains(id)
                    && state.wal_frontiers.get(id).copied().unwrap_or(0) < end_offset
            }) {
                actions.push(ModelAction::WalFsync(id, end_offset));
            }
        }
        // A client appends to the single current (live) leader (only when the
        // target is unambiguous and the append budget remains). A fresh client id
        // per append keeps every linearizability "thread" single-op.
        let leaders: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
            .map(|(&id, _)| id)
            .collect();
        if state.appends_issued < self.max_appends {
            let client = ClientId::from(state.appends_issued) + 1;
            let value = u64::from(state.appends_issued) + 1;
            if self.enable_append_via {
                // Stateless appenders can enter through any live broker. Do
                // not reintroduce the old `leaders.len() == 1` emission gate:
                // routing resolves the highest-epoch live authority when the
                // action executes, including during authority handoff.
                if live_authority(state).is_some() {
                    // Each append uses the next canonical appender label. This
                    // represents every permutation of two exchangeable labels
                    // while retaining the target concurrent-appender path.
                    let appender = AppenderId::try_from(state.appends_issued)
                        .expect("append bound fits the appender domain");
                    assert2::assert!(appender < APPENDER_COUNT);
                    actions.push(ModelAction::AppendVia(appender, client, value));
                }
            } else if leaders.len() == 1 {
                actions.push(ModelAction::ClientAppend(client, value));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        state.step_witness = None;
        match action {
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                // A crashed destination is unreachable: the message is consumed
                // (removed) but produces no transition.
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::DropMsg(env) => {
                // Network loss: remove without delivering. No-op if already gone.
                if !state.network.remove(&env) {
                    return None;
                }
            }
            ModelAction::DuplicateDeliver(env) => {
                // Network duplication: deliver a copy, leave the original queued.
                if !state.network.contains(&env) {
                    return None;
                }
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::Crash(id) => {
                if !state.crashed.insert(id) {
                    return None;
                }
                // Omission model: drop all messages to/from the crashed node.
                state.network.retain(|e| e.src != id && e.dst != id);
            }
            ModelAction::Recover(id) => {
                if !state.crashed.remove(&id) {
                    return None;
                }
            }
            ModelAction::Timeout(id, kind) => {
                let event = match kind {
                    TimerKind::Election => Event::ElectionTimeout,
                    TimerKind::Fetch => Event::FetchTimeout,
                    TimerKind::CheckQuorum => Event::CheckQuorumTimeout,
                };
                self.step(&mut state, id, event);
                if kind == TimerKind::CheckQuorum && self.voter_ids.len() > 1 {
                    // The core re-arms this timer only when a majority has
                    // fetched, so an expiry that leaves the node still leading
                    // means an isolated leader kept its epoch. That is the
                    // whole defect check-quorum exists to close.
                    state.check_quorum_violation |= is_leader(&state.nodes[&id]);
                }
                state.leader_resigned |= state
                    .nodes
                    .values()
                    .any(|n| matches!(n.machine.role(), Role::Resigned));
            }
            ModelAction::ClientAppend(client, value) => {
                let leader = state
                    .nodes
                    .iter()
                    .find(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
                    .map(|(&id, _)| id)?;
                // Record the invocation, append at the leader, track until committed.
                let offset = append_to_leader(&mut state, leader, client, value);
                state
                    .pending
                    .insert(offset, (client, value, CommitPoint::KRaftHighWatermark));
                state.appends_issued += 1;
            }
            ModelAction::AppendVia(appender, client, value) => {
                let leader = live_authority(&state)?;
                let offset = append_to_leader(&mut state, leader, client, value);
                state.appenders_seen.insert(appender);
                state
                    .pending
                    .insert(offset, (client, value, CommitPoint::WalQuorumDurable));
                state.appends_issued += 1;
            }
            ModelAction::WalFsync(node, end_offset) => {
                if state.crashed.contains(&node) {
                    return None;
                }
                state
                    .wal_frontiers
                    .entry(node)
                    .and_modify(|frontier| *frontier = (*frontier).max(end_offset));
            }
        }
        // After any transition, return the contiguous prefix that crossed its
        // configured durability boundary.
        settle_committed(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Anti-vacuity witness: a leader is actually elected in some state.
            Property::sometimes("leader_elected", |_, s: &ModelState| {
                s.nodes.values().any(is_leader)
            }),
            // Safety: a leader whose check-quorum window expires must step
            // down. Without it, an old leader isolated by a partition holds its
            // epoch indefinitely — KIP-996 pre-vote never bumps its epoch, so
            // nothing else would ever tell it the majority side has moved on.
            // `election_safety` cannot see that: the two leaders hold different
            // epochs.
            Property::always(
                "check_quorum_expiry_ends_leadership",
                |_, s: &ModelState| !s.check_quorum_violation,
            ),
            // Anti-vacuity witness for the property above: the resignation is
            // actually reached, rather than the check holding because no
            // check-quorum expiry was ever explored.
            Property::sometimes("leader_resigns", |m: &ConsensusModel, s: &ModelState| {
                // Only required where the expiry is offered; a config that
                // does not explore it satisfies this trivially.
                !m.enable_check_quorum || s.leader_resigned
            }),
            // Safety: at most one leader per leader-epoch.
            Property::always("election_safety", |_, s: &ModelState| {
                let mut by_epoch: BTreeMap<Epoch, NodeId> = BTreeMap::new();
                for (&id, n) in &s.nodes {
                    if is_leader(n) {
                        let epoch = n.machine.quorum_state().leader_epoch;
                        if let Some(&other) = by_epoch.get(&epoch)
                            && other != id
                        {
                            return false;
                        }
                        by_epoch.insert(epoch, id);
                    }
                }
                true
            }),
            // Safety: the committed log is linearizable — there exists a single
            // total order of client appends consistent with every observed
            // invoke/return. A lost or reordered committed entry has no such
            // serialization.
            Property::always("linearizable", |_, s: &ModelState| {
                s.linz.serialized_history().is_some()
            }),
            Property::always("assigned_offsets_gap_free", |_, s: &ModelState| {
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(offset, value)| u64::try_from(offset + 1).is_ok_and(|v| *value == v))
            }),
            Property::always("committed_values_unique", |_, s: &ModelState| {
                s.committed.iter().copied().collect::<BTreeSet<_>>().len() == s.committed.len()
            }),
            // Anti-vacuity witness: a CLIENT append is actually committed.
            // Without this, `linearizable` could hold vacuously because no
            // client value ever committed (a control-record-only HWM advance
            // would not count).
            Property::sometimes("entry_committed", |m: &ConsensusModel, s: &ModelState| {
                // Only required when client appends are enabled; a no-append
                // config (election focus) satisfies this trivially.
                m.max_appends == 0 || !s.committed.is_empty()
            }),
            Property::sometimes(
                "two_appenders_concurrent",
                |m: &ConsensusModel, s: &ModelState| {
                    !m.enable_append_via
                        || (s.appenders_seen.len() == usize::from(APPENDER_COUNT)
                            && s.pending.len() == usize::from(APPENDER_COUNT))
                },
            ),
            // Safety (Raft log matching): two logs may diverge only as an
            // uncommitted suffix — if they disagree on the epoch at some offset
            // `k`, they must not agree again at any later offset (equal entries
            // imply equal prefixes). Re-agreement after disagreement is a true
            // matching violation.
            Property::always("log_matching", |_, s: &ModelState| {
                let logs: Vec<&Vec<Epoch>> = s.nodes.values().map(|n| &n.log.epochs).collect();
                for i in 0..logs.len() {
                    for j in (i + 1)..logs.len() {
                        let (a, b) = (logs[i], logs[j]);
                        let common = a.len().min(b.len());
                        for k in 0..common {
                            if a[k] != b[k] && (k + 1..common).any(|m| a[m] == b[m]) {
                                return false;
                            }
                        }
                    }
                }
                true
            }),
            // Safety (Raft leader completeness, Figure 8): a leader of epoch `e`
            // holds every entry committed in an epoch at or below `e`, stamped
            // with the epoch it committed under. Entries of a LATER epoch are
            // excluded: a leader that has been superseded and not yet heard so
            // legitimately lags behind its successor's commits, and holding it
            // to them would flag ordinary staleness rather than a safety
            // failure.
            //
            // A vote granted to a candidate whose log is behind the granting
            // majority is exactly what breaks this: the winner would open a new
            // epoch missing an already-acknowledged entry, and its replication
            // would then overwrite it everywhere.
            Property::always("leader_completeness", |_, s: &ModelState| {
                s.nodes.values().filter(|n| is_leader(n)).all(|n| {
                    let leader_epoch = n.machine.quorum_state().leader_epoch;
                    s.committed_epochs
                        .iter()
                        .enumerate()
                        .filter(|&(_, &epoch)| epoch <= leader_epoch)
                        .all(|(offset, &epoch)| {
                            i64::try_from(offset)
                                .is_ok_and(|offset| n.log.epoch_at(offset) == Some(epoch))
                        })
                })
            }),
            // Anti-vacuity witness for the property above: a voter actually
            // refuses a candidate for log recency alone, which is the only
            // reason `leader_completeness` can hold under a majority that has
            // fallen behind. The transition that delivers the request
            // establishes the reason by running the voter's real machine on
            // the same request with only the candidate's log end changed.
            Property::sometimes(
                "stale_candidate_refused",
                |m: &ConsensusModel, s: &ModelState| {
                    // Only reachable where a majority can fall behind a
                    // committed prefix: three voters that take client appends.
                    if m.voter_ids.len() < 3 || m.max_appends == 0 {
                        return true;
                    }
                    s.step_witness == Some(StepWitness::StaleCandidateRefused)
                },
            ),
            // Anti-vacuity witness for `log_matching`: a follower whose log
            // disagrees with the leader's is cut back by the production
            // truncation path, since the model never overwrites a log.
            Property::sometimes(
                "divergent_log_truncated",
                |m: &ConsensusModel, s: &ModelState| {
                    // Required where a node can be cut off from a leader it
                    // then outlives: the crash configs. Without a crash the
                    // bounded configs never leave a follower holding an entry
                    // the next leader lacks.
                    m.max_crashes == 0 || s.step_witness == Some(StepWitness::DivergentLogTruncated)
                },
            ),
            // Safety: no node's committed high-watermark exceeds its own log end
            // (a node cannot have committed past what it physically holds).
            Property::always("hwm_within_log", |_, s: &ModelState| {
                s.nodes
                    .values()
                    .all(|n| node_high_watermark(n) <= n.log.end_offset())
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound the space HARD: stateright BFS/DFS keeps every visited unique
        // state in memory, so loose bounds OOM the machine. Cap in-flight
        // messages and the maximum leader epoch per the model's config.
        state.network.len() <= self.max_inflight
            && state
                .nodes
                .values()
                .all(|n| n.machine.quorum_state().leader_epoch <= self.max_epoch)
    }
}
