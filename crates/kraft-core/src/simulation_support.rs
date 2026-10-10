//! Shared deterministic mechanics for the browser and storage simulation harnesses.

use std::collections::BTreeSet;

use crate::{Action, Epoch, Event, LogEnd, NodeId, QuorumStateMachine, Role, SimInstant};

/// Stop after two timer rounds leave the cluster fingerprint unchanged.
pub struct StableFingerprint<F> {
    previous: F,
    unchanged: u32,
}

impl<F: PartialEq> StableFingerprint<F> {
    #[must_use]
    pub fn new(previous: F) -> Self {
        Self {
            previous,
            unchanged: 0,
        }
    }

    /// Observe this round and report whether two consecutive rounds were stable.
    pub fn observe(&mut self, fingerprint: F) -> bool {
        if fingerprint == self.previous {
            self.unchanged = self.unchanged.saturating_add(1);
        } else {
            self.unchanged = 0;
            self.previous = fingerprint;
        }
        self.unchanged >= 2
    }
}

/// Static simulator voters have no directory identity or network endpoints.
#[must_use]
pub fn voter_set(ids: &[NodeId]) -> krabka_voters::VoterSet {
    krabka_voters::VoterSet::from_voters(ids.iter().map(|&id| krabka_voters::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: Vec::new(),
        kraft_version: krabka_voters::KRaftVersionRange::default(),
    }))
}

/// Translate the consensus actions that only send messages.
/// `None` leaves local log and timer effects to the harness.
#[must_use]
pub fn action_messages(
    source: NodeId,
    action: &Action,
    voters: &[NodeId],
    peers: &[NodeId],
    log_end: LogEnd,
) -> Option<Vec<(NodeId, Event)>> {
    let recipients = match action {
        Action::SendVoteRequest { .. } => voters,
        Action::SendBeginQuorumEpoch { .. } | Action::SendEndQuorumEpoch { .. } => peers,
        Action::ReplyVote { to, epoch, granted } => {
            return Some(vec![(
                *to,
                Event::ReceiveVoteResponse {
                    from: source,
                    epoch: *epoch,
                    vote_granted: *granted,
                },
            )]);
        }
        _ => return None,
    };
    Some(
        recipients
            .iter()
            .copied()
            .filter(|peer| *peer != source)
            .map(|peer| {
                let event = match action {
                    Action::SendVoteRequest { epoch, pre_vote } => Event::ReceiveVoteRequest {
                        from: source,
                        cluster_id: None,
                        voter_id: peer,
                        voter_directory_id: uuid::Uuid::nil(),
                        candidate_epoch: *epoch,
                        candidate: source,
                        candidate_directory_id: uuid::Uuid::nil(),
                        candidate_log_end: log_end,
                        pre_vote: *pre_vote,
                    },
                    Action::SendBeginQuorumEpoch { epoch } => Event::ReceiveBeginQuorumEpoch {
                        leader_id: source,
                        leader_epoch: *epoch,
                    },
                    Action::SendEndQuorumEpoch { epoch, .. } => Event::ReceiveEndQuorumEpoch {
                        leader_id: source,
                        leader_epoch: *epoch,
                        successor_rank: crate::SuccessorRank::default(),
                    },
                    _ => unreachable!("message action selected above"),
                };
                (peer, event)
            })
            .collect(),
    )
}

/// Answer a leader-side fetch only when data or divergence can advance the follower.
#[must_use]
pub fn fetch_response(
    leader: NodeId,
    epoch: Epoch,
    actions: &[Action],
    leader_end: i64,
    follower_end: i64,
) -> Option<Event> {
    let diverging = actions.iter().find_map(|action| match action {
        Action::ReplyDivergingEpoch(point) => Some(*point),
        _ => None,
    });
    (diverging.is_some() || follower_end < leader_end).then_some(Event::ReceiveFetchResponse {
        leader_id: leader,
        leader_epoch: epoch,
        diverging,
    })
}

/// The timer wheel includes a leader heartbeat in addition to core timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationTimer {
    Election,
    Fetch,
    Heartbeat,
    CheckQuorum,
}

/// Earliest timer, retaining the input node order and timer order on equal deadlines.
#[must_use]
pub fn earliest_timer(
    nodes: impl Iterator<Item = (NodeId, [Option<SimInstant>; 4])>,
) -> Option<(SimInstant, NodeId, SimulationTimer)> {
    let mut best = None;
    for (id, deadlines) in nodes {
        for (deadline, kind) in deadlines.into_iter().zip([
            SimulationTimer::Election,
            SimulationTimer::Fetch,
            SimulationTimer::Heartbeat,
            SimulationTimer::CheckQuorum,
        ]) {
            if let Some(deadline) = deadline
                && best.is_none_or(|(current, _, _)| deadline < current)
            {
                best = Some((deadline, id, kind));
            }
        }
    }
    best
}

/// Disarm the timer that just fired before applying its event.
pub fn clear_timer(
    kind: SimulationTimer,
    election: &mut Option<SimInstant>,
    fetch: &mut Option<SimInstant>,
    heartbeat: &mut Option<SimInstant>,
    quorum: &mut Option<SimInstant>,
) {
    *match kind {
        SimulationTimer::Election => election,
        SimulationTimer::Fetch => fetch,
        SimulationTimer::Heartbeat => heartbeat,
        SimulationTimer::CheckQuorum => quorum,
    } = None;
}

/// Enforce per-role timer ownership, preserving existing leader heartbeats.
pub fn reconcile_timers(
    role: &Role,
    election: &mut Option<SimInstant>,
    fetch: &mut Option<SimInstant>,
    heartbeat: &mut Option<SimInstant>,
    quorum: &mut Option<SimInstant>,
    heartbeat_deadline: SimInstant,
) {
    match role {
        Role::Leader { .. } => {
            *election = None;
            *fetch = None;
            heartbeat.get_or_insert(heartbeat_deadline);
        }
        Role::Follower { .. } | Role::Observer { .. } => {
            *election = None;
            *heartbeat = None;
            *quorum = None;
        }
        _ => {
            *fetch = None;
            *heartbeat = None;
            *quorum = None;
        }
    }
}

/// A fetch watchdog can poll again while the node's leader is reachable.
#[must_use]
pub fn reachable_leader(
    role: &Role,
    follower: NodeId,
    partitioned: &BTreeSet<NodeId>,
    is_leader: impl FnOnce(NodeId) -> bool,
) -> Option<NodeId> {
    let leader = match role {
        Role::Follower { leader_id, .. }
        | Role::Observer {
            leader_id: Some(leader_id),
            ..
        } => *leader_id,
        _ => return None,
    };
    (!partitioned.contains(&follower) && !partitioned.contains(&leader) && is_leader(leader))
        .then_some(leader)
}

/// A node fingerprint uses the core's leader watermark when it leads.
#[must_use]
pub fn fingerprint(
    id: NodeId,
    machine: &QuorumStateMachine,
    records: usize,
    hwm: i64,
) -> (NodeId, &'static str, Epoch, usize, i64) {
    let hwm = match machine.role() {
        Role::Leader { high_watermark, .. } => *high_watermark,
        _ => hwm,
    };
    (
        id,
        machine.role().name(),
        machine.quorum_state().leader_epoch,
        records,
        hwm,
    )
}

/// Append one leader epoch per simulated record.
pub fn append_epochs(epochs: &mut Vec<Epoch>, epoch: Epoch, count: usize) {
    for _ in 0..count {
        epochs.push(epoch);
    }
}

/// Clamp negative cuts to zero; a cut beyond the log preserves it.
pub fn truncate_epochs(epochs: &mut Vec<Epoch>, offset: i64) {
    epochs.truncate(usize::try_from(offset.max(0)).unwrap_or(usize::MAX));
}
