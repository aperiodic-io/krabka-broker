//! Translation of the actions the real consensus core emits into mutations of
//! the model state. This is the model's counterpart to the sim harness's
//! scheduler, and it is the one place production `Action`s become network
//! envelopes, log writes, and high-watermark movement.

use {
    super::{
        config::ConsensusModel,
        state::{Envelope, ModelState, StepWitness, node_high_watermark},
    },
    krabka_raft::kraft::{
        action::Action,
        event::Event,
        types::{LogView, NodeId, SimInstant},
    },
};

/// Constant logical time. Timeouts are modeled as nondeterministic actions, so
/// the core never needs a varying clock. A constant `now` keeps the role
/// deadlines constant and the state space finite.
const NOW: SimInstant = SimInstant(0);

impl ConsensusModel {
    /// Translates one `Action` emitted by `id` into mutations of `state`, which
    /// are the network envelopes, the log, and the HWM. This is ported from
    /// `apply_action` in `sim_harness`, without the timer arming, because here
    /// the timeouts are model actions.
    // A single match over every `Action` variant: long by nature, and `action`
    // is logically consumed (translated) here, so take it by value.
    pub(super) fn apply_action(&self, state: &mut ModelState, id: NodeId, action: &Action) {
        match action.clone() {
            Action::SendVoteRequest { epoch, pre_vote } => {
                let cand_log = state.nodes[&id].log.log_end();
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveVoteRequest {
                                from: id,
                                cluster_id: None,
                                voter_id: peer,
                                voter_directory_id: uuid::Uuid::nil(),
                                candidate_epoch: epoch,
                                candidate: id,
                                candidate_directory_id: uuid::Uuid::nil(),
                                candidate_log_end: cand_log,
                                pre_vote,
                            },
                        });
                    }
                }
            }
            Action::ReplyVote { to, epoch, granted } => {
                state.network.insert(Envelope {
                    src: id,
                    dst: to,
                    event: Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                });
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveBeginQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        });
                    }
                }
            }
            Action::SendEndQuorumEpoch { epoch, .. } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveEndQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                                successor_rank: krabka_raft::kraft::event::SuccessorRank::default(),
                            },
                        });
                    }
                }
            }
            Action::SendFetch { leader_id } => Self::apply_fetch_action(state, id, leader_id),
            Action::AppendLeaderChange { epoch } => {
                state
                    .nodes
                    .get_mut(&id)
                    .expect("appender exists")
                    .log
                    .append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = state.nodes.get_mut(&id).expect("leader exists");
                node.high_watermark = hwm;
                let leader_log = node.log.clone();
                // The HWM rides the leader's fetch responses to followers; mirror
                // that by pushing the committed boundary to peers, clamped to
                // what each has actually replicated. A peer whose log disagrees
                // with the leader's would be answered with a diverging epoch
                // instead, which carries no high watermark.
                let peers: Vec<NodeId> = self
                    .voter_ids
                    .iter()
                    .copied()
                    .filter(|&p| p != id)
                    .collect();
                for peer in peers {
                    if let Some(p) = state.nodes.get_mut(&peer)
                        && p.log.is_prefix_of(&leader_log)
                    {
                        p.high_watermark = hwm.min(p.log.end_offset());
                    }
                }
            }
            Action::TruncateTo(point) => {
                let node = state.nodes.get_mut(&id).expect("truncator exists");
                let before = node.log.end_offset();
                node.log.truncate_to(point.offset);
                // A node cannot retain a committed-prefix marker past what it
                // now physically holds: clamp its high-watermark to the new log
                // end (the eager HWM propagation in `AdvanceHighWatermark` may
                // have set it higher before this divergence-driven truncation).
                node.high_watermark = node.high_watermark.min(node.log.end_offset());
                if node.log.end_offset() < before {
                    state.step_witness = Some(StepWitness::DivergentLogTruncated);
                }
            }
            // Timer arming is modeled by the `Timeout` action set; durable-state
            // + role-transition signals have no cross-node effect in the model.
            // A diverging epoch rides in the fetch response built above.
            Action::ResetTimer { .. }
            | Action::TransitionedTo(_)
            | Action::PersistQuorumState
            | Action::ReplyDivergingEpoch(_) => {}
        }
    }

    fn apply_fetch_action(state: &mut ModelState, id: NodeId, leader_id: NodeId) {
        // Replication abstraction: the records a fetch would carry back are
        // applied when the fetch is sent, and only when the leader's log
        // extends the follower's. The follower then fetches at its advanced
        // tip, so the leader can advance the HWM. A follower whose log
        // disagrees with the leader's keeps it: its fetch below draws a
        // diverging-epoch response from the production `handle_fetch`, and the
        // production `handle_fetch_response` truncates it.
        if leader_id != id
            && state
                .nodes
                .get(&leader_id)
                .is_some_and(|n| n.machine.role().is_leader())
        {
            let leader_log = state.nodes[&leader_id].log.clone();
            let leader_hwm = node_high_watermark(&state.nodes[&leader_id]);
            let f = state.nodes.get_mut(&id).expect("fetcher exists");
            if f.log.extend_from(&leader_log) {
                f.high_watermark = leader_hwm.min(f.log.end_offset());
            }
        }
        let (fetch_epoch, fetch_offset) = {
            let log = &state.nodes[&id].log;
            (log.last_epoch(), log.end_offset())
        };
        // Single outstanding fetch per follower (one Kafka fetch
        // connection): a new fetch supersedes any in-flight one from this
        // node. Without this, the unordered network could deliver a stale
        // lower-offset fetch after a newer one, regressing the leader's
        // recorded follower progress — which the production core forbids
        // (`handle_fetch` overwrites `progress.fetch_offset`
        // unconditionally, relying on per-follower fetch offsets arriving
        // monotonically, as they do over a single TCP connection).
        state
            .network
            .retain(|e| !(e.src == id && matches!(e.event, Event::ReceiveFetch { .. })));
        state.network.insert(Envelope {
            src: id,
            dst: leader_id,
            event: Event::ReceiveFetch {
                from: id,
                fetch_epoch,
                fetch_offset,
            },
        });
    }

    /// For a vote request, whether `dst` would grant it if only the
    /// candidate's log were as up to date as its own. It runs a copy of the
    /// real machine on the same request with `candidate_log_end` replaced by
    /// the voter's own log end, which KIP-595's comparison always accepts. A
    /// refusal of the real request that this copy grants is a refusal for log
    /// recency alone: the epoch, the vote already cast and the known leader
    /// were all the same.
    fn recency_counterfactual(state: &ModelState, dst: NodeId, event: &Event) -> bool {
        let Event::ReceiveVoteRequest { .. } = event else {
            return false;
        };
        let node = &state.nodes[&dst];
        let mut counterfactual = *event;
        if let Event::ReceiveVoteRequest {
            candidate_log_end, ..
        } = &mut counterfactual
        {
            *candidate_log_end = node.log.log_end();
        }
        node.machine
            .clone()
            .on_event(counterfactual, &node.log, NOW)
            .iter()
            .any(|a| matches!(a, Action::ReplyVote { granted: true, .. }))
    }

    /// Delivers `event` to `dst`. The method runs the real machine and
    /// translates the emitted actions. It also synthesizes the leader's fetch
    /// RESPONSE, because the core emits HWM and Truncate actions and not a
    /// response message. This is ported from `step` in `sim_harness`.
    pub(super) fn step(&self, state: &mut ModelState, dst: NodeId, event: Event) {
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        // Only the configs that require the `stale_candidate_refused` witness
        // pay for it: elsewhere the flag would only add states.
        let refusal_counterfactual = self.voter_ids.len() >= 3
            && self.max_appends > 0
            && Self::recency_counterfactual(state, dst, &event);
        let actions = {
            let node = state.nodes.get_mut(&dst).expect("dst exists");
            node.machine.on_event(event, &node.log, NOW)
        };
        if refusal_counterfactual
            && actions
                .iter()
                .any(|a| matches!(a, Action::ReplyVote { granted: false, .. }))
        {
            state.step_witness = Some(StepWitness::StaleCandidateRefused);
        }
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::ReplyDivergingEpoch(point) => Some(*point),
                _ => None,
            });
            if state.nodes[&dst].machine.role().is_leader() && state.nodes.contains_key(&follower) {
                let leader_epoch = state.nodes[&dst].machine.quorum_state().leader_epoch;
                let leader_end = state.nodes[&dst].log.end_offset();
                let follower_end = state.nodes[&follower].log.end_offset();
                if diverging.is_some() || follower_end < leader_end {
                    state.network.insert(Envelope {
                        src: dst,
                        dst: follower,
                        event: Event::ReceiveFetchResponse {
                            leader_id: dst,
                            leader_epoch,
                            diverging,
                        },
                    });
                }
            }
        }
        for action in actions {
            self.apply_action(state, dst, &action);
        }
    }
}
