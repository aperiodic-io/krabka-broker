//! The event loop: which timer fires next, how a queued message is delivered,
//! and how one event is fed to a node's consensus machine. This is the part of
//! the harness that makes the simulation deterministic.

use krabka_raft::kraft::{action::Action, event::Event, types::NodeId};

use super::{
    cluster::Sim,
    node::Message,
    node_log::SimNodeLog,
    timers::{HEARTBEAT_MS, SimTimer, election_timeout_ms_of},
};

impl<L: SimNodeLog> Sim<L> {
    /// Finds the earliest armed timer across all nodes, advances the clock to
    /// it, and fires it. A partitioned node still ticks internally and still
    /// counts here. Returns `false` if no timer is armed.
    pub(super) fn fire_next_timer(&mut self) -> bool {
        // Pick the node with the earliest deadline; ties break by node id
        // (BTreeMap iteration is ascending by id, so the first minimum wins).
        let best = krabka_kraft_core::simulation_support::earliest_timer(self.nodes.values().map(
            |node| {
                (
                    node.id,
                    [
                        node.election_deadline,
                        node.fetch_deadline,
                        node.heartbeat_deadline,
                        node.check_quorum_deadline,
                    ],
                )
            },
        ));
        let Some((deadline, id, kind)) = best else {
            return false;
        };
        if deadline > self.now {
            self.now = deadline;
        }
        // Clear the fired timer; the handler re-arms below / via ResetTimer.
        {
            let node = self.nodes.get_mut(&id).unwrap();
            krabka_kraft_core::simulation_support::clear_timer(
                kind,
                &mut node.election_deadline,
                &mut node.fetch_deadline,
                &mut node.heartbeat_deadline,
                &mut node.check_quorum_deadline,
            );
        }
        match kind {
            SimTimer::Heartbeat => {
                self.fire_leader_heartbeat(id);
                true
            }
            SimTimer::CheckQuorum => {
                self.step(id, Event::CheckQuorumTimeout);
                true
            }
            SimTimer::Fetch => {
                // A fetch watchdog firing while the follower's leader is still
                // reachable is a routine long-poll expiry: re-poll the leader
                // rather than escalate to an election. Only when the leader is
                // gone (unreachable / unknown) does the watchdog become a real
                // `FetchTimeout` that elects. This mirrors `KRaft`, where continuous
                // polling resets the timer and only sustained silence elects.
                if let Some(leader_id) = self.reachable_leader(id) {
                    let deadline = self.now.saturating_add_ms(election_timeout_ms_of(id));
                    self.nodes.get_mut(&id).unwrap().fetch_deadline = Some(deadline);
                    self.apply_action(id, Action::SendFetch { leader_id });
                    return true;
                }
                self.step(id, Event::FetchTimeout);
                true
            }
            SimTimer::Election => {
                self.step(id, Event::ElectionTimeout);
                true
            }
        }
    }

    // A leader's periodic heartbeat. It re-broadcasts `BeginQuorumEpoch` to
    // every peer, faithful to the `KRaft` resend to non-fetching voters, and
    // re-arms the heartbeat. This is how a stale leader that rejoins after a
    // partition learns of the newer epoch from the current leader and steps
    // down to follower.
    krabka_macros::simulation_heartbeat!(krabka_kraft_core, HEARTBEAT_MS);

    /// Delivers a queued message, and drops it if either endpoint is
    /// partitioned.
    pub(super) fn deliver(&mut self, msg: Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        self.step(msg.dst, msg.event);
    }

    // Feeds one event to a node and translates the resulting actions into new
    // messages, timer arming, and log and HWM bookkeeping.
    krabka_macros::simulation_step!(krabka_kraft_core);

    /// Enforces per-role timer ownership, which the core does not fully manage
    /// through `ResetTimer` actions alone:
    ///
    /// - A leader runs neither an election timer nor a fetch timer. Its liveness
    ///   is a separate check-quorum mechanism, out of scope for the in-memory consensus core.
    /// - A follower or an observer runs only the fetch watchdog, and never an
    ///   election timer. But `handle_begin_quorum_epoch` emits only
    ///   `ResetTimer{Fetch}`, which leaves a previously-armed election timer
    ///   live. Without a clear of that timer, a healthy follower's stale
    ///   election timer fires, the follower goes `Prospective`, and the cluster
    ///   never stabilises.
    /// - An electing role, which is Unattached, Voted, Prospective, or
    ///   Candidate, runs only the election timer, and never a fetch watchdog.
    ///
    /// The core does arm the correct timer on each transition. This method only
    /// clears the stale opposite timer, so the harness scheduler matches the
    /// per-role timer model of `KRaft`.
    fn reconcile_timers_for_role(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        krabka_kraft_core::simulation_support::reconcile_timers(
            node.machine.role(),
            &mut node.election_deadline,
            &mut node.fetch_deadline,
            &mut node.heartbeat_deadline,
            &mut node.check_quorum_deadline,
            self.now.saturating_add_ms(HEARTBEAT_MS),
        );
    }
}
