//! The single owning engine task: its `select!` loop over the command mpsc and
//! the election, fetch, check-quorum and heartbeat timers, the dispatch of each
//! core [`Action`] it produces, and the timer reconciliation that follows a
//! role change.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_units::prelude::TimeExt as _;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

use super::{
    Engine, METADATA_LOG_CLEAN_INTERVAL,
    offsets::leader_alone_is_majority,
    timing::{
        election_timeout_ms, election_timer_starts_election, following_leader_for_role,
        heartbeat_period, instant_from_clock_base, should_fail_waiters_on_leadership_change,
    },
};
use crate::{
    error::RaftError,
    kraft::{
        action::{Action, TimerKind},
        event::Event,
        role::Role,
        transport::{Command, TimerTick, api_key, wire},
        types::{NodeId, SimInstant},
    },
};

impl Engine {
    /// The event loop. `select!`s the command mpsc against the election/fetch
    /// timers and the leader heartbeat interval, turning each into core input
    /// and executing the resulting [`Action`]s. Single-threaded over all
    /// consensus state, so no locking is needed inside; peer sends are
    /// fire-and-forget (see the module docs).
    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<Command>) {
        // A dynamically formatted controller starts as an observer with no
        // known leader.  It still has one or more discovery voters from the
        // bootstrap configuration, so poll one immediately.  The Fetch reply
        // redirects us to the current leader and begins normal replication.
        // Without this kick, observers never arm either election or fetch
        // timers and therefore cannot reach the auto-join workflow.
        self.send_discovery_fetch();

        // Heartbeat ticks the whole time; the loop only acts on it while leader.
        let hb_period = heartbeat_period(self.election_timeout, self.heartbeat_interval);
        let mut heartbeat = tokio::time::interval(hb_period.to_std());
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut faulted = false;
        loop {
            // Build the timer futures fresh each turn from the current deadlines.
            let election_sleep = sleep_until_opt(self.election_at);
            let fetch_sleep = sleep_until_opt(self.fetch_at);
            let check_quorum_sleep = sleep_until_opt(self.check_quorum_at);
            let parked_fetch_sleep = sleep_until_opt(self.next_parked_fetch_deadline());
            let noop_sleep = sleep_until_opt(self.noop_at);
            let clean_sleep = tokio::time::sleep_until(self.clean_at);
            tokio::pin!(election_sleep);
            tokio::pin!(fetch_sleep);
            tokio::pin!(check_quorum_sleep);
            tokio::pin!(parked_fetch_sleep);
            tokio::pin!(noop_sleep);
            tokio::pin!(clean_sleep);

            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None | Some(Command::Shutdown) => break,
                        Some(c) => self.on_command(c),
                    }
                }
                () = &mut election_sleep => {
                    self.election_at = None;
                    self.on_timer(TimerTick::Election);
                }
                () = &mut fetch_sleep => {
                    self.fetch_at = None;
                    self.on_timer(TimerTick::Fetch);
                }
                () = &mut check_quorum_sleep => {
                    self.check_quorum_at = None;
                    self.on_timer(TimerTick::CheckQuorum);
                }
                _ = heartbeat.tick() => {
                    self.on_timer(TimerTick::Heartbeat);
                }
                // A parked fetch whose wait ran out is answered below.
                () = &mut parked_fetch_sleep => {}
                () = &mut noop_sleep => {
                    self.on_noop_timer();
                }
                () = &mut clean_sleep => {
                    self.clean_at = Instant::now() + METADATA_LOG_CLEAN_INTERVAL;
                    if let Err(error) = self.maybe_clean() {
                        tracing::error!(?error, "kraft: metadata log cleaning failed");
                    }
                }
            }
            // A write to the metadata log directory that failed this turn
            // ends the controller (KIP-858), and so does a failed controller
            // activation. The engine stops taking part in the quorum at once,
            // rather than serve from a log it can no longer extend or that it
            // could not seed.
            if self.publish_fault() {
                faulted = true;
                break;
            }
            // Whatever this turn did may have grown the log, moved the high
            // watermark or ended the leadership a parked fetch waits on.
            self.complete_parked_fetches(Instant::now());
            self.retry_pending_downgrade_snapshot();
            // A quiet log still needs to snapshot on the time cap: an apply
            // that advances the HWM already retriggers this check, but an
            // idle voter only ticks the loop on its timers.
            self.maybe_snapshot();
        }
        if faulted {
            // The engine takes part in nothing more, but it keeps its watch
            // channels open, and it answers no caller, until the controller
            // shuts it down. The controller publishes the fault as fatal
            // first. So a reader that sees the image close, or a submit fail,
            // also sees the fatal fault and not a bare shutdown.
            let mut held = Vec::new();
            while let Some(command) = cmd_rx.recv().await {
                if matches!(command, Command::Shutdown) {
                    break;
                }
                held.push(command);
            }
        }
        // Fail any parked submitters so callers don't hang on shutdown.
        for w in self.commit_waiters.drain(..) {
            let _ = w.reply.send(Err(RaftError::Shutdown));
        }
    }

    /// Publish the fault that stops the engine: the failure of the metadata
    /// log directory when a write to it returned an I/O error, or else the
    /// failure of a controller activation, or else a committed record this
    /// node could not replay. Returns `true` once a fault is published.
    pub fn publish_fault(&mut self) -> bool {
        let fault = if let Some(failure) = self.log.failure() {
            format!(
                "the metadata log directory {} has failed: {failure}",
                self.data_dir
                    .parent()
                    .unwrap_or(self.data_dir.as_path())
                    .display()
            )
        } else if let Some(fault) = &self.activation_fault {
            fault.clone()
        } else if let Some(fault) = &self.replay_fault {
            fault.to_string()
        } else {
            return false;
        };
        if self.fault_tx.borrow().is_none() {
            tracing::error!(%fault, "kraft: stopping the controller");
            self.fault_tx.send_replace(Some(fault));
        }
        true
    }

    /// Logical "now" for the core, derived from the monotonic clock base.
    pub fn now(&self) -> SimInstant {
        let ms = Instant::now()
            .saturating_duration_since(self.clock_base)
            .as_millis();
        SimInstant(u64::try_from(ms).unwrap_or(u64::MAX))
    }

    pub fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Shutdown => {}
            Command::Event(event) => self.on_event(event),
            Command::FetchResponse { from, body } => self.on_fetch_response(from, &body),
            Command::FetchSnapshotResponse { from, body } => {
                self.on_fetch_snapshot_response(from, &body);
            }
            Command::Inbound(inbound) => self.on_inbound(inbound),
            Command::Timer(tick) => self.on_timer(tick),
            Command::SubmitChange {
                records,
                expected_image,
                reply,
            } => {
                self.on_submit_change_if_image(&records, expected_image.as_ref(), reply);
            }
            Command::SubmitDelegationTokenMutations { mutations, reply } => {
                self.on_submit_delegation_token_mutations(&mutations, reply);
            }
            Command::Reconfigure { change, reply } => self.on_reconfigure(change, reply),
            Command::TriggerSnapshot { reply } => {
                let _ = reply.send(self.do_trigger_snapshot());
            }
            Command::QuorumStateSnapshot { reply } => {
                let _ = reply.send(self.quorum_state_snapshot());
            }
            Command::MetadataFetch {
                fetch_offset,
                max_size,
                replica,
                reply,
            } => {
                if let Some(replica) = replica {
                    self.record_metadata_fetch(replica, fetch_offset);
                }
                let _ = reply.send(self.metadata_fetch_slice(fetch_offset, max_size));
            }
            #[cfg(test)]
            Command::TestAppendAndCommit { records, reply } => {
                let off = self.test_append_and_commit(&records);
                let _ = reply.send(off);
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, role = self.core.role().name())
    )]
    pub fn on_event(&mut self, event: Event) {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
    }

    /// Map a timer tick to liveness behavior.
    pub fn on_timer(&mut self, tick: TimerTick) {
        match tick {
            TimerTick::Election => {
                // The election timer is only armed for voters not currently
                // leading. Firing it starts an election.
                if election_timer_starts_election(
                    self.core.is_voter(),
                    self.core.role().is_leader(),
                ) {
                    self.on_event(Event::ElectionTimeout);
                }
            }
            TimerTick::Fetch => {
                // A fetch-timer expiry while we still believe in a reachable
                // leader RE-POLLS rather than electing; only a sustained loss
                // (the configured consecutive-miss limit) feeds FetchTimeout.
                let leader = self.following_leader();
                if let Some(leader_id) = leader {
                    self.fetch_misses += 1;
                    if self.fetch_misses >= self.controller_fetch_miss_limit.get() {
                        self.fetch_misses = 0;
                        self.on_event(Event::FetchTimeout);
                    } else {
                        // Re-poll the leader and re-arm the fetch timer.
                        self.send_fetch(leader_id);
                        self.arm_fetch_timer();
                    }
                } else if self.core.is_voter() {
                    // No leader to poll but the fetch watchdog fired: elect.
                    self.on_event(Event::FetchTimeout);
                } else {
                    // An observer has no election timeout. Retry discovery
                    // until a bootstrap voter redirects it to the leader.
                    self.send_discovery_fetch();
                }
            }
            TimerTick::CheckQuorum => {
                // Only a leader runs this watchdog. The core re-arms it from
                // every Fetch/FetchSnapshot that completes a majority, so
                // reaching the deadline means the quorum has gone quiet.
                if self.core.role().is_leader() {
                    self.on_event(Event::CheckQuorumTimeout);
                }
            }
            TimerTick::Heartbeat => {
                if self.core.role().is_leader() {
                    let epoch = self.core.quorum_state().leader_epoch;
                    self.broadcast_begin_quorum_epoch(epoch);
                }
            }
        }
    }

    /// The leader id we are actively following (Follower / attached Observer),
    /// if any.
    pub fn following_leader(&self) -> Option<NodeId> {
        following_leader_for_role(self.core.role())
    }

    /// Pick the peer for the next observer leader discovery Fetch: the voters
    /// other than this node, or the bootstrap servers when it knows no voter,
    /// taken in turn by the count in `discovery_attempts`.
    ///
    /// A discovery Fetch to a peer that is down, or that knows no leader,
    /// attaches to nothing, so the next one goes to the next peer. Kafka's
    /// `RequestManager.findReadyBootstrapServer` moves on the same way. A
    /// controller lists its own address in `controller.quorum.bootstrap.servers`
    /// as every other node does, and a discovery Fetch can go to that address.
    pub fn discovery_peer(&self) -> Option<NodeId> {
        if self.core.is_voter() || self.following_leader().is_some() {
            return None;
        }
        let voters: Vec<NodeId> = self
            .core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|id| *id != self.me)
            .collect();
        let peers = if voters.is_empty() {
            self.peers.discovery_peers()
        } else {
            voters
        };
        peers
            .get(self.discovery_attempts % peers.len().max(1))
            .copied()
    }

    /// Send the next observer leader discovery Fetch, if this node is a
    /// leaderless observer with a peer to ask, and arm the fetch timer that
    /// retries it.
    fn send_discovery_fetch(&mut self) {
        if let Some(peer) = self.discovery_peer() {
            self.discovery_attempts = self.discovery_attempts.wrapping_add(1);
            self.send_fetch(peer);
            self.arm_fetch_timer();
        }
    }

    /// Execute a batch of [`Action`]s, dispatching peer sends fire-and-forget.
    pub fn execute(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => {
                    self.broadcast_vote(epoch, pre_vote);
                }
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch {
                    epoch,
                    preferred_successors,
                } => {
                    self.broadcast_end_quorum_epoch(epoch, &preferred_successors);
                }
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                // The Fetch handler reads it off the actions and sends it in
                // the response. It changes nothing on this node.
                Action::ReplyDivergingEpoch(_) => {}
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute only the local (non-network, non-reply) actions in `actions`.
    pub fn execute_local_only(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => self.broadcast_vote(epoch, pre_vote),
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch {
                    epoch,
                    preferred_successors,
                } => self.broadcast_end_quorum_epoch(epoch, &preferred_successors),
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                Action::ReplyVote { .. } | Action::ReplyDivergingEpoch(_) => {}
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute a single non-network [`Action`] synchronously.
    pub fn execute_one_local(&mut self, action: Action) {
        match action {
            Action::AppendLeaderChange { epoch } => {
                if let Err(e) = self.append_leader_change(epoch) {
                    tracing::error!(?e, "kraft: append leader-change failed");
                    return;
                }
                // Kafka's controller activation: the first write of the
                // epoch seeds a log that holds no `metadata.version`.
                self.complete_activation();
                if leader_alone_is_majority(
                    self.core.quorum_state().majority(),
                    self.core.is_voter(),
                ) {
                    // There is no follower Fetch acknowledgement to drive HWM
                    // recomputation in a one-voter quorum. The local append is
                    // already a majority, so commit the current-epoch barrier.
                    self.advance_and_apply(self.log.log_end_offset());
                }
            }
            Action::AdvanceHighWatermark(n) => {
                // `n` is the core's raw i64 HWM target; wrap into the log domain.
                self.advance_and_apply(Offset(n));
            }
            Action::TruncateTo(point) => {
                // `point.offset` is the core's raw i64 divergence point.
                if let Err(e) = self.log.truncate_to(Offset(point.offset)) {
                    tracing::error!(?e, "kraft: truncate failed");
                } else {
                    self.restore_control_state_after_truncation(self.log.log_end_offset().0);
                }
            }
            Action::PersistQuorumState => {
                if let Err(e) = self.persist_quorum_state() {
                    tracing::error!(?e, "kraft: persist quorum-state failed");
                }
            }
            Action::ResetTimer { kind, deadline } => match kind {
                TimerKind::Election => self.election_at = Some(self.deadline_instant(deadline)),
                TimerKind::Fetch => self.fetch_at = Some(self.deadline_instant(deadline)),
                TimerKind::CheckQuorum => {
                    self.check_quorum_at = Some(self.deadline_instant(deadline));
                }
            },
            Action::TransitionedTo(_name) => {}
            Action::SendVoteRequest { .. }
            | Action::SendBeginQuorumEpoch { .. }
            | Action::SendEndQuorumEpoch { .. }
            | Action::SendFetch { .. }
            | Action::ReplyVote { .. }
            | Action::ReplyDivergingEpoch(_) => {
                assert2::assert!(false, "network/reply action routed to local executor");
            }
        }
    }

    /// After processing an event, cancel timers irrelevant to the new role and
    /// arm the ones it needs that the core did not explicitly reset.
    pub fn reconcile_timers(&mut self, _prev_role: &'static str) {
        match self.core.role() {
            Role::Leader { .. } => {
                // A leader never elects on a timer and never fetch-watchdogs.
                // Its liveness is the check-quorum deadline, which the core
                // arms on promotion and re-arms from majority contact.
                self.election_at = None;
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                // A follower has no election timer; the fetch watchdog (armed by
                // the core's ResetTimer/Fetch) covers liveness.
                self.election_at = None;
                self.check_quorum_at = None;
            }
            Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Unattached { .. }
            | Role::Voted { .. } => {
                // Mid-election: no leader to fetch from, election timer governs.
                self.fetch_at = None;
                self.fetch_misses = 0;
                self.check_quorum_at = None;
            }
            Role::Resigned => {
                // Stepped down: no leadership left to watch and no leader to
                // fetch from. The election timer the core armed on the way in
                // is what carries this node into its next pre-vote round.
                self.fetch_at = None;
                self.fetch_misses = 0;
                self.check_quorum_at = None;
            }
        }
        self.reconcile_noop_timer();
        self.fail_waiters_on_leadership_loss();
    }

    /// Detect a transition away from leadership — Leader → non-Leader, or a
    /// leader-epoch bump while we still nominally lead — and fail every parked
    /// `submit_change` waiter with `NotLeader` so the caller's future resolves
    /// promptly instead of hanging until shutdown (FIX 1). Records appended at
    /// our old epoch can no longer commit once we step down (a new leader may
    /// truncate them), so the parked waiters are unresolvable and must error.
    pub fn fail_waiters_on_leadership_loss(&mut self) {
        let is_leader = self.core.role().is_leader();
        let epoch = self.core.quorum_state().leader_epoch;
        if self.was_leader != is_leader || self.held_epoch != epoch {
            // An admission image also identifies the authority that planned
            // the mutation. Invalidate it on role/epoch changes even when no
            // metadata records changed. This runs before the next command,
            // including while the new leader's control batch is uncommitted.
            // Preserve the published contents: self.image can include a
            // downgrade whose checkpoint has not yet made it publishable.
            let published = self.image_tx.borrow().as_ref().clone();
            self.image_tx.send_replace(Arc::new(published));
        }
        let lost_leadership = should_fail_waiters_on_leadership_change(
            self.was_leader,
            is_leader,
            self.held_epoch,
            epoch,
        );
        if lost_leadership {
            // A later leadership starts with no observers, as a new
            // `LeaderState` does.
            self.observers.clear();
        }
        if lost_leadership && !self.commit_waiters.is_empty() {
            let current_leader = self.core.quorum_state().leader_id;
            for w in self.commit_waiters.drain(..) {
                let _ = w.reply.send(Err(RaftError::NotLeader { current_leader }));
            }
        }
        if lost_leadership {
            self.fail_pending_reconfiguration();
        }
        self.was_leader = is_leader;
        self.held_epoch = epoch;
    }

    /// Arm the fetch timer one election-timeout out from now (re-poll cadence).
    pub fn arm_fetch_timer(&mut self) {
        self.fetch_at = Some(
            Instant::now() + Duration::from_millis(election_timeout_ms(self.election_timeout)),
        );
    }

    /// Convert a core [`SimInstant`] deadline into a `tokio::time::Instant`.
    pub fn deadline_instant(&self, deadline: SimInstant) -> Instant {
        instant_from_clock_base(self.clock_base, deadline)
    }

    pub fn publish_leader(&self) {
        let leader = self.core.quorum_state().leader_id;
        if *self.leader_tx.borrow() != leader {
            let _ = self.leader_tx.send(leader);
        }
        // Republish the structured consensus snapshot for the handle's
        // synchronous `quorum_state()` (DescribeQuorum). `send_replace` keeps
        // the watch's stored value current even with no active receiver.
        let snapshot = self.quorum_state_snapshot();
        self.quorum_tx.send_replace(snapshot);
    }
}

/// Decode a non-Fetch peer response body into the matching `Receive*Response`
/// event. `peer` is the responder, used to fill `from`. Returns `None` for
/// `Ack` (Begin/End acks produce no core event), `Fetch` (handled by the
/// dedicated [`Engine::on_fetch_response`] path, which must touch the log before
/// the core sees the event), and undecodable bodies.
pub fn response_to_event(peer: NodeId, api_key: i16, body: &[u8]) -> Option<Event> {
    match api_key {
        self::api_key::VOTE => match wire::PeerResponse::decode_vote(body)? {
            wire::PeerResponse::Vote { epoch, granted } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
            }),
            _ => None,
        },
        // Begin/End acks produce no core event; Fetch is handled by the
        // dedicated `FetchResponse` command path before reaching here.
        _ => None,
    }
}

/// `Some` sleep future for an armed deadline; a never-ready future otherwise so
/// `select!` ignores the disarmed timer.
pub async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}
