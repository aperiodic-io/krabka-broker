//! Service of inbound peer RPCs: each request body is decoded, run through the
//! consensus core, and answered on its oneshot, including the leader's Fetch
//! and `FetchSnapshot` serve paths and the Fetch long poll.

use {
    super::{
        Engine,
        quorum_requests::{INCONSISTENT_CLUSTER_ID, INVALID_REQUEST, NOT_LEADER_OR_FOLLOWER},
        replication::should_serve_fetch_records,
    },
    crate::kraft::{
        action::Action,
        event::Event,
        role::Role,
        transport::{Inbound, PeerApi, wire},
        types::{Epoch, LogOffsetMetadata, LogView as _, NodeId, ReplicaKey},
    },
    krabka_ids::Offset,
    krabka_metadata::VoterSet,
    krabka_protocol::owned::fetch_request::FetchRequest,
    tokio::{sync::oneshot, time::Instant},
};

/// The fields of one validated Fetch partition that answering it needs, kept
/// while the request waits in the purgatory.
#[derive(Debug, Clone, Copy)]
pub(super) struct FetchPartitionRequest {
    /// Kafka's `FetchRequest.replicaId`. A negative id is a fetcher that is
    /// not a replica, and its offset does not count.
    replica_id: i32,
    replica_directory_id: uuid::Uuid,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    /// The fetcher's high watermark (v18), `i64::MAX` below v18.
    high_watermark: i64,
}

/// A Fetch that found nothing new and waits, as in Kafka's `fetchPurgatory`,
/// until the log grows past its offset, the high watermark moves, leadership
/// ends or `MaxWaitMs` runs out.
pub(super) struct ParkedFetch {
    fetch: FetchPartitionRequest,
    version: i16,
    reply: oneshot::Sender<bytes::Bytes>,
    /// The answer computed when the request arrived. Kafka sends it unchanged
    /// when the wait runs out.
    answer: wire::FetchAnswer,
    /// The leader epoch and high watermark when the request was parked.
    epoch: Epoch,
    high_watermark: Offset,
    deadline: Instant,
}

impl Engine {
    #[tracing::instrument(
        name = "kraft_on_inbound",
        level = "debug",
        skip_all,
        fields(role = self.core.role().name(), epoch = self.core.quorum_state().leader_epoch, node = self.me.0)
    )]
    pub fn on_inbound(&mut self, inbound: Inbound) {
        // Decode the request body, run it through the core, and encode the
        // produced reply back onto the oneshot.
        let (api, req, version, reply) = inbound.into_parts();
        let response = match api {
            PeerApi::Fetch => return self.on_fetch_request(&req, version, reply),
            PeerApi::Vote => self.answer_vote(&req, version),
            PeerApi::BeginQuorumEpoch => self.answer_begin_quorum_epoch(&req, version),
            PeerApi::EndQuorumEpoch => self.answer_end_quorum_epoch(&req, version),
            PeerApi::FetchSnapshot => self.answer_fetch_snapshot(&req, version),
        };
        // A body that does not decode drops `reply`, which closes the
        // connection, as Kafka's `RequestContext.parseRequest` does.
        if let Some(response) = response {
            let _ = reply.send(response);
        }
    }

    /// Kafka's `KafkaRaftClient.handleFetchRequest`: the request checks, then
    /// `tryCompleteFetchRequest`, then the long poll.
    ///
    /// A `ClusterId` of another cluster is a top-level
    /// `INCONSISTENT_CLUSTER_ID`, and a request that does not name exactly the
    /// metadata topic id and partition 0 a top-level `INVALID_REQUEST`. A
    /// negative `MaxWaitMs`, `FetchOffset` or `LastFetchedEpoch`, or a
    /// `LastFetchedEpoch` above `CurrentLeaderEpoch`, is a partition
    /// `INVALID_REQUEST`. The answer goes back at once when it carries an
    /// error, records, a divergence or a snapshot id, when `MaxWaitMs` is 0, or
    /// when the leader's high watermark is above the fetcher's. Otherwise the
    /// request is parked until [`Self::complete_parked_fetches`] answers it.
    fn on_fetch_request(&mut self, req: &[u8], version: i16, reply: oneshot::Sender<bytes::Bytes>) {
        let Some(request) = wire::decode_request::<FetchRequest>(req, version) else {
            return;
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            let _ = reply.send(wire::encode_fetch_top_level_error(
                INCONSISTENT_CLUSTER_ID,
                version,
            ));
            return;
        }
        let (Some(topic), 1) = (request.topics.first(), request.topics.len()) else {
            let _ = reply.send(wire::encode_fetch_top_level_error(INVALID_REQUEST, version));
            return;
        };
        let [partition] = topic.partitions.as_slice() else {
            let _ = reply.send(wire::encode_fetch_top_level_error(INVALID_REQUEST, version));
            return;
        };
        if topic.topic_id != wire::METADATA_TOPIC_ID
            || partition.partition != wire::METADATA_PARTITION
        {
            let _ = reply.send(wire::encode_fetch_top_level_error(INVALID_REQUEST, version));
            return;
        }
        if request.max_wait_ms < 0
            || partition.fetch_offset < 0
            || partition.last_fetched_epoch < 0
            || partition.last_fetched_epoch > partition.current_leader_epoch
        {
            let _ = reply.send(self.empty_fetch_answer(INVALID_REQUEST).encode(version));
            return;
        }
        let fetch = FetchPartitionRequest {
            replica_id: wire::fetch_replica_id(&request, version),
            replica_directory_id: wire::uuid_from_wire(partition.replica_directory_id),
            current_leader_epoch: partition.current_leader_epoch,
            fetch_offset: partition.fetch_offset,
            last_fetched_epoch: partition.last_fetched_epoch,
            high_watermark: partition.high_watermark,
        };
        let answer = self.try_complete_fetch(&fetch);
        if answer.error_code != 0
            || !answer.records.is_empty()
            || request.max_wait_ms == 0
            || answer.diverging.is_some()
            || answer.snapshot_id.is_some()
            || fetch.high_watermark < answer.hwm
        {
            let _ = reply.send(answer.encode(version));
            return;
        }
        let wait = std::time::Duration::from_millis(request.max_wait_ms.unsigned_abs().into());
        self.fetch_purgatory.push(ParkedFetch {
            fetch,
            version,
            reply,
            answer,
            epoch: self.core.quorum_state().leader_epoch,
            high_watermark: self.log.hwm(),
            deadline: Instant::now() + wait,
        });
    }

    /// Answers the parked fetches that can be answered at `now`, as Kafka's
    /// `fetchPurgatory` completes them.
    ///
    /// Leadership that ended or moved to another epoch answers every one with
    /// `NOT_LEADER_OR_FOLLOWER` (`onBecomeFollower`, `transitionToResigned`).
    /// A high watermark that moved completes every one (`completeAll`), and
    /// a log that grew past a fetch offset completes that one
    /// (`maybeComplete`), both through `tryCompleteFetchRequest` again. A wait
    /// that ran out sends the answer computed when the request arrived.
    pub(super) fn complete_parked_fetches(&mut self, now: Instant) {
        if self.fetch_purgatory.is_empty() {
            return;
        }
        let epoch = self.core.quorum_state().leader_epoch;
        let leading = self.core.role().is_leader();
        let high_watermark = self.log.hwm();
        let log_end = self.log.log_end_offset();
        for parked in std::mem::take(&mut self.fetch_purgatory) {
            if parked.reply.is_closed() {
                continue;
            }
            let answer = if !leading || parked.epoch != epoch {
                self.empty_fetch_answer(NOT_LEADER_OR_FOLLOWER)
            } else if parked.high_watermark != high_watermark
                || log_end > Offset(parked.fetch.fetch_offset)
            {
                self.try_complete_fetch(&parked.fetch)
            } else if now >= parked.deadline {
                parked.answer
            } else {
                self.fetch_purgatory.push(parked);
                continue;
            };
            let _ = parked.reply.send(answer.encode(parked.version));
        }
    }

    /// The earliest deadline among the parked fetches.
    pub(super) fn next_parked_fetch_deadline(&self) -> Option<Instant> {
        self.fetch_purgatory
            .iter()
            .map(|parked| parked.deadline)
            .min()
    }

    /// Kafka's `buildEmptyFetchResponse`: `error_code` beside the leader this
    /// replica knows, no records and no high watermark.
    fn empty_fetch_answer(&self, error_code: i16) -> wire::FetchAnswer {
        wire::FetchAnswer {
            error_code,
            leader: self.quorum_leader(),
            diverging: None,
            snapshot_id: None,
            hwm: -1,
            log_start_offset: self.log.log_start_offset().0,
            records: bytes::Bytes::new(),
        }
    }

    /// Answer one Fetch partition as Kafka's
    /// `KafkaRaftClient.tryCompleteFetchRequest` does.
    ///
    /// `validateLeaderOnlyRequest` runs first: a request from another epoch,
    /// or one that reaches a replica that is not the leader, is answered with
    /// its error, the leader and epoch this replica knows, and that leader's
    /// endpoint, and changes nothing here. So a follower or an observer that
    /// is asked redirects the fetcher to the real leader rather than claiming
    /// the epoch, and the fetcher addresses that leader by the endpoint the
    /// response names, not by the responder's address. Only the leader runs
    /// the fetch through the core and serves records. A fetcher with a
    /// negative replica id is served, but its offset moves no replica state:
    /// `LeaderState.updateReplicaState` ignores it.
    fn try_complete_fetch(&mut self, fetch: &FetchPartitionRequest) -> wire::FetchAnswer {
        if let Some(error_code) = self.leader_only_request_error(fetch.current_leader_epoch) {
            return self.empty_fetch_answer(error_code);
        }
        let fetch_epoch = wire::epoch_from_wire(fetch.last_fetched_epoch);
        let fetch_offset = fetch.fetch_offset;
        // If the follower's fetch offset is below our pruned log-start, it
        // cannot replicate from the log -- point it at the latest snapshot
        // instead (KIP-630). `fetch_offset` arrives raw on the KIP-595 wire;
        // wrap it into the `KraftLog` offset domain to compare against log
        // bounds.
        let log_start = self.log.log_start_offset();
        let fetch_offset_in_log = Offset(fetch_offset);
        let snapshot_id = if fetch_offset_in_log >= 0 && fetch_offset_in_log < log_start {
            self.latest_snapshot_id()
        } else {
            None
        };
        // Only a fetch that validates against the log is replica progress:
        // `tryCompleteFetchRequest` calls `updateReplicaState` in its VALID
        // branch and nowhere else, so a snapshot redirect moves nothing here.
        let diverging = if snapshot_id.is_some() {
            None
        } else {
            match u64::try_from(fetch.replica_id) {
                Ok(replica) => self.run_replica_fetch(
                    NodeId(replica),
                    fetch_epoch,
                    fetch_offset,
                    fetch.replica_directory_id,
                ),
                Err(_) => self.non_replica_divergence(fetch_epoch, fetch_offset),
            }
        };
        // Serve the follower the batch bytes it is missing: every batch
        // at/after its `fetch_offset` up to our log end (KRaft replicates up
        // to the leader's log end, not just the HWM -- the HWM rides
        // separately in the response). A divergent fetch sends none (the
        // follower truncates first, then re-fetches).
        let fetch_offset = fetch_offset_in_log;
        let records = if should_serve_fetch_records(
            snapshot_id.is_some(),
            diverging.is_some(),
            self.core.role().is_leader(),
        ) {
            self.serve_fetch_records(fetch_offset)
        } else {
            bytes::Bytes::new()
        };
        // Handling the fetch may have ended this leadership (a check-quorum
        // resignation), so the leader view is read again for the answer.
        wire::FetchAnswer {
            error_code: 0,
            leader: self.quorum_leader(),
            diverging,
            snapshot_id,
            hwm: self.log.hwm().0,
            log_start_offset: self.log.log_start_offset().0,
            records,
        }
    }

    /// Whether `key` names a current voter, as Kafka's
    /// `LeaderState.ReplicaState.matchesKey` does: the ids are equal and either
    /// the voter records no directory id, or the fetcher's is the same. A
    /// replica that fetches with the id of a voter but another directory is an
    /// observer.
    fn names_current_voter(&self, key: ReplicaKey) -> bool {
        self.core
            .quorum_state()
            .voters
            .get(key.id)
            .is_some_and(|voter| {
                voter.directory_id.is_nil() || voter.directory_id == key.directory_id
            })
    }

    /// Runs a replica's fetch. A voter's goes through the core, which records
    /// its progress, scores it for check-quorum and may advance the high
    /// watermark; any other replica's is tracked as an observer. Returns the
    /// divergence found, if any.
    fn run_replica_fetch(
        &mut self,
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
        replica_directory_id: uuid::Uuid,
    ) -> Option<LogOffsetMetadata> {
        let key = ReplicaKey {
            id: from,
            directory_id: replica_directory_id,
        };
        if !self.names_current_voter(key) {
            let diverging = self.non_replica_divergence(fetch_epoch, fetch_offset);
            if diverging.is_none() {
                self.record_observer_fetch(key, fetch_offset);
            }
            return diverging;
        }
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(
            Event::ReceiveFetch {
                from,
                fetch_epoch,
                fetch_offset,
            },
            &self.log,
            now,
        );
        // A Fetch may yield a diverging epoch for the follower, or
        // AdvanceHighWatermark for the leader. Encode the divergence into the
        // response; apply HWM locally. The diverging epoch is a reply only:
        // the leader's log keeps its records.
        let diverging = actions.iter().find_map(|action| match action {
            Action::ReplyDivergingEpoch(point) => Some(*point),
            _ => None,
        });
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
        diverging
    }

    /// The divergence check of Kafka's `RaftLog.validateOffsetAndEpoch` for a
    /// fetcher the core does not track: one that is not a replica, or an
    /// observer.
    fn non_replica_divergence(
        &self,
        fetch_epoch: Epoch,
        fetch_offset: i64,
    ) -> Option<LogOffsetMetadata> {
        if fetch_offset <= 0 {
            return None;
        }
        let end = self.log.end_offset_for_epoch(fetch_epoch);
        (end.epoch != fetch_epoch || end.offset < fetch_offset).then_some(end)
    }

    /// Record a valid fetch by the observer `key` at `fetch_offset`, as
    /// `LeaderState.updateReplicaState` does for a replica that is not a voter,
    /// and forget the observers that can no longer matter. Only the leader
    /// tracks observers.
    ///
    /// Kafka drops an observer that has been silent for five minutes in
    /// `observerStates()`, which only `DescribeQuorum` calls, and
    /// `isReplicaCaughtUp` reads the same map with a window of an hour. So the
    /// five minutes hide an observer from `DescribeQuorum`
    /// (`queries::observer_session_expired`, applied when the snapshot is taken), and
    /// a fetch prunes only what neither one can see again: an observer that
    /// has been silent for the hour. Pruning at five minutes here would refuse
    /// a candidate that `AddRaftVoter` finds caught up.
    ///
    /// A fetch that adds an observer, or that prunes one, publishes the quorum
    /// snapshot again, because `DescribeQuorum` reads its observers from that
    /// snapshot. Without it, a leader with no commits to publish would go on
    /// to describe the observers it had when it was elected.
    pub(super) fn record_observer_fetch(&mut self, key: ReplicaKey, fetch_offset: i64) {
        if !self.core.role().is_leader() {
            return;
        }
        let now = self.now();
        let leader_log_end = self.log.log_end_offset().0;
        let joined = !self.observers.contains_key(&key);
        self.observers
            .entry(key)
            .or_default()
            .record_fetch(now, fetch_offset, leader_log_end);
        let tracked = self.observers.len();
        self.observers
            .retain(|_, progress| progress.fetched_within_caught_up_window(now));
        if joined || self.observers.len() != tracked {
            self.publish_leader();
        }
    }

    /// Track a broker-only observer's `MetadataFetch` (1004) as the Fetch it
    /// stands in for. The request carries no epoch, so it is valid when it lies
    /// within the log, where `RaftLog.validateOffsetAndEpoch` says VALID: not
    /// below the log start, which a snapshot answers, and not past the end. A
    /// voter's progress comes only from its own Fetch.
    pub(super) fn record_metadata_fetch(&mut self, key: ReplicaKey, fetch_offset: i64) {
        let within_log =
            (self.log.log_start_offset().0..=self.log.log_end_offset().0).contains(&fetch_offset);
        if within_log && !self.names_current_voter(key) {
            self.record_observer_fetch(key, fetch_offset);
        }
    }

    /// Apply `voters` to the core, and when this node leads, move replicas
    /// between the voter and observer maps as Kafka's
    /// `LeaderState.updateVoterAndObserverStates` does: a replica that became a
    /// voter keeps the progress it had as an observer, and a voter that left the
    /// set becomes an observer with the progress it had as a voter.
    pub(super) fn apply_voter_set(&mut self, voters: VoterSet) -> Vec<Action> {
        let now = self.now();
        let Role::Leader { replicas, .. } = self.core.role() else {
            return self.core.apply_voter_set(voters, now);
        };
        let voter_progress = replicas.clone();
        let old_voters = self.core.quorum_state().voters.clone();
        let actions = self.core.apply_voter_set(voters, now);
        let voters = self.core.quorum_state().voters.clone();
        for (id, progress) in voter_progress {
            if let (None, Some(voter)) = (voters.get(id), old_voters.get(id)) {
                let key = ReplicaKey {
                    id,
                    directory_id: voter.directory_id,
                };
                self.observers.entry(key).or_insert(progress);
            }
        }
        let joined: Vec<ReplicaKey> = self
            .observers
            .keys()
            .filter(|key| self.names_current_voter(**key))
            .copied()
            .collect();
        for key in joined {
            if let Some(progress) = self.observers.remove(&key) {
                self.core.adopt_replica_progress(key.id, progress);
            }
        }
        actions
    }

    /// Adopt the epoch of a vote or pre-vote from `candidate` that this replica
    /// is about to refuse for the voter key it names. Kafka's `handleVoteRequest`
    /// makes the higher-epoch transition before it checks that key, so the
    /// refusal steps a stale leader or follower down and carries the new epoch.
    pub(super) fn advance_epoch_for_vote(&mut self, candidate: ReplicaKey, epoch: Epoch) {
        let prev_role = self.core.role().name();
        let actions = self
            .core
            .advance_epoch_for_vote(candidate, epoch, self.now());
        self.execute_local_only(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
    }

    /// Run an inbound `ReceiveVoteRequest` and return whether the vote was
    /// granted. The loop side effects of the other actions apply as well.
    pub fn run_vote_request(&mut self, event: Event) -> bool {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        let mut granted = false;
        let mut local = Vec::new();
        for action in actions {
            if let Action::ReplyVote {
                granted: reply_granted,
                ..
            } = action
            {
                granted = reply_granted;
            } else {
                local.push(action);
            }
        }
        self.execute_local_only(local);
        self.reconcile_timers(prev_role);
        self.publish_leader();
        granted
    }
}
