//! Outbound peer RPC. Every send is fire-and-forget on a spawned task that
//! posts the decoded response back to the loop, which is what keeps the engine
//! from ever awaiting a peer inline.

use {
    super::{
        Engine, engine_loop::response_to_event, replication::fetch_epoch_for_request,
        timing::election_timeout_ms,
    },
    crate::kraft::{
        transport::{Command, api_key, wire},
        types::{Epoch, LogView, NodeId},
    },
    std::sync::Arc,
};

/// Kafka's `KafkaRaftClient.MAX_FETCH_WAIT_MS`: how long a leader may hold a
/// Fetch that has nothing new.
const MAX_FETCH_WAIT_MS: u64 = 500;

/// The `MaxWaitMs` of this replica's Fetch: Kafka's 500 ms, or a quarter of
/// the fetch timer when that is shorter. Kafka holds a Fetch for a quarter of
/// its default fetch timeout (500 ms of 2 s), so a held Fetch is answered well
/// before the timer counts it as a miss; a krabka replica runs its fetch timer
/// on the election timeout, which can be far shorter.
fn fetch_max_wait_ms(election_timeout: krabka_units::prelude::Time) -> i32 {
    let quarter_timer = election_timeout_ms(election_timeout) / 4;
    i32::try_from(MAX_FETCH_WAIT_MS.min(quarter_timer)).unwrap_or(0)
}

impl Engine {
    /// Voter ids other than self.
    pub fn other_voters(&self) -> Vec<NodeId> {
        self.core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|&id| id != self.me)
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch, pre_vote))]
    pub fn broadcast_vote(&self, epoch: Epoch, pre_vote: bool) {
        let last_epoch = self.log.last_epoch();
        let last_offset = self.log.end_offset();
        let state = self.core.quorum_state();
        let Some(candidate_voter) = state.voters.get(self.me) else {
            tracing::error!(candidate = self.me.0, "cannot encode Vote for a non-voter");
            return;
        };
        let cluster_id = state.cluster_id;
        let candidate_directory_id = candidate_voter.directory_id;
        // The wire top-level `voterId` must name the recipient voter; the JVM
        // rejects a Vote addressed to anyone else (or to the sentinel `-1`). So
        // build a per-recipient body inside the loop rather than broadcasting a
        // single shared body.
        for peer in self.other_voters() {
            let Some(voter_directory_id) = state.voters.get(peer).map(|voter| voter.directory_id)
            else {
                continue;
            };
            let request = wire::PeerRequest::Vote {
                cluster_id: Some(cluster_id),
                voter_id: peer,
                voter_directory_id,
                candidate_epoch: epoch,
                candidate: self.me,
                candidate_directory_id,
                last_epoch,
                last_offset,
                pre_vote,
            };
            let Some(body) = request.try_encode() else {
                tracing::error!(
                    voter = peer.0,
                    candidate = self.me.0,
                    epoch,
                    last_epoch,
                    "Vote fields exceed Kafka int32 wire range"
                );
                continue;
            };
            self.spawn_send(peer, api_key::VOTE, body);
        }
    }

    /// Sends `BeginQuorumEpoch` to each other voter, as Kafka's
    /// `buildBeginQuorumEpochRequest` builds it: the cluster id, the
    /// recipient's voter key, and this leader's own listeners in
    /// `LeaderEndpoints`, so a recipient whose voter set does not name this
    /// leader still reaches it.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    pub fn broadcast_begin_quorum_epoch(&self, epoch: Epoch) {
        let state = self.core.quorum_state();
        let leader_endpoints: Vec<(String, String, u16)> = state
            .voters
            .get(self.me)
            .map(|voter| {
                voter
                    .endpoints
                    .iter()
                    .map(|endpoint| (endpoint.name.clone(), endpoint.host.clone(), endpoint.port))
                    .collect()
            })
            .unwrap_or_default();
        for peer in self.other_voters() {
            let body = wire::PeerRequest::BeginQuorumEpoch {
                cluster_id: Some(state.cluster_id),
                voter_id: peer,
                voter_directory_id: state
                    .voters
                    .get(peer)
                    .map_or(uuid::Uuid::nil(), |voter| voter.directory_id),
                leader_id: self.me,
                leader_epoch: epoch,
                leader_endpoints: leader_endpoints.clone(),
            }
            .encode();
            self.spawn_send(peer, api_key::BEGIN_QUORUM_EPOCH, body);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    /// Sends `EndQuorumEpoch` to the other voters, as Kafka's
    /// `buildEndQuorumEpochRequest` builds it. `preferred_successors` is the
    /// core's ranking of them, most caught up first. Each goes into
    /// `PreferredCandidates` with its directory id from the voter set.
    pub fn broadcast_end_quorum_epoch(&self, epoch: Epoch, preferred_successors: &[NodeId]) {
        let state = self.core.quorum_state();
        let voters = &state.voters;
        let body = wire::PeerRequest::EndQuorumEpoch {
            cluster_id: Some(state.cluster_id),
            leader_id: self.me,
            leader_epoch: epoch,
            preferred_candidates: preferred_successors
                .iter()
                .map(|&id| {
                    (
                        id,
                        voters
                            .get(id)
                            .map_or(uuid::Uuid::nil(), |voter| voter.directory_id),
                    )
                })
                .collect(),
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::END_QUORUM_EPOCH, body.clone());
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, leader_id = leader_id.0, fetch_offset = self.log.end_offset()))]
    pub fn send_fetch(&self, leader_id: NodeId) {
        if leader_id == self.me {
            return;
        }
        let fetch_offset = self.log.end_offset();
        // Post-install epoch hazard: right after installing a snapshot the log is
        // empty at the snapshot boundary, so it carries no epoch of its own and
        // `last_epoch()` would report 0. Sending `fetch_epoch = 0` from a
        // non-zero boundary makes the leader's divergence check emit a spurious
        // truncate hint → a re-fetch loop. While we hold a freshly-installed
        // epoch AND the log is still empty at the boundary, fetch with that
        // epoch instead. Cleared once a normal fetch appends past the boundary.
        let fetch_epoch = fetch_epoch_for_request(
            self.installed_snapshot_epoch,
            self.log.log_start_offset(),
            self.log.log_end_offset(),
            self.log.last_epoch(),
        );
        // A voter names itself by the directory its voters record holds, which
        // may be absent under a static voter set. A replica that is not a voter
        // yet, such as a controller waiting to be added, names its own, so the
        // leader tracks it under the key `AddRaftVoter` will name.
        let replica_directory_id = self
            .core
            .quorum_state()
            .voters
            .get(self.me)
            .map(|voter| voter.directory_id)
            .filter(|directory_id| !directory_id.is_nil())
            .unwrap_or(self.directory_id);
        // Kafka's `buildFetchRequest`: the cluster id, `quorum.epoch()`, which
        // the responder's `validateLeaderOnlyRequest` checks, and the local
        // high watermark, which the leader answers at once when its own is
        // higher.
        let body = wire::PeerRequest::Fetch {
            cluster_id: Some(self.core.quorum_state().cluster_id),
            from: self.me,
            max_wait_ms: fetch_max_wait_ms(self.election_timeout),
            current_leader_epoch: i32::try_from(self.core.quorum_state().leader_epoch)
                .unwrap_or(i32::MAX),
            fetch_epoch,
            fetch_offset,
            replica_directory_id,
            high_watermark: self.log.hwm().0,
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH, body);
    }

    /// (Follower side) request a byte range of `snapshot_id` from `leader_id`.
    pub fn send_fetch_snapshot(&self, leader_id: NodeId, snapshot_id: (i64, i32), position: i64) {
        if leader_id == self.me {
            return;
        }
        let state = self.core.quorum_state();
        let body = wire::PeerRequest::FetchSnapshot {
            cluster_id: Some(state.cluster_id),
            from: self.me,
            current_leader_epoch: i32::try_from(state.leader_epoch).unwrap_or(i32::MAX),
            snapshot_id,
            position,
            // KIP-595 `FetchSnapshot.MaxBytes` is an `int32`; the quantity
            // converts here, at the wire boundary.
            max_bytes: self.metadata_raft_fetch_max.bytes(),
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH_SNAPSHOT, body);
    }

    /// Fire-and-forget a peer send: spawn a task that performs the RPC, decodes
    /// the response into the matching `Receive*Response` core event, and posts
    /// it back to the loop. The loop NEVER awaits a peer RPC inline.
    pub fn spawn_send(&self, peer: NodeId, api_key: i16, body: bytes::Bytes) {
        let peers = Arc::clone(&self.peers);
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            match peers.send(peer, api_key, body).await {
                Ok(resp_body) => {
                    // A Fetch response carries log records the follower must
                    // truncate/append/apply before the core sees it, so it goes
                    // through the dedicated `FetchResponse` command. Every other
                    // response decodes to a pure `Receive*Response` event.
                    if api_key == self::api_key::FETCH {
                        let _ = cmd_tx
                            .send(Command::FetchResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if api_key == self::api_key::FETCH_SNAPSHOT {
                        // A FetchSnapshot response carries snapshot bytes the
                        // follower reassembles + installs before resuming, so it
                        // takes its own command path (mirrors FetchResponse).
                        let _ = cmd_tx
                            .send(Command::FetchSnapshotResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if let Some(event) = response_to_event(peer, api_key, &resp_body) {
                        let _ = cmd_tx.send(Command::Event(event)).await;
                    }
                }
                Err(e) => tracing::debug!(peer = peer.0, ?e, "kraft: peer send failed"),
            }
        });
    }
}
