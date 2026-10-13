//! Kafka's request checks for an inbound `Vote`, `BeginQuorumEpoch`,
//! `EndQuorumEpoch` and `FetchSnapshot`, and the leader view every response
//! carries.
//!
//! The checks and their order are those of `KafkaRaftClient.handleVoteRequest`,
//! `handleBeginQuorumEpochRequest`, `handleEndQuorumEpochRequest` and
//! `handleFetchSnapshotRequest`:
//!
//! 1. A `ClusterId` that names another cluster is a top-level
//!    `INCONSISTENT_CLUSTER_ID`, with no partition.
//! 2. A request that does not name exactly the `__cluster_metadata` partition 0
//!    is a top-level `INVALID_REQUEST`.
//! 3. The rest are partition errors beside the responder's leader id, epoch
//!    and leader endpoint.
//!
//! A body that does not decode gets no answer. Kafka's
//! `RequestContext.parseRequest` fails such a request, and the connection
//! closes.

use {
    super::{
        Engine,
        checkpoint::{BOOTSTRAP_SNAPSHOT_ID, load_checkpoint_by_id},
    },
    crate::{
        kraft::{
            event::{Event, LogEnd, SuccessorRank},
            transport::wire,
            types::{Epoch, NodeId, ReplicaKey},
        },
        snapshot::SnapshotReader,
    },
    bytes::Bytes,
    krabka_protocol::owned::{
        begin_quorum_epoch_request::BeginQuorumEpochRequest,
        end_quorum_epoch_request::EndQuorumEpochRequest,
        fetch_snapshot_request::FetchSnapshotRequest, vote_request::VoteRequest,
    },
};

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub(super) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub(super) const INVALID_REQUEST: i16 = 42;
const FENCED_LEADER_EPOCH: i16 = 74;
const UNKNOWN_LEADER_EPOCH: i16 = 75;
const SNAPSHOT_NOT_FOUND: i16 = 98;
const POSITION_OUT_OF_RANGE: i16 = 99;
pub(super) const INCONSISTENT_CLUSTER_ID: i16 = 104;
const INVALID_VOTER_KEY: i16 = 125;

/// The listener name a controller advertises its peer RPCs on. A voter
/// endpoint with another name is used only when the voter has no such
/// endpoint, the convention of `controller_endpoint_addr`.
const CONTROLLER_LISTENER_NAME: &str = "CONTROLLER";

/// Kafka's `hasValidTopicPartition` for the quorum RPCs: one topic, named
/// `__cluster_metadata`, with one partition, index 0.
fn names_only_the_metadata_partition<'a>(
    mut topics: impl ExactSizeIterator<Item = (&'a str, Vec<i32>)>,
) -> bool {
    topics.len() == 1
        && topics.next().is_some_and(|(name, partitions)| {
            name == wire::METADATA_TOPIC && partitions == [wire::METADATA_PARTITION]
        })
}

/// The shared wire shape of Begin/EndQuorumEpoch, before their distinct transitions.
trait QuorumEpochRequest {
    fn cluster_id(&self) -> Option<&str>;
    fn topics(&self) -> impl ExactSizeIterator<Item = (&str, Vec<i32>)>;
    fn leader(&self) -> Option<(i32, i32)>;
    fn endpoints(&self) -> impl Iterator<Item = (&str, &str, u16)>;
}

macro_rules! quorum_epoch_request {
    ($request:ty) => {
        impl QuorumEpochRequest for $request {
            fn cluster_id(&self) -> Option<&str> { self.cluster_id.as_deref() }

            fn topics(&self) -> impl ExactSizeIterator<Item = (&str, Vec<i32>)> {
                self.topics.iter().map(|topic| {
                    (
                        topic.topic_name.as_str(),
                        topic
                            .partitions
                            .iter()
                            .map(|partition| partition.partition_index)
                            .collect(),
                    )
                })
            }

            fn leader(&self) -> Option<(i32, i32)> {
                self.topics
                    .first()?
                    .partitions
                    .first()
                    .map(|partition| (partition.leader_id, partition.leader_epoch))
            }

            fn endpoints(&self) -> impl Iterator<Item = (&str, &str, u16)> {
                self.leader_endpoints.iter().map(|endpoint| {
                    (
                        endpoint.name.as_str(),
                        endpoint.host.as_str(),
                        endpoint.port,
                    )
                })
            }
        }
    };
}
quorum_epoch_request!(BeginQuorumEpochRequest);
quorum_epoch_request!(EndQuorumEpochRequest);

impl Engine {
    /// The responder's leader id, epoch and leader endpoint.
    pub(super) fn quorum_leader(&self) -> wire::QuorumLeader {
        let state = self.core.quorum_state();
        let endpoint = state
            .leader_id
            .and_then(|leader| self.core.current_or_adjacent_voter_entry(leader))
            .and_then(|voter| {
                voter
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.name.eq_ignore_ascii_case(CONTROLLER_LISTENER_NAME))
                    .or_else(|| voter.endpoints.first())
            })
            .map(|endpoint| (endpoint.host.clone(), endpoint.port));
        wire::QuorumLeader {
            leader_id: state.leader_id,
            epoch: state.leader_epoch,
            endpoint,
        }
    }

    /// Keeps the `LeaderEndpoints` of a `BeginQuorumEpoch` or `EndQuorumEpoch`
    /// as the address of `leader_id`, as Kafka's
    /// `handleBeginQuorumEpochRequest` and `handleEndQuorumEpochRequest` take
    /// them over the voter set's listeners. The endpoint named `CONTROLLER`
    /// is taken, or else the first. The voter set's own listener for the
    /// leader still takes precedence, so this matters while the voter set does
    /// not name the leader, during an uncommitted KIP-853 voter change.
    fn remember_request_leader_endpoints<'a>(
        &self,
        leader_id: NodeId,
        endpoints: impl Iterator<Item = (&'a str, &'a str, u16)>,
    ) {
        let endpoints: Vec<_> = endpoints.collect();
        let chosen = endpoints
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(CONTROLLER_LISTENER_NAME))
            .or_else(|| endpoints.first());
        if let Some((_, host, port)) = chosen {
            self.peers
                .remember_leader_endpoint(leader_id, format!("{host}:{port}"));
        }
    }

    /// Kafka's `hasValidClusterId`: an absent id is valid.
    pub(super) fn has_valid_cluster_id(&self, cluster_id: Option<&str>) -> bool {
        cluster_id.is_none_or(|id| {
            wire::parse_cluster_id(id) == Some(self.core.quorum_state().cluster_id)
        })
    }

    /// Kafka's `validateVoterOnlyRequest`.
    fn voter_only_request_error(&self, remote_id: i32, request_epoch: i32) -> Option<i16> {
        if i64::from(request_epoch) < i64::from(self.core.quorum_state().leader_epoch) {
            Some(FENCED_LEADER_EPOCH)
        } else if remote_id < 0 {
            Some(INVALID_REQUEST)
        } else {
            None
        }
    }

    /// Kafka's `validateLeaderOnlyRequest`, without the shutdown check: the
    /// engine answers no request once it stops.
    pub(super) fn leader_only_request_error(&self, request_epoch: i32) -> Option<i16> {
        let local_epoch = i64::from(self.core.quorum_state().leader_epoch);
        if i64::from(request_epoch) < local_epoch {
            Some(FENCED_LEADER_EPOCH)
        } else if i64::from(request_epoch) > local_epoch {
            Some(UNKNOWN_LEADER_EPOCH)
        } else if !self.core.role().is_leader() {
            Some(NOT_LEADER_OR_FOLLOWER)
        } else {
            None
        }
    }

    /// The directory id of this replica, when the voter set records one.
    fn local_directory_id(&self) -> Option<uuid::Uuid> {
        self.core
            .quorum_state()
            .voters
            .get(self.me)
            .map(|voter| voter.directory_id)
            .filter(|directory_id| !directory_id.is_nil())
    }

    /// Kafka's `isValidVoterKey`: a negative voter id names no voter key, and
    /// an empty directory id matches any directory.
    fn is_valid_voter_key(&self, voter_id: i32, voter_directory_id: uuid::Uuid) -> bool {
        if voter_id < 0 {
            return true;
        }
        u64::try_from(voter_id) == Ok(self.me.0)
            && (voter_directory_id.is_nil()
                || self
                    .local_directory_id()
                    .is_none_or(|local| local == voter_directory_id))
    }

    /// Where this replica stands in `PreferredCandidates`, as Kafka's
    /// `endEpochElectionBackoff` walks the list.
    fn successor_rank(
        &self,
        candidates: &[krabka_protocol::owned::end_quorum_epoch_request::ReplicaInfo],
    ) -> SuccessorRank {
        let local_directory_id = self.local_directory_id();
        let position = candidates
            .iter()
            .position(|candidate| {
                u64::try_from(candidate.candidate_id) == Ok(self.me.0) && {
                    let directory_id = wire::uuid_from_wire(candidate.candidate_directory_id);
                    directory_id.is_nil() || local_directory_id.is_none_or(|l| l == directory_id)
                }
            })
            .unwrap_or(candidates.len());
        SuccessorRank {
            position: u32::try_from(position).unwrap_or(u32::MAX),
            successors: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        }
    }

    /// Answers a `Vote` request. `None` means the body did not decode.
    pub(super) fn answer_vote(&mut self, body: &[u8], version: i16) -> Option<Bytes> {
        let request = wire::decode_request::<VoteRequest>(body, version)?;
        let refuse = |engine: &Self, top: i16, partition: i16| {
            Some(wire::encode_vote_response(
                top,
                partition,
                false,
                &engine.quorum_leader(),
                version,
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return refuse(self, INCONSISTENT_CLUSTER_ID, 0);
        }
        if !names_only_the_metadata_partition(request.topics.iter().map(|topic| {
            (
                topic.topic_name.as_str(),
                topic.partitions.iter().map(|p| p.partition_index).collect(),
            )
        })) {
            return refuse(self, INVALID_REQUEST, 0);
        }
        let partition = &request.topics[0].partitions[0];
        // A standard vote's candidate has bumped its epoch past its log, so its
        // last epoch must be below the replica epoch. A pre-vote does not bump.
        let illegal_epoch = if partition.pre_vote {
            partition.last_offset_epoch > partition.replica_epoch
        } else {
            partition.last_offset_epoch >= partition.replica_epoch
        };
        if partition.last_offset < 0 || partition.last_offset_epoch < 0 || illegal_epoch {
            return refuse(self, 0, INVALID_REQUEST);
        }
        if let Some(error) =
            self.voter_only_request_error(partition.replica_id, partition.replica_epoch)
        {
            return refuse(self, 0, error);
        }
        let voter_directory_id = wire::uuid_from_wire(partition.voter_directory_id);
        if !self.is_valid_voter_key(request.voter_id, voter_directory_id) {
            // Kafka moves to the request's epoch before it checks the voter key,
            // so the refusal names the epoch this replica is at afterwards.
            if let (Ok(candidate), Ok(epoch)) = (
                u64::try_from(partition.replica_id),
                Epoch::try_from(partition.replica_epoch),
            ) {
                self.advance_epoch_for_vote(
                    ReplicaKey {
                        id: NodeId(candidate),
                        directory_id: wire::uuid_from_wire(partition.replica_directory_id),
                    },
                    epoch,
                );
            }
            return refuse(self, 0, INVALID_VOTER_KEY);
        }
        // Every field below passed the checks above, so the conversions hold.
        let (Ok(candidate_epoch), Ok(candidate), Ok(last_epoch)) = (
            Epoch::try_from(partition.replica_epoch),
            u64::try_from(partition.replica_id),
            Epoch::try_from(partition.last_offset_epoch),
        ) else {
            return refuse(self, 0, INVALID_REQUEST);
        };
        // The request is addressed to this replica, or to no voter key at all,
        // which Kafka also takes as this replica. The core sees its own key.
        let event = Event::ReceiveVoteRequest {
            from: NodeId(candidate),
            cluster_id: Some(self.core.quorum_state().cluster_id),
            voter_id: self.me,
            voter_directory_id: self
                .core
                .quorum_state()
                .voters
                .get(self.me)
                .map_or(uuid::Uuid::nil(), |voter| voter.directory_id),
            candidate_epoch,
            candidate: NodeId(candidate),
            candidate_directory_id: wire::uuid_from_wire(partition.replica_directory_id),
            candidate_log_end: LogEnd {
                last_epoch,
                last_offset: partition.last_offset,
            },
            pre_vote: partition.pre_vote,
        };
        let granted = self.run_vote_request(event);
        Some(wire::encode_vote_response(
            0,
            0,
            granted,
            &self.quorum_leader(),
            version,
        ))
    }

    fn quorum_epoch_request_leader(
        &self,
        request: &impl QuorumEpochRequest,
    ) -> Result<(NodeId, Epoch), (i16, i16)> {
        if !self.has_valid_cluster_id(request.cluster_id()) {
            return Err((INCONSISTENT_CLUSTER_ID, 0));
        }
        if !names_only_the_metadata_partition(request.topics()) {
            return Err((INVALID_REQUEST, 0));
        }
        let (leader, epoch) = request
            .leader()
            .expect("one metadata partition validated above");
        if let Some(error) = self.voter_only_request_error(leader, epoch) {
            return Err((0, error));
        }
        let (Ok(leader), Ok(epoch)) = (u64::try_from(leader), Epoch::try_from(epoch)) else {
            return Err((0, INVALID_REQUEST));
        };
        let leader = NodeId(leader);
        self.remember_request_leader_endpoints(leader, request.endpoints());
        Ok((leader, epoch))
    }

    /// Answers a `BeginQuorumEpoch` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_begin_quorum_epoch(&mut self, body: &[u8], version: i16) -> Option<Bytes> {
        self.answer_epoch_request(
            body,
            version,
            wire::encode_begin_quorum_epoch_response,
            |engine, request: &BeginQuorumEpochRequest, leader_id, leader_epoch| {
                let partition = &request.topics[0].partitions[0];
                engine.on_event(Event::ReceiveBeginQuorumEpoch {
                    leader_id,
                    leader_epoch,
                });
                // Kafka transitions first, then checks that the request was meant for
                // this replica.
                let voter_directory_id = wire::uuid_from_wire(partition.voter_directory_id);
                if engine.is_valid_voter_key(request.voter_id, voter_directory_id) {
                    0
                } else {
                    INVALID_VOTER_KEY
                }
            },
        )
    }

    /// Answers an `EndQuorumEpoch` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_end_quorum_epoch(&mut self, body: &[u8], version: i16) -> Option<Bytes> {
        self.answer_epoch_request(
            body,
            version,
            wire::encode_end_quorum_epoch_response,
            |engine, request: &EndQuorumEpochRequest, leader_id, leader_epoch| {
                let partition = &request.topics[0].partitions[0];
                engine.on_event(Event::ReceiveEndQuorumEpoch {
                    leader_id,
                    leader_epoch,
                    successor_rank: engine.successor_rank(&partition.preferred_candidates),
                });
                0
            },
        )
    }

    fn answer_epoch_request<Request>(
        &mut self,
        body: &[u8],
        version: i16,
        encode: fn(i16, i16, &wire::QuorumLeader, i16) -> Bytes,
        transition: impl FnOnce(&mut Self, &Request, NodeId, Epoch) -> i16,
    ) -> Option<Bytes>
    where
        Request: QuorumEpochRequest + for<'de> krabka_protocol::Decode<'de>,
    {
        let request = wire::decode_request::<Request>(body, version)?;
        let (top, partition) = match self.quorum_epoch_request_leader(&request) {
            Ok((leader, epoch)) => (0, transition(self, &request, leader, epoch)),
            Err(errors) => errors,
        };
        Some(encode(top, partition, &self.quorum_leader(), version))
    }

    /// Answers a `FetchSnapshot` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_fetch_snapshot(&mut self, body: &[u8], version: i16) -> Option<Bytes> {
        let request = wire::decode_request::<FetchSnapshotRequest>(body, version)?;
        let top_level = |engine: &Self, error_code: i16| {
            Some(wire::encode_fetch_snapshot_answer(
                error_code,
                None,
                &engine.quorum_leader(),
                version,
            ))
        };
        let partition_error = |engine: &Self, error_code: i16| {
            Some(wire::encode_fetch_snapshot_answer(
                0,
                Some(wire::FetchSnapshotPartition {
                    topic: wire::METADATA_TOPIC.into(),
                    index: wire::METADATA_PARTITION,
                    error_code,
                    current_leader: true,
                    chunk: None,
                }),
                &engine.quorum_leader(),
                version,
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return top_level(self, INCONSISTENT_CLUSTER_ID);
        }
        let [topic] = request.topics.as_slice() else {
            return top_level(self, INVALID_REQUEST);
        };
        let [partition] = topic.partitions.as_slice() else {
            return top_level(self, INVALID_REQUEST);
        };
        if topic.name != wire::METADATA_TOPIC || partition.partition != wire::METADATA_PARTITION {
            return Some(wire::encode_fetch_snapshot_answer(
                0,
                Some(wire::FetchSnapshotPartition {
                    topic: topic.name.clone(),
                    index: partition.partition,
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    current_leader: false,
                    chunk: None,
                }),
                &self.quorum_leader(),
                version,
            ));
        }
        if let Some(error) = self.leader_only_request_error(partition.current_leader_epoch) {
            return partition_error(self, error);
        }
        let snapshot_id = (
            partition.snapshot_id.end_offset,
            partition.snapshot_id.epoch,
        );
        let checkpoint = (snapshot_id != BOOTSTRAP_SNAPSHOT_ID)
            .then(|| load_checkpoint_by_id(&self.data_dir, snapshot_id.0, snapshot_id.1))
            .flatten();
        let Some(bytes) = checkpoint else {
            return partition_error(self, SNAPSHOT_NOT_FOUND);
        };
        let size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        if partition.position < 0 || partition.position >= size {
            return partition_error(self, POSITION_OUT_OF_RANGE);
        }
        // A voter catching up through KIP-630 is in contact with the leader even
        // though it sends no Fetch, so score it for check-quorum. Kafka does
        // this once the request passed its checks.
        if let Ok(from) = u64::try_from(request.replica_id) {
            self.on_event(Event::ReceiveFetchSnapshot { from: NodeId(from) });
        }
        // Both fields are slice indices off the wire. The position is inside
        // the checkpoint, and a negative `MaxBytes` reads nothing.
        let max = usize::try_from(request.max_bytes.max(0)).unwrap_or(0);
        let position = usize::try_from(partition.position).unwrap_or(0);
        let chunk = SnapshotReader::byte_range(&bytes, position, max);
        Some(wire::encode_fetch_snapshot_answer(
            0,
            Some(wire::FetchSnapshotPartition {
                topic: wire::METADATA_TOPIC.into(),
                index: wire::METADATA_PARTITION,
                error_code: 0,
                current_leader: true,
                chunk: Some((
                    snapshot_id,
                    size,
                    partition.position,
                    Bytes::copy_from_slice(chunk),
                )),
            }),
            &self.quorum_leader(),
            version,
        ))
    }
}
