//! The `PeerRequest` body codec.
//!
//! `PeerRequest` is the flat request vocabulary the engine reasons in. This
//! module maps each variant onto the generated KIP-595 request message at its
//! captured version, and decodes each one back.

use {
    super::codec::{
        FETCH_SNAPSHOT_VERSION, FETCH_VERSION, METADATA_PARTITION, METADATA_TOPIC,
        METADATA_TOPIC_ID, QUORUM_EPOCH_VERSION, VOTE_VERSION, cluster_id_to_wire, encode_body,
        epoch_from_wire, epoch_to_wire, node_from_wire, node_to_wire, uuid_from_wire, uuid_to_wire,
    },
    crate::kraft::types::{Epoch, NodeId},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::Bytes,
    krabka_protocol::{
        Decode,
        owned::{
            begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
            end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
            fetch_request::{self as fetch_req, FetchRequest},
            fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
            vote_request::{self as vote_req, VoteRequest},
        },
    },
    krabka_verified::{
        VoteWireDecision,
        vote::{VoteEncodeDecision, vote_encode_decision},
        vote_wire_decision,
    },
};

macro_rules! singleton_metadata_topic {
    ($topic:ty, $partition:expr) => {{
        let mut topic: $topic = Default::default();
        topic.topic_name = METADATA_TOPIC.to_string();
        topic.partitions = vec![$partition];
        topic
    }};
}

#[cfg(test)]
mod tests;

/// A peer RPC request body, as encoded by the sending engine and decoded by
/// the receiving engine's inbound dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRequest {
    Vote {
        cluster_id: Option<uuid::Uuid>,
        /// The recipient voter this request is addressed to. This is the
        /// wire top-level `voterId`. The JVM validates that an incoming
        /// Vote is addressed to it before it considers the grant, and it
        /// silently rejects a stale `voterId` or a `voterId` of `-1`.
        /// `broadcast_vote` builds this field for each recipient.
        voter_id: NodeId,
        voter_directory_id: uuid::Uuid,
        candidate_epoch: Epoch,
        candidate: NodeId,
        candidate_directory_id: uuid::Uuid,
        last_epoch: Epoch,
        last_offset: i64,
        pre_vote: bool,
    },
    BeginQuorumEpoch {
        /// The sender's cluster id, which the recipient checks.
        cluster_id: Option<uuid::Uuid>,
        /// The recipient voter key: `VoterId` and `VoterDirectoryId`, which
        /// the recipient checks against its own (`isValidVoterKey`).
        /// `broadcast_begin_quorum_epoch` builds it for each recipient.
        voter_id: NodeId,
        voter_directory_id: uuid::Uuid,
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// The leader's own listeners, `(name, host, port)`:
        /// `LeaderEndpoints`, which a recipient whose voter set does not
        /// name the leader dials it at.
        leader_endpoints: Vec<(String, String, u16)>,
    },
    EndQuorumEpoch {
        /// The sender's cluster id, which the recipient checks.
        cluster_id: Option<uuid::Uuid>,
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// The voters in order of replication progress, most caught up first,
        /// each with its directory id: `PreferredCandidates` (KIP-853).
        preferred_candidates: Vec<(NodeId, uuid::Uuid)>,
    },
    Fetch {
        /// The sender's cluster id, which the leader checks.
        cluster_id: Option<uuid::Uuid>,
        from: NodeId,
        /// How long the leader may hold the request when it has nothing new:
        /// `MaxWaitMs`.
        max_wait_ms: i32,
        /// The epoch the sender is in: `CurrentLeaderEpoch`, which the
        /// responder checks in `validateLeaderOnlyRequest`. Raw, as for
        /// `FetchSnapshot`, so a negative epoch stays fenced.
        current_leader_epoch: i32,
        /// The epoch of the sender's last fetched record: `LastFetchedEpoch`,
        /// which the leader checks for divergence.
        fetch_epoch: Epoch,
        fetch_offset: i64,
        replica_directory_id: uuid::Uuid,
        /// The sender's high watermark, or -1 while it knows none:
        /// `HighWatermark` (v18). The leader answers at once when its own is
        /// higher.
        high_watermark: i64,
    },
    FetchSnapshot {
        /// The sender's cluster id, which the leader checks.
        cluster_id: Option<uuid::Uuid>,
        from: NodeId,
        /// The epoch the sender believes the leader holds:
        /// `CurrentLeaderEpoch`, which the leader checks against its own.
        current_leader_epoch: i32,
        snapshot_id: (i64, i32),
        position: i64,
        max_bytes: i32,
    },
}

impl PeerRequest {
    /// Encode a request whose Vote fields have already passed host validation.
    ///
    /// # Panics
    ///
    /// Panics if a Vote identity or epoch exceeds Kafka's signed `int32`
    /// range. Production Vote sends use [`Self::try_encode`] and fail closed.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        self.try_encode()
            .expect("Vote fields must fit Kafka signed int32 wire fields")
    }

    /// Encode after checking the Vote identity and epoch wire range.
    #[must_use]
    pub fn try_encode(&self) -> Option<Bytes> {
        match *self {
            PeerRequest::Vote {
                cluster_id,
                voter_id,
                voter_directory_id,
                candidate_epoch,
                candidate,
                candidate_directory_id,
                last_epoch,
                last_offset,
                pre_vote,
            } => {
                if vote_encode_decision(voter_id.0, candidate.0, candidate_epoch, last_epoch)
                    != VoteEncodeDecision::Accept
                {
                    return None;
                }
                let req = VoteRequest {
                    cluster_id: cluster_id_to_wire(cluster_id),
                    voter_id: i32::try_from(voter_id.0).ok()?,
                    topics: vec![vote_req::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![vote_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            replica_epoch: i32::try_from(candidate_epoch).ok()?,
                            replica_id: i32::try_from(candidate.0).ok()?,
                            replica_directory_id: uuid_to_wire(candidate_directory_id),
                            voter_directory_id: uuid_to_wire(voter_directory_id),
                            last_offset_epoch: i32::try_from(last_epoch).ok()?,
                            last_offset,
                            pre_vote,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, VOTE_VERSION))
            }
            PeerRequest::BeginQuorumEpoch {
                cluster_id,
                voter_id,
                voter_directory_id,
                leader_id,
                leader_epoch,
                ref leader_endpoints,
            } => {
                // Kafka's `RaftUtil.singletonBeginQuorumEpochRequest`.
                let req = BeginQuorumEpochRequest {
                    cluster_id: cluster_id_to_wire(cluster_id),
                    voter_id: node_to_wire(voter_id),
                    leader_endpoints: leader_endpoints
                        .iter()
                        .map(|(name, host, port)| bqe_req::LeaderEndpoint {
                            name: name.clone(),
                            host: host.clone(),
                            port: *port,
                            ..Default::default()
                        })
                        .collect(),
                    topics: vec![singleton_metadata_topic!(
                        bqe_req::TopicData,
                        bqe_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            voter_directory_id: uuid_to_wire(voter_directory_id),
                            leader_id: node_to_wire(leader_id),
                            leader_epoch: epoch_to_wire(leader_epoch),
                            ..Default::default()
                        }
                    )],
                    ..Default::default()
                };
                Some(encode_body(&req, QUORUM_EPOCH_VERSION))
            }
            PeerRequest::EndQuorumEpoch {
                cluster_id,
                leader_id,
                leader_epoch,
                ref preferred_candidates,
            } => {
                // Kafka's `RaftUtil.singletonEndQuorumEpochRequest`, which
                // names the successors both ways: `PreferredSuccessors` for
                // v0, `PreferredCandidates` from v1.
                let req = EndQuorumEpochRequest {
                    cluster_id: cluster_id_to_wire(cluster_id),
                    topics: vec![singleton_metadata_topic!(
                        eqe_req::TopicData,
                        eqe_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            leader_id: node_to_wire(leader_id),
                            leader_epoch: epoch_to_wire(leader_epoch),
                            preferred_successors: preferred_candidates
                                .iter()
                                .map(|&(candidate, _)| node_to_wire(candidate))
                                .collect(),
                            preferred_candidates: preferred_candidates
                                .iter()
                                .map(|&(candidate, directory_id)| eqe_req::ReplicaInfo {
                                    candidate_id: node_to_wire(candidate),
                                    candidate_directory_id: uuid_to_wire(directory_id),
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        }
                    )],
                    ..Default::default()
                };
                Some(encode_body(&req, QUORUM_EPOCH_VERSION))
            }
            PeerRequest::Fetch {
                cluster_id,
                from,
                max_wait_ms,
                current_leader_epoch,
                fetch_epoch,
                fetch_offset,
                replica_directory_id,
                high_watermark,
            } => {
                // Kafka's `KafkaRaftClient.buildFetchRequest`.
                let req = FetchRequest {
                    cluster_id: cluster_id_to_wire(cluster_id),
                    replica_id: node_to_wire(from),
                    max_wait_ms,
                    min_bytes: 1,
                    max_bytes: 1024 * 1024,
                    replica_state: fetch_req::ReplicaState {
                        replica_id: node_to_wire(from),
                        ..Default::default()
                    },
                    topics: vec![fetch_req::FetchTopic {
                        topic: METADATA_TOPIC.to_string(),
                        topic_id: METADATA_TOPIC_ID,
                        partitions: vec![fetch_req::FetchPartition {
                            partition: METADATA_PARTITION,
                            current_leader_epoch,
                            fetch_offset,
                            last_fetched_epoch: epoch_to_wire(fetch_epoch),
                            replica_directory_id: uuid_to_wire(replica_directory_id),
                            high_watermark,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, FETCH_VERSION))
            }
            PeerRequest::FetchSnapshot {
                cluster_id,
                from,
                current_leader_epoch,
                snapshot_id,
                position,
                max_bytes,
            } => Some(encode_fetch_snapshot_request(
                cluster_id,
                from,
                current_leader_epoch,
                snapshot_id,
                (position, max_bytes),
            )),
        }
    }

    /// Decodes a request body. Returns `None` on a malformed frame.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> { decode_vote(buf) }
}

/// Decodes a request body at `version`. The inbound handlers run Kafka's
/// request checks on the result, so this does not refuse a field value: only a
/// body that does not decode gives `None`. Kafka's
/// `RequestContext.parseRequest` fails such a request, and the connection
/// closes. Like that parser, this ignores bytes after the message.
pub(crate) fn decode_request<T>(buf: &[u8], version: i16) -> Option<T>
where
    T: for<'de> Decode<'de>,
{
    let mut cur = buf;
    T::decode(&mut cur, version).ok()
}

/// Kafka's `FetchRequest.replicaId`: the top-level `ReplicaId` up to v14, the
/// `ReplicaState` from v15.
#[must_use]
pub fn fetch_replica_id(request: &FetchRequest, version: i16) -> i32 {
    if version >= 15 {
        request.replica_state.replica_id
    } else {
        request.replica_id
    }
}

/// Decodes a Vote request body (api 52).
#[must_use]
pub fn decode_vote(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = VoteRequest::decode(&mut cur, VOTE_VERSION).ok()?;
    if !cur.is_empty() || req.topics.len() != 1 {
        return None;
    }
    let topic = req.topics.first()?;
    if topic.topic_name != METADATA_TOPIC || topic.partitions.len() != 1 {
        return None;
    }
    let p = topic.partitions.first()?;
    if p.partition_index != METADATA_PARTITION
        || vote_wire_decision(
            req.voter_id,
            p.replica_id,
            p.replica_epoch,
            p.last_offset_epoch,
        ) != VoteWireDecision::Accept
    {
        return None;
    }
    Some(PeerRequest::Vote {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        voter_id: NodeId(u64::try_from(req.voter_id).ok()?),
        voter_directory_id: uuid_from_wire(p.voter_directory_id),
        candidate_epoch: u32::try_from(p.replica_epoch).ok()?,
        candidate: NodeId(u64::try_from(p.replica_id).ok()?),
        candidate_directory_id: uuid_from_wire(p.replica_directory_id),
        last_epoch: u32::try_from(p.last_offset_epoch).ok()?,
        last_offset: p.last_offset,
        pre_vote: p.pre_vote,
    })
}

/// Parses an optional request `ClusterId`. An absent one is `Ok(None)`; one
/// that is present but does not parse is the error.
fn request_cluster_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, ()> {
    value.map(|id| parse_cluster_id(id).ok_or(())).transpose()
}

/// Parses a request `ClusterId`: Kafka's base64 form, or the hyphenated form.
pub(crate) fn parse_cluster_id(value: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(value).ok().or_else(|| {
        let bytes: [u8; 16] = URL_SAFE_NO_PAD.decode(value).ok()?.try_into().ok()?;
        Some(uuid::Uuid::from_bytes(bytes))
    })
}

/// Decodes a `BeginQuorumEpoch` request body (api 53).
#[must_use]
pub fn decode_begin(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<BeginQuorumEpochRequest>(buf, QUORUM_EPOCH_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::BeginQuorumEpoch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        voter_id: node_from_wire(req.voter_id),
        voter_directory_id: uuid_from_wire(p.voter_directory_id),
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
        leader_endpoints: req
            .leader_endpoints
            .iter()
            .map(|endpoint| (endpoint.name.clone(), endpoint.host.clone(), endpoint.port))
            .collect(),
    })
}

/// Decodes an `EndQuorumEpoch` request body (api 54).
#[must_use]
pub fn decode_end(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<EndQuorumEpochRequest>(buf, QUORUM_EPOCH_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::EndQuorumEpoch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
        preferred_candidates: p
            .preferred_candidates
            .iter()
            .map(|candidate| {
                (
                    node_from_wire(candidate.candidate_id),
                    uuid_from_wire(candidate.candidate_directory_id),
                )
            })
            .collect(),
    })
}

/// Decodes a Fetch request body (api 1).
#[must_use]
pub fn decode_fetch(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<FetchRequest>(buf, FETCH_VERSION)?;
    let from = node_from_wire(req.replica_state.replica_id);
    let p = req.topics.first()?.partitions.first()?;
    let replica_directory_id = uuid_from_wire(p.replica_directory_id);
    Some(PeerRequest::Fetch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        from,
        max_wait_ms: req.max_wait_ms,
        current_leader_epoch: p.current_leader_epoch,
        fetch_epoch: epoch_from_wire(p.last_fetched_epoch),
        fetch_offset: p.fetch_offset,
        replica_directory_id,
        high_watermark: p.high_watermark,
    })
}

/// Encodes a `FetchSnapshot` request body (api 59), as Kafka's
/// `RaftUtil.singletonFetchSnapshotRequest` builds it.
fn encode_fetch_snapshot_request(
    cluster_id: Option<uuid::Uuid>,
    from: NodeId,
    current_leader_epoch: i32,
    snapshot_id: (i64, i32),
    (position, max_bytes): (i64, i32),
) -> Bytes {
    let (end_offset, epoch) = snapshot_id;
    let req = FetchSnapshotRequest {
        cluster_id: cluster_id_to_wire(cluster_id),
        replica_id: node_to_wire(from),
        max_bytes,
        topics: vec![fs_req::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_req::PartitionSnapshot {
                partition: METADATA_PARTITION,
                current_leader_epoch,
                snapshot_id: fs_req::SnapshotId {
                    end_offset,
                    epoch,
                    ..Default::default()
                },
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    encode_body(&req, FETCH_SNAPSHOT_VERSION)
}

/// Decodes a `FetchSnapshot` request body (api 59).
#[must_use]
pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<FetchSnapshotRequest>(buf, FETCH_SNAPSHOT_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::FetchSnapshot {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        from: node_from_wire(req.replica_id),
        current_leader_epoch: p.current_leader_epoch,
        snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
        position: p.position,
        max_bytes: req.max_bytes,
    })
}
