//! The `PeerResponse` body codec.
//!
//! `PeerResponse` is the flat response vocabulary the engine reasons in. This
//! module maps each variant onto the generated KIP-595 response message at its
//! captured version, and decodes each one back into the variant the sending
//! engine feeds to the core.

use {
    super::codec::{
        FETCH_SNAPSHOT_VERSION, FETCH_VERSION, METADATA_PARTITION, METADATA_TOPIC,
        METADATA_TOPIC_ID, QUORUM_EPOCH_VERSION, VOTE_VERSION, encode_body, epoch_from_wire,
        epoch_to_wire, node_from_wire, node_to_wire, records_payload_to_bytes,
    },
    crate::kraft::types::{Epoch, LogOffsetMetadata, NodeId},
    bytes::Bytes,
    krabka_protocol::{
        Decode,
        owned::{
            begin_quorum_epoch_response::{self as bqe_resp, BeginQuorumEpochResponse},
            end_quorum_epoch_response::{self as eqe_resp, EndQuorumEpochResponse},
            fetch_response::{self as fetch_resp, FetchResponse},
            fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse},
            vote_response::{self as vote_resp, VoteResponse},
        },
        records::RecordsPayload,
    },
};

#[cfg(test)]
mod tests;

/// A peer RPC response body. The sending engine decodes it back into the
/// matching `Receive*Response` event, or applies it directly for Fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerResponse {
    Vote {
        epoch: Epoch,
        granted: bool,
    },
    /// `BeginQuorumEpoch` and `EndQuorumEpoch` acks carry the responder's
    /// epoch. They produce no core event.
    Ack {
        epoch: Epoch,
    },
    Fetch(FetchAnswer),
    FetchSnapshot {
        snapshot_id: (i64, i32),
        size: i64,
        position: i64,
        bytes: Bytes,
        error_code: i16,
    },
}

/// A Fetch answer, in the one shape Kafka's
/// `KafkaRaftClient.buildFetchResponse` gives both a served fetch and a
/// refused one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchAnswer {
    /// The partition `ErrorCode`: `NONE` from the leader, or
    /// `validateLeaderOnlyRequest`'s `FENCED_LEADER_EPOCH`,
    /// `UNKNOWN_LEADER_EPOCH` or `NOT_LEADER_OR_FOLLOWER`.
    pub error_code: i16,
    /// `CurrentLeader`, and the leader's `NodeEndpoints` entry.
    pub leader: QuorumLeader,
    pub diverging: Option<LogOffsetMetadata>,
    /// When set, the follower's fetch offset is below the leader's pruned
    /// log-start, and the follower must `FetchSnapshot` this snapshot
    /// instead. The tuple is `(end_offset, epoch)`.
    pub snapshot_id: Option<(i64, i32)>,
    /// Leader's high watermark at serve time, -1 in a refusal.
    pub hwm: i64,
    /// The responder's log start offset.
    pub log_start_offset: i64,
    /// Verbatim concatenated `RecordBatch` bytes for `[fetch_offset, log_end)`.
    pub records: Bytes,
}

impl FetchAnswer {
    /// Encodes the answer as a Fetch response body (api 1) at `version`, the
    /// version of the request it answers.
    #[must_use]
    pub fn encode(&self, version: i16) -> Bytes {
        // Kafka always sets `Records`, to `MemoryRecords.EMPTY` when
        // there are none, and leaves `AbortedTransactions` at its
        // generated default, an empty list: neither is null on the
        // wire.
        let mut partition = fetch_resp::PartitionData {
            partition_index: METADATA_PARTITION,
            error_code: self.error_code,
            high_watermark: self.hwm,
            log_start_offset: self.log_start_offset,
            aborted_transactions: Some(Vec::new()),
            current_leader: fetch_resp::LeaderIdAndEpoch {
                leader_id: self.leader.leader_id_to_wire(),
                leader_epoch: epoch_to_wire(self.leader.epoch),
                ..Default::default()
            },
            records: Some(RecordsPayload::Raw(self.records.clone())),
            ..Default::default()
        };
        if let Some(point) = self.diverging {
            partition.diverging_epoch = fetch_resp::EpochEndOffset {
                epoch: epoch_to_wire(point.epoch),
                end_offset: point.offset,
                ..Default::default()
            };
        }
        if let Some((end_offset, epoch)) = self.snapshot_id {
            partition.snapshot_id = fetch_resp::SnapshotId {
                end_offset,
                epoch,
                ..Default::default()
            };
        }
        // `RaftUtil.singletonFetchResponse`: from v16 `NodeEndpoints`
        // names the leader when its id and endpoint are known.
        let resp = FetchResponse {
            responses: vec![fetch_resp::FetchableTopicResponse {
                topic: METADATA_TOPIC.to_string(),
                topic_id: METADATA_TOPIC_ID,
                partitions: vec![partition],
                ..Default::default()
            }],
            node_endpoints: self
                .leader
                .node_endpoint()
                .filter(|_| version >= 17)
                .map(|(node_id, host, port)| fetch_resp::NodeEndpoint {
                    node_id,
                    host,
                    port: i32::from(port),
                    ..Default::default()
                })
                .into_iter()
                .collect(),
            ..Default::default()
        };
        encode_body(&resp, version)
    }
}

/// Encodes Kafka's bare top-level Fetch error response at `version`:
/// `new FetchResponseData().setErrorCode(error)`, with no topic.
#[must_use]
pub fn encode_fetch_top_level_error(error_code: i16, version: i16) -> Bytes {
    let resp = FetchResponse {
        error_code,
        ..Default::default()
    };
    encode_body(&resp, version)
}

/// What a quorum RPC response says about the responder's view of the leader,
/// as Kafka's `RaftUtil.singleton*Response` helpers fill it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumLeader {
    /// The leader of the epoch, or `None` (wire -1) while it is unknown.
    pub leader_id: Option<NodeId>,
    /// The responder's epoch.
    pub epoch: Epoch,
    /// The leader's controller-listener host and port. The response carries it
    /// in `NodeEndpoints` only when the leader is known.
    pub endpoint: Option<(String, u16)>,
}

impl QuorumLeader {
    fn leader_id_to_wire(&self) -> i32 { self.leader_id.map_or(-1, node_to_wire) }

    /// The `NodeEndpoints` entry, when the leader and its endpoint are known.
    fn node_endpoint(&self) -> Option<(i32, String, u16)> {
        let leader_id = self.leader_id?;
        let (host, port) = self.endpoint.clone()?;
        Some((node_to_wire(leader_id), host, port))
    }
}

/// Encodes a Vote response body (api 52) at `version`, the version of the
/// request it answers.
///
/// A `top_level_error` other than 0 is Kafka's bare error response: the code
/// and nothing else. Otherwise the body names the metadata partition with
/// `partition_error`, the grant and the responder's leader view.
#[must_use]
pub fn encode_vote_response(
    top_level_error: i16,
    partition_error: i16,
    vote_granted: bool,
    leader: &QuorumLeader,
    version: i16,
) -> Bytes {
    let resp = if top_level_error == 0 {
        VoteResponse {
            topics: vec![vote_resp::TopicData {
                topic_name: METADATA_TOPIC.to_string(),
                partitions: vec![vote_resp::PartitionData {
                    partition_index: METADATA_PARTITION,
                    error_code: partition_error,
                    leader_id: leader.leader_id_to_wire(),
                    leader_epoch: epoch_to_wire(leader.epoch),
                    vote_granted,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            node_endpoints: leader
                .node_endpoint()
                .map(|(node_id, host, port)| vote_resp::NodeEndpoint {
                    node_id,
                    host,
                    port,
                    ..Default::default()
                })
                .into_iter()
                .collect(),
            ..Default::default()
        }
    } else {
        VoteResponse {
            error_code: top_level_error,
            ..Default::default()
        }
    };
    encode_body(&resp, version)
}

// Begin/EndQuorumEpoch share Kafka's complete response layout.
macro_rules! quorum_epoch_response_encoder {
    ($(#[$attr:meta])* $name:ident, $response:ident, $wire:ident) => {
        $(#[$attr])*
        pub fn $name(
            top_level_error: i16,
            partition_error: i16,
            leader: &QuorumLeader,
            version: i16,
        ) -> Bytes {
            let response = if top_level_error == 0 {
                $response {
                    topics: vec![$wire::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![$wire::PartitionData {
                            partition_index: METADATA_PARTITION,
                            error_code: partition_error,
                            leader_id: leader.leader_id_to_wire(),
                            leader_epoch: epoch_to_wire(leader.epoch),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    node_endpoints: leader
                        .node_endpoint()
                        .map(|(node_id, host, port)| $wire::NodeEndpoint {
                            node_id,
                            host,
                            port,
                            ..Default::default()
                        })
                        .into_iter()
                        .collect(),
                    ..Default::default()
                }
            } else {
                $response {
                    error_code: top_level_error,
                    ..Default::default()
                }
            };
            encode_body(&response, version)
        }
    };
}

quorum_epoch_response_encoder! {
    /// Encodes a `BeginQuorumEpoch` response body (api 53) at `version`, with the
    /// same shape rules as [`encode_vote_response`].
    #[must_use]
    encode_begin_quorum_epoch_response, BeginQuorumEpochResponse, bqe_resp
}
quorum_epoch_response_encoder! {
    /// Encodes an `EndQuorumEpoch` response body (api 54) at `version`, with the
    /// same shape rules as [`encode_vote_response`].
    #[must_use]
    encode_end_quorum_epoch_response, EndQuorumEpochResponse, eqe_resp
}

/// The one partition of a `FetchSnapshot` answer that names a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSnapshotPartition {
    /// The topic the request named. It is `__cluster_metadata` except in the
    /// `UNKNOWN_TOPIC_OR_PARTITION` answer.
    pub topic: String,
    /// The partition the request named.
    pub index: i32,
    pub error_code: i16,
    /// Whether `CurrentLeader` carries the responder's leader view. Kafka's
    /// `addQuorumLeader` fills it in every answer except
    /// `UNKNOWN_TOPIC_OR_PARTITION`.
    pub current_leader: bool,
    /// The snapshot id, size, position and bytes of a served chunk.
    pub chunk: Option<((i64, i32), i64, i64, Bytes)>,
}

/// Encodes a `FetchSnapshot` response body (api 59) at `version`, as Kafka's
/// `RaftUtil.singletonFetchSnapshotResponse` builds it.
///
/// `partition` `None` is Kafka's bare top-level error response,
/// `FetchSnapshotResponse.withTopLevelError`. Otherwise `NodeEndpoints` names
/// the leader when the leader and its endpoint are known.
#[must_use]
pub fn encode_fetch_snapshot_answer(
    top_level_error: i16,
    partition: Option<FetchSnapshotPartition>,
    leader: &QuorumLeader,
    version: i16,
) -> Bytes {
    let Some(partition) = partition else {
        let resp = FetchSnapshotResponse {
            error_code: top_level_error,
            ..Default::default()
        };
        return encode_body(&resp, version);
    };
    let mut snapshot = fs_resp::PartitionSnapshot {
        index: partition.index,
        error_code: partition.error_code,
        ..Default::default()
    };
    if partition.current_leader {
        snapshot.current_leader = fs_resp::LeaderIdAndEpoch {
            leader_id: leader.leader_id_to_wire(),
            leader_epoch: epoch_to_wire(leader.epoch),
            ..Default::default()
        };
    }
    if let Some(((end_offset, epoch), size, position, bytes)) = partition.chunk {
        snapshot.snapshot_id = fs_resp::SnapshotId {
            end_offset,
            epoch,
            ..Default::default()
        };
        snapshot.size = size;
        snapshot.position = position;
        snapshot.unaligned_records = RecordsPayload::Raw(bytes);
    }
    let resp = FetchSnapshotResponse {
        error_code: top_level_error,
        topics: vec![fs_resp::TopicSnapshot {
            name: partition.topic,
            partitions: vec![snapshot],
            ..Default::default()
        }],
        node_endpoints: leader
            .node_endpoint()
            .map(|(node_id, host, port)| fs_resp::NodeEndpoint {
                node_id,
                host,
                port,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    };
    encode_body(&resp, version)
}

/// Encodes a `FetchSnapshot` response body (api 59).
fn encode_fetch_snapshot_response(
    snapshot_id: (i64, i32),
    size: i64,
    position: i64,
    bytes: &Bytes,
    error_code: i16,
) -> Bytes {
    let (end_offset, epoch) = snapshot_id;
    let resp = FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_resp::PartitionSnapshot {
                index: METADATA_PARTITION,
                error_code,
                snapshot_id: fs_resp::SnapshotId {
                    end_offset,
                    epoch,
                    ..Default::default()
                },
                size,
                position,
                unaligned_records: RecordsPayload::Raw(bytes.clone()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    encode_body(&resp, FETCH_SNAPSHOT_VERSION)
}

impl PeerResponse {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        match self {
            PeerResponse::Vote { epoch, granted } => {
                let resp = VoteResponse {
                    topics: vec![vote_resp::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![vote_resp::PartitionData {
                            partition_index: METADATA_PARTITION,
                            leader_id: -1,
                            leader_epoch: epoch_to_wire(*epoch),
                            vote_granted: *granted,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&resp, VOTE_VERSION)
            }
            PeerResponse::Ack { epoch } => {
                // A Begin/End ack is encoded as the corresponding
                // BeginQuorumEpochResponse with the responder's leader_epoch.
                let resp = BeginQuorumEpochResponse {
                    topics: vec![
                        krabka_protocol::owned::begin_quorum_epoch_response::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![
                                krabka_protocol::owned::begin_quorum_epoch_response::PartitionData {
                                    partition_index: METADATA_PARTITION,
                                    leader_id: -1,
                                    leader_epoch: epoch_to_wire(*epoch),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                encode_body(&resp, QUORUM_EPOCH_VERSION)
            }
            PeerResponse::Fetch(answer) => answer.encode(FETCH_VERSION),
            PeerResponse::FetchSnapshot {
                snapshot_id,
                size,
                position,
                bytes,
                error_code,
            } => encode_fetch_snapshot_response(*snapshot_id, *size, *position, bytes, *error_code),
        }
    }

    /// Decodes a Vote response body (api 52). The round, pre-vote or
    /// real, is not on the wire. The engine infers it from the candidate's
    /// role.
    #[must_use]
    pub fn decode_vote(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        Some(PeerResponse::Vote {
            epoch: epoch_from_wire(p.leader_epoch),
            granted: p.vote_granted,
        })
    }

    /// Decodes a `BeginQuorumEpoch` or `EndQuorumEpoch` ack body
    /// (api 53 and api 54).
    #[must_use]
    pub fn decode_ack(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        Some(PeerResponse::Ack {
            epoch: epoch_from_wire(p.leader_epoch),
        })
    }

    /// Decodes a Fetch response body (api 1).
    ///
    /// The leader's endpoint is the `NodeEndpoints` entry whose node id is
    /// `CurrentLeader.LeaderId`, as Kafka's `Endpoints.fromFetchResponse`
    /// selects it; an entry for any other node is ignored.
    #[must_use]
    pub fn decode_fetch(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = FetchResponse::decode(&mut cur, FETCH_VERSION).ok()?;
        let p = resp.responses.first()?.partitions.first()?;
        let wire_leader = p.current_leader.leader_id;
        let leader_id = (wire_leader >= 0).then(|| node_from_wire(wire_leader));
        let endpoint = leader_id.and_then(|_| {
            resp.node_endpoints
                .iter()
                .find(|endpoint| endpoint.node_id == wire_leader)
                .and_then(|endpoint| {
                    u16::try_from(endpoint.port)
                        .ok()
                        .map(|port| (endpoint.host.clone(), port))
                })
        });
        let leader = QuorumLeader {
            leader_id,
            epoch: epoch_from_wire(p.current_leader.leader_epoch),
            endpoint,
        };
        // diverging_epoch defaults to (-1, -1); a real divergence carries a
        // non-negative end_offset.
        let diverging = if p.diverging_epoch.end_offset >= 0 {
            Some(LogOffsetMetadata {
                offset: p.diverging_epoch.end_offset,
                epoch: epoch_from_wire(p.diverging_epoch.epoch),
            })
        } else {
            None
        };
        let snapshot_id = if p.snapshot_id.end_offset >= 0 {
            Some((p.snapshot_id.end_offset, p.snapshot_id.epoch))
        } else {
            None
        };
        let records = p
            .records
            .as_ref()
            .map_or_else(Bytes::new, records_payload_to_bytes);
        Some(PeerResponse::Fetch(FetchAnswer {
            error_code: p.error_code,
            leader,
            diverging,
            snapshot_id,
            hwm: p.high_watermark,
            log_start_offset: p.log_start_offset,
            records,
        }))
    }

    /// Decodes a `FetchSnapshot` response body (api 59).
    #[must_use]
    pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = FetchSnapshotResponse::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        let bytes = records_payload_to_bytes(&p.unaligned_records);
        Some(PeerResponse::FetchSnapshot {
            snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
            size: p.size,
            position: p.position,
            bytes,
            error_code: p.error_code,
        })
    }
}
