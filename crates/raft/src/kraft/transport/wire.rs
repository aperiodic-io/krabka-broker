//! Real KIP-595 peer-RPC body codec.
//!
//! The engine's loop reasons in terms of the flat `PeerRequest` and
//! `PeerResponse` enums. This module maps each variant to and from the genuine
//! generated KIP-595 message bodies. Those bodies are header-less, because the
//! framing layer in `server.rs` and `network.rs` adds the request header and
//! the response header. The captured wire versions are Vote v2,
//! `BeginQuorumEpoch` v1, `EndQuorumEpoch` v1, and Fetch v18. Krabka-to-Krabka
//! replication rides these exact bytes.
//!
//! The metadata log is the single `KRaft` topic `__cluster_metadata`, partition
//! 0, so every RPC body carries exactly one topic and exactly one partition.
//! Kafka's `VoteResponse` carries no pre-vote field. A candidate matches a
//! reply to its round from its own `Prospective` or `Candidate` role, so Krabka
//! encodes a byte-faithful `VoteResponse` and the core infers the round itself
//! (KIP-996).
//!
//! The shared constants and integer conversions live in `codec`, the request
//! bodies in `request`, and the response bodies in `response`.

mod codec;
mod request;
mod response;

/// The versions [`PeerRequest`] encodes each peer RPC at. Public because a
/// broker-only observer sends `FetchSnapshot` itself, and the version on its
/// request header has to be the one the body was encoded at.
pub use self::codec::{FETCH_SNAPSHOT_VERSION, FETCH_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION};
pub(crate) use self::{
    codec::{
        METADATA_PARTITION, METADATA_TOPIC, METADATA_TOPIC_ID, epoch_from_wire, node_from_wire,
        uuid_from_wire,
    },
    request::{decode_request, parse_cluster_id},
};
pub use self::{
    request::{
        PeerRequest, decode_begin, decode_end, decode_fetch, decode_fetch_snapshot, decode_vote,
        fetch_replica_id,
    },
    response::{
        FetchAnswer, FetchSnapshotPartition, PeerResponse, QuorumLeader,
        encode_begin_quorum_epoch_response, encode_end_quorum_epoch_response,
        encode_fetch_snapshot_answer, encode_fetch_top_level_error, encode_vote_response,
    },
};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::{
        PeerRequest, PeerResponse,
        codec::{FETCH_VERSION, METADATA_TOPIC_ID},
    };
    use crate::kraft::types::NodeId;

    #[test]
    fn fetch_wire_carries_metadata_topic_id() {
        use krabka_protocol::{
            Decode,
            owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        };
        let req = PeerRequest::Fetch {
            from: NodeId(2),
            current_leader_epoch: 1,
            fetch_epoch: 1,
            fetch_offset: 5,
            replica_directory_id: uuid::Uuid::nil(),
            cluster_id: None,
            max_wait_ms: 0,
            high_watermark: -1,
        };
        let mut c = &req.encode()[..];
        let dreq = FetchRequest::decode(&mut c, FETCH_VERSION).unwrap();
        assert2::assert!(dreq.topics[0].topic_id == METADATA_TOPIC_ID);

        let resp = PeerResponse::Fetch(super::FetchAnswer {
            error_code: 0,
            leader: super::QuorumLeader {
                leader_id: Some(NodeId(1)),
                epoch: 4,
                endpoint: None,
            },
            diverging: None,
            snapshot_id: None,
            hwm: 0,
            log_start_offset: 0,
            records: Bytes::new(),
        });
        let mut c2 = &resp.encode()[..];
        let dresp = FetchResponse::decode(&mut c2, FETCH_VERSION).unwrap();
        assert2::assert!(dresp.responses[0].topic_id == METADATA_TOPIC_ID);
    }
}
