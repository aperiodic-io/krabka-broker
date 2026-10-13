//! Constants and integer conversions shared by the peer request and response
//! codecs.
//!
//! The metadata topic identity, the captured wire versions, the `Epoch`,
//! `NodeId`, uuid and cluster id conversions to and from their wire forms, and the
//! `encode_body` helper are all needed by both [`request`](super::request) and
//! [`response`](super::response), so they sit in one place instead of being
//! duplicated.

use {
    crate::kraft::types::{Epoch, NodeId},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::{Bytes, BytesMut},
    krabka_protocol::{Encode, primitives::uuid::Uuid as MetaUuid, records::RecordsPayload},
};

/// `KRaft` metadata log topic name.
pub const METADATA_TOPIC: &str = "__cluster_metadata";
/// The single metadata partition.
pub const METADATA_PARTITION: i32 = 0;
/// The fixed `KRaft` `__cluster_metadata` topic id (KIP-595). Fetch v13 and
/// above key the topic by this id, not by name.
pub const METADATA_TOPIC_ID: MetaUuid = MetaUuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

/// Captured flexible wire versions, byte-validated against fixture frames.
pub const VOTE_VERSION: i16 = 2;
pub const QUORUM_EPOCH_VERSION: i16 = 1;
pub const FETCH_VERSION: i16 = 18;
pub const FETCH_SNAPSHOT_VERSION: i16 = 1;

pub fn records_payload_to_bytes(payload: &RecordsPayload) -> Bytes {
    match payload {
        RecordsPayload::Raw(bytes) => bytes.clone(),
        other => {
            let mut out = BytesMut::new();
            let _ = other.encode_to(&mut out);
            out.freeze()
        }
    }
}

/// Converts between the consensus `Epoch` (u32) and the wire `i32`.
/// `KRaft` uses an i32 `leaderEpoch`. The KIP-595 wire carries the leader
/// epoch as a raw `int32` and stays raw here. The consensus epoch is always
/// non-negative.
pub fn epoch_to_wire(e: Epoch) -> i32 { i32::try_from(e).unwrap_or(i32::MAX) }
pub fn epoch_from_wire(e: i32) -> Epoch { u32::try_from(e).unwrap_or(0) }
/// Converts between the `NodeId` (u64) and the wire `i32` replica id.
pub fn node_to_wire(n: NodeId) -> i32 { i32::try_from(n.0).unwrap_or(i32::MAX) }
pub fn node_from_wire(n: i32) -> NodeId { NodeId(u64::try_from(n).unwrap_or(0)) }
/// Converts between a `uuid::Uuid` and the wire `uuid` of the generated codecs.
pub fn uuid_to_wire(id: uuid::Uuid) -> MetaUuid { MetaUuid(id.into_bytes()) }
pub fn uuid_from_wire(id: MetaUuid) -> uuid::Uuid { uuid::Uuid::from_bytes(id.0) }
/// A request `ClusterId` in Kafka's base64 form.
pub fn cluster_id_to_wire(id: Option<uuid::Uuid>) -> Option<String> {
    id.map(|id| URL_SAFE_NO_PAD.encode(id.as_bytes()))
}

pub fn encode_body<T: Encode>(msg: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    // Generated codecs only error on out-of-range version, which is fixed
    // here, so encode is infallible in practice.
    let _ = msg.encode(&mut buf, version);
    buf.freeze()
}
