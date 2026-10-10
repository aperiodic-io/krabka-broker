//! Shard-addressed WAL RPC descriptors.

use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_protocol::{Decode, Encode, owned::fetch_request::FetchRequest};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

const KRAFT_METADATA_TOPIC_ID: krabka_protocol::primitives::uuid::Uuid =
    krabka_protocol::primitives::uuid::Uuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
pub(crate) const KIP_595_FETCH_VERSION: i16 = 17;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub(crate) const OFFSET_OUT_OF_RANGE: i16 = 1;
pub(crate) const FENCED_LEADER_EPOCH: i16 = 74;
pub(crate) const UNKNOWN_LEADER_EPOCH: i16 = 75;
const WAL_FETCH_RACK_ID: &str = "__krabka_diskless_wal";

/// Group discriminator for KIP-595 traffic carried by the broker listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QuorumGroup {
    Metadata,
    DisklessWal {
        topic_id: uuid::Uuid,
        partition: PartitionIndex,
    },
}

// No `Eq`: `max_size` is a `ByteSize`, which stores `f64`. The derive was
// unused — `WalFetchRequest` is only destructured, never compared or hashed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WalFetchRequest {
    pub(crate) group: QuorumGroup,
    pub(crate) from: krabka_raft::NodeId,
    pub(crate) current_leader_epoch: i32,
    pub(crate) last_fetched_epoch: i32,
    pub(crate) fetch_offset: i64,
    pub(crate) max_size: ByteSize,
}

impl QuorumGroup {
    #[must_use]
    pub(crate) fn diskless_wal(topic_id: uuid::Uuid, partition: PartitionIndex) -> Self {
        Self::DisklessWal {
            topic_id,
            partition,
        }
    }
}

/// Metadata and WAL quorums share the Fetch topic/partition routing shape.
fn fetch_partition(group: QuorumGroup) -> (String, krabka_protocol::primitives::uuid::Uuid, i32) {
    match group {
        QuorumGroup::Metadata => ("__cluster_metadata".to_string(), KRAFT_METADATA_TOPIC_ID, 0),
        QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } => (
            String::new(),
            krabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes()),
            partition.0,
        ),
    }
}

/// Decodes enough of the KIP-595 `Fetch` body to choose the target quorum
/// group.
///
/// The fixed `__cluster_metadata` topic id identifies Kafka's metadata quorum.
/// Diskless WAL shards reuse the same KIP-595 Fetch envelope, but they use the
/// data topic id and the partition as the shard address.
#[cfg(test)]
pub(crate) fn classify_fetch(body: &[u8]) -> Option<QuorumGroup> {
    decode_fetch(body).map(|request| request.group)
}

pub(crate) fn decode_fetch(body: &[u8]) -> Option<WalFetchRequest> {
    let mut cur = body;
    let request = FetchRequest::decode(&mut cur, KIP_595_FETCH_VERSION).ok()?;
    decode_fetch_request(&request)
}

pub(crate) fn decode_fetch_request(request: &FetchRequest) -> Option<WalFetchRequest> {
    let topic = request.topics.first()?;
    let partition = topic.partitions.first()?;
    let max_bytes = request.max_bytes.min(partition.partition_max_bytes);
    let max_bytes = std::num::NonZeroU32::new(u32::try_from(max_bytes).ok()?)?;
    let group = if topic.topic_id == KRAFT_METADATA_TOPIC_ID {
        QuorumGroup::Metadata
    } else {
        if request.rack_id != WAL_FETCH_RACK_ID {
            return None;
        }
        QuorumGroup::diskless_wal(
            uuid::Uuid::from_bytes(topic.topic_id.0),
            PartitionIndex(partition.partition),
        )
    };
    Some(WalFetchRequest {
        group,
        from: krabka_raft::NodeId(u64::try_from(request.replica_state.replica_id).ok()?),
        current_leader_epoch: partition.current_leader_epoch,
        last_fetched_epoch: partition.last_fetched_epoch,
        fetch_offset: partition.fetch_offset,
        max_size: ByteSize::from_bytes(u64::from(max_bytes.get())),
    })
}

pub(crate) fn conventional_node_id(principal_name: &str) -> Option<krabka_raft::NodeId> {
    let id = principal_name
        .strip_prefix("broker-")
        .or_else(|| {
            principal_name
                .split(',')
                .find_map(|rdn| rdn.trim().strip_prefix("CN=broker-"))
        })?
        .parse()
        .ok()?;
    Some(krabka_raft::NodeId(id))
}

pub(crate) fn fetch_response(
    group: QuorumGroup,
    hwm: i64,
    log_end_offset: i64,
    log_start_offset: i64,
    records: Bytes,
    error_code: i16,
    diverging_epoch: Option<(i32, i64)>,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    use krabka_protocol::{owned::fetch_response as fetch_resp, records::RecordsPayload};

    let (topic, topic_id, partition) = fetch_partition(group);
    fetch_resp::FetchResponse {
        responses: vec![fetch_resp::FetchableTopicResponse {
            topic,
            topic_id,
            partitions: vec![fetch_resp::PartitionData {
                partition_index: partition,
                error_code,
                high_watermark: hwm,
                // This is a private, rack-discriminated Fetch protocol. WAL
                // followers need the leader LEO as well as its committed HWM,
                // so the otherwise-unused LSO field carries that frontier.
                last_stable_offset: log_end_offset,
                log_start_offset,
                // An empty record set, never a null one. This transport is
                // krabka-to-krabka, so no third-party decoder reads it today
                // and nothing is broken by the null -- but Kafka's own raft
                // fetch writes `MemoryRecords.EMPTY` here too, and a reader
                // that refuses a negative length (sarama's `getSubset` does)
                // would drop the connection on it. The client-facing encoders
                // already guarantee this; a private transport is not a reason
                // to be the one path that does not.
                records: Some(RecordsPayload::Raw(records)),
                diverging_epoch: diverging_epoch.map_or_else(
                    Default::default,
                    |(epoch, end_offset)| fetch_resp::EpochEndOffset {
                        epoch,
                        end_offset,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) fn unknown_shard_fetch_response(
    group: QuorumGroup,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    fetch_response(
        group,
        0,
        0,
        0,
        Bytes::new(),
        UNKNOWN_TOPIC_OR_PARTITION,
        None,
    )
}

pub(crate) fn encode_fetch_response_struct(
    response: &krabka_protocol::owned::fetch_response::FetchResponse,
) -> Bytes {
    let mut out = bytes::BytesMut::new();
    let _ = response.encode(&mut out, KIP_595_FETCH_VERSION);
    out.freeze()
}

pub(crate) fn fetch_request(
    group: QuorumGroup,
    from: krabka_raft::NodeId,
    current_leader_epoch: i32,
    last_fetched_epoch: i32,
    fetch_offset: i64,
    max_size: ByteSize,
) -> FetchRequest {
    use krabka_protocol::owned::fetch_request as fetch_req;

    let (topic, topic_id, partition) = fetch_partition(group);
    FetchRequest {
        rack_id: matches!(group, QuorumGroup::DisklessWal { .. })
            .then_some(WAL_FETCH_RACK_ID.to_string())
            .unwrap_or_default(),
        replica_state: fetch_req::ReplicaState {
            replica_id: i32::try_from(from.0).unwrap_or(i32::MAX),
            replica_epoch: -1,
            ..Default::default()
        },
        max_bytes: max_size.bytes_i32(),
        topics: vec![fetch_req::FetchTopic {
            topic,
            topic_id,
            partitions: vec![fetch_req::FetchPartition {
                partition,
                current_leader_epoch,
                fetch_offset,
                last_fetched_epoch,
                partition_max_bytes: max_size.bytes_i32(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) fn encode_fetch_for_group(
    group: QuorumGroup,
    from: krabka_raft::NodeId,
    fetch_epoch: i32,
    fetch_offset: i64,
) -> Bytes {
    let request = fetch_request(
        group,
        from,
        fetch_epoch,
        fetch_epoch,
        fetch_offset,
        ByteSize::from_bytes(u64::MAX),
    );
    let mut out = bytes::BytesMut::new();
    let _ = request.encode(&mut out, KIP_595_FETCH_VERSION);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use krabka_raft::NodeId;

    use super::*;

    #[test]
    fn classify_fetch_distinguishes_metadata_quorum() {
        let body = encode_fetch_for_group(QuorumGroup::Metadata, NodeId(2), 7, 11);

        assert2::assert!((classify_fetch(&body)) == (Some(QuorumGroup::Metadata)));
    }

    #[test]
    fn classify_fetch_extracts_diskless_wal_shard() {
        let topic_id = uuid::Uuid::from_u128(42);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, PartitionIndex(3)),
            NodeId(2),
            7,
            11,
        );

        assert2::assert!(
            (classify_fetch(&body))
                == (Some(QuorumGroup::DisklessWal {
                    topic_id,
                    partition: PartitionIndex(3),
                }))
        );
    }

    #[test]
    fn classify_fetch_does_not_capture_normal_follower_fetch() {
        let topic_id = uuid::Uuid::from_u128(42);
        let mut cur = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, PartitionIndex(3)),
            NodeId(2),
            7,
            11,
        );
        let mut request = FetchRequest::decode(&mut cur, KIP_595_FETCH_VERSION).unwrap();
        request.rack_id.clear();
        let mut body = bytes::BytesMut::new();
        request.encode(&mut body, KIP_595_FETCH_VERSION).unwrap();

        assert2::assert!((classify_fetch(&body)) == (None));
    }

    #[test]
    fn fetch_request_applies_both_byte_caps() {
        let group = QuorumGroup::diskless_wal(uuid::Uuid::from_u128(42), PartitionIndex(3));
        let max_size = ByteSize::from_bytes(4_096);
        let request = fetch_request(group, NodeId(2), 7, 6, 11, max_size);

        assert2::check!(request.max_bytes == 4_096);
        assert2::check!(request.topics[0].partitions[0].partition_max_bytes == 4_096);
        let decoded = decode_fetch_request(&request).unwrap();
        assert2::check!(decoded.max_size == max_size);
        assert2::check!(decoded.current_leader_epoch == 7);
        assert2::check!(decoded.last_fetched_epoch == 6);
    }

    #[test]
    fn decode_fetch_request_requires_positive_effective_byte_cap() {
        let group = QuorumGroup::diskless_wal(uuid::Uuid::from_u128(42), PartitionIndex(3));
        let mut request = fetch_request(group, NodeId(2), 7, 6, 11, ByteSize::from_bytes(4_096));

        request.max_bytes = 0;
        assert2::check!(decode_fetch_request(&request).is_none());
        request.max_bytes = 2_048;
        request.topics[0].partitions[0].partition_max_bytes = 1_024;
        assert2::check!(
            decode_fetch_request(&request).unwrap().max_size == ByteSize::from_bytes(1_024)
        );
        request.topics[0].partitions[0].partition_max_bytes = 0;
        assert2::check!(decode_fetch_request(&request).is_none());
    }
}
