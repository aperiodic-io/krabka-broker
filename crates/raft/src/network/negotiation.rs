//! The version of one outbound KIP-595 request, negotiated against the peer's
//! advertised range, and the conversion of the engine's body to and from it.
//!
//! Kafka's `KafkaNetworkChannel` sends each raft request through
//! `NetworkClient`, which picks the highest version both ends support. The
//! engine encodes each body at the version [`api_version_for`] names, the
//! newest it speaks. A peer that advertises a lower maximum gets the body
//! re-encoded at that maximum, and its answer is re-encoded back at the
//! engine's version before the engine decodes it. The generated codecs carry
//! each field only at the versions that define it, so the conversion drops
//! the fields the lower version does not have, as Kafka's own encoder does.

use bytes::{Bytes, BytesMut};
use krabka_ids::ApiKey;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        begin_quorum_epoch_request::BeginQuorumEpochRequest,
        begin_quorum_epoch_response::BeginQuorumEpochResponse,
        end_quorum_epoch_request::EndQuorumEpochRequest,
        end_quorum_epoch_response::EndQuorumEpochResponse, fetch_request::FetchRequest,
        fetch_response::FetchResponse, fetch_snapshot_request::FetchSnapshotRequest,
        fetch_snapshot_response::FetchSnapshotResponse, vote_request::VoteRequest,
        vote_response::VoteResponse,
    },
};

use crate::{kraft::transport::PeerApi, network::addressing::api_version_for};

/// The version to send `key` at: the engine's version, or the peer's maximum
/// when the peer advertises a range that ends below it. A peer that
/// advertises nothing for `key`, or a range above the engine's version, gets
/// the engine's version.
pub(crate) fn negotiated_version(key: i16, peer_range: Option<(i16, i16)>) -> i16 {
    let ours = api_version_for(ApiKey(key)).get();
    match peer_range {
        Some((min, max)) if max < ours && min <= max => max,
        _ => ours,
    }
}

/// Re-encodes a request body for `key` from version `from` to version `to`.
/// `None` when the body does not decode at `from` or does not encode at `to`.
pub(crate) fn convert_request(key: i16, body: &[u8], from: i16, to: i16) -> Option<Bytes> {
    match PeerApi::from_api_key(key)? {
        PeerApi::Fetch => convert::<FetchRequest>(body, from, to),
        PeerApi::Vote => convert::<VoteRequest>(body, from, to),
        PeerApi::BeginQuorumEpoch => convert::<BeginQuorumEpochRequest>(body, from, to),
        PeerApi::EndQuorumEpoch => convert_end_quorum_epoch(body, from, to),
        PeerApi::FetchSnapshot => convert::<FetchSnapshotRequest>(body, from, to),
    }
}

/// `EndQuorumEpoch` names the successors in `PreferredSuccessors` at v0 and
/// in `PreferredCandidates` from v1. Kafka's
/// `RaftUtil.singletonEndQuorumEpochRequest` fills both, so a body decoded at
/// v1 gets its successor ids back before it is encoded at v0.
fn convert_end_quorum_epoch(body: &[u8], from: i16, to: i16) -> Option<Bytes> {
    let mut cur = body;
    let mut message = EndQuorumEpochRequest::decode(&mut cur, from).ok()?;
    for partition in message
        .topics
        .iter_mut()
        .flat_map(|topic| &mut topic.partitions)
    {
        if partition.preferred_successors.is_empty() {
            partition.preferred_successors = partition
                .preferred_candidates
                .iter()
                .map(|candidate| candidate.candidate_id)
                .collect();
        }
    }
    let mut out = BytesMut::new();
    message.encode(&mut out, to).ok()?;
    Some(out.freeze())
}

/// Re-encodes a response body for `key` from version `from` to version `to`.
/// `None` when the body does not decode at `from` or does not encode at `to`.
pub(crate) fn convert_response(key: i16, body: &[u8], from: i16, to: i16) -> Option<Bytes> {
    match PeerApi::from_api_key(key)? {
        PeerApi::Fetch => convert::<FetchResponse>(body, from, to),
        PeerApi::Vote => convert::<VoteResponse>(body, from, to),
        PeerApi::BeginQuorumEpoch => convert::<BeginQuorumEpochResponse>(body, from, to),
        PeerApi::EndQuorumEpoch => convert::<EndQuorumEpochResponse>(body, from, to),
        PeerApi::FetchSnapshot => convert::<FetchSnapshotResponse>(body, from, to),
    }
}

fn convert<T>(body: &[u8], from: i16, to: i16) -> Option<Bytes>
where
    T: for<'de> Decode<'de> + Encode,
{
    let mut cur = body;
    let message = T::decode(&mut cur, from).ok()?;
    let mut out = BytesMut::new();
    message.encode(&mut out, to).ok()?;
    Some(out.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::fetch_request::{self as fetch_req};

    use super::*;
    use crate::kraft::transport::{
        api_key,
        wire::{FETCH_VERSION, METADATA_TOPIC_ID, VOTE_VERSION},
    };

    /// A peer whose range ends below the engine's version gets its maximum;
    /// any other peer gets the engine's version.
    #[test]
    fn negotiation_takes_the_peer_maximum_only_below_the_engine_version() {
        let cases = [
            ("no range", api_key::FETCH, None, FETCH_VERSION),
            ("a 4.3 peer", api_key::FETCH, Some((4, 18)), FETCH_VERSION),
            ("a 4.0 peer", api_key::FETCH, Some((4, 17)), 17),
            ("a newer peer", api_key::FETCH, Some((4, 19)), FETCH_VERSION),
            ("vote", api_key::VOTE, Some((0, 2)), VOTE_VERSION),
            ("an old vote peer", api_key::VOTE, Some((0, 1)), 1),
        ];
        for (name, key, range, expected) in cases {
            check!(negotiated_version(key, range) == expected, "{name}");
        }
    }

    /// A v18 Fetch re-encoded at v17 loses only `HighWatermark`, which v17
    /// does not carry, and a response converts back unchanged.
    #[test]
    fn a_fetch_converts_to_a_lower_version_and_back() {
        let request = FetchRequest {
            max_wait_ms: 500,
            replica_state: fetch_req::ReplicaState {
                replica_id: 2,
                ..Default::default()
            },
            topics: vec![fetch_req::FetchTopic {
                topic_id: METADATA_TOPIC_ID,
                partitions: vec![fetch_req::FetchPartition {
                    fetch_offset: 7,
                    high_watermark: 5,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, 18).expect("encode v18");
        let converted = convert_request(api_key::FETCH, &body, 18, 17).expect("convert");
        let decoded = FetchRequest::decode(&mut &converted[..], 17).expect("decode v17");
        let mut expected = request.clone();
        expected.topics[0].partitions[0].high_watermark = i64::MAX;
        check!(decoded == expected);

        let response = FetchResponse {
            error_code: 0,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        response.encode(&mut body, 17).expect("encode v17");
        let back = convert_response(api_key::FETCH, &body, 17, 18).expect("convert back");
        check!(FetchResponse::decode(&mut &back[..], 18).ok() == Some(response));
    }
}
