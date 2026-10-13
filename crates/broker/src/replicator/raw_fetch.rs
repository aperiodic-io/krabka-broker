//! Fetch response decoding that leaves record batches in their wire form.

use {
    bytes::{Buf, BufMut},
    krabka_protocol::{
        Decode, Encode, ProtocolError, ProtocolRequest,
        owned::{
            fetch_request::FetchRequest,
            fetch_response::{
                AbortedTransaction, EpochEndOffset, FetchResponse, FetchableTopicResponse,
                LeaderIdAndEpoch, PartitionData, SnapshotId,
            },
        },
        primitives::{
            array::{get_array_len, get_nullable_array_len},
            fixed::{get_i16, get_i32, get_i64},
            string_bytes::{
                get_compact_nullable_bytes_owned, get_compact_string_owned,
                get_nullable_bytes_owned, get_string_owned,
            },
            uuid::get_uuid,
        },
        records::RecordsPayload,
        tagged_fields::read_tagged_fields,
    },
};

pub(super) struct RawFetchRequest(pub(super) FetchRequest);

impl Encode for RawFetchRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize { self.0.encoded_len(version) }
}

impl ProtocolRequest for RawFetchRequest {
    type Response = RawFetchResponse;

    const API_KEY: i16 = <FetchRequest as ProtocolRequest>::API_KEY;
    const FLEXIBLE_MIN: i16 = <FetchRequest as ProtocolRequest>::FLEXIBLE_MIN;
    const LATEST_STABLE_VERSION: i16 = <FetchRequest as ProtocolRequest>::LATEST_STABLE_VERSION;
    const MAX_VERSION: i16 = <FetchRequest as ProtocolRequest>::MAX_VERSION;
    const MIN_VERSION: i16 = <FetchRequest as ProtocolRequest>::MIN_VERSION;
}

pub(super) struct RawFetchResponse(pub(super) FetchResponse);

impl Decode<'_> for RawFetchResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(<FetchRequest as ProtocolRequest>::MIN_VERSION
            ..=<FetchRequest as ProtocolRequest>::MAX_VERSION)
            .contains(&version)
        {
            return Err(ProtocolError::UnsupportedVersion {
                api_key: <FetchRequest as ProtocolRequest>::API_KEY,
                version,
            });
        }
        let flex = version >= <FetchRequest as ProtocolRequest>::FLEXIBLE_MIN;
        let mut out = FetchResponse {
            throttle_time_ms: get_i32(buf)?,
            ..FetchResponse::default()
        };
        if version >= 7 {
            out.error_code = get_i16(buf)?;
            out.session_id = get_i32(buf)?;
        }
        let count = get_array_len(buf, flex)?;
        out.responses.reserve(count);
        for _ in 0..count {
            out.responses.push(decode_topic(buf, version, flex)?);
        }
        if flex {
            out.unknown_tagged_fields = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        }
        Ok(Self(out))
    }
}

fn decode_topic<B: Buf>(
    buf: &mut B,
    version: i16,
    flex: bool,
) -> Result<FetchableTopicResponse, ProtocolError> {
    let mut out = FetchableTopicResponse::default();
    if version <= 12 {
        out.topic = if flex {
            get_compact_string_owned(buf)?
        } else {
            get_string_owned(buf)?
        };
    } else {
        out.topic_id = get_uuid(buf)?;
    }
    let count = get_array_len(buf, flex)?;
    out.partitions.reserve(count);
    for _ in 0..count {
        out.partitions.push(decode_partition(buf, version, flex)?);
    }
    if flex {
        out.unknown_tagged_fields = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
    }
    Ok(out)
}

fn decode_partition<B: Buf>(
    buf: &mut B,
    version: i16,
    flex: bool,
) -> Result<PartitionData, ProtocolError> {
    let mut out = PartitionData {
        partition_index: get_i32(buf)?,
        error_code: get_i16(buf)?,
        high_watermark: get_i64(buf)?,
        ..PartitionData::default()
    };
    out.last_stable_offset = get_i64(buf)?;
    if version >= 5 {
        out.log_start_offset = get_i64(buf)?;
    }
    out.aborted_transactions = match get_nullable_array_len(buf, flex)? {
        None => None,
        Some(count) => {
            let mut transactions = Vec::with_capacity(count);
            for _ in 0..count {
                transactions.push(AbortedTransaction::decode(buf, version)?);
            }
            Some(transactions)
        }
    };
    if version >= 11 {
        out.preferred_read_replica = get_i32(buf)?;
    }
    let records = if flex {
        get_compact_nullable_bytes_owned(buf)?
    } else {
        get_nullable_bytes_owned(buf)?
    };
    out.records = records.map(RecordsPayload::Raw);
    if flex {
        let mut diverging_epoch = None;
        let mut current_leader = None;
        let mut snapshot_id = None;
        out.unknown_tagged_fields = read_tagged_fields(buf, |tag, payload| match tag {
            0 => {
                diverging_epoch = Some(EpochEndOffset::decode(payload, version)?);
                Ok(true)
            }
            1 => {
                current_leader = Some(LeaderIdAndEpoch::decode(payload, version)?);
                Ok(true)
            }
            2 => {
                snapshot_id = Some(SnapshotId::decode(payload, version)?);
                Ok(true)
            }
            _ => Ok(false),
        })?;
        if let Some(value) = diverging_epoch {
            out.diverging_epoch = value;
        }
        if let Some(value) = current_leader {
            out.current_leader = value;
        }
        if let Some(value) = snapshot_id {
            out.snapshot_id = value;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use {
        super::{RawFetchRequest, RawFetchResponse},
        assert2::assert,
        bytes::{Bytes, BytesMut},
        krabka_protocol::{
            Decode as _, Encode as _, ProtocolError, UnknownTaggedField, UnknownTaggedFields,
            owned::{
                fetch_request::FetchRequest,
                fetch_response::{
                    AbortedTransaction, EpochEndOffset, FetchResponse, FetchableTopicResponse,
                    LeaderIdAndEpoch, PartitionData, SnapshotId,
                },
            },
            primitives::uuid::Uuid,
            records::RecordsPayload,
        },
    };

    #[test]
    fn keeps_records_encoded() {
        let raw = Bytes::from_static(b"wire record bytes");
        for version in [4, 5, 7, 11, 12, 13, 18] {
            let response = FetchResponse {
                throttle_time_ms: 3,
                error_code: 4,
                session_id: 5,
                responses: vec![FetchableTopicResponse {
                    topic: "t".into(),
                    topic_id: Uuid([7; 16]),
                    partitions: vec![PartitionData {
                        partition_index: 2,
                        high_watermark: 11,
                        last_stable_offset: 10,
                        log_start_offset: 1,
                        aborted_transactions: Some(vec![AbortedTransaction {
                            producer_id: 8,
                            first_offset: 9,
                            ..AbortedTransaction::default()
                        }]),
                        preferred_read_replica: 6,
                        records: Some(RecordsPayload::Raw(raw.clone())),
                        diverging_epoch: EpochEndOffset {
                            epoch: 7,
                            end_offset: 8,
                            ..EpochEndOffset::default()
                        },
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 4,
                            leader_epoch: 9,
                            ..LeaderIdAndEpoch::default()
                        },
                        snapshot_id: SnapshotId {
                            end_offset: 12,
                            epoch: 10,
                            ..SnapshotId::default()
                        },
                        unknown_tagged_fields: UnknownTaggedFields(vec![UnknownTaggedField {
                            tag: 9,
                            bytes: Bytes::from_static(b"unknown"),
                        }]),
                        ..PartitionData::default()
                    }],
                    ..FetchableTopicResponse::default()
                }],
                ..FetchResponse::default()
            };
            let mut wire = BytesMut::new();
            response.encode(&mut wire, version).unwrap();
            let decoded = RawFetchResponse::decode(&mut wire.freeze(), version)
                .unwrap()
                .0;
            let partition = &decoded.responses[0].partitions[0];
            assert!(partition.records == Some(RecordsPayload::Raw(raw.clone())));
            if version >= 12 {
                assert!(partition.current_leader.leader_epoch == 9);
                assert!(partition.diverging_epoch.end_offset == 8);
                assert!(partition.snapshot_id.end_offset == 12);
                assert!(partition.unknown_tagged_fields.0[0].tag == 9);
            }
            assert!(partition.aborted_transactions.as_ref().unwrap()[0].producer_id == 8);
        }
    }

    #[test]
    fn request_encoding_delegates_and_response_rejects_unknown_versions() {
        let request = FetchRequest::default();
        let wrapped = RawFetchRequest(request.clone());
        let mut expected = BytesMut::new();
        request.encode(&mut expected, 18).unwrap();
        let mut actual = BytesMut::new();
        wrapped.encode(&mut actual, 18).unwrap();
        assert!(actual == expected);
        assert!(wrapped.encoded_len(18) == expected.len());

        assert!(matches!(
            RawFetchResponse::decode(&mut Bytes::new(), -1),
            Err(ProtocolError::UnsupportedVersion { version: -1, .. })
        ));
        assert!(matches!(
            RawFetchResponse::decode(&mut Bytes::new(), 19),
            Err(ProtocolError::UnsupportedVersion { version: 19, .. })
        ));
    }
}
