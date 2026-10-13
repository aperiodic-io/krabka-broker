//! The streams member metadata record at key version 19.
//!
//! The value holds a member's static identity: its instance and rack ids, its
//! client id and host, its process id, its client tags, and the rebalance
//! timeout and topology epoch it joined with. [`StreamsEndpoint`] is the
//! optional host and port a member advertises for interactive queries.
//!
//! # Layout
//!
//! From `StreamsGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`,
//! which declares `"flexibleVersions": "0+"`. In field order: `InstanceId`
//! (nullable string), `RackId` (nullable string), `ClientId` (string),
//! `ClientHost` (string), `RebalanceTimeoutMs` (int32), `TopologyEpoch`
//! (int32), `ProcessId` (string), `UserEndpoint` (nullable `Endpoint{Host
//! string, Port uint16}`) and `ClientTags` (`[]KeyValue{Key string, Value
//! string}`).
//!
//! `UserEndpoint` is a nullable struct that is not tagged, so it is on the wire
//! as one `int8`: -1 for null, or 1 followed by the struct and the struct's own
//! tagged-field count. The port is a `uint16`, two bytes, not four.

use {
    super::codec::{decode_key_value_list, encode_key_value_list},
    crate::coordinator::unified::persistence::{
        flex::{
            get_compact_nullable_string, get_compact_string, get_i8, get_member_client, get_u16,
            put_compact_nullable_string, put_compact_string, put_empty_tagged_fields,
            put_member_client, skip_tagged_fields, value_codec,
        },
        get_i32,
    },
    bytes::BufMut,
};

/// A member's advertised host and port endpoint, for interactive-query
/// routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsEndpoint {
    pub host: String,
    /// Kafka's `uint16`, so the whole unsigned range fits and nothing wider
    /// does.
    pub port: u16,
}

/// Key v19 value: a streams group member's static metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,
    pub topology_epoch: i32,
}

value_codec! {
    StreamsGroupMemberMetadataValue("StreamsGroupMemberMetadataValue"),
    encode(&self) -> buf {
        put_compact_nullable_string(buf, self.instance_id.as_deref());
        put_member_client(buf, self.rack_id.as_deref(), &self.client_id, &self.client_host);
        buf.put_i32(self.rebalance_timeout_ms);
        buf.put_i32(self.topology_epoch);
        put_compact_string(buf, &self.process_id);
        match &self.user_endpoint {
            Some(ep) => {
                buf.put_i8(1);
                put_compact_string(buf, &ep.host);
                buf.put_u16(ep.port);
                put_empty_tagged_fields(buf);
            }
            None => buf.put_i8(-1),
        }
        encode_key_value_list(buf, &self.client_tags);
    }
    decode(buf) {
        let instance_id = get_compact_nullable_string(buf)?;
        let (rack_id, client_id, client_host) = get_member_client(buf)?;
        let rebalance_timeout_ms = get_i32(buf)?;
        let topology_epoch = get_i32(buf)?;
        let process_id = get_compact_string(buf)?;
        let user_endpoint = if get_i8(buf)? < 0 {
            None
        } else {
            let host = get_compact_string(buf)?;
            let port = get_u16(buf)?;
            skip_tagged_fields(buf)?;
            Some(StreamsEndpoint { host, port })
        };
        let client_tags = decode_key_value_list(buf)?;
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            process_id,
            user_endpoint,
            client_tags,
            rebalance_timeout_ms,
            topology_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            streams::persistence::{
                KEY_STREAMS_MEMBER_METADATA, StreamsGroupKey, encode_member_metadata_key,
                parse_streams_key,
            },
            test_support::{peek_version, wire_bytes},
        },
        assert2::assert,
    };

    fn sample() -> StreamsGroupMemberMetadataValue {
        StreamsGroupMemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "p-uuid".into(),
            user_endpoint: Some(StreamsEndpoint {
                host: "host-a".into(),
                port: 8080,
            }),
            client_tags: vec![("zone".into(), "a".into()), ("tier".into(), "hot".into())],
            rebalance_timeout_ms: 60_000,
            topology_epoch: 3,
        }
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            process_id: "p".into(),
            user_endpoint: Some(StreamsEndpoint {
                host: "e".into(),
                port: 8080,
            }),
            client_tags: vec![("k".into(), "v".into())],
            rebalance_timeout_ms: 1,
            topology_epoch: 2,
        };
        let want = wire_bytes(&[
            "0000", "0000",     // InstanceId, RackId null
            "02630268", // ClientId, ClientHost
            "00000001", // RebalanceTimeoutMs
            "00000002", // TopologyEpoch
            "0270",     // ProcessId
            "01",       // UserEndpoint present
            "0265",     // Host
            "1f90",     // Port, uint16
            "00",       // Endpoint tagged fields
            "02",       // one ClientTags entry
            "026b0276", "00", // KeyValue tagged fields
            "00", // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn null_user_endpoint_is_one_negative_byte() {
        let v = StreamsGroupMemberMetadataValue {
            user_endpoint: None,
            client_tags: vec![],
            ..sample()
        };
        let bytes = v.encode();
        // ... ProcessId "p-uuid", then -1, then the empty ClientTags and the
        // message's tagged-field count.
        assert!(&bytes[bytes.len() - 3..] == b"\xff\x01\x00");
    }

    #[test]
    fn member_metadata_round_trip() {
        let kb = encode_member_metadata_key("g1", "m1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_MEMBER_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::MemberMetadata {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let v = sample();
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_null_optionals_round_trip() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            user_endpoint: None,
            client_tags: vec![],
            rebalance_timeout_ms: 45_000,
            topology_epoch: 0,
            ..sample()
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn a_port_above_the_i16_range_round_trips() {
        let v = StreamsGroupMemberMetadataValue {
            user_endpoint: Some(StreamsEndpoint {
                host: "h".into(),
                port: 60_000,
            }),
            ..sample()
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let full = sample().encode();
        assert!(StreamsGroupMemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
