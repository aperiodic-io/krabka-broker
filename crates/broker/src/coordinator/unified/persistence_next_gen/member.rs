//! The KIP-848 member metadata record at key version 5.
//!
//! [`MemberMetadataValue`] holds what a next-gen consumer told the coordinator
//! about itself: its client identity, its subscription by name and by regex,
//! and the server assignor it asked for. An upgraded group also stores the
//! member's classic sub-state in [`ClassicMemberMetadata`] so that a downgrade
//! can restore it.
//!
//! # Layout
//!
//! From `ConsumerGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`,
//! which declares `"flexibleVersions": "0+"`. In field order: `InstanceId`
//! (nullable string), `RackId` (nullable string), `ClientId` (string),
//! `ClientHost` (string), `SubscribedTopicNames` (`[]string`),
//! `SubscribedTopicRegex` (nullable string), `RebalanceTimeoutMs` (int32),
//! `ServerAssignor` (nullable string), and then the tagged
//! `ClassicMemberMetadata` (tag 0, nullable, default null) whose own fields are
//! `SessionTimeoutMs` (int32) and `SupportedProtocols`
//! (`[]ClassicProtocol{Name string, Metadata bytes}`).
//!
//! Every string is compact, every array is compact, and the message and the
//! nested structs each end with a tagged-field count.
//!
//! The record carries Kafka's fields and nothing else. What a hosted classic
//! member last synced is not stored here: it is the member's k8 current
//! assignment, translated back into a `ConsumerProtocolAssignment` blob when
//! `apply_seed` hydrates the group. See
//! `crate::coordinator::unified::actor::seed`.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array, get_compact_bytes, get_compact_nullable_string, get_compact_string,
            get_member_client, get_string_array, get_value_version, put_compact_array,
            put_compact_bytes, put_compact_nullable_string, put_compact_string,
            put_empty_tagged_fields, put_member_client, put_string_array, put_tagged_fields,
            read_tagged, skip_tagged_fields,
        },
        get_i32,
    },
    error::BrokerError,
};

/// Kafka's tag for the `ClassicMemberMetadata` field of
/// `ConsumerGroupMemberMetadataValue`.
const TAG_CLASSIC_MEMBER_METADATA: u32 = 0;

/// Classic-protocol sub-state for a member hosted inside an upgraded consumer
/// group (KIP-848 migration). It mirrors Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`. It lets a
/// downgrade restore the classic member losslessly after a coordinator
/// failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberMetadata {
    pub session_timeout_ms: i32,
    pub supported_protocols: Vec<(String, Bytes)>,
}

impl ClassicMemberMetadata {
    /// Encodes the payload of Kafka's `ClassicMemberMetadata` tagged field:
    /// the struct's own fields plus its own tagged-field trailer.
    fn encode_tag_payload(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32(self.session_timeout_ms);
        put_compact_array(
            &mut buf,
            self.supported_protocols.iter(),
            |buf, (name, meta)| {
                put_compact_string(buf, name);
                put_compact_bytes(buf, meta);
                put_empty_tagged_fields(buf);
            },
        );
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    fn decode_tag_payload(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let session_timeout_ms = get_i32(buf)?;
        let supported_protocols = get_compact_array(buf, |buf| {
            let name = get_compact_string(buf)?;
            let meta = get_compact_bytes(buf)?;
            skip_tagged_fields(buf)?;
            Ok((name, meta))
        })?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            session_timeout_ms,
            supported_protocols,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. `None` = exact-name
    /// subscription only. The reconciler unions regex matches with
    /// `subscribed_topic_names` against the current metadata image.
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout_ms: i32,
    /// `Some` if and only if this is a hosted classic member. `None` for a
    /// native consumer-protocol member.
    pub classic: Option<ClassicMemberMetadata>,
}

impl MemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_nullable_string(&mut buf, self.instance_id.as_deref());
        put_member_client(
            &mut buf,
            self.rack_id.as_deref(),
            &self.client_id,
            &self.client_host,
        );
        put_string_array(&mut buf, &self.subscribed_topic_names);
        put_compact_nullable_string(&mut buf, self.subscribed_topic_regex.as_deref());
        buf.put_i32(self.rebalance_timeout_ms);
        put_compact_nullable_string(&mut buf, self.server_assignor.as_deref());
        let mut tags = Vec::new();
        if let Some(c) = &self.classic {
            tags.push((TAG_CLASSIC_MEMBER_METADATA, c.encode_tag_payload()));
        }
        put_tagged_fields(&mut buf, tags);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(
            &mut buf,
            0,
            "unknown ConsumerGroupMemberMetadataValue version",
        )?;
        let instance_id = get_compact_nullable_string(&mut buf)?;
        let (rack_id, client_id, client_host) = get_member_client(&mut buf)?;
        let subscribed_topic_names = get_string_array(&mut buf)?;
        let subscribed_topic_regex = get_compact_nullable_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let server_assignor = get_compact_nullable_string(&mut buf)?;

        let mut classic: Option<ClassicMemberMetadata> = None;
        let mut tag_error: Option<BrokerError> = None;
        read_tagged(&mut buf, |tag, payload| match tag {
            TAG_CLASSIC_MEMBER_METADATA => match ClassicMemberMetadata::decode_tag_payload(payload)
            {
                Ok(c) => {
                    classic = Some(c);
                    Ok(true)
                }
                Err(e) => {
                    tag_error = Some(e);
                    Err(ProtocolError::InvalidValue("bad ClassicMemberMetadata"))
                }
            },
            _ => Ok(false),
        })?;
        if let Some(e) = tag_error {
            return Err(e);
        }
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            rebalance_timeout_ms,
            classic,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::test_support::wire_bytes;

    fn native() -> MemberMetadataValue {
        MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        }
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let want = wire_bytes(&[
            "0000",                   // value version 0
            "036931",                 // InstanceId, compact string
            "00",                     // RackId, compact null
            "036331",                 // ClientId
            "0b2f3132372e302e302e31", // ClientHost
            "0302610262",             // SubscribedTopicNames
            "00",                     // SubscribedTopicRegex, null
            "0000ea60",               // RebalanceTimeoutMs
            "08756e69666f726d",       // ServerAssignor
            "00",                     // no tagged fields
        ]);
        assert!(&native().encode()[..] == &want[..]);
    }

    #[test]
    fn member_metadata_roundtrip() {
        let v = native();
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_with_regex_roundtrip() {
        // KIP-848 v1+: `subscribed_topic_regex` survives encode/decode
        // so bootstrap replay can hydrate the regex subscription
        // without waiting for a heartbeat.
        let v = MemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            subscribed_topic_names: vec!["audit".into()],
            subscribed_topic_regex: Some("^orders-.*".into()),
            ..native()
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn classic_sub_state_bytes_match_kafka_schema() {
        let v = MemberMetadataValue {
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 2,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"m"))],
            }),
            client_id: "c".into(),
            client_host: "h".into(),
            rebalance_timeout_ms: 1,
            ..Default::default()
        };
        let want = wire_bytes(&[
            "0000",         // version
            "0000",         // InstanceId, RackId null
            "02630268",     // ClientId, ClientHost
            "01",           // empty SubscribedTopicNames
            "00",           // SubscribedTopicRegex null
            "00000001",     // RebalanceTimeoutMs
            "00",           // ServerAssignor null
            "01000f",       // one tagged field: tag 0, payload length 15
            "00000002",     // SessionTimeoutMs
            "02",           // one supported protocol
            "0672616e6765", // Name "range"
            "026d",         // Metadata "m", compact bytes
            "00",           // ClassicProtocol tagged fields
            "00",           // ClassicMemberMetadata tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn member_metadata_round_trips_classic_block() {
        let v = MemberMetadataValue {
            instance_id: Some("inst-a".into()),
            subscribed_topic_names: vec!["t1".into(), "t2".into()],
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 30_000,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
            }),
            ..native()
        };
        let decoded = MemberMetadataValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);

        let mut plain = v.clone();
        plain.classic = None;
        assert!(MemberMetadataValue::decode(&plain.encode()).unwrap() == plain);
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let full = native().encode();
        assert!(MemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
