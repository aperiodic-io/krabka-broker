//! The KIP-932 share-group member metadata record at key version 10.
//!
//! [`ShareGroupMemberMetadataValue`] holds what a share consumer told the
//! coordinator about itself: its optional rack, its client identity, and the
//! topics it subscribed to by name. Share groups have no regex subscription, no
//! server assignor, and no rebalance timeout, so those consumer-only fields are
//! absent.
//!
//! # Layout
//!
//! From `ShareGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`, which
//! declares `"flexibleVersions": "0+"`: `RackId` (nullable string), `ClientId`
//! (string), `ClientHost` (string) and `SubscribedTopicNames` (`[]string`).
//! Every string is compact, the array is compact, and the message ends with its
//! tagged-field count.

use crate::coordinator::unified::persistence::flex::{
    get_member_client, get_string_array, put_member_client, put_string_array, value_codec,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberMetadataValue {
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
}

value_codec! {
    ShareGroupMemberMetadataValue("ShareGroupMemberMetadataValue"),
    encode(&self) -> buf {
        put_member_client(buf, self.rack_id.as_deref(), &self.client_id, &self.client_host);
        put_string_array(buf, &self.subscribed_topic_names);
    }
    decode(buf) {
        let (rack_id, client_id, client_host) = get_member_client(buf)?;
        let subscribed_topic_names = get_string_array(buf)?;
        Ok(Self {
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_MEMBER_METADATA, ShareGroupKey,
    };

    #[test]
    fn member_metadata_round_trip() {
        let key = ShareGroupKey::MemberMetadata {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_MEMBER_METADATA),
        );

        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("r".into()),
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec!["a".into()],
        };
        assert!(&v.encode()[..] == b"\x00\x00\x02r\x02c\x02h\x02\x02a\x00");
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec![],
        };
        let full = v.encode();
        assert!(ShareGroupMemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }

    #[test]
    fn member_metadata_null_rack_round_trip() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec![],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
