//! KIP-932 share-state record codecs for the `__share_group_state` internal
//! topic.
//!
//! The layouts are Kafka's `ShareSnapshotKey`, `ShareSnapshotValue`,
//! `ShareUpdateKey` and `ShareUpdateValue` schemas under
//! `share-coordinator/src/main/resources/common/message/`, framed as Kafka's
//! `CoordinatorRecordSerde` frames them:
//!
//! - A key is the `i16` record type (`0` for `ShareSnapshot`, a full
//!   per-partition state image, and `1` for `ShareUpdate`, a delta), then the
//!   non-flexible version 0 key: `group_id` (`i16`-length string), `topic_id`
//!   (16 raw bytes), and `partition` (`i32`).
//! - A value is the `i16` version `0`, then the flexible version 0 message: a
//!   compact batch array, a tagged-field trailer after every batch and after
//!   the message, and `DeliveryCompleteCount` as tag 0 with default `-1`.
//!
//! krabka-protocol generates no coordinator-record schemas, so these codecs
//! are written by hand on the `flex` leaf codecs that the `__consumer_offsets`
//! records use.
//!
//! These keys are distinct from the `__consumer_offsets` share-group keys
//! (versions 9–14). This is a different topic with its own discriminator
//! space.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_log::Offset;
use krabka_protocol::ProtocolError;
use uuid::Uuid;

use crate::{
    coordinator::unified::persistence::{flex, get_i16, get_i32, get_i64, get_string, put_string},
    error::BrokerError,
};

pub const KEY_SHARE_SNAPSHOT: i16 = 0;
pub const KEY_SHARE_UPDATE: i16 = 1;

/// The default of the tagged `DeliveryCompleteCount` field: the count is not
/// known. Kafka writes the tag only when the value differs from it.
pub const UNKNOWN_DELIVERY_COMPLETE_COUNT: i32 = -1;

/// Tag of `DeliveryCompleteCount` in both value schemas.
const TAG_DELIVERY_COMPLETE_COUNT: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateKey {
    pub record_type: i16,
    pub group_id: String,
    pub topic_id: Uuid,
    pub partition: i32,
}

/// Encodes a [`ShareStateKey`].
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the group id is longer than 32767
/// bytes, which a non-flexible key string cannot carry.
pub fn encode_state_key(k: &ShareStateKey) -> Result<Bytes, BrokerError> {
    let mut b = BytesMut::new();
    b.put_i16(k.record_type);
    put_string(&mut b, &k.group_id)?;
    b.put_slice(k.topic_id.as_bytes());
    b.put_i32(k.partition);
    Ok(b.freeze())
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_state_key(mut buf: &[u8]) -> Result<ShareStateKey, BrokerError> {
    let record_type = get_i16(&mut buf)?;
    if record_type != KEY_SHARE_SNAPSHOT && record_type != KEY_SHARE_UPDATE {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unknown share-state key type",
        )));
    }
    let group_id = get_string(&mut buf)?;
    if buf.len() < 16 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "short share-state key",
        )));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&buf[..16]);
    buf.advance(16);
    let topic_id = Uuid::from_bytes(id);
    let partition = get_i32(&mut buf)?;
    Ok(ShareStateKey {
        record_type,
        group_id,
        topic_id,
        partition,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBatch {
    pub first_offset: Offset,
    pub last_offset: Offset,
    pub delivery_state: i8,
    pub delivery_count: i16,
}

/// Share reads and writes carry the same batch fields in distinct wire types.
/// Conversions keep protocol defaults for their tagged fields.
macro_rules! wire_state_batch {
    ($wire:ty; from [$($from_ref:tt)*]; to [$($to_ref:tt)*]) => {
        impl From<$($from_ref)* $wire> for StateBatch {
            fn from(batch: $($from_ref)* $wire) -> Self {
                Self {
                    first_offset: Offset(batch.first_offset),
                    last_offset: Offset(batch.last_offset),
                    delivery_state: batch.delivery_state,
                    delivery_count: batch.delivery_count,
                }
            }
        }
        impl From<$($to_ref)* StateBatch> for $wire {
            fn from(batch: $($to_ref)* StateBatch) -> Self {
                Self {
                    first_offset: batch.first_offset.0,
                    last_offset: batch.last_offset.0,
                    delivery_state: batch.delivery_state,
                    delivery_count: batch.delivery_count,
                    ..Self::default()
                }
            }
        }
    };
}
wire_state_batch!(krabka_protocol::owned::read_share_group_state_response::StateBatch; from []; to [&]);
wire_state_batch!(krabka_protocol::owned::write_share_group_state_request::StateBatch; from [&]; to []);

/// Kafka's `ShareSnapshotValue` version 0, a full state image of one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareSnapshotValue {
    pub snapshot_epoch: i32,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    /// Milliseconds since the epoch at which the state was created.
    pub create_timestamp: i64,
    /// Milliseconds since the epoch at which this snapshot was written.
    pub write_timestamp: i64,
    pub state_batches: Vec<StateBatch>,
}

/// Kafka's `ShareUpdateValue` version 0, a delta over the latest snapshot of
/// one key. It has no state epoch and no timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareUpdateValue {
    pub snapshot_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
}

/// The two schemas share framing, batches and tags, but declare their own wire fields.
macro_rules! state_value_codec {
    ($name:ident, $encode:literal, $error:literal; $($field:ident: $codec:ident),+ $(,)?) => {
        impl $name {
            #[doc = $encode]
            #[must_use]
            pub fn encode(&self) -> Bytes {
                let mut buf = BytesMut::new();
                buf.put_i16(0);
                $(state_value_codec!(@put &mut buf, self.$field, $codec);)+
                put_batches(&mut buf, &self.state_batches);
                put_value_tags(&mut buf, self.delivery_complete_count);
                buf.freeze()
            }

            /// # Errors
            ///
            #[doc = $error]
            pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
                check_version(&mut buf)?;
                $(let $field = state_value_codec!(@get &mut buf, $codec);)+
                let state_batches = get_batches(&mut buf)?;
                let delivery_complete_count = get_value_tags(&mut buf)?;
                Ok(Self { $($field,)+ delivery_complete_count, state_batches })
            }
        }
    };
    (@put $buf:expr, $value:expr, i32) => { ($buf).put_i32($value) };
    (@put $buf:expr, $value:expr, i64) => { ($buf).put_i64($value) };
    (@put $buf:expr, $value:expr, Offset) => { ($buf).put_i64(($value).0) };
    (@get $buf:expr, i32) => { get_i32($buf)? };
    (@get $buf:expr, i64) => { get_i64($buf)? };
    (@get $buf:expr, Offset) => { Offset(get_i64($buf)?) };
}

state_value_codec!(ShareSnapshotValue, "Encodes the value with its `i16` version prefix, as Kafka's `CoordinatorRecordSerde.serializeValue` does.", "Returns an error when the bytes are not a version 0 `ShareSnapshotValue`.";
    snapshot_epoch: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
    create_timestamp: i64,
    write_timestamp: i64,
);
state_value_codec!(ShareUpdateValue, "Encodes the value with its `i16` version prefix.", "Returns an error when the bytes are not a version 0 `ShareUpdateValue`.";
    snapshot_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
);

/// Reads the `i16` value version and refuses every version but 0, the only
/// version of both schemas.
fn check_version(buf: &mut &[u8]) -> Result<(), BrokerError> {
    if get_i16(buf)? == 0 {
        Ok(())
    } else {
        Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unsupported share-state value version",
        )))
    }
}

/// Writes the message trailer: tag 0 when the delivery complete count is not
/// the default, and no tag when it is.
fn put_value_tags(buf: &mut BytesMut, delivery_complete_count: i32) {
    if delivery_complete_count == UNKNOWN_DELIVERY_COMPLETE_COUNT {
        flex::put_empty_tagged_fields(buf);
    } else {
        flex::put_tagged_fields(
            buf,
            vec![(
                TAG_DELIVERY_COMPLETE_COUNT,
                Bytes::copy_from_slice(&delivery_complete_count.to_be_bytes()),
            )],
        );
    }
}

/// Reads the message trailer. It returns the delivery complete count, or the
/// default when tag 0 is absent, and skips every other tag.
fn get_value_tags(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    let mut delivery_complete_count = UNKNOWN_DELIVERY_COMPLETE_COUNT;
    flex::read_tagged(buf, |tag, payload| {
        if tag != TAG_DELIVERY_COMPLETE_COUNT {
            return Ok(false);
        }
        if payload.len() != 4 {
            return Err(ProtocolError::InvalidValue(
                "DeliveryCompleteCount tag is not an int32",
            ));
        }
        delivery_complete_count = payload.get_i32();
        Ok(true)
    })?;
    Ok(delivery_complete_count)
}

fn put_batches(buf: &mut BytesMut, batches: &[StateBatch]) {
    flex::put_compact_array_len(buf, batches.len());
    for b in batches {
        buf.put_i64(b.first_offset.0);
        buf.put_i64(b.last_offset.0);
        buf.put_i8(b.delivery_state);
        buf.put_i16(b.delivery_count);
        flex::put_empty_tagged_fields(buf);
    }
}

fn get_batches(buf: &mut &[u8]) -> Result<Vec<StateBatch>, BrokerError> {
    let n = flex::get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        let first_offset = Offset(get_i64(buf)?);
        let last_offset = Offset(get_i64(buf)?);
        let delivery_state = flex::get_i8(buf)?;
        let delivery_count = get_i16(buf)?;
        flex::skip_tagged_fields(buf)?;
        out.push(StateBatch {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::share_coordinator::coordinator::test_support::{
        DeliveryAttemptCount, FixtureDeliveryState, StateBatchSetup, state_batch,
    };

    fn peek_type(buf: &[u8]) -> i16 {
        let mut r = buf;
        r.get_i16()
    }

    /// Bytes from a hex string that may hold spaces.
    fn hex(s: &str) -> Vec<u8> {
        crate::coordinator::unified::test_support::wire_bytes(
            &s.split_whitespace().collect::<Vec<_>>(),
        )
    }

    #[test]
    fn state_key_round_trip() {
        let rows = [
            (KEY_SHARE_SNAPSHOT, "g1", [7; 16], 3),
            (KEY_SHARE_UPDATE, "another-group", [1; 16], 0),
        ];
        for (record_type, group_id, topic_id, partition) in rows {
            let key = ShareStateKey {
                record_type,
                group_id: group_id.into(),
                topic_id: Uuid::from_bytes(topic_id),
                partition,
            };
            let bytes = encode_state_key(&key).unwrap();
            check!(peek_type(&bytes) == record_type);
            check!(parse_state_key(&bytes).unwrap() == key);
        }
    }

    /// The key writes its group id with an `INT16` length: a group id of 32767
    /// bytes encodes, and one of 32768 bytes is an error, not a panic.
    #[test]
    fn a_group_id_over_32767_bytes_does_not_encode() {
        for (length, encodes) in [(32_767, true), (32_768, false)] {
            let key = ShareStateKey {
                record_type: KEY_SHARE_UPDATE,
                group_id: "g".repeat(length),
                topic_id: Uuid::from_bytes([7; 16]),
                partition: 0,
            };

            let encoded = encode_state_key(&key);

            check!(encoded.is_ok() == encodes, "{length} bytes");
            if let Ok(bytes) = &encoded {
                check!(parse_state_key(bytes).unwrap() == key);
            } else {
                check!(matches!(encoded, Err(BrokerError::Protocol(_))));
            }
        }
    }

    #[test]
    fn unknown_key_type_rejected() {
        let mut b = BytesMut::new();
        b.put_i16(99);
        put_string(&mut b, "g").unwrap();
        b.put_slice(&[0u8; 16]);
        b.put_i32(0);
        assert!(parse_state_key(&b.freeze()).is_err());
    }

    /// The bytes of each value, derived field by field from Kafka's
    /// `ShareSnapshotValue.json` and `ShareUpdateValue.json` at version 0:
    /// the `i16` version, the fixed fields in schema order, a compact batch
    /// array whose batches each end with an empty tag trailer, and a message
    /// trailer that carries tag 0 only when `DeliveryCompleteCount` is not
    /// `-1`.
    #[test]
    fn snapshot_values_match_the_kafka_layout() {
        let rows = [
            (
                ShareSnapshotValue {
                    snapshot_epoch: 0,
                    state_epoch: 1,
                    leader_epoch: 0,
                    start_offset: Offset(-1),
                    delivery_complete_count: -1,
                    create_timestamp: 1000,
                    write_timestamp: 1000,
                    state_batches: vec![],
                },
                "0000 00000000 00000001 00000000 ffffffffffffffff \
                 00000000000003e8 00000000000003e8 01 00",
            ),
            (
                ShareSnapshotValue {
                    snapshot_epoch: 3,
                    state_epoch: 2,
                    leader_epoch: 4,
                    start_offset: Offset(10),
                    delivery_complete_count: 5,
                    create_timestamp: 1000,
                    write_timestamp: 2000,
                    state_batches: vec![
                        state_batch(StateBatchSetup {
                            bounds: Offset(10)..=Offset(19),
                            ..Default::default()
                        }),
                        state_batch(StateBatchSetup {
                            bounds: Offset(20)..=Offset(29),
                            delivery: FixtureDeliveryState::Acknowledged,
                            attempts: DeliveryAttemptCount(3),
                        }),
                    ],
                },
                "0000 00000003 00000002 00000004 000000000000000a \
                 00000000000003e8 00000000000007d0 03 \
                 000000000000000a 0000000000000013 00 0001 00 \
                 0000000000000014 000000000000001d 02 0003 00 \
                 01 00 04 00000005",
            ),
        ];
        for (value, expected) in rows {
            let bytes = value.encode();
            check!(bytes.as_ref() == hex(expected).as_slice());
            check!(ShareSnapshotValue::decode(&bytes).unwrap() == value);
        }
    }

    #[test]
    fn update_values_match_the_kafka_layout() {
        let rows = [
            (
                ShareUpdateValue {
                    snapshot_epoch: 3,
                    leader_epoch: 4,
                    start_offset: Offset(12),
                    delivery_complete_count: -1,
                    state_batches: vec![state_batch(StateBatchSetup {
                        bounds: Offset(12)..=Offset(15),
                        ..Default::default()
                    })],
                },
                "0000 00000003 00000004 000000000000000c 02 \
                 000000000000000c 000000000000000f 00 0001 00 00",
            ),
            (
                ShareUpdateValue {
                    snapshot_epoch: 3,
                    leader_epoch: 4,
                    start_offset: Offset(12),
                    delivery_complete_count: 7,
                    state_batches: vec![],
                },
                "0000 00000003 00000004 000000000000000c 01 01 00 04 00000007",
            ),
        ];
        for (value, expected) in rows {
            let bytes = value.encode();
            check!(bytes.as_ref() == hex(expected).as_slice());
            check!(ShareUpdateValue::decode(&bytes).unwrap() == value);
        }
    }

    /// A reader skips a tag it does not know, and refuses a version other
    /// than 0 and a malformed tag 0.
    #[test]
    fn decode_skips_unknown_tags_and_refuses_bad_input() {
        let unknown_tag =
            hex("0000 00000001 00000002 0000000000000003 01 02 00 04 00000009 05 01 ff");
        check!(
            ShareUpdateValue::decode(&unknown_tag).unwrap()
                == ShareUpdateValue {
                    snapshot_epoch: 1,
                    leader_epoch: 2,
                    start_offset: Offset(3),
                    delivery_complete_count: 9,
                    state_batches: vec![],
                }
        );
        let bad = [
            "0001 00000001 00000002 0000000000000003 01 00",
            "0000 00000001 00000002 0000000000000003 01 01 00 02 0009",
        ];
        for bytes in bad {
            check!(ShareUpdateValue::decode(&hex(bytes)).is_err(), "{bytes}");
        }
    }
}
