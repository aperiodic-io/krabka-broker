//! KIP-482 flexible-version leaf codecs for the `__consumer_offsets` record
//! values.
//!
//! # Which records are flexible
//!
//! The layouts came from the Apache Kafka schemas at tag `4.3.1`, under
//! `group-coordinator/src/main/resources/common/message/`. Every
//! `coordinator-key` there declares `"flexibleVersions": "none"`, and every
//! `coordinator-value` of the KIP-848, KIP-932 and KIP-1071 families declares
//! `"flexibleVersions": "0+"`. So:
//!
//! - **Keys** keep the legacy encoding: an `i16` length for a string, an `i32`
//!   count for an array, and no tagged-field trailer. They use the
//!   `super::{get_string, put_string}` helpers, not this module.
//! - **Values** of those three families are flexible at version 0: a string is
//!   a compact string (unsigned varint `len + 1`), an array is a compact array
//!   (unsigned varint `count + 1`), a null takes the `0` prefix, and the
//!   message **and every nested struct** end with an unsigned-varint
//!   tagged-field count.
//!
//! The two classic families are the exception. `OffsetCommitValue` and
//! `GroupMetadataValue` declare `"flexibleVersions": "4+"`, and the broker
//! writes versions 1, 3 and 3, so those stay on the legacy encoding in
//! [`super`].
//!
//! A dropped trailing tagged-field count is not a lenient-reader nit: Kafka's
//! generated reader consumes it unconditionally, so its absence surfaces as a
//! `BufferUnderflowException` and takes down `kafka-console-consumer` in the
//! middle of the topic.
//!
//! # What this module adds
//!
//! `krabka_protocol::primitives` already implements every leaf, and
//! `krabka_protocol::tagged_fields` implements the trailer. These wrappers only
//! bind them to the broker's `&mut &[u8]` reader and map `ProtocolError` into
//! [`BrokerError`], so a record codec reads as a list of fields.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    ProtocolError,
    primitives::{array, fixed, string_bytes, uuid::Uuid, varint},
    tagged_fields::{UnknownTaggedFields, WriteTaggedFields, read_tagged_fields},
};

use crate::{coordinator::unified::persistence::get_i16, error::BrokerError};

/// Writes a version-0 flexible record with no known message tags.
pub(crate) fn encode_value(write: impl FnOnce(&mut BytesMut)) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(0);
    write(&mut buf);
    put_empty_tagged_fields(&mut buf);
    buf.freeze()
}

/// Reads the common version and tagged-field framing around a record's fields.
/// `record` is the error that names the record when its version is not 0, as
/// [`get_value_version`] refuses it.
///
/// # Errors
/// Returns the field reader's error, or an error when the version is not 0 or
/// the version or trailer is truncated.
pub(crate) fn decode_value<T>(
    mut buf: &[u8],
    record: &'static str,
    read: impl FnOnce(&mut &[u8]) -> Result<T, BrokerError>,
) -> Result<T, BrokerError> {
    get_value_version(&mut buf, 0, record)?;
    let value = read(&mut buf)?;
    skip_tagged_fields(&mut buf)?;
    Ok(value)
}

/// Defines field codecs within the shared version-0 flexible record framing.
/// The string literal is Kafka's record name, which the value-version error
/// names.
macro_rules! value_codec {
    ($name:ident($record:literal), encode($($receiver:tt)*) -> $writer:ident $write:block decode($reader:ident) $read:block) => {
        impl $name {
            #[must_use]
            pub fn encode($($receiver)*) -> bytes::Bytes {
                $crate::coordinator::unified::persistence::flex::encode_value(|$writer| $write)
            }

            /// # Errors
            /// Returns an error when the value version is not 0, or when a
            /// field or the tagged-field trailer is truncated or invalid.
            pub fn decode(buf: &[u8]) -> Result<Self, $crate::error::BrokerError> {
                $crate::coordinator::unified::persistence::flex::decode_value(
                    buf,
                    concat!("unknown ", $record, " version"),
                    |$reader| $read,
                )
            }
        }
    };
}

pub(crate) use value_codec;

/// Implements a version-0 flexible value of an `int32` epoch followed by a
/// tagged-field trailer whose one field is an `int64` at tag 0 with default 0.
///
/// Four Kafka 4.3.1 records have that layout: `ConsumerGroupMetadataValue`,
/// whose tag is `MetadataHash`, and the consumer, share and streams
/// `*TargetAssignmentMetadataValue`, whose tag is KIP-1263's
/// `AssignmentTimestamp`. The macro keeps their domain types distinct while
/// they share the codec. The string literal is Kafka's record name, which the
/// value-version error names.
macro_rules! epoch_value {
    (
        $(#[$meta:meta])*
        $name:ident($record:literal) {
            $(#[$epoch_meta:meta])* $epoch:ident,
            $(#[$tagged_meta:meta])* $tagged:ident $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name {
            $(#[$epoch_meta])*
            pub $epoch: i32,
            $(#[$tagged_meta])*
            pub $tagged: i64,
        }

        impl $name {
            #[must_use]
            pub fn encode(self) -> bytes::Bytes {
                let mut buf = bytes::BytesMut::new();
                bytes::BufMut::put_i16(&mut buf, 0);
                bytes::BufMut::put_i32(&mut buf, self.$epoch);
                $crate::coordinator::unified::persistence::flex::put_tag0_i64(
                    &mut buf,
                    self.$tagged,
                );
                buf.freeze()
            }

            /// # Errors
            /// Returns an error when `buf` ends before a field, when the value
            /// version is not 0, or when the tag 0 payload is shorter than
            /// eight bytes.
            pub fn decode(mut buf: &[u8]) -> Result<Self, $crate::error::BrokerError> {
                $crate::coordinator::unified::persistence::flex::get_value_version(
                    &mut buf,
                    0,
                    concat!("unknown ", $record, " version"),
                )?;
                let $epoch = $crate::coordinator::unified::persistence::get_i32(&mut buf)?;
                let $tagged =
                    $crate::coordinator::unified::persistence::flex::get_tag0_i64(&mut buf)?;
                Ok(Self { $epoch, $tagged })
            }
        }
    };
}

pub(crate) use epoch_value;

/// Implements a version-0 flexible value with one array-shaped field. The
/// string literal is Kafka's record name, which the value-version error names.
macro_rules! array_value_codec {
    ($name:ident($record:literal), $field:ident, $encode:path, $decode:path) => {
        $crate::coordinator::unified::persistence::flex::value_codec! {
            $name($record),
            encode(&self) -> buf {
                $encode(buf, &self.$field);
            }
            decode(buf) {
                Ok(Self { $field: $decode(buf)? })
            }
        }
    };
}

pub(crate) use array_value_codec;

fn protocol(e: ProtocolError) -> BrokerError {
    BrokerError::Protocol(e)
}

// ───────────────────────────────────────────────────────────────── writers ──

pub(crate) fn put_compact_string(buf: &mut BytesMut, s: &str) {
    string_bytes::put_compact_string(buf, s);
}

pub(crate) fn put_compact_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    string_bytes::put_compact_nullable_string(buf, s);
}

/// The rack, client id and host fields common to all modern group members.
pub(crate) fn put_member_client(buf: &mut BytesMut, rack: Option<&str>, id: &str, host: &str) {
    put_compact_nullable_string(buf, rack);
    put_compact_string(buf, id);
    put_compact_string(buf, host);
}

pub(crate) fn put_compact_bytes(buf: &mut BytesMut, b: &[u8]) {
    string_bytes::put_compact_bytes(buf, b);
}

/// Writes the compact `count + 1` prefix of a non-null array.
pub(crate) fn put_compact_array_len(buf: &mut BytesMut, n: usize) {
    array::put_array_len(buf, n, true);
}

/// Writes a `uuid` field: sixteen raw bytes, with no length prefix.
pub(crate) fn put_uuid(buf: &mut BytesMut, bytes: [u8; 16]) {
    krabka_protocol::primitives::uuid::put_uuid(buf, Uuid(bytes));
}

/// Writes the tagged-field trailer of a message or a nested struct that sets no
/// tag: a single `0` byte.
pub(crate) fn put_empty_tagged_fields(buf: &mut BytesMut) {
    varint::put_uvarint(buf, 0);
}

/// Writes a tagged-field trailer that carries `entries`, each a `(tag,
/// payload)` pair. [`WriteTaggedFields`] sorts them, which the wire format
/// requires.
pub(crate) fn put_tagged_fields(buf: &mut BytesMut, entries: Vec<(u32, Bytes)>) {
    let mut w = WriteTaggedFields::new();
    for (tag, payload) in entries {
        w.add(tag, payload);
    }
    w.write(buf, &UnknownTaggedFields::default());
}

/// Writes a tagged-field trailer whose only field is an `int64` at tag 0 with
/// default 0, as Kafka's generated writer does: the field only when it is not
/// 0, so a default value ends in the empty trailer.
pub(crate) fn put_tag0_i64(buf: &mut BytesMut, value: i64) {
    let tags = if value == 0 {
        Vec::new()
    } else {
        vec![(0, Bytes::copy_from_slice(&value.to_be_bytes()))]
    };
    put_tagged_fields(buf, tags);
}

// ───────────────────────────────────────────────────────────────── readers ──

/// Reads a fixed-width `int8`.
pub(crate) fn get_i8(buf: &mut &[u8]) -> Result<i8, BrokerError> {
    fixed::get_i8(buf).map_err(protocol)
}

/// Reads a fixed-width `uint16`, the type of the streams member's advertised
/// port.
pub(crate) fn get_u16(buf: &mut &[u8]) -> Result<u16, BrokerError> {
    fixed::get_u16(buf).map_err(protocol)
}

/// Reads the leading `i16` version of a `coordinator-value` and refuses one
/// outside `0..=highest`.
///
/// Kafka's `CoordinatorRecordSerde.deserialize` throws
/// `UnknownRecordVersionException` for a value version outside the schema's
/// `validVersions`, and the loader fails on it. Every value read here declares
/// `"validVersions": "0"`, so `highest` is 0 for all of them today.
pub(crate) fn get_value_version(
    buf: &mut &[u8],
    highest: i16,
    record: &'static str,
) -> Result<i16, BrokerError> {
    let version = get_i16(buf)?;
    if (0..=highest).contains(&version) {
        Ok(version)
    } else {
        Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue(record),
        ))
    }
}

/// Reads the trailer that [`put_tag0_i64`] writes and returns the tag 0
/// `int64`, 0 when it is absent. Every other tag is skipped.
///
/// # Errors
///
/// Returns an error when the trailer does not decode, or when the tag 0
/// payload is shorter than eight bytes.
pub(crate) fn get_tag0_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
    let mut value = 0;
    read_tagged(buf, |tag, payload| {
        if tag != 0 {
            return Ok(false);
        }
        value = fixed::get_i64(payload)?;
        Ok(true)
    })?;
    Ok(value)
}

pub(crate) fn get_compact_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    string_bytes::get_compact_string_owned(buf).map_err(protocol)
}

pub(crate) fn get_compact_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, BrokerError> {
    string_bytes::get_compact_nullable_string_owned(buf).map_err(protocol)
}

/// Reads the rack and client identity written by [`put_member_client`].
///
/// # Errors
/// Returns an error when a string field is truncated or invalid.
pub(crate) fn get_member_client(
    buf: &mut &[u8],
) -> Result<(Option<String>, String, String), BrokerError> {
    Ok((
        get_compact_nullable_string(buf)?,
        get_compact_string(buf)?,
        get_compact_string(buf)?,
    ))
}

pub(crate) fn get_compact_bytes(buf: &mut &[u8]) -> Result<Bytes, BrokerError> {
    string_bytes::get_compact_bytes_owned(buf).map_err(protocol)
}

/// Reads the compact `count + 1` prefix of a non-null array.
pub(crate) fn get_compact_array_len(buf: &mut &[u8]) -> Result<usize, BrokerError> {
    array::get_array_len(buf, true).map_err(protocol)
}

pub(crate) fn get_uuid(buf: &mut &[u8]) -> Result<[u8; 16], BrokerError> {
    krabka_protocol::primitives::uuid::get_uuid(buf)
        .map(|u| u.0)
        .map_err(protocol)
}

/// Reads and discards a tagged-field trailer whose tags the broker does not
/// interpret. Every tag Kafka defines on these records is optional with a
/// default, so dropping them loses nothing the broker keeps state for.
pub(crate) fn skip_tagged_fields(buf: &mut &[u8]) -> Result<(), BrokerError> {
    read_tagged_fields(buf, |_, _| Ok(false))
        .map(|_| ())
        .map_err(protocol)
}

/// Reads a tagged-field trailer, handing each entry to `known`. The closure
/// returns `true` when it recognized the tag and consumed the whole payload;
/// every other entry is skipped.
pub(crate) fn read_tagged<F>(buf: &mut &[u8], known: F) -> Result<(), BrokerError>
where
    F: FnMut(u32, &mut &[u8]) -> Result<bool, ProtocolError>,
{
    read_tagged_fields(buf, known).map(|_| ()).map_err(protocol)
}

// ───────────────────────────────────────────────────── array conveniences ──

/// Writes a compact array in iterator order with the supplied element codec.
pub(crate) fn put_compact_array<T>(
    buf: &mut BytesMut,
    items: impl ExactSizeIterator<Item = T>,
    mut write: impl FnMut(&mut BytesMut, T),
) {
    put_compact_array_len(buf, items.len());
    for item in items {
        write(buf, item);
    }
}

/// Reads a compact array with the supplied element codec.
pub(crate) fn get_compact_array<T>(
    buf: &mut &[u8],
    mut read: impl FnMut(&mut &[u8]) -> Result<T, BrokerError>,
) -> Result<Vec<T>, BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(read(buf)?);
    }
    Ok(out)
}

/// Write topic-id/partition-list structs with empty nested tagged fields.
pub(crate) fn put_assigned_topics<T>(
    buf: &mut BytesMut,
    items: &[T],
    fields: impl Fn(&T) -> (&Uuid, &[i32]),
) {
    put_compact_array(buf, items.iter(), |buf, item| {
        let (topic, partitions) = fields(item);
        put_uuid(buf, topic.0);
        put_i32_array(buf, partitions);
        put_empty_tagged_fields(buf);
    });
}

/// Read the topic-id/partition-list structs of target and share assignments.
///
/// # Errors
/// Returns a protocol error for malformed fields or a missing nested trailer.
pub(crate) fn get_assigned_topics<T>(
    buf: &mut &[u8],
    build: impl Fn(Uuid, Vec<i32>) -> T,
) -> Result<Vec<T>, BrokerError> {
    get_compact_array(buf, |buf| {
        let topic = Uuid(get_uuid(buf)?);
        let partitions = get_i32_array(buf)?;
        skip_tagged_fields(buf)?;
        Ok(build(topic, partitions))
    })
}

/// Writes a compact array of `i32`, the `[]int32` of the schemas.
pub(crate) fn put_i32_array(buf: &mut BytesMut, items: &[i32]) {
    put_compact_array(buf, items.iter(), |buf, &v| buf.put_i32(v));
}

/// Reads the `[]int32` written by [`put_i32_array`].
pub(crate) fn get_i32_array(buf: &mut &[u8]) -> Result<Vec<i32>, BrokerError> {
    get_compact_array(buf, super::get_i32)
}

/// Writes a compact array of compact strings, the `[]string` of the schemas.
pub(crate) fn put_string_array(buf: &mut BytesMut, items: &[String]) {
    put_compact_array(buf, items.iter(), |buf, s| put_compact_string(buf, s));
}

/// Reads the `[]string` written by [`put_string_array`].
pub(crate) fn get_string_array(buf: &mut &[u8]) -> Result<Vec<String>, BrokerError> {
    get_compact_array(buf, get_compact_string)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn compact_string_is_length_plus_one() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "abc");
        assert!(&buf[..] == b"\x04abc");
    }

    #[test]
    fn compact_null_string_is_zero() {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None);
        assert!(&buf[..] == b"\x00");
    }

    #[test]
    fn empty_tagged_fields_is_one_zero_byte() {
        let mut buf = BytesMut::new();
        put_empty_tagged_fields(&mut buf);
        assert!(&buf[..] == b"\x00");
    }

    #[test]
    fn i32_array_prefix_is_count_plus_one() {
        let mut buf = BytesMut::new();
        put_i32_array(&mut buf, &[7]);
        assert!(&buf[..] == b"\x02\x00\x00\x00\x07");
        let mut r = &buf[..];
        assert!(get_i32_array(&mut r).unwrap() == vec![7]);
        assert!(r.is_empty());
    }

    #[test]
    fn tagged_fields_round_trip_in_tag_order() {
        let mut buf = BytesMut::new();
        put_tagged_fields(
            &mut buf,
            vec![(9, Bytes::from_static(b"z")), (1, Bytes::from_static(b"a"))],
        );
        assert!(&buf[..] == b"\x02\x01\x01a\x09\x01z");
    }

    #[test]
    fn skip_tagged_fields_consumes_the_trailer() {
        let mut buf = BytesMut::new();
        put_tagged_fields(&mut buf, vec![(3, Bytes::from_static(b"xy"))]);
        buf.put_u8(0xff);
        let mut r = &buf[..];
        skip_tagged_fields(&mut r).unwrap();
        assert!(r == b"\xff");
    }
}
