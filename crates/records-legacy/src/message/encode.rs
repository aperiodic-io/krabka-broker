//! Encoding one v0/v1 message: the byte count it occupies inside its frame,
//! and the write that prefixes the CRC-covered body with its CRC.
//!
//! The size calculation sits beside the write because both walk the same
//! optional fields, the v1-only timestamp and the two nullable byte strings,
//! and a frame whose `message_size` disagrees with the bytes written is
//! unparseable.

use {
    super::{Magic, Message},
    bytes::{BufMut, Bytes},
};

impl Message {
    /// Number of bytes that the message occupies inside the per-entry frame.
    ///
    /// The count starts at the CRC field. It goes up to and includes the
    /// value.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        // crc(4) + magic(1) + attrs(1) [+ ts(8) if v1] + key + value
        let mut n = 4 + 1 + 1;
        if matches!(self.magic, Magic::V1) {
            n += 8;
        }
        n += nullable_bytes_len(self.key.as_ref());
        n += nullable_bytes_len(self.value.as_ref());
        n
    }

    /// Encode the message into `buf`, including the CRC field.
    ///
    /// This method extends `buf`. It writes nothing before the CRC.
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        // Build the CRC-covered payload first, then prefix it with the CRC.
        // `encoded_len` counts the 4-byte CRC this body excludes; reserving it
        // anyway costs four bytes and keeps arithmetic out of a capacity hint,
        // where a wrong answer is invisible and so untestable.
        let mut body = Vec::with_capacity(self.encoded_len());
        body.push(self.magic.as_i8().cast_unsigned());
        body.push(self.attributes.cast_unsigned());
        if matches!(self.magic, Magic::V1) {
            let ts = self.timestamp.unwrap_or(-1);
            body.extend_from_slice(&ts.to_be_bytes());
        }
        put_nullable_bytes(&mut body, self.key.as_ref());
        put_nullable_bytes(&mut body, self.value.as_ref());

        let crc = crc32fast::hash(&body);
        buf.put_u32(crc);
        buf.put_slice(&body);
    }
}

fn nullable_bytes_len(b: Option<&Bytes>) -> usize { 4 + b.map_or(0, Bytes::len) }

fn put_nullable_bytes(buf: &mut Vec<u8>, b: Option<&Bytes>) {
    match b {
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(data) => {
            let len = i32::try_from(data.len()).unwrap_or(i32::MAX);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(data);
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::message::test_support::{
            RoundTrip, fixture_v0, fixture_v1, fixture_v1_null, round_trip,
        },
    };

    #[yare::parameterized(
        v0 = { fixture_v0() },
        v1 = { fixture_v1() },
    )]
    fn message_roundtrips(message: Message) {
        let RoundTrip { buffer, decoded } = round_trip(&message);
        assert2::assert!(buffer.len() == message.encoded_len());
        assert2::assert!(decoded == message);
    }

    #[test]
    fn v1_null_key_and_value() {
        let m = fixture_v1_null();
        let RoundTrip { decoded, .. } = round_trip(&m);
        assert2::assert!(decoded == m);
    }

    #[test]
    fn v1_missing_timestamp_encodes_minus_one() {
        let m = Message {
            magic: Magic::V1,
            attributes: 0,
            timestamp: None,
            key: None,
            value: None,
        };
        let RoundTrip { decoded, .. } = round_trip(&m);
        assert2::assert!(
            decoded
                == Message {
                    timestamp: Some(-1),
                    ..m
                }
        );
    }

    #[test]
    fn empty_key_is_some_not_null() {
        // len == 0 is an empty (non-null) field; only len == -1 is null.
        let m = Message {
            magic: Magic::V0,
            attributes: 0,
            timestamp: None,
            key: Some(Bytes::new()),
            value: Some(Bytes::from_static(b"v")),
        };
        let RoundTrip { decoded, .. } = round_trip(&m);
        assert2::assert!(decoded == m);
    }
}
