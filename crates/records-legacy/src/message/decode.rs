//! Decoding one v0/v1 message: the CRC check over the frame, then the parse
//! of the magic byte, the v1 timestamp, and the nullable key and value.
//!
//! The parse is its own module because nearly every branch in it is a
//! rejection: a short buffer, an undersized frame, a bad CRC, an unknown
//! magic byte, a negative length, or bytes left over after the value each
//! map to a distinct [`LegacyRecordsError`] variant.

use {
    super::{Magic, Message},
    crate::error::LegacyRecordsError,
    bytes::{Buf, Bytes},
};

impl Message {
    /// Decode a message from `buf`.
    ///
    /// `buf` must be positioned at the CRC and must contain at least
    /// `frame_size` bytes. `frame_size` is the `message_size` from the
    /// outer `MessageSet` frame.
    /// # Errors
    /// Returns `Truncated` if `buf` holds fewer than `frame_size` bytes.
    /// Returns `Malformed` if `frame_size` is less than the 6-byte minimum.
    /// Returns `CrcMismatch` if the computed CRC differs from the CRC field.
    /// Returns `UnsupportedMagic` if the magic byte is not 0 or 1.
    /// Returns `Truncated` if a v1 frame holds fewer than 8 timestamp bytes.
    /// Returns `NegativeLength` if the `key` or `value` length is negative and is not -1.
    /// Returns `Truncated` if the `key` or `value` bytes stop early.
    /// Returns `Malformed` if bytes remain after the value.
    /// # Panics
    /// Panics if a nonnegative `i32` length does not fit in a `usize`. This cannot occur on any target Krabka builds for.
    pub fn decode_from<B: Buf>(buf: &mut B, frame_size: usize) -> Result<Self, LegacyRecordsError> {
        if buf.remaining() < frame_size {
            return Err(LegacyRecordsError::Truncated {
                needed: frame_size - buf.remaining(),
            });
        }
        if frame_size < 6 {
            return Err(LegacyRecordsError::Malformed(format!(
                "message frame {frame_size} bytes < 6 minimum"
            )));
        }
        let mut frame = vec![0u8; frame_size];
        buf.copy_to_slice(&mut frame);

        let expected_crc = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let computed = crc32fast::hash(&frame[4..]);
        if expected_crc != computed {
            return Err(LegacyRecordsError::CrcMismatch {
                expected: expected_crc,
                computed,
            });
        }

        let mut cur = &frame[4..];
        let magic = Magic::from_i8(cur.get_i8())?;
        let attributes = cur.get_i8();
        let timestamp = match magic {
            Magic::V0 => None,
            Magic::V1 => {
                if cur.remaining() < 8 {
                    return Err(LegacyRecordsError::Truncated {
                        needed: 8 - cur.remaining(),
                    });
                }
                Some(cur.get_i64())
            }
        };
        let key = get_nullable_bytes(&mut cur, "key")?;
        let value = get_nullable_bytes(&mut cur, "value")?;
        if !cur.is_empty() {
            return Err(LegacyRecordsError::Malformed(format!(
                "trailing {} byte(s) inside message frame",
                cur.len()
            )));
        }
        Ok(Self {
            magic,
            attributes,
            timestamp,
            key,
            value,
        })
    }
}

fn get_nullable_bytes(
    buf: &mut &[u8],
    label: &'static str,
) -> Result<Option<Bytes>, LegacyRecordsError> {
    if buf.remaining() < 4 {
        return Err(LegacyRecordsError::Truncated {
            needed: 4 - buf.remaining(),
        });
    }
    let len = buf.get_i32();
    if len < 0 {
        if len == -1 {
            return Ok(None);
        }
        return Err(LegacyRecordsError::NegativeLength { label, len });
    }
    let n = usize::try_from(len).expect("nonnegative i32 fits usize");
    if buf.remaining() < n {
        return Err(LegacyRecordsError::Truncated {
            needed: n - buf.remaining(),
        });
    }
    let data = Bytes::copy_from_slice(&buf[..n]);
    buf.advance(n);
    Ok(Some(data))
}

#[cfg(test)]
mod tests {

    use {super::*, crate::message::test_support::fixture_v1, bytes::BytesMut};

    #[test]
    fn rejects_bad_crc() {
        let m = fixture_v1();
        let mut buf = BytesMut::new();
        m.encode_into(&mut buf);
        buf[0] ^= 0xFF;
        let mut cur: &[u8] = &buf[..];
        assert2::assert!(matches!(
            Message::decode_from(&mut cur, m.encoded_len()),
            Err(LegacyRecordsError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_magic() {
        let mut buf = BytesMut::new();
        // Build a fake message with magic = 2.
        let mut body = vec![2u8, 0u8]; // magic, attrs
        body.extend_from_slice(&(-1i32).to_be_bytes()); // key=null
        body.extend_from_slice(&(-1i32).to_be_bytes()); // value=null
        let crc = crc32fast::hash(&body);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&body);
        let frame_size = buf.len();
        let mut cur: &[u8] = &buf[..];
        assert2::assert!(matches!(
            Message::decode_from(&mut cur, frame_size),
            Err(LegacyRecordsError::UnsupportedMagic { found: 2 })
        ));
    }

    // Build a frame: crc(4) | magic | attrs | trailing, with a valid CRC.
    fn frame_with_body(magic: u8, attrs: u8, trailing: &[u8]) -> Vec<u8> {
        let mut body = vec![magic, attrs];
        body.extend_from_slice(trailing);
        let crc = crc32fast::hash(&body);
        let mut frame = crc.to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[test]
    fn decode_buffer_shorter_than_frame_reports_needed() {
        // 4 bytes available, frame claims 10: needed = 10 - 4 = 6.
        let data = [0u8; 4];
        let mut cur: &[u8] = &data;
        let err = Message::decode_from(&mut cur, 10).unwrap_err();
        assert2::assert!(matches!(err, LegacyRecordsError::Truncated { needed: 6 }));
    }

    #[test]
    fn decode_rejects_frame_below_minimum() {
        // frame_size 5 (< 6) is malformed; buffer has >= 5 bytes.
        let data = [0u8; 5];
        let mut cur: &[u8] = &data;
        assert2::assert!(matches!(
            Message::decode_from(&mut cur, 5),
            Err(LegacyRecordsError::Malformed(_))
        ));
    }

    #[test]
    fn decode_min_size_frame_parses_past_guard() {
        // A 6-byte frame clears the `< 6` guard (6 < 6 false) and then fails on
        // the missing key-length field -> Truncated, not Malformed. This
        // distinguishes `<` from `<=` at the boundary.
        let frame = frame_with_body(0, 0, &[]);
        assert2::assert!(frame.len() == 6);
        let mut cur: &[u8] = &frame;
        let err = Message::decode_from(&mut cur, 6).unwrap_err();
        assert2::assert!(matches!(err, LegacyRecordsError::Truncated { .. }));
    }

    fn check_truncated_frame(magic: u8, trailing: &[u8], needed: usize) {
        let frame = frame_with_body(magic, 0, trailing);
        let err = Message::decode_from(&mut &frame[..], frame.len()).unwrap_err();
        assert2::assert!(
            matches!(err, LegacyRecordsError::Truncated { needed: actual } if actual == needed)
        );
    }

    #[test]
    fn decode_v1_truncated_timestamp_reports_needed() {
        // magic+attrs then only 4 of the 8 timestamp bytes: needed = 8 - 4 = 4.
        check_truncated_frame(1, &[0u8; 4], 4);
    }

    #[test]
    fn decode_v1_timestamp_present_then_missing_kv() {
        // Full 8 timestamp bytes but no key/value: clears the `< 8` timestamp
        // guard (8 < 8 false) and fails on the missing key length (needed 4),
        // not the guard's own needed (0). Distinguishes `<` from `<=`.
        check_truncated_frame(1, &[0u8; 8], 4);
    }

    #[test]
    fn decode_truncated_key_length_reports_needed() {
        // V0 frame with only 1 byte where the 4-byte key length is expected:
        // needed = 4 - 1 = 3.
        check_truncated_frame(0, &[0xAA], 3);
    }

    #[test]
    fn decode_truncated_key_body_reports_needed() {
        // V0 frame: key length = 5 but only 2 body bytes present.
        // needed = 5 - 2 = 3.
        let mut trailing = 5i32.to_be_bytes().to_vec();
        trailing.extend_from_slice(&[0xAA, 0xBB]);
        check_truncated_frame(0, &trailing, 3);
    }
}
