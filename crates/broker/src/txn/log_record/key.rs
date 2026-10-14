//! Codec for the Kafka `TransactionLogKey` record, version 0.
//!
//! The key carries the transactional id, which the companion value record does
//! not repeat. A `__transaction_state` record is therefore decoded key first,
//! and the id it yields is handed to the value decoder.

use {
    crate::{coordinator::unified::persistence::put_string, error::BrokerError},
    bytes::BytesMut,
    krabka_protocol::{
        ProtocolError,
        primitives::{
            fixed::{get_i16, put_i16},
            string_bytes::get_string_owned,
        },
    },
};

/// Encode the Kafka `TransactionLogKey`, version 0.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the transactional id is longer than
/// 32767 bytes, which the key's `int16` string length cannot carry.
pub(crate) fn encode_key(transactional_id: &str) -> Result<Vec<u8>, BrokerError> {
    let mut buf = BytesMut::new();
    put_i16(&mut buf, 0);
    put_string(&mut buf, transactional_id)?;
    Ok(buf.to_vec())
}

/// Decode a Kafka `TransactionLogKey` and return the transactional id.
pub(crate) fn decode_key(bytes: &[u8]) -> Result<String, BrokerError> {
    let mut buf = bytes;
    let version = get_i16(&mut buf)?;
    if version != 0 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unsupported TransactionLogKey version",
        )));
    }
    // Kafka's generated `TransactionLogKey` reader stops after its last field
    // and ignores any bytes that follow, so this decoder does too.
    Ok(get_string_owned(&mut buf)?)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn key_round_trip() {
        let encoded = encode_key("abc").unwrap();
        assert!(decode_key(&encoded).unwrap() == "abc");
        // `00 00` version + int16 length (3) + bytes.
        assert!(encoded == &[0x00, 0x00, 0x00, 0x03, b'a', b'b', b'c']);
    }

    /// The key writes the transactional id with an `int16` length: an id of
    /// 32767 bytes encodes, and one of 32768 bytes is an error, not a panic.
    #[yare::parameterized(
        at_limit = { 32_767, true },
        over_limit = { 32_768, false },
    )]
    fn a_transactional_id_over_32767_bytes_does_not_encode(length: usize, encodes: bool) {
        let id = "t".repeat(length);

        let encoded = encode_key(&id);

        assert2::assert!(encoded.is_ok() == encodes, "{length} bytes");
        if let Ok(bytes) = &encoded {
            assert2::assert!(decode_key(bytes).unwrap() == id);
        } else {
            assert2::assert!(matches!(encoded, Err(BrokerError::Protocol(_))));
        }
    }

    #[test]
    fn decode_key_rejects_unknown_version_and_truncation() {
        let key = encode_key("abc").unwrap();
        // unknown version
        let mut bad = key.clone();
        bad[1] = 0x09;
        assert!(decode_key(&bad).is_err());
        // truncated
        assert!(decode_key(&key[..1]).is_err());
    }

    /// Kafka's generated reader ignores bytes after the last field.
    #[test]
    fn decode_key_ignores_trailing_bytes() {
        let mut key = encode_key("abc").unwrap();
        key.push(0xff);
        assert!(decode_key(&key).unwrap() == "abc");
    }
}
