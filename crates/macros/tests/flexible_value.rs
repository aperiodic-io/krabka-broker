//! Ordinary field methods retain ownership, wire order and early error returns.

#[path = "support/codec.rs"]
mod error;
#[path = "support/framing.rs"]
mod framing;

use {
    assert2::assert,
    bytes::{BufMut, BytesMut},
    error::{BrokerError, take},
    framing::coordinator,
};

#[derive(Debug, PartialEq, Eq)]
struct BorrowedValue {
    payload: i32,
    state: u8,
}

#[krabka_macros::flexible_value(record = "BorrowedRecord")]
impl BorrowedValue {
    fn encode_fields(&self, wire: &mut BytesMut) {
        wire.put_u8(self.state);
        wire.put_i32(self.payload);
    }

    fn decode_fields(wire: &mut &[u8]) -> Result<Self, BrokerError> {
        let [state] = take(wire)?;
        if state == 0xff {
            return Err("invalid state");
        }
        let payload = i32::from_be_bytes(take(wire)?);
        Ok(Self { payload, state })
    }
}

struct OwnedValue(String);

#[krabka_macros::flexible_value(record = "OwnedRecord")]
impl OwnedValue {
    fn encode_fields(self, buf: &mut BytesMut) {
        let Self(text) = self;
        buf.extend(text.into_bytes());
    }

    fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let [value] = take(buf)?;
        Ok(Self(char::from(value).to_string()))
    }
}

#[test]
fn borrowed_encoding_keeps_api_ownership_and_explicit_field_order() {
    let encode: fn(&BorrowedValue) -> bytes::Bytes = BorrowedValue::encode;
    let value = BorrowedValue {
        payload: -2,
        state: 3,
    };
    let golden = b"\x00\x00\x03\xff\xff\xff\xfe\x00";
    assert!(encode(&value) == golden[..]);
    assert!(encode(&value) == golden[..]);
    assert!(BorrowedValue::decode(golden) == Ok(value));
    for end in 0..golden.len() {
        assert!(BorrowedValue::decode(&golden[..end]).is_err());
    }
}

#[test]
fn consuming_field_method_keeps_the_owned_public_receiver() {
    let encode: fn(OwnedValue) -> bytes::Bytes = OwnedValue::encode;
    assert!(encode(OwnedValue("a".into())) == b"\x00\x00a\x00"[..]);
    assert!(OwnedValue::decode(b"\x00\x00a\x00").unwrap().0 == "a");
}

#[test]
fn versions_precede_field_reads_and_field_errors_precede_trailers() {
    assert!(BorrowedValue::decode(b"\x00\x01\xff") == Err("unknown BorrowedRecord version"));
    assert!(OwnedValue::decode(b"\xff\xff").err() == Some("unknown OwnedRecord version"));
    assert!(BorrowedValue::decode(b"\x00\x00\xff") == Err("invalid state"));
    assert!(BorrowedValue::decode(b"\x00\x00\x01\x00\x00\x00\x02\x01") == Err("bad trailer"));
}
