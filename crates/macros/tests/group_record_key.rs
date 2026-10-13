//! Compiled group-key adapters preserve legacy framing and decoder boundaries.

use {
    assert2::assert,
    bytes::{BufMut, Bytes, BytesMut},
};

#[derive(Debug, PartialEq, Eq)]
enum ProtocolError {
    InvalidValue(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
enum BrokerError {
    Protocol(ProtocolError),
    InvalidString,
}

fn get_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    let [high, low, ..] = *buf else {
        return Err(BrokerError::InvalidString);
    };
    let length = usize::try_from(i16::from_be_bytes([*high, *low]))
        .map_err(|_| BrokerError::InvalidString)?;
    let text = buf.get(2..2 + length).ok_or(BrokerError::InvalidString)?;
    let text = str::from_utf8(text)
        .map_err(|_| BrokerError::InvalidString)?
        .to_owned();
    *buf = &buf[2 + length..];
    Ok(text)
}

fn encode_string_key(version: i16, parts: &[&str]) -> Result<Bytes, BrokerError> {
    let mut output = BytesMut::new();
    output.put_i16(version);
    for part in parts {
        let length = i16::try_from(part.len()).map_err(|_| BrokerError::InvalidString)?;
        output.put_i16(length);
        output.extend_from_slice(part.as_bytes());
    }
    Ok(output.freeze())
}

const GROUP: i16 = 3;
const MEMBER: i16 = 5;
const REGEX: i16 = 16;

#[derive(Debug, PartialEq, Eq, krabka_macros::GroupRecordKey)]
#[group_key(invalid = "unknown fixture key version")]
enum RecordKey {
    #[group_key(version = GROUP)]
    GroupMetadata { group_id: String },
    #[group_key(version = MEMBER)]
    MemberMetadata { group_id: String, member_id: String },
    #[group_key(version = REGEX)]
    RegularExpression { group_id: String, regex: String },
}

#[test]
fn concrete_variants_keep_wire_bytes_group_access_and_trailing_byte_acceptance() {
    let cases: [(RecordKey, &[u8]); 3] = [
        (
            RecordKey::GroupMetadata {
                group_id: "g".into(),
            },
            b"\x00\x03\x00\x01g",
        ),
        (
            RecordKey::MemberMetadata {
                group_id: "g".into(),
                member_id: "m".into(),
            },
            b"\x00\x05\x00\x01g\x00\x01m",
        ),
        (
            RecordKey::RegularExpression {
                group_id: "g".into(),
                regex: "a.*".into(),
            },
            b"\x00\x10\x00\x01g\x00\x03a.*",
        ),
    ];
    for (key, golden) in cases {
        assert!(key.group_id() == "g");
        assert!(key.encode().unwrap().as_ref() == golden);
        let version = i16::from_be_bytes([golden[0], golden[1]]);
        assert!(RecordKey::parse(version, &golden[2..]).unwrap() == key);
        let mut with_trailer = golden[2..].to_vec();
        with_trailer.extend_from_slice(b"ignored");
        assert!(RecordKey::parse(version, &with_trailer).unwrap() == key);
        for length in 2..golden.len() {
            assert!(
                RecordKey::parse(version, &golden[2..length]) == Err(BrokerError::InvalidString),
                "truncated to {length}: {golden:?}"
            );
        }
    }
}

#[test]
fn invalid_versions_precede_string_reads_and_helper_errors_propagate() {
    assert!(
        RecordKey::parse(99, &[])
            == Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown fixture key version"
            )))
    );
    for malformed in [&b"\xff\xff"[..], &b"\x00\x01\xff"[..], &b"\x00\x02g"[..]] {
        assert!(RecordKey::parse(GROUP, malformed) == Err(BrokerError::InvalidString));
    }
    let too_long = RecordKey::RegularExpression {
        group_id: "g".into(),
        regex: "x".repeat(32768),
    };
    assert!(too_long.encode() == Err(BrokerError::InvalidString));
}
