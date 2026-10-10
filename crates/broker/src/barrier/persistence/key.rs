//! The key of every `__barrier_state` record, and the kind tag inside it.
//!
//! The key is the one part of the format that all three record kinds share, so
//! the kind tag, the key struct, and the key codec live together here.

use super::{NO_EPOCH, primitives as codec};

/// Which of the three record kinds a key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    /// A barrier group definition.
    Group,
    /// The frozen target set of one injection, written before the first
    /// marker append.
    InjectionStart,
    /// The published offsets of one epoch.
    Cut,
}

impl RecordKind {
    /// The `i16` that this kind writes into a key.
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::Group => 0,
            Self::InjectionStart => 1,
            Self::Cut => 2,
        }
    }
}

impl TryFrom<i16> for RecordKind {
    type Error = codec::ProtocolError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Group),
            1 => Ok(Self::InjectionStart),
            2 => Ok(Self::Cut),
            _ => Err(codec::ProtocolError::InvalidValue(
                "unknown barrier record kind",
            )),
        }
    }
}

/// The key of any `__barrier_state` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordKey {
    pub(crate) kind: RecordKind,
    pub(crate) group: String,
    /// The epoch this record belongs to, or [`NO_EPOCH`] for a group record.
    pub(crate) epoch: i64,
}

impl RecordKey {
    /// The key of the group record for `group`.
    pub(crate) fn group(group: impl Into<String>) -> Self {
        Self {
            kind: RecordKind::Group,
            group: group.into(),
            epoch: NO_EPOCH,
        }
    }

    /// The key of the injection-start record for one epoch.
    pub(crate) fn injection_start(group: impl Into<String>, epoch: i64) -> Self {
        Self {
            kind: RecordKind::InjectionStart,
            group: group.into(),
            epoch,
        }
    }

    /// The key of the cut record for one epoch.
    pub(crate) fn cut(group: impl Into<String>, epoch: i64) -> Self {
        Self {
            kind: RecordKind::Cut,
            group: group.into(),
            epoch,
        }
    }
}

/// Encode a record key.
///
/// # Errors
/// Returns [`codec::BrokerError::Protocol`] when the group name is longer than 32767
/// bytes, which the `i16` length cannot carry.
pub(crate) fn encode_key(key: &RecordKey) -> Result<Vec<u8>, codec::BrokerError> {
    let mut out = Vec::with_capacity(12 + key.group.len());
    codec::put_i16(&mut out, codec::RECORD_VERSION);
    codec::put_i16(&mut out, key.kind.code());
    codec::put_string(&mut out, &key.group)?;
    codec::put_i64(&mut out, key.epoch);
    Ok(out)
}

/// Decode a record key.
///
/// # Errors
/// Returns a [`codec::ProtocolError`] when the key is truncated, carries a version
/// other than [`codec::RECORD_VERSION`], names an unknown record kind, holds a
/// non-UTF-8 group name, or has trailing bytes.
pub(crate) fn decode_key(bytes: &[u8]) -> Result<RecordKey, codec::ProtocolError> {
    let mut cur = bytes;
    codec::expect_version(&mut cur)?;
    let kind = RecordKind::try_from(codec::get_i16(&mut cur)?)?;
    let group = codec::get_string_owned(&mut cur)?;
    let epoch = codec::get_i64(&mut cur)?;
    codec::expect_end(cur)?;
    Ok(RecordKey { kind, group, epoch })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::persistence::MAX_STRING_BYTES;

    #[test]
    fn every_key_kind_round_trips() {
        let cases = [
            ("group", RecordKey::group("orders-cut")),
            (
                "injection start",
                RecordKey::injection_start("orders-cut", 7),
            ),
            ("cut", RecordKey::cut("orders-cut", 7)),
        ];
        for (case, key) in cases {
            let decoded = decode_key(&encode_key(&key).expect("encodes")).ok();
            assert!(decoded.as_ref() == Some(&key), "{case}");
        }
    }

    #[test]
    fn a_group_key_carries_no_epoch() {
        let key = RecordKey::group("orders-cut");
        assert!(key.epoch == NO_EPOCH);
        assert!(decode_key(&encode_key(&key).expect("encodes")).ok() == Some(key));
    }

    #[test]
    fn a_key_rejects_an_unknown_record_kind() {
        let mut bytes = encode_key(&RecordKey::cut("orders-cut", 7)).expect("encodes");
        bytes[3] = 9;
        assert!(decode_key(&bytes).is_err());
    }

    /// The group name is a string with an `i16` length. The encoder writes one
    /// of 32767 bytes and refuses a longer one with an error, because a panic
    /// here would take down the request that named the group.
    #[test]
    fn a_group_name_of_32767_bytes_encodes_and_one_of_32768_is_refused() {
        for (length, encodes) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let key = RecordKey::cut("g".repeat(length), 7);
            let encoded = encode_key(&key);
            assert!(encoded.is_ok() == encodes, "{length} bytes");
            if let Ok(bytes) = encoded {
                assert!(decode_key(&bytes).ok() == Some(key), "{length} bytes");
            }
        }
    }

    #[test]
    fn a_kind_survives_its_wire_code() {
        for kind in [
            RecordKind::Group,
            RecordKind::InjectionStart,
            RecordKind::Cut,
        ] {
            assert!(RecordKind::try_from(kind.code()).ok() == Some(kind));
        }
    }
}
