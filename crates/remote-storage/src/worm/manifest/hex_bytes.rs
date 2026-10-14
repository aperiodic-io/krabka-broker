//! The byte values a manifest serialises as a lowercase hex string.
//!
//! `Sha256Digest` fixes a 32-byte digest and `HexBytes` carries a key or a
//! signature, whose length the type system cannot pin. Both share the hex
//! codec in this file, and `ChainHead` uses it too.

use {
    hex::FromHex as _,
    serde::{Deserialize, Deserializer, Serializer, de},
    sha2::{Digest, Sha256},
};

/// `SHA-256` digest of one archived object's body.
///
/// Serialises as a lowercase hex string.
#[krabka_macros::hex_bytes_identity]
#[derive(Copy)]
pub struct Sha256Digest(
    #[serde(
        serialize_with = "serialize_hex",
        deserialize_with = "deserialize_hex32"
    )]
    pub [u8; 32],
);

impl Sha256Digest {
    /// Digest of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self { Self(Sha256::digest(bytes).into()) }
}

/// Variable-length bytes that serialise as a lowercase hex string.
///
/// Used for the public key and the signature, neither of which has a fixed
/// length the type system can pin.
#[krabka_macros::hex_bytes_identity]
pub struct HexBytes(
    #[serde(serialize_with = "serialize_hex", deserialize_with = "deserialize_hex")] pub Vec<u8>,
);

pub(super) fn serialize_hex<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&hex::encode(bytes))
}

pub(super) fn deserialize_hex32<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    <[u8; 32]>::from_hex(&text)
        .map_err(|_| de::Error::custom(format!("expected 64 hex characters, got `{text}`")))
}

fn deserialize_hex<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    hex::decode(&text).map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::worm::manifest::ChainHead, assert2::check};

    #[test]
    fn byte_identities_keep_their_hex_json_and_display() {
        let digest = Sha256Digest([0x12; 32]);
        let encoded = format!("\"{}\"", "12".repeat(32));
        check!(serde_json::to_string(&digest).unwrap() == encoded);
        check!(serde_json::from_str::<Sha256Digest>(&encoded).unwrap() == digest);
        check!(digest.to_string() == "12".repeat(32));
        check!(format!("{digest:?}") == format!("Sha256Digest({})", "12".repeat(32)));

        let bytes = HexBytes(vec![0, 0xa5, 0xff]);
        check!(serde_json::to_string(&bytes).unwrap() == "\"00a5ff\"");
        check!(serde_json::from_str::<HexBytes>("\"00a5ff\"").unwrap() == bytes);
        check!(bytes.to_string() == "00a5ff");
        check!(format!("{bytes:?}") == "HexBytes(00a5ff)");
    }

    #[test]
    fn hex_newtypes_round_trip_and_format() {
        let head = ChainHead([0xa1; 32]);
        let encoded = serde_json::to_string(&head).unwrap();
        check!(encoded == format!("\"{}\"", "a1".repeat(32)));
        check!(serde_json::from_str::<ChainHead>(&encoded).unwrap() == head);
        check!(head.to_string() == "a1".repeat(32));
        check!(format!("{head:?}") == format!("ChainHead({})", "a1".repeat(32)));

        let digest = Sha256Digest::of(b"body");
        check!(
            serde_json::from_str::<Sha256Digest>(&serde_json::to_string(&digest).unwrap()).unwrap()
                == digest
        );
        check!(format!("{digest:?}") == format!("Sha256Digest({digest})"));

        check!(HexBytes(vec![0xde, 0xad]).to_string() == "dead");
        check!(format!("{:?}", HexBytes(vec![0xde, 0xad])) == "HexBytes(dead)");
    }

    #[yare::parameterized(
        empty = { "\"\"".to_owned() },
        too_short = { format!("\"{}\"", "00".repeat(31)) },
        too_long = { format!("\"{}\"", "00".repeat(33)) },
        odd_length = { format!("\"{}0\"", "00".repeat(31)) },
        non_hex = { format!("\"{}\"", "zz".repeat(32)) },
        not_a_string = { "12345".to_owned() },
        null = { "null".to_owned() },
        array = { "[]".to_owned() },
    )]
    fn fixed_width_hex_newtypes_reject_bad_input_without_panicking(json: String) {
        check!(serde_json::from_str::<ChainHead>(&json).is_err());
        check!(serde_json::from_str::<Sha256Digest>(&json).is_err());
    }

    #[yare::parameterized(
        empty = { "\"\"", Some(Vec::new()) },
        even_length_hex = { "\"00ff10\"", Some(vec![0x00, 0xff, 0x10]) },
        odd_length = { "\"abc\"", None },
        non_hex = { "\"zz\"", None },
        not_a_string = { "42", None },
        null = { "null", None },
    )]
    fn hex_bytes_decode_cases_are_independent(json: &str, expected: Option<Vec<u8>>) {
        check!(serde_json::from_str::<HexBytes>(json).ok().map(|h| h.0) == expected);
    }
}
