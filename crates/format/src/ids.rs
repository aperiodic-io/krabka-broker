//! The two identity UUIDs that `format` writes, in Kafka's string form.
//!
//! `format` makes or accepts a **cluster id**, and makes a **directory id** for
//! each log directory. The directory id of the metadata directory is also the
//! node's KIP-853 voter identity. Both ids go into `meta.properties`, and the
//! cluster id also goes into the bootstrap manifest.
//!
//! Kafka prints an `org.apache.kafka.common.Uuid` as 22 characters of unpadded
//! base64url, for example `AQIDBAUGBwgJCgsMDQ4PEA`, not as the hyphenated hex
//! form that [`uuid::Uuid`] prints. [`ClusterId`] and [`DirectoryId`] use the
//! Kafka form for [`Display`](core::fmt::Display), [`FromStr`](core::str::FromStr),
//! and serde. An id that `format` writes can thus go into a Kafka tool, and an
//! id from a Kafka cluster can go into `format`.
//!
//! [`FromStr`](core::str::FromStr) accepts the same strings as Kafka 4.3.1's
//! `Uuid.fromString`:
//! 22 base64url characters, optionally followed by `==`, with the unused low
//! bits of the last character ignored. Parsing does not refuse the two
//! reserved ids, because Kafka does not: `kafka-storage format -t
//! AAAAAAAAAAAAAAAAAAAAAA` succeeds on `apache/kafka:4.3.1`. The command line
//! also accepts the 36-character hyphenated form, through
//! [`ClusterId::parse_cli`] and [`DirectoryId::parse_cli`]. A file is read in
//! the Kafka form only.
//!
//! Both ids wrap a `uuid::Uuid`, so a bare-`Uuid` signature such as
//! `write_meta_properties(_, cluster, dir)` would compile with the two
//! arguments transposed. A distinct newtype for each id makes the compiler
//! reject that mix-up.

use {
    base64::{
        Engine as _,
        alphabet::URL_SAFE,
        engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    },
    derive_more::{From, Into},
    thiserror::Error,
    uuid::Uuid,
};

/// The longest input, in UTF-16 code units, that Kafka tries to decode.
const MAX_ENCODED_LEN: usize = 24;

/// Unpadded base64url that, as the JDK decoder does, ignores the unused low
/// bits of the last character.
const KAFKA_BASE64: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone),
);

/// A Kafka cluster id: the identity that every node of a cluster shares.
///
/// This id is distinct from [`DirectoryId`], which is per log directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, From, Into, krabka_macros::KafkaUuid)]
pub struct ClusterId(pub Uuid);

/// A log directory's stable id (KIP-858). The metadata directory's id is also
/// the node's KIP-853 voter identity.
///
/// `format` writes this id to `meta.properties`, and the broker reads it back
/// on every boot. The type is distinct from [`ClusterId`], so a call site
/// cannot transpose the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, From, Into, krabka_macros::KafkaUuid)]
pub struct DirectoryId(pub Uuid);

impl ClusterId {
    /// Returns a random id, as Kafka's `Uuid.randomUuid` does.
    ///
    /// The result is never one of Kafka's two reserved ids, and its string
    /// form never starts with `-`, which a command-line parser would read as
    /// an option.
    #[must_use]
    pub fn random() -> Self { Self(random_uuid()) }

    /// Parses a command-line value: the Kafka form, or the hyphenated form.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaUuidError::Hyphenated`] for an input that has the shape
    /// of the hyphenated form but is not hex. Otherwise returns the error of
    /// [`FromStr`](core::str::FromStr).
    pub fn parse_cli(input: &str) -> Result<Self, KafkaUuidError> {
        parse_kafka_or_hyphenated(input).map(Self)
    }
}

impl DirectoryId {
    /// Returns a random directory id, as Kafka's `DirectoryId.random` does.
    ///
    /// The result obeys the rules of [`ClusterId::random`], and it is never
    /// one of the 100 lowest ids, which Kafka reserves as directory-id
    /// sentinels.
    #[must_use]
    pub fn random() -> Self {
        Self(first_accepted(|id| {
            is_reserved_directory_id(id) || starts_with_dash(id)
        }))
    }

    /// Returns a random directory id that is not in `used`, as Kafka's
    /// `MetaPropertiesEnsemble.Copier.generateValidDirectoryId` returns one
    /// that no other directory of the node holds.
    #[must_use]
    pub fn random_unused(used: &[Self]) -> Self {
        loop {
            let id = Self::random();
            if !used.contains(&id) {
                return id;
            }
        }
    }

    /// Returns `true` for one of the 100 lowest ids, which Kafka reserves as
    /// directory ids (Kafka's `DirectoryId.reserved`).
    #[must_use]
    pub fn is_reserved(self) -> bool { is_reserved_directory_id(self.0) }

    /// Parses a command-line value: the Kafka form, or the hyphenated form.
    ///
    /// # Errors
    ///
    /// As [`ClusterId::parse_cli`].
    pub fn parse_cli(input: &str) -> Result<Self, KafkaUuidError> {
        parse_kafka_or_hyphenated(input).map(Self)
    }
}

/// Returns a random id, as Kafka's `Uuid.randomUuid` does: never the zero id
/// or the id with value one, and never one whose string form starts with `-`,
/// which a command-line parser would read as an option.
///
/// [`ClusterId::random`] uses it. So does the broker for a new topic id, which
/// Kafka's `ReplicationControlManager.createTopic` draws the same way.
#[must_use]
pub fn random_uuid() -> Uuid { first_accepted(|id| is_reserved(id) || starts_with_dash(id)) }

/// Kafka's `Uuid.RESERVED`: the zero id and the id with value one.
fn is_reserved(id: Uuid) -> bool { id.as_u128() <= 1 }

/// Kafka's `DirectoryId.reserved`: the most significant half is zero, and the
/// least significant half, as a signed integer, is below 100.
fn is_reserved_directory_id(id: Uuid) -> bool {
    let (most, least) = id.as_u64_pair();
    most == 0 && least.cast_signed() < 100
}

/// Returns `true` when the first base64url character is `-`, which stands for
/// the six-bit value 62.
fn starts_with_dash(id: Uuid) -> bool { id.as_bytes()[0] >> 2 == 62 }

/// Draws version 4 ids until one is not rejected.
fn first_accepted(rejected: impl Fn(Uuid) -> bool) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if !rejected(candidate) {
            return candidate;
        }
    }
}

/// Kafka's `Uuid.toString`: unpadded base64url of the 16 bytes.
fn encode(id: Uuid) -> String { KAFKA_BASE64.encode(id.as_bytes()) }

/// Kafka's `Uuid.fromString`.
///
/// Kafka accepts exactly 22 base64url characters, optionally followed by
/// `==`. It measures the length of a Java string, in UTF-16 code units, and
/// refuses more than 24 before it decodes anything.
fn decode(input: &str) -> Result<Uuid, KafkaUuidError> {
    if input.encode_utf16().count() > MAX_ENCODED_LEN {
        return Err(KafkaUuidError::TooLong {
            prefix: java_prefix(input, MAX_ENCODED_LEN),
        });
    }
    let unpadded = input.strip_suffix("==").unwrap_or(input);
    let bytes = KAFKA_BASE64
        .decode(unpadded)
        .map_err(|error| KafkaUuidError::NotBase64 {
            input: input.to_owned(),
            reason: error.to_string(),
        })?;
    if bytes.len() != 16 {
        return Err(KafkaUuidError::WrongLength {
            input: input.to_owned(),
            decoded_len: bytes.len(),
        });
    }
    Ok(Uuid::from_slice(&bytes).expect("22 base64 characters decode to 16 bytes"))
}

/// Parses the hyphenated form when the input has its shape, and the Kafka
/// form otherwise.
fn parse_kafka_or_hyphenated(input: &str) -> Result<Uuid, KafkaUuidError> {
    let bytes = input.as_bytes();
    if bytes.len() == 36 && [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-') {
        return Uuid::try_parse(input).map_err(|error| KafkaUuidError::Hyphenated {
            input: input.to_owned(),
            reason: error.to_string(),
        });
    }
    decode(input)
}

/// Returns the first `max` UTF-16 code units of `input`, as Java's
/// `substring(0, max)` does. A cut that splits a surrogate pair keeps the lone
/// high surrogate, which the JVM prints as `?`.
fn java_prefix(input: &str, max: usize) -> String {
    let mut prefix = String::new();
    let mut units = 0;
    for c in input.chars() {
        let width = c.len_utf16();
        if units + width > max {
            if units < max {
                prefix.push('?');
            }
            break;
        }
        prefix.push(c);
        units += width;
    }
    prefix
}

/// A string that is not a Kafka `Uuid`.
///
/// [`TooLong`](Self::TooLong) and [`WrongLength`](Self::WrongLength) carry the
/// message of the `IllegalArgumentException` that Kafka's `Uuid.fromString`
/// throws for the same input. Kafka's other messages come from
/// `java.util.Base64`; this type names the problem in its own words there.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KafkaUuidError {
    /// The input is longer than 24 UTF-16 code units.
    #[error("Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID")]
    TooLong {
        /// The first 24 UTF-16 code units of the input.
        prefix: String,
    },

    /// The input is base64url, but not of 16 bytes.
    #[error(
        "Input string `{input}` decoded as {decoded_len} bytes, which is not equal to the \
         expected 16 bytes of a base64-encoded UUID"
    )]
    WrongLength {
        /// The input.
        input: String,
        /// The number of bytes that the input decodes to.
        decoded_len: usize,
    },

    /// The input is not base64url.
    #[error("Input string `{input}` is not base64url: {reason}")]
    NotBase64 {
        /// The input.
        input: String,
        /// What the decoder refused.
        reason: String,
    },

    /// The input has the shape of the hyphenated hex form but is not hex.
    /// Only the `parse_cli` functions return it.
    #[error("Input string `{input}` is not a valid hyphenated UUID: {reason}")]
    Hyphenated {
        /// The input.
        input: String,
        /// What the hex parser refused.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Every accepted input renders as the string that
    /// `org.apache.kafka.common.Uuid` in `kafka-clients-4.3.1.jar` prints for
    /// it, and every input Kafka refuses is refused.
    #[yare::parameterized(
        kafka_cluster = { "AQIDBAUGBwgJCgsMDQ4PEA", Ok("AQIDBAUGBwgJCgsMDQ4PEA") },
        kafka_directory = { "U-hCdTzaSPyFB3swwc7Qdw", Ok("U-hCdTzaSPyFB3swwc7Qdw") },
        reserved_zero = { "AAAAAAAAAAAAAAAAAAAAAA", Ok("AAAAAAAAAAAAAAAAAAAAAA") },
        reserved_one = { "AAAAAAAAAAAAAAAAAAAAAQ", Ok("AAAAAAAAAAAAAAAAAAAAAQ") },
        ignored_low_bits = { "_____________________x", Ok("_____________________w") },
        rounded_low_bits = { "AAAAAAAAAAAAAAAAAAAAAB", Ok("AAAAAAAAAAAAAAAAAAAAAA") },
        padded = { "AAAAAAAAAAAAAAAAAAAAAA==", Ok("AAAAAAAAAAAAAAAAAAAAAA") },
        hyphenated_is_too_long = {
            "01020304-0506-0708-090a-0b0c0d0e0f10",
            Err(
                "Input string with prefix `01020304-0506-0708-090a-` is too long to be decoded as a base64 UUID",
            ),
        },
        unicode_too_long = {
            "AAAAAAAAAAAAAAAAAAAAAAA😀",
            Err(
                "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAA?` is too long to be decoded as a base64 UUID",
            ),
        },
        twenty_four_chars = {
            "AAAAAAAAAAAAAAAAAAAAAAAA",
            Err(
                "Input string `AAAAAAAAAAAAAAAAAAAAAAAA` decoded as 18 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            ),
        },
        empty = {
            "",
            Err(
                "Input string `` decoded as 0 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            ),
        },
        short = {
            "AAAAAA",
            Err(
                "Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            ),
        },
        bad_padding_length = {
            "AAAAAAAAAAAAAAAAAAAA==",
            Err(
                "Input string `AAAAAAAAAAAAAAAAAAAA==` decoded as 15 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            ),
        },
        twenty_one_chars = {
            "AAAAAAAAAAAAAAAAAAAAA",
            Err("Input string `AAAAAAAAAAAAAAAAAAAAA` is not base64url: Invalid input length: 21"),
        },
        invalid_plus = {
            "AAAAAAAAAAAAAAAAAAAA+A",
            Err("Input string `AAAAAAAAAAAAAAAAAAAA+A` is not base64url: Invalid symbol 43, offset 20."),
        },
        bad_padding = {
            "AAAAAAAAAAAAAAAAAAAAAA=",
            Err("Input string `AAAAAAAAAAAAAAAAAAAAAA=` is not base64url: Invalid padding"),
        },
        non_ascii = {
            "AAAAAAAAAAAAAAAAAAAAAé",
            Err("Input string `AAAAAAAAAAAAAAAAAAAAAé` is not base64url: Invalid symbol 195, offset 21."),
        },
    )]
    fn parse_and_render_match_kafka_uuid(input: &str, want: Result<&str, &str>) {
        let want = want.map(str::to_owned).map_err(str::to_owned);
        let as_cluster = input
            .parse::<ClusterId>()
            .map(|id| id.to_string())
            .map_err(|e| e.to_string());
        assert2::check!(as_cluster == want, "{input:?}");
        let as_directory = input
            .parse::<DirectoryId>()
            .map(|id| id.to_string())
            .map_err(|e| e.to_string());
        assert2::check!(as_directory == want, "{input:?} as a directory id");
    }

    /// The command line also takes the hyphenated form, and only a string of
    /// exactly that shape is read as hex.
    #[yare::parameterized(
        kafka_form = { "AQIDBAUGBwgJCgsMDQ4PEA" },
        lowercase_hex = { "01020304-0506-0708-090a-0b0c0d0e0f10" },
        uppercase_hex = { "01020304-0506-0708-090A-0B0C0D0E0F10" },
    )]
    fn the_command_line_accepts_both_forms(input: &str) {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::check!(ClusterId::parse_cli(input) == Ok(ClusterId(id)), "{input}");
        assert2::check!(
            DirectoryId::parse_cli(input) == Ok(DirectoryId(id)),
            "{input}"
        );
    }

    #[test]
    fn the_command_line_refuses_malformed_hex_and_other_uuid_spellings() {
        let not_hex = "01020304-0506-0708-090a-0b0c0d0e0f1g";
        check!(matches!(
            ClusterId::parse_cli(not_hex),
            Err(KafkaUuidError::Hyphenated { input, .. }) if input == not_hex
        ));
        // The other spellings `uuid` accepts are not the hyphenated form.
        check!(matches!(
            ClusterId::parse_cli("0102030405060708090a0b0c0d0e0f10"),
            Err(KafkaUuidError::TooLong { .. })
        ));
    }

    /// Serde carries the `Display` string and refuses the hyphenated form, so
    /// a file holds only the Kafka form.
    #[test]
    fn serde_uses_the_kafka_string() {
        let id = ClusterId(Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10));
        check!(serde_json::to_string(&id).unwrap() == r#""AQIDBAUGBwgJCgsMDQ4PEA""#);
        check!(serde_json::from_str::<ClusterId>(r#""AQIDBAUGBwgJCgsMDQ4PEA""#).unwrap() == id);
        check!(
            serde_json::from_str::<DirectoryId>(r#""01020304-0506-0708-090a-0b0c0d0e0f10""#)
                .is_err()
        );
    }

    /// The generators skip what Kafka's generators skip.
    #[test]
    fn random_ids_obey_kafka_generation_rules() {
        // One draw in 64 starts with a dash before the rule applies, so
        // 5000 draws would miss a missing rule with a chance of e^-78.
        for _ in 0..5000 {
            let plain = random_uuid();
            check!(!is_reserved(plain));
            check!(!encode(plain).starts_with('-'));
        }
        for _ in 0..500 {
            let cluster = ClusterId::random();
            check!(!is_reserved(cluster.0));
            check!(!cluster.to_string().starts_with('-'));
            let directory = DirectoryId::random();
            check!(!directory.is_reserved());
            check!(!directory.to_string().starts_with('-'));
        }
    }

    /// The reservation and dash predicates agree with Kafka's definitions.
    #[yare::parameterized(
        zero = { Uuid::from_u64_pair(0, 0), true, true },
        one = { Uuid::from_u64_pair(0, 1), true, true },
        two = { Uuid::from_u64_pair(0, 2), false, true },
        ninety_nine = { Uuid::from_u64_pair(0, 99), false, true },
        one_hundred = { Uuid::from_u64_pair(0, 100), false, false },
        low_bits_max = { Uuid::from_u64_pair(0, u64::MAX), false, true },
        high_bits_one = { Uuid::from_u64_pair(1, 0), false, false },
        high_and_low_one = { Uuid::from_u64_pair(1, 1), false, false },
    )]
    fn reservations_match_kafka(id: Uuid, reserved: bool, reserved_directory: bool) {
        assert2::check!(
            (is_reserved(id), is_reserved_directory_id(id)) == (reserved, reserved_directory),
            "{id}"
        );
    }

    /// Exhaustively checks each possible leading byte; this is a property
    /// sweep over the byte domain, rather than a finite set of independent ids.
    #[test]
    fn dash_check_matches_kafka_for_every_first_byte() {
        for first_byte in 0..=u8::MAX {
            let mut bytes = [0u8; 16];
            bytes[0] = first_byte;
            let id = Uuid::from_bytes(bytes);
            check!(
                starts_with_dash(id) == encode(id).starts_with('-'),
                "{first_byte:#x}"
            );
        }
    }
}
