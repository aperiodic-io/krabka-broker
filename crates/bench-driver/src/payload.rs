//! Record-payload generator. The first 24 bytes of every produced record
//! hold `(magic_be, scenario_id_be, send_unix_nanos_be)`, so a consumer can
//! compute the end-to-end latency when it re-reads the embedded
//! `send_unix_nanos`. The remaining bytes are a deterministic filler, so
//! the wire size is exactly the scenario's message size.
//!
//! The header is 24 bytes and not the 16 the plan sketched, because it needs a
//! magic to detect "this is one of ours". Kafka's own producers leave their own
//! headers in there, and this driver must not misread their bytes.

use {
    crate::numeric::saturating_u128_to_u64,
    bytes::{Bytes, BytesMut},
    krabka_units::prelude::*,
    std::time::{SystemTime, UNIX_EPOCH},
};

/// Magic prefix on every record, so a consumer can confirm that this driver
/// produced the record and that it is not data already in the topic.
pub const MAGIC: [u8; 8] = *b"KRABKA_B";
pub const HEADER_LEN: usize = MAGIC.len() + 8 + 8; // magic + scenario_id + send_nanos = 24

/// Builds a reusable filler template of exactly `msg_size` bytes, or of the
/// header length if that is larger. The first 24 bytes are zero, and
/// `stamp_into` overwrites them at send time. The remaining bytes are a
/// repeating pattern.
#[must_use]
pub fn template(msg_size: ByteSize) -> BytesMut {
    let len = msg_size.bytes_usize().max(HEADER_LEN);
    let mut b = BytesMut::with_capacity(len);
    b.resize(len, 0u8);
    // Fill the body with a repeating ramp so compression has *some* work.
    // All-zeros compresses too well; all-random compresses too poorly.
    for (i, byte) in b.iter_mut().enumerate().skip(HEADER_LEN) {
        *byte = u8::try_from(i & 0xff).unwrap_or_default();
    }
    b
}

/// Builds a record key of exactly `key_size` bytes, distinct per `sequence`.
///
/// A scenario that sets `key_size` asks for a keyed workload, and a keyed
/// workload is not a keyless one: the partitioner hashes the key instead of
/// round-robining, and the key adds its own bytes to the wire. The key has to
/// vary, or every record hashes to one partition and the run measures a single
/// partition rather than the scenario's partition count.
///
/// The first eight bytes are the big-endian sequence, so keys are distinct and
/// spread. The rest is the same repeating ramp the value filler uses, so a
/// compressed batch is not dominated by a run of zeros. A `key_size` below
/// eight bytes is honoured as given and takes a prefix of the sequence.
#[must_use]
pub fn key(key_size: ByteSize, sequence: u64) -> Option<Bytes> {
    let len = key_size.bytes_usize();
    if len == 0 {
        return None;
    }
    let mut buf = BytesMut::with_capacity(len);
    buf.resize(len, 0u8);
    let stamped = len.min(8);
    buf[..stamped].copy_from_slice(&sequence.to_be_bytes()[..stamped]);
    for (index, byte) in buf.iter_mut().enumerate().skip(stamped) {
        *byte = u8::try_from(index & 0xff).unwrap_or_default();
    }
    Some(buf.freeze())
}

/// Stamps the magic, the `scenario_id`, and the current `unix_nanos` into the
/// first 24 bytes of `buf`. Returns the value as a `Bytes`, which the caller
/// clones cheaply with a `BytesMut::freeze`-style copy.
pub fn stamp_into(buf: &mut BytesMut, scenario_id: u64) -> Bytes {
    assert2::debug_assert!(buf.len() >= HEADER_LEN, "buf too short for header");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| saturating_u128_to_u64(d.as_nanos()));
    buf[..MAGIC.len()].copy_from_slice(&MAGIC);
    buf[8..16].copy_from_slice(&scenario_id.to_be_bytes());
    buf[16..24].copy_from_slice(&nanos.to_be_bytes());
    // Freeze a copy into a Bytes the producer can keep.
    Bytes::copy_from_slice(buf)
}

/// Reads the embedded `send_unix_nanos` if the record is one of ours.
/// Returns `None` if the magic does not match, and also if the record is too
/// short. The consumer skips both cases silently.
#[must_use]
pub fn read_send_nanos(value: &[u8], scenario_id: u64) -> Option<u64> {
    if value.len() < HEADER_LEN || value[..MAGIC.len()] != MAGIC {
        return None;
    }
    let sid = u64::from_be_bytes(value[8..16].try_into().ok()?);
    if sid != scenario_id {
        return None;
    }
    Some(u64::from_be_bytes(value[16..24].try_into().ok()?))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn a_zero_key_size_means_a_keyless_record() {
        assert2::assert!(key(ByteSize::ZERO, 7).is_none());
    }

    #[test]
    fn a_key_has_the_configured_size_and_varies_per_record() {
        let first = key(bytes(16), 1).expect("a sized key");
        let second = key(bytes(16), 2).expect("a sized key");
        assert2::assert!(first.len() == 16);
        assert2::assert!(second.len() == 16);
        assert2::assert!(first != second);
        assert2::assert!(first[..8] == 1u64.to_be_bytes());
    }

    #[test]
    fn a_key_shorter_than_the_sequence_is_still_the_configured_size() {
        let short = key(bytes(4), 0x0102_0304_0506_0708).expect("a sized key");
        assert2::assert!(short.len() == 4);
        assert2::assert!(short[..] == [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn round_trip_send_nanos() {
        let mut t = template(bytes(64));
        let b = stamp_into(&mut t, 0xdead_beef);
        let n = read_send_nanos(&b, 0xdead_beef).expect("magic+sid match");
        assert2::assert!(n > 0);
    }

    #[test]
    fn rejects_wrong_scenario_id() {
        let mut t = template(bytes(64));
        let b = stamp_into(&mut t, 42);
        assert2::assert!(read_send_nanos(&b, 7).is_none());
    }

    #[test]
    fn rejects_short() {
        assert2::assert!(read_send_nanos(&[0u8; 8], 0).is_none());
    }

    #[test]
    fn rejects_bad_magic() {
        let mut b = vec![0u8; HEADER_LEN];
        b[16..24].copy_from_slice(&123u64.to_be_bytes());
        assert2::assert!(read_send_nanos(&b, 0).is_none());
    }

    #[test]
    fn template_size_honoured_above_header() {
        assert2::assert!(template(kibibytes(1)).len() == 1024);
        assert2::assert!(template(bytes(512)).len() == 512);
    }

    #[test]
    fn template_min_size_is_header() {
        assert2::assert!(template(ByteSize::ZERO).len() == HEADER_LEN);
    }
}
