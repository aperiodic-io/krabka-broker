//! The byte codec for one spooled record, the length-prefixed frame that
//! carries it, and the version header every file in the spool directory
//! starts with.
//!
//! ## File header
//!
//! Each of `audit.spool`, `audit.losses`, `audit.replay-offset` and
//! `audit.replay-poison` starts with [`FILE_MAGIC`] and then
//! [`FORMAT_VERSION`] as a big-endian `i16`. The `i16` is the version marker
//! the 1.x on-disk contract uses everywhere. The magic is there because a
//! version alone cannot tell a 1.x file from a 0.x one here: a 0.x
//! `audit.spool` starts with a `u32` frame length and a 0.x
//! `audit.replay-poison` with a `u64` offset, so both almost always start with
//! `00 00`, which is version 0. Without the magic, a 0.x spool would read as a
//! 1.x spool whose first frame is torn, and `open` would cut every record off
//! as a torn tail. A file that does not start with the magic is refused as
//! predating 1.0, and one with an unknown version is refused by number.
//!
//! ## Frames
//!
//! A frame is `[u32 len][record]` and a record is `[u8 class_tag]
//! [u32 value_len][value][u32 header_count]([u32 klen][k][u32 vlen][v])*`, all
//! lengths big-endian. The codec is deliberately its own module: the bytes it
//! writes are the on-disk format, and the decoders return `None` rather than
//! panicking so that the scan can treat a short or corrupt tail as
//! end-of-data.

use crate::{
    event::AuditEventClass,
    sink::{AuditError, AuditRecord},
};

/// The four bytes every file in the audit spool directory starts with.
///
/// Part of the 1.x on-disk contract.
pub(super) const FILE_MAGIC: [u8; 4] = *b"KAUD";

/// The version of every file in the audit spool directory, written after
/// [`FILE_MAGIC`] as a big-endian `i16`.
///
/// Part of the 1.x on-disk contract. A later 1.x build that changes the
/// layout of any of these files bumps it and keeps reading this one.
pub(super) const FORMAT_VERSION: i16 = 0;

/// The length of [`file_header`].
pub(crate) const HEADER_LEN: usize = FILE_MAGIC.len() + 2;

/// The header a file in the audit spool directory starts with.
pub(super) fn file_header() -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..FILE_MAGIC.len()].copy_from_slice(&FILE_MAGIC);
    header[FILE_MAGIC.len()..].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
    header
}

/// `bytes` with its header checked and stripped.
///
/// # Errors
///
/// [`AuditError::UnsupportedSpoolFormat`] naming `file` when `bytes` does not
/// start with [`FILE_MAGIC`] and a version (`found: None`, a 0.x file), or
/// when the version is not [`FORMAT_VERSION`].
pub(super) fn strip_file_header<'a>(file: &str, bytes: &'a [u8]) -> Result<&'a [u8], AuditError> {
    let unsupported = |found| AuditError::UnsupportedSpoolFormat {
        file: file.to_owned(),
        found,
    };
    let (header, body) = bytes
        .split_at_checked(HEADER_LEN)
        .ok_or_else(|| unsupported(None))?;
    let (magic, version) = header.split_at(FILE_MAGIC.len());
    if magic != FILE_MAGIC {
        return Err(unsupported(None));
    }
    let version = i16::from_be_bytes([version[0], version[1]]);
    if version != FORMAT_VERSION {
        return Err(unsupported(Some(version)));
    }
    Ok(body)
}

pub(super) fn encode_frame(record: &AuditRecord) -> Vec<u8> {
    let body = encode_record(record);
    let len = u32::try_from(body.len()).expect("audit record fits u32");
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn encode_record(record: &AuditRecord) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(record.class.tag());
    put_bytes(&mut b, &record.value);
    let hc = u32::try_from(record.headers.len()).expect("header count fits u32");
    b.extend_from_slice(&hc.to_be_bytes());
    for (k, v) in &record.headers {
        put_bytes(&mut b, k.as_bytes());
        put_bytes(&mut b, v);
    }
    b
}

fn put_bytes(b: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field fits u32");
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(bytes);
}

pub(super) fn decode_record(mut b: &[u8]) -> Option<AuditRecord> {
    let class = AuditEventClass::from_tag(*b.first()?)?;
    b = &b[1..];
    let value = take_bytes(&mut b)?;
    let hc = usize::try_from(take_u32(&mut b)?).unwrap_or(0);
    let mut headers = Vec::with_capacity(hc);
    for _ in 0..hc {
        let k = take_bytes(&mut b)?;
        let v = take_bytes(&mut b)?;
        headers.push((String::from_utf8(k).ok()?, v));
    }
    Some(AuditRecord {
        class,
        value,
        headers,
    })
}

fn take_u32(b: &mut &[u8]) -> Option<u32> {
    if b.len() < 4 {
        return None;
    }
    let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(n)
}

fn take_bytes(b: &mut &[u8]) -> Option<Vec<u8>> {
    let len = usize::try_from(take_u32(b)?).unwrap_or(0);
    if b.len() < len {
        return None;
    }
    let out = b[..len].to_vec();
    *b = &b[len..];
    Some(out)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Four bytes is the boundary: exactly four is a readable `u32`, and fewer
    /// must yield `None` rather than index past the end.
    #[yare::parameterized(
        empty = { &[][..], None, 0 },
        one_byte = { &[0x00][..], None, 1 },
        three_bytes = { &[0x00, 0x00, 0x01][..], None, 3 },
        exact_four_bytes = { &[0x00, 0x00, 0x01, 0x00][..], Some(256), 0 },
        one_trailing_byte = { &[0x00, 0x00, 0x01, 0x00, 0xff][..], Some(256), 1 },
    )]
    fn take_u32_reads_at_the_four_byte_boundary(
        input: &'static [u8],
        want: Option<u32>,
        want_left: usize,
    ) {
        let mut b: &[u8] = input;
        let got = take_u32(&mut b);
        check!((got, b.len()) == (want, want_left));
    }
}
