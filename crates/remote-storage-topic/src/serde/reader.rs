//! Unsigned-varint writing and a bounds-checked byte cursor.
//!
//! These helpers are retained because `snapshot.rs` uses them for its own
//! envelope framing (format-version, committed-offsets, entry-length
//! prefixes). They are NOT part of the [`MetadataEvent`] wire format.
//!
//! [`MetadataEvent`]: super::MetadataEvent

use {
    crate::error::CodecError,
    bytes::{BufMut, BytesMut},
};

/// Unsigned LEB128: 7 data bits per byte, and the MSB is the continuation
/// flag. It encodes 1 byte for values < 128.
pub(crate) fn write_uvarint(mut v: u64, buf: &mut BytesMut) {
    while v >= 0x80 {
        let byte = u8::try_from(v & 0x7F).expect("varint payload is seven bits");
        buf.put_u8(byte | 0x80);
        v >>= 7;
    }
    buf.put_u8(u8::try_from(v).expect("final varint byte is less than 128"));
}

pub(crate) fn read_uvarint(r: &mut Reader<'_>) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    for shift in (0..10).map(|i| i * 7) {
        let byte = r.read_u8()?;
        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(CodecError::LengthOverflow(u64::MAX))
}

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self { Self { buf, pos: 0 } }

    /// Bytes not yet consumed.
    pub(crate) fn remaining(&self) -> usize { self.buf.len() - self.pos }

    pub(crate) fn read_u8(&mut self) -> Result<u8, CodecError> {
        let &b = self
            .buf
            .get(self.pos)
            .ok_or(CodecError::UnexpectedEof(self.pos))?;
        self.pos += 1;
        Ok(b)
    }

    pub(crate) fn read_i32(&mut self) -> Result<i32, CodecError> {
        let bytes: [u8; 4] = self
            .read_n(4)?
            .try_into()
            .expect("read_n returned exact length");
        Ok(i32::from_be_bytes(bytes))
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64, CodecError> {
        let bytes: [u8; 8] = self
            .read_n(8)?
            .try_into()
            .expect("read_n returned exact length");
        Ok(i64::from_be_bytes(bytes))
    }

    pub(crate) fn read_n(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(CodecError::LengthOverflow(n as u64))?;
        if end > self.buf.len() {
            return Err(CodecError::UnexpectedEof(self.pos));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}
