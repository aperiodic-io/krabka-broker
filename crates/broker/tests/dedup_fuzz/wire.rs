//! Record batches as structure, and their encoding with tampered fields.
//!
//! A [`Batch`] holds what a producer means to send. [`Batch::records_bytes`]
//! writes its records the way the v2 format lays them out, except where a
//! [`Tampers`] entry says one length or number of one record should lie, be
//! padded past its canonical width, or run into the next field.
//! [`Batch::assemble`] then puts the 61-byte header in front of a payload,
//! with whatever header fields [`HeaderLies`] overrides, and sets the CRC.

use krabka_protocol::records::{Attributes, CRC_COVERAGE_START, HEADER_LEN};

/// The bytes of a v2 batch header before `batch_length` stops counting.
const LENGTH_PREFIX: usize = 12;

/// One record header: a name, which need not be UTF-8, and a nullable value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub name: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// One record as a producer means it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rec {
    pub attributes: u8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

/// One batch as a producer means it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    pub attributes: Attributes,
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Rec>,
}

/// A length or number in one record that an encoding can tamper with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    Length,
    TimestampDelta,
    OffsetDelta,
    KeyLength,
    ValueLength,
    HeaderCount,
    HeaderKeyLength(usize),
    HeaderValueLength(usize),
}

/// How a tampered field is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tamper {
    /// This number instead of the true one.
    Value(i64),
    /// The true number plus this amount.
    Shift(i64),
    /// The true number, padded with this many redundant continuation groups.
    Overlong(usize),
    /// The true number with the continuation bit left set on its last byte,
    /// so the varint runs into the next field.
    Unterminated,
}

/// The tampered fields of one encoding: record index, field, tamper.
pub type Tampers = Vec<(usize, Field, Tamper)>;

/// Header fields that lie. `None` writes the true value.
#[derive(Clone, Copy, Debug, Default)]
pub struct HeaderLies {
    pub batch_length: Option<i32>,
    pub records_count: Option<i32>,
    pub last_offset_delta: Option<i32>,
    pub magic: Option<i8>,
    /// A CRC to write instead of the computed one.
    pub crc: Option<u32>,
}

impl Batch {
    /// The records, in the v2 layout, with `tampers` applied.
    pub fn records_bytes(&self, tampers: &[(usize, Field, Tamper)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (index, record) in self.records.iter().enumerate() {
            let tamper = |field: Field| {
                tampers
                    .iter()
                    .find(|(at, tampered, _)| *at == index && *tampered == field)
                    .map(|(_, _, tamper)| *tamper)
            };
            let mut body = vec![record.attributes];
            put_var(
                &mut body,
                record.timestamp_delta,
                Width::Long,
                tamper(Field::TimestampDelta),
            );
            put_var(
                &mut body,
                record.offset_delta.into(),
                Width::Int,
                tamper(Field::OffsetDelta),
            );
            put_nullable(&mut body, record.key.as_deref(), tamper(Field::KeyLength));
            put_nullable(
                &mut body,
                record.value.as_deref(),
                tamper(Field::ValueLength),
            );
            put_var(
                &mut body,
                length(record.headers.len()),
                Width::Int,
                tamper(Field::HeaderCount),
            );
            for (at, header) in record.headers.iter().enumerate() {
                put_var(
                    &mut body,
                    length(header.name.len()),
                    Width::Int,
                    tamper(Field::HeaderKeyLength(at)),
                );
                body.extend_from_slice(&header.name);
                put_nullable(
                    &mut body,
                    header.value.as_deref(),
                    tamper(Field::HeaderValueLength(at)),
                );
            }
            put_var(
                &mut out,
                length(body.len()),
                Width::Long,
                tamper(Field::Length),
            );
            out.extend_from_slice(&body);
        }
        out
    }

    /// The batch: its header, with `lies` applied and the CRC set over the
    /// result, then `payload`.
    pub fn assemble(&self, payload: &[u8], lies: HeaderLies) -> Vec<u8> {
        let count = i32::try_from(self.records.len()).unwrap_or(i32::MAX);
        let batch_length =
            i32::try_from(HEADER_LEN - LENGTH_PREFIX + payload.len()).unwrap_or(i32::MAX);
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.extend_from_slice(&self.base_offset.to_be_bytes());
        out.extend_from_slice(&lies.batch_length.unwrap_or(batch_length).to_be_bytes());
        out.extend_from_slice(&self.partition_leader_epoch.to_be_bytes());
        out.extend_from_slice(&lies.magic.unwrap_or(2).to_be_bytes());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&self.attributes.0.to_be_bytes());
        out.extend_from_slice(
            &lies
                .last_offset_delta
                .unwrap_or(count.saturating_sub(1))
                .to_be_bytes(),
        );
        out.extend_from_slice(&self.base_timestamp.to_be_bytes());
        out.extend_from_slice(&self.max_timestamp.to_be_bytes());
        out.extend_from_slice(&self.producer_id.to_be_bytes());
        out.extend_from_slice(&self.producer_epoch.to_be_bytes());
        out.extend_from_slice(&self.base_sequence.to_be_bytes());
        out.extend_from_slice(&lies.records_count.unwrap_or(count).to_be_bytes());
        out.extend_from_slice(payload);
        let crc = lies
            .crc
            .unwrap_or_else(|| crc32c::crc32c(&out[CRC_COVERAGE_START..]));
        out[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
        out
    }
}

/// Set the CRC of the batch at the start of `wire` over everything after the
/// CRC field.
pub fn refresh_crc(wire: &mut [u8]) {
    if wire.len() < HEADER_LEN {
        return;
    }
    let crc = crc32c::crc32c(&wire[CRC_COVERAGE_START..]);
    wire[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
}

/// A length as the varint it is written as.
fn length(len: usize) -> i64 { i64::try_from(len).unwrap_or(i64::MAX) }

/// Whether a field is a varint (32 bits) or a varlong (64 bits).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Width {
    Int,
    Long,
}

/// A nullable byte string: its length, `-1` for null, then its bytes.
fn put_nullable(out: &mut Vec<u8>, bytes: Option<&[u8]>, tamper: Option<Tamper>) {
    match bytes {
        None => put_var(out, -1, Width::Int, tamper),
        Some(bytes) => {
            put_var(out, length(bytes.len()), Width::Int, tamper);
            out.extend_from_slice(bytes);
        }
    }
}

/// A zigzag varint. A value too wide for a 32-bit field is written with the
/// 64-bit zigzag, which a 32-bit reader must refuse as too long.
fn put_var(out: &mut Vec<u8>, value: i64, width: Width, tamper: Option<Tamper>) {
    let (value, padding, unterminated) = match tamper {
        None => (value, 0, false),
        Some(Tamper::Value(lie)) => (lie, 0, false),
        Some(Tamper::Shift(by)) => (value.wrapping_add(by), 0, false),
        Some(Tamper::Overlong(padding)) => (value, padding, false),
        Some(Tamper::Unterminated) => (value, 0, true),
    };
    let mut zigzag = match i32::try_from(value) {
        Ok(narrow) if width == Width::Int => {
            u64::from(((narrow << 1) ^ (narrow >> 31)).cast_unsigned())
        }
        _ => ((value << 1) ^ (value >> 63)).cast_unsigned(),
    };
    let mut groups = Vec::new();
    loop {
        groups.push(zigzag.to_le_bytes()[0] & 0x7F);
        zigzag >>= 7;
        if zigzag == 0 {
            break;
        }
    }
    groups.extend(std::iter::repeat_n(0, padding));
    let last = groups.len() - 1;
    for (at, group) in groups.into_iter().enumerate() {
        out.push(if at < last || unterminated {
            group | 0x80
        } else {
            group
        });
    }
}
