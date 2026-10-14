//! The structure-aware mutator: a valid batch, then random damage to it.
//!
//! [`generate`] draws a batch a producer could send, then applies a random
//! subset of the mutations below and records each one in [`Case::notes`], so
//! a failure names what was done to the bytes:
//!
//! - structure: duplicate, null, invalid-UTF-8 and missing header names, null
//!   keys and values, shared identities, extreme timestamps, odd offset
//!   deltas, idempotent, transactional, control and `LogAppendTime` flags,
//!   and codec ids no codec has;
//! - fields: a record length, a key, value or header length, a header count,
//!   a timestamp or offset delta that lies, is overlong, or runs into the
//!   next field;
//! - bytes: flips, inserts, deletes and copies inside the records;
//! - payload: compressed bytes that are damaged, that decompress to garbage,
//!   that a different codec wrote, or that decompress to far more than they
//!   are;
//! - header: a `batch_length`, `records_count`, `last_offset_delta` or magic
//!   that lies;
//! - framing: a truncated batch, trailing garbage, or two batches.
//!
//! The CRC is usually recomputed after the damage, so the bytes get past the
//! CRC check into the record walk and the dedup classification; sometimes it
//! is left wrong.

use {
    crate::{
        rng::Rng,
        wire::{Batch, Field, Header, HeaderLies, Rec, Tamper, Tampers, refresh_crc},
    },
    krabka_compression::CompressionType,
    krabka_protocol::records::{Attributes, HEADER_LEN, TimestampType},
    std::{fmt::Write as _, sync::OnceLock},
};

/// The header a header-mode topic names in `dedup.key.header`.
pub const DEDUP_HEADER: &str = "dedup-id";

/// How a topic picks a record's identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The record key.
    Key,
    /// The value of [`DEDUP_HEADER`].
    Header,
}

/// The five codecs, in attribute order.
pub const CODECS: [CompressionType; 5] = [
    CompressionType::None,
    CompressionType::Gzip,
    CompressionType::Snappy,
    CompressionType::Lz4,
    CompressionType::Zstd,
];

/// One generated request body: the records field, and what was done to it.
#[derive(Clone, Debug)]
pub struct Case {
    pub bytes: Vec<u8>,
    pub notes: Vec<String>,
}

impl Case {
    /// The case as a failure message prints it.
    pub fn describe(&self) -> String {
        let mut hex = String::with_capacity(self.bytes.len() * 2);
        for byte in &self.bytes {
            let _ = write!(hex, "{byte:02x}");
        }
        format!(
            "mutations: [{}]\ninput ({} bytes): {hex}",
            self.notes.join(", "),
            self.bytes.len()
        )
    }
}

/// Compressed payloads that decompress to far more than they are, made once
/// per run: compressing them is the slow part.
pub struct Bombs {
    /// Per codec (index into [`CODECS`]): zeros past the decompression floor.
    zeros: Vec<Vec<u8>>,
    /// Per codec: [`RECORD_BOMB_RECORDS`] copies of one small keyed record.
    records: Vec<Vec<u8>>,
}

/// The records a record bomb holds.
pub const RECORD_BOMB_RECORDS: usize = 100_000;

/// Zeros past the default 16 MiB decompression floor.
const ZERO_BOMB_BYTES: usize = 17 << 20;

impl Bombs {
    /// The bombs of this process, made on first use.
    pub fn shared() -> &'static Self {
        static BOMBS: OnceLock<Bombs> = OnceLock::new();
        BOMBS.get_or_init(Self::new)
    }

    fn new() -> Self {
        let zeros = vec![0_u8; ZERO_BOMB_BYTES];
        let one = Batch::plain(vec![Rec {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(b"bomb".to_vec()),
            value: None,
            headers: vec![Header {
                name: DEDUP_HEADER.as_bytes().to_vec(),
                value: Some(b"bomb".to_vec()),
            }],
        }])
        .records_bytes(&[]);
        let records = one.repeat(RECORD_BOMB_RECORDS);
        let compress = |raw: &[u8]| -> Vec<Vec<u8>> {
            CODECS.iter().map(|codec| compressed(*codec, raw)).collect()
        };
        Self {
            zeros: compress(&zeros),
            records: compress(&records),
        }
    }
}

impl Batch {
    /// A plain-producer, uncompressed, `CreateTime` batch of `records`.
    fn plain(records: Vec<Rec>) -> Self {
        Self {
            attributes: Attributes::default(),
            base_offset: 0,
            partition_leader_epoch: -1,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }
}

/// `raw` under `codec`, or nothing when the codec refuses it.
fn compressed(codec: CompressionType, raw: &[u8]) -> Vec<u8> {
    krabka_compression::compress(codec, raw)
        .map(|bytes| bytes.to_vec())
        .unwrap_or_default()
}

/// A case with no mutation: the batch a producer could send. Returns the
/// case and its record count.
pub fn valid_case(rng: &mut Rng, mode: Mode, now_ms: i64) -> (Case, usize) {
    let batch = valid_batch(rng, mode, now_ms);
    let payload = compressed(batch.attributes.compression(), &batch.records_bytes(&[]));
    let case = Case {
        bytes: batch.assemble(&payload, HeaderLies::default()),
        notes: Vec::new(),
    };
    (case, batch.records.len())
}

/// Generate one case for a topic in `mode`, at broker time `now_ms`.
pub fn generate(rng: &mut Rng, mode: Mode, now_ms: i64, bombs: &Bombs) -> Case {
    let mut notes = Vec::new();
    let mut batch = valid_batch(rng, mode, now_ms);
    if rng.one_in(2) {
        for _ in 0..=rng.below(3) {
            mutate_structure(rng, &mut batch, now_ms, &mut notes);
        }
    }
    let tampers = if rng.one_in(3) {
        tamper_fields(rng, &batch, &mut notes)
    } else {
        Vec::new()
    };
    let mut records = batch.records_bytes(&tampers);
    if rng.one_in(5) {
        damage(rng, &mut records, "records", &mut notes);
    }
    let codec_index = CODECS
        .iter()
        .position(|codec| *codec == batch.attributes.compression())
        .unwrap_or(0);
    let payload = payload(rng, &mut batch, codec_index, &records, bombs, &mut notes);
    let lies = header_lies(rng, &batch, &mut notes);
    let mut bytes = batch.assemble(&payload, lies);
    frame(rng, mode, now_ms, &mut bytes, &mut notes);
    Case { bytes, notes }
}

/// An identity: a small shared pool, so that cases collide, or fresh bytes.
fn identity(rng: &mut Rng) -> Vec<u8> {
    match rng.below(5) {
        0 | 1 => format!("id-{}", rng.below(48)).into_bytes(),
        2 => rng.some_bytes(24),
        3 => Vec::new(),
        _ => format!("fresh-{:016x}", rng.next_u64()).into_bytes(),
    }
}

fn header_name(rng: &mut Rng) -> Vec<u8> {
    let names: [&[u8]; 8] = [
        b"trace",
        b"content-type",
        b"",
        b"a",
        b"dedup-i",
        b"dedup-id2",
        b"abcdefgh",
        b"abcdefghijklmnopq",
    ];
    if rng.one_in(4) {
        rng.some_bytes(12)
            .into_iter()
            .map(|byte| b'a' + byte % 26)
            .collect()
    } else {
        rng.pick(&names).to_vec()
    }
}

fn valid_record(rng: &mut Rng, mode: Mode, index: usize) -> Rec {
    let key = match mode {
        Mode::Key if rng.one_in(8) => None,
        Mode::Key => Some(identity(rng)),
        Mode::Header if rng.one_in(3) => None,
        Mode::Header => Some(rng.some_bytes(8)),
    };
    let value = match rng.below(12) {
        0 => None,
        1 => Some(rng.some_bytes_from(2048, 6144)),
        _ => Some(rng.some_bytes(64)),
    };
    let header_count = if rng.one_in(20) {
        rng.below(40)
    } else {
        rng.below(4)
    };
    let mut headers: Vec<Header> = (0..header_count)
        .map(|_| Header {
            name: header_name(rng),
            value: if rng.one_in(6) {
                None
            } else {
                Some(rng.some_bytes(16))
            },
        })
        .collect();
    if mode == Mode::Header && !rng.one_in(12) {
        let at = rng.below(headers.len() + 1);
        headers.insert(at, Header {
            name: DEDUP_HEADER.as_bytes().to_vec(),
            value: Some(identity(rng)),
        });
    }
    Rec {
        attributes: 0,
        timestamp_delta: i64::try_from(index).unwrap_or(0) * rng.between(0, 50),
        offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
        key,
        value,
        headers,
    }
}

pub fn valid_batch(rng: &mut Rng, mode: Mode, now_ms: i64) -> Batch {
    let count = if rng.one_in(10) {
        1 + rng.below(200)
    } else {
        1 + rng.below(8)
    };
    let records: Vec<Rec> = (0..count).map(|at| valid_record(rng, mode, at)).collect();
    let base_timestamp = now_ms - rng.between(0, 5_000);
    let max_delta = records
        .iter()
        .map(|record| record.timestamp_delta)
        .max()
        .unwrap_or(0);
    Batch {
        attributes: Attributes::default().with_compression(rng.pick(&CODECS)),
        base_timestamp,
        max_timestamp: base_timestamp + max_delta,
        ..Batch::plain(records)
    }
}

/// Header names that are not UTF-8, or are UTF-8 only by accident.
const INVALID_NAMES: [&[u8]; 6] = [
    b"\xff",
    b"\xc3",
    b"\xed\xa0\x80",
    b"dedup-\xff",
    b"\x80dedup-id",
    b"dedup-id\xc0\xaf",
];

/// Timestamps no producer should send.
fn extreme_timestamp(rng: &mut Rng, now_ms: i64) -> i64 {
    rng.pick(&[
        i64::MIN,
        i64::MAX,
        -1,
        0,
        now_ms + 2 * 3_600_000,
        now_ms + 3_000_000,
        now_ms - 2 * 86_400_000,
        now_ms - 86_400_000,
    ])
}

fn mutate_structure(rng: &mut Rng, batch: &mut Batch, now_ms: i64, notes: &mut Vec<String>) {
    let at = rng.below(batch.records.len());
    let record = &mut batch.records[at];
    match rng.below(16) {
        0 => {
            let copy = record.headers.get(rng.below(record.headers.len())).cloned();
            let copy = copy.unwrap_or(Header {
                name: DEDUP_HEADER.as_bytes().to_vec(),
                value: Some(b"dup".to_vec()),
            });
            let to = rng.below(record.headers.len() + 1);
            record.headers.insert(to, copy);
            notes.push(format!("record {at}: duplicate header name"));
        }
        1 => {
            let to = rng.below(record.headers.len() + 1);
            record.headers.insert(to, Header {
                name: DEDUP_HEADER.as_bytes().to_vec(),
                value: None,
            });
            notes.push(format!("record {at}: null {DEDUP_HEADER} at {to}"));
        }
        2 => {
            let name = rng.pick(&INVALID_NAMES).to_vec();
            let to = rng.below(record.headers.len() + 1);
            record.headers.insert(to, Header {
                name,
                value: Some(b"x".to_vec()),
            });
            notes.push(format!("record {at}: invalid UTF-8 header name"));
        }
        3 => {
            record.headers.retain(|h| h.name != DEDUP_HEADER.as_bytes());
            notes.push(format!("record {at}: no {DEDUP_HEADER}"));
        }
        4 => {
            record.key = None;
            notes.push(format!("record {at}: null key"));
        }
        5 => {
            record.value = None;
            notes.push(format!("record {at}: null value"));
        }
        6 => {
            let shared = identity(rng);
            for record in &mut batch.records {
                record.key = Some(shared.clone());
                for header in &mut record.headers {
                    if header.name == DEDUP_HEADER.as_bytes() {
                        header.value = Some(shared.clone());
                    }
                }
            }
            notes.push("every record shares one identity".to_owned());
        }
        7 => {
            batch.base_timestamp = extreme_timestamp(rng, now_ms);
            notes.push(format!("base_timestamp {}", batch.base_timestamp));
        }
        8 => {
            record.timestamp_delta = extreme_timestamp(rng, now_ms);
            notes.push(format!(
                "record {at}: timestamp_delta {}",
                record.timestamp_delta
            ));
        }
        9 => {
            record.offset_delta = rng.pick(&[-1, 0, i32::MAX, i32::MIN, 7]);
            notes.push(format!("record {at}: offset_delta {}", record.offset_delta));
        }
        10 => {
            batch.producer_id = rng.between(0, 1 << 20);
            batch.producer_epoch = 0;
            batch.base_sequence = rng.pick(&[0, 0, 5, i32::MAX]);
            notes.push(format!(
                "idempotent pid {} seq {}",
                batch.producer_id, batch.base_sequence
            ));
        }
        11 => {
            batch.attributes = batch.attributes.with_transactional(true);
            if rng.one_in(2) {
                batch.producer_id = rng.between(0, 1 << 20);
                batch.producer_epoch = 0;
                batch.base_sequence = 0;
            }
            notes.push(format!("transactional pid {}", batch.producer_id));
        }
        12 => {
            batch.attributes = Attributes(batch.attributes.0 | Attributes::CONTROL_BIT);
            notes.push("control bit".to_owned());
        }
        13 => {
            batch.attributes = batch
                .attributes
                .with_timestamp_type(TimestampType::LogAppendTime);
            notes.push("LogAppendTime".to_owned());
        }
        14 => {
            record.attributes = rng.byte();
            notes.push(format!("record {at}: attributes {}", record.attributes));
        }
        _ => {
            let extra = rng.pick(&[0x40_i16, 0x80, 0x100, 0x7F00, -0x8000]);
            batch.attributes = Attributes(batch.attributes.0 | extra);
            notes.push(format!("attribute bits {extra:#x}"));
        }
    }
}

/// Field lies a record can carry.
fn tamper(rng: &mut Rng) -> Tamper {
    match rng.below(6) {
        0 => Tamper::Value(rng.pick(&[
            -1,
            -2,
            0,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
            i64::MIN,
            i64::MAX,
            1 << 31,
            1 << 20,
        ])),
        1 | 2 => Tamper::Shift(rng.pick(&[-3, -2, -1, 1, 2, 3, 100])),
        3 => Tamper::Overlong(rng.pick(&[1, 2, 4, 6, 9])),
        4 => Tamper::Unterminated,
        _ => Tamper::Value(rng.between(-1_000, 100_000)),
    }
}

fn tamper_fields(rng: &mut Rng, batch: &Batch, notes: &mut Vec<String>) -> Tampers {
    (0..=rng.below(3))
        .map(|_| {
            let at = rng.below(batch.records.len());
            let headers = batch.records[at].headers.len().max(1);
            let field = match rng.below(8) {
                0 => Field::Length,
                1 => Field::TimestampDelta,
                2 => Field::OffsetDelta,
                3 => Field::KeyLength,
                4 => Field::ValueLength,
                5 => Field::HeaderCount,
                6 => Field::HeaderKeyLength(rng.below(headers)),
                _ => Field::HeaderValueLength(rng.below(headers)),
            };
            let how = tamper(rng);
            notes.push(format!("record {at}: {field:?} {how:?}"));
            (at, field, how)
        })
        .collect()
}

/// Flip, insert, delete, copy or overwrite bytes of `bytes`.
fn damage(rng: &mut Rng, bytes: &mut Vec<u8>, what: &str, notes: &mut Vec<String>) {
    for _ in 0..=rng.below(4) {
        let at = rng.below(bytes.len() + 1);
        match rng.below(6) {
            0 if at < bytes.len() => {
                let bit = rng.below(8);
                bytes[at] ^= 1 << bit;
                notes.push(format!("{what}: flip bit {bit} of byte {at}"));
            }
            1 if at < bytes.len() => {
                bytes[at] = rng.pick(&[0x00, 0xFF, 0x7F, 0x80, 0x01, 0xFE]);
                notes.push(format!("{what}: byte {at} = {:#04x}", bytes[at]));
            }
            2 => {
                let insert = rng.some_bytes_from(1, 8);
                notes.push(format!("{what}: insert {} bytes at {at}", insert.len()));
                bytes.splice(at..at, insert);
            }
            3 => {
                let end = (at + 1 + rng.below(16)).min(bytes.len());
                notes.push(format!("{what}: delete {at}..{end}"));
                bytes.drain(at.min(end)..end);
            }
            4 => {
                let end = (at + 1 + rng.below(32)).min(bytes.len());
                let copy = bytes[at.min(end)..end].to_vec();
                let to = rng.below(bytes.len() + 1);
                notes.push(format!("{what}: copy {at}..{end} to {to}"));
                bytes.splice(to..to, copy);
            }
            _ => {
                notes.push(format!("{what}: truncate at {at}"));
                bytes.truncate(at);
            }
        }
    }
}

/// The batch payload: the records under the batch's codec, or a damaged,
/// garbage, mislabelled or bomb payload in its place.
fn payload(
    rng: &mut Rng,
    batch: &mut Batch,
    codec_index: usize,
    records: &[u8],
    bombs: &Bombs,
    notes: &mut Vec<String>,
) -> Vec<u8> {
    let codec = CODECS[codec_index];
    match rng.below(40) {
        0 | 1 if codec != CompressionType::None => {
            let mut bytes = compressed(codec, records);
            damage(rng, &mut bytes, "compressed", notes);
            bytes
        }
        2 if codec != CompressionType::None => {
            notes.push("compressed garbage".to_owned());
            let garbage = rng.some_bytes(2_000);
            compressed(codec, &garbage)
        }
        3 => {
            let other = rng.pick(&CODECS[1..]);
            notes.push(format!("{other:?} bytes labelled {codec:?}"));
            compressed(other, records)
        }
        4 if codec != CompressionType::None => {
            notes.push(format!("{codec:?} zero bomb"));
            bombs.zeros[codec_index].clone()
        }
        5 if codec != CompressionType::None => {
            notes.push(format!("{codec:?} record bomb"));
            batch.records = vec![batch_bomb_record(); RECORD_BOMB_RECORDS];
            bombs.records[codec_index].clone()
        }
        6 => {
            let id = rng.pick(&[5_i16, 6, 7]);
            batch.attributes = Attributes((batch.attributes.0 & !0x07) | id);
            notes.push(format!("codec id {id}"));
            records.to_vec()
        }
        _ => compressed(codec, records),
    }
}

/// A stand-in for the record a record bomb repeats: only the count of
/// these reaches the header, and this one allocates nothing.
fn batch_bomb_record() -> Rec {
    Rec {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: None,
        value: None,
        headers: Vec::new(),
    }
}

fn header_lies(rng: &mut Rng, batch: &Batch, notes: &mut Vec<String>) -> HeaderLies {
    let mut lies = HeaderLies::default();
    if !rng.one_in(4) {
        return lies;
    }
    let count = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
    match rng.below(5) {
        0 => {
            let lie = rng.pick(&[-1, 0, 48, 49, 1 << 20, i32::MIN, i32::MAX]);
            notes.push(format!("batch_length {lie}"));
            lies.batch_length = Some(lie);
        }
        1 | 2 => {
            let lie = rng.pick(&[
                count + 1,
                count - 1,
                0,
                -1,
                i32::MIN,
                i32::MAX,
                1 << 20,
                count * 2,
            ]);
            notes.push(format!("records_count {lie}"));
            lies.records_count = Some(lie);
            if rng.one_in(3) {
                notes.push("last_offset_delta left true".to_owned());
            } else {
                lies.last_offset_delta = Some(lie.wrapping_sub(1));
            }
        }
        3 => {
            let lie = rng.pick(&[-1, 0, count, i32::MAX, i32::MIN]);
            notes.push(format!("last_offset_delta {lie}"));
            lies.last_offset_delta = Some(lie);
        }
        _ => {
            let lie = rng.pick(&[0_i8, 1, 3, -1]);
            notes.push(format!("magic {lie}"));
            lies.magic = Some(lie);
        }
    }
    if rng.one_in(8) {
        let crc = rng.pick(&[0, u32::MAX, 0xDEAD_BEEF]);
        notes.push(format!("crc {crc:#x}"));
        lies.crc = Some(crc);
    }
    lies
}

/// Damage the framing of the finished batch: truncate it, add garbage after
/// it, add a second batch, or change a byte after the CRC was set.
fn frame(rng: &mut Rng, mode: Mode, now_ms: i64, bytes: &mut Vec<u8>, notes: &mut Vec<String>) {
    match rng.below(24) {
        0 => {
            let at = rng.below(bytes.len());
            notes.push(format!("truncated to {at} bytes"));
            bytes.truncate(at);
        }
        1 => {
            let garbage = rng.some_bytes_from(1, 64);
            notes.push(format!("{} bytes of trailing garbage", garbage.len()));
            bytes.extend_from_slice(&garbage);
        }
        2 => {
            notes.push("the same batch twice".to_owned());
            let copy = bytes.clone();
            bytes.extend_from_slice(&copy);
        }
        3 => {
            notes.push("a second batch".to_owned());
            let (second, _) = valid_case(rng, mode, now_ms);
            bytes.extend_from_slice(&second.bytes);
        }
        4 if bytes.len() > HEADER_LEN => {
            let at = HEADER_LEN + rng.below(bytes.len() - HEADER_LEN);
            bytes[at] ^= 0x01;
            notes.push(format!("byte {at} flipped after the CRC"));
        }
        5 => {
            // A body damaged after assembly, then the CRC set again: the
            // header no longer describes the body, but the CRC holds.
            let mut body = bytes.split_off(HEADER_LEN.min(bytes.len()));
            damage(rng, &mut body, "assembled body", notes);
            bytes.extend_from_slice(&body);
            refresh_crc(bytes);
        }
        _ => {}
    }
}
