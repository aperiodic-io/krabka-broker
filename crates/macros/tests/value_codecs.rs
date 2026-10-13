//! Consumer checks for direct state and array derives, with small codec interfaces.

use assert2::assert;

#[path = "support/codec.rs"]
mod error;

use {
    bytes::{BufMut, BytesMut},
    error::{BrokerError, take},
};

fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> { Ok(i32::from_be_bytes(take(buf)?)) }
fn get_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> { Ok(i64::from_be_bytes(take(buf)?)) }

#[path = "support/framing.rs"]
mod framing;

use framing::coordinator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Offset(i64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateBatch {
    first_offset: Offset,
    last_offset: Offset,
    delivery_state: i8,
    delivery_count: i16,
}

fn check_version(buf: &mut &[u8]) -> Result<(), BrokerError> {
    if take::<2>(buf)? == [0, 0] {
        Ok(())
    } else {
        Err("unsupported share-state value version")
    }
}

fn put_batches(buf: &mut BytesMut, batches: &[StateBatch]) {
    // shortcut: fixture array counts and tag sizes fit one byte; use protocol helpers for wider values.
    buf.put_u8(u8::try_from(batches.len() + 1).unwrap());
    for batch in batches {
        buf.put_i64(batch.first_offset.0);
        buf.put_i64(batch.last_offset.0);
        buf.put_i8(batch.delivery_state);
        buf.put_i16(batch.delivery_count);
        buf.put_u8(0);
    }
}

fn get_batches(buf: &mut &[u8]) -> Result<Vec<StateBatch>, BrokerError> {
    let [length] = take(buf)?;
    let mut batches = Vec::new();
    for _ in 0..length.checked_sub(1).ok_or("null batches")? {
        let batch = StateBatch {
            first_offset: Offset(get_i64(buf)?),
            last_offset: Offset(get_i64(buf)?),
            delivery_state: i8::from_be_bytes(take(buf)?),
            delivery_count: i16::from_be_bytes(take(buf)?),
        };
        if take::<1>(buf)? != [0] {
            return Err("bad batch trailer");
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn put_value_tags(buf: &mut BytesMut, count: i32) {
    if count == -1 {
        buf.put_u8(0);
    } else {
        buf.extend_from_slice(&[1, 0, 4]);
        buf.put_i32(count);
    }
}

fn get_value_tags(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    match take::<1>(buf)? {
        [0] => Ok(-1),
        [1] if take::<2>(buf)? == [0, 4] => get_i32(buf),
        _ => Err("bad value tags"),
    }
}

#[derive(Debug, PartialEq, Eq, krabka_macros::ShareStateValue)]
#[share_state(encode_doc = "snapshot bytes", decode_doc = "invalid snapshot")]
struct SnapshotValue {
    snapshot_epoch: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
    delivery_complete_count: i32,
    create_timestamp: i64,
    write_timestamp: i64,
    state_batches: Vec<StateBatch>,
}

#[derive(Debug, PartialEq, Eq, krabka_macros::ShareStateValue)]
#[share_state(encode_doc = "update bytes", decode_doc = "invalid update")]
struct Update {
    snapshot_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
    delivery_complete_count: i32,
    state_batches: Vec<StateBatch>,
}

fn hex(text: &str) -> Vec<u8> {
    text.split_whitespace()
        .flat_map(|part| {
            (0..part.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&part[index..index + 2], 16).unwrap())
        })
        .collect()
}

#[test]
fn share_state_derives_keep_the_kafka_scalar_array_and_tag_order() {
    let snapshot = SnapshotValue {
        snapshot_epoch: 0,
        state_epoch: 1,
        leader_epoch: 0,
        start_offset: Offset(-1),
        delivery_complete_count: -1,
        create_timestamp: 1000,
        write_timestamp: 1000,
        state_batches: vec![],
    };
    let golden = hex(
        "0000 00000000 00000001 00000000 ffffffffffffffff 00000000000003e8 00000000000003e8 01 00",
    );
    assert!(snapshot.encode() == golden);
    assert!(SnapshotValue::decode(&golden) == Ok(snapshot));
    for end in 0..golden.len() {
        assert!(SnapshotValue::decode(&golden[..end]).is_err());
    }

    let update = Update {
        snapshot_epoch: 3,
        leader_epoch: 4,
        start_offset: Offset(12),
        delivery_complete_count: 7,
        state_batches: vec![StateBatch {
            first_offset: Offset(12),
            last_offset: Offset(15),
            delivery_state: 0,
            delivery_count: 1,
        }],
    };
    let golden = hex(
        "0000 00000003 00000004 000000000000000c 02 000000000000000c 000000000000000f 00 0001 00 01 00 04 00000007",
    );
    assert!(update.encode() == golden);
    assert!(Update::decode(&golden) == Ok(update));
    for end in 0..golden.len() {
        assert!(Update::decode(&golden[..end]).is_err());
    }
    assert!(Update::decode(&hex("0001")) == Err("unsupported share-state value version"));
}

fn write_items(buf: &mut BytesMut, items: &[i32]) {
    buf.put_u8(u8::try_from(items.len() + 1).unwrap());
    for &value in items {
        buf.put_i32(value);
    }
}

fn read_items(buf: &mut &[u8]) -> Result<Vec<i32>, BrokerError> {
    let [length] = take(buf)?;
    (0..length.checked_sub(1).ok_or("null array")?)
        .map(|_| get_i32(buf))
        .collect()
}

#[derive(Debug, PartialEq, Eq, krabka_macros::FlexibleArrayValue)]
#[array_value(record = "AssignmentRecord", encode = write_items, decode = read_items)]
struct Assignment {
    items: Vec<i32>,
}

#[test]
fn array_derive_keeps_version_field_framing_and_record_errors() {
    for (items, golden) in [
        (vec![], hex("0000 01 00")),
        (vec![3, -1], hex("0000 03 00000003 ffffffff 00")),
    ] {
        let value = Assignment { items };
        assert!(value.encode() == golden);
        assert!(Assignment::decode(&golden) == Ok(value));
        for end in 0..golden.len() {
            assert!(Assignment::decode(&golden[..end]).is_err());
        }
    }
    assert!(Assignment::decode(&hex("0001")) == Err("unknown AssignmentRecord version"));
    assert!(Assignment::decode(&hex("0000 00 00")) == Err("null array"));
    assert!(Assignment::decode(&hex("0000 01 01")) == Err("bad trailer"));
}
