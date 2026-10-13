//! Compile and exercise each owned/borrowed share-batch conversion direction.

use {assert2::assert, krabka_macros::WireStateBatch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Position(i64);

#[derive(Debug, Clone, PartialEq, Eq, WireStateBatch)]
#[wire_state_batch(read = ReadBatch, write = WriteBatch)]
struct Batch {
    first_offset: Position,
    last_offset: Position,
    delivery_state: i8,
    delivery_count: i16,
}

#[derive(Default)]
struct ReadBatch {
    first_offset: i64,
    last_offset: i64,
    delivery_state: i8,
    delivery_count: i16,
    extensions: Vec<u8>,
}

#[derive(Default)]
struct WriteBatch {
    first_offset: i64,
    last_offset: i64,
    delivery_state: i8,
    delivery_count: i16,
    future_flag: bool,
}

#[test]
fn every_direction_preserves_signed_fields_and_resets_wire_extensions() {
    for (first, last, state, attempts) in [
        (i64::MIN, i64::MAX, i8::MIN, i16::MAX),
        (-1, -1, i8::MAX, i16::MIN),
        (10, 19, 2, 3),
    ] {
        let value = Batch {
            first_offset: Position(first),
            last_offset: Position(last),
            delivery_state: state,
            delivery_count: attempts,
        };
        let mut read = ReadBatch::from(&value);
        assert!(read.extensions.is_empty());
        read.extensions.push(99);
        assert!(Batch::from(read) == value);

        let mut write = WriteBatch::from(value.clone());
        assert!(!write.future_flag);
        write.future_flag = true;
        assert!(Batch::from(&write) == value);
        // The borrowed request remains usable after conversion.
        assert!(write.future_flag);
    }
}
