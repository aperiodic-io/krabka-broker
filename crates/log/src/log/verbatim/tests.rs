//! Unit tests for the zero-copy passthrough append: byte-exactness
//! against the owned path, the offset each variant assigns, and the
//! transaction state it tracks.

use {
    super::*,
    crate::{
        leader_epoch_checkpoint::EpochEntry,
        log::test_support::{
            EncodedVerbatim, TempLog, check_append_time_lookup, install_stamps,
            log_append_time_log, sample_batch, stamped_test_log, test_batch_at, test_log,
            verbatim_from,
        },
    },
    assert2::{assert, check},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{kibibytes, mebibytes},
};

fn check_transaction_offsets(log: &Log, stable: Offset) {
    assert2::assert!(log.log_end_offset() == Offset(2));
    assert2::assert!(log.lso() == stable);
}

/// Append `producer` verbatim at leader epoch 4 after giving it a bogus base
/// offset (999) and epoch (-1), which the log must overwrite.
fn append_with_bogus_base(log: &mut Log, producer: &mut RecordBatch) {
    producer.base_offset = 999;
    producer.partition_leader_epoch = -1;
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(producer, LeaderEpoch(4));
    log.append_verbatim(&vb).unwrap();
}

fn producer_with_timestamp(timestamp: i64) -> RecordBatch {
    let mut producer = test_batch_at(0);
    producer.base_timestamp = timestamp;
    producer.max_timestamp = timestamp;
    producer
}

fn two_record_producer(producer_id: i64, transactional: bool) -> RecordBatch {
    let mut producer = test_batch_at(0);
    producer.last_offset_delta = 1; // spans offsets 0..=1
    producer.producer_id = producer_id;
    producer.producer_epoch = 0;
    if transactional {
        producer.attributes = producer.attributes.with_transactional(true);
    }
    producer
}

fn stored_bytes(log: &Log) -> bytes::Bytes {
    log.read_raw(Offset(0), log.log_end_offset(), mebibytes(10))
        .unwrap()
        .bytes
}

/// The two fields outside the CRC are always assigned by the replication append.
fn assigned_header(wire: &[u8], epoch: i32) -> Vec<u8> {
    let mut expected = wire.to_vec();
    expected[0..8].copy_from_slice(&0i64.to_be_bytes());
    expected[12..16].copy_from_slice(&epoch.to_be_bytes());
    expected
}

#[test]
fn append_verbatim_assigns_offsets_and_is_byte_exact() {
    let TempLog { dir, mut log } = test_log();

    // Append three single-record batches verbatim. Each producer batch
    // carries a bogus base_offset (999) that the log must overwrite.
    let mut expected_wire = bytes::BytesMut::new();
    for _ in 0..3 {
        let mut producer = test_batch_at(0);
        append_with_bogus_base(&mut log, &mut producer);
        // Re-encode the expectation with the assigned offset + epoch.
        let mut stamped = producer.clone();
        stamped.base_offset = (log.log_end_offset() - 1).0;
        stamped.partition_leader_epoch = 4;
        stamped.encode(&mut expected_wire).unwrap();
    }
    assert2::assert!(log.log_end_offset() == 3);

    let log_end = log.log_end_offset();
    let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    assert2::assert!(&r.bytes[..] == &expected_wire[..]);

    // Decodes cleanly (CRC valid) with the assigned offsets.
    let mut cur: &[u8] = &r.bytes;
    let mut bases = Vec::new();
    while !cur.is_empty() {
        bases.push(Offset(RecordBatch::decode(&mut cur).unwrap().base_offset));
    }
    assert2::assert!(bases == vec![Offset(0), Offset(1), Offset(2)]);
    drop(dir);
}

#[test]
fn append_verbatim_at_stamps_base_byte_exact() {
    let TempLog { dir, mut log } = test_log();

    let mut prefix = test_batch_at(0);
    prefix.partition_leader_epoch = 2;
    log.append(&mut prefix).unwrap();

    let mut producer = test_batch_at(0);
    producer.base_offset = 999;
    producer.partition_leader_epoch = -1;
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(4));

    let appended = log.append_verbatim_at(&vb, Offset(1)).unwrap();

    assert!(appended == Offset(1));
    assert!(log.log_end_offset() == Offset(2));
    assert!(
        log.epoch_checkpoint().entries()
            == &[
                EpochEntry {
                    epoch: LeaderEpoch(2),
                    start_offset: Offset(0),
                },
                EpochEntry {
                    epoch: LeaderEpoch(4),
                    start_offset: Offset(1),
                },
            ]
    );

    let mut expected_wire = bytes::BytesMut::new();
    prefix.encode(&mut expected_wire).unwrap();
    let mut stamped = producer.clone();
    stamped.base_offset = 1;
    stamped.partition_leader_epoch = 4;
    stamped.encode(&mut expected_wire).unwrap();

    let r = log
        .read_raw(Offset(0), log.log_end_offset(), mebibytes(10))
        .unwrap();
    assert!(
        r.bytes[..] == expected_wire[..],
        "verbatim append_at must be byte-exact after supplied base+epoch stamping"
    );
    drop(dir);
}

/// Kafka's `appendAsFollower` refuses a first offset below the log end
/// offset, and takes one above it: the hole a compacted leader's log has.
#[test]
fn append_verbatim_at_refuses_a_base_below_the_log_end_offset_and_takes_one_above() {
    let TempLog { dir, mut log } = test_log();
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&test_batch_at(0), LeaderEpoch(4));
    log.append_verbatim_at(&vb, Offset(0)).unwrap();

    let err = log.append_verbatim_at(&vb, Offset(0)).unwrap_err();

    assert!(
        matches!(err, LogError::OffsetMismatch {
            expected: Offset(1),
            actual: Offset(0)
        }),
        "a base below the log end offset must report OffsetMismatch"
    );
    assert!(log.log_end_offset() == Offset(1));

    let appended = log.append_verbatim_at(&vb, Offset(4)).unwrap();

    assert!(appended == Offset(4));
    assert!(log.log_end_offset() == Offset(5));
    let read = log
        .read_raw(Offset(1), log.log_end_offset(), kibibytes(1))
        .unwrap();
    let mut cursor = &read.bytes[..];
    let stored = RecordBatch::decode(&mut cursor).unwrap();
    assert!(stored.base_offset == 4 && cursor.is_empty());
    drop(dir);
}

#[test]
fn append_verbatim_at_uses_reconciled_frontier_floor() {
    let TempLog { dir, mut log } = test_log();

    log.reconcile_next_offset(Offset(5));
    let producer = test_batch_at(0);
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(4));

    let appended = log.append_verbatim_at(&vb, Offset(5)).unwrap();

    assert!(appended == Offset(5));
    assert!(log.log_end_offset() == Offset(6));
    drop(dir);
}

#[test]
fn append_verbatim_matches_owned_append_bytes() {
    // The verbatim path and the owned path must write byte-identical
    // .log bytes for the same logical batch — proving passthrough does
    // not perturb the stored representation.
    let TempLog {
        dir: dir_owned,
        log: mut log_owned,
    } = test_log();
    let TempLog {
        dir: dir_verb,
        log: mut log_verb,
    } = test_log();

    let mut producer = test_batch_at(0);
    producer.base_offset = 12345; // overwritten by both paths
    producer.partition_leader_epoch = -1;

    // Owned path: stamp epoch like the produce handler does, then append.
    let mut owned = producer.clone();
    owned.partition_leader_epoch = 9;
    log_owned.append(&mut owned).unwrap();

    // Verbatim path: same epoch via the meta.
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(9));
    log_verb.append_verbatim(&vb).unwrap();

    let end_owned = log_owned.log_end_offset();
    let end_verb = log_verb.log_end_offset();
    assert2::assert!(end_owned == end_verb);
    let r_owned = log_owned
        .read_raw(Offset(0), end_owned, mebibytes(10))
        .unwrap();
    let r_verb = log_verb
        .read_raw(Offset(0), end_verb, mebibytes(10))
        .unwrap();
    assert2::assert!(&r_owned.bytes[..] == &r_verb.bytes[..]);
    drop(dir_owned);
    drop(dir_verb);
}

#[test]
fn append_verbatim_transactional_holds_lso() {
    let TempLog { dir, mut log } = test_log();
    // A transactional batch must hold the LSO at the batch's base offset
    // (it isn't stable until a commit/abort marker arrives).
    let producer = two_record_producer(77, true);
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(0));
    log.append_verbatim(&vb).unwrap();
    // LSO stays at 0 (the open txn's first offset), not log_end (2).
    check_transaction_offsets(&log, Offset(0));
    drop(dir);
}

/// The verbatim replication append path stamps the *full* offset span of
/// the batch. A multi-record batch appended through `append_verbatim`
/// records `last_offset == base_offset + last_offset_delta`. Interior
/// offsets and the inclusive end offset therefore resolve, and one offset
/// past the end does not. This test guards the `base + delta` arithmetic
/// on that path, which the owned-append tests above do not exercise.
#[test]
fn append_verbatim_stamps_full_offset_range() {
    let TempLog { dir, mut log } = stamped_test_log(500, 1);

    // A four-record producer batch, appended verbatim, spans offsets 0..=3.
    let mut producer = sample_batch(4);
    append_with_bogus_base(&mut log, &mut producer); // the log overwrites 999 with 0

    // last_offset is base(0) + delta(3) = 3.
    check!(log.stamp_for_offset(Offset(0)) == Some(500));
    check!(log.stamp_for_offset(Offset(3)) == Some(500));
    check!(log.stamp_for_offset(Offset(4)) == None);

    assert2::assert!(
        crate::log::test_support::stamp_entries(dir.path(), 0)
            == [crate::test_support::stamp_entry(0, 3, 500)]
    );
}

// Verbatim counterpart of `non_txn_batch_with_valid_pid_advances_lso`,
// pinning the `&&` in the verbatim LSO-tracking branch. A non-transactional
// verbatim batch with a valid producer_id must advance LSO; `&&`→`||`
// would hold it at the batch base (0).
#[test]
fn non_txn_verbatim_batch_with_valid_pid_advances_lso() {
    let TempLog { dir, mut log } = test_log();
    let producer = two_record_producer(55, false); // valid pid, but NOT transactional
    assert2::assert!(!producer.attributes.is_transactional());
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(0));
    log.append_verbatim(&vb).unwrap();
    check_transaction_offsets(&log, Offset(2));
    drop(dir);
}

/// The passthrough path stamps the same three header fields the owned path
/// stamps, in place in the producer's own bytes: the timestamp-type attribute
/// bit, `max_timestamp`, and the CRC that covers them. Everything else on the
/// wire, the record bodies included, is byte-for-byte what the producer sent,
/// which is what keeps the path a passthrough.
#[test]
fn verbatim_log_append_time_patches_three_header_fields_and_nothing_else() {
    let TempLog { dir, mut log } = log_append_time_log();
    let mut producer = producer_with_timestamp(1_000);
    producer.base_offset = 999;
    producer.partition_leader_epoch = -1;
    let EncodedVerbatim { wire, batch: vb } = verbatim_from(&producer, LeaderEpoch(4));

    let (base_offset, stamp) = log.append_verbatim(&vb).unwrap();

    let stamp = stamp.expect("a LogAppendTime log reports the stamp it wrote");
    assert!(base_offset == Offset(0));
    // The expectation is the producer's bytes with the three fields patched,
    // plus the two fields every verbatim append patches outside the CRC.
    let mut expected = assigned_header(&wire, 4);
    let attributes = producer
        .attributes
        .with_timestamp_type(krabka_protocol::records::TimestampType::LogAppendTime);
    expected[ATTRIBUTES_RANGE].copy_from_slice(&attributes.0.to_be_bytes());
    expected[MAX_TIMESTAMP_RANGE].copy_from_slice(&stamp.to_be_bytes());
    let crc = crc32c::crc32c(&expected[CRC_COVERAGE_START..]);
    expected[CRC_RANGE].copy_from_slice(&crc.to_be_bytes());

    let stored = stored_bytes(&log);
    assert!(&stored[..] == &expected[..]);
    // And the stored bytes decode, which is the CRC check the reader runs.
    let mut cursor: &[u8] = &stored;
    let decoded = RecordBatch::decode(&mut cursor).unwrap();
    assert!(
        decoded
            == RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 4,
                attributes,
                last_offset_delta: producer.last_offset_delta,
                base_timestamp: 1_000,
                max_timestamp: stamp,
                producer_id: producer.producer_id,
                producer_epoch: producer.producer_epoch,
                base_sequence: producer.base_sequence,
                records: producer.records.clone(),
            }
    );
    drop(dir);
}

/// The time index follows the stamp on the passthrough path too, so a
/// `ListOffsets` by time answers in append time rather than in producer time.
#[test]
fn verbatim_offset_for_timestamp_answers_in_append_time() {
    let TempLog { dir, mut log } = log_append_time_log();
    let producer = producer_with_timestamp(1_000);
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(0));

    let (_base_offset, stamp) = log.append_verbatim(&vb).unwrap();

    let stamp = check_append_time_lookup(&log, stamp);
    check!(log.offset_for_timestamp(stamp + 1) == None);
    drop(dir);
}

/// A `CreateTime` partition is the default, and the passthrough path keeps
/// every byte the producer sent apart from the two fields outside the CRC.
#[test]
fn verbatim_create_time_reports_no_stamp() {
    let TempLog { dir, mut log } = test_log();
    let mut producer = test_batch_at(0);
    producer.base_offset = 999;
    let EncodedVerbatim { wire, batch: vb } = verbatim_from(&producer, LeaderEpoch(4));

    let (base_offset, stamp) = log.append_verbatim(&vb).unwrap();

    assert!(base_offset == Offset(0));
    assert!(stamp == None);
    let expected = assigned_header(&wire, 4);
    let stored = stored_bytes(&log);
    assert!(&stored[..] == &expected[..]);
    drop(dir);
}

#[test]
fn verbatim_stamped_with_log_append_time_patches_attributes_and_crc() {
    use krabka_protocol::records::TimestampType;
    let mut producer = test_batch_at(0);
    producer.attributes = producer.attributes.with_transactional(true);
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(4));
    let stamped = vb.stamped_with_log_append_time(98765);
    assert!(stamped.max_timestamp == 98765);
    let mut cur = &stamped.bytes[..];
    let decoded = RecordBatch::decode(&mut cur).unwrap();
    assert!(decoded.max_timestamp == 98765);
    assert!(decoded.attributes.timestamp_type() == TimestampType::LogAppendTime);
    assert!(decoded.attributes.is_transactional());
}

#[test]
fn verbatim_append_flush_logic() {
    use crate::log::sync::sync_observer;
    let TempLog { dir, mut log } = test_log();
    // Default config: flush_on_append is false, stamp_source is None
    let producer = test_batch_at(0);
    let EncodedVerbatim { batch: vb, .. } = verbatim_from(&producer, LeaderEpoch(0));

    sync_observer::take_segment_flushes();
    log.append_verbatim(&vb).unwrap();
    // Non-transactional without stamp_source or flush_on_append does not flush
    assert!(sync_observer::take_segment_flushes().is_empty());

    // With stamp_source, non-transactional flushes
    install_stamps(&mut log, 10, 1);
    sync_observer::take_segment_flushes();
    log.append_verbatim(&vb).unwrap();
    assert!(!sync_observer::take_segment_flushes().is_empty());

    // With stamp_source, transactional does NOT flush
    let mut txn_producer = test_batch_at(0);
    txn_producer.producer_id = 100;
    txn_producer.producer_epoch = 1;
    txn_producer.attributes = txn_producer.attributes.with_transactional(true);
    let EncodedVerbatim { batch: txn_vb, .. } = verbatim_from(&txn_producer, LeaderEpoch(0));
    sync_observer::take_segment_flushes();
    log.append_verbatim(&txn_vb).unwrap();
    assert!(sync_observer::take_segment_flushes().is_empty());
    drop(dir);
}
