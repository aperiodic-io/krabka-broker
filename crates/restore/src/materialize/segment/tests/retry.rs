use {
    super::{args_from, batch, record, record_with_key, topic_id_partition, verified_segment},
    crate::{bound::Predicates, materialize::write_segment},
    krabka_ids::{Offset, ProducerId},
    krabka_log::{Log, LogConfig},
    krabka_verified::producer::{
        ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, producer_decision,
    },
};

#[yare::parameterized(
    filtered_batch_sequence_11 = { false, 11 },
    filtered_batch_max_sequence = { false, i32::MAX - 1 },
    empty_batch_sequence_11 = { true, 11 },
    empty_batch_max_sequence = { true, i32::MAX - 1 },
)]
#[test_macro(tokio::test)]
async fn filtered_and_empty_restore_batches_preserve_retry_coordinates_after_reopen(
    empty: bool,
    sequence: i32,
) {
    let target = tempfile::tempdir().unwrap();
    let args = args_from(&["--exclude-key", "^drop$"], target.path());
    let partition = topic_id_partition("orders", 0);
    let predicates = Predicates::from_args(&args).unwrap();
    let mut original = batch(9, vec![
        if empty {
            record_with_key(0, "drop")
        } else {
            record(0, "head")
        },
        if empty {
            record_with_key(1, "drop")
        } else {
            record(1, "middle")
        },
        record_with_key(2, "drop"),
    ]);
    original.producer_id = 7;
    original.producer_epoch = 2;
    original.base_sequence = sequence;
    let segment = verified_segment(9, std::slice::from_ref(&original));
    let outcome = write_segment(&args, &partition, &segment, &predicates)
        .await
        .unwrap();
    assert2::assert!(outcome.end_offset == Offset(11));
    assert2::assert!(outcome.records_kept == if empty { 0 } else { 2 });
    let dir = krabka_log::name::partition_dir(&args.target.log_dir, "orders", 0);
    let mut log = Log::open(&dir, LogConfig::default()).unwrap();
    assert2::assert!(log.log_end_offset() == Offset(12));
    let restored = log
        .read(Offset(9), LogConfig::default().segment_size)
        .unwrap();
    let expected = krabka_protocol::records::RecordBatch {
        records: if empty {
            vec![]
        } else {
            original.records[..2].to_vec()
        },
        ..original
    };
    assert2::assert!(restored.batches == vec![expected]);
    let replayed = log.producer_state_entry(ProducerId(7)).unwrap();
    log.take_producer_snapshot().unwrap();
    assert2::assert!(krabka_log::name::producer_snapshot_path(&dir, 12).is_file());
    drop(log);
    let log = Log::open(&dir, LogConfig::default()).unwrap();
    let entry = log.producer_state_entry(ProducerId(7)).unwrap();
    assert2::assert!(entry == replayed);
    assert2::assert!(entry.last_offset == Offset(11) && entry.offset_delta == 2);
    assert2::assert!(entry.last_sequence == sequence.wrapping_add(2) & i32::MAX);
    let first_sequence = entry.last_sequence.wrapping_sub(entry.offset_delta) & i32::MAX;
    assert2::assert!(first_sequence == sequence);
    assert2::assert!(
        producer_decision(
            Some(ProducerEntryFacts {
                epoch: entry.producer_epoch,
                last_sequence: entry.last_sequence
            }),
            &[
                None,
                None,
                None,
                None,
                Some(RetainedSequenceRange {
                    base_sequence: first_sequence,
                    last_sequence: entry.last_sequence
                })
            ],
            2,
            sequence,
            2,
            false,
            true,
        ) == ProducerDecision::Duplicate { retained: 4 }
    );
}
