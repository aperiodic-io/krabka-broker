//! Unit tests for Kafka trunk's `max.decompressed.message.bytes` on the
//! cleaner's two passes: the offset-map build and the rewrite.

use {
    super::{
        CleanedTransactionMetadata, CleaningRound, ProducerLastRecord, RewriteRetention,
        rewrite_segments,
    },
    crate::{
        compact::{
            build_offset_map,
            test_support::{RETENTION, control_batch, write_sealed_batches},
        },
        error::LogError,
        io::FileIo,
        txn_index::AbortedTxn,
    },
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes},
    std::collections::HashMap,
};

const LIMIT: ByteSize = bytes(100);

#[derive(Clone, Copy, Default)]
enum BatchProducer {
    #[default]
    Anonymous,
    Transactional(ProducerId),
}

#[derive(Clone, Copy)]
enum ExpectedLimit {
    AllowsRecord,
    RefusesRecord,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct KeyedBatchSetup {
    offset: Offset,
    #[default(bytes(1_000))]
    value_size: ByteSize,
    #[default(CompressionType::Gzip)]
    compression: CompressionType,
    producer: BatchProducer,
}

/// One oversized keyed record, compressed with gzip and anonymous by default.
#[bon::builder]
fn keyed_batch(
    #[builder(into, default)] offset: Offset,
    #[builder(default = bytes (1_000))] value_size: ByteSize,
    #[builder(default = CompressionType :: Gzip)] compression: CompressionType,
    #[builder(default)] producer: BatchProducer,
) -> RecordBatch {
    let setup = KeyedBatchSetup {
        offset,
        value_size,
        compression,
        producer,
    };
    let producer_id = match setup.producer {
        BatchProducer::Anonymous => -1,
        BatchProducer::Transactional(producer) => producer.0,
    };
    RecordBatch {
        base_offset: setup.offset.0,
        last_offset_delta: 0,
        producer_id,
        attributes: Attributes::default()
            .with_compression(setup.compression)
            .with_transactional(matches!(setup.producer, BatchProducer::Transactional(_))),
        records: vec![Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from(vec![7_u8; setup.value_size.bytes_usize()])),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// Whether the pass refused a record above `LIMIT`; any other failure fails the
/// test.
fn refused<T>(result: Result<T, LogError>) -> bool {
    match result {
        Ok(_) => false,
        Err(LogError::RecordTooLarge { limit, .. }) => {
            assert2::check!(limit == LIMIT.bytes_usize());
            true
        }
        Err(other) => panic!("unexpected error: {other}"),
    }
}

/// Both passes read every record of a compressed batch that the log keeps, and
/// only of one: a batch the producer did not compress is never held to the
/// limit.
#[yare::parameterized(
    compressed_oversized = {
        "compressed, oversized",
        CompressionType::Gzip,
        Some(LIMIT),
        ExpectedLimit::RefusesRecord,
    },
    compressed_no_limit = {
        "compressed, no limit",
        CompressionType::Gzip,
        None,
        ExpectedLimit::AllowsRecord,
    },
    uncompressed_oversized = {
        "uncompressed, oversized",
        CompressionType::None,
        Some(LIMIT),
        ExpectedLimit::AllowsRecord,
    },
)]
fn the_passes_refuse_a_compressed_record_above_the_limit(
    name: &str,
    codec: CompressionType,
    limit: Option<ByteSize>,
    refuses: ExpectedLimit,
) {
    let dir = tempfile::tempdir().unwrap();
    let seg = write_sealed_batches(dir.path(), &[keyed_batch().compression(codec).call()]);
    let segments = [&seg];

    let map = build_offset_map(&segments, vec![], limit);
    assert2::check!(
        refused(map) == matches!(refuses, ExpectedLimit::RefusesRecord),
        "{name}: offset map"
    );

    let unchecked = build_offset_map(&segments, vec![], None).unwrap();
    let mut txn = CleanedTransactionMetadata::default();
    let active = HashMap::new();
    let rewritten = rewrite_segments(
        &FileIo,
        dir.path(),
        &segments,
        &unchecked,
        &mut txn,
        RewriteRetention {
            now_ms: 0,
            delete_retention: RETENTION,
        },
        CleaningRound {
            active_producers: &active,
            upper_bound: Offset(1),
            max_decompressed_record: limit,
        },
    );
    assert2::check!(
        refused(rewritten) == matches!(refuses, ExpectedLimit::RefusesRecord),
        "{name}: rewrite"
    );
}

/// `MemoryRecords.filterTo` skips a batch the pass deletes outright, which is
/// an aborted batch that neither keeps an active producer's state nor ends the
/// round, so such a batch is not read; one the pass keeps empty is read.
#[yare::parameterized(
    deleted_outright = { "deleted outright", Offset(10), None, ExpectedLimit::AllowsRecord },
    last_batch_of_round = {
        "the last batch of the round",
        Offset(1),
        None,
        ExpectedLimit::RefusesRecord,
    },
    active_producers_last_batch = {
        "an active producer's last data batch",
        Offset(10),
        Some(Offset(0)),
        ExpectedLimit::RefusesRecord,
    },
    another_active_producer_batch = {
        "another batch of an active producer",
        Offset(10),
        Some(Offset(5)),
        ExpectedLimit::AllowsRecord,
    },
)]
fn an_aborted_batch_is_read_only_when_the_pass_keeps_it_empty(
    name: &str,
    upper_bound: Offset,
    active_offset: Option<Offset>,
    refuses: ExpectedLimit,
) {
    let producer = ProducerId(2000);
    let last_data_offset = |offset| ProducerLastRecord {
        last_data_offset: Some(offset),
        producer_epoch: 0,
    };
    let dir = tempfile::tempdir().unwrap();
    // An oversized compressed batch of an aborted transaction, then the
    // abort marker in the next segment (past the pass), which the
    // aborted-transaction list names all the same.
    let seg = write_sealed_batches(dir.path(), &[keyed_batch()
        .producer(BatchProducer::Transactional(producer))
        .call()]);
    let segments = [&seg];
    let aborted = AbortedTxn {
        start_offset: Offset(0),
        last_offset: Offset(1),
        producer_id: producer,
        last_stable_offset: Offset(2),
    };
    // The offset-map build skips the aborted batch whatever it holds.
    let map = build_offset_map(&segments, vec![aborted], Some(LIMIT)).unwrap();

    let mut txn = CleanedTransactionMetadata::default();
    txn.add_aborted_transactions([aborted]);
    let active: HashMap<_, _> = active_offset
        .map(|offset| (producer, last_data_offset(offset)))
        .into_iter()
        .collect();
    let rewritten = rewrite_segments(
        &FileIo,
        dir.path(),
        &segments,
        &map,
        &mut txn,
        RewriteRetention {
            now_ms: 0,
            delete_retention: RETENTION,
        },
        CleaningRound {
            active_producers: &active,
            upper_bound,
            max_decompressed_record: Some(LIMIT),
        },
    );
    assert2::check!(
        refused(rewritten) == matches!(refuses, ExpectedLimit::RefusesRecord),
        "{name}"
    );
}

/// A control batch is never compressed, so the limit has nothing to say about
/// it.
#[test]
fn a_control_batch_is_never_held_to_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let seg = write_sealed_batches(dir.path(), &[control_batch(0, 1000, 1)]);
    assert2::check!(!refused(build_offset_map(&[&seg], vec![], Some(bytes(1)))));
}
