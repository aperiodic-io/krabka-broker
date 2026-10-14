//! A batch header that lies about its record count.

use {
    crate::{
        Target,
        child::{self, Report, block_on},
        support::{
            dedup::{Rec, create_dedup_topic, keyed_batch},
            produce::{
                ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
                single_partition_produce,
            },
            start,
        },
    },
    assert2::assert,
    bytes::{Bytes, BytesMut},
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::records::{CRC_COVERAGE_START, RecordsPayload},
};

/// The byte range of `last_offset_delta` in a v2 batch header.
const LAST_OFFSET_DELTA: std::ops::Range<usize> = 23..27;
/// The byte range of `records_count` in a v2 batch header.
const RECORDS_COUNT: std::ops::Range<usize> = 57..61;

/// The address space the child may use: far above what a broker needs, and
/// far below what an allocation sized from `records_count` asks for.
const ADDRESS_SPACE: u64 = 32 << 30;

/// One valid record, under a header that declares `records_count` records
/// and the matching `last_offset_delta`, with a CRC that covers both. The
/// header passes every check that reads the header alone; the body ends after
/// the first record.
fn lying_batch(records_count: i32) -> Bytes {
    let mut wire = BytesMut::new();
    keyed_batch()
        .records(&[Rec::key("k")])
        .call()
        .encode(&mut wire)
        .expect("encode a batch");
    wire[LAST_OFFSET_DELTA].copy_from_slice(&(records_count - 1).to_be_bytes());
    wire[RECORDS_COUNT].copy_from_slice(&records_count.to_be_bytes());
    let crc = crc32c::crc32c(&wire[CRC_COVERAGE_START..]);
    wire[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
    wire.freeze()
}

async fn produce_raw(client: &Client, target: Target<'_>, records: Bytes) -> i16 {
    let request = single_partition_produce(SinglePartitionProduceSetup {
        topic: target.topic.to_owned(),
        topic_id: target.topic_id,
        partition: krabka_ids::PartitionIndex(target.partition),
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        timeout: ProduceTimeoutMillis(30_000),
        records: Some(RecordsPayload::Raw(records)),
    });
    let response = client.send(request).await.expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

/// Child side: produce a batch that declares `i32::MAX` records to a dedup
/// topic, then an ordinary batch, and report both error codes.
pub fn records_count_bomb() -> Report {
    block_on(async {
        let p = start().await;
        let topic_id = create_dedup_topic()
            .broker(&p.broker)
            .client(&p.client)
            .topic("bomb")
            .call()
            .await;
        let target = Target {
            topic: "bomb",
            topic_id,
            partition: 0,
        };
        let bomb = produce_raw(&p.client, target, lying_batch(i32::MAX)).await;
        let after = target
            .send(
                &p.client,
                keyed_batch().records(&[Rec::key("after")]).call(),
            )
            .await;
        p.broker.shutdown().await;
        vec![
            ("bomb_error", u64::from(bomb.unsigned_abs())),
            ("after_error", u64::from(after.error_code.unsigned_abs())),
        ]
    })
}

/// A v2 batch whose header declares `i32::MAX` records around a one-record
/// body passes every header check (`last_offset_delta + 1 ==
/// records_count`, CRC) and is refused only when the record walk runs out of
/// body. A dedup topic's produce handler must not size its identity vector
/// from the declared count before that walk: an unclamped
/// `Vec::with_capacity(i32::MAX)` of 32-byte entries is 64 GiB and aborts the
/// broker on allocation failure. The broker must answer `INVALID_RECORD` and
/// keep serving.
///
/// Target: coordinator finding 1. This passes once the reservation is clamped
/// to what the body can hold; before that fix the child aborted (SIGABRT).
/// Runs in a child capped at 32 GiB of address space so an unclamped
/// allocation is a failed test, not a dead runner.
#[test]
fn a_lying_records_count_is_refused_without_a_huge_allocation() {
    let run = child::run("records_count_bomb", Some(ADDRESS_SPACE));
    eprintln!("records_count_bomb child: {run:?}");

    assert!(
        run.success,
        "child exited {}: {}",
        run.status, run.stderr_tail
    );
    assert!(run.get("bomb_error") == u64::from(codes::INVALID_RECORD.unsigned_abs()));
    assert!(run.get("after_error") == 0);
}
