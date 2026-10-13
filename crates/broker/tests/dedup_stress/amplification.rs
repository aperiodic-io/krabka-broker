//! Per-request allocations and per-partition overhead: the shapes where a
//! small, in-policy request or a modest partition count turns into a large
//! amount of broker work or memory.

use {
    crate::{
        Target,
        child::{self, Peak, Report, block_on},
        support::{
            dedup::{DEDUP_HEADER, Rec, create_dedup_topic, keyed_batch},
            start, start_configured,
        },
    },
    assert2::assert,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_compression::CompressionType,
};

/// The address space a child may use for the batch-rewrite scenarios: well
/// above a broker's own needs, so an abort means a genuine blow-up and not an
/// under-provisioned test.
const REWRITE_ADDRESS_SPACE: u64 = 24 << 30;

/// A `max.message.bytes` large enough for the batches below, which hold tens
/// of thousands to a million records.
const BIG_MESSAGE_BYTES: &str = "134217728";

/// The value size of each record in the partial-drop batches. Large enough
/// that a 50,000-record batch is several MiB once decoded, small enough that
/// the batch stays within the broker's default decompression policy (the
/// 16 MiB output floor and the 100x ratio ceiling), which refuses a batch
/// that decompresses larger before dedup ever sees it.
const VALUE_BYTES: usize = 100;

/// A batch of `count` records keyed `k-0` upwards, each with a value of
/// `value_bytes`, under `compression`.
fn numbered_batch(
    count: usize,
    value_bytes: usize,
    compression: CompressionType,
) -> krabka_protocol::records::RecordBatch {
    let records: Vec<Rec> = (0..count)
        .map(|n| Rec::key(&format!("k-{n}")).value(&"v".repeat(value_bytes)))
        .collect();
    keyed_batch()
        .records(&records)
        .compression(compression)
        .call()
}

/// Produce a batch that is a near-total duplicate of one already stored, so
/// the leader rewrites it: one new key among `duplicates` already-seen keys.
/// Measures the peak resident growth of that one rewrite.
async fn one_partial_drop(client: &Client, target: Target<'_>, duplicates: usize) -> u64 {
    let big = numbered_batch(duplicates, VALUE_BYTES, CompressionType::Zstd);
    assert!(target.send(client, big).await.error_code == 0);
    // The same keys, plus one new key, in one batch. Every stored key is a
    // duplicate, so the leader drops all but the new record and re-encodes.
    let mut records: Vec<Rec> = (0..duplicates)
        .map(|n| Rec::key(&format!("k-{n}")).value(&"v".repeat(VALUE_BYTES)))
        .collect();
    records.push(Rec::key("new-one").value("small"));
    let batch = keyed_batch()
        .records(&records)
        .compression(CompressionType::Zstd)
        .call();
    let peak = Peak::start();
    let row = target.send(client, batch).await;
    assert!(row.error_code == 0);
    peak.growth_kib()
}

/// Child side: a partial-drop rewrite of a large, highly compressible zstd
/// batch on a dedup topic, and the same request pattern on a plain topic for
/// comparison.
fn partial_drop(dedup: bool) -> Report {
    block_on(async {
        let p = start().await;
        let (topic, topic_id) = if dedup {
            let id = create_dedup_topic()
                .broker(&p.broker)
                .client(&p.client)
                .topic("rewrite")
                .extra_configs(&[("max.message.bytes", BIG_MESSAGE_BYTES)])
                .call()
                .await;
            ("rewrite", id)
        } else {
            crate::support::client::create_topic_with(
                &p.client,
                crate::support::topics::CreateTopicSetup {
                    topic: "plain",
                    configs: &[("max.message.bytes", BIG_MESSAGE_BYTES)],
                    ..Default::default()
                },
            )
            .await;
            p.broker.wait_until_partition_present("plain", 0).await;
            (
                "plain",
                crate::support::topic_id_for(&p.client, "plain").await,
            )
        };
        let target = Target {
            topic,
            topic_id,
            partition: 0,
        };
        let growth = one_partial_drop(&p.client, target, 50_000).await;
        let rebuilt = target
            .metric(&p.broker, "dedup_batches_rebuilt_total")
            .await;
        p.broker.shutdown().await;
        vec![("rewrite_peak_kib", growth), ("rebuilt", rebuilt)]
    })
}

pub fn partial_drop_dedup() -> Report { partial_drop(true) }

pub fn partial_drop_plain() -> Report { partial_drop(false) }

/// Child side: a header-mode batch where every record lacks the configured
/// header, filling just under the topic's default `max.message.bytes`. The
/// broker builds one `record_errors` row per record. Measures the peak
/// resident growth of serving that one request and the row count, to show
/// that `max.message.bytes` bounds how many rows a refusal can build.
pub fn header_refusal() -> Report {
    block_on(async {
        // Each record is a short distinct key, an empty value and one wrong
        // header; about 20,000 of these fit under Kafka's default
        // max.message.bytes of ~1 MiB.
        const RECORDS: usize = 20_000;
        let p = start().await;
        let topic_id = create_dedup_topic()
            .broker(&p.broker)
            .client(&p.client)
            .topic("header")
            .key_header(DEDUP_HEADER)
            .call()
            .await;
        let target = Target {
            topic: "header",
            topic_id,
            partition: 0,
        };
        // Every record carries a different, wrong header, so none has an
        // identity and the whole batch is refused. Keys are short, values
        // empty, and the header name is one byte, to pack the most records
        // under the message-size limit.
        let records: Vec<Rec> = (0..RECORDS)
            .map(|n| Rec::key(&format!("{n}")).value("").header("o", Some("")))
            .collect();
        let batch = keyed_batch().records(&records).call();
        let peak = Peak::start();
        let row = target.send(&p.client, batch).await;
        let error = u64::from(row.error_code.unsigned_abs());
        let rows = u64::try_from(row.record_errors.len()).unwrap();
        let growth = peak.growth_kib();
        p.broker.shutdown().await;
        vec![
            ("refusal_error", error),
            ("record_error_rows", rows),
            ("refusal_peak_kib", growth),
        ]
    })
}

/// Child side: a broker that leads many idle dedup partitions. Each partition
/// holds an index that never indexed a record. Measures the resident growth
/// from creating them, to show what an empty index costs per partition.
pub fn idle_filters() -> Report {
    block_on(async {
        const PARTITIONS: i32 = 2_000;
        let p = start_configured(|config| {
            config.dedup_max_entries_per_partition = 1_000_000;
        })
        .await;
        let peak = Peak::start();
        let topic_id = create_dedup_topic()
            .broker(&p.broker)
            .client(&p.client)
            .topic("fanout")
            .partitions(PARTITIONS)
            .call()
            .await;
        // One record to each partition, so each writer has built its index.
        for partition in 0..PARTITIONS {
            let target = Target {
                topic: "fanout",
                topic_id,
                partition,
            };
            assert!(
                target
                    .send(&p.client, keyed_batch().records(&[Rec::key("seed")]).call())
                    .await
                    .error_code
                    == 0
            );
        }
        let growth = peak.growth_kib();
        p.broker.shutdown().await;
        vec![
            ("partitions", u64::try_from(PARTITIONS).unwrap()),
            ("fanout_peak_kib", growth),
        ]
    })
}

/// A partial-drop rewrite re-encodes the kept records in the topic's codec.
/// On a highly compressible zstd batch the decode holds every record
/// uncompressed at once. Bound the broker's peak resident growth for one such
/// rewrite of a 50,000-record batch at 512 MiB, and confirm that the rewrite
/// ran (`dedup_batches_rebuilt_total` is 1).
#[test]
fn a_partial_drop_of_a_large_compressed_batch_does_not_blow_up_memory() {
    let run = child::run("partial_drop_dedup", Some(REWRITE_ADDRESS_SPACE));
    eprintln!("partial_drop_dedup child: {run:?}");

    assert!(
        run.success,
        "child exited {}: {}",
        run.status, run.stderr_tail
    );
    assert!(run.get("rebuilt") == 1);
    let peak = run.get("rewrite_peak_kib");
    eprintln!("partial-drop rewrite peak growth: {peak} KiB");
    assert!(peak < 512 * 1024, "{peak} KiB");
}

/// The same request pattern on a plain topic never rewrites, so its peak is
/// the control the dedup case is judged against. It is reported, not bounded.
#[test]
fn a_plain_topic_does_not_rewrite_the_same_request() {
    let run = child::run("partial_drop_plain", Some(REWRITE_ADDRESS_SPACE));
    eprintln!("partial_drop_plain child: {run:?}");

    assert!(
        run.success,
        "child exited {}: {}",
        run.status, run.stderr_tail
    );
    assert!(run.get("rebuilt") == 0);
}

/// A header-mode batch whose every record lacks the header is refused with
/// one `record_errors` row per record, each carrying a per-record message.
/// The row count is bounded by how many records fit under the topic's
/// `max.message.bytes`, so a refusal under the default limit builds tens of
/// thousands of rows, not millions, and serving it costs little memory.
/// Bound the broker's peak resident growth at 256 MiB and confirm the
/// refusal and its row count.
///
/// This is the answer to the "millions of `record_errors` rows" concern: the
/// request-size limit is the ceiling on the rows, so the only way to reach
/// millions is to also raise `max.message.bytes` far above its default.
#[test]
fn a_header_refusal_builds_one_bounded_row_per_record() {
    let run = child::run("header_refusal", Some(REWRITE_ADDRESS_SPACE));
    eprintln!("header_refusal child: {run:?}");

    assert!(
        run.success,
        "child exited {}: {}",
        run.status, run.stderr_tail
    );
    assert!(run.get("refusal_error") == u64::from(codes::INVALID_RECORD.unsigned_abs()));
    assert!(run.get("record_error_rows") == 20_000);
    let peak = run.get("refusal_peak_kib");
    eprintln!("header refusal peak growth: {peak} KiB");
    assert!(peak < 256 * 1024, "{peak} KiB");
}

/// A broker that leads 2,000 idle dedup partitions holds 2,000 indexes that
/// have each indexed one record. Each index is 64 segment tables plus a
/// histogram; the design note puts an idle index at a few KiB. Bound the
/// resident growth from creating and seeding them at 512 MiB (about 256 KiB
/// per partition, well above the index itself, to leave room for the log and
/// metadata each partition also costs).
///
/// This is the scaling check behind the brief's "100k partitions" question:
/// it shows the per-partition index cost is small and does not dominate.
#[test]
#[ignore = "long: creates 2,000 partitions; run with --include-ignored"]
fn many_idle_dedup_partitions_cost_little_memory_each() {
    let run = child::run("idle_filters", Some(REWRITE_ADDRESS_SPACE));
    eprintln!("idle_filters child: {run:?}");

    assert!(
        run.success,
        "child exited {}: {}",
        run.status, run.stderr_tail
    );
    let partitions = run.get("partitions");
    let peak = run.get("fanout_peak_kib");
    eprintln!(
        "{partitions} idle dedup partitions: peak growth {peak} KiB ({} KiB each)",
        peak / partitions.max(1)
    );
    assert!(peak < 512 * 1024, "{peak} KiB for {partitions} partitions");
}
