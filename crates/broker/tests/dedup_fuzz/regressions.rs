//! Deterministic cases for what the fuzzer found, one test each.

use {
    crate::{
        driver::{Topic, send},
        mutate::{DEDUP_HEADER, Mode, valid_batch},
        rng::Rng,
        support::{
            InProcess,
            dedup::{Rec, create_dedup_topic, keyed_batch, produce, read_partition},
            records::now_ms,
            start, start_with_dir,
        },
        wire::{Batch, Field, Header, HeaderLies, Tamper},
    },
    assert2::assert,
    krabka_broker::codes,
    krabka_compression::CompressionType,
    krabka_protocol::{owned::produce_response::PartitionProduceResponse, records::Attributes},
    std::time::Duration,
};

/// A dedup topic named `name` on `p`, in `mode`.
async fn dedup_topic(p: &InProcess, name: &'static str, mode: Mode) -> Topic {
    let id = create_dedup_topic()
        .broker(&p.broker)
        .client(&p.client)
        .topic(name)
        .maybe_key_header((mode == Mode::Header).then_some(DEDUP_HEADER))
        .call()
        .await;
    Topic { name, id, mode }
}

/// Produce a valid uncompressed batch for `topic`, drawn from `seed`, after
/// `change` has altered it and with `tampers` applied; the CRC covers both.
async fn produce_changed(
    p: &InProcess,
    topic: Topic,
    seed: u64,
    change: impl FnOnce(&mut Batch),
    tampers: &[(usize, Field, Tamper)],
) -> PartitionProduceResponse {
    let mut batch = valid_batch(&mut Rng::for_iteration(seed, 0), topic.mode, now_ms());
    batch.attributes = Attributes::default().with_compression(CompressionType::None);
    change(&mut batch);
    let bytes = batch.assemble(&batch.records_bytes(tampers), HeaderLies::default());
    send(p, topic, &bytes).await.expect("an answer")
}

/// A v2 batch whose attributes carry codec id 5, 6 or 7 names no codec.
/// Kafka's `CompressionType.forId` throws for it, so the append fails and
/// the producer gets an error; a JVM consumer that fetched such a batch would
/// throw the same way on `compressionType()`. krabka's
/// `Attributes::compression` reads an unknown id as `None` instead, so the
/// produce path stores the batch, with its unknown id, verbatim.
///
/// Fuzzer finding: any uncompressed batch with one of these ids and a CRC set
/// over them. The broker answers `NONE`.
#[tokio::test]
#[ignore = "product bug: a batch with an unknown codec id (5-7) is accepted and stored"]
async fn refuses_a_batch_with_an_unknown_codec_id() {
    let p = start().await;
    let topic = dedup_topic(&p, "codec", Mode::Key).await;
    for codec_id in [5_i16, 6, 7] {
        let row = produce_changed(
            &p,
            topic,
            1,
            |batch| {
                batch.attributes = Attributes(codec_id);
            },
            &[],
        )
        .await;
        assert!(
            row.error_code != codes::NONE,
            "codec id {codec_id}: {row:?}"
        );
    }
    p.broker.shutdown().await;
}

/// Kafka reads a record header name with `Utils.utf8`, which is `new
/// String(bytes, UTF_8)`: malformed bytes become U+FFFD and the batch is
/// appended. krabka's record parser refuses a name that is not UTF-8, so the
/// whole batch is refused with `INVALID_RECORD`. On a header-mode dedup topic
/// that refuses a batch whose dedup header is fine, because some other
/// header's name is not UTF-8.
///
/// Fuzzer finding: the `invalid UTF-8 header name` mutation is refused on
/// every topic.
#[tokio::test]
#[ignore = "product bug (Kafka compatibility): a header name that is not UTF-8 refuses the batch"]
async fn accepts_a_header_name_that_is_not_utf8_as_kafka_does() {
    let p = start().await;
    let topic = dedup_topic(&p, "names", Mode::Header).await;
    let not_utf8 = |batch: &mut Batch| {
        for record in &mut batch.records {
            record
                .headers
                .retain(|header| header.name != DEDUP_HEADER.as_bytes());
            record.headers.push(Header {
                name: DEDUP_HEADER.as_bytes().to_vec(),
                value: record.key.clone().or(Some(Vec::new())),
            });
            record.headers.push(Header {
                name: b"trace-\xff".to_vec(),
                value: Some(b"x".to_vec()),
            });
        }
    };
    let row = produce_changed(&p, topic, 2, not_utf8, &[]).await;
    assert!(row.error_code == codes::NONE, "{row:?}");
    p.broker.shutdown().await;
}

/// Kafka reads a record's length prefix with `ByteUtils.readVarint`, which
/// throws once a varint runs past five bytes, both on the produce path
/// (`LogValidator` walks every record) and in every consumer. krabka reads
/// the prefix as a varlong of up to ten bytes, so a record whose length is
/// written with redundant continuation groups (`0x86 0x80 0x80 0x80 0x80
/// 0x00` for 3) is accepted and stored verbatim. A JVM consumer that fetches
/// it then fails on the same varint.
///
/// Fuzzer finding: `record N: Length Overlong(5..=9)` on an otherwise valid
/// batch is accepted, here and by the protocol crate's own parser.
#[tokio::test]
#[ignore = "product bug (Kafka compatibility): a record length varint longer than five bytes is accepted"]
async fn refuses_a_record_length_varint_longer_than_five_bytes() {
    let p = start().await;
    let topic = dedup_topic(&p, "lengths", Mode::Key).await;
    for padding in [5, 9] {
        let row = produce_changed(&p, topic, 3, |_| {}, &[(
            0,
            Field::Length,
            Tamper::Overlong(padding),
        )])
        .await;
        assert!(row.error_code != codes::NONE, "padding {padding}: {row:?}");
    }
    p.broker.shutdown().await;
}

/// The window, in milliseconds, of the rebuild case below: short, so the
/// case waits it out.
const SHORT_WINDOW_MS: i64 = 1_000;

/// A record dated ahead of the broker is indexed at the broker's time, so it
/// stops matching one window later. A rebuild reads it back from the log and
/// clamps its `CreateTime` to the time of the rebuild instead, so every
/// rebuild within the producer's skew (up to `message.timestamp.after.max.ms`,
/// an hour by default) brings the identity back for another window. A record
/// with the same key, dated half an hour before the first, is then dropped
/// after a restart, where the running index kept it.
///
/// Fuzzer finding (`dedup_index` target, minimized): populate entries,
/// step the clock back past their timestamps, rebuild twice. The second
/// rebuild holds entries that the index after the first had expired.
/// `rebuild_from_log` calls `populate(digest, record.timestamp_ms, now_ms)`,
/// and `populate` clamps a future timestamp to `now_ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_does_not_re_arm_a_record_dated_ahead() {
    let dir = tempfile::tempdir().expect("a broker directory");
    let (broker, client) = start_with_dir(dir.path()).await;
    let topic_id = create_dedup_topic()
        .broker(&broker)
        .client(&client)
        .topic("skewed")
        .window_ms(SHORT_WINDOW_MS)
        .call()
        .await;
    let send = |client, record: Rec| {
        produce()
            .client(client)
            .topic("skewed")
            .topic_id(topic_id)
            .batch(keyed_batch().records(&[record]).call())
            .call()
    };
    let ahead = send(&client, Rec::key("k").aged(-1_800_000)).await;
    assert!(ahead.error_code == codes::NONE, "{ahead:?}");
    // Intentional: the window is wall-clock time, and nothing else ends it.
    tokio::time::sleep(Duration::from_millis(3 * SHORT_WINDOW_MS.unsigned_abs())).await;
    broker.shutdown().await;
    let (broker, client) = start_with_dir(dir.path()).await;
    broker.wait_until_partition_present("skewed", 0).await;
    let now = send(&client, Rec::key("k")).await;
    assert!(now.error_code == codes::NONE, "{now:?}");
    let contents = read_partition(&client, "skewed", topic_id, 0).await;
    assert!(contents.records.len() == 2, "{contents:?}");
    broker.shutdown().await;
}
