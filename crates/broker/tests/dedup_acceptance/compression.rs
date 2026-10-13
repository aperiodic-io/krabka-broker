//! Compressed batches: a batch with nothing to drop appends in the producer's
//! own bytes, a partial drop decodes the batch and encodes the rest again in
//! its codec, and an all-duplicate batch becomes an empty uncompressed batch.

use {
    crate::{
        harness::{Topic, appended_at, recs, stored_key, topic},
        support::{
            self,
            dedup::{StoredBatch, keyed_batch},
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
        },
    },
    assert2::check,
    krabka_compression::CompressionType,
    krabka_protocol::records::{RecordBatch, RecordsPayload},
};

/// The codec of every batch partition 0 holds, in offset order.
async fn stored_codecs(t: Topic<'_>) -> Vec<CompressionType> {
    let request = single_partition_fetch()
        .topic(t.name.to_owned())
        .topic_id(t.id)
        .partition(fetch_partition().partition(0).offset(0).call())
        .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
        .call();
    let response = t.p.client.send(request).await.expect("Fetch");
    response.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .map(<[RecordBatch]>::to_vec)
        .unwrap_or_default()
        .iter()
        .map(|batch| batch.attributes.compression())
        .collect()
}

/// Every codec, sent as the producer's own (`compression.type=producer`) or
/// recompressed to the topic's codec: a batch with nothing to drop is not
/// rebuilt, a partial drop is rebuilt in the codec the log stores, and an
/// all-duplicate batch is stored as an empty uncompressed batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_batches_are_deduplicated_in_every_codec() {
    let p = support::start().await;
    // The producer's codec, the topic's `compression.type`, and the codec the
    // log stores the records in.
    let cases = [
        (CompressionType::Gzip, "producer", CompressionType::Gzip),
        (CompressionType::Snappy, "producer", CompressionType::Snappy),
        (CompressionType::Lz4, "producer", CompressionType::Lz4),
        (CompressionType::Zstd, "producer", CompressionType::Zstd),
        (CompressionType::Gzip, "zstd", CompressionType::Zstd),
        (CompressionType::None, "lz4", CompressionType::Lz4),
    ];
    for (index, (codec, topic_codec, stored_codec)) in cases.into_iter().enumerate() {
        let topic_name = format!("codec-{index}");
        let t = topic()
            .p(&p)
            .name(&topic_name)
            .extra_configs(&[("compression.type", topic_codec)])
            .call()
            .await;
        let recompressed = (topic_codec != "producer").then_some(stored_codec);
        t.await_log_config(|config| config.compression_type == recompressed)
            .await;
        let send = |keys: &'static [&'static str]| {
            t.send_batch(keyed_batch().records(&recs(keys)).compression(codec).call())
        };

        let fresh = send(&["a", "b", "c"]).await;
        let rebuilt_after_fresh = t.metric("dedup_batches_rebuilt_total").await;
        let partial = send(&["a", "d", "b", "e"]).await;
        let rebuilt_after_partial = t.metric("dedup_batches_rebuilt_total").await;
        let duplicates = send(&["c", "d"]).await;
        let rebuilt_after_duplicates = t.metric("dedup_batches_rebuilt_total").await;
        let contents = t.contents().await;

        check!(
            [fresh, partial, duplicates] == [appended_at(0), appended_at(3), appended_at(7)],
            "{codec:?} to {topic_codec}"
        );
        check!(
            [
                rebuilt_after_fresh,
                rebuilt_after_partial,
                rebuilt_after_duplicates
            ] == [0, 1, 1],
            "{codec:?} to {topic_codec}"
        );
        check!(
            contents.records
                == [
                    stored_key(0, "a"),
                    stored_key(1, "b"),
                    stored_key(2, "c"),
                    stored_key(4, "d"),
                    stored_key(6, "e"),
                ],
            "{codec:?} to {topic_codec}"
        );
        check!(
            contents.batches
                == [
                    StoredBatch {
                        base_offset: 0,
                        last_offset: 2,
                        records: 3,
                    },
                    StoredBatch {
                        base_offset: 3,
                        last_offset: 6,
                        records: 2,
                    },
                    StoredBatch {
                        base_offset: 7,
                        last_offset: 8,
                        records: 0,
                    },
                ],
            "{codec:?} to {topic_codec}"
        );
        check!(
            stored_codecs(t).await == [stored_codec, stored_codec, CompressionType::None],
            "{codec:?} to {topic_codec}"
        );
        check!(
            t.metric("dedup_records_dropped_total").await == 4,
            "{codec:?} to {topic_codec}"
        );
    }
    p.broker.shutdown().await;
}
