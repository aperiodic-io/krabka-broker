//! The three scenarios that decide between the verbatim (zero-copy) append and
//! the owned (decode, recompress, re-encode) fallback: a producer-compressed
//! LZ4 batch, an uncompressed batch checked byte for byte, and a topic whose
//! `compression.type` forces the broker to recompress.

use {
    crate::{
        harness::{
            batch, boot, create_topic, create_topic_with_configs, encode_batch, fetch_first_batch,
            produce_one, topic_id_for, wait_for_compression,
        },
        support::client::connect_client,
    },
    assert2::{assert, check},
    krabka_compression::CompressionType,
    krabka_protocol::{owned::create_topics_request::CreatableTopicConfig, records::HEADER_LEN},
};

/// A producer-LZ4-compressed v2 batch whose DECOMPRESSED form is large
/// (~100 KiB) takes the verbatim path. The broker validates it, retains the
/// Lz4 codec with no recompression, and round-trips the data on Fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lz4_batch_passes_through_and_roundtrips() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "lz4t").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "lz4t").await;

    // 200 records of a highly-compressible 512-byte value → ~100 KiB raw,
    // tiny compressed. An accidental uncompressed re-encode is obvious.
    let value = vec![b'Z'; 512];
    let b = batch(CompressionType::Lz4, 200, &value);
    let wire = encode_batch(&b);
    let raw_uncompressed_size = 200 * 512;
    assert!(
        wire.len() < raw_uncompressed_size / 8,
        "lz4 wire ({} B) must be far smaller than raw ({} B)",
        wire.len(),
        raw_uncompressed_size
    );

    let base = produce_one(&client, "lz4t", topic_id, b.clone())
        .await
        .expect("produce ok");
    assert!(base == 0);

    // Fetch it back: the stored batch must still be Lz4-compressed (no
    // recompression to a different codec) and decode to the same records.
    let fetched = fetch_first_batch(&broker, &client, "lz4t", topic_id, 200).await;
    check!(
        fetched.attributes.compression() == CompressionType::Lz4,
        "stored batch must keep producer's Lz4 codec; got {:?}",
        fetched.attributes.compression()
    );
    assert!(fetched.records.len() == 200, "all records round-trip");
    check!(fetched.records[0].value.as_deref() == Some(&value[..]));
    check!(fetched.records[199].value.as_deref() == Some(&value[..]));
    check!(fetched.base_offset == 0);

    broker.shutdown().await;
}

/// An UNCOMPRESSED v2 batch takes the verbatim path and round-trips
/// byte-identically. The CRC-covered region (bytes 21..) of the stored bytes
/// equals the producer's wire bytes exactly. The broker patches only
/// `base_offset` and `partition_leader_epoch`, which both sit before the CRC
/// region. The test re-encodes the fetched batch and compares. For an
/// uncompressed batch the re-encode is deterministic and byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncompressed_batch_roundtrips_byte_identically() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "raw").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "raw").await;

    let b = batch(CompressionType::None, 3, b"payload");
    let wire = encode_batch(&b);

    produce_one(&client, "raw", topic_id, b.clone())
        .await
        .expect("produce ok");

    let fetched = fetch_first_batch(&broker, &client, "raw", topic_id, 3).await;
    let fetched_wire = encode_batch(&fetched);

    // The CRC-covered region (attributes onward) must be byte-identical to
    // what the producer sent — proving no decode/re-encode/recompress.
    check!(
        fetched_wire[HEADER_LEN..] == wire[HEADER_LEN..],
        "record body must be verbatim"
    );
    check!(
        fetched_wire[21..HEADER_LEN] == wire[21..HEADER_LEN],
        "CRC-covered header (attributes..records_count) must be verbatim"
    );
    // The producer's CRC bytes (17..21) are preserved (no recompute).
    check!(fetched_wire[17..21] == wire[17..21], "CRC field unchanged");

    broker.shutdown().await;
}

/// A topic configured with a concrete `compression.type` that differs from
/// the producer's codec forces broker-side recompression, which is the OWNED
/// path. The stored batch must carry the TOPIC's codec, and the data must still
/// be correct. This pins the verbatim predicate's recompression gate to the
/// owned fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recompression_config_takes_owned_path() {
    let (broker, bootstrap, _dir) = boot().await;
    // Topic forces zstd; producer sends lz4 → must recompress (owned path).
    create_topic_with_configs(&broker, &bootstrap, "recmp", vec![CreatableTopicConfig {
        name: "compression.type".into(),
        value: Some("zstd".into()),
        ..Default::default()
    }])
    .await;
    wait_for_compression(&broker, "recmp", Some(CompressionType::Zstd)).await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "recmp").await;

    let value = vec![b'Q'; 256];
    let b = batch(CompressionType::Lz4, 10, &value);
    produce_one(&client, "recmp", topic_id, b.clone())
        .await
        .expect("produce ok");

    let fetched = fetch_first_batch(&broker, &client, "recmp", topic_id, 10).await;
    // Owned path recompressed lz4 → zstd: stored batch carries the TOPIC codec.
    check!(
        fetched.attributes.compression() == CompressionType::Zstd,
        "recompression config must rewrite codec to zstd; got {:?}",
        fetched.attributes.compression()
    );
    assert!(fetched.records.len() == 10);
    check!(fetched.records[0].value.as_deref() == Some(&value[..]));

    broker.shutdown().await;
}
