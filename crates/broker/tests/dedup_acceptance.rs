//! KFC-10 produce-time deduplication, driven through the Kafka protocol
//! against one in-process broker.
//!
//! Each child module covers one part of the contract in
//! `docs/KFCs/KFC-10-produce-time-record-deduplication.md`: the first-wins
//! rule and its offsets, the header identity, timestamps and the window,
//! compressed batches, producer kinds and `acks`, the config surface, config
//! changes on a live topic, the metrics, and large records. A `tests/*.rs`
//! file is a crate root, so every child names its path.

mod support;

#[path = "dedup_acceptance/compression.rs"]
mod compression;
#[path = "dedup_acceptance/config_surface.rs"]
mod config_surface;
#[path = "dedup_acceptance/first_wins.rs"]
mod first_wins;
#[path = "dedup_acceptance/harness.rs"]
mod harness;
#[path = "dedup_acceptance/header_mode.rs"]
mod header_mode;
#[path = "dedup_acceptance/large_records.rs"]
mod large_records;
#[path = "dedup_acceptance/metrics.rs"]
mod metrics;
#[path = "dedup_acceptance/producers.rs"]
mod producers;
#[path = "dedup_acceptance/runtime_config.rs"]
mod runtime_config;
#[path = "dedup_acceptance/timestamps.rs"]
mod timestamps;

use {
    assert2::assert,
    support::{
        dedup::{Rec, StoredBatch, create_dedup_topic, keyed_batch, produce, read_partition},
        start,
    },
};

#[tokio::test]
async fn duplicates_across_requests_leave_offset_gaps_and_exact_base_offsets() {
    let p = start().await;
    let topic_id = create_dedup_topic()
        .broker(&p.broker)
        .client(&p.client)
        .topic("orders")
        .call()
        .await;

    let mut bases = Vec::new();
    for keys in [&["a", "b"][..], &["a", "c", "b", "d"], &["c", "d"]] {
        let records: Vec<Rec> = keys.iter().map(|k| Rec::key(k)).collect();
        let row = produce()
            .client(&p.client)
            .topic("orders")
            .topic_id(topic_id)
            .batch(keyed_batch().records(&records).call())
            .call()
            .await;
        assert!(row.error_code == 0);
        bases.push(row.base_offset);
    }
    let contents = read_partition(&p.client, "orders", topic_id, 0).await;

    assert!(bases == [0, 2, 6]);
    assert!(
        contents.offsets_and_keys()
            == [
                (0, "a".to_owned()),
                (1, "b".to_owned()),
                (3, "c".to_owned()),
                (5, "d".to_owned())
            ]
    );
    assert!(
        contents.batches
            == [
                StoredBatch {
                    base_offset: 0,
                    last_offset: 1,
                    records: 2
                },
                StoredBatch {
                    base_offset: 2,
                    last_offset: 5,
                    records: 2
                },
                StoredBatch {
                    base_offset: 6,
                    last_offset: 7,
                    records: 0
                },
            ]
    );
    assert!(contents.high_watermark == 8);
    p.broker.shutdown().await;
}
