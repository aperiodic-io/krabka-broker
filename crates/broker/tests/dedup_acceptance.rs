//! KFC-10 produce-time deduplication, driven through the Kafka protocol
//! against one in-process broker.

mod support;

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
