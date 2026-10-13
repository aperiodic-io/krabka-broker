//! The idempotent-producer sequence checks on the produce path.
//!
//! A batch that carries a producer id and a base sequence is deduplicated when
//! it repeats, and it is rejected when its sequence skips ahead, so both cases
//! drive the produce path with a producer-stamped batch.

use {
    crate::{
        harness::{create_topic, topic_id_for},
        support::{
            self,
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{
                ProducerSequence, ProducerValuesSetup,
                producer_values_batch as one_batch_with_producer,
            },
            transactions::claim_idempotent_producer,
        },
    },
    assert2::assert,
};

#[tokio::test]
async fn idempotent_produce_dedups_duplicate_batch() {
    let p = support::start().await;

    create_topic(&p, "idem", 1).await;
    let idem_id = topic_id_for(&p.client, "idem").await;

    let init = claim_idempotent_producer(&p.client).await;
    let pid = init.producer_id;

    let req = batch_request(
        one_batch_with_producer(ProducerValuesSetup {
            pid: krabka_ids::ProducerId(pid),
            values: &["a", "b", "c"],
            ..Default::default()
        }),
        SinglePartitionProduceSetup {
            topic: ("idem").into(),
            topic_id: idem_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    );

    let r1 = p.client.send(req.clone()).await.expect("Produce 1");
    assert!(r1.responses[0].partition_responses[0].error_code == 0);
    assert!(r1.responses[0].partition_responses[0].base_offset == 0);

    // Send the same batch again — must be deduped (error 0, base_offset 0).
    let r2 = p.client.send(req).await.expect("Produce 2 (dup)");
    assert!(r2.responses[0].partition_responses[0].error_code == 0);
    assert!(r2.responses[0].partition_responses[0].base_offset == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_order_returns_45() {
    let p = support::start().await;

    create_topic(&p, "ooo", 1).await;
    let ooo_id = topic_id_for(&p.client, "ooo").await;

    let init = claim_idempotent_producer(&p.client).await;
    let pid = init.producer_id;

    let mk = |base_seq: i32| {
        batch_request(
            one_batch_with_producer(ProducerValuesSetup {
                pid: krabka_ids::ProducerId(pid),
                base_seq: ProducerSequence(base_seq),
                values: &["x", "y"],
                ..Default::default()
            }),
            SinglePartitionProduceSetup {
                topic: ("ooo").into(),
                topic_id: ooo_id,
                ..SinglePartitionProduceSetup::replicated()
            },
        )
    };

    // First batch (base_seq=0, 2 records → last_seq=1). Must succeed.
    let r1 = p.client.send(mk(0)).await.expect("Produce seq=0");
    assert!(r1.responses[0].partition_responses[0].error_code == 0);

    // Skip to base_seq=10 — gap → OUT_OF_ORDER_SEQUENCE_NUMBER (45).
    let r2 = p.client.send(mk(10)).await.expect("Produce seq=10");
    assert!(r2.responses[0].partition_responses[0].error_code == 45);

    p.broker.shutdown().await;
}
