//! Producer kinds and acknowledgement modes: an idempotent producer's
//! sequences stay continuous across drops, a transactional batch bypasses
//! deduplication and is never indexed, and every `acks` mode deduplicates.

use {
    crate::{
        harness::{appended_at, rec, recs, stored, stored_key, topic},
        support::{
            self,
            dedup::{Idempotence, StoredRecord, keyed_batch, produce},
            produce::{
                ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
                batch_request,
            },
            producer::{producer_record, transactional_producer},
            transactions::claim_idempotent_producer,
        },
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::codes,
    krabka_ids::PartitionIndex,
    krabka_protocol::owned::produce_response::PartitionProduceResponse,
};

/// An idempotent producer's batches are deduplicated like any other. A
/// dropped record keeps its offset and the batch keeps its sequence span, so
/// the producer's next sequence is the one it expects. An exact retry of any
/// of its batches, the partial and the all-duplicate ones included, is
/// recognized by the producer-state check and answered with the batch's
/// original base offset, without appending again. A batch refused for its
/// sequence indexes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idempotent_producer_keeps_its_sequences_across_drops() {
    let p = support::start().await;
    let t = topic().p(&p).name("idempotent").call().await;
    let init = claim_idempotent_producer(&p.client).await;
    assert!(init.error_code == codes::NONE);
    let sequenced = |keys: &[&str], base_sequence| {
        keyed_batch()
            .records(&recs(keys))
            .idempotence(Idempotence {
                producer_id: init.producer_id,
                producer_epoch: init.producer_epoch,
                base_sequence,
            })
            .call()
    };
    let batches = [
        ("fresh", sequenced(&["a", "b"], 0), 0),
        ("partial drop", sequenced(&["a", "c"], 2), 2),
        ("all duplicates", sequenced(&["b", "c"], 4), 4),
        ("the next sequence", sequenced(&["d"], 6), 6),
    ];

    for (name, batch, base) in &batches {
        check!(
            t.send_batch(batch.clone()).await == appended_at(*base),
            "{name}"
        );
    }
    let before_retries = t.contents().await;
    for (name, batch, base) in &batches {
        // Kafka's `UnifiedLog` answers a duplicate batch with the timestamp
        // its producer-state entry recorded, the batch's `maxTimestamp`.
        let original = PartitionProduceResponse {
            log_append_time_ms: batch.max_timestamp,
            ..appended_at(*base)
        };
        check!(
            t.send_batch(batch.clone()).await == original,
            "retry of {name}"
        );
    }
    let after_retries = t.contents().await;
    let out_of_order = t.send_batch(sequenced(&["x"], 9)).await;
    // The refused batch indexed nothing, so its `x` is new to the next one.
    let next = t.send_batch(sequenced(&["e", "a", "x"], 7)).await;
    let contents = t.contents().await;

    assert!(
        before_retries.records
            == [
                stored_key(0, "a"),
                stored_key(1, "b"),
                stored_key(3, "c"),
                stored_key(6, "d"),
            ]
    );
    assert!(before_retries.high_watermark == 7);
    assert!(after_retries == before_retries);
    assert!(out_of_order.error_code == codes::OUT_OF_ORDER_SEQUENCE_NUMBER);
    assert!(next == appended_at(7));
    assert!(contents.records[4..] == [stored_key(7, "e"), stored_key(9, "x")]);
    assert!(contents.high_watermark == 10);
    p.broker.shutdown().await;
}

/// The key of the control record a `COMMIT` marker holds: version 0, type 1.
const COMMIT_MARKER_KEY: [u8; 4] = [0, 0, 0, 1];

/// A transactional batch bypasses deduplication on the leader and is never
/// indexed: it appends a key the index already holds, and a later plain
/// duplicate of a key that only a transactional batch wrote is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_batch_bypasses_dedup_and_is_never_indexed() {
    let p = support::start().await;
    p.broker.wait_until_transaction_coordinator_ready().await;
    let t = topic().p(&p).name("transactional").call().await;

    let plain = t.send(&[rec("a", "plain-a")]).await;
    let producer = transactional_producer(p.broker.listen_addr().to_string(), "dedup-txn").await;
    let transaction = producer.begin_transaction().await.expect("begin");
    for (key, value) in [("a", "txn-a"), ("t", "txn-t")] {
        producer
            .send(
                producer_record()
                    .topic(t.name.to_owned())
                    .key(Bytes::from(key.to_owned()))
                    .maybe_value(Some(Some(Bytes::from(value.to_owned()))))
                    .call(),
            )
            .await
            .expect("transactional record acknowledged");
    }
    transaction.commit().await.expect("commit");
    producer.close().await.expect("close");
    let after = t
        .send(&[rec("t", "plain-t"), rec("a", "plain-a-again")])
        .await;
    let contents = t.contents().await;

    let (markers, records): (Vec<StoredRecord>, Vec<StoredRecord>) = contents
        .records
        .into_iter()
        .partition(|record| record.key.as_deref() == Some(&COMMIT_MARKER_KEY[..]));
    assert!(plain == appended_at(0));
    assert!(after == appended_at(4));
    assert!(
        markers
            .iter()
            .map(|marker| marker.offset)
            .collect::<Vec<_>>()
            == [3]
    );
    assert!(
        records
            == [
                stored(0, "a", "plain-a"),
                stored(1, "a", "txn-a"),
                stored(2, "t", "txn-t"),
                stored(4, "t", "plain-t"),
            ]
    );
    assert!(t.metric("dedup_records_dropped_total").await == 1);
    assert!(t.metric("dedup_index_entries").await == 2);
    p.broker.shutdown().await;
}

/// `acks=0`, `acks=1` and `acks=-1` all deduplicate, and with `acks=-1` a
/// request whose records are all duplicates still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_acks_mode_deduplicates() {
    let p = support::start().await;
    let requests: [&[&str]; 3] = [&["a", "b"], &["b", "a"], &["a", "c"]];
    for (name, acks) in [
        ("acks0", ProduceAcknowledgements::NoResponse),
        ("acks1", ProduceAcknowledgements::Leader),
        ("acks-all", ProduceAcknowledgements::AllReplicas),
    ] {
        let t = topic().p(&p).name(name).call().await;
        let mut rows = Vec::new();
        for keys in requests {
            let batch = keyed_batch().records(&recs(keys)).call();
            if let ProduceAcknowledgements::NoResponse = acks {
                let request = batch_request(batch, SinglePartitionProduceSetup {
                    topic: name.to_owned(),
                    topic_id: t.id,
                    partition: PartitionIndex(0),
                    acknowledgements: acks,
                    timeout: ProduceTimeoutMillis(30_000),
                    records: None,
                });
                p.client
                    .send_no_response(request)
                    .await
                    .expect("Produce acks=0");
            } else {
                let row = produce()
                    .client(&p.client)
                    .topic(name)
                    .topic_id(t.id)
                    .batch(batch)
                    .acks(acks)
                    .call()
                    .await;
                rows.push(row);
            }
        }
        if rows.is_empty() {
            p.broker.wait_until_high_watermark(name, 0, 6).await;
        } else {
            check!(
                rows == [appended_at(0), appended_at(2), appended_at(4)],
                "{name}"
            );
        }
        let contents = t.contents().await;

        check!(
            contents.records == [stored_key(0, "a"), stored_key(1, "b"), stored_key(5, "c")],
            "{name}"
        );
        check!(contents.high_watermark == 6, "{name}");
        check!(t.metric("dedup_records_dropped_total").await == 3, "{name}");
    }
    p.broker.shutdown().await;
}
