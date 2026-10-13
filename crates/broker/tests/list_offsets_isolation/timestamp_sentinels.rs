//! The sentinels that resolve against record data: `MAX_TIMESTAMP` and a
//! lookup by timestamp.
//!
//! `LATEST` is not the only way to ask where a partition ends. A record
//! written inside an open transaction carries a timestamp like any other, so
//! the same bound has to fence `MAX_TIMESTAMP` and `offsetsForTimes` as well,
//! or a `read_committed` consumer walks into the open transaction that
//! `LATEST` was careful to keep it out of. These lookups are refused with
//! `UNKNOWN_OFFSET` and no error code, which is what separates a fence from a
//! partition that is unavailable, and a lookup that lands below the bound is
//! still answered normally to both isolation levels.

use {
    crate::{
        support,
        wire::{
            EndOfPartition, both_levels, create_topic, matched_row, refused_row, send_at,
            wait_for_settled_log,
        },
    },
    assert2::check,
    krabka_client_producer::Producer,
};

/// An open transaction fences `MAX_TIMESTAMP` and a by-timestamp lookup for a
/// `read_committed` client, while a `read_uncommitted` client still resolves
/// them, and a lookup that lands below the bound is answered for both.
///
/// LATEST is not the only way to ask where a partition ends. A record written
/// inside an open transaction has a timestamp like any other, so a
/// `read_committed` consumer that asked `MAX_TIMESTAMP` -- or offered the
/// transaction's own timestamp to `offsetsForTimes` -- would be handed the
/// offset of a record it is not allowed to read, and would seek straight into
/// the open transaction that LATEST was careful to keep it out of. Kafka
/// refuses both with `UNKNOWN_OFFSET` and no error code.
///
/// The unfenced control matters as much as the fenced readings: a lookup
/// matching one of the settled records must still be answered normally at both
/// isolation levels. Without it, a broker that answered -1 to every timestamp
/// request would pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_transaction_fences_max_timestamp_and_a_timestamp_lookup() {
    const TOPIC: &str = "list-offsets-isolation-timestamps";
    /// Request timestamp sentinel (-3, KIP-734) asking for the offset of the
    /// record with the highest timestamp. Kafka's
    /// `ListOffsetsRequest.MAX_TIMESTAMP`.
    const MAX_TIMESTAMP: i64 = -3;
    /// The two settled records' timestamps, far below the transactional ones.
    const SETTLED: [i64; 2] = [1_000, 1_100];
    /// The two transactional records' timestamps. The higher of them is the
    /// partition's maximum while the transaction is open, which is what makes
    /// `MAX_TIMESTAMP` land above the bound.
    const IN_TXN: [i64; 2] = [5_000, 5_100];

    let p = support::start().await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    p.broker.wait_until_transaction_coordinator_ready().await;
    let bootstrap = p.broker.listen_addr().to_string();

    create_topic(&p.client, TOPIC).await;
    p.broker.wait_until_partition_present(TOPIC, 0).await;

    let plain = Producer::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .expect("plain producer build");
    send_at(&plain, TOPIC, "settled-a", SETTLED[0]).await;
    send_at(&plain, TOPIC, "settled-b", SETTLED[1]).await;
    wait_for_settled_log(&p.broker, TOPIC, 2).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("list-offsets-isolation-timestamps-tid")
        .build()
        .await
        .expect("transactional producer build");
    producer
        .init_transactions()
        .await
        .expect("init_transactions");
    let txn = producer
        .begin_transaction()
        .await
        .expect("begin_transaction");
    send_at(&producer, TOPIC, "open-c", IN_TXN[0]).await;
    send_at(&producer, TOPIC, "open-d", IN_TXN[1]).await;
    wait_for_settled_log(&p.broker, TOPIC, 4).await;

    // The highest timestamp on the partition belongs to the last transactional
    // record, at offset 3. The `read_committed` bound is offset 2, so 3 is out
    // of reach and the answer is refused; `read_uncommitted` is bounded at the
    // high watermark, 4, and gets the record.
    check!(
        both_levels(&p.client, TOPIC, MAX_TIMESTAMP).await
            == EndOfPartition {
                read_uncommitted: matched_row(3, IN_TXN[1]),
                read_committed: refused_row(),
            },
        "MAX_TIMESTAMP inside an open transaction is refused to read_committed"
    );

    // The same shape by timestamp. The transaction's first record is the
    // earliest at or after its own timestamp, and it sits exactly *on* the
    // bound -- Kafka's test is `offset >= lastFetchableOffset`, so the bound
    // itself is already out of reach.
    check!(
        both_levels(&p.client, TOPIC, IN_TXN[0]).await
            == EndOfPartition {
                read_uncommitted: matched_row(2, IN_TXN[0]),
                read_committed: refused_row(),
            },
        "a lookup landing on the bound is refused to read_committed"
    );

    // The control: a timestamp matching a settled record is below the bound at
    // both isolation levels, so both are answered normally. A broker that
    // refused every timestamp request would fail here.
    check!(
        both_levels(&p.client, TOPIC, SETTLED[0]).await
            == EndOfPartition {
                read_uncommitted: matched_row(0, SETTLED[0]),
                read_committed: matched_row(0, SETTLED[0]),
            },
        "a lookup below the bound is answered to both isolation levels"
    );

    txn.commit().await.expect("commit");
    wait_for_settled_log(&p.broker, TOPIC, 5).await;

    // The commit marker releases the last stable offset to 5, so the bound is
    // the same for both isolation levels and every sentinel answers alike. The
    // rows are compared to each other rather than to a literal because the
    // marker carries a wall-clock timestamp, which makes it the partition's
    // maximum and unnameable here.
    for (label, timestamp) in [
        ("MAX_TIMESTAMP", MAX_TIMESTAMP),
        ("the transaction's own timestamp", IN_TXN[0]),
        ("a settled record's timestamp", SETTLED[0]),
    ] {
        let answers = both_levels(&p.client, TOPIC, timestamp).await;
        check!(
            answers.read_committed == answers.read_uncommitted,
            "{label}: a resolved transaction leaves one answer"
        );
        check!(
            answers.read_committed != refused_row(),
            "{label}: and it is a real answer rather than a refusal"
        );
    }

    producer
        .close()
        .await
        .expect("transactional producer close");
    plain.close().await.expect("plain producer close");
    p.broker.shutdown().await;
}
