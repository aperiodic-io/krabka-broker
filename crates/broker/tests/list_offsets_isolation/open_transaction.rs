//! `LATEST` while a transaction is open on the partition, and once it has
//! resolved either way.
//!
//! This is the core claim of the suite in one case: `read_committed` stops at
//! the first record the open transaction wrote, `read_uncommitted` sees the
//! whole log, and both come back to one answer after the marker lands. The
//! case runs commit and abort through the same shape because both resolutions
//! write a marker and both release the last stable offset past it -- a broker
//! that released it only on commit would leave every `read_committed` consumer
//! of the partition pinned behind a transaction that is over.

use {
    crate::{
        support,
        wire::{
            EndOfPartition, create_topic, end_of_partition, latest_row, send_ok,
            wait_for_settled_log,
        },
    },
    assert2::check,
    krabka_client_producer::Producer,
};

/// How the transaction ends.
///
/// Both resolutions write a control marker to the partition and both release
/// the last stable offset past it, so `ListOffsets` must answer the same way
/// for either. Which of the records a later `Fetch` then hands back is the
/// difference between them, and that is `Fetch`'s business rather than this
/// suite's.
#[derive(Clone, Copy, Debug)]
enum Resolution {
    Commit,
    Abort,
}

/// `LATEST` under `read_committed` stops at the first record of an open
/// transaction, and catches up with `read_uncommitted` once that transaction
/// resolves either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_latest_stops_at_an_open_transaction() {
    let p = support::start().await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    p.broker.wait_until_transaction_coordinator_ready().await;
    let bootstrap = p.broker.listen_addr().to_string();

    for (label, resolution) in [
        // A commit publishes the records. The last stable offset moves past
        // the commit marker, so the end of the partition is the whole log.
        ("commit", Resolution::Commit),
        // An abort discards them for a `read_committed` reader, but it still
        // writes a marker and still releases the last stable offset. A broker
        // that released it only on commit would leave every `read_committed`
        // consumer of the partition pinned behind a transaction that is over.
        ("abort", Resolution::Abort),
    ] {
        let topic = format!("list-offsets-isolation-{label}");
        create_topic(&p.client, &topic).await;
        p.broker.wait_until_partition_present(&topic, 0).await;

        // Two ordinary records first, so the transaction does not start at
        // offset 0 and the `read_committed` answer below cannot be confused
        // with the log start offset.
        let plain = Producer::builder()
            .bootstrap(bootstrap.clone())
            .build()
            .await
            .expect("plain producer build");
        for value in ["settled-a", "settled-b"] {
            send_ok(&plain, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 2).await;

        // With no transaction anywhere on the partition the two isolation
        // levels must already agree. This is the control reading: it separates
        // "the last stable offset holds a `read_committed` client back" from
        // "the `read_committed` answer is broken".
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(2),
                    read_committed: latest_row(2),
                },
            "{label}: a partition with no transaction on it has one end"
        );

        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(format!("list-offsets-isolation-{label}-tid"))
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
        for value in ["open-c", "open-d"] {
            send_ok(&producer, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 4).await;

        // The transaction is open. `read_uncommitted` sees the whole log, and
        // `read_committed` stops at offset 2, the first record the transaction
        // wrote. This is the reading the bug got wrong: it answered 4 to both.
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(4),
                    read_committed: latest_row(2),
                },
            "{label}: an open transaction holds read_committed at its first record"
        );

        match resolution {
            Resolution::Commit => txn.commit().await.expect("commit"),
            Resolution::Abort => txn.abort().await.expect("abort"),
        }

        // The marker is the fifth record on the partition, and it releases the
        // last stable offset. Both isolation levels are back to one answer.
        wait_for_settled_log(&p.broker, &topic, 5).await;
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(5),
                    read_committed: latest_row(5),
                },
            "{label}: a resolved transaction releases read_committed"
        );

        producer
            .close()
            .await
            .expect("transactional producer close");
        plain.close().await.expect("plain producer close");
    }

    p.broker.shutdown().await;
}
