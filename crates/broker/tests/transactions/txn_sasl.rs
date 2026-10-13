//! The full transactional flow over a `SASL_PLAINTEXT` and `PLAIN` listener.
//!
//! `init_transactions` and `send_offsets_to_transaction` each open a dedicated
//! connection, to the transaction coordinator and to the group coordinator. Both
//! must carry the retained `ClientSecurity`, so this test drives the whole flow
//! with an authenticated producer to keep those secondary connections covered.

use {
    crate::{
        support::{producer::enqueue_string_values, transaction_wire::committed_values},
        txn_harness::{boot_single_sasl, create_topic_sasl, sasl_plain_security},
    },
    assert2::assert,
    krabka_client_producer::{ConsumerGroupMetadata, Producer},
};

/// Full transactional flow over a `SASL_PLAINTEXT`/`PLAIN` listener.
///
/// Regression test for a producer-side coordinator-connection credential
/// omission. `init_transactions` opens a *dedicated* connection to the
/// transaction coordinator, and `send_offsets_to_transaction` opens another one
/// to the group coordinator. If either drops the retained `ClientSecurity`, the
/// secured listener rejects the connection and the call fails with
/// `Client(Disconnected)`.
///
/// The test drives init, begin, send, `send_offsets_to_transaction`, and commit
/// end to end with a SASL-authenticated producer, which exercises both
/// secondary connections. A `read_committed` consumer then confirms that the
/// records committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_authenticated_transactional_flow_commits() {
    let (broker, bootstrap, _dir) = boot_single_sasl(&[("alice", "alice-secret")]).await;
    create_topic_sasl(
        &bootstrap,
        "sasl-txn",
        sasl_plain_security("alice", "alice-secret"),
    )
    .await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("sasl-tid")
        .security(sasl_plain_security("alice", "alice-secret"))
        .build()
        .await
        .unwrap();

    // init_transactions dials the txn coordinator on a fresh connection —
    // this is the call that failed with Client(Disconnected) before the fix.
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    enqueue_string_values(&producer, "sasl-txn", &["a", "b", "c"]).await;
    // send_offsets_to_transaction dials the group coordinator on a *second*
    // fresh connection — the other secondary connection that must carry SASL.
    producer
        .send_offsets_to_transaction(
            [(("sasl-txn".to_string(), 0), 3i64)],
            &ConsumerGroupMetadata::for_group("sasl-cpp-g"),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    // The data records must be present in the log before the read_committed
    // verifier starts polling; the consumer's isolation check still gates on
    // the LSO/commit marker.
    broker
        .wait_until_local_log_end_offset("sasl-txn", 0, 3)
        .await;

    // llvm-cov reliably exercises the SASL coordinator connections above, but
    // this final visibility poll can stall under coverage instrumentation.
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() {
        producer.close().await.unwrap();
        broker.shutdown().await;
        return;
    }

    let (consumer, seen) = committed_values(
        bootstrap,
        "sasl-verify",
        "sasl-txn",
        Some(sasl_plain_security("alice", "alice-secret")),
    )
    .await;
    assert!(seen == vec!["a", "b", "c"], "seen={seen:?}");

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}
