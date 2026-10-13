//! The consume-process-produce loop with `send_offsets_to_transaction`.
//!
//! It verifies the atomic-output half of the pattern: the transactional offset
//! commit and the output produces commit together, and the output records
//! become visible under `read_committed` once the commit marker advances the
//! LSO. `txn_offset_commit_materialize.rs` covers the offset-visibility half.

use {
    crate::{
        support::producer::{default_producer, transactional_producer},
        txn_harness::{boot_single, create_topic, rec, send_ok},
    },
    assert2::assert,
    krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel},
    std::time::Duration,
};

/// Consume-process-produce loop with `send_offsets_to_transaction`. After the
/// commit, 5 records must appear on the output topic under `read_committed`.
///
/// This verifies the atomic-output half of the pattern: the transactional
/// offset commit and the output produces are flushed and committed together,
/// and the output records become visible under `read_committed` once the commit
/// marker advances the LSO. `txn_offset_commit_materialize.rs` separately
/// verifies that the same marker makes committed offsets visible through
/// `OffsetFetch` and drops them on abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_offsets_to_transaction_atomic_with_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "input").await;
    create_topic(&bootstrap, "output").await;

    // Pre-seed the input topic with 5 records via a non-transactional producer.
    {
        let nt = default_producer(bootstrap.clone()).await;
        for v in ["i0", "i1", "i2", "i3", "i4"] {
            send_ok(&nt, rec("input", v)).await;
        }
        nt.flush().await.unwrap();
        nt.close().await.unwrap();
    }

    // Consume-process-produce loop inside one transaction.
    {
        let mut input_consumer = Consumer::builder()
            .bootstrap(bootstrap.clone())
            .group_id("cpp-g")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["input".to_string()])
            .build()
            .await
            .unwrap();

        let producer = transactional_producer(bootstrap.clone(), "cpp-tid").await;
        let txn = producer.begin_transaction().await.unwrap();

        // Read all 5 records from input.
        let mut last_offset = None;
        let mut read = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while read < 5 && std::time::Instant::now() < deadline {
            for r in input_consumer
                .poll(krabka_units::millis(200))
                .await
                .unwrap()
            {
                let out_val = format!(
                    "{}_v",
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or(b""))
                );
                send_ok(&producer, rec("output", &out_val)).await;
                last_offset = Some((("input".into(), r.partition), r.offset + 1));
                read += 1;
            }
        }
        assert!(read == 5, "expected to read 5 input records");

        // Commit the input consumer offset as part of the transaction.
        if let Some(offset_entry) = last_offset {
            producer
                .send_offsets_to_transaction([offset_entry], &input_consumer.group_metadata())
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
        // Wait for the transactional data batches and commit marker to hit the
        // local log before a read_committed verifier polls. `commit()` returns
        // after the coordinator flow completes, but LSO advancement can lag on
        // slow CI runners.
        broker.wait_until_local_log_end_offset("output", 0, 5).await;

        input_consumer.close().await.unwrap();
        producer.close().await.unwrap();
    }

    // Verify that 5 records arrived on the output topic under read_committed.
    let mut c2 = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("cpp-verify")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["output".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen < 5 && std::time::Instant::now() < deadline {
        seen += c2.poll(krabka_units::millis(200)).await.unwrap().len();
    }
    assert!(seen == 5, "expected 5 records on output topic");

    c2.close().await.unwrap();
    broker.shutdown().await;
}
