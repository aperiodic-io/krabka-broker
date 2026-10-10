//! The full transactional cycle run at each downgraded `transaction.version`
//! level.
//!
//! A produce → commit → `read_committed` consume at `TV_1` and at `TV_0` is
//! what proves that the coordinator's encode path for the resolved level runs
//! and that the transaction commits and reads end to end.

use assert2::assert;

use crate::{
    support,
    support::producer::string_record as rec,
    txnver_harness::{admin_client, boot_single, create_topic, downgrade_transaction_version},
};

/// Run a full transactional cycle at whatever `transaction.version` the
/// cluster is currently finalized at: init → begin → send 3 → commit, then a
/// fresh `read_committed` consumer must observe exactly `["a","b","c"]`.
///
/// The commit forces the coordinator to write `TransactionLogValue` records to
/// `__transaction_state` at the resolved level (v0 for `TV_0`, v1 for `TV_1/TV_2`)
/// across its state transitions. A successful produce→commit→read cycle
/// therefore proves that the level's *encode* path runs and that the
/// transaction commits and reads end-to-end. In-memory state drives the
/// transitions within one broker lifetime. Unit tests in `txn::log_record`
/// cover decode and recovery from disk.
async fn full_cycle_commit_and_read(bootstrap: &str, topic: &str, tid: &str, group: &str) {
    let producer =
        crate::support::producer::transactional_producer(bootstrap.to_string(), tid).await;
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        producer
            .send(rec(topic, v))
            .await
            .expect("produce acknowledged");
    }
    txn.commit().await.unwrap();

    let (consumer, seen) = crate::support::transaction_wire::committed_values(
        bootstrap.to_string(),
        group,
        topic,
        None,
    )
    .await;
    assert!(
        seen == vec!["a", "b", "c"],
        "tid={tid} level cycle: {seen:?}"
    );

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}

/// Exercise the complete transactional cycle at both downgraded feature
/// levels. `TV_1` writes flexible v1 log values and `TV_0` writes classic v0
/// log values. In both cases a committed read proves that the selected encode
/// path works end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_full_cycles_commit_and_read() {
    struct Case {
        level: i16,
        topic: &'static str,
        tid: &'static str,
        group: &'static str,
    }

    let cases = [
        Case {
            level: 1,
            topic: "tv1",
            tid: "tv1-tid",
            group: "tv1-g",
        },
        Case {
            level: 0,
            topic: "tv0",
            tid: "tv0-tid",
            group: "tv0-g",
        },
    ];

    for case in cases {
        let (broker, bootstrap, _dir) = boot_single().await;
        let admin = admin_client(&bootstrap).await;
        create_topic(
            &admin,
            case.topic,
            crate::support::topics::TopicPartitionCount(1),
        )
        .await;
        downgrade_transaction_version(&admin, case.level).await;
        // The first lookup asks for `__transaction_state` and answers
        // COORDINATOR_NOT_AVAILABLE. A Kafka producer retries it; the krabka
        // producer does not, so the test makes the lookup a client makes.
        support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, case.tid).await;

        full_cycle_commit_and_read(&bootstrap, case.topic, case.tid, case.group).await;

        broker.shutdown().await;
    }
}
