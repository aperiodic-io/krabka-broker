//! The happy-path moves: `AlterReplicaLogDirs` relocates live partition
//! directories to the requested `log.dirs` entry, and the records already in
//! the log survive the relocation.
//!
//! Both scenarios pick the target directory from where the partition happens
//! to sit, because placement is least-loaded and therefore not fixed.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::Producer;

use crate::{
    harness::{count_topic_dirs, wait_for_move_complete},
    wire::alter_replica_log_dirs,
};

#[tokio::test]
async fn alter_replica_log_dirs_moves_partitions_to_target_dir() {
    let n: i32 = 2;
    let (handle, primary, extra, addr) = crate::two_dir_topic::start(crate::two_dir_topic::Setup {
        client_id: crate::wire::CLIENT_ID,
        partitions: crate::support::topics::TopicPartitionCount(n),
        ..Default::default()
    })
    .await;

    // Identify which dir holds which partitions today (placement is
    // least-loaded; with n=2 each dir gets one). Pick the source dir
    // that DOES hold partition 0 and move both partitions to the
    // OTHER directory.
    let primary_has_0 = primary.path().join("t-0").exists();
    let target_dir = if primary_has_0 {
        extra.path()
    } else {
        primary.path()
    };

    let resp = alter_replica_log_dirs(addr, target_dir, "t", vec![0, 1]).await;
    let topic_results: Vec<_> = resp
        .results
        .iter()
        .filter(|t| t.topic_name == "t")
        .collect();
    assert!(
        topic_results.len() == 1,
        "topic must be present in response"
    );
    for p in &topic_results[0].partitions {
        assert!(
            p.error_code == 0,
            "partition {} ack must be NONE, got {}",
            p.partition_index,
            p.error_code
        );
    }

    wait_for_move_complete(addr, target_dir, "t", &[0, 1]).await;

    // Both partitions now live in the target dir; the source is empty.
    let source_dir = if primary_has_0 {
        primary.path()
    } else {
        extra.path()
    };
    assert!(count_topic_dirs(target_dir, "t") == 2);
    assert!(count_topic_dirs(source_dir, "t") == 0);
    // No future dirs should remain anywhere.
    for d in [primary.path(), extra.path()] {
        for entry in std::fs::read_dir(d).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.ends_with("-future"),
                "future dir lingered in {}: {name}",
                d.display()
            );
        }
    }

    // A second call to the same target is a no-op success.
    let resp2 = alter_replica_log_dirs(addr, target_dir, "t", vec![0]).await;
    let topic2 = resp2
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("response includes t");
    assert!(topic2.partitions[0].error_code == 0);

    handle.shutdown().await;
}

/// Produce a few batches, move the partition to the other dir, then
/// consume and verify that every produced record survives. This test
/// exercises the `catch_up` batch-copy path in `future_log.rs`. The
/// empty-log move hits zero-batch catch-up, which does not run the
/// `append_at` loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_replica_log_dirs_preserves_records_across_move() {
    let (handle, primary, extra, addr) = crate::two_dir_topic::start(crate::two_dir_topic::Setup {
        client_id: crate::wire::CLIENT_ID,
        ..Default::default()
    })
    .await;

    let bootstrap = addr.to_string();
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .expect("producer build");
    crate::support::producer::enqueue_unkeyed_values(
        &producer,
        "t",
        (0..50i32).map(|i| Bytes::from(format!("v{i}"))),
    )
    .await;
    producer.flush().await.expect("flush");

    // Pick the OTHER dir as the move target.
    let primary_has_0 = primary.path().join("t-0").exists();
    let target_dir = if primary_has_0 {
        extra.path()
    } else {
        primary.path()
    };

    let resp = alter_replica_log_dirs(addr, target_dir, "t", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic");
    assert!(topic.partitions[0].error_code == 0);
    wait_for_move_complete(addr, target_dir, "t", &[0]).await;

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("arld-move-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["t".to_string()])
        .build()
        .await
        .expect("consumer build");

    let mut received_values: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while received_values.len() < 50 && Instant::now() < deadline {
        for r in consumer
            .poll(krabka_units::millis(200))
            .await
            .expect("poll")
        {
            if let Some(v) = r.value {
                received_values.push(String::from_utf8(v.to_vec()).unwrap());
            }
        }
    }
    received_values.sort();
    let mut expected: Vec<String> = (0..50).map(|i| format!("v{i}")).collect();
    expected.sort();
    assert!(received_values == expected, "all records survived the move");

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    handle.shutdown().await;
}
