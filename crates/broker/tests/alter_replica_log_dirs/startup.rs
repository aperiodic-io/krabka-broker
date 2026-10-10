//! What `Broker::start` does with a `<topic>-<partition>-future` directory it
//! finds on disk: resume the move when the real partition still exists, and
//! sweep the directory away when it does not.
//!
//! Both scenarios plant the future directory themselves rather than crashing a
//! broker mid-move, because the planted directory is exactly the state a crash
//! between the rename and the catch-up leaves behind.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::Broker;
use krabka_client_producer::Producer;

use crate::{
    harness::{count_topic_dirs, wait_for_move_complete},
    wire::describe_log_dirs,
};

/// Boot a broker, create a topic, produce records, shut down, then
/// plant a `<topic>-<partition>-future/` directory in the OTHER log
/// dir before the restart. The restart must (a) re-discover the
/// stranded future log with `log_dir::scan_future`, (b) call
/// `future_log::resume_move` for the real partition, and (c) drive
/// the move to completion, so that `DescribeLogDirs` reports the
/// partition in the target dir with `is_future_key=false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_resumes_move_for_existing_partition() {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();

    // First boot: create topic, produce a handful of records, then
    // shut down cleanly so the partition directory is left on disk.
    let cfg = crate::support::storage::two_dir_config(primary.path(), extra.path());
    let handle = Broker::start(cfg).await.expect("first boot");
    let addr = handle.listen_addr();
    crate::two_dir_topic::create_and_wait(
        &handle,
        addr,
        crate::two_dir_topic::Setup {
            client_id: crate::wire::CLIENT_ID,
            ..Default::default()
        },
    )
    .await;

    let producer = Producer::builder()
        .bootstrap(addr.to_string())
        .build()
        .await
        .expect("producer");
    crate::support::producer::enqueue_unkeyed_values(
        &producer,
        "t",
        (0..5i32).map(|i| Bytes::from(format!("v{i}"))),
    )
    .await;
    producer.flush().await.expect("flush");
    producer.close().await.expect("producer close");

    // Find which dir holds `t-0` and pick the OTHER as the target.
    let primary_has_0 = primary.path().join("t-0").exists();
    let (current_dir, target_dir) = if primary_has_0 {
        (primary.path(), extra.path())
    } else {
        (extra.path(), primary.path())
    };
    handle.shutdown().await;

    // Plant an empty future dir to simulate a crash mid-ARLD before
    // the move task got a chance to copy anything. On restart, the
    // broker discovers it and resumes the move, copying the
    // already-produced batches into it.
    let future_path = target_dir.join("t-0-future");
    std::fs::create_dir_all(&future_path).expect("plant future dir");
    assert!(future_path.exists());
    assert!(
        current_dir.join("t-0").exists(),
        "source must still be here"
    );

    // Restart against the same dirs. `BootstrapMode::Rejoin`
    // because the raft log from the first boot is still on disk.
    let mut cfg = crate::support::storage::two_dir_config(primary.path(), extra.path());
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let handle = Broker::start(cfg).await.expect("restart");
    let addr = handle.listen_addr();

    // Wait for the resumed move to converge: partition lives in
    // target dir with no remaining future entries.
    wait_for_move_complete(addr, target_dir, "t", &[0]).await;
    check!(count_topic_dirs(target_dir, "t") == 1);
    check!(count_topic_dirs(current_dir, "t") == 0);
    check!(!future_path.exists(), "future dir must be renamed away");

    handle.shutdown().await;
}

/// Plant a `<topic>-<partition>-future` directory in one of the
/// configured log.dirs for a topic that does not exist, then start the
/// broker. The startup scan in `Broker::start` must remove the
/// stranded future dir. `DescribeLogDirs` then reports no future entries.
#[tokio::test]
async fn startup_cleans_up_stranded_future_dir() {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();

    // Stranded future dir: topic "ghost" was never created.
    let stranded = extra.path().join("ghost-0-future");
    std::fs::create_dir_all(&stranded).unwrap();
    assert!(stranded.exists());

    let cfg = crate::support::storage::two_dir_config(primary.path(), extra.path());
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    // Broker startup must have swept the stranded future dir.
    assert!(
        !stranded.exists(),
        "startup must remove stranded future dir at {}",
        stranded.display()
    );

    // DescribeLogDirs surfaces no future entries.
    let resp = describe_log_dirs(addr).await;
    let any_future = resp
        .results
        .iter()
        .flat_map(|r| r.topics.iter())
        .flat_map(|t| t.partitions.iter())
        .any(|p| p.is_future_key);
    assert!(!any_future, "no future entries should remain after sweep");

    handle.shutdown().await;
}
