//! A partition rebuilding its index after a restart, and the other partitions
//! of the same broker.
//!
//! The first produce to a partition whose index a restart left stale rebuilds
//! that index from the last window of its log before it appends. Each
//! partition has its own writer task and its own log lock, so one partition's
//! rebuild must not hold up produces to another. This is the cross-partition
//! counterpart to the bounds suite's same-partition fetch check.

use {
    crate::{
        Target,
        support::{
            client::connect_to_broker,
            dedup::{Rec, create_dedup_topic, keyed_batch},
            start_with_dir,
        },
    },
    assert2::assert,
    krabka_client_core::Client,
    std::time::{Duration, Instant},
};

const HOUR_MS: i64 = 3_600_000;

/// Produce a batch of `count` records keyed `{prefix}-0` upwards with 100-byte
/// values, and return the partition row's error code.
async fn produce(client: &Client, target: Target<'_>, prefix: &str, count: u64) -> i16 {
    let records: Vec<Rec> = (0..count)
        .map(|n| Rec::key(&format!("{prefix}-{n}")).value(&"v".repeat(100)))
        .collect();
    let row = target
        .send(client, keyed_batch().records(&records).call())
        .await;
    row.error_code
}

/// A restarted broker's stale partition rebuilds its index on its first
/// produce, and produces to a second partition of the same broker stay fast
/// while that rebuild runs. Each partition's writer task and log lock are its
/// own, so the rebuild of one is not head-of-line blocking of the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_partitions_rebuild_does_not_block_produces_to_another() {
    const BACKLOG: u64 = 60_000;
    const PER_BATCH: u64 = 2_000;
    let dir = tempfile::tempdir().expect("a temp dir for the two-partition broker");
    let (broker, client) = start_with_dir(dir.path()).await;
    let topic_id = create_dedup_topic()
        .window_ms(HOUR_MS)
        .partitions(2)
        .topic("two")
        .broker(&broker)
        .client(&client)
        .call()
        .await;
    let heavy = Target {
        topic: "two",
        topic_id,
        partition: 0,
    };
    let light = Target {
        topic: "two",
        topic_id,
        partition: 1,
    };
    for first in (0..BACKLOG).step_by(usize::try_from(PER_BATCH).unwrap()) {
        assert!(produce(&client, heavy, &format!("b{first}-"), PER_BATCH).await == 0);
    }
    broker.shutdown().await;
    drop(client);

    let (broker, heavy_client) = start_with_dir(dir.path()).await;
    broker.wait_until_partition_present("two", 0).await;
    broker.wait_until_partition_present("two", 1).await;
    let light_client = connect_to_broker(&broker).await;
    // Warm up partition 1 so its first produce after boot does not pay for a
    // connection or a metadata fetch inside the measured window.
    assert!(produce(&light_client, light, "warm", 1).await == 0);

    let heavy_task = async {
        let started = Instant::now();
        let code = produce(&heavy_client, heavy, "fresh-", PER_BATCH).await;
        (code, started.elapsed())
    };
    let light_task = async {
        let mut slowest = Duration::ZERO;
        let mut done = 0;
        for round in 0..50 {
            let started = Instant::now();
            let code = produce(&light_client, light, &format!("l{round}-"), 10).await;
            assert!(code == 0);
            slowest = slowest.max(started.elapsed());
            done += 1;
        }
        (slowest, done)
    };
    let ((heavy_code, rebuild), (slowest_light, light_count)) =
        tokio::join!(heavy_task, light_task);

    eprintln!(
        "rebuild-triggering produce to partition 0 took {rebuild:?}; {light_count} produces to \
         partition 1 during it, slowest {slowest_light:?}"
    );
    assert!(heavy_code == 0);
    // The rebuild reads one window of 60,000 records; partition 1's produces
    // run on their own task and must not wait for it.
    assert!(
        slowest_light < Duration::from_secs(2),
        "slowest light produce {slowest_light:?}"
    );
    broker.shutdown().await;
}
