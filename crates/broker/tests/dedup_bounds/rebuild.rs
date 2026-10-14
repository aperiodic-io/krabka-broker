//! Fetches of a partition while a restarted broker rebuilds that partition's
//! index from its log.

use {
    crate::{
        HOUR_MS, Latencies, Partition, counter, numbered,
        support::{
            client::connect_to_broker,
            dedup::create_dedup_topic,
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            start_with_dir,
        },
        timed_produce,
    },
    assert2::assert,
    krabka_client_core::Client,
    std::{
        sync::atomic::{AtomicBool, Ordering},
        time::{Duration, Instant},
    },
};

// Fetch up to 1 MiB of `target` from `offset`, and return how long the
// request took.
async fn timed_fetch(client: &Client, target: Partition<'_>, offset: i64) -> Duration {
    let request = single_partition_fetch()
        .topic(target.topic.to_owned())
        .topic_id(target.topic_id)
        .partition(
            fetch_partition()
                .partition(target.index)
                .offset(offset)
                .call(),
        )
        .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
        .call();
    let started = Instant::now();
    let response = client.send(request).await.expect("Fetch");
    let took = started.elapsed();
    let row = &response.responses[0].partitions[0];
    assert!(row.error_code == 0);
    assert!(row.records.is_some());
    took
}

// What the fetch loop saw while the first produce after the restart ran.
#[derive(Debug)]
struct DuringRebuild {
    produce: Duration,
    fetches: Latencies,
}

// Fetch `target` in a loop, walking through the backlog, until `done` is
// set. Returns the latency of every fetch that finished before it was set.
async fn fetch_until(
    client: &Client,
    target: Partition<'_>,
    backlog: i64,
    done: &AtomicBool,
) -> Latencies {
    let mut fetches = Latencies::default();
    let mut offset = 0;
    while !done.load(Ordering::Acquire) {
        let took = timed_fetch(client, target, offset).await;
        if !done.load(Ordering::Acquire) {
            fetches.push(took);
        }
        offset = (offset + 7_919) % backlog;
    }
    fetches
}

// A restart leaves the index of a partition with a large backlog stale, and
// the first produce rebuilds it from the log before it appends. The rebuild
// takes the log lock for one read at a time, so fetches of the same
// partition keep completing while that produce waits, and none of them
// waits for the whole rebuild. The produced record duplicates one of the
// backlog, so it is dropped: the rebuild finished before the produce was
// classified.
async fn fetch_during_a_rebuild(backlog: u64) {
    const PER_REQUEST: u64 = 1_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, client) = start_with_dir(dir.path()).await;
    let topic_id = create_dedup_topic()
        .broker(&broker)
        .client(&client)
        .topic("backlog")
        .window_ms(HOUR_MS)
        .call()
        .await;
    let target = Partition {
        topic: "backlog",
        topic_id,
        index: 0,
    };
    for first in (0..backlog).step_by(usize::try_from(PER_REQUEST).unwrap()) {
        let records = numbered()
            .keys(first..first + PER_REQUEST)
            .value_bytes(100)
            .call();
        timed_produce(&client, target, &records).await;
    }
    broker.shutdown().await;
    drop(client);

    let (broker, producer) = start_with_dir(dir.path()).await;
    broker.wait_until_partition_present("backlog", 0).await;
    let reader = connect_to_broker(&broker).await;
    let backlog_end = i64::try_from(backlog).unwrap();
    // A warm-up fetch, so that the loop below measures fetches and not the
    // first connection's setup.
    timed_fetch(&reader, target, 0).await;
    let done = AtomicBool::new(false);
    let duplicate = numbered().keys(0..1).value_bytes(100).call();
    let (produce, fetches) = tokio::join!(
        async {
            let took = timed_produce(&producer, target, &duplicate).await;
            done.store(true, Ordering::Release);
            took
        },
        fetch_until(&reader, target, backlog_end, &done),
    );
    let during = DuringRebuild { produce, fetches };
    let metrics = (
        counter(&broker, "dedup_index_rebuilds_total", target).await,
        counter(&broker, "dedup_records_dropped_total", target).await,
    );
    eprintln!(
        "rebuild of {backlog} records: produce took {:?}; fetches during it: {}; rebuilds, \
         dropped {metrics:?}",
        during.produce, during.fetches
    );

    assert!(metrics == (1, 1));
    assert!(during.fetches.0.len() >= 5, "{during:?}");
    assert!(during.fetches.max() < Duration::from_secs(2), "{during:?}");
    assert!(during.fetches.max() * 3 < during.produce, "{during:?}");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetches_keep_completing_while_a_restarted_partition_rebuilds_its_index() {
    fetch_during_a_rebuild(100_000).await;
}
