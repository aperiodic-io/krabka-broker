//! The per-partition and broker-wide entry limits, as the broker's gauges
//! report them, and the entries a topic gives back when it stops
//! deduplicating.

use {
    crate::{
        Arrival, HOUR_MS, Partition, broker_gauge, counter, index_entries, numbered,
        support::{
            InProcess,
            client::create_topic_with,
            configs::alter_topic_config,
            dedup::{Rec, create_dedup_topic},
            start, start_configured, topic_id_for,
            topics::{CreateTopicSetup, TopicPartitionCount},
        },
        timed_produce,
    },
    assert2::assert,
    futures_util::future::join_all,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    std::time::{Duration, Instant},
};

const PER_BATCH: u64 = 500;

// Create a dedup topic with an hour window and `partitions` partitions, and
// address each of its partitions.
async fn dedup_partitions<'a>(
    broker: &BrokerHandle,
    client: &Client,
    topic: &'a str,
    partitions: i32,
) -> Vec<Partition<'a>> {
    let topic_id = create_dedup_topic()
        .broker(broker)
        .client(client)
        .topic(topic)
        .window_ms(HOUR_MS)
        .partitions(partitions)
        .call()
        .await;
    (0..partitions)
        .map(|partition| Partition {
            topic,
            topic_id,
            index: partition,
        })
        .collect()
}

async fn delete_topic(client: &Client, topic: &str) {
    let response = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.to_owned()),
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    assert!(response.responses[0].error_code == 0);
}

// Poll `read` until it returns `want` or ten seconds pass, and return the last
// value read.
async fn eventually<F, Fut>(want: u64, mut read: F) -> u64
where
    F: FnMut() -> Fut,
    Fut: Future<Output = u64>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = read().await;
        if value == want || Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// The broker-wide index gauge and the sum of the per-partition index gauges.
async fn broker_and_partition_sum(
    broker: &BrokerHandle,
    partitions: &[Partition<'_>],
) -> (u64, u64) {
    let mut sum = 0;
    for &target in partitions {
        sum += index_entries(broker, target).await;
    }
    (
        broker_gauge(broker, "dedup_broker_index_entries").await,
        sum,
    )
}

// One partition with a limit of 1,000 receives 20,000 new identities in
// batches of 500. After every produce its index holds at most 1,000, and
// once full the partition either evicts entries or admits records unindexed.
// When the identities arrive as a stream, the index keeps the newest ones, so
// a duplicate of each of the last 100 keys is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_index_stays_within_its_limit_under_twenty_times_as_many_identities() {
    const LIMIT: u64 = 1_000;
    const KEYS: u64 = 20_000;
    let p = start_configured(|config| {
        config.dedup_max_entries_per_partition = usize::try_from(LIMIT).unwrap();
    })
    .await;

    for (topic, arrival) in [("burst", Arrival::Burst), ("stream", Arrival::Spread)] {
        let target = dedup_partitions(&p.broker, &p.client, topic, 1).await[0];
        let mut largest_index = 0;
        for first in (0..KEYS).step_by(usize::try_from(PER_BATCH).unwrap()) {
            let records = numbered()
                .keys(first..first + PER_BATCH)
                .arrival(arrival)
                .call();
            timed_produce(&p.client, target, &records).await;
            largest_index = largest_index.max(index_entries(&p.broker, target).await);
        }
        let evicted = counter(&p.broker, "dedup_capacity_evicted_entries_total", target).await;
        let unindexed = counter(&p.broker, "dedup_records_unindexed_total", target).await;
        let dropped_before = counter(&p.broker, "dedup_records_dropped_total", target).await;
        let newest = numbered().keys(KEYS - 100..KEYS).arrival(arrival).call();
        timed_produce(&p.client, target, &newest).await;
        let dropped = counter(&p.broker, "dedup_records_dropped_total", target).await;
        eprintln!(
            "{arrival:?}: largest index {largest_index}, evicted {evicted}, unindexed \
             {unindexed}, newest duplicates dropped {}",
            dropped - dropped_before
        );

        assert!(largest_index <= LIMIT, "{arrival:?}");
        assert!(largest_index >= LIMIT * 9 / 10, "{arrival:?}");
        assert!(evicted + unindexed > 0, "{arrival:?}");
        if arrival == Arrival::Spread {
            assert!(evicted > 0);
            assert!(dropped - dropped_before == 100);
        }
    }
    p.broker.shutdown().await;
}

const PER_ROUND: u64 = 200;
const BROKER_LIMIT: u64 = 3_000;

// The `PER_ROUND` new identities of `round`.
fn round_records(round: u64, arrival: Arrival) -> Vec<Rec> {
    let first = round * PER_ROUND;
    numbered()
        .keys(first..first + PER_ROUND)
        .arrival(arrival)
        .call()
}

// One produce of a round's identities to each partition, one after another.
async fn sequential_round(
    client: &Client,
    partitions: &[Partition<'_>],
    round: u64,
    arrival: Arrival,
) {
    for &target in partitions {
        timed_produce(client, target, &round_records(round, arrival)).await;
    }
}

// The same produces, all at once.
async fn concurrent_round(
    client: &Client,
    partitions: &[Partition<'_>],
    round: u64,
    arrival: Arrival,
) {
    join_all(partitions.iter().map(|&target| async move {
        timed_produce(client, target, &round_records(round, arrival)).await
    }))
    .await;
}

// A broker whose partitions may each hold 10,000 identities and which may
// hold `BROKER_LIMIT`, with six partitions of one dedup topic and two of
// another.
async fn eight_partitions() -> (InProcess, Vec<Partition<'static>>) {
    let p = start_configured(|config| {
        config.dedup_max_entries_per_partition = 10_000;
        config.dedup_max_entries = usize::try_from(BROKER_LIMIT).unwrap();
    })
    .await;
    let mut partitions = dedup_partitions(&p.broker, &p.client, "wide", 6).await;
    partitions.extend(dedup_partitions(&p.broker, &p.client, "narrow", 2).await);
    (p, partitions)
}

// Eight partitions of two dedup topics may each hold 10,000 identities, and
// the broker may hold 3,000. After every round of produces to all of them,
// the broker gauge is at most 3,000 and equals the sum of the partition
// gauges, and no produce fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_broker_budget_bounds_the_sum_of_every_partition_index() {
    let (p, partitions) = eight_partitions().await;

    let mut rounds = Vec::new();
    for round in 0..10 {
        sequential_round(&p.client, &partitions, round, Arrival::Spread).await;
        rounds.push(broker_and_partition_sum(&p.broker, &partitions).await);
    }
    let limit = broker_gauge(&p.broker, "dedup_broker_index_max_entries").await;
    eprintln!("broker and partition-sum gauges per round: {rounds:?}");

    assert!(limit == BROKER_LIMIT);
    for (round, &(broker, sum)) in rounds.iter().enumerate() {
        assert!(broker <= BROKER_LIMIT, "round {round}");
        assert!(broker == sum, "round {round}");
    }
    assert!(rounds.last().unwrap().0 >= BROKER_LIMIT * 9 / 10);
    p.broker.shutdown().await;
}

// The same budget with every partition written at once, with identities
// that arrive in a burst so that no partition can trim below the limit and
// every round runs into it. The indexes then check the shared count from
// several threads together. Each check reads the count before its own insert
// adds to it, so the total can pass the limit by at most one entry for each
// other partition inserting at that moment, and no further: a new identity
// at the limit makes room first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_partitions_pass_the_broker_budget_by_at_most_one_entry_each() {
    let (p, partitions) = eight_partitions().await;
    let others = u64::try_from(partitions.len()).unwrap() - 1;

    let mut rounds = Vec::new();
    for round in 0..20 {
        concurrent_round(&p.client, &partitions, round, Arrival::Burst).await;
        // Gauges are set by whichever partition reported last, which can be a
        // report that read the shared count before another partition's last
        // change. The same identities once more to every partition, one after
        // another, make every report current.
        sequential_round(&p.client, &partitions, round, Arrival::Burst).await;
        rounds.push(broker_and_partition_sum(&p.broker, &partitions).await);
    }
    eprintln!("broker and partition-sum gauges per round: {rounds:?}");

    assert!(rounds.iter().any(|&(broker, _)| broker >= BROKER_LIMIT));
    for (round, &(broker, sum)) in rounds.iter().enumerate() {
        assert!(broker <= BROKER_LIMIT + others, "round {round}");
        assert!(broker == sum, "round {round}");
    }
    p.broker.shutdown().await;
}

const RELEASE_BROKER_LIMIT: u64 = 2_000;

// A broker whose budget of `RELEASE_BROKER_LIMIT` entries the dedup topic
// `first` fills, and the dedup topic `second` that then cannot index the 500
// identities it receives.
async fn full_budget() -> (InProcess, Partition<'static>) {
    let p = start_configured(|config| {
        config.dedup_max_entries_per_partition = 10_000;
        config.dedup_max_entries = usize::try_from(RELEASE_BROKER_LIMIT).unwrap();
    })
    .await;
    let full = dedup_partitions(&p.broker, &p.client, "first", 1).await[0];
    let starved = dedup_partitions(&p.broker, &p.client, "second", 1).await[0];
    for first in (0..4_000).step_by(usize::try_from(PER_BATCH).unwrap()) {
        let records = numbered().keys(first..first + PER_BATCH).call();
        timed_produce(&p.client, full, &records).await;
    }
    let records = numbered().prefix("starved").keys(0..PER_BATCH).call();
    timed_produce(&p.client, starved, &records).await;
    (p, starved)
}

#[derive(Debug, PartialEq)]
struct ReleaseOutcome {
    // The broker gauge with the budget full.
    full: u64,
    // The starved topic's index with the budget full.
    starved: u64,
    // The broker gauge, and the starved topic's index, after it indexes
    // again with the other topic's entries released.
    reindexed: (u64, u64),
}

// Run `release` against a full budget, then index 500 new identities on the
// topic that could not index before.
async fn release_and_reindex<F, Fut>(release: F) -> ReleaseOutcome
where
    F: FnOnce(InProcess) -> Fut,
    Fut: Future<Output = InProcess>,
{
    let (p, starved) = full_budget().await;
    let full_gauge = broker_gauge(&p.broker, "dedup_broker_index_entries").await;
    let starved_gauge = index_entries(&p.broker, starved).await;

    let p = release(p).await;
    let records = numbered().prefix("again").keys(0..PER_BATCH).call();
    timed_produce(&p.client, starved, &records).await;
    let reindexed = (
        broker_gauge(&p.broker, "dedup_broker_index_entries").await,
        index_entries(&p.broker, starved).await,
    );
    p.broker.shutdown().await;
    ReleaseOutcome {
        full: full_gauge,
        starved: starved_gauge,
        reindexed,
    }
}

// Deleting a dedup topic gives its entries back to the broker budget: a
// topic that could not index at the full budget indexes all 500 of its new
// identities afterwards, and the broker gauge holds only that topic's index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_dedup_topic_returns_its_entries_to_the_broker_budget() {
    let outcome = release_and_reindex(|p| async move {
        delete_topic(&p.client, "first").await;
        p
    })
    .await;

    let starved = outcome.starved;
    assert!(outcome.full == RELEASE_BROKER_LIMIT);
    assert!(starved < 10);
    assert!(outcome.reindexed == (starved + 500, starved + 500));
}

// Turning dedup off with `dedup.window.ms=-1` gives the topic's entries back
// at once, in the gauges too, and another topic then indexes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_dedup_off_returns_its_entries_to_the_broker_budget() {
    let mut released = (u64::MAX, u64::MAX);
    let outcome = release_and_reindex(|p| {
        let released = &mut released;
        async move {
            let altered = alter_topic_config()
                .client(&p.client)
                .topic("first")
                .name("dedup.window.ms")
                .value("-1")
                .call()
                .await;
            assert!(altered.error_code == 0);
            let target = Partition {
                topic: "first",
                topic_id: topic_id_for(&p.client, "first").await,
                index: 0,
            };
            let index = eventually(0, || index_entries(&p.broker, target)).await;
            let starved = index_entries(&p.broker, Partition {
                topic: "second",
                ..target
            })
            .await;
            let broker = eventually(starved, || {
                broker_gauge(&p.broker, "dedup_broker_index_entries")
            })
            .await;
            *released = (index, broker - starved);
            p
        }
    })
    .await;

    let starved = outcome.starved;
    assert!(released == (0, 0));
    assert!(outcome.full == RELEASE_BROKER_LIMIT);
    assert!(outcome.reindexed == (starved + 500, starved + 500));
}

// The broker gauge follows a deleted topic down without waiting for another
// topic to produce: the deleted topic's writer drops its index, which gives
// the entries back to the budget and publishes the budget's new size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_dedup_topic_lowers_the_broker_gauge_at_once() {
    let (p, starved) = full_budget().await;
    let starved_gauge = index_entries(&p.broker, starved).await;

    delete_topic(&p.client, "first").await;
    let broker = eventually(starved_gauge, || {
        broker_gauge(&p.broker, "dedup_broker_index_entries")
    })
    .await;

    assert!(broker == starved_gauge);
    p.broker.shutdown().await;
}

// A broker with no dedup topic holds no index: the broker gauge is zero
// before and after a plain topic takes 5,000 keyed records, and the plain
// topic reports no index of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_without_dedup_topics_holds_no_index() {
    let p = start().await;
    let before = broker_gauge(&p.broker, "dedup_broker_index_entries").await;
    let topic_id = create_topic_with(&p.client, CreateTopicSetup {
        topic: "plain",
        num_partitions: TopicPartitionCount(1),
        ..Default::default()
    })
    .await;
    p.broker.wait_until_partition_present("plain", 0).await;
    let target = Partition {
        topic: "plain",
        topic_id,
        index: 0,
    };
    for first in (0..5_000).step_by(usize::try_from(PER_BATCH).unwrap()) {
        let records = numbered().keys(first..first + PER_BATCH).call();
        timed_produce(&p.client, target, &records).await;
    }

    let gauges = (
        before,
        broker_gauge(&p.broker, "dedup_broker_index_entries").await,
        broker_gauge(&p.broker, "dedup_broker_index_max_entries").await,
        index_entries(&p.broker, target).await,
    );
    assert!(gauges == (0, 0, 16_000_000, 0));
    p.broker.shutdown().await;
}
