//! Produce latency with a full index and with a growing one, against the same
//! workload on a topic without dedup.

use {
    crate::{
        Arrival, HOUR_MS, Latencies, Partition, counter, index_entries, numbered,
        support::{
            InProcess,
            client::create_topic_with,
            dedup::create_dedup_topic,
            start_configured,
            topics::{CreateTopicSetup, TopicPartitionCount},
        },
        timed_produce,
    },
    assert2::assert,
    std::time::Duration,
};

// A bound no single produce of this suite may reach in a debug build on a
// loaded machine. A produce that waits for a whole-index pass, or for a lock
// held across a rebuild, of the sizes these tests reach takes longer.
const STALL: Duration = Duration::from_secs(2);

// The relative bound: a dedup produce's tail latency may be this many times
// the baseline's, plus `SLACK`.
const FACTOR: u32 = 20;
const SLACK: Duration = Duration::from_millis(50);

fn within(measured: Duration, baseline: Duration) -> bool { measured < baseline * FACTOR + SLACK }

async fn dedup_topic<'a>(p: &InProcess, topic: &'a str) -> Partition<'a> {
    let topic_id = create_dedup_topic()
        .broker(&p.broker)
        .client(&p.client)
        .topic(topic)
        .window_ms(HOUR_MS)
        .call()
        .await;
    Partition {
        topic,
        topic_id,
        index: 0,
    }
}

async fn plain_topic<'a>(p: &InProcess, topic: &'a str) -> Partition<'a> {
    let topic_id = create_topic_with(&p.client, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(1),
        ..Default::default()
    })
    .await;
    p.broker.wait_until_partition_present(topic, 0).await;
    Partition {
        topic,
        topic_id,
        index: 0,
    }
}

// The latencies of one topic before and after its index is full.
#[derive(Debug, Default)]
struct Phases {
    filling: Latencies,
    saturated: Latencies,
}

// Produce `requests` requests of ten new identities each to a partition whose
// index is full, after a warm-up that fills it to 90%. Three topics take the
// same requests in turn: one whose identities arrive as a stream, so each
// new identity at the limit evicts the oldest entries; one whose identities
// arrive in a burst, so each is admitted unindexed; and one without dedup.
async fn produce_into_a_full_index(requests: u64) {
    const LIMIT: u64 = 1_000;
    const PER_REQUEST: u64 = 10;
    let p = start_configured(|config| {
        config.dedup_max_entries_per_partition = usize::try_from(LIMIT).unwrap();
    })
    .await;
    let topics = [
        (dedup_topic(&p, "evicting").await, Arrival::Spread),
        (dedup_topic(&p, "unindexed").await, Arrival::Burst),
        (plain_topic(&p, "plain").await, Arrival::Burst),
    ];
    let filling = LIMIT * 9 / 10 / PER_REQUEST;
    let full = LIMIT / PER_REQUEST;

    let mut phases: [Phases; 3] = Default::default();
    for request in 0..full + requests {
        let first = request * PER_REQUEST;
        for ((target, arrival), phase) in topics.iter().zip(&mut phases) {
            let records = numbered()
                .keys(first..first + PER_REQUEST)
                .arrival(*arrival)
                .call();
            let took = timed_produce(&p.client, *target, &records).await;
            if request < filling {
                phase.filling.push(took);
            } else if request >= full {
                phase.saturated.push(took);
            }
        }
    }
    let [(evicting, _), (unindexed, _), _] = topics;
    let metrics = (
        index_entries(&p.broker, evicting).await,
        counter(&p.broker, "dedup_capacity_evicted_entries_total", evicting).await,
        index_entries(&p.broker, unindexed).await,
        counter(&p.broker, "dedup_records_unindexed_total", unindexed).await,
    );
    for (name, phase) in ["evicting", "unindexed", "plain"].iter().zip(&phases) {
        eprintln!(
            "{name}: filling {}; saturated {}",
            phase.filling, phase.saturated
        );
    }
    eprintln!("index, evicted, index, unindexed: {metrics:?}");

    let [evicting_phases, unindexed_phases, plain] = &phases;
    let (evicting_index, evicted, unindexed_index, skipped) = metrics;
    assert!(evicting_index <= LIMIT);
    assert!(unindexed_index <= LIMIT);
    assert!(evicted > 0);
    assert!(skipped > 0);
    for (name, phase) in [
        ("evicting", evicting_phases),
        ("unindexed", unindexed_phases),
    ] {
        let saturated = &phase.saturated;
        assert!(saturated.max() < STALL, "{name}");
        assert!(
            within(saturated.p99(), phase.filling.p99()),
            "{name}: {saturated} against filling {}",
            phase.filling
        );
        assert!(
            within(saturated.p99(), plain.saturated.p99()),
            "{name}: {saturated} against plain {}",
            plain.saturated
        );
    }
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_latency_stays_flat_when_the_index_is_full() {
    produce_into_a_full_index(500).await;
}

// The 2,000-request variant, about a minute in a debug build:
// `cargo test -p krabka-broker --test dedup_bounds -- --ignored --nocapture
// produce_latency_stays_flat_over_2000_requests_into_a_full_index`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long: about a minute in a debug build"]
async fn produce_latency_stays_flat_over_2000_requests_into_a_full_index() {
    produce_into_a_full_index(2_000).await;
}

// Grow one partition's index to `identities` entries with requests of 1,000
// new identities, and the same requests to a topic without dedup. The index
// is 64 tables that each grow on their own, so no request moves more than a
// table's share of the index: the slowest request stays near the typical one
// however large the index already is, and every identity is indexed.
async fn grow_the_index(identities: u64) {
    const PER_REQUEST: u64 = 1_000;
    let p = start_configured(|config| {
        // Twice the identities, so that the index stays below the 90% where
        // capacity trimming starts and every identity stays indexed.
        config.dedup_max_entries_per_partition = 4_000_000;
        config.dedup_max_entries = 8_000_000;
    })
    .await;
    let growing = dedup_topic(&p, "growing").await;
    let plain = plain_topic(&p, "plain").await;

    let mut grown = Latencies::default();
    let mut baseline = Latencies::default();
    for first in (0..identities).step_by(usize::try_from(PER_REQUEST).unwrap()) {
        let records = numbered().keys(first..first + PER_REQUEST).call();
        grown.push(timed_produce(&p.client, growing, &records).await);
        baseline.push(timed_produce(&p.client, plain, &records).await);
    }
    let metrics = (
        index_entries(&p.broker, growing).await,
        counter(&p.broker, "dedup_records_unindexed_total", growing).await,
    );
    eprintln!(
        "growing to {identities}: dedup {grown}; plain {baseline}; index, unindexed {metrics:?}"
    );

    assert!(metrics == (identities, 0));
    assert!(grown.max() < STALL);
    // A spike proportional to the index would put the slowest request far
    // above the median of a run whose index grew a hundredfold.
    assert!(
        grown.max() < grown.median() * FACTOR + SLACK * 4,
        "dedup {grown}"
    );
    assert!(
        within(grown.p99(), baseline.p99()),
        "dedup {grown}; plain {baseline}"
    );
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_latency_has_no_spike_while_the_index_grows() { grow_the_index(300_000).await; }

// The two-million-identity variant, several minutes in a debug build:
// `cargo test -p krabka-broker --test dedup_bounds -- --ignored --nocapture
// produce_latency_has_no_spike_while_the_index_grows_to_two_million`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long: several minutes in a debug build"]
async fn produce_latency_has_no_spike_while_the_index_grows_to_two_million() {
    grow_the_index(2_000_000).await;
}
