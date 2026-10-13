//! `CodSpeed` microbenchmarks for the topic label a metric observation builds.
//!
//! The broker's partition registry is a nested `DashMap`
//! rather than a flat one keyed by `(String, i32)` so that resolving a
//! partition allocates nothing. The metric recorded immediately beside that
//! lookup used to allocate a `String` for its label anyway, once per
//! observation, which put the allocation back on the path the registry's shape
//! exists to keep it off. The label sets now hold an `Arc<str>` and the
//! registry hands out the copy it already keys the topic by.
//!
//! This suite puts a number on that. Two cases, one topic, one partition:
//!
//!   * `owned_string` — the previous shape, reconstructed here: a
//!     `String`-labelled counter family and a recorder that copies the topic
//!     name into a fresh label on every call;
//!   * `shared_arc` — the current shape:
//!     [`BrokerMetrics::record_replication_in`], which clones the caller's
//!     `Arc<str>` into the label.
//!
//! Everything else is held equal. Both cases hash the same bytes, find the
//! same single entry, and bump the same `Counter`, so what separates them is
//! the label's allocation and nothing else.
//!
//! One asymmetry is left in on purpose, and it runs against the measured win
//! rather than for it: `owned_string` is defined in this file and inlines,
//! while `shared_arc` is a call across the crate boundary into `krabka-broker`
//! that nothing in this repository's profiles enables LTO for. The `Arc` side
//! therefore pays a real call the `String` side does not, so the ratio below
//! understates the difference rather than flattering it. Removing the
//! asymmetry would mean either copying `record_replication_in` into the bench
//! — measuring a reconstruction on both sides and the shipped recorder on
//! neither — or turning on LTO for a comparison that is about an allocation.
//!
//! `bench_ratio` prints nanoseconds per observation for both, and the ratio,
//! because criterion compares a benchmark against its own previous run and
//! never against a sibling — the before-and-after number this suite exists for
//! has to be computed here.
//!
//! Run it with `cargo bench -p krabka-broker --bench metric_labels`. This
//! repository has no benchmark CI workflow; nothing runs it for you.

use {
    assert2::assert,
    criterion::{Criterion, Throughput, criterion_group, criterion_main},
    krabka_broker::metrics::BrokerMetrics,
    prometheus_client::{
        encoding::EncodeLabelSet,
        metrics::{counter::Counter, family::Family},
    },
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// Observations per measured run.
///
/// The acceptance this suite was written for is a million calls against one
/// topic, which is also the shape that matters: a single label entry, found
/// on every call, so the measurement is the label's cost and not the map's.
const CALLS: u32 = 1_000_000;

/// The topic every observation is recorded against.
const TOPIC: &str = "orders";

/// The partition every observation is recorded against.
const PARTITION: i32 = 7;

/// Bytes per observation. Non-zero because both recorders skip a zero.
const BYTES: u64 = 4096;

/// `PartitionLabel` exactly as it read before `topic` became an `Arc<str>`.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OwnedPartitionLabel {
    topic: String,
    partition: i32,
}

/// `BrokerMetrics::record_replication_in` exactly as it read before `topic`
/// became an `Arc<str>`: the same zero guard, the same label build, the same
/// `get_or_create` and `inc_by`, over a `String`-labelled family.
fn record_replication_in_owned(
    family: &Family<OwnedPartitionLabel, Counter>,
    topic: &str,
    partition: i32,
    bytes: u64,
) {
    if bytes == 0 {
        return;
    }
    let lbl = OwnedPartitionLabel {
        topic: topic.to_string(),
        partition,
    };
    family.get_or_create(&lbl).inc_by(bytes);
}

/// Time `calls` observations through the previous, `String`-labelled shape.
///
/// The assertion is what keeps the number meaningful: it says the family
/// really took `calls` observations, so a run that was optimised away would
/// fail rather than report the copy as free.
fn timed_owned(calls: u64) -> Duration {
    let family = Family::<OwnedPartitionLabel, Counter>::default();
    let start = Instant::now();
    for _ in 0..calls {
        record_replication_in_owned(&family, TOPIC, PARTITION, BYTES);
    }
    let elapsed = start.elapsed();
    let total = family
        .get_or_create(&OwnedPartitionLabel {
            topic: TOPIC.to_string(),
            partition: PARTITION,
        })
        .get();
    assert!(total == calls * BYTES, "the owned case recorded every call");
    elapsed
}

/// Time `calls` observations through the current shape, where the caller
/// holds the topic name the way a replicator task holds it.
///
/// Two assertions. The counter total says the calls happened. The strong
/// count says what this change is actually about: the family's stored label
/// is a clone of the caller's handle, not a fresh copy of the bytes, so a
/// change that reintroduced an owned copy here would fail the run instead of
/// quietly widening the gap it is supposed to have closed.
fn timed_shared(calls: u64) -> Duration {
    let metrics = BrokerMetrics::new();
    let topic: Arc<str> = Arc::from(TOPIC);
    let before = Arc::strong_count(&topic);
    let start = Instant::now();
    for _ in 0..calls {
        metrics.record_replication_in(&topic, PARTITION, BYTES);
    }
    let elapsed = start.elapsed();
    let total = metrics
        .replication_bytes_in
        .get_or_create(&krabka_broker::metrics::PartitionLabel {
            topic: Arc::clone(&topic),
            partition: PARTITION,
        })
        .get();
    assert!(
        total == calls * BYTES,
        "the shared case recorded every call"
    );
    assert!(
        Arc::strong_count(&topic) > before,
        "the label kept a clone of the caller's Arc rather than a copy"
    );
    elapsed
}

/// One case's measured run: take that many observations, return only the time
/// they took.
type TimedRun = fn(u64) -> Duration;

/// One case: the name it is reported under and the run that measures it.
struct Case {
    name: &'static str,
    run: TimedRun,
}

/// The two cases, in report order.
const CASES: [Case; 2] = [
    Case {
        name: "owned_string",
        run: timed_owned,
    },
    Case {
        name: "shared_arc",
        run: timed_shared,
    },
];

fn bench_replication_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/metric_labels");
    group.throughput(Throughput::Elements(u64::from(CALLS)));
    // A single sample is a million observations, so the default sample count
    // would spend minutes on two cases that are already stable at ten.
    group.sample_size(10);

    for Case { name, run } in CASES {
        group.bench_function(format!("record_replication_in/{name}"), |b| {
            b.iter_custom(|iters| (0..iters).map(|_| run(u64::from(CALLS))).sum());
        });
    }

    group.finish();
}

/// Print nanoseconds per observation for both shapes and the ratio between
/// them — what a `String` label per observation cost against an `Arc` clone.
fn bench_ratio(_c: &mut Criterion) {
    /// Measurement runs per case, of which the fastest is reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else. Interference only ever adds time, so the minimum over a few runs
    /// is the estimator it cannot pull the wrong way.
    const RUNS: u32 = 5;

    println!(
        "\nbroker/metric_labels — nanoseconds per record_replication_in, \
         {CALLS} calls on one topic (fastest of {RUNS} runs)"
    );
    println!(
        "{:>14} {:>14} {:>18}",
        "owned_string", "shared_arc", "owned/shared"
    );
    let nanos: Vec<f64> = CASES
        .into_iter()
        .map(|Case { run, .. }| {
            (0..RUNS)
                .map(|_| run(u64::from(CALLS)).as_secs_f64() * 1.0e9 / f64::from(CALLS))
                .fold(f64::INFINITY, f64::min)
        })
        .collect();
    let (owned, shared) = (nanos[0], nanos[1]);
    println!("{owned:>14.1} {shared:>14.1} {:>17.2}x", owned / shared);
    println!();
}

criterion_group!(benches, bench_replication_in, bench_ratio);
criterion_main!(benches);
