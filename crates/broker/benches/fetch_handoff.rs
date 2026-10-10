//! `CodSpeed` microbenchmarks for the thread-pool hand-off in the fetch read
//! loop.
//!
//! A consumer subscribed to many partitions turns one Fetch request into one
//! blocking log read per partition. How those reads reach a blocking thread is
//! a free choice, and the three ways of making it differ by a fixed cost per
//! read that a wide subscription pays once per partition:
//!
//!   * `spawn_blocking_per_partition` — one `spawn_blocking` per partition,
//!     awaited in turn. Every partition pays a task allocation, a queue push, a
//!     pool wakeup and a `JoinHandle` await, and the awaits serialize;
//!   * `spawn_blocking_batched` — one `spawn_blocking` for the whole pending
//!     set, which pays that cost once per fetch however wide it is;
//!   * `block_in_place` — no hand-off at all: one call per partition, as the
//!     read loop makes it, and the reads run on the reactor worker after tokio
//!     moves its other tasks to a fresh worker.
//!
//! The arms mirror `bench_append_handoff` in `krabka-log`'s suite, which
//! settled the same question for the append side, so the two sets of numbers
//! are comparable.
//!
//! The measured work is deliberately a warm read: the fetch offset is the same
//! on every iteration, so the bytes come from page cache and what remains
//! around them is the hand-off. That is the case the hand-off cost matters in.
//! A consumer that fetches 200 partitions and finds a handful of records on
//! each pays the per-partition cost 200 times over a few hundred microseconds
//! of real reading.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use krabka_log::{Log, LogConfig, Offset};
use krabka_units::prelude::{ByteSize, mebibytes};
use tempfile::TempDir;

/// Partition counts: a single-partition fetch, a modest subscription, and the
/// wide one from the issue that motivated the measurement.
const PARTITION_COUNTS: [usize; 3] = [1, 16, 200];

/// Record shapes per partition, as `(name, records, payload)`.
///
/// The small shape is the one a wide subscription actually sees: a consumer on
/// 200 partitions rarely finds a full `fetch.max.bytes` waiting on each. The
/// larger shape is the check that the hand-off stops mattering once there are
/// real bytes to move.
const SHAPES: [(&str, i32, usize); 2] = [("1rec_1KiB", 1, 1024), ("100rec_1KiB", 100, 1024)];

/// A read budget larger than any partition the suite builds, so the read is
/// never clipped and every arm moves the same bytes.
const UNBOUNDED: ByteSize = mebibytes(64);

/// One partition's log, its read limit, and the temp dir that backs it.
struct BenchPartition {
    _dir: TempDir,
    log: Mutex<Log>,
    limit: Offset,
}

krabka_macros::record_batch_fixture!(make_batch);

/// `count` partition logs, each holding one batch of the given shape.
fn partitions(count: usize, records: i32, payload: usize) -> Arc<Vec<BenchPartition>> {
    Arc::new(
        (0..count)
            .map(|_| {
                let dir = tempfile::tempdir().expect("temp dir");
                let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");
                log.append(&mut make_batch(records, payload))
                    .expect("append the partition's batch");
                let limit = log.log_end_offset();
                BenchPartition {
                    _dir: dir,
                    log: Mutex::new(log),
                    limit,
                }
            })
            .collect(),
    )
}

/// The blocking work one partition of a fetch does: take the log mutex, seek,
/// and copy the records run out.
fn read_one(partition: &BenchPartition) -> usize {
    let log = partition.log.lock().expect("log mutex poisoned");
    log.read_raw(Offset(0), partition.limit, UNBOUNDED)
        .expect("the bench log always holds the range it is asked for")
        .bytes
        .len()
}

fn read_all(partitions: &[BenchPartition]) -> usize {
    partitions.iter().map(read_one).sum()
}

/// The three hand-off shapes, in report order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Handoff {
    /// One `spawn_blocking` per partition, awaited in turn.
    PerPartition,
    /// One `spawn_blocking` covering the whole pending set.
    Batched,
    /// No hand-off: one `block_in_place` per partition on the reactor worker,
    /// which is the shape the read loop actually has -- `do_read` is called
    /// once per partition, so the choice this arm stands for is made once per
    /// partition too. The first call hands the worker's core to a replacement
    /// thread and the rest find no core left to hand over, so the per-call
    /// cost after the first is a thread-local check.
    InPlace,
}

impl Handoff {
    const ALL: [Self; 3] = [Self::PerPartition, Self::Batched, Self::InPlace];

    fn name(self) -> &'static str {
        match self {
            Self::PerPartition => "spawn_blocking_per_partition",
            Self::Batched => "spawn_blocking_batched",
            Self::InPlace => "block_in_place",
        }
    }
}

/// Serve one fetch across every partition, under one hand-off shape.
async fn serve_fetch(handoff: Handoff, partitions: &Arc<Vec<BenchPartition>>) -> usize {
    match handoff {
        Handoff::PerPartition => {
            let mut bytes = 0;
            for index in 0..partitions.len() {
                let partitions = Arc::clone(partitions);
                bytes += tokio::task::spawn_blocking(move || read_one(&partitions[index]))
                    .await
                    .expect("the read task does not panic");
            }
            bytes
        }
        Handoff::Batched => {
            let partitions = Arc::clone(partitions);
            tokio::task::spawn_blocking(move || read_all(&partitions))
                .await
                .expect("the read task does not panic")
        }
        Handoff::InPlace => partitions
            .iter()
            .map(|partition| tokio::task::block_in_place(|| read_one(partition)))
            .sum(),
    }
}

/// A multi-threaded runtime, which is what the broker's `#[tokio::main]` gives
/// it and what `block_in_place` requires: it panics on a current-thread
/// runtime, so the `InPlace` arm cannot even be measured without one.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("build a multi-threaded runtime")
}

/// Serve `iters` fetches, returning only the time they took.
fn timed_fetches(
    runtime: &tokio::runtime::Runtime,
    handoff: Handoff,
    partitions: &Arc<Vec<BenchPartition>>,
    iters: u64,
) -> Duration {
    runtime.block_on(async {
        // One untimed fetch, so the pool's threads are already spun up and the
        // partition files are already in page cache when the clock starts.
        serve_fetch(handoff, partitions).await;
        let start = Instant::now();
        for _ in 0..iters {
            serve_fetch(handoff, partitions).await;
        }
        start.elapsed()
    })
}

fn bench_fetch_handoff(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/fetch_handoff");
    let runtime = runtime();

    for (shape, records, payload) in SHAPES {
        for count in PARTITION_COUNTS {
            let partitions = partitions(count, records, payload);
            for handoff in Handoff::ALL {
                group.bench_function(format!("{shape}/{count}p/{}", handoff.name()), |b| {
                    b.iter_custom(|iters| timed_fetches(&runtime, handoff, &partitions, iters));
                });
            }
        }
    }

    group.finish();
}

/// Print microseconds per fetch for each hand-off shape, and the two ratios
/// against the per-partition arm the read loop starts from.
///
/// Criterion compares a benchmark against its own previous run, not against a
/// sibling, so the number this suite exists for — what the per-partition
/// hand-off costs against batching it — has to be computed here.
fn bench_ratio(_c: &mut Criterion) {
    /// Fetches per measurement run.
    const SAMPLES: u32 = 50;
    /// Measurement runs per case, of which the fastest is the one reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else. Interference only ever adds time, so the minimum over a few runs
    /// is the estimator it cannot pull the wrong way.
    const RUNS: u32 = 5;

    let runtime = runtime();
    println!(
        "\nbroker/fetch_handoff — microseconds per fetch (fastest of {RUNS} runs), and what the per-partition hand-off costs"
    );
    println!(
        "{:<14} {:>6} {:>14} {:>14} {:>14} {:>14} {:>14}",
        "shape",
        "parts",
        "per_partition",
        "batched",
        "block_in_place",
        "per/batched",
        "per/in_place"
    );
    for (shape, records, payload) in SHAPES {
        for count in PARTITION_COUNTS {
            let partitions = partitions(count, records, payload);
            let micros: Vec<f64> = Handoff::ALL
                .into_iter()
                .map(|handoff| {
                    (0..RUNS)
                        .map(|_| {
                            timed_fetches(&runtime, handoff, &partitions, u64::from(SAMPLES))
                                .as_secs_f64()
                                * 1e6
                                / f64::from(SAMPLES)
                        })
                        .fold(f64::INFINITY, f64::min)
                })
                .collect();
            let (per_partition, batched, in_place) = (micros[0], micros[1], micros[2]);
            println!(
                "{shape:<14} {count:>6} {per_partition:>14.1} {batched:>14.1} {in_place:>14.1} {:>13.2}x {:>13.2}x",
                per_partition / batched,
                per_partition / in_place
            );
        }
    }
    println!();
}

criterion_group!(benches, bench_fetch_handoff, bench_ratio);
criterion_main!(benches);
