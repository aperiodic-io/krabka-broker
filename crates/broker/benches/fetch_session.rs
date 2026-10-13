//! `CodSpeed` microbenchmarks for the KIP-227 fetch-session cache.
//!
//! A broker whose session cache is full is the steady state for a large
//! consumer fleet, not an edge case: every consumer that reconnects asks for a
//! session, and every one of those allocations has to displace someone. The
//! cache answers that with the global fetch mutex held, so whatever the
//! allocation costs, every concurrent `classify` waits for it. This suite puts
//! a number on the part of that cost which used to grow with occupancy.
//!
//! The timed unit is one session's whole turn through the cache, as the fetch
//! handler drives it: `classify` on a `(session_id = 0, session_epoch = 0)`
//! request, which returns `NewSession`, then `try_allocate` for the partition
//! set the broker just served, and then the retirement that balances the
//! insertion. Building that partition set is setup, so it happens outside the
//! timer.
//!
//! Counting the retirement matters, because *where* it happens is exactly what
//! occupancy changes. In a full cache the allocation evicts, so the teardown is
//! inside `try_allocate`; below capacity nothing is displaced and the session
//! leaves later through `close`. Timing only the allocation would charge the
//! full cache for a teardown the others do off the clock, and report a slowdown
//! that is really just bookkeeping moved across the timer.
//!
//! Two axes:
//!   * **occupancy** — the cache is pre-filled to 0%, 50% or 100% of
//!     `max_incremental_fetch_session_cache_slots` before the measurement. At
//!     100% every allocation evicts, which is the case that used to scan every
//!     live session to pick its victim. The cache runs on a manual clock that
//!     each turn moves past Kafka's two-minute staleness threshold, because
//!     Kafka refuses a newcomer while every session is in use: without that
//!     the 100% rows would time the refusal and not the eviction.
//!   * **concurrency** — 1, 8 or 64 threads share one cache, which is what
//!     puts the allocation on the critical path of everybody else's fetch.
//!
//! The two regimes still differ in one way that no arrangement can remove: a
//! full cache retires the old session inside the same lock the new one takes,
//! where the others pay for a second acquisition. That costs a few nanoseconds
//! uncontended and rather more at 64 threads, so read the single-threaded row
//! for the cost of the allocation itself and the wide rows for what contention
//! does to it.
//!
//! `bench_occupancy_ratio` prints the 100%-over-0% ratio per thread count, so
//! a run answers "does a full cache cost more to allocate into" without
//! reading the criterion report. Criterion compares a benchmark against its
//! own previous run and never against a sibling, so that number has to be
//! computed here.

use {
    assert2::assert,
    criterion::{Criterion, criterion_group, criterion_main},
    krabka_broker::{
        config::DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
        fetch_session::{
            CachedPartitionState, FetchSessionCache, FetchSessionKey, INITIAL_EPOCH,
            INVALID_SESSION_ID, SessionDecision,
        },
    },
    krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        primitives::uuid::Uuid as WireUuid,
    },
    qubit_clock::ManualMonotonicClock,
    std::{
        sync::Arc,
        thread,
        time::{Duration, Instant},
    },
};

/// Cache capacity, taken from the broker's own default so the bench measures
/// the size a real broker runs at.
const SLOTS: usize = DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS;

/// Partitions each session subscribes to.
///
/// Enough that a session is a real map rather than an empty struct, and few
/// enough that the per-session `HashMap` build does not swamp the thing under
/// test. It is the same for every row, so it cancels out of the ratios.
const PARTITIONS: i32 = 8;

/// Cache fill, as a percentage of [`SLOTS`], before the measurement starts.
const OCCUPANCY: [usize; 3] = [0, 50, 100];

/// Threads sharing the one cache.
const CONCURRENCY: [u32; 3] = [1, 8, 64];

const TOPIC: &str = "bench-topic";
const PRINCIPAL: &str = "bench-principal";
/// The requests below name their topic, as `Fetch` v12 and earlier do.
const NAME_FETCH_VERSION: i16 = 12;

/// One partition of a session's partition set: its key and cached state.
#[derive(Clone)]
struct CachedPartition {
    key: FetchSessionKey,
    state: CachedPartitionState,
}

/// The partition set the handler hands `try_allocate` after serving a fetch.
fn partition_set() -> Vec<CachedPartition> {
    (0..PARTITIONS)
        .map(|partition| CachedPartition {
            key: FetchSessionKey {
                topic_name: TOPIC.to_string(),
                topic_id: WireUuid::ZERO,
                partition,
            },
            state: CachedPartitionState {
                max_bytes: 1024 * 1024,
                ..CachedPartitionState::default()
            },
        })
        .collect()
}

/// A client opening a session: `session_id = 0`, `session_epoch = 0`.
fn new_session_request() -> FetchRequest {
    FetchRequest {
        session_id: INVALID_SESSION_ID,
        session_epoch: INITIAL_EPOCH,
        topics: vec![FetchTopic {
            topic: TOPIC.to_string(),
            topic_id: WireUuid::ZERO,
            partitions: (0..PARTITIONS)
                .map(|partition| FetchPartition {
                    partition,
                    partition_max_bytes: 1024 * 1024,
                    ..FetchPartition::default()
                })
                .collect(),
            ..FetchTopic::default()
        }],
        ..FetchRequest::default()
    }
}

/// More than Kafka's two-minute staleness threshold, which the clock advances
/// by before each turn so the least recently used session is always stale.
const STALE: Duration = Duration::from_secs(121);

/// A cache and the manual clock it stamps sessions from.
struct ClockedCache {
    cache: FetchSessionCache,
    clock: Arc<ManualMonotonicClock>,
}

/// A cache holding `occupancy` percent of [`SLOTS`] live consumer sessions,
/// and the manual clock it stamps sessions from.
fn prefilled(occupancy: usize) -> ClockedCache {
    let clock = ManualMonotonicClock::new_shared();
    let cache = FetchSessionCache::with_clock(SLOTS, clock.clone());
    let template = partition_set();
    for _ in 0..(SLOTS * occupancy / 100) {
        let partitions = template
            .iter()
            .map(|partition| (partition.key.clone(), partition.state.clone()))
            .collect();
        let id = cache.try_allocate(false, false, PRINCIPAL.to_string(), partitions);
        assert!(id != INVALID_SESSION_ID, "prefill was refused a session");
    }
    ClockedCache { cache, clock }
}

/// Borrowed views of a [`ClockedCache`], which worker threads share.
struct CacheRefs<'a> {
    cache: &'a FetchSessionCache,
    clock: &'a ManualMonotonicClock,
}

/// Run `ops` session turns against `cache`, returning the time they took.
///
/// `close_explicitly` is set below 100% occupancy, where the allocation
/// displaces nobody. It both balances the insertion — otherwise the measured
/// allocations would fill the cache and turn every row into the 100% row — and
/// keeps the timed work the same across occupancy levels. At 100% the
/// allocation evicts one session per insertion, so the level holds on its own
/// and the retirement is already accounted for.
///
/// Timing each turn individually, rather than the loop as a whole, keeps the
/// partition-set clone out of the number. The two `Instant` reads are a
/// constant addition to every row, so the ratios this suite reports are
/// unaffected.
fn timed_session_turns(
    CacheRefs { cache, clock }: CacheRefs<'_>,
    request: &FetchRequest,
    template: &[CachedPartition],
    close_explicitly: bool,
    ops: u64,
) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..ops {
        let partitions = template
            .iter()
            .map(|partition| (partition.key.clone(), partition.state.clone()))
            .collect();
        let principal = PRINCIPAL.to_string();
        clock.advance(STALE).expect("manual time moves forward");

        let start = Instant::now();
        let decision = cache.classify(request, NAME_FETCH_VERSION);
        let id = cache.try_allocate(false, false, principal, partitions);
        if close_explicitly {
            cache.close(id);
        }
        elapsed += start.elapsed();

        // A refused allocation skips the eviction this suite exists to
        // measure, and a decision other than `NewSession` would mean the
        // request stopped being the one the fetch handler builds.
        assert!(matches!(decision, SessionDecision::NewSession));
        assert!(id != INVALID_SESSION_ID, "allocation was refused");
    }
    elapsed
}

/// Spread `iters` session turns over `threads` callers of one cache, and
/// return the total time those turns took.
///
/// Summing the per-thread totals rather than taking the wall clock keeps the
/// thread spawn out of the measurement and makes `total / iters` the mean
/// latency of one turn, contention included.
fn timed_across_threads(occupancy: usize, threads: u32, iters: u64) -> Duration {
    let ClockedCache { cache, clock } = prefilled(occupancy);
    let cache = Arc::new(cache);
    let request = new_session_request();
    let template = partition_set();
    let close_explicitly = occupancy < 100;

    let threads = u64::from(threads);
    thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|worker| {
                // Hand out the remainder one op at a time, so the workers
                // together run exactly `iters` turns.
                let ops = iters / threads + u64::from(worker < iters % threads);
                let cache = Arc::clone(&cache);
                let clock = Arc::clone(&clock);
                let request = &request;
                let template = &template;
                scope.spawn(move || {
                    timed_session_turns(
                        CacheRefs {
                            cache: &cache,
                            clock: &clock,
                        },
                        request,
                        template,
                        close_explicitly,
                        ops,
                    )
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("a bench worker panicked"))
            .sum()
    })
}

fn bench_fetch_session_allocate(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/fetch_session");

    for occupancy in OCCUPANCY {
        for threads in CONCURRENCY {
            group.bench_function(format!("full{occupancy}pct/threads{threads}"), |b| {
                b.iter_custom(|iters| timed_across_threads(occupancy, threads, iters));
            });
        }
    }

    group.finish();
}

/// Print the per-turn cost at each occupancy, and what a full cache costs over
/// an empty one.
///
/// That last column is the claim the O(1) victim lookup exists to support: it
/// should stay near 1 as occupancy rises, where a scan of the session map made
/// it grow with [`SLOTS`].
fn bench_occupancy_ratio(_c: &mut Criterion) {
    /// Session turns per measurement run, shared out across the workers. It is
    /// a total rather than a per-worker count, so the 64-thread row does not
    /// run 64 times the work of the single-threaded one.
    const SAMPLES: u32 = 20_000;
    /// Measurement runs per case, of which the fastest is the one reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else, and the wide rows are the worst of it: 64 threads on a busy box
    /// spend most of a run descheduled. Interference only ever adds time, so
    /// the minimum over several runs is the estimator it cannot pull the wrong
    /// way, and the wide rows want more of them than the narrow ones.
    const RUNS: u32 = 15;

    println!(
        "\nbroker/fetch_session — nanoseconds per session turn at {SLOTS} slots: classify,\ntry_allocate and the retirement that balances it, including time spent waiting\non the cache mutex (fastest of {RUNS} runs)"
    );
    println!(
        "{:<10} {:>12} {:>12} {:>12} {:>16}",
        "threads", "0% full", "50% full", "100% full", "100%/0%"
    );
    for threads in CONCURRENCY {
        let means: Vec<f64> = OCCUPANCY
            .into_iter()
            .map(|occupancy| {
                (0..RUNS)
                    .map(|_| {
                        let elapsed = timed_across_threads(occupancy, threads, u64::from(SAMPLES));
                        elapsed.as_secs_f64() * 1.0e9 / f64::from(SAMPLES)
                    })
                    .fold(f64::INFINITY, f64::min)
            })
            .collect();
        let (empty, half, full) = (means[0], means[1], means[2]);
        println!(
            "{threads:<10} {empty:>12.0} {half:>12.0} {full:>12.0} {:>15.2}x",
            full / empty
        );
    }
    println!();
}

criterion_group!(benches, bench_fetch_session_allocate, bench_occupancy_ratio);
criterion_main!(benches);
