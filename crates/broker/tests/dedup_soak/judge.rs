//! The verdicts of a run, printed whether they pass or fail.
//!
//! Three kinds of judgement, each over what [`Recorder`] kept:
//!
//! - **Continuous series** -- the resident set, the descriptors by kind and
//!   the summed index heap, sampled every few seconds -- go to
//!   [`drift::judge`]: a ceiling over every sample, and a least-squares slope
//!   over the second half of the run that may not rise by more than a
//!   fraction of that half's mean, or of a floor when the mean is below it.
//! - **Per-cycle series** -- the resident set and the descriptors right after
//!   each drain, and each cycle's produce p99 -- go to
//!   [`trend::judge_cycles`], which compares the medians of the first and
//!   last thirds of the cycles after the warm-up.
//! - **Coverage.** A soak that never reached a limit or never restarted a
//!   broker proved nothing about them, so each path the soak is for must have
//!   run at least once.
//!
//! The tolerances, and why each is what it is. The resident-set figures are
//! measured on this soak run as [`child`](crate::child) runs it, with one
//! allocator arena:
//!
//! | series | tolerance | why |
//! | --- | --- | --- |
//! | resident set, second-half slope | 10% of the mean, at least 12 MiB | as the resident set after each drain |
//! | resident set after each drain | 5% of the first third, at least 12 MiB | the process grows by about 0.75 MiB for every thread the blocking pool adds, and by 5 MiB at most between the thirds of a 90-second run; over 20 minutes the thirds rose 2.7 MiB. A leak of 100 KiB a cycle exceeds the floor within a 20-minute run |
//! | descriptors other than segment files, second-half slope | 10% of the mean, at least 25 | sockets and sidecars do not move once the cluster is up |
//! | descriptors other than segment files after each drain | 5% of the first third, at least 32 | as above |
//! | segment-file descriptors | the retention ceiling of [`Settings::segment_fd_ceiling`] | they swing by 200 within a few cycles as segments roll and retention deletes them, so only their ceiling means anything |
//! | descriptors of deleted files | the ceiling of [`Settings::deleted_fd_ceiling`], and at most 16 more after each drain | a deleted file that stays open is a handle that outlived its owner, and its disk space with it |
//! | index heap, second-half slope | 25% of the mean, at least the residual of every partition | the heap swings from residual to full every cycle |
//! | produce p99 per cycle | 100% of the first third, at least 100 ms | latency on a shared, loaded machine moves by tens of percent; a cost that grows with the history of the run doubles it |

use {
    crate::{
        drift::{self, Bound, BoundedSeries},
        process::ProcessSample,
        record::{CycleRow, Recorder, real},
        settings::{BROKERS, HOSTED_PARTITIONS, OTHER_FD_HEADROOM, Settings},
        trend::{self, Tolerance},
    },
    std::fmt::Display,
};

/// The rise of the resident set that is always allowed, in KiB.
const RSS_FLOOR_KIB: f64 = 12.0 * 1024.0;

/// The tolerance of the resident set after each drain.
const DRAINED_RSS: Tolerance = Tolerance {
    fraction: 0.05,
    floor: RSS_FLOOR_KIB,
};

/// The tolerance of the descriptors other than segment files after each
/// drain.
const DRAINED_OTHER_FDS: Tolerance = Tolerance {
    fraction: 0.05,
    floor: 32.0,
};

/// The tolerance of the descriptors of deleted files after each drain.
const DRAINED_DELETED_FDS: Tolerance = Tolerance {
    fraction: 0.0,
    floor: 16.0,
};

/// The tolerance of each cycle's produce p99, in microseconds.
const CYCLE_P99: Tolerance = Tolerance {
    fraction: 1.0,
    floor: 100_000.0,
};

/// One per-cycle series: what it is called, its unit and tolerance, and how
/// a cycle's row gives its value.
struct PerCycle {
    series: &'static str,
    unit: &'static str,
    tolerance: Tolerance,
    value: fn(&CycleRow) -> f64,
}

const PER_CYCLE: [PerCycle; 4] = [
    PerCycle {
        series: "process VmRSS after each drain",
        unit: "KiB",
        tolerance: DRAINED_RSS,
        value: |cycle| real(cycle.drained.rss_kib),
    },
    PerCycle {
        series: "process fds other than segment files after each drain",
        unit: "fds",
        tolerance: DRAINED_OTHER_FDS,
        value: |cycle| real(cycle.drained.fds.other()),
    },
    PerCycle {
        series: "process fds of deleted files after each drain",
        unit: "fds",
        tolerance: DRAINED_DELETED_FDS,
        value: |cycle| real(cycle.drained.fds.deleted),
    },
    PerCycle {
        series: "produce p99 per cycle",
        unit: "us",
        tolerance: CYCLE_P99,
        value: |cycle| real(u64::try_from(cycle.latency.p99.as_micros()).unwrap_or(u64::MAX)),
    },
];

/// Print every verdict of the run and return the failures among them.
///
/// `warm` is the process right after the last warm-up drain, when its peak
/// was reset; `last` is the process after the teardown.
pub fn judge(
    settings: &Settings,
    recorder: &Recorder,
    warm: ProcessSample,
    last: ProcessSample,
) -> Vec<String> {
    let mut failures = recorder.violations.clone();
    let mut report = |passed: bool, verdict: &dyn Display| {
        println!("VERDICT {} {verdict}", if passed { "pass" } else { "FAIL" });
        if !passed {
            failures.push(verdict.to_string());
        }
    };
    let peak_ceiling = settings.peak_rss_ceiling_kib(warm.rss_kib);
    for verdict in drift::judge_all(&continuous(settings, recorder, warm, peak_ceiling)) {
        report(!verdict.is_failure(), &verdict);
    }
    let judged = recorder
        .cycles
        .get(settings.warmup_cycles..)
        .unwrap_or_default();
    for PerCycle {
        series,
        unit,
        tolerance,
        value,
    } in PER_CYCLE
    {
        let values: Vec<f64> = judged.iter().map(value).collect();
        let verdict = trend::judge_cycles(series, unit, &values, tolerance);
        report(!verdict.is_failure(), &verdict);
    }
    // The kernel moves `VmHWM` only at some events, so a sample can read a
    // resident set above it: the peak is the higher of the two.
    let sampled = recorder
        .rss
        .points
        .iter()
        .map(|point| point.value)
        .fold(0.0, f64::max);
    let peak = real(last.peak_rss_kib).max(sampled);
    report(
        peak <= real(peak_ceiling),
        &format!(
            "peak VmRSS after the warm-up: {peak:.0} KiB, warmed-up {} KiB, ceiling \
             {peak_ceiling} KiB from the entry limits",
            warm.rss_kib
        ),
    );
    for (reached, what) in coverage(settings, recorder) {
        report(reached, &what);
    }
    failures
}

/// The continuous series and their bounds.
fn continuous(
    settings: &Settings,
    recorder: &Recorder,
    warm: ProcessSample,
    peak_ceiling: u64,
) -> Vec<BoundedSeries> {
    let all_partitions = BROKERS * HOSTED_PARTITIONS;
    let segment_fds = real(Settings::segment_fd_ceiling());
    let deleted_fds = real(Settings::deleted_fd_ceiling());
    vec![
        BoundedSeries {
            series: recorder.rss.clone(),
            bound: Bound {
                ceiling: real(peak_ceiling),
                drift_fraction: 0.10,
                floor: RSS_FLOOR_KIB / 0.10,
            },
        },
        BoundedSeries {
            series: recorder.other_fds.clone(),
            bound: Bound {
                ceiling: real(warm.fds.other() + OTHER_FD_HEADROOM),
                drift_fraction: 0.10,
                floor: 256.0,
            },
        },
        // Bounded by retention, not by time: only the ceiling binds.
        BoundedSeries {
            series: recorder.segment_fds.clone(),
            bound: Bound {
                ceiling: segment_fds,
                drift_fraction: 1.0,
                floor: segment_fds,
            },
        },
        BoundedSeries {
            series: recorder.deleted_fds.clone(),
            bound: Bound {
                ceiling: deleted_fds,
                drift_fraction: 1.0,
                floor: deleted_fds,
            },
        },
        BoundedSeries {
            series: recorder.heap.clone(),
            bound: Bound {
                ceiling: real(BROKERS * settings.heap_ceiling()),
                drift_fraction: 0.25,
                floor: real(Settings::residual_heap_ceiling(all_partitions)),
            },
        },
    ]
}

/// Whether each path the soak is for ran, and what the run saw of it.
fn coverage(settings: &Settings, recorder: &Recorder) -> Vec<(bool, String)> {
    let exercised = &recorder.exercised;
    let counters = exercised.counters;
    let churned = |name: &str| recorder.cycles.iter().any(|c| c.churn.contains(name));
    let mut paths = vec![
        (
            exercised.partition_limit,
            "a partition index reached its entry limit".into(),
        ),
        (
            exercised.broker_limit,
            "a broker's indexes reached the broker budget".into(),
        ),
        (
            counters.dropped > 0,
            format!("{} duplicate records dropped", counters.dropped),
        ),
        (
            counters.unindexed > 0,
            format!(
                "{} records admitted unindexed at a full index",
                counters.unindexed
            ),
        ),
        (
            counters.rebuilds > 0,
            format!("{} index rebuilds from the log", counters.rebuilds),
        ),
        (
            counters.evicted > 0,
            format!("{} live entries evicted at a limit", counters.evicted),
        ),
        (
            churned("replace"),
            "a topic was deleted and replaced".into(),
        ),
        (churned("toggle"), "dedup was turned off and on".into()),
        (churned("window"), "a window changed".into()),
        (churned("rule"), "an identity rule changed".into()),
    ];
    if settings.restart_every.is_some() {
        paths.push((churned("restart"), "a broker restarted".into()));
    }
    paths
}
