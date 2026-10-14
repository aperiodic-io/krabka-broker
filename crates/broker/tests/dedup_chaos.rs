//! KFC-10 produce-time deduplication under randomized, seeded faults.
//!
//! Each seed plans a fault schedule and drives it against a live
//! three-broker in-process cluster while six producers, plain and
//! idempotent, write keyed batches with `acks=all` and retries to five dedup
//! topics: one deduplicated by key over two partitions, one by header, one
//! whose rule, window and enabled state the faults change, one whose window
//! expires mid-run, and one that holds more identities than a partition index
//! may keep. The faults overlap: graceful stops, crashes that skip the clean
//! shutdown, crashes that leave a torn or a cut last batch, the partition
//! leader or the controller leader taken down, short pauses, preferred
//! elections, leader moves and config changes.
//!
//! When the faults end, every broker comes back and the producers stop. The
//! run then checks the whole history against the invariants of
//! [`check::INVARIANTS`]: every acknowledged record survives on every
//! replica, each identity of a deduplicating topic is in the log at most
//! once, each acknowledged offset holds the record sent or the gap of a
//! dropped duplicate, every replica serves the same bytes, the entry gauges
//! never pass their limits, and the cluster keeps and regains liveness.
//!
//! Every failure prints the seed, the fault plan and the timeline of what
//! ran. `DEDUP_CHAOS_SEED=<n>` replays one seed, `DEDUP_CHAOS_SEEDS=<count>`
//! runs that many fresh seeds, and `DEDUP_CHAOS_SECS=<s>` sets the fault
//! phase of each:
//!
//! ```text
//! DEDUP_CHAOS_SEEDS=10 DEDUP_CHAOS_SECS=60 \
//!     cargo test -p krabka-broker --test dedup_chaos -- --nocapture
//! ```
//!
//! A seed replays the same plan, not the same interleaving, so a failure
//! that depends on timing may need a few replays. [`regressions`] holds the
//! deterministic, minimized form of each failure the runs found.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `dedup_chaos/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "dedup_chaos/check.rs"]
mod check;
#[path = "dedup_chaos/cluster.rs"]
mod cluster;
#[path = "dedup_chaos/disk.rs"]
mod disk;
#[path = "dedup_chaos/gauges.rs"]
mod gauges;
#[path = "dedup_chaos/plan.rs"]
mod plan;
#[path = "dedup_chaos/regressions.rs"]
mod regressions;
#[path = "dedup_chaos/rng.rs"]
mod rng;
#[path = "dedup_chaos/run.rs"]
mod run;
#[path = "dedup_chaos/topics.rs"]
mod topics;
#[path = "dedup_chaos/workload.rs"]
mod workload;

use {assert2::assert, plan::RunSize, run::run_seed, std::time::Instant, support::cluster_lock};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_fault_schedules_keep_every_dedup_invariant() {
    let _g = cluster_lock().lock().await;
    let size = RunSize::from_env();
    eprintln!(
        "dedup-chaos: seeds {:?}, {}s of faults each",
        size.seeds,
        size.fault_time.as_secs()
    );
    let began = Instant::now();
    let mut failures = Vec::new();
    for seed in size.seeds {
        // Each seed runs in its own task, so a panic deep in a broker helper
        // still reports its seed and the run goes on to the next one.
        match tokio::spawn(run_seed(seed, size.fault_time)).await {
            Ok(outcome) => {
                eprintln!(
                    "dedup-chaos: seed {seed} took {:.1}s: {}",
                    outcome.elapsed.as_secs_f64(),
                    outcome.findings.verdicts()
                );
                if outcome.findings.is_clean() {
                    eprintln!("{}", outcome.report());
                } else {
                    failures.push(outcome.report());
                }
            }
            Err(error) => failures.push(format!(
                "seed {seed} panicked: {error}; replay with DEDUP_CHAOS_SEED={seed} (its plan and \
                 timeline are in the output above)"
            )),
        }
    }
    eprintln!("dedup-chaos: done in {:.1}s", began.elapsed().as_secs_f64());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
