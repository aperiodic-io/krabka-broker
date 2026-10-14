//! KFC-10 memory soak: produce-time deduplication over a long, churning
//! workload, failed when memory or descriptors leak or drift rather than only
//! when something crashes.
//!
//! The bounds suite proves the entry limits hold on one request; the stress
//! suite proves one hostile input cannot blow a broker up. Neither runs long
//! enough to see what an operator sees on day three: an index that leaves a
//! little behind every time it fills, ages out and is rebuilt. This suite
//! repeats exactly that, many times, and judges the resident set at the same
//! point of every repetition.
//!
//! # How it runs
//!
//! Three brokers, booted with small entry limits inside one process of their
//! own -- the test runs this binary again as a [`child`], with one allocator
//! arena so that the allocator's own growth does not hide a leak -- carry four
//! dedup topics of three partitions, every partition replicated to all three
//! brokers. Windows are a few seconds long. Each cycle (see [`run`]):
//!
//! - **loads** the topics with the [`workload`] mix -- mostly unique keys and
//!   headers, bursts of duplicates, large keys and values -- and then floods
//!   one partition past its entry limit and every partition past the broker
//!   budget, so trimming, fail-open admission and budget accounting churn;
//! - **drains**: produces nothing for longer than any window, until the
//!   broker's idle ticker has emptied every index on every broker;
//! - **churns** one topic, as [`topics`] tabulates -- deleted and recreated,
//!   dedup toggled, its window or identity rule changed. The change of window
//!   or rule makes each replica rebuild its index from the log. In the restart
//!   variant the churn also restarts a broker every few cycles, which rebuilds
//!   every index it follows.
//!
//! [`record`] samples the process (`VmRSS`, `VmHWM`, `RssAnon`, the threads,
//! and the open descriptors split into segment files, deleted files and the
//! rest, see [`process`]) and every broker's `krabka_broker_dedup_*` gauges every few
//! seconds, with the produce latency of each interval, and prints each sample
//! and each cycle as a comma-separated line.
//!
//! # What fails it
//!
//! - an index that does not drain to zero in an idle period, a drained index
//!   whose heap is above its residual, and a deleted topic whose series
//!   outlive it, or any index left once every topic is deleted;
//! - a sample in which a partition gauge exceeds
//!   `dedup_max_entries_per_partition`, a broker gauge exceeds
//!   `dedup_max_entries`, or the heap gauges exceed what the limits allow;
//! - a resident set, descriptor count or produce p99 that climbs, by the rules
//!   and tolerances [`judge`] tabulates -- the second-half slope of the
//!   container soak's [`drift`] for continuous series, and a comparison of
//!   first- and last-third medians for per-cycle ones;
//! - a peak resident set above a ceiling derived from the entry limits, see
//!   [`settings`];
//! - a run that never reached a limit, dropped a duplicate, admitted a record
//!   unindexed, rebuilt an index, or made one of the churn changes.
//!
//! A failure does not stop the run: every broken bound is printed as a
//! `VIOLATION` line when it happens and every judgement as a `VERDICT` line at
//! the end, and the test fails on all of them at once.
//!
//! # The restart variant
//!
//! [`dedup_memory_stays_level_across_broker_restarts`] is the same soak with
//! a broker restart every four cycles. It is ignored as a product bug: a
//! broker that `BrokerHandle::shutdown` stopped inside a process leaves two
//! tasks running that never end.
//! `SharePartitionLeaderManager::spawn_lock_sweeper`
//! (`src/share_partition/manager/sweeper.rs`) loops on its timer with no
//! shutdown token and holds the manager, and with it the broker's whole
//! `PartitionRegistry`, so every segment file and index of the stopped broker
//! stays open; `auto_join::voter_updates::run_voter_updates` loops the same
//! way and holds its controller. Each restart therefore leaks one broker's
//! partitions: the descriptor count climbs with every restart, and once the
//! new broker's retention deletes those segments they stay open as deleted
//! files. Nothing about it is specific to deduplication, and the variant
//! fails on its descriptor bounds until the two tasks stop on shutdown.
//!
//! # Running it
//!
//! By default it runs about a minute and a half, as an ordinary test:
//!
//! ```text
//! cargo test -p krabka-broker --test dedup_soak -- --nocapture
//! ```
//!
//! `DEDUP_SOAK_SECS` lengthens the cycling to any number of seconds; the
//! default run is the CI-sized check of the same assertions, and a run of
//! twenty minutes or more is the soak proper:
//!
//! ```text
//! DEDUP_SOAK_SECS=1200 cargo test -p krabka-broker --test dedup_soak -- --nocapture
//! ```
//!
//! The restart variant runs the same way, by name and with `--ignored`:
//!
//! ```text
//! cargo test -p krabka-broker --test dedup_soak -- --ignored restarts --nocapture
//! ```
//!
//! To plot a run, keep the `sample,` or `cycle,` lines of the output; the
//! `# sample,` and `# cycle,` lines name their columns.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `dedup_soak/` directory, and onto the two modules this suite shares with
// others: the container soak's drift judgement, and the recovery suite's
// cluster harness, of which this suite uses the boot, stop and restart steps.
#[path = "dedup_soak/child.rs"]
mod child;
#[path = "dedup_soak/cluster.rs"]
mod cluster;
#[path = "soak/drift.rs"]
mod drift;
#[path = "dedup_soak/gauges.rs"]
mod gauges;
#[allow(dead_code)]
#[path = "dedup_recovery/harness.rs"]
mod harness;
#[path = "dedup_soak/judge.rs"]
mod judge;
#[path = "dedup_soak/process.rs"]
mod process;
#[path = "dedup_soak/record.rs"]
mod record;
#[path = "dedup_soak/run.rs"]
mod run;
#[path = "dedup_soak/settings.rs"]
mod settings;
#[path = "dedup_soak/topics.rs"]
mod topics;
#[path = "dedup_soak/trend.rs"]
mod trend;
#[path = "dedup_soak/workload.rs"]
mod workload;

use self::child::Variant;

#[test]
fn dedup_memory_stays_bounded_and_level_under_churn() { child::soak(Variant::Churn); }

#[test]
#[ignore = "product bug: a broker stopped in process keeps its partition logs open, \
            SharePartitionLeaderManager::spawn_lock_sweeper never ends"]
fn dedup_memory_stays_level_across_broker_restarts() { child::soak(Variant::Restarts); }
