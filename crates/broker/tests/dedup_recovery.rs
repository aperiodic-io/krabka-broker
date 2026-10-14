//! KFC-10 produce-time deduplication across crashes, restarts and failovers.
//!
//! The dedup index is in memory only and is a function of the partition log.
//! A broker that restarts, a log that loses its tail, a follower that
//! truncates a divergent suffix, and a follower that is promoted must all end
//! up with an index that holds exactly what their log holds: every identity of
//! the last window that the log still has, and nothing that it lost.
//!
//! The suites drive real in-process brokers through the Kafka protocol only.
//! An in-process broker cannot be killed with `SIGKILL`, so a crash is modelled
//! the way `process_kill_durability` reasons about it: the broker stops, and
//! then the test damages the bytes it left on disk -- a torn copy of the last
//! batch, a last batch cut in half, a missing clean-shutdown proof -- before
//! the next boot's log recovery reads them.
//!
//! [`restart`] holds the single-broker restart scenarios, [`failover`] the
//! three-broker ones, and [`harness`] what both share.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `dedup_recovery/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "dedup_recovery/failover.rs"]
mod failover;
#[path = "dedup_recovery/harness.rs"]
mod harness;
#[path = "dedup_recovery/restart.rs"]
mod restart;
