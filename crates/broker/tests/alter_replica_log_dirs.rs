// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by `jbod.rs`.

//! KIP-113: `AlterReplicaLogDirs` (`api_key` 34) end-to-end.
//!
//! The test starts a single broker with two `log.dirs`, creates a
//! 2-partition topic, then sends `AlterReplicaLogDirs` to move both
//! partitions into the second directory. It asserts that:
//!   1. the partition directories migrate on disk to the target dir,
//!   2. `DescribeLogDirs` polls converge with `is_future_key = false`
//!      in the target dir for both partitions,
//!   3. an invalid target and a missing replica return the correct Kafka
//!      error codes.
//!
//! # Module layout
//!
//! This file is the test-binary root and holds the module wiring alone. The
//! `wire` child holds the request drivers, `harness` the two-directory broker
//! and the on-disk and `DescribeLogDirs` readings the scenarios make of it,
//! and `moves`, `errors` and `startup` one scenario group each.

mod kafka_wire;
// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `alter_replica_log_dirs/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
mod support;
#[path = "support/two_dir_topic.rs"]
mod two_dir_topic;

#[path = "alter_replica_log_dirs/cordoned.rs"]
mod cordoned;
#[path = "alter_replica_log_dirs/errors.rs"]
mod errors;
#[path = "alter_replica_log_dirs/harness.rs"]
mod harness;
#[path = "alter_replica_log_dirs/moves.rs"]
mod moves;
#[path = "alter_replica_log_dirs/startup.rs"]
mod startup;
#[path = "alter_replica_log_dirs/wire.rs"]
mod wire;
