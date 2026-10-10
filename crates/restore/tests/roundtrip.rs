//! End-to-end proof that `krabka-restore`'s discover/verify/bound/materialize
//! stages compose correctly against a REAL KIP-405 archive.
//!
//! Every module under `src/` already has thorough unit-level tests, built
//! against a hand-faked `SegmentInventory`/`VerifiedSegment`. This file does
//! not repeat that: it builds a real archive the same way the broker's own
//! tiered-storage copy path builds one -- a real `krabka_log::Log`, appended
//! real batches, sealed into real segments, archived through the real
//! `LocalTieredStorage::copy_log_segment_data` (the pattern
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` uses to prove the JVM
//! reads a Krabka-offloaded segment) -- then drives the whole pipeline
//! through `krabka_restore::run_from_args`/`restore`, the crate's own
//! documented in-process entry points (a subprocess needs a Cargo working
//! tree, which a Bazel sandbox does not have; `crates/format/src/lib.rs`'s
//! `run_from_args` doc gives the same rationale for `krabka format`).
//!
//! The fixture spans two topics, both with two partitions, and one partition
//! ("orders-0") with two archived segments, so discovery has more than one
//! topic and partition to group and materialize has to continue a partition's
//! log across a segment boundary. One partition ("payments-1") has its oldest
//! segments missing from the archive, the way remote retention leaves a
//! partition whose earliest history has aged out, so its restored log starts
//! above offset 0.
//!
//! `consume.rs` then boots a broker on a restore output and reads it back
//! over the wire, which is where the client-visible contract KFC-3 states --
//! the offset bounds, the batches, the leader epoch, and all of it again
//! after a restart -- is actually checked.

#[path = "support/topic_configuration.rs"]
mod topic_configuration;

#[path = "support/archive_fixture.rs"]
mod archive_fixture;

#[path = "roundtrip/args.rs"]
mod args;
#[path = "roundtrip/batches.rs"]
mod batches;
#[path = "roundtrip/bootstrap_metadata.rs"]
mod bootstrap_metadata;
#[path = "roundtrip/cli_entry.rs"]
mod cli_entry;
#[path = "roundtrip/consume.rs"]
mod consume;
#[path = "roundtrip/fixture.rs"]
mod fixture;
#[path = "roundtrip/partition_data.rs"]
mod partition_data;
#[path = "roundtrip/report.rs"]
mod report;

#[path = "roundtrip/restored.rs"]
mod restored;
