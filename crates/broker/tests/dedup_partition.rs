//! KFC-10 produce-time deduplication under network partitions.
//!
//! The dedup index is a function of the partition log, and a partition is
//! what makes replica logs diverge: a deposed leader holds a suffix that no
//! other replica has, and truncates it when it rejoins. Its index must forget
//! that suffix with it. Otherwise a key whose only copy was lost there is
//! dropped as a "duplicate" when a producer writes it again, and the record is
//! gone for good. The scenarios here cut live brokers off the network and
//! check, after every heal, that each replica's log and index agree with what
//! was acknowledged.
//!
//! # The network
//!
//! The cluster is three combined controller and broker nodes behind
//! `support::linked_cluster::PeerLinks`: a relay for every ordered pair of
//! nodes, so a cut is two-way. An isolated node can neither be reached by its
//! peers nor reach them, for raft, heartbeats, replication and forwarding
//! alike, so it is fenced and deposed as a node on the far side of a real
//! partition is. Test clients reach every broker at its real listener and are
//! never cut: they model producers that can see both sides.
//!
//! # Kafka semantics the scenarios assert
//!
//! * An `acks=all` write on a leader that its replicas cannot reach is never
//!   acknowledged. It can still be in that leader's log, and is truncated
//!   when the leader rejoins under a newer leader epoch.
//! * An `acks=1` write is acknowledged once the leader appended it. If that
//!   leader is then deposed before a replica copied the write, it is lost:
//!   Kafka's documented trade-off for `acks=1`, which deduplication must not
//!   make worse.
//! * A write that timed out is not known to have failed: if the leader's ISR
//!   later shrinks to itself, its high watermark passes the write and it is
//!   committed. The invariants are therefore about acknowledged writes, and
//!   about keys never appearing twice, not about unacknowledged ones.
//!
//! # Running
//!
//! `cargo test -p krabka-broker --test dedup_partition`. The flapping
//! scenario runs one seed for 20 seconds by default; `DEDUP_PARTITION_SEEDS`
//! sets how many seeds, `DEDUP_PARTITION_SECS` how long each runs, and
//! `DEDUP_PARTITION_SEED` replays one seed. Add `-- --nocapture` to see each
//! seed's schedule and counts.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `dedup_partition/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "dedup_partition/client_ids.rs"]
mod client_ids;
#[path = "dedup_partition/flapping.rs"]
mod flapping;
#[path = "dedup_partition/harness.rs"]
mod harness;
#[path = "dedup_partition/history.rs"]
mod history;
#[path = "dedup_partition/isolated_controller.rs"]
mod isolated_controller;
#[path = "dedup_partition/isolated_follower.rs"]
mod isolated_follower;
#[path = "dedup_partition/isolated_leader.rs"]
mod isolated_leader;
#[path = "dedup_partition/no_quorum.rs"]
mod no_quorum;
