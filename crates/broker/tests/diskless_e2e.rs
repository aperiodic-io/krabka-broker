//! End-to-end proof that the diskless WAL subsystem works across brokers.
//!
//! Every other test of this subsystem stubs the network, the placement or the
//! object store. This suite stubs none of them. Three in-process brokers boot
//! with distinct racks, a shared `Local` object store and a topic-backed
//! diskless WAL index. A `krabka.diskless=true` topic is created through the
//! real `CreateTopics` handler, and every produce and every fetch goes over
//! the wire.
//!
//! ## What each case proves
//!
//! [`failover`] is the subsystem's whole value proposition: an `acks=all`
//! append survives the loss of the broker that acked it. The case asserts the
//! append reached a *second voter's* WAL directory byte for byte and moved
//! that voter's durable checkpoint, kills the leader, and requires the
//! promoted broker to serve the same offsets byte for byte. Nothing here is
//! satisfied by a local fsync: the bytes are read out of another broker's
//! `__diskless_wal_quorum` tree, and the survivor's own partition log starts
//! empty.
//!
//! [`cold_read`] covers the other half of the durability story, the part that
//! leaves the local disk entirely: flush to the object store, trim behind the
//! committed index frontier, then read a *trimmed* offset back. The
//! discriminating property is that the offset is below the partition's log
//! start, so only the object store can answer.
//!
//! [`restart`] crashes the leader while its flusher is running and brings it
//! back on the same addresses. A flush is a multi-step commit -- PUT the
//! object, publish the index record, wait for the projection, trim the local
//! log -- and a crash can land between any two steps. Every offset that was
//! ever acked must still read back afterwards.
//!
//! ## Layout
//!
//! The binary root carries the module tree and the constants every part
//! shares. [`cluster`] boots, waits on and restarts the three brokers,
//! [`topic`] creates the diskless topic and finds its leader, [`wire`] is the
//! produce and fetch traffic, and [`voter_dir`] reads a voter's WAL directory
//! off the filesystem. The three cases order those pieces.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `diskless_e2e/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "diskless_e2e/cluster.rs"]
mod cluster;
#[path = "diskless_e2e/cold_read.rs"]
mod cold_read;
#[path = "diskless_e2e/failover.rs"]
mod failover;
#[path = "diskless_e2e/restart.rs"]
mod restart;
#[path = "diskless_e2e/topic.rs"]
mod topic;
#[path = "diskless_e2e/voter_dir.rs"]
mod voter_dir;
#[path = "diskless_e2e/wire.rs"]
mod wire;

/// The one diskless topic every case creates. The name is
/// `[A-Za-z0-9_-]`-only on purpose: the WAL shard directory sanitizes any
/// other character, and [`voter_dir`] builds that path by hand.
const TOPIC: &str = "diskless-e2e";

/// Records produced before the case's fault is injected. Small enough that one
/// `Fetch` returns the whole log, large enough to span several flush ticks.
const RECORDS: usize = 24;

/// The diskless WAL quorum every case runs: three voters, so one loss still
/// leaves a strict majority.
const VOTERS: usize = 3;

/// The one password every SASL PLAIN principal in the suite authenticates
/// with. The cluster runs `SASL_PLAINTEXT` because a WAL follower's Fetch is
/// authenticated: the leader maps the caller's principal to a node id and
/// serves the shard only to a voter that is who it claims to be. An anonymous
/// follower is refused, so a plaintext cluster could never form a quorum here.
const PASSWORD: &str = "diskless-e2e";

/// The principal this suite's own admin, producer and fetch clients use. It is
/// deliberately not a `broker-*` name: a client is not a voter.
const CLIENT_PRINCIPAL: &str = "diskless-e2e-client";
