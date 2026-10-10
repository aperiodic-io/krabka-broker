// rustc 1.95 clippy::pedantic ICEs on this file (the same upstream bug
// in clippy's body-analysis / doc lint pass that already triggers on
// `tests/admin_handlers.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.

//! Broker-side ACL integration tests. No Docker.
//!
//! T22, the first of three integration test batches, drives the
//! `CreateAcls` / `DescribeAcls` / `DeleteAcls` flow over a real
//! `SASL_PLAINTEXT` listener with the wire-typed `krabka-protocol`
//! request/response codecs. This file copies the SASL framing helpers
//! (`drive_*`, `round_trip`) inline instead of sharing them through
//! `mod common`, because Rust integration tests do not easily allow
//! sibling-module reuse across files in `tests/`.
//!
//! The suite is gated to non-Windows to match the multi-broker test
//! convention. The SASL listener starts correctly on Windows, but a uniform
//! gate avoids one-off CI matrix surprises.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `acl_handlers/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "acl_handlers/acl_admin.rs"]
mod acl_admin;
#[path = "acl_handlers/client_api.rs"]
mod client_api;
#[path = "acl_handlers/framing.rs"]
mod framing;
#[path = "acl_handlers/implications.rs"]
mod implications;
mod kafka_wire;
#[path = "acl_handlers/metadata_group_txn.rs"]
mod metadata_group_txn;
#[path = "acl_handlers/polling.rs"]
mod polling;
#[path = "acl_handlers/produce_fetch.rs"]
mod produce_fetch;
#[path = "acl_handlers/provisioning.rs"]
mod provisioning;
#[path = "acl_handlers/sasl_cluster.rs"]
mod sasl_cluster;
#[path = "acl_handlers/super_users.rs"]
mod super_users;
mod support;

// Wire `i8` discriminants for the Kafka ACL enums. Kept inline (rather
// than imported from `krabka-broker::handlers::acl_wire`, which is
// crate-private) so the tests exercise the same byte values JVM clients
// would send. Sourced from `crates/broker/src/handlers/acl_wire.rs`.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const PATTERN_TYPE_ANY: i8 = 1;
const PATTERN_TYPE_LITERAL: i8 = 3;
const OPERATION_ANY: i8 = 1;
const OPERATION_READ: i8 = 3;
const OPERATION_WRITE: i8 = 4;
const PERMISSION_ANY: i8 = 1;
const PERMISSION_ALLOW: i8 = 3;

// API versions chosen so the request header is the flexible v2 form
// (matches what's exercised by the `drive_sasl_plain_session`
// helper for any flexible body). All three ACL APIs went flexible at v2.
const CREATE_ACLS_VERSION: i16 = 3;
const DESCRIBE_ACLS_VERSION: i16 = 3;
const DELETE_ACLS_VERSION: i16 = 3;

// Versions chosen for the T23 Produce/Fetch integration tests:
//   * CreateTopics v7 — flexible (FLEXIBLE_MIN=5), topic id round-trips
//     so the admin path matches what JVM clients send.
//   * Produce v11 — flexible (FLEXIBLE_MIN=9) and still uses topic
//     `name` rather than topic_id (the latter is v ≥ 13).
//   * Fetch v12 — flexible (FLEXIBLE_MIN=12) and still uses topic
//     `name` rather than topic_id, and predates KIP-903's tagged
//     `replica_state` (v ≥ 15) so the request stays a simple shape.
const FETCH_VERSION: i16 = 12;

// T24 versions:
//   * Metadata v9 — first flexible version (FLEXIBLE_MIN=9), still uses
//     topic `name` rather than `topic_id` (the latter is v ≥ 10), and
//     predates the `topic_authorized_operations` per-topic field (v ≥ 8
//     in request, but we don't request it).
//   * JoinGroup v9 — flexible (FLEXIBLE_MIN=6) and max supported by the
//     handler. Carries `skip_assignment` (v ≥ 9) but we don't read it.
//   * InitProducerId v4 — flexible (FLEXIBLE_MIN=2). Past v3 we have
//     producer_id + producer_epoch on the wire but no enable2_pc fields
//     (those are v ≥ 6).
const METADATA_VERSION: i16 = 9;
const JOIN_GROUP_VERSION: i16 = 9;
const INIT_PRODUCER_ID_VERSION: i16 = 4;

// Kafka error codes consumed by the T23/T24 assertions.
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERR_GROUP_AUTHORIZATION_FAILED: i16 = 30;
const ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;
