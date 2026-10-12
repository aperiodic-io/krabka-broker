// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.

//! Broker-side integration tests for KIP-73 replication throttle.
//!
//! Tests:
//! 1. `broker_scoped_alter_persists_in_image`: `IncrementalAlterConfigs`
//!    (`resource_type=Broker`) sets `leader.replication.throttled.rate`. The
//!    value is visible in `MetadataImage` through `controller_image_for_test`.
//! 2. `topic_throttle_config_propagates`: `IncrementalAlterConfigs`
//!    (`resource_type=Topic`) sets `leader.replication.throttled.replicas`.
//!    The `TopicThrottle` helper reports it correctly.
//! 3. `throttle_rate_caps_fetch_response_size`: produce 8 KB, set
//!    leader-rate=512, then Fetch with `replica_id >= 0`. Assert that the
//!    response is well under 8 KB.
//! 4. `unthrottled_partition_unaffected`: the same setup without throttle
//!    config. Fetch delivers the full 8 KB.
//!
//! The suite is gated to non-Windows to match the multi-broker test convention
//! established by the existing integration suites.
//!
//! Tests 1 and 2 live in `config_propagation`, tests 3 and 4 in `fetch_size`.
//! The helpers they share are split by layer: `kafka_wire` for the framing and
//! the SASL handshake, `cluster` for broker and topic setup, `configs` for the
//! `IncrementalAlterConfigs` and `DescribeConfigs` drivers, and `records` for
//! Produce and Fetch.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `throttle/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "throttle/cluster.rs"]
mod cluster;
#[path = "throttle/config_propagation.rs"]
mod config_propagation;
#[path = "throttle/configs.rs"]
mod configs;
#[path = "throttle/fetch_size.rs"]
mod fetch_size;
mod kafka_wire;
#[path = "throttle/records.rs"]
mod records;
mod support;
#[path = "throttle/test_support.rs"]
mod test_support;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-throttle-test";
