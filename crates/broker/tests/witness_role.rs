//! End-to-end coverage for the data-bearing witness role (KFC-2).
//!
//! A witness replicates partition data and votes in `KRaft`, so it is an ISR
//! member and counts toward `min.insync.replicas`. It serves no client traffic
//! and it never leads a partition. Those two halves are what this suite pins
//! down on a live three-site cluster:
//!
//! * **Visible.** The witness registers like any other broker, rack and all.
//!   `kafka-topics` and `kafka-reassign-partitions` render replica ids through
//!   `Metadata.brokers[]`, so a witness that vanished from that list would turn
//!   every admin tool's replica column into an unresolved id.
//! * **In the replica set and in the ISR.** That membership is the whole point
//!   of the role: it is what keeps `acks=all` writable after a site loss.
//! * **Closed to clients.** A `Produce` or a consumer `Fetch` that reaches a
//!   witness gets `NOT_LEADER_OR_FOLLOWER`, the code that makes a Kafka client
//!   refresh its metadata and go elsewhere. A *follower* fetch still works,
//!   which is why the witness holds the data at all.
//! * **Never a read replica.** A KIP-392 consumer whose `client.rack` names the
//!   witness site is not redirected there, even though the witness is an
//!   in-ISR same-rack replica and therefore the most attractive candidate the
//!   rack-aware selector sees.
//! * **Read-only in the config surface.** `broker.witness` is controller-
//!   managed: `DescribeConfigs` reports it read-only and
//!   `IncrementalAlterConfigs` rejects it with `INVALID_CONFIG`.
//!
//! Every wait here is bounded. The shared `support` awaiters carry their own
//! 30s bound; the loops in this file wrap theirs in `tokio::time::timeout`, so
//! a stuck cluster reports in seconds instead of hitting CI's 600s kill, which
//! is reported as TIMEOUT with no cause.

use {
    std::{future::Future, time::Duration},
    support::{SITE_A, SITE_B, SITE_C, SITES, cluster_lock},
};

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `witness_role/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "witness_role/witness_client_traffic.rs"]
mod witness_client_traffic;
#[path = "witness_role/witness_cluster.rs"]
mod witness_cluster;
#[path = "witness_role/witness_config_surface.rs"]
mod witness_config_surface;
#[path = "witness_role/witness_isr_membership.rs"]
mod witness_isr_membership;
#[path = "witness_role/witness_wire.rs"]
mod witness_wire;

/// The witness's broker id, which is also its `KRaft` node id.
const WITNESS_ID: i32 = 3;
/// The leader's broker id. Placement puts `replicas[0]` in the preferred site.
const LEADER_ID: i32 = 1;

/// Kafka's `BROKER` config-resource type.
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `IncrementalAlterConfigs` SET.
/// `config_source` `DYNAMIC_BROKER_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
/// `config_source` `DYNAMIC_DEFAULT_BROKER_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER: i8 = 3;

/// The per-broker config key that marks a data-bearing witness.
const BROKER_WITNESS: &str = "broker.witness";
/// The cluster-default config key that names the preferred leader site.
const STRETCH_PREFERRED_LEADER_SITE: &str = "stretch.preferred.leader.site";

const TOPIC: &str = "witness-topic";
const N_RECORDS: i32 = 5;

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    support::poll::within(what, Duration::from_secs(45), future).await
}
