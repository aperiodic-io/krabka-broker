//! Apache Kafka-compatible broker for Krabka.
//!
//! `krabka-broker` ships a library and a binary that unmodified JVM Kafka
//! clients can produce to and consume from. It is the runtime that connects the
//! wire protocol, `KRaft` metadata controller, log storage, replication, security,
//! quotas, compaction, tiered storage, transactions, and observability.
//!
//! # Capability areas
//!
//! - Accepts Kafka wire-protocol TCP connections and negotiates API versions.
//! - Handles topic metadata and administration, `Produce`, `Fetch`,
//!   `ListOffsets`, configs, group coordination, offset commits, share groups,
//!   transactions, producer-state inspection, quotas, and client telemetry.
//! - Runs an embedded [`krabka_raft`] `KRaft` metadata quorum, registers brokers,
//!   tracks broker liveness, and drives partition leadership and reassignment.
//! - Persists partition data through [`krabka_log`], including leader-epoch
//!   checkpoints, transaction indexes, retention, and log compaction.
//! - Supports idempotent and transactional producers, read-committed fetches,
//!   high-watermark enforcement for `acks=all`, follower replication, ISR
//!   maintenance, and leader election.
//! - Supports plaintext, TLS, SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER,
//!   SASL/GSSAPI, mTLS principal extraction, ACL authorization, and SCRAM / ACL
//!   mutation through the admin APIs.
//! - Supports KIP-405 tiered storage through local and S3-compatible remote
//!   storage managers plus the topic-backed remote-log metadata manager.
//!
//! # Quick start
//!
//! ```no_run
//! use krabka_broker::{Broker, BrokerConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let handle = Broker::start(BrokerConfig::default()).await?;
//! tokio::signal::ctrl_c().await?;
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Public surface
//!
//! - [`Broker`] — owns the partition registry, metadata image, and
//!   handler table; constructed by [`Broker::start`].
//! - [`BrokerHandle`] — lifecycle handle returned by
//!   [`Broker::start`]; call [`BrokerHandle::shutdown`] to drain.
//! - [`BrokerConfig`] — listen address, advertised listener, log dir,
//!   broker id, per-log [`LogConfig`](krabka_log::LogConfig).
//! - [`BrokerError`] — error returned by [`Broker::start`].
//!
//! ## Replication
//!
//! `CreateTopics` with `replication_factor > 1` assigns N replicas per
//! partition with round-robin over `MetadataImage::brokers()`. The
//! `replicator_supervisor` subscribes to controller metadata changes
//! and spawns a `replicator` task per partition where this broker is
//! a non-leader replica. Each replicator opens a
//! `krabka_client_core::Client` to its partition's leader, loops
//! on `Fetch` with `replica_id` set, and appends every returned
//! `RecordBatch` to the local log.
//!
//! The replication path includes follower fetch loops, high-watermark tracking,
//! `acks=all` blocking, leader-epoch fencing, ISR shrink/expand proposals, and
//! controller-driven leader election for broker failures. Produce routing still
//! follows the normal Kafka client contract: clients refresh metadata and send
//! partition writes to the advertised leader.
//!
//! ## Transactions
//!
//! The broker supports Kafka transactions (KIP-98 and full KIP-1319 v2)
//! through a per-broker `txn::coordinator::TxnCoordinator` backed by the
//! `__transaction_state` internal topic (50 partitions, lazily bootstrapped on
//! first `FindCoordinator(TRANSACTION)`). Producers call `init_transactions`,
//! `begin_transaction`, and `send_offsets_to_transaction`. `begin_transaction`
//! returns a guard whose `commit` or `abort` finishes the transaction.
//! Consumers set `isolation_level=read_committed` to filter aborted records
//! with the per-segment `.txnindex` and the partition-level LSO.
//!
//! The transaction coordinator works with the replication/high-watermark path:
//! `acks=all` transactional writes wait for the partition high watermark,
//! consumers that use `read_committed` fetch only records visible below the LSO,
//! and leader-epoch fencing plus controller-driven leader election protect the
//! log after broker failover.
//!
//! ## Bulletproof EOS — HW + acks=all
//!
//! `ReplicaState`, which lives on `Partition`, tracks the High Watermark per
//! partition. The leader maintains each follower's LEO from
//! their Fetch requests and caches HW = `min(LEO over ISR)`. `acks=-1`
//! Produces gate on `Partition::await_hw_at_least` before they respond.
//! On timeout the producer gets per-partition `REQUEST_TIMED_OUT` (code 7),
//! and `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20) when the HW arrived but
//! the ISR had shrunk below `min.insync.replicas`. Consumer Fetches
//! (`replica_id == -1`) clamp visible batches and `last_stable_offset`
//! at HW. The `read_committed` LSO becomes `Log::last_stable_offset(HW)`.
//!
//! On its own, this leaves a remaining bulletproof-EOS gap: a leader
//! crash mid-transaction still loses records. KIP-101 leader-epoch
//! fencing, leader-election-on-failure, and ISR shrink/expand (below)
//! close that gap.
//!
//! ## Bulletproof EOS — leader-epoch + election + ISR
//!
//! The broker tags KIP-101 leader-epoch fencing onto every appended batch
//! through `Partition::current_leader_epoch`. The per-partition
//! `.leader-epoch-checkpoint` file (Apache Kafka byte-compat) backs the
//! `OffsetForLeaderEpoch` RPC for follower-side truncation on leader
//! change. Leader election runs on the controller.
//! `heartbeat::controller_state::ControllerLivenessState` tracks
//! per-broker `last_heartbeat`. A 1s ticker times out brokers at
//! `heartbeat_timeout` and calls `leader_election::on_broker_dead`,
//! which scans partitions of the dead broker, picks the first alive
//! ISR replica, and bumps `leader_epoch`. `isr_maintenance` drives ISR
//! shrink and expand from the leader. It proposes `AlterPartition`
//! whenever a follower's last-fetch time exceeds
//! `replica_lag_time_max`.
//!
//! With the HW and acks=all behavior above, bulletproof EOS is complete.
//! `acks=all` produces survive arbitrary single-broker failures with
//! no data loss and no zombie writes.

#![doc(html_root_url = "https://docs.rs/krabka-broker/1.0.1")]

/// Emit the wrapped item(s) only on platforms with a usable file→socket
/// `sendfile(2)` for the zero-copy fetch path: Linux, the Apple targets, and
/// FreeBSD/DragonFly (the "SENDFILE alias"). Windows is excluded because there
/// is no safe `TransmitFile` wrapper under `unsafe_code = "forbid"`. The fetch
/// path therefore uses `pread` and `write_all` there, and `WriteOp` carries
/// only the `Inline` variant.
///
/// One macro per crate keeps the predicate identical across every sendfile-gated
/// item (the `WriteOp::File` drain helpers, the `tcp_for_sendfile` trait method,
/// the sendfile resolver, and so on), so the cfg set cannot drift. File-region
/// eligibility uses the broker's configured `sendfile_min_bytes` threshold.
/// The single per-OS syscall *inside* `sendfile_region` is gated separately
/// (Linux `rustix` vs Apple/BSD `nix`), not by this macro.
macro_rules! sendfile_cfg {
    ($($item:item)*) => {
        $(krabka_macros::sendfile_platform! { $item })*
    };
}
pub(crate) use sendfile_cfg;

pub mod api_catalog;
pub(crate) mod assign_dirs;
pub mod audit_authorizer;
pub(crate) mod audit_recovery;
pub mod audit_sink;
pub mod authorizer;
pub(crate) mod auto_join;
pub(crate) mod auto_topic_creation;
#[macro_use]
mod wire_handler;

pub(crate) mod barrier;
// The barrier marker's read half is public: `krabka-barrier verify` and any
// recovery tool has to read a marker back out of the log, and a second decoder
// beside this one would be a second thing to drift.
pub use barrier::marker::{BarrierMarker, parse_barrier_marker};
/// Where blocking work runs: in place, on the blocking pool, or inline on
/// `wasm32-wasip1`, which has no threads.
pub(crate) mod blocking;
/// Drives the `wasm32-wasip1` placement of blocking work on a native thread.
#[cfg(any(test, feature = "test-helpers"))]
pub use blocking::{InlineBlockingGuard, inline_blocking_on_this_thread};
pub mod bootstrap;
pub(crate) mod break_glass;
mod broker;
pub(crate) mod clean_shutdown;
pub(crate) mod cleaner;
mod client_metrics;
/// Client-visible failover model: producer retry/routing composed with broker
/// `acks=all` durability and idempotent producer-state checks.
#[cfg(test)]
mod client_server_failover_model;
pub(crate) mod cluster_id;
pub mod codes;
pub mod config;
pub(crate) mod config_keys;
pub mod config_value;
mod controller_admin;
pub(crate) mod controller_endpoint;
pub mod coordinator;
pub(crate) mod cordoned_log_dirs;
/// Compositional end-to-end data-path verification model (produce → replicate →
/// commit → fetch across clean and unclean failover). It wraps the real
/// HWM/ISR, leader-epoch-truncation, failover-selection, and fetch-visibility
/// cores.
#[cfg(test)]
mod data_path_model;
pub(crate) mod delegation_token_cleanup;
pub(crate) mod delivery;
pub mod disk_scanner;
mod diskless;
#[cfg(test)]
mod diskless_crash_model;
pub(crate) mod elr;
mod envelope;
mod error;
mod features;
pub mod fetch_session;
pub mod file_config;
pub(crate) mod freeze;
pub(crate) mod future_log;
mod handlers;
pub mod health;
pub(crate) mod heartbeat;
pub(crate) mod host_port;
#[cfg(not(target_family = "wasm"))]
mod http_server;
pub(crate) mod incarnation;
pub(crate) mod internal_topics;
pub(crate) mod isr_maintenance;
pub(crate) mod kafka_hash;
pub(crate) mod lag;
pub(crate) mod leader_election;
pub mod leader_rebalance;
mod log_dir;
pub mod log_dir_id;
mod log_dir_status;
pub(crate) mod log_retention;
pub(crate) mod metadata_epoch;
pub mod metadata_observer;
pub mod metadata_source;
pub mod metrics;
pub(crate) mod metrics_server;
/// Shared breadth-first runner for the exhaustive Stateright models.
#[cfg(test)]
pub(crate) mod model_check;
pub mod network;
// The OAUTHBEARER token introspection client and the JWKS refresher need an
// HTTP client stack, which wasm32-wasip1 does not have.
#[cfg(not(target_family = "wasm"))]
pub(crate) mod oauth_introspection;
#[cfg(not(target_family = "wasm"))]
pub(crate) mod oauth_jwks;
pub mod operator_keys;
mod partition;
pub(crate) mod partition_registry;
mod partition_writer;
pub(crate) mod platform;
mod producer_id_manager;
mod producer_state;
pub mod quota;
pub mod raft_handshake;
pub(crate) mod re2j;
pub(crate) mod reassignment;
pub(crate) mod remote_log_manager;
pub(crate) mod remote_reader;
pub mod replica_selector;
pub(crate) mod replica_state;
mod replicator;
mod replicator_supervisor;
pub mod schema_validation;
pub mod share_coordinator;
pub mod share_partition;
pub(crate) mod signing_domains;
pub(crate) mod site_placement;
/// Three-site stretch-cluster model: site failure and network partition
/// composed with the real site placement, controller failover, preferred
/// election, and `acks=all` durability under a data-bearing witness.
#[cfg(test)]
mod stretch_cluster_model;
pub(crate) mod task_util;
pub mod telemetry;
/// Shared scaffolding for the per-handler `#[cfg(test)] mod tests` modules
/// (deny-all authorizer, principal/peer/context builders, wire codec helpers,
/// temp-dir broker launcher, and the one `MetadataSource` fake this crate's
/// suites read from). It consolidates the copies that the mutant-hardening
/// pass duplicated across ~40 handlers.
#[cfg(test)]
pub(crate) mod test_support;
pub mod throttle;
pub(crate) mod time_util;
pub(crate) mod tls_reload;
pub(crate) mod topic_creator;
pub mod topic_policy;
pub(crate) mod topic_resolve;
mod txn;
pub(crate) mod unclean_recovery;
mod wal;

pub use broker::{Broker, BrokerHandle};
pub use config::{BootstrapMode, BrokerConfig, KafkaRlmmConfig, RemoteStorageBackend, RlmmKind};
pub use config_keys::{TopicConfigDoc, topic_config_docs};
pub use error::BrokerError;
#[cfg(any(test, feature = "test-helpers"))]
pub use txn::coordinator::fanout_gate::MarkerFanoutMode;
/// Benchmark seam over the zero-copy fetch drain, driven by
/// `benches/fetch_drain.rs`. It is the drain the connection writer calls, with
/// the two records resolvers that decide whether a records region reaches the
/// socket through the kernel or through a buffer.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fetch_drain {
    pub use crate::network::fetch_writer::{
        SendfileSink, WriteOp, resolve_records_inline, write_fetch_plan,
    };

    crate::sendfile_cfg! {
        pub use crate::network::fetch_writer::resolve_records_sendfile;
    }
}
/// Benchmark seam over the produce hot path, driven by `benches/produce.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub use handlers::produce::hot_path as produce_hot_path;
pub use health::{HealthState, NotReady};
pub use krabka_raft::NodeId;
/// The SASL/GSSAPI accept-path configuration, [`config::BrokerConfig::gssapi`].
pub use network::auth::GssapiConfig;
/// The RFC 2253 Subject DN of an mTLS peer certificate, the string
/// `ssl.principal.mapping.rules` are matched against.
pub use network::auth::subject_dn_rfc2253;
/// The Kafka `auth_to_local` rules that [`GssapiConfig`] carries.
#[cfg(not(target_family = "wasm"))]
pub use network::auth::{KerberosNameError, KerberosRule};
/// KIP-371 `ssl.principal.mapping.rules`, reachable here because
/// [`config::ListenerSpec`] carries a parsed rule list per listener.
pub use network::auth::{SslPrincipalMapper, SslPrincipalRuleError};
/// Benchmark seam over the response-framing deferral, driven by
/// `benches/perf_deferrals.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub use network::dispatch::response_framing;
/// Benchmark seam over the replicated-batch deferral, driven by
/// `benches/perf_deferrals.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub use replicator::hot_path as replicate_hot_path;
