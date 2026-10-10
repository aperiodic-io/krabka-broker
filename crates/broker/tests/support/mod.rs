//! Shared helpers for broker integration tests.
//!
//! # Single-broker helper
//!
//! [`start`] and [`InProcess`] boot one broker and one client for simple
//! unit-style integration tests.
//!
//! # Multi-broker helpers
//!
//! [`start_n_node_with_retry`] boots an `n`-broker cluster with
//! ephemeral ports and short raft timings. Each `tests/*.rs` integration-test
//! crate that needs a 3-broker cluster declares `mod support;` and calls
//! `start_n_node_with_retry`.
//!
//! # Fault injection
//!
//! [`relay`] is a test-only TCP forwarder. Point a broker at a relay instead of
//! at its peer and the test can cut the link — including the connections that
//! are already open — without stopping either node, which is the only way to
//! produce a live minority.
//!
//! # Layout
//!
//! One child module per role: [`single_broker`] and [`operator_keys`] boot a
//! single node, [`ports`] reserves the addresses a cluster binds and
//! [`cluster`] and [`cluster_boot`] boot it, [`containers`] addresses the JVM
//! container suites, [`coordinator`] makes a client's coordinator lookup, [`audit`] reads the audit topic back, and [`relay`] cuts
//! links. Every helper is re-exported here, so a suite reaches all of them as
//! `support::<name>`. What stays in this file is the tracing setup, the lock
//! that serializes a binary's cluster tests, the metadata round-trip that
//! resolves a topic id, and the two pollers that wait on the audit topic.
//!
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as
//! a non-binary submodule, so it does not compile the file as its own test
//! crate.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use assert2::assert;

use crate::support::{discovery::topic_metadata_request, topics::metadata_topic};

pub mod acl;
pub mod admin;
mod audit;
pub mod classic;
pub mod client;
mod cluster;
mod cluster_boot;
pub mod configs;
pub mod consumer_groups;
mod containers;
mod coordinator;
pub mod discovery;
pub mod diskless;
pub mod durability;
pub mod fetch;
pub mod listeners;
pub mod offsets;
mod operator_keys;
pub mod partitions;
pub mod poll;
mod ports;
pub mod produce;
pub mod producer;
pub mod quorum;
pub mod records;
pub mod sasl;
pub mod share;
mod single_broker;
pub mod storage;
pub mod streams;
pub mod tiered;
pub mod tls;
pub mod topics;
pub mod transaction_wire;
pub mod transactions;
pub mod wire;
// A cut-and-heal TCP relay for partition tests. Declared here so every suite
// that pulls in `support` can reach it as `support::relay`.
pub mod relay;

// Each suite declares `mod support;` and reaches the helpers it needs through
// this one re-export, so every binary compiles the whole surface and uses only
// part of it. That is why this statement carries the `unused_imports` allow:
// the same reason the module carries `allow(dead_code)`.
/// Set an exact container-fixture permission mode with the caller's diagnostic.
///
/// # Panics
/// Panics if the permissions cannot be set.
#[cfg(unix)]
pub fn chmod_for_container(path: &std::path::Path, mode: u32, context: &str) {
    containers::chmod_for_container(path, mode, context);
}

#[allow(unused_imports)]
pub use self::{
    audit::{audit_record_seqs, consume_audit_records},
    cluster::{
        NodeIndex, await_broker_start, controller_voters, fixed_internal_isr_cluster,
        listener_pairs, node_config, start_n_node_client, start_n_node_with,
    },
    cluster_boot::{
        AddressedNodeSetup, ClusterBootstrapSetup, ClusterNodeSetup, RoleNodeSetup, RoleTopology,
        addressed_node_config, await_controller_replacement, broker_config, registered_cluster,
        shutdown_cluster, single_controller_endpoints, start_first_held, start_held_node,
        start_n_node, start_n_node_customized_with_retry, start_n_node_with_retry,
        start_reusing_addrs, two_controller_followers, wait_for_all_brokers_registered,
    },
    containers::{
        ContainerInput, JvmAdminSetup, JvmBrokerSetup, JvmDockerSetup, JvmListeners,
        JvmSingleBrokerSetup, JvmStaticVoterSetup, bridge_gateway, combined_output, docker,
        docker_exec, docker_logs, docker_output, docker_run_blocking, docker_tool_command,
        fixture_cache_dir, format_jvm_voter, free_port, init_jvm_tracing, jvm_acks_all_producer,
        jvm_admin_args, jvm_admin_config, jvm_bootstrap_servers, jvm_broker_config,
        jvm_client_addr, jvm_client_ports, jvm_docker_command, jvm_docker_run, jvm_finalized_level,
        jvm_listeners, jvm_output_lines, jvm_parse_offset, jvm_single_broker_config,
        jvm_spawn_piped, jvm_static_voter_config, jvm_stdin_output, jvm_tool_output,
        kafka_single_node_env_args, manifest_dir, print_log_tail, remove_container,
        remove_container_with_volumes, save_jvm_logs, start_jvm_bound, start_jvm_cluster,
        start_jvm_single, unique_container_name,
    },
    coordinator::{KEY_TYPE_GROUP, KEY_TYPE_SHARE, KEY_TYPE_TRANSACTION, find_coordinator},
    operator_keys::{
        ANONYMOUS, OperatorKey, OperatorKeysSetup, OperatorSaslSetup, mint_operator_key,
        sasl_client, sasl_plain_security, start_with_operator_key, start_with_operator_keys,
        start_with_operator_keys_sasl,
    },
    ports::{bind_and_drop_ports, bind_and_hold_ports},
    sasl::{sasl_plaintext_config, sasl_plaintext_with_users, start_broker},
    single_broker::{
        InProcess, boot_single, standalone_broker, start, start_configured,
        start_group_coordinator, start_legacy, start_ready_group, start_with_audit_key,
        start_with_bound_listeners, start_with_deny_all_authz, start_with_dir,
    },
};

/// Lazily-initialized tracing subscriber so `RUST_LOG=...` works in
/// integration tests. It is safe to call this many times, because `try_init`
/// is a no-op after the first success.
pub fn init_tracing() {
    init_tracing_with("warn");
}

/// Initialize the same subscriber with the caller's original fallback filter.
pub fn init_tracing_with(default_filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_test_writer()
        .try_init();
}

/// Serializes the multi-broker tests of one test binary.
///
/// Each such test boots a loopback cluster with short raft timings. Two at
/// once exhaust the ephemeral ports and starve the openraft election, which
/// shows as intermittent `FENCED_LEADER_EPOCH` churn, so a test takes this lock
/// for its whole body. The binary is then effectively single-threaded for
/// these scenarios, whether or not nextest test groups also serialize it.
///
/// This is a `tokio::sync::Mutex` and not a `std::sync::Mutex`, so a test can
/// hold the lock across the `.await` calls in its body without a report from
/// clippy's `await_holding_lock`.
pub fn cluster_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    lazy_mutex(&LOCK)
}

/// A `CreateTopics` row that pins partition `p` to the brokers `replicas[p]`,
/// the first of them leading.
///
/// Automatic placement starts at a random broker, as Kafka's
/// `StripedReplicaPlacer` does, so a test that needs to know which broker
/// leads or replicates a partition names the brokers itself.
pub fn topic_on(
    name: &str,
    replicas: &[&[i32]],
) -> krabka_protocol::owned::create_topics_request::CreatableTopic {
    use krabka_protocol::owned::create_topics_request::{
        CreatableReplicaAssignment, CreatableTopic,
    };

    CreatableTopic {
        name: name.into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: replicas
            .iter()
            .enumerate()
            .map(|(partition, broker_ids)| CreatableReplicaAssignment {
                partition_index: i32::try_from(partition).expect("partition index"),
                broker_ids: broker_ids.to_vec(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Fetch all records from `AUDIT_TOPIC` partition 0, JSON-decode each
/// record value, and return the decoded objects. Mirrors the
/// `broker_started_event_is_written_to_audit_topic` fetch pattern.
pub async fn wait_for_audit_record<F>(
    client: &krabka_client_core::Client,
    what: &str,
    mut predicate: F,
) -> Vec<serde_json::Value>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = consume_audit_records(client).await;
        if records.iter().any(&mut predicate) {
            return records;
        }
        assert!(
            Instant::now() <= deadline,
            "audit record '{what}' did not appear within 30s; last={records:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_audit_seq_count(
    client: &krabka_client_core::Client,
    min_count: usize,
) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seqs = audit_record_seqs(client).await;
        if seqs.len() >= min_count {
            return seqs;
        }
        assert!(
            Instant::now() <= deadline,
            "audit seq count did not reach {min_count} within 30s; last={seqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Round-trip a Metadata request to learn the topic's assigned UUID.
/// Produce / Fetch at v ≥ 13 carry only `topic_id` on the wire, so the
/// caller must plumb the real UUID through.
pub async fn topic_id_for(
    client: &krabka_client_core::Client,
    name: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    let resp = client
        .send(topic_metadata_request(Some(vec![metadata_topic(
            Some(name.into()),
            krabka_protocol::primitives::uuid::Uuid::default(),
        )])))
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

pub fn lazy_mutex(lock: &std::sync::OnceLock<tokio::sync::Mutex<()>>) -> &tokio::sync::Mutex<()> {
    lock.get_or_init(|| tokio::sync::Mutex::new(()))
}
