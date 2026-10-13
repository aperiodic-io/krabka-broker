//! Handler dispatch. Each API module owns its admission and response behavior.
//!
//! Most crate-private handlers receive a decoded request and a
//! [`RequestContext`] and return a response struct. The registry generates
//! adapters that decode the wire body and encode that response. APIs that
//! forward a request untouched or need a custom codec instead receive the raw
//! body and return encoded bytes. The network dispatcher prepends the response
//! header to those bytes.
//!
//! The modules that do not name one api key hold what the handlers share: the
//! wire type aliases, the krabka-private api keys, the response encoders, the
//! ACL gates, the coordinator lookups, and the admin audit hook. Each of them
//! re-exports here, so a caller writes `crate::handlers::<item>` for every one
//! of them.
//!
//! Whether a topic is broker-owned is not one of them. `Metadata` and
//! `DescribeTopicPartitions` both read [`crate::internal_topics`], which lives
//! beside the subsystems whose topic-name constants it is built from rather
//! than under the handlers that happen to answer with it.

use crate::error::BrokerError;

mod wire_types;
#[cfg(test)]
#[macro_use]
pub(crate) mod test_support;

#[cfg(test)]
#[macro_use]
mod acl_test_support;

mod acl_gates;
mod admin_audit;
#[cfg(test)]
mod controller_mutation_tests;
mod coordinator_routing;
mod partition_transition;
#[cfg(test)]
#[macro_use]
mod delegation_token_test_support;
#[cfg(test)]
mod group_heartbeat_test_support;
mod node_endpoints;
mod partition_materialization;
mod private_api_keys;
mod raft_voter;
mod record_strings;
mod registration_listeners;
mod request_names;
mod response_encoding;

pub(crate) fn map_partition_rows<T>(
    indexes: impl Iterator<Item = i32>,
    row: impl FnMut(i32) -> T,
) -> Vec<T> {
    indexes.map(row).collect()
}
mod submit_failure;

pub(crate) use self::{
    acl_gates::{
        acl_denied, acl_denied_quiet, allowed_topics, any_topic_describe_denied,
        cluster_action_denied, cluster_alter_configs_denied, cluster_alter_denied,
        cluster_describe_denied, cluster_shortcut_denied, config_resource_refusal, denied_topics,
        group_delete_denied, group_describe_denied, group_read_denied, requested_topic_name,
        topic_decisions,
    },
    admin_audit::{audit_admin, audit_admin_for, audit_admin_success, audit_resource},
    coordinator_routing::{
        group_coordinator_error, group_coordinator_error_response, group_partition_loading,
        group_version_disabled, share_group_not_found_message, streams_protocol_enabled,
    },
    node_endpoints::leader_endpoints,
    private_api_keys::{
        ALTER_BARRIER_GROUPS_API_KEY, APPROVE_BREAK_GLASS_API_KEY, DESCRIBE_BARRIER_GROUPS_API_KEY,
        DESCRIBE_BREAK_GLASS_API_KEY, DESCRIBE_TOPIC_FREEZES_API_KEY, KRABKA_PRIVATE_API_KEY_FLOOR,
        LIST_BARRIER_CUTS_API_KEY, PROPOSE_BREAK_GLASS_API_KEY, SET_TOPIC_FREEZE_API_KEY,
        TRIGGER_BARRIER_API_KEY, WRITE_BARRIER_MARKERS_API_KEY,
    },
    record_strings::decode_group_request,
    response_encoding::{
        ErrorCodeResponse, ErrorResponse, decode_request, encode_response, stamp_unset,
    },
    submit_failure::submit_failure_code,
    wire_types::{ApiKeyCode, ApiVersion, CorrelationId},
};

/// Encoded response returned by a raw-body handler dispatch function.
pub(crate) type DispatchResult = Result<bytes::Bytes, BrokerError>;

pub(crate) mod context;
pub(crate) use context::{RequestContext, TelemetryContext};

pub(crate) mod registry;
pub(crate) use registry::{DispatchEntry, DispatchKind, DispatchRegistry, RequestQuotaPolicy};

pub(crate) mod acl_wire;
// KIP-590 forwarding of an admin RPC to the active controller, for the handlers
// that must run there (`KafkaApis.forwardToController`).
pub(crate) mod forward_to_controller;
// KIP-853 dynamic-quorum reconfiguration (api_keys 80/81/82).
pub(crate) mod add_raft_voter;
pub(crate) mod allocate_producer_ids;
pub(crate) mod alter_client_quotas;
pub(crate) mod alter_configs;
pub(crate) mod alter_partition;
pub(crate) mod alter_partition_reassignments;
pub(crate) mod alter_replica_log_dirs;
pub(crate) mod alter_user_scram_credentials;
pub(crate) mod api_versions;
pub(crate) mod assign_replicas_to_dirs;
// KIP-430: authorized-operations bitfield helper used by metadata,
// describe_cluster, describe_groups when the request opts in.
pub(crate) mod authorized_operations;
pub(crate) mod broker_heartbeat;
pub(crate) mod broker_registration;
pub(crate) mod consumer_group_describe;
pub(crate) mod consumer_group_heartbeat;
pub(crate) mod controller_id;
pub(crate) mod controller_registration;
pub(crate) mod create_acls;
pub(crate) mod create_delegation_token;
pub(crate) mod create_partitions;
pub(crate) mod create_topics;
pub(crate) mod delete_acls;
pub(crate) mod delete_groups;
pub(crate) mod delete_records;
pub(crate) mod delete_topics;
pub(crate) mod describe_acls;
pub(crate) mod describe_client_quotas;
pub(crate) mod describe_cluster;
pub(crate) mod describe_configs;
pub(crate) mod describe_delegation_token;
pub(crate) mod describe_groups;
pub(crate) mod describe_log_dirs;
// KIP-664 producer-state introspection (api_key 61).
pub(crate) mod describe_producers;
// KIP-595 raft-quorum introspection (api_key 55).
pub(crate) mod describe_quorum;
// KIP-664 transaction introspection (api_key 65).
pub(crate) mod describe_transactions;
// KIP-966 paginated topic listing (api_key 75).
// KIP-966: the eligible-leader-replica columns DescribeTopicPartitions
// reports, read out of the metadata image.
pub(crate) mod describe_topic_partitions;
pub(crate) mod describe_user_scram_credentials;
pub(crate) mod elect_leaders;
pub(crate) mod expire_delegation_token;
pub(crate) mod fetch;
pub(crate) mod fetch_downconvert;
// KIP-630 controller-snapshot fetch (api_key 59).
pub(crate) mod fetch_snapshot;
pub(crate) mod find_coordinator;
pub(crate) mod get_replica_log_info;
// KIP-714 client telemetry. `get` assigns configured subscriptions and `push`
// validates, decodes, and exports OTLP metrics to the configured sinks.
pub(crate) mod get_telemetry_subscriptions;
pub(crate) mod heartbeat;
pub(crate) mod incremental_alter_configs;
pub(crate) mod init_producer_id;
pub(crate) mod join_group;
pub(crate) mod leave_group;
// KIP-1142 list-config-resources admin RPC (api_key 74). Generalises the
// v0 ListClientMetricsResources call (KIP-714) into a typed enumeration.
pub(crate) mod list_config_resources;
pub(crate) mod list_groups;
pub(crate) mod list_offsets;
pub(crate) mod list_partition_reassignments;
// KIP-664 transaction-summary admin RPC (api_key 66).
pub(crate) mod list_transactions;
pub(crate) mod metadata;
pub(crate) mod offline_replicas;
pub(crate) mod offset_commit;
pub(crate) mod offset_delete;
pub(crate) mod offset_fetch;
pub(crate) mod offset_for_leader_epoch;
pub(crate) mod produce;
// KIP-714 client-metrics push, paired with get_telemetry_subscriptions.
pub(crate) mod push_telemetry;
pub(crate) mod remove_raft_voter;
pub(crate) mod renew_delegation_token;
// KIP-932 ShareGroupDescribe (api_key 77), with a per-group Describe ACL gate.
pub(crate) mod share_group_describe;
// KIP-932 share-group membership (api_key 76).
pub(crate) mod share_group_heartbeat;
// KIP-932 admin offset RPCs (api_key 90/91/92), with per-group
// Describe/Alter/Delete ACL gates.
pub(crate) mod alter_share_group_offsets;
pub(crate) mod delete_share_group_offsets;
pub(crate) mod describe_share_group_offsets;
// KIP-932 ShareAcknowledge (api_key 79), with a per-topic Read ACL gate.
pub(crate) mod share_acknowledge;
// KIP-932 ShareFetch (api_key 78), with a per-topic Read ACL gate.
pub(crate) mod share_fetch;
// KIP-1071 StreamsGroupDescribe (api_key 89), with per-group and
// topology-topic Describe ACL gates.
pub(crate) mod streams_group_describe;
// KIP-1071 streams-group membership / rebalance protocol (api_key 88).
pub(crate) mod streams_group_heartbeat;
// KIP-1331 topology description push (api_key 93, Kafka trunk).
pub(crate) mod streams_group_topology_description_update;
pub(crate) mod sync_group;
// KIP-919 admin RPC to permanently drop a broker registration (api_key 64).
pub(crate) mod unregister_broker;
// KIP-1312 admin RPC to drop a controller registration (api_key 94, Kafka
// trunk).
pub(crate) mod unregister_controller;
// KIP-584 feature finalization (api_key 57), with a Cluster:Alter ACL gate.
pub(crate) mod update_features;
pub(crate) mod update_raft_voter;
