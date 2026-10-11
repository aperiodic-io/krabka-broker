//! Tests for the krabka-private registration table: that every private api
//! key reaches its own adapter, and that none of them is advertised.

use std::collections::BTreeSet;

use assert2::{assert, check};

use super::*;
use crate::{
    api_catalog::dispatched_apis,
    handlers::{
        self,
        registry::{DispatchKind, RequestQuotaPolicy, build_registry},
    },
};

/// Every krabka-private api key, the adapter that dispatch must reach, and
/// the label a failure reports.
///
/// The five barrier keys and the five KFC-9 keys hold the same contract, so
/// one table covers both. An entry here fails when a key loses its
/// registration, when it reaches the wrong handler, or when it starts being
/// advertised.
fn krabka_private_dispatches() -> [(&'static str, ApiKeyCode, ContextHandler); 10] {
    [
        (
            "AlterBarrierGroups",
            handlers::ALTER_BARRIER_GROUPS_API_KEY,
            alter_barrier_groups_adapter,
        ),
        (
            "DescribeBarrierGroups",
            handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
            describe_barrier_groups_adapter,
        ),
        (
            "TriggerBarrier",
            handlers::TRIGGER_BARRIER_API_KEY,
            trigger_barrier_adapter,
        ),
        (
            "ListBarrierCuts",
            handlers::LIST_BARRIER_CUTS_API_KEY,
            list_barrier_cuts_adapter,
        ),
        (
            "WriteBarrierMarkers",
            handlers::WRITE_BARRIER_MARKERS_API_KEY,
            write_barrier_markers_adapter,
        ),
        (
            "SetTopicFreeze",
            handlers::SET_TOPIC_FREEZE_API_KEY,
            set_topic_freeze_adapter,
        ),
        (
            "DescribeTopicFreezes",
            handlers::DESCRIBE_TOPIC_FREEZES_API_KEY,
            describe_topic_freezes_adapter,
        ),
        (
            "ProposeBreakGlass",
            handlers::PROPOSE_BREAK_GLASS_API_KEY,
            propose_break_glass_adapter,
        ),
        (
            "ApproveBreakGlass",
            handlers::APPROVE_BREAK_GLASS_API_KEY,
            approve_break_glass_adapter,
        ),
        (
            "DescribeBreakGlass",
            handlers::DESCRIBE_BREAK_GLASS_API_KEY,
            describe_break_glass_adapter,
        ),
    ]
}

#[test]
fn registry_dispatches_every_krabka_private_key_to_its_own_handler() {
    let registry = build_registry();

    for (label, api_key, adapter) in krabka_private_dispatches() {
        let entry = registry
            .get(api_key)
            .unwrap_or_else(|| panic!("{label} ({api_key}) is registered"));

        assert!(let DispatchKind::Context(handler) = entry.kind(), "{label}");
        check!(std::ptr::fn_addr_eq(handler, adapter), "{label}");
        // Version 0 only, flexible framing, and exempt from the
        // request-quota accounting a Kafka client drives.
        check!(entry.body_flexible(0), "{label}");
        check!(
            entry.quota_policy() == RequestQuotaPolicy::InlineExempt,
            "{label}"
        );
    }
}

#[test]
fn no_krabka_private_key_reaches_api_versions() {
    let dispatched: BTreeSet<ApiKeyCode> = dispatched_apis()
        .into_iter()
        .map(|api| api.api_key)
        .collect();

    for (label, api_key, _) in krabka_private_dispatches() {
        check!(api_key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR, "{label}");
        check!(!dispatched.contains(&api_key), "{label}");
    }
}
