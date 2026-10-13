//! Tests for the krabka-private registration table: that every private api
//! key reaches its own adapter, and that none of them is advertised.

use {
    super::*,
    crate::{
        api_catalog::dispatched_apis,
        handlers::{
            self,
            registry::{DispatchKind, RequestQuotaPolicy, build_registry},
        },
    },
    assert2::{assert, check},
    std::collections::BTreeSet,
};

/// A krabka-private api key, the adapter that dispatch must reach, and the
/// label a failure reports.
struct PrivateDispatch {
    label: &'static str,
    api_key: ApiKeyCode,
    adapter: ContextHandler,
}

/// Every krabka-private api key, the adapter that dispatch must reach, and
/// the label a failure reports.
///
/// The five barrier keys and the five KFC-9 keys hold the same contract, so
/// one table covers both. An entry here fails when a key loses its
/// registration, when it reaches the wrong handler, or when it starts being
/// advertised.
fn krabka_private_dispatches() -> [PrivateDispatch; 10] {
    [
        PrivateDispatch {
            label: "AlterBarrierGroups",
            api_key: handlers::ALTER_BARRIER_GROUPS_API_KEY,
            adapter: alter_barrier_groups_adapter,
        },
        PrivateDispatch {
            label: "DescribeBarrierGroups",
            api_key: handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
            adapter: describe_barrier_groups_adapter,
        },
        PrivateDispatch {
            label: "TriggerBarrier",
            api_key: handlers::TRIGGER_BARRIER_API_KEY,
            adapter: trigger_barrier_adapter,
        },
        PrivateDispatch {
            label: "ListBarrierCuts",
            api_key: handlers::LIST_BARRIER_CUTS_API_KEY,
            adapter: list_barrier_cuts_adapter,
        },
        PrivateDispatch {
            label: "WriteBarrierMarkers",
            api_key: handlers::WRITE_BARRIER_MARKERS_API_KEY,
            adapter: write_barrier_markers_adapter,
        },
        PrivateDispatch {
            label: "SetTopicFreeze",
            api_key: handlers::SET_TOPIC_FREEZE_API_KEY,
            adapter: set_topic_freeze_adapter,
        },
        PrivateDispatch {
            label: "DescribeTopicFreezes",
            api_key: handlers::DESCRIBE_TOPIC_FREEZES_API_KEY,
            adapter: describe_topic_freezes_adapter,
        },
        PrivateDispatch {
            label: "ProposeBreakGlass",
            api_key: handlers::PROPOSE_BREAK_GLASS_API_KEY,
            adapter: propose_break_glass_adapter,
        },
        PrivateDispatch {
            label: "ApproveBreakGlass",
            api_key: handlers::APPROVE_BREAK_GLASS_API_KEY,
            adapter: approve_break_glass_adapter,
        },
        PrivateDispatch {
            label: "DescribeBreakGlass",
            api_key: handlers::DESCRIBE_BREAK_GLASS_API_KEY,
            adapter: describe_break_glass_adapter,
        },
    ]
}

#[test]
fn registry_dispatches_every_krabka_private_key_to_its_own_handler() {
    let registry = build_registry();

    for PrivateDispatch {
        label,
        api_key,
        adapter,
    } in krabka_private_dispatches()
    {
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

    for PrivateDispatch { label, api_key, .. } in krabka_private_dispatches() {
        check!(api_key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR, "{label}");
        check!(!dispatched.contains(&api_key), "{label}");
    }
}
