//! Tests for the assembled dispatch table: which `api_key` maps to which
//! handler kind, and how the table agrees with the advertised api catalog.

use std::collections::BTreeSet;

use assert2::assert;

use super::*;
use crate::{
    api_catalog::dispatched_apis,
    handlers::{self, ApiKeyCode, KRABKA_PRIVATE_API_KEY_FLOOR},
};

#[test]
fn registry_registers_raw_context_handlers() {
    let registry = build_registry();

    for api_key in [
        ApiKey::Produce as i16,
        ApiKey::AssignReplicasToDirs as i16,
        ApiKey::Metadata as i16,
        ApiKey::OffsetCommit as i16,
        ApiKey::OffsetFetch as i16,
        ApiKey::FindCoordinator as i16,
        ApiKey::JoinGroup as i16,
        ApiKey::Heartbeat as i16,
        ApiKey::LeaveGroup as i16,
        ApiKey::SyncGroup as i16,
        ApiKey::DeleteGroups as i16,
        ApiKey::ListOffsets as i16,
        ApiKey::OffsetForLeaderEpoch as i16,
        ApiKey::CreateTopics as i16,
        ApiKey::DeleteTopics as i16,
        ApiKey::AlterConfigs as i16,
        ApiKey::IncrementalAlterConfigs as i16,
        ApiKey::DeleteRecords as i16,
        ApiKey::CreatePartitions as i16,
        ApiKey::DescribeGroups as i16,
        ApiKey::ListGroups as i16,
        ApiKey::OffsetDelete as i16,
        ApiKey::DescribeCluster as i16,
        ApiKey::DescribeProducers as i16,
        ApiKey::DescribeTransactions as i16,
        ApiKey::ListTransactions as i16,
        ApiKey::UnregisterBroker as i16,
        ApiKey::UnregisterController as i16,
        ApiKey::StreamsGroupTopologyDescriptionUpdate as i16,
        ApiKey::DescribeTopicPartitions as i16,
        ApiKey::ListConfigResources as i16,
        ApiKey::DescribeQuorum as i16,
        ApiKey::AddRaftVoter as i16,
        ApiKey::RemoveRaftVoter as i16,
        ApiKey::UpdateRaftVoter as i16,
        ApiKey::AlterPartition as i16,
        ApiKey::BrokerHeartbeat as i16,
        ApiKey::GetReplicaLogInfo as i16,
        ApiKey::ConsumerGroupHeartbeat as i16,
        ApiKey::ConsumerGroupDescribe as i16,
        ApiKey::ShareGroupDescribe as i16,
        ApiKey::ShareFetch as i16,
        ApiKey::ShareAcknowledge as i16,
        ApiKey::ShareGroupHeartbeat as i16,
        ApiKey::StreamsGroupHeartbeat as i16,
        ApiKey::StreamsGroupDescribe as i16,
        ApiKey::DescribeShareGroupOffsets as i16,
        ApiKey::AlterShareGroupOffsets as i16,
        ApiKey::DeleteShareGroupOffsets as i16,
        ApiKey::InitProducerId as i16,
        ApiKey::AddPartitionsToTxn as i16,
        ApiKey::EndTxn as i16,
        ApiKey::TxnOffsetCommit as i16,
        ApiKey::AddOffsetsToTxn as i16,
        ApiKey::WriteTxnMarkers as i16,
        ApiKey::FetchSnapshot as i16,
        ApiKey::AllocateProducerIds as i16,
        ApiKey::InitializeShareGroupState as i16,
        ApiKey::ReadShareGroupState as i16,
        ApiKey::WriteShareGroupState as i16,
        ApiKey::DeleteShareGroupState as i16,
        ApiKey::ReadShareGroupStateSummary as i16,
    ] {
        let key = api_key;
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(
                entry.kind(),
                DispatchKind::Context(_) | DispatchKind::Produce(_)
            ),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_telemetry_handlers() {
    let registry = build_registry();

    for key in [71, 72] {
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Telemetry(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_decoded_context_handlers() {
    let registry = build_registry();

    for api_key in [
        ApiKey::DescribeAcls as i16,
        ApiKey::CreateAcls as i16,
        ApiKey::DeleteAcls as i16,
        ApiKey::ElectLeaders as i16,
        ApiKey::AlterPartitionReassignments as i16,
        ApiKey::ListPartitionReassignments as i16,
        ApiKey::DescribeClientQuotas as i16,
        ApiKey::AlterClientQuotas as i16,
        ApiKey::DescribeUserScramCredentials as i16,
        ApiKey::AlterUserScramCredentials as i16,
        ApiKey::UpdateFeatures as i16,
    ] {
        let key = api_key;
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Context(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_auth_handlers() {
    let registry = build_registry();

    for key in [34, 38, 39, 40, 41] {
        let entry = registry
            .get(key)
            .unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Auth(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_reports_missing_keys() {
    let registry = build_registry();

    assert!(registry.get(9999).is_none());
}

#[test]
fn registry_and_api_catalog_cover_the_same_kafka_api_keys() {
    let registry = build_registry();
    let registered: BTreeSet<ApiKeyCode> = registry.registered_api_keys().collect();
    let dispatched: BTreeSet<ApiKeyCode> = dispatched_apis()
        .into_iter()
        .map(|api| api.api_key)
        .collect();

    // A key with a generated schema is in the catalog: every Kafka key, and
    // `GetReplicaLogInfo` (1020), the one krabka-private key that has a
    // schema in krabka-protocol.
    let registered_with_schema: BTreeSet<ApiKeyCode> = registered
        .iter()
        .copied()
        .filter(|key| ApiKey::from_i16(*key).is_some())
        .collect();

    // Every dispatched key is registered, and every registered key with a
    // generated schema is dispatched. The other krabka-private keys are
    // deliberately absent from the catalog: advertising them would put
    // UNKNOWN(1010) rows into kafka-broker-api-versions output, a visible
    // divergence from a real broker, and a client that does not find a key
    // negotiates (0, 0), which is right for a MIN = MAX = 0 request.
    assert!(dispatched.is_subset(&registered));
    assert!(registered_with_schema == dispatched);
}

/// The only krabka-private key in the catalog is `GetReplicaLogInfo` (1020),
/// and it is inter-broker only, so no listener a client can reach advertises
/// it and `kafka-broker-api-versions` never prints an `UNKNOWN(1020)` row.
#[test]
fn the_only_catalogued_krabka_private_key_is_inter_broker_only() {
    let floor = KRABKA_PRIVATE_API_KEY_FLOOR;
    let private: Vec<ApiKeyCode> = dispatched_apis()
        .into_iter()
        .map(|api| api.api_key)
        .filter(|key| *key >= floor)
        .collect();

    assert!(private == vec![ApiKey::GetReplicaLogInfo as i16]);
    assert!(ApiKey::GetReplicaLogInfo as i16 == 1020);
    assert!(crate::api_catalog::INTER_BROKER_ONLY_APIS.contains(&1020));
}

#[test]
fn registry_version_bounds_match_api_catalog() {
    let registry = build_registry();

    for api in dispatched_apis() {
        let entry = registry
            .get(api.api_key)
            .unwrap_or_else(|| panic!("registered api_key {}", api.api_key));
        assert!(
            entry.version_range() == (api.min_version..=api.max_version),
            "api_key {}",
            api.api_key
        );
    }
}

#[test]
fn registry_body_flexible_matches_selected_schema_boundaries() {
    use krabka_protocol::owned;

    let registry = build_registry();
    let cases = [
        (0, owned::produce_request::FLEXIBLE_MIN - 1, false),
        (0, owned::produce_request::FLEXIBLE_MIN, true),
        (1, owned::fetch_request::FLEXIBLE_MIN - 1, false),
        (1, owned::fetch_request::FLEXIBLE_MIN, true),
        (
            36,
            owned::sasl_authenticate_request::FLEXIBLE_MIN - 1,
            false,
        ),
        (36, owned::sasl_authenticate_request::FLEXIBLE_MIN, true),
        (17, i16::MAX, false),
        (999, 0, false),
    ];

    for (api_key, version, want) in cases {
        assert!(
            registry.body_flexible(api_key, version) == want,
            "api_key {api_key} version {version}"
        );
    }
}

/// `ApiVersions` is the one context dispatch that charges the request quota
/// itself: its `ThrottleTimeMs` sits behind the `ApiKeys` array, so the
/// dispatch loop's leading-int32 patch cannot report a delay on it and the
/// handler fills the field in instead.
#[test]
fn api_versions_is_a_self_accounted_context_dispatch() {
    let registry = build_registry();
    let entry = registry
        .get(ApiKey::ApiVersions as i16)
        .expect("ApiVersions");

    assert!(entry.is_context());
    assert!(entry.quota_policy() == RequestQuotaPolicy::SelfAccounted);
    assert!(entry.body_flexible(3));
    assert!(!entry.body_flexible(2));

    let handler = registry
        .get_context(ApiKey::ApiVersions as i16)
        .expect("context ApiVersions handler");
    assert!(std::ptr::fn_addr_eq(
        handler,
        handlers::api_versions::handle as ContextHandler
    ));
}

/// KIP-124 (#692): Kafka charges the request quota for every request, and
/// exempts only the apis it answers through `sendResponseExemptThrottle` and
/// the SASL handshake, which its authenticator answers. `Produce`, `Fetch`
/// and `ApiVersions` charge it in their handler. Kafka defines no
/// krabka-private api, so the ones in the private table are exempt. Every
/// other registered api, `GetReplicaLogInfo` included, is charged by the
/// dispatch loop.
#[test]
fn every_api_but_kafkas_exemptions_is_charged_to_the_request_quota() {
    use std::collections::BTreeMap;

    let registry = build_registry();
    let self_accounted = [ApiKey::Produce, ApiKey::Fetch, ApiKey::ApiVersions];
    let exempt = [
        ApiKey::SaslHandshake,
        ApiKey::SaslAuthenticate,
        ApiKey::WriteTxnMarkers,
        ApiKey::AlterPartition,
        ApiKey::FetchSnapshot,
        ApiKey::UpdateRaftVoter,
    ];

    let observed: BTreeMap<ApiKeyCode, RequestQuotaPolicy> = registry
        .registered_api_keys()
        .map(|api_key| {
            let policy = registry.get(api_key).expect("registered").quota_policy();
            (api_key, policy)
        })
        .collect();
    let expected: BTreeMap<ApiKeyCode, RequestQuotaPolicy> = observed
        .keys()
        .map(|&api_key| {
            let policy = if self_accounted.iter().any(|api| *api as i16 == api_key) {
                RequestQuotaPolicy::SelfAccounted
            } else if exempt.iter().any(|api| *api as i16 == api_key)
                // The krabka-private apis registered through the private
                // table. `GetReplicaLogInfo` (1020) is in the private range,
                // but it is registered like a Kafka api and stays charged.
                || (api_key >= handlers::KRABKA_PRIVATE_API_KEY_FLOOR
                    && ApiKey::from_i16(api_key).is_none())
            {
                RequestQuotaPolicy::InlineExempt
            } else {
                RequestQuotaPolicy::ApplyFallbackAccounting
            };
            (api_key, policy)
        })
        .collect();

    assert!(observed == expected);
}
