//! KIP-219 throttle-echo audit.
//!
//! [`super::response::throttle_is_leading_field`] decides whether the dispatch
//! loop may report a request-quota delay by patching the first int32 of an
//! already-encoded response body. Getting that table wrong is silent in both
//! directions: an API missing from it answers `throttle_time_ms = 0` while the
//! broker holds the response, so a client never backs off and the quota
//! degrades into latency injection; an API wrongly in it has four bytes of
//! some other field overwritten.
//!
//! This module pins the table against the generated encoders. For every
//! `(api_key, version)` pair [`crate::api_catalog::dispatched_apis`] serves
//! it encodes that API's response with a sentinel in `throttle_time_ms` and
//! looks at where the sentinel lands, which is the byte layout the pinned
//! `krabka-protocol` response schemas produce rather than a restatement of the
//! table under test. An API added to the catalog but not to [`probes`] fails
//! [`every_advertised_api_is_classified`].

use {
    super::response::throttle_is_leading_field,
    crate::{
        api_catalog::dispatched_apis,
        handlers::{ApiKeyCode, ApiVersion, registry::build_registry},
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_protocol::Encode,
    std::collections::BTreeMap,
};

/// Written into `throttle_time_ms` before encoding. Any value works as long as
/// it is not a field default, so that finding it at offset 0 means the encoder
/// really put `ThrottleTimeMs` first.
const SENTINEL: i32 = 0x5EED_0219;

/// Where `ThrottleTimeMs` sits in an encoded response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThrottlePosition {
    /// First field: the dispatch loop can patch it in place.
    Leading,
    /// Present, but behind at least one other field. A leading patch would
    /// corrupt the response, so the delay is applied without being echoed.
    Buried,
    /// The schema has no `ThrottleTimeMs` at this version.
    Absent,
}

/// How far a schema-layout divergence gets at run time.
///
/// Where `ThrottleTimeMs` sits in the encoded body is a property of the
/// schema; whether a client ever sees a request-quota delay go unreported on
/// that API is a property of the dispatch entry's
/// [`crate::handlers::RequestQuotaPolicy`]. The two are recorded separately
/// because they answer different questions, and
/// [`recorded_reach_matches_the_dispatch_registry`] pins this half against the
/// registry rather than against prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuotaReach {
    /// `RequestQuotaPolicy::SelfAccounted`. The handler charges the quota and
    /// sets `ThrottleTimeMs` on the typed response before encoding, so
    /// `apply_request_quota` returns before it reaches the patch and the
    /// client does see the delay. The buried field costs nothing.
    SelfAccounted,
    /// `RequestQuotaPolicy::ApplyFallbackAccounting`. An ordinary request can
    /// be delayed by the request quota, and the response it waits behind
    /// reports `throttle_time_ms = 0`.
    FallbackAccounted,
    /// `RequestQuotaPolicy::ApplyFallbackAccounting` on an api that
    /// [`super::response::buried_throttle_is_reencoded`] names. The dispatch
    /// loop decodes the response, sets `ThrottleTimeMs` and encodes it again,
    /// so the client does see the delay.
    Reencoded,
    /// `RequestQuotaPolicy::InlineExempt`. The ordinary dispatch path never
    /// charges the request quota for this API, so `send_registry_response`
    /// never delays one. The unsupported-version reply path charges every
    /// `api_key` regardless of policy, so a request outside the advertised
    /// version range is the only one that can be delayed without an echo.
    UnsupportedVersionOnly,
}

/// `(api_key, first version, last version, reach)` ranges whose responses
/// carry `ThrottleTimeMs` somewhere other than first, so the dispatch loop's
/// leading-int32 patch cannot reach the field.
///
/// * `Produce` (0) and `ApiVersions` (18) carry it after a variable-length
///   array, so its offset is not knowable from the header alone.
/// * The delegation-token APIs (38-41) carry it as the last field, behind
///   principal strings, timestamps, the token list or the HMAC.
/// * `OffsetDelete` (47) leads with `ErrorCode`, an int16 that a leading int32
///   patch would overwrite. Its throttle is the one that sits at a fixed
///   offset, so it alone could be patched by a second, per-API offset table.
///   The dispatch loop instead decodes these five responses, sets the field
///   on the typed response, and encodes it again.
///
/// The `reach` column says what that costs on the wire, which is not the same
/// for all seven -- see [`QuotaReach`].
///
/// Mirrored as rows in the generated `docs/KIP_MATRIX.md`.
/// `aspect generate-kip-matrix` parses this constant, renders one row per
/// entry, and fails unless the set it parses is exactly the set of rows it
/// renders, so the page cannot omit a divergence added here; CI regenerates
/// the page and fails on a diff.
const THROTTLE_ECHO_DIVERGENCES: &[(ApiKeyCode, ApiVersion, ApiVersion, QuotaReach)] = &[
    (0, 1, 13, QuotaReach::SelfAccounted), // Produce
    (18, 1, 5, QuotaReach::SelfAccounted), // ApiVersions
    (38, 1, 3, QuotaReach::Reencoded),     // CreateDelegationToken
    (39, 1, 2, QuotaReach::Reencoded),     // RenewDelegationToken
    (40, 1, 2, QuotaReach::Reencoded),     // ExpireDelegationToken
    (41, 1, 3, QuotaReach::Reencoded),     // DescribeDelegationToken
    (47, 0, 0, QuotaReach::Reencoded),     // OffsetDelete
];

/// Encodes `response` at `version` and reports where `ThrottleTimeMs` landed.
///
/// `default_json` is the generated per-version field map for the same schema;
/// it distinguishes [`ThrottlePosition::Buried`] from
/// [`ThrottlePosition::Absent`] once the sentinel is known not to lead. A
/// version the type cannot encode -- Produce below v3 and Fetch below v4 route
/// to the `kafka_3_6_2` flavors, and `ListOffsets` v0 is hand-rolled -- has no
/// leading throttle by construction.
fn position<R: Encode>(
    response: &R,
    version: ApiVersion,
    default_json: &serde_json::Value,
) -> ThrottlePosition {
    let mut body = BytesMut::new();
    if response.encode(&mut body, version).is_ok()
        && body.len() >= 4
        && body[..4] == SENTINEL.to_be_bytes()
    {
        return ThrottlePosition::Leading;
    }
    if default_json.get("throttleTimeMs").is_some() {
        ThrottlePosition::Buried
    } else {
        ThrottlePosition::Absent
    }
}

type Probe = fn(ApiVersion) -> ThrottlePosition;

/// One probe per advertised `api_key`, keyed by the generated `API_KEY`
/// constant so an entry cannot drift onto the wrong API.
///
/// * `throttled`: the response type has a `throttle_time_ms` field, which the
///   probe sets to [`SENTINEL`].
/// * `unthrottled`: the response schema has no `ThrottleTimeMs` at any
///   version. The probe cannot set the sentinel, so it can only answer
///   [`ThrottlePosition::Absent`] or -- if a schema update grows the field --
///   [`ThrottlePosition::Buried`], which then fails the divergence test.
/// * `legacy_split: Api = N`: below version `N` the response is encoded from
///   the `kafka_3_6_2` flavor, mirroring `handlers::produce` and
///   `handlers::fetch::encode_fetch_response`.
fn probes() -> BTreeMap<ApiKeyCode, Probe> {
    krabka_macros::throttle_probes! {
        throttled:
            ListOffsets, Metadata, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, Heartbeat,
            LeaveGroup, SyncGroup, DescribeGroups, ListGroups, ApiVersions, CreateTopics,
            DeleteTopics, DeleteRecords, InitProducerId, OffsetForLeaderEpoch, AddPartitionsToTxn,
            AddOffsetsToTxn, EndTxn, TxnOffsetCommit, DescribeAcls, CreateAcls, DeleteAcls,
            DescribeConfigs, AlterConfigs, AlterReplicaLogDirs, DescribeLogDirs, CreatePartitions,
            CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken,
            DescribeDelegationToken, DeleteGroups, ElectLeaders, IncrementalAlterConfigs,
            AlterPartitionReassignments, ListPartitionReassignments, OffsetDelete,
            DescribeClientQuotas, AlterClientQuotas, DescribeUserScramCredentials,
            AlterUserScramCredentials, AlterPartition, UpdateFeatures, FetchSnapshot,
            DescribeCluster, DescribeProducers, BrokerRegistration, BrokerHeartbeat,
            UnregisterBroker, DescribeTransactions, ListTransactions, AllocateProducerIds,
            ConsumerGroupHeartbeat, ConsumerGroupDescribe, ControllerRegistration,
            GetTelemetrySubscriptions, PushTelemetry, AssignReplicasToDirs, ListConfigResources,
            DescribeTopicPartitions, ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch,
            ShareAcknowledge, AddRaftVoter, RemoveRaftVoter, UpdateRaftVoter, StreamsGroupHeartbeat,
            StreamsGroupDescribe, DescribeShareGroupOffsets, AlterShareGroupOffsets,
            DeleteShareGroupOffsets, StreamsGroupTopologyDescriptionUpdate, UnregisterController;
        unthrottled:
            SaslHandshake, WriteTxnMarkers, SaslAuthenticate, DescribeQuorum,
            InitializeShareGroupState, ReadShareGroupState, WriteShareGroupState,
            DeleteShareGroupState, ReadShareGroupStateSummary, GetReplicaLogInfo;
        legacy_split:
            Produce = 3, Fetch = 4;
    }
    .into_iter()
    .collect()
}

/// An api key at one version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ApiKeyVersion {
    api_key: ApiKeyCode,
    version: ApiVersion,
}

/// An api key and the reach of the quota it is charged under.
#[derive(Debug, PartialEq, Eq)]
struct ApiKeyReach {
    api_key: ApiKeyCode,
    reach: QuotaReach,
}

/// Every `(api_key, version)` pair the broker dispatches, in ascending order.
fn advertised_pairs() -> Vec<ApiKeyVersion> {
    let mut pairs: Vec<ApiKeyVersion> = dispatched_apis()
        .iter()
        .flat_map(|api| {
            let api_key = api.api_key;
            (api.min_version..=api.max_version)
                .map(move |version| ApiKeyVersion { api_key, version })
        })
        .collect();
    pairs.sort_unstable();
    pairs
}

/// A `(api_key, version)` pair where the table and the encoder disagree.
#[derive(Debug, PartialEq, Eq)]
struct Mismatch {
    api_key: ApiKeyCode,
    version: ApiVersion,
    encoder: ThrottlePosition,
    table_says_leading: bool,
}

#[test]
fn every_advertised_api_is_classified() {
    let probes = probes();
    let mut unclassified: Vec<ApiKeyCode> = advertised_pairs()
        .into_iter()
        .map(|pair| pair.api_key)
        .filter(|api_key| !probes.contains_key(api_key))
        .collect();
    unclassified.dedup();

    assert!(
        unclassified == Vec::<ApiKeyCode>::new(),
        "advertised api_keys with no throttle-echo probe: add them to \
         `throttle_audit::probes` and classify them in \
         `response::throttle_is_leading_field`"
    );
}

#[test]
fn throttle_table_matches_every_advertised_response_schema() {
    let probes = probes();
    let mismatches: Vec<Mismatch> = advertised_pairs()
        .into_iter()
        .filter_map(|ApiKeyVersion { api_key, version }| {
            let encoder = probes.get(&api_key)?(version);
            let table_says_leading = throttle_is_leading_field(api_key, version);
            (table_says_leading != (encoder == ThrottlePosition::Leading)).then_some(Mismatch {
                api_key,
                version,
                encoder,
                table_says_leading,
            })
        })
        .collect();

    assert!(mismatches == Vec::<Mismatch>::new());
}

#[test]
fn throttle_echo_divergences_are_the_recorded_ones() {
    let probes = probes();
    let buried: Vec<ApiKeyVersion> = advertised_pairs()
        .into_iter()
        .filter(|&ApiKeyVersion { api_key, version }| {
            probes
                .get(&api_key)
                .is_some_and(|probe| probe(version) == ThrottlePosition::Buried)
        })
        .collect();

    let mut recorded: Vec<ApiKeyVersion> = THROTTLE_ECHO_DIVERGENCES
        .iter()
        .flat_map(|&(api_key, min, max, _)| {
            (min..=max).map(move |version| ApiKeyVersion { api_key, version })
        })
        .collect();
    recorded.sort_unstable();

    assert!(buried == recorded);
}

/// The `reach` column of [`THROTTLE_ECHO_DIVERGENCES`] is a claim about the
/// dispatch table, not about the schemas, so it is pinned against the
/// assembled registry. Flipping an API between `InlineExempt` and
/// `ApplyFallbackAccounting` changes what a client observes on a divergent
/// API and must be re-recorded here and in `docs/KIP_MATRIX.md`.
#[test]
fn recorded_reach_matches_the_dispatch_registry() {
    use crate::handlers::RequestQuotaPolicy;

    let registry = build_registry();
    let observed: Vec<ApiKeyReach> = THROTTLE_ECHO_DIVERGENCES
        .iter()
        .map(|&(api_key, ..)| {
            let policy = registry
                .get(api_key)
                .unwrap_or_else(|| panic!("divergent api_key {api_key} is not registered"))
                .quota_policy();
            let reach = match policy {
                RequestQuotaPolicy::SelfAccounted => QuotaReach::SelfAccounted,
                RequestQuotaPolicy::ApplyFallbackAccounting
                    if super::response::buried_throttle_is_reencoded(api_key) =>
                {
                    QuotaReach::Reencoded
                }
                RequestQuotaPolicy::ApplyFallbackAccounting => QuotaReach::FallbackAccounted,
                RequestQuotaPolicy::InlineExempt => QuotaReach::UnsupportedVersionOnly,
            };
            ApiKeyReach { api_key, reach }
        })
        .collect();
    let recorded: Vec<ApiKeyReach> = THROTTLE_ECHO_DIVERGENCES
        .iter()
        .map(|&(api_key, _, _, reach)| ApiKeyReach { api_key, reach })
        .collect();

    assert!(observed == recorded);
}

#[test]
fn sentinel_probe_detects_a_leading_throttle_and_a_buried_one() {
    use krabka_protocol::owned::{
        metadata_response::{self, MetadataResponse},
        offset_delete_response::{self, OffsetDeleteResponse},
    };

    // Metadata moved ThrottleTimeMs to the front at v3, so the same struct
    // reads Absent at v2 and Leading at v3.
    let metadata = MetadataResponse {
        throttle_time_ms: SENTINEL,
        ..Default::default()
    };
    assert!(
        position(&metadata, 2, &metadata_response::default_json(2)) == ThrottlePosition::Absent
    );
    assert!(
        position(&metadata, 3, &metadata_response::default_json(3)) == ThrottlePosition::Leading
    );

    // OffsetDelete keeps ErrorCode in front of it at every version.
    let offset_delete = OffsetDeleteResponse {
        throttle_time_ms: SENTINEL,
        ..Default::default()
    };
    assert!(
        position(&offset_delete, 0, &offset_delete_response::default_json(0))
            == ThrottlePosition::Buried
    );
}
