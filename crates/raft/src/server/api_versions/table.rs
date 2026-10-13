//! The controller listener's advertised API table.
//!
//! Every entry is derived from the generated per-message constants in
//! `krabka-protocol`, so bumping the sibling revision moves the advertised
//! range and the codec that serves it together. Every API, the KIP-595 peer
//! RPCs included, is decoded at the version its request header carries and
//! answered at that version, so each advertises the generated
//! `MIN_VERSION..=MAX_VERSION` whole, as Kafka's `ApiKeys` advertises
//! `oldestVersion()..latestVersion()`. A request at a version outside the range
//! does not decode, and the connection closes.

use crate::config::ControllerApiVersion;

/// One advertised controller-listener range, taken whole from the generated
/// `krabka_protocol::owned::$request` message.
///
/// This table and the broker's KIP-919 Admin table in
/// `crates/broker/src/controller_admin.rs` both build their entries with it.
#[macro_export]
macro_rules! controller_api_version {
    ($request:ident) => {
        $crate::ControllerApiVersion {
            api_key: ::krabka_protocol::owned::$request::API_KEY,
            min_version: ::krabka_protocol::owned::$request::MIN_VERSION,
            max_version: ::krabka_protocol::owned::$request::MAX_VERSION,
            released_max: $crate::kafka_4_3_1_max(::krabka_protocol::owned::$request::API_KEY),
            flexible_min: ::krabka_protocol::owned::$request::FLEXIBLE_MIN,
        }
    };
}
use crate::controller_api_version as api_version;

/// Every API the controller listener serves itself, ordered by API key.
///
/// The KIP-919 Admin surface the broker attaches through
/// [`ControllerAdminRouter`](crate::ControllerAdminRouter) is advertised
/// alongside this table but declared by the broker, not here.
/// `BrokerRegistration` and `BrokerHeartbeat` are two of those: only the broker
/// crate holds the heartbeat registry that answering them reads and maintains,
/// so they are declared and served there rather than listed below.
pub(in crate::server) const CONTROLLER_LISTENER_APIS: &[ControllerApiVersion] = &[
    api_version!(fetch_request),
    api_version!(sasl_handshake_request),
    api_version!(api_versions_request),
    api_version!(sasl_authenticate_request),
    api_version!(vote_request),
    api_version!(begin_quorum_epoch_request),
    api_version!(end_quorum_epoch_request),
    api_version!(describe_quorum_request),
    api_version!(fetch_snapshot_request),
    api_version!(describe_cluster_request),
    api_version!(controller_registration_request),
    api_version!(add_raft_voter_request),
    api_version!(remove_raft_voter_request),
    api_version!(update_raft_voter_request),
];

/// The version from which requests for `api_key` carry tagged fields, if this
/// listener serves that API at all.
pub(in crate::server) fn flexible_min(api_key: i16) -> Option<i16> {
    CONTROLLER_LISTENER_APIS
        .iter()
        .find(|api| api.api_key == api_key)
        .map(|api| api.flexible_min)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            UnstableApiVersions,
            kraft::{
                transport::{api_key, wire::PeerRequest},
                types::NodeId,
            },
            network::addressing::api_version_for,
        },
        assert2::{assert, check},
        bytes::Bytes,
        krabka_ids::ApiKey,
        krabka_protocol::{
            Decode,
            owned::{
                add_raft_voter_request, api_versions_request,
                api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
                begin_quorum_epoch_request::{self, BeginQuorumEpochRequest},
                controller_registration_request, describe_cluster_request, describe_quorum_request,
                end_quorum_epoch_request::{self, EndQuorumEpochRequest},
                fetch_request::{self, FetchRequest},
                fetch_snapshot_request::{self, FetchSnapshotRequest},
                remove_raft_voter_request, sasl_authenticate_request, sasl_handshake_request,
                update_raft_voter_request,
                vote_request::{self, VoteRequest},
            },
        },
    };

    /// Whether `buf` is exactly one `T` at `version`, with nothing left over.
    fn decodes_whole<T>(buf: &[u8], version: i16) -> bool
    where
        T: for<'de> Decode<'de>,
    {
        let mut cur = buf;
        T::decode(&mut cur, version).is_ok() && cur.is_empty()
    }

    /// The ranges the listener should advertise, restated from the generated
    /// constants rather than from the table under test: every API, the KIP-595
    /// peer RPCs included, is decoded at the version its request header
    /// carries, so each names the whole generated range.
    fn expected_entries(api_versions_max: i16) -> Vec<ApiVersionEntry> {
        let entry = |api_key, min_version, max_version| ApiVersionEntry {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        let mut expected = vec![
            entry(
                fetch_request::API_KEY,
                fetch_request::MIN_VERSION,
                fetch_request::MAX_VERSION,
            ),
            entry(
                vote_request::API_KEY,
                vote_request::MIN_VERSION,
                vote_request::MAX_VERSION,
            ),
            entry(
                begin_quorum_epoch_request::API_KEY,
                begin_quorum_epoch_request::MIN_VERSION,
                begin_quorum_epoch_request::MAX_VERSION,
            ),
            entry(
                end_quorum_epoch_request::API_KEY,
                end_quorum_epoch_request::MIN_VERSION,
                end_quorum_epoch_request::MAX_VERSION,
            ),
            entry(
                fetch_snapshot_request::API_KEY,
                fetch_snapshot_request::MIN_VERSION,
                fetch_snapshot_request::MAX_VERSION,
            ),
            entry(
                sasl_handshake_request::API_KEY,
                sasl_handshake_request::MIN_VERSION,
                sasl_handshake_request::MAX_VERSION,
            ),
            entry(
                sasl_authenticate_request::API_KEY,
                sasl_authenticate_request::MIN_VERSION,
                sasl_authenticate_request::MAX_VERSION,
            ),
            entry(
                api_versions_request::API_KEY,
                api_versions_request::MIN_VERSION,
                api_versions_max,
            ),
            entry(
                describe_quorum_request::API_KEY,
                describe_quorum_request::MIN_VERSION,
                describe_quorum_request::MAX_VERSION,
            ),
            entry(
                describe_cluster_request::API_KEY,
                describe_cluster_request::MIN_VERSION,
                describe_cluster_request::MAX_VERSION,
            ),
            entry(
                controller_registration_request::API_KEY,
                controller_registration_request::MIN_VERSION,
                controller_registration_request::MAX_VERSION,
            ),
            entry(
                add_raft_voter_request::API_KEY,
                add_raft_voter_request::MIN_VERSION,
                add_raft_voter_request::MAX_VERSION,
            ),
            entry(
                remove_raft_voter_request::API_KEY,
                remove_raft_voter_request::MIN_VERSION,
                remove_raft_voter_request::MAX_VERSION,
            ),
            entry(
                update_raft_voter_request::API_KEY,
                update_raft_voter_request::MIN_VERSION,
                update_raft_voter_request::MAX_VERSION,
            ),
        ];
        expected.sort_unstable_by_key(|version| version.api_key);
        expected
    }

    /// A JVM controller peer negotiates off the `api_keys` table this listener
    /// puts on the wire, so every advertised range has to be a range the
    /// dispatch path can really decode. Comparing the decoded table whole means
    /// a `krabka-protocol` bump that widens a generated range without widening
    /// the handler shows up here.
    #[test]
    fn advertised_versions_are_the_versions_the_listener_decodes_with() {
        // Every key this listener answers itself is one Kafka 4.3.1 has at
        // the vendored range, except `ApiVersions`, whose KIP-1242 v5 is
        // trunk's.
        for (unstable, api_versions_max) in [
            (UnstableApiVersions::Disabled, 4),
            (
                UnstableApiVersions::Enabled,
                api_versions_request::MAX_VERSION,
            ),
        ] {
            let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            let body = super::super::api_versions_response_body(4, super::super::ApiVersionsView {
                image: &image,
                metadata_offset: -1,
                admin_router: None,
                unstable,
            });
            let response = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode response");
            assert!(
                response.api_keys == expected_entries(api_versions_max),
                "{unstable:?}"
            );
        }
    }

    /// The engine encodes each KIP-595 peer body at one captured version and
    /// labels the request header with the version [`api_version_for`] returns.
    /// That version has to be inside the advertised range, and the body has to
    /// decode at it, or a peer decodes the body at a version nobody wrote it
    /// at.
    #[test]
    fn peer_sends_carry_the_advertised_version_in_header_and_body() {
        type DecodesWhole = fn(&[u8], i16) -> bool;
        let cases: [(&str, i16, PeerRequest, DecodesWhole); 5] = [
            (
                "vote",
                api_key::VOTE,
                PeerRequest::Vote {
                    cluster_id: None,
                    voter_id: NodeId(1),
                    voter_directory_id: uuid::Uuid::nil(),
                    candidate_epoch: 3,
                    candidate: NodeId(2),
                    candidate_directory_id: uuid::Uuid::nil(),
                    last_epoch: 2,
                    last_offset: 9,
                    pre_vote: true,
                },
                decodes_whole::<VoteRequest>,
            ),
            (
                "begin quorum epoch",
                api_key::BEGIN_QUORUM_EPOCH,
                PeerRequest::BeginQuorumEpoch {
                    cluster_id: None,
                    voter_id: NodeId(2),
                    voter_directory_id: uuid::Uuid::nil(),
                    leader_endpoints: Vec::new(),
                    leader_id: NodeId(1),
                    leader_epoch: 4,
                },
                decodes_whole::<BeginQuorumEpochRequest>,
            ),
            (
                "end quorum epoch",
                api_key::END_QUORUM_EPOCH,
                PeerRequest::EndQuorumEpoch {
                    cluster_id: None,
                    leader_id: NodeId(1),
                    leader_epoch: 4,
                    preferred_candidates: Vec::new(),
                },
                decodes_whole::<EndQuorumEpochRequest>,
            ),
            (
                "fetch",
                api_key::FETCH,
                PeerRequest::Fetch {
                    cluster_id: None,
                    max_wait_ms: 0,
                    high_watermark: -1,
                    from: NodeId(2),
                    current_leader_epoch: 1,
                    fetch_epoch: 1,
                    fetch_offset: 5,
                    replica_directory_id: uuid::Uuid::nil(),
                },
                decodes_whole::<FetchRequest>,
            ),
            (
                "fetch snapshot",
                api_key::FETCH_SNAPSHOT,
                PeerRequest::FetchSnapshot {
                    cluster_id: None,
                    from: NodeId(2),
                    current_leader_epoch: 1,
                    snapshot_id: (10, 1),
                    position: 0,
                    max_bytes: 32,
                },
                decodes_whole::<FetchSnapshotRequest>,
            ),
        ];

        for (case, key, request, decodes) in cases {
            let advertised = CONTROLLER_LISTENER_APIS
                .iter()
                .find(|api| api.api_key == key)
                .unwrap_or_else(|| panic!("{case} is advertised"));
            let sent = api_version_for(ApiKey(key)).get();
            check!(
                (advertised.min_version..=advertised.max_version).contains(&sent),
                "{case}: header version against the advertised range"
            );
            let body: Bytes = request.encode();
            check!(
                decodes(&body, sent),
                "{case}: body decodes whole at the advertised version"
            );
        }
    }

    /// An API the listener does not serve has no flexibility rule of its own;
    /// the caller falls back to the Admin router's table for those.
    #[test]
    fn flexible_min_is_reported_only_for_served_apis() {
        check!(flexible_min(vote_request::API_KEY) == Some(vote_request::FLEXIBLE_MIN));
        check!(flexible_min(fetch_request::API_KEY) == Some(fetch_request::FLEXIBLE_MIN));
        check!(
            flexible_min(krabka_protocol::owned::create_topics_request::API_KEY) == None,
            "CreateTopics is an Admin-router API, not a listener-owned one"
        );
        check!(
            flexible_min(krabka_protocol::owned::broker_heartbeat_request::API_KEY) == None,
            "BrokerHeartbeat is served by the broker's handler through the Admin              router, so its framing rule comes from the router's table"
        );
    }
}
