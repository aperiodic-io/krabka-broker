//! The KIP-853 request checks that `AddRaftVoter`, `RemoveRaftVoter` and
//! `UpdateRaftVoter` share on the controller listener and on the broker
//! listener, with Kafka's codes, order and messages.
//!
//! The order is that of `KafkaRaftClient.handleAddVoterRequest`,
//! `handleRemoveVoterRequest` and `handleUpdateVoterRequest`: the cluster id,
//! then the leader, then the voter key, then the endpoints. Each function
//! answers `None` when the request passes, so the caller goes on to the
//! reconfiguration itself.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_protocol::{
    owned::{
        add_raft_voter_request::AddRaftVoterRequest,
        remove_raft_voter_request::RemoveRaftVoterRequest,
        update_raft_voter_request::UpdateRaftVoterRequest,
        update_raft_voter_response::CurrentLeader,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{RaftError, kraft::transport::QuorumStateSnapshot, reconfig::ReconfigOutcome};

/// Kafka's `NOT_LEADER_OR_FOLLOWER`.
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// Kafka's `REQUEST_TIMED_OUT`.
pub const REQUEST_TIMED_OUT: i16 = 7;
/// Kafka's `UNSUPPORTED_VERSION`.
pub const UNSUPPORTED_VERSION: i16 = 35;
/// Kafka's `INVALID_REQUEST`.
pub const INVALID_REQUEST: i16 = 42;
/// Kafka's `FENCED_LEADER_EPOCH`.
pub const FENCED_LEADER_EPOCH: i16 = 74;
/// Kafka's `UNKNOWN_LEADER_EPOCH`.
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
/// Kafka's `INCONSISTENT_CLUSTER_ID`.
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
/// Kafka's `DUPLICATE_VOTER`.
pub const DUPLICATE_VOTER: i16 = 126;
/// Kafka's `VOTER_NOT_FOUND`.
pub const VOTER_NOT_FOUND: i16 = 127;
/// Kafka's `UNKNOWN_SERVER_ERROR`.
pub const UNKNOWN_SERVER_ERROR: i16 = -1;

/// The listener name a controller advertises its peer RPCs on, by the
/// convention of `controller_endpoint_addr`.
const CONTROLLER_LISTENER_NAME: &str = "CONTROLLER";

/// An error code and the message beside it.
pub type Refusal = (i16, Option<String>);

/// Kafka's `ReplicaKey.toString`, with the directory id in Kafka's base64
/// `Uuid` form.
#[must_use]
pub fn replica_key(voter_id: i32, directory_id: WireUuid) -> String {
    let directory = if directory_id == WireUuid::ZERO {
        "<undefined>".to_owned()
    } else {
        URL_SAFE_NO_PAD.encode(directory_id.0)
    };
    format!("ReplicaKey(id={voter_id}, directoryId={directory})")
}

/// Whether a request's cluster id names this cluster. `cluster_id` is
/// `java.util.UUID`'s hyphenated form, while krabka now reports its cluster
/// id in Kafka's base64 `Uuid` form (`Metadata`, `DescribeCluster`; see
/// #1042), so a caller may send either form of the same id, and both match.
fn names_this_cluster(request_cluster_id: &str, cluster_id: &str) -> bool {
    request_cluster_id == cluster_id
        || uuid::Uuid::parse_str(cluster_id)
            .is_ok_and(|id| URL_SAFE_NO_PAD.encode(id.as_bytes()) == request_cluster_id)
}

/// Kafka's `hasValidClusterId` refusal, with its message. An absent id is
/// valid.
fn cluster_id_refusal(request_cluster_id: Option<&str>, cluster_id: &str) -> Option<Refusal> {
    let request_cluster_id = request_cluster_id.filter(|id| !names_this_cluster(id, cluster_id))?;
    Some((
        INCONSISTENT_CLUSTER_ID,
        Some(format!(
            "The given id \"{request_cluster_id}\" doesn't match the cluster id \"{cluster_id}\""
        )),
    ))
}

/// The name of the leader's controller listener: Kafka's
/// `channel.listenerName()` on the leader, which a new or updated voter must
/// advertise. It is `None` when this node does not know its own endpoints.
#[must_use]
pub fn leader_listener_name(quorum: &QuorumStateSnapshot) -> Option<String> {
    let endpoints = &quorum.voters.get(quorum.leader_id?)?.endpoints;
    endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case(CONTROLLER_LISTENER_NAME))
        .or_else(|| endpoints.first())
        .map(|endpoint| endpoint.name.clone())
}

/// Whether `listeners` carry the leader's controller listener name. Kafka's
/// `ListenerName.normalised` makes the match case-insensitive.
fn carries_leader_listener<'a>(
    quorum: &QuorumStateSnapshot,
    mut names: impl Iterator<Item = &'a str>,
) -> bool {
    leader_listener_name(quorum)
        .is_none_or(|leader| names.any(|name| name.eq_ignore_ascii_case(&leader)))
}

/// A listener set is usable when every entry is named, hosted and on a real
/// port, no name repeats, and there is at least one.
#[must_use]
pub fn valid_wire_listeners<'a>(
    listeners: impl IntoIterator<Item = (&'a str, &'a str, u16)>,
) -> bool {
    let mut names = std::collections::BTreeSet::new();
    let mut count = 0usize;
    for (name, host, port) in listeners {
        count += 1;
        if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name) {
            return false;
        }
    }
    count != 0
}

fn leader_request_refusal(
    request_cluster: Option<&str>,
    cluster_id: &str,
    quorum: &QuorumStateSnapshot,
) -> Option<Refusal> {
    cluster_id_refusal(request_cluster, cluster_id)
        .or_else(|| (!quorum.is_leader).then_some((NOT_LEADER_OR_FOLLOWER, None)))
}

fn voter_identity_refusal(
    request_cluster: Option<&str>,
    cluster_id: &str,
    quorum: &QuorumStateSnapshot,
    valid: impl FnOnce() -> bool,
    invalid_message: &str,
) -> Option<Refusal> {
    leader_request_refusal(request_cluster, cluster_id, quorum)
        .or_else(|| (!valid()).then(|| (INVALID_REQUEST, Some(invalid_message.into()))))
}

/// The refusal of an `AddRaftVoter` request before the candidate probe, as
/// `KafkaRaftClient.handleAddVoterRequest` orders it.
#[must_use]
pub fn add_voter_refusal(
    request: &AddRaftVoterRequest,
    cluster_id: &str,
    quorum: &QuorumStateSnapshot,
) -> Option<Refusal> {
    if let Some(refusal) = voter_identity_refusal(
        request.cluster_id.as_deref(),
        cluster_id,
        quorum,
        || {
            request.voter_id >= 0
                && request.voter_directory_id != WireUuid::ZERO
                && valid_wire_listeners(request.listeners.iter().map(|listener| {
                    (
                        listener.name.as_str(),
                        listener.host.as_str(),
                        listener.port,
                    )
                }))
        },
        "Add voter request didn't include a valid voter",
    ) {
        return Some(refusal);
    }
    if !carries_leader_listener(
        quorum,
        request
            .listeners
            .iter()
            .map(|listener| listener.name.as_str()),
    ) {
        return Some((
            INVALID_REQUEST,
            Some(format!(
                "Add voter request didn't include the endpoint for the default listener {}",
                leader_listener_name(quorum).unwrap_or_default()
            )),
        ));
    }
    None
}

/// The refusal of a candidate whose `kraft.version` range does not cover the
/// finalized version: `INVALID_REQUEST`, as `AddVoterHandler` answers it.
#[must_use]
pub fn candidate_kraft_version_refusal(
    voter_id: i32,
    directory_id: WireUuid,
    finalized_version: u16,
) -> Refusal {
    (
        INVALID_REQUEST,
        Some(format!(
            "Aborted add voter operation for {} since the kraft.version range doesn't support \
             the finalized version {finalized_version}",
            replica_key(voter_id, directory_id)
        )),
    )
}

/// The refusal of a candidate that could not be asked for its
/// `ApiVersions`: `REQUEST_TIMED_OUT`, as `AddVoterHandler` answers it.
#[must_use]
pub fn candidate_unavailable_refusal(
    voter_id: i32,
    directory_id: WireUuid,
    error: &str,
) -> Refusal {
    (
        REQUEST_TIMED_OUT,
        Some(format!(
            "Aborted add voter operation for {} since API_VERSIONS returned an error {error}",
            replica_key(voter_id, directory_id)
        )),
    )
}

/// The refusal of a `RemoveRaftVoter` request before the reconfiguration, as
/// `KafkaRaftClient.handleRemoveVoterRequest` orders it.
#[must_use]
pub fn remove_voter_refusal(
    request: &RemoveRaftVoterRequest,
    cluster_id: &str,
    quorum: &QuorumStateSnapshot,
) -> Option<Refusal> {
    voter_identity_refusal(
        request.cluster_id.as_deref(),
        cluster_id,
        quorum,
        || request.voter_id >= 0 && request.voter_directory_id != WireUuid::ZERO,
        "Remove voter request didn't include a valid voter",
    )
}

/// The refusal code of an `UpdateRaftVoter` request before the
/// reconfiguration, as `KafkaRaftClient.handleUpdateVoterRequest` and
/// `UpdateVoterHandler` order it. The response carries no message.
#[must_use]
pub fn update_voter_refusal(
    request: &UpdateRaftVoterRequest,
    cluster_id: &str,
    quorum: &QuorumStateSnapshot,
) -> Option<i16> {
    if cluster_id_refusal(request.cluster_id.as_deref(), cluster_id).is_some() {
        return Some(INCONSISTENT_CLUSTER_ID);
    }
    let local_epoch = i64::from(quorum.leader_epoch);
    let request_epoch = i64::from(request.current_leader_epoch);
    if request_epoch < local_epoch {
        return Some(FENCED_LEADER_EPOCH);
    }
    if request_epoch > local_epoch {
        return Some(UNKNOWN_LEADER_EPOCH);
    }
    if !quorum.is_leader {
        return Some(NOT_LEADER_OR_FOLLOWER);
    }
    let feature = &request.k_raft_version_feature;
    if request.voter_id < 0
        || request.voter_directory_id == WireUuid::ZERO
        || feature.min_supported_version < 0
        || feature.max_supported_version < feature.min_supported_version
        || !valid_wire_listeners(request.listeners.iter().map(|listener| {
            (
                listener.name.as_str(),
                listener.host.as_str(),
                listener.port,
            )
        }))
        || !carries_leader_listener(
            quorum,
            request
                .listeners
                .iter()
                .map(|listener| listener.name.as_str()),
        )
    {
        return Some(INVALID_REQUEST);
    }
    None
}

/// `UpdateRaftVoterResponse.CurrentLeader`, as `RaftUtil.updateVoterResponse`
/// fills it in every answer: the leader id or -1, the epoch, and the leader's
/// host and port on the controller listener when this node knows them.
#[must_use]
pub fn update_voter_current_leader(quorum: &QuorumStateSnapshot) -> CurrentLeader {
    let endpoint = quorum.leader_id.and_then(|leader| {
        let name = leader_listener_name(quorum)?;
        quorum
            .voters
            .get(leader)?
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == name)
            .cloned()
    });
    CurrentLeader {
        leader_id: quorum
            .leader_id
            .map_or(-1, |leader| i32::try_from(leader.0).unwrap_or(-1)),
        leader_epoch: i32::try_from(quorum.leader_epoch).unwrap_or(i32::MAX),
        host: endpoint
            .as_ref()
            .map(|endpoint| endpoint.host.clone())
            .unwrap_or_default(),
        port: endpoint.map_or(0, |endpoint| i32::from(endpoint.port)),
        ..Default::default()
    }
}

/// The voter operation a request asked for, which its refusal names as Kafka's
/// `AddVoterHandler` and `RemoveVoterHandler` do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(as_str = gerund)]
pub enum VoterOperation {
    #[enum_str(name = "adding")]
    Add,
    #[enum_str(name = "removing")]
    Remove,
    #[enum_str(name = "updating")]
    Update,
}

/// The code and message of a reconfiguration's outcome, for the voter
/// `(voter_id, directory_id)` the request named.
#[must_use]
pub fn reconfiguration_refusal(
    result: Result<ReconfigOutcome, RaftError>,
    operation: VoterOperation,
    voter_id: i32,
    directory_id: WireUuid,
) -> Refusal {
    match result {
        Ok(ReconfigOutcome::Committed) => (0, None),
        Ok(ReconfigOutcome::NotLeader { .. }) | Err(RaftError::NotLeader { .. }) => {
            (NOT_LEADER_OR_FOLLOWER, None)
        }
        Err(RaftError::ReconfigInProgress) => (
            REQUEST_TIMED_OUT,
            Some(
                "Request timed out waiting for leader to handle previous voter change request"
                    .into(),
            ),
        ),
        // `AddVoterHandler` aborts a candidate that is not caught up with
        // `REQUEST_TIMED_OUT`, so the operator retries once it has fetched.
        Err(RaftError::VoterNotCaughtUp { .. }) => (
            REQUEST_TIMED_OUT,
            Some(format!(
                "Aborted add voter operation for {} since it is lagging behind",
                replica_key(voter_id, directory_id)
            )),
        ),
        Err(RaftError::DuplicateVoter(_)) => (
            DUPLICATE_VOTER,
            Some(format!(
                "The voter id for {} is already part of the set of voters",
                replica_key(voter_id, directory_id)
            )),
        ),
        Err(RaftError::VoterNotFound(_)) => (
            VOTER_NOT_FOUND,
            Some(format!(
                "Cannot remove voter {} from the set of voters",
                replica_key(voter_id, directory_id)
            )),
        ),
        Err(RaftError::UnsupportedKraftVersion(version)) => (
            UNSUPPORTED_VERSION,
            Some(format!(
                "Cluster doesn't support {} voter because the kraft.version feature is {version}",
                operation.gerund()
            )),
        ),
        // Kafka has no "invalid voter update" code, so a rejected change and a
        // malformed one land on the same `INVALID_REQUEST` that
        // `UpdateVoterHandler` and `KafkaRaftClient` return.
        Err(RaftError::InvalidVoterUpdate(message) | RaftError::ReconfigRejected(message)) => {
            (INVALID_REQUEST, Some(message))
        }
        Err(error) => (UNKNOWN_SERVER_ERROR, Some(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_protocol::owned::{
        add_raft_voter_request::Listener as AddListener,
        update_raft_voter_request::{KRaftVersionFeature, Listener as UpdateListener},
    };

    use super::*;
    use crate::NodeId;

    const CLUSTER: &str = "cluster-a";
    const DIRECTORY: WireUuid = WireUuid([9; 16]);

    /// Node 1 leading epoch 7 over voters 1 and 2, when `is_leader`, or node 2
    /// following it.
    fn quorum(is_leader: bool) -> QuorumStateSnapshot {
        let voter = |id: u64, port: u16| krabka_metadata::Voter {
            id: NodeId(id),
            directory_id: uuid::Uuid::from_u128(u128::from(id)),
            endpoints: vec![
                krabka_metadata::VoterEndpoint {
                    name: "PLAINTEXT".into(),
                    host: "broker-host".into(),
                    port: port + 1,
                },
                krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: format!("controller-{id}"),
                    port,
                },
            ],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        };
        QuorumStateSnapshot {
            leader_id: Some(NodeId(1)),
            leader_epoch: 7,
            high_watermark: 0,
            last_applied_record_timestamp_ms: 0,
            pending_record_processing_time_us_avg: 0,
            pending_record_processing_time_us_max: 0,
            quorum_high_watermark: 0,
            log_end_offset: 0,
            log_start_offset: 0,
            voters: krabka_metadata::VoterSet::from_voters([voter(1, 9093), voter(2, 9094)]),
            voted_directory_id: None,
            observers: Vec::new(),
            per_replica_fetch_offset: BTreeMap::new(),
            per_replica_last_fetch_ms: BTreeMap::new(),
            per_replica_last_caught_up_ms: BTreeMap::new(),
            is_leader,
            current_state: if is_leader { "leader" } else { "follower" },
        }
    }

    fn foreign_cluster_refusal() -> Refusal {
        (
            INCONSISTENT_CLUSTER_ID,
            Some("The given id \"cluster-b\" doesn't match the cluster id \"cluster-a\"".into()),
        )
    }

    #[test]
    fn a_cluster_id_matches_in_either_form() {
        let id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        let local = id.to_string();
        for (label, request, names_it) in [
            ("hyphenated", local.clone(), true),
            ("Kafka base64", URL_SAFE_NO_PAD.encode(id.as_bytes()), true),
            (
                "another cluster",
                uuid::Uuid::from_u128(7).to_string(),
                false,
            ),
            ("not an id", "cluster-b".to_owned(), false),
        ] {
            check!(names_this_cluster(&request, &local) == names_it, "{label}");
        }
    }

    #[test]
    fn replica_key_names_the_directory_in_kafka_form() {
        check!(replica_key(3, WireUuid::ZERO) == "ReplicaKey(id=3, directoryId=<undefined>)");
        check!(replica_key(3, DIRECTORY) == "ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ)");
    }

    #[test]
    fn add_voter_checks_run_in_kafka_order() {
        let listener = |name: &str| AddListener {
            name: name.into(),
            host: "host-3".into(),
            port: 9095,
            ..Default::default()
        };
        let request = |edit: fn(&mut AddRaftVoterRequest)| {
            let mut request = AddRaftVoterRequest {
                // A request with no cluster id skips Kafka's cluster check.
                cluster_id: None,
                voter_id: 3,
                voter_directory_id: DIRECTORY,
                listeners: vec![listener("controller")],
                ..Default::default()
            };
            edit(&mut request);
            request
        };
        let invalid_voter = Some((
            INVALID_REQUEST,
            Some("Add voter request didn't include a valid voter".into()),
        ));
        let rows: Vec<(&str, AddRaftVoterRequest, bool, Option<Refusal>)> =
            vec![
            (
                "foreign cluster id, on a follower",
                request(|r| r.cluster_id = Some("cluster-b".into())),
                false,
                Some(foreign_cluster_refusal()),
            ),
            ("a follower", request(|_| {}), false, Some((NOT_LEADER_OR_FOLLOWER, None))),
            (
                "a follower, with an invalid voter",
                request(|r| r.voter_id = -1),
                false,
                Some((NOT_LEADER_OR_FOLLOWER, None)),
            ),
            ("negative voter id", request(|r| r.voter_id = -1), true, invalid_voter.clone()),
            (
                "zero directory id",
                request(|r| r.voter_directory_id = WireUuid::ZERO),
                true,
                invalid_voter.clone(),
            ),
            ("no listeners", request(|r| r.listeners.clear()), true, invalid_voter),
            (
                "no controller listener",
                request(|r| r.listeners[0].name = "PLAINTEXT".into()),
                true,
                Some((
                    INVALID_REQUEST,
                    Some(
                        "Add voter request didn't include the endpoint for the default listener \
                         CONTROLLER"
                            .into(),
                    ),
                )),
            ),
            ("this cluster", request(|r| r.cluster_id = Some(CLUSTER.into())), true, None),
            ("no cluster id", request(|_| {}), true, None),
        ];
        for (label, request, is_leader, expected) in rows {
            check!(
                add_voter_refusal(&request, CLUSTER, &quorum(is_leader)) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn remove_voter_checks_run_in_kafka_order() {
        let request =
            |cluster_id: Option<&str>, voter_id, voter_directory_id| RemoveRaftVoterRequest {
                cluster_id: cluster_id.map(str::to_owned),
                voter_id,
                voter_directory_id,
                ..Default::default()
            };
        let invalid_voter = Some((
            INVALID_REQUEST,
            Some("Remove voter request didn't include a valid voter".into()),
        ));
        let rows = [
            (
                "foreign cluster id",
                request(Some("cluster-b"), 2, DIRECTORY),
                true,
                Some(foreign_cluster_refusal()),
            ),
            (
                "a follower",
                request(None, -1, DIRECTORY),
                false,
                Some((NOT_LEADER_OR_FOLLOWER, None)),
            ),
            (
                "negative voter id",
                request(None, -1, DIRECTORY),
                true,
                invalid_voter.clone(),
            ),
            (
                "zero directory id",
                request(None, 2, WireUuid::ZERO),
                true,
                invalid_voter,
            ),
            (
                "a valid voter",
                request(Some(CLUSTER), 2, DIRECTORY),
                true,
                None,
            ),
        ];
        for (label, request, is_leader, expected) in rows {
            check!(
                remove_voter_refusal(&request, CLUSTER, &quorum(is_leader)) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn update_voter_checks_run_in_kafka_order() {
        let request = |edit: fn(&mut UpdateRaftVoterRequest)| {
            let mut request = UpdateRaftVoterRequest {
                // A request with no cluster id skips Kafka's cluster check.
                cluster_id: None,
                voter_id: 2,
                voter_directory_id: DIRECTORY,
                current_leader_epoch: 7,
                k_raft_version_feature: KRaftVersionFeature {
                    min_supported_version: 0,
                    max_supported_version: 1,
                    ..Default::default()
                },
                listeners: vec![UpdateListener {
                    name: "CONTROLLER".into(),
                    host: "controller-2".into(),
                    port: 9094,
                    ..Default::default()
                }],
                ..Default::default()
            };
            edit(&mut request);
            request
        };
        let rows: Vec<(&str, UpdateRaftVoterRequest, bool, Option<i16>)> = vec![
            (
                "foreign cluster id",
                request(|r| r.cluster_id = Some("cluster-b".into())),
                true,
                Some(INCONSISTENT_CLUSTER_ID),
            ),
            (
                "an epoch below the local one",
                request(|r| r.current_leader_epoch = 6),
                true,
                Some(FENCED_LEADER_EPOCH),
            ),
            (
                "an epoch above the local one",
                request(|r| r.current_leader_epoch = 8),
                true,
                Some(UNKNOWN_LEADER_EPOCH),
            ),
            (
                "a follower",
                request(|_| {}),
                false,
                Some(NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "zero directory id",
                request(|r| r.voter_directory_id = WireUuid::ZERO),
                true,
                Some(INVALID_REQUEST),
            ),
            (
                "a negative kraft.version",
                request(|r| r.k_raft_version_feature.min_supported_version = -1),
                true,
                Some(INVALID_REQUEST),
            ),
            (
                "an inverted kraft.version range",
                request(|r| r.k_raft_version_feature.min_supported_version = 2),
                true,
                Some(INVALID_REQUEST),
            ),
            (
                "no controller listener",
                request(|r| r.listeners[0].name = "PLAINTEXT".into()),
                true,
                Some(INVALID_REQUEST),
            ),
            ("a valid update", request(|_| {}), true, None),
        ];
        for (label, request, is_leader, expected) in rows {
            check!(
                update_voter_refusal(&request, CLUSTER, &quorum(is_leader)) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn update_voter_answers_name_the_leader() {
        let mut unknown = quorum(false);
        unknown.leader_id = None;
        let rows = [
            (
                "a known leader",
                quorum(false),
                CurrentLeader {
                    leader_id: 1,
                    leader_epoch: 7,
                    host: "controller-1".into(),
                    port: 9093,
                    ..Default::default()
                },
            ),
            (
                "no leader",
                unknown,
                CurrentLeader {
                    leader_id: -1,
                    leader_epoch: 7,
                    ..Default::default()
                },
            ),
        ];
        for (label, quorum, expected) in rows {
            check!(update_voter_current_leader(&quorum) == expected, "{label}");
        }
    }

    #[test]
    fn reconfiguration_outcomes_get_kafka_codes() {
        let key = "ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ)";
        let rows: Vec<(&str, Result<ReconfigOutcome, RaftError>, Refusal)> = vec![
            ("committed", Ok(ReconfigOutcome::Committed), (0, None)),
            (
                "not the leader",
                Ok(ReconfigOutcome::NotLeader {
                    leader: Some(NodeId(2)),
                }),
                (NOT_LEADER_OR_FOLLOWER, None),
            ),
            (
                "a lagging candidate",
                Err(RaftError::VoterNotCaughtUp {
                    id: NodeId(3),
                    lag: 10,
                }),
                (
                    REQUEST_TIMED_OUT,
                    Some(format!(
                        "Aborted add voter operation for {key} since it is lagging behind"
                    )),
                ),
            ),
            (
                "a pending change",
                Err(RaftError::ReconfigInProgress),
                (
                    REQUEST_TIMED_OUT,
                    Some(
                        "Request timed out waiting for leader to handle previous voter change \
                         request"
                            .into(),
                    ),
                ),
            ),
            (
                "a duplicate voter",
                Err(RaftError::DuplicateVoter(NodeId(3))),
                (
                    DUPLICATE_VOTER,
                    Some(format!(
                        "The voter id for {key} is already part of the set of voters"
                    )),
                ),
            ),
            (
                "an unknown voter",
                Err(RaftError::VoterNotFound(NodeId(3))),
                (
                    VOTER_NOT_FOUND,
                    Some(format!("Cannot remove voter {key} from the set of voters")),
                ),
            ),
            (
                "a rejected change",
                Err(RaftError::ReconfigRejected("empty voter set".into())),
                (INVALID_REQUEST, Some("empty voter set".into())),
            ),
        ];
        for (label, result, expected) in rows {
            check!(
                reconfiguration_refusal(result, VoterOperation::Add, 3, DIRECTORY) == expected,
                "{label}"
            );
        }
    }

    /// `AddVoterHandler` and `RemoveVoterHandler` name their own operation in
    /// the `UNSUPPORTED_VERSION` message, and the level is the finalized one.
    #[test]
    fn unsupported_kraft_version_names_the_operation() {
        let rows = [
            (VoterOperation::Add, 0, "adding"),
            (VoterOperation::Remove, 0, "removing"),
            (VoterOperation::Add, 2, "adding"),
        ];
        for (operation, level, verb) in rows {
            check!(
                reconfiguration_refusal(
                    Err(RaftError::UnsupportedKraftVersion(level)),
                    operation,
                    3,
                    DIRECTORY
                ) == (
                    UNSUPPORTED_VERSION,
                    Some(format!(
                        "Cluster doesn't support {verb} voter because the kraft.version feature \
                         is {level}"
                    ))
                ),
                "{operation:?} at {level}"
            );
        }
    }
}
