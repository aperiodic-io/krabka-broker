//! Behaviour tests for the voter-reconfiguration handlers: what each
//! request must carry before the quorum sees it, and what a refusal says.

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        add_raft_voter_request::AddRaftVoterRequest,
        add_raft_voter_response::AddRaftVoterResponse,
        remove_raft_voter_request::{self, RemoveRaftVoterRequest},
        remove_raft_voter_response::RemoveRaftVoterResponse,
    },
};

use super::*;
use crate::server::test_support::{decoded, single_voter_engine, wait_for_leader};

#[derive(Clone, Copy)]
struct ListenerPort(u16);

#[derive(Clone, Copy, Debug, PartialEq)]
enum ListenerUsability {
    Usable,
    Unusable,
}

#[derive(Clone, Copy)]
struct VoterApiVersion(i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct VoterResponseSetup<'a> {
    #[default(VoterApiVersion(remove_raft_voter_request::MAX_VERSION))]
    version: VoterApiVersion,
    #[default("response")]
    context: &'a str,
}

/// A wire listener set is usable only when every entry is named, hosted
/// and on a real port, no name repeats, and there is at least one.
#[test]
fn wire_listeners_must_be_usable_and_uniquely_named() {
    type Row<'a> = (
        &'a str,
        Vec<(&'a str, &'a str, ListenerPort)>,
        ListenerUsability,
    );
    let cases: Vec<Row<'_>> = vec![
        (
            "one usable listener",
            vec![("CONTROLLER", "host", ListenerPort(9093))],
            ListenerUsability::Usable,
        ),
        (
            "two, differently named",
            vec![
                ("CONTROLLER", "host", ListenerPort(9093)),
                ("PLAINTEXT", "host", ListenerPort(9092)),
            ],
            ListenerUsability::Usable,
        ),
        ("none at all", vec![], ListenerUsability::Unusable),
        (
            "a nameless listener",
            vec![("", "host", ListenerPort(9093))],
            ListenerUsability::Unusable,
        ),
        (
            "a hostless listener",
            vec![("CONTROLLER", "", ListenerPort(9093))],
            ListenerUsability::Unusable,
        ),
        (
            "port zero",
            vec![("CONTROLLER", "host", ListenerPort(0))],
            ListenerUsability::Unusable,
        ),
        (
            "a repeated name",
            vec![
                ("CONTROLLER", "host", ListenerPort(9093)),
                ("CONTROLLER", "other", ListenerPort(9094)),
            ],
            ListenerUsability::Unusable,
        ),
        (
            "one good listener followed by a bad one",
            vec![
                ("CONTROLLER", "host", ListenerPort(9093)),
                ("", "host", ListenerPort(9094)),
            ],
            ListenerUsability::Unusable,
        ),
    ];
    for (what, listeners, usable) in cases {
        let actual = if valid_wire_listeners(
            listeners
                .iter()
                .map(|&(name, host, port)| (name, host, port.0)),
        ) {
            ListenerUsability::Usable
        } else {
            ListenerUsability::Unusable
        };
        check!(actual == usable, "{what}");
    }
}

async fn add_response(
    request: AddRaftVoterRequest,
    engine: &KraftController,
    setup: VoterResponseSetup<'_>,
) -> AddRaftVoterResponse {
    let version = setup.version.0;
    let mut body = BytesMut::new();
    krabka_protocol::Encode::encode(&request, &mut body, version).expect("encode");
    let response = add_raft_voter_response(version, &body.freeze(), engine)
        .await
        .expect("response");
    decoded::<AddRaftVoterResponse>(&response, version)
}

async fn remove_response(
    request: &RemoveRaftVoterRequest,
    engine: &KraftController,
    setup: VoterResponseSetup<'_>,
) -> RemoveRaftVoterResponse {
    let version = setup.version.0;
    let mut body = BytesMut::new();
    request.encode(&mut body, version).expect("encode request");
    let bytes = remove_raft_voter_response(version, &body.freeze(), engine)
        .await
        .expect(setup.context);
    decoded::<RemoveRaftVoterResponse>(&bytes, version)
}

/// A reconfiguration request is refused before it reaches the quorum when
/// it names the wrong cluster, a negative voter, or a zero directory id,
/// each with the code `KafkaRaftClient.handleRemoveVoterRequest` answers.
///
/// A request that slipped past them would be applied to the voter set -- a
/// zero directory id in particular names no real incarnation.
#[tokio::test]
async fn a_malformed_reconfiguration_is_refused_before_the_quorum_sees_it() {
    const INVALID_REQUEST: i16 = 42;
    let version = remove_raft_voter_request::MAX_VERSION;
    let response_setup = VoterResponseSetup {
        version: VoterApiVersion(version),
        ..Default::default()
    };
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let cluster_id = engine.current_image().cluster_id().to_string();

    let real_directory = krabka_protocol::primitives::uuid::Uuid([7u8; 16]);
    let invalid_voter = RemoveRaftVoterResponse {
        error_code: INVALID_REQUEST,
        error_message: Some("Remove voter request didn't include a valid voter".into()),
        ..Default::default()
    };
    // (what it is, cluster id sent, voter id, directory id, response)
    let cases: Vec<(
        &str,
        Option<String>,
        i32,
        krabka_protocol::primitives::uuid::Uuid,
        RemoveRaftVoterResponse,
    )> = vec![
        (
            "another cluster's id",
            Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            2,
            real_directory,
            RemoveRaftVoterResponse {
                error_code: 104,
                error_message: Some(format!(
                    "The given id \"00000000-0000-0000-0000-0000000000ff\" doesn't match the \
                     cluster id \"{cluster_id}\""
                )),
                ..Default::default()
            },
        ),
        (
            "a negative voter id",
            Some(cluster_id.clone()),
            -1,
            real_directory,
            invalid_voter.clone(),
        ),
        (
            "a zero directory id",
            Some(cluster_id.clone()),
            2,
            krabka_protocol::primitives::uuid::Uuid::ZERO,
            invalid_voter,
        ),
    ];

    for (what, request_cluster_id, voter_id, voter_directory_id, expected) in cases {
        let request = RemoveRaftVoterRequest {
            cluster_id: request_cluster_id,
            voter_id,
            voter_directory_id,
            ..Default::default()
        };
        let response = remove_response(
            &request,
            &engine,
            VoterResponseSetup {
                context: "a refusal is still a response",
                ..response_setup
            },
        )
        .await;
        check!(response == expected, "{what}");
    }

    // Omitting the cluster id entirely is allowed: the field is optional,
    // and only a mismatch is a refusal.
    let request = RemoveRaftVoterRequest {
        cluster_id: None,
        voter_id: 2,
        voter_directory_id: real_directory,
        ..Default::default()
    };
    let response = remove_response(&request, &engine, response_setup).await;
    check!(
        response.error_code != INVALID_REQUEST,
        "a well-formed request reaches the quorum, got {}",
        response.error_code
    );

    // voter_id == 0 is valid (non-negative) and must not be rejected with INVALID_REQUEST
    let request = RemoveRaftVoterRequest {
        cluster_id: None,
        voter_id: 0,
        voter_directory_id: real_directory,
        ..Default::default()
    };
    let response = remove_response(&request, &engine, response_setup).await;
    check!(
        response.error_code != INVALID_REQUEST,
        "voter_id 0 reaches the quorum, got {}",
        response.error_code
    );
}

/// `AddRaftVoter` refuses a candidate it cannot place: an unusable
/// listener set is as disqualifying as a bad id.
///
/// A voter with no reachable endpoint cannot be fetched from, so admitting
/// one puts a member in the quorum that can never catch up -- and the
/// majority it counts toward is computed from the set, not from who
/// answers.
#[tokio::test]
async fn adding_a_voter_needs_an_id_and_a_reachable_listener() {
    use krabka_protocol::{
        Encode as _,
        owned::{
            add_raft_voter_request::{self, AddRaftVoterRequest, Listener},
            add_raft_voter_response::AddRaftVoterResponse,
        },
    };

    const INVALID_REQUEST: i16 = 42;
    let version = add_raft_voter_request::MAX_VERSION;
    let response_setup = VoterResponseSetup {
        version: VoterApiVersion(version),
        ..Default::default()
    };
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;

    let good_listener = || Listener {
        name: "CONTROLLER".to_owned(),
        host: "host-b".to_owned(),
        port: 9093,
        ..Default::default()
    };
    let directory = krabka_protocol::primitives::uuid::Uuid([9u8; 16]);

    let cases = [
        ("a negative voter id", -1, directory, vec![good_listener()]),
        (
            "a zero directory id",
            2,
            krabka_protocol::primitives::uuid::Uuid::ZERO,
            vec![good_listener()],
        ),
        ("no listeners at all", 2, directory, vec![]),
        (
            "a listener on port zero",
            2,
            directory,
            vec![Listener {
                port: 0,
                ..good_listener()
            }],
        ),
        (
            "a listener with no host",
            2,
            directory,
            vec![Listener {
                host: String::new(),
                ..good_listener()
            }],
        ),
    ];

    for (what, voter_id, voter_directory_id, listeners) in cases {
        let request = AddRaftVoterRequest {
            cluster_id: None,
            voter_id,
            voter_directory_id,
            listeners,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version).expect("encode");
        let bytes = add_raft_voter_response(version, &body.freeze(), &engine)
            .await
            .expect("a refusal is still a response");
        let response = decoded::<AddRaftVoterResponse>(&bytes, version);
        check!(response.error_code == INVALID_REQUEST, "{what}");
        check!(response.error_message.is_some(), "{what}: says why");
    }

    // Foreign cluster_id is refused with INCONSISTENT_CLUSTER_ID (104)
    let request = AddRaftVoterRequest {
        cluster_id: Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
        voter_id: 2,
        voter_directory_id: directory,
        listeners: vec![good_listener()],
        ..Default::default()
    };
    let response = add_response(request, &engine, response_setup).await;
    check!(response.error_code == 104);

    // Matching cluster_id is accepted (does not return INVALID_REQUEST)
    let request = AddRaftVoterRequest {
        cluster_id: Some(engine.current_image().cluster_id().to_string()),
        voter_id: 2,
        voter_directory_id: directory,
        listeners: vec![good_listener()],
        ..Default::default()
    };
    let response = add_response(request, &engine, response_setup).await;
    check!(response.error_code != INVALID_REQUEST);

    // At kraft.version >= 1, AddRaftVoter probes the candidate listeners.
    // Unreachable candidate fails probe with code 7.
    crate::server::test_support::activate_dynamic_membership(&engine).await;
    let request = AddRaftVoterRequest {
        cluster_id: None,
        voter_id: 2,
        voter_directory_id: directory,
        listeners: vec![good_listener()],
        ..Default::default()
    };
    let response = add_response(request, &engine, response_setup).await;
    check!(
        response.error_code == 7,
        "ApiVersions probe failed on unreachable candidate"
    );
    check!(
        response
            .error_message
            .as_deref()
            .is_some_and(|m| m.contains("API_VERSIONS returned an error"))
    );
}

/// `UpdateRaftVoter` additionally requires the caller to be at the leader's
/// epoch, to name this cluster if it names one at all, and to advertise a
/// coherent `kraft.version` range.
///
/// The epoch check is what stops a stale caller rewriting a voter's
/// endpoints against a quorum that has since moved on, and an inverted
/// version range advertises support for nothing.
///
/// Each check reports the code `KafkaRaftClient.handleUpdateVoterRequest`
/// reports for it. KIP-853 adds no "invalid voter update" code, so a
/// malformed request lands on one of the generic ones.
#[tokio::test]
async fn updating_a_voter_needs_the_cluster_the_epoch_and_a_coherent_range() {
    use krabka_protocol::{
        Encode as _,
        owned::{
            update_raft_voter_request::{
                self, KRaftVersionFeature, Listener, UpdateRaftVoterRequest,
            },
            update_raft_voter_response::UpdateRaftVoterResponse,
        },
    };

    const INCONSISTENT_CLUSTER_ID: i16 = 104;
    const UNKNOWN_LEADER_EPOCH: i16 = 75;
    const FENCED_LEADER_EPOCH: i16 = 74;
    const INVALID_REQUEST: i16 = 42;
    let version = update_raft_voter_request::MAX_VERSION;
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let epoch = i32::try_from(engine.quorum_state().await.expect("quorum").leader_epoch)
        .expect("epoch fits i32");
    let directory = krabka_protocol::primitives::uuid::Uuid([9u8; 16]);
    let listener = || Listener {
        name: "CONTROLLER".to_owned(),
        host: "host-b".to_owned(),
        port: 9093,
        ..Default::default()
    };
    let range = |min: i16, max: i16| KRaftVersionFeature {
        min_supported_version: min,
        max_supported_version: max,
        ..Default::default()
    };
    let update =
        |request_cluster_id, current_leader_epoch, k_raft_version_feature| UpdateRaftVoterRequest {
            cluster_id: request_cluster_id,
            voter_id: 1,
            voter_directory_id: directory,
            current_leader_epoch,
            k_raft_version_feature,
            listeners: vec![listener()],
            ..Default::default()
        };
    let code_for = async |request: UpdateRaftVoterRequest| {
        let mut body = BytesMut::new();
        request.encode(&mut body, version).expect("encode");
        let bytes = update_raft_voter_response(version, &body.freeze(), &engine)
            .await
            .expect("response");
        decoded::<UpdateRaftVoterResponse>(&bytes, version).error_code
    };

    // (what it is, cluster id, epoch offered, version range, code expected)
    let cases: Vec<(&str, Option<String>, i32, KRaftVersionFeature, i16)> = vec![
        (
            "another cluster's id",
            Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            epoch,
            range(0, 1),
            INCONSISTENT_CLUSTER_ID,
        ),
        (
            "an epoch ahead of the quorum's",
            Some(cluster_id.clone()),
            epoch + 1,
            range(0, 1),
            UNKNOWN_LEADER_EPOCH,
        ),
        (
            "an epoch the quorum has left behind",
            Some(cluster_id.clone()),
            epoch - 1,
            range(0, 1),
            FENCED_LEADER_EPOCH,
        ),
        (
            "an inverted kraft.version range",
            Some(cluster_id.clone()),
            epoch,
            range(2, 1),
            INVALID_REQUEST,
        ),
    ];

    for (what, request_cluster_id, current_leader_epoch, k_raft_version_feature, want) in cases {
        let request = update(
            request_cluster_id,
            current_leader_epoch,
            k_raft_version_feature,
        );
        check!(code_for(request).await == want, "{what}");
    }

    // `KafkaRaftClient.hasValidClusterId` answers true for a null cluster id,
    // so a request that names no cluster is not an inconsistent one: it runs
    // the rest of the checks and gets what the same request naming this
    // cluster gets.
    let named = code_for(update(Some(cluster_id.clone()), epoch, range(0, 1))).await;
    let anonymous = code_for(update(None, epoch, range(0, 1))).await;
    check!(anonymous == named);
    check!(anonymous != INCONSISTENT_CLUSTER_ID);

    // Every answer, a refusal included, names the leader and its controller
    // endpoint, as `RaftUtil.updateVoterResponse` fills it.
    let mut body = BytesMut::new();
    update(None, epoch - 1, range(0, 1))
        .encode(&mut body, version)
        .expect("encode");
    let bytes = update_raft_voter_response(version, &body.freeze(), &engine)
        .await
        .expect("response");
    check!(
        UpdateRaftVoterResponse::decode(&mut &bytes[..], version).expect("decode")
            == UpdateRaftVoterResponse {
                error_code: FENCED_LEADER_EPOCH,
                current_leader: krabka_protocol::owned::update_raft_voter_response::CurrentLeader {
                    leader_id: 1,
                    leader_epoch: epoch,
                    host: "controller-1".into(),
                    port: 9093,
                    ..Default::default()
                },
                ..Default::default()
            }
    );

    // A negative voter id is rejected as INVALID_REQUEST
    let mut bad_voter = update(Some(cluster_id.clone()), epoch, range(0, 1));
    bad_voter.voter_id = -1;
    check!(code_for(bad_voter).await == INVALID_REQUEST);

    // A zero directory id is rejected as INVALID_REQUEST
    let mut zero_dir = update(Some(cluster_id.clone()), epoch, range(0, 1));
    zero_dir.voter_directory_id = krabka_protocol::primitives::uuid::Uuid::ZERO;
    check!(code_for(zero_dir).await == INVALID_REQUEST);

    // A voter id of 0 is valid and not rejected as INVALID_REQUEST
    let mut zero_voter = update(Some(cluster_id.clone()), epoch, range(0, 1));
    zero_voter.voter_id = 0;
    check!(code_for(zero_voter).await != INVALID_REQUEST);
}

#[test]
fn add_voter_ack_when_committed_rules() {
    // Version 0 always waits for commitment regardless of request flag
    assert2::assert!(add_voter_ack_when_committed(0, false));
    assert2::assert!(add_voter_ack_when_committed(0, true));

    // Version >= 1 respects the request flag
    assert2::assert!(!add_voter_ack_when_committed(1, false));
    assert2::assert!(add_voter_ack_when_committed(1, true));
    assert2::assert!(!add_voter_ack_when_committed(2, false));
    assert2::assert!(add_voter_ack_when_committed(2, true));
}
