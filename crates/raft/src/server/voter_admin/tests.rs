//! Behaviour tests for the voter-reconfiguration handlers: what each
//! request must carry before the quorum sees it, and what a refusal says.

use {
    super::*,
    crate::{
        server::test_support::{
            activate_dynamic_membership, decoded, single_voter_engine, wait_for_leader,
        },
        voter_requests::ListenerAddress,
    },
    assert2::check,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request::AddRaftVoterRequest,
            add_raft_voter_response::AddRaftVoterResponse,
            remove_raft_voter_request::{self, RemoveRaftVoterRequest},
            remove_raft_voter_response::RemoveRaftVoterResponse,
        },
    },
};

#[derive(Clone, Copy)]
struct ListenerPort(u16);

#[derive(Clone, Copy, Debug, PartialEq)]
enum ListenerUsability {
    Usable,
    Unusable,
}

struct TestListener<'a> {
    name: &'a str,
    host: &'a str,
    port: ListenerPort,
}

fn test_listener(name: &'static str, host: &'static str, port: u16) -> TestListener<'static> {
    TestListener {
        name,
        host,
        port: ListenerPort(port),
    }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct VoterApiVersion(i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct VoterResponseSetup<'a> {
    #[default(VoterApiVersion(remove_raft_voter_request::MAX_VERSION))]
    version: VoterApiVersion,
    #[default("response")]
    context: &'a str,
}

#[bon::builder]
fn voter_response_setup(
    version: VoterApiVersion,
    #[builder(default = "response")] context: &str,
) -> VoterResponseSetup<'_> {
    VoterResponseSetup { version, context }
}

async fn remove_response_for(
    request: &RemoveRaftVoterRequest,
    engine: &KraftController,
    version: VoterApiVersion,
    context: &str,
) -> RemoveRaftVoterResponse {
    remove_response()
        .request(request)
        .engine(engine)
        .version(version)
        .context(context)
        .call()
        .await
}

async fn add_response_for(
    request: AddRaftVoterRequest,
    engine: &KraftController,
    version: VoterApiVersion,
    context: &str,
) -> AddRaftVoterResponse {
    add_response()
        .request(request)
        .engine(engine)
        .version(version)
        .context(context)
        .call()
        .await
}

/// A wire listener set is usable only when every entry is named, hosted
/// and on a real port, no name repeats, and there is at least one.
#[yare::parameterized(
    one_listener = {
        "one usable listener",
        vec![test_listener("CONTROLLER", "host", 9093)],
        ListenerUsability::Usable
    },
    distinct_listeners = {
        "two, differently named",
        vec![
            test_listener("CONTROLLER", "host", 9093),
            test_listener("PLAINTEXT", "host", 9092),
        ],
        ListenerUsability::Usable,
    },
    empty = { "none at all", vec![], ListenerUsability::Unusable },
    nameless = {
        "a nameless listener",
        vec![test_listener("", "host", 9093)],
        ListenerUsability::Unusable
    },
    hostless = {
        "a hostless listener",
        vec![test_listener("CONTROLLER", "", 9093)],
        ListenerUsability::Unusable
    },
    zero_port = {
        "port zero",
        vec![test_listener("CONTROLLER", "host", 0)],
        ListenerUsability::Unusable
    },
    duplicate_name = {
        "a repeated name",
        vec![
            test_listener("CONTROLLER", "host", 9093),
            test_listener("CONTROLLER", "other", 9094),
        ],
        ListenerUsability::Unusable,
    },
    later_bad_listener = {
        "one good listener followed by a bad one",
        vec![
            test_listener("CONTROLLER", "host", 9093),
            test_listener("", "host", 9094),
        ],
        ListenerUsability::Unusable,
    },
)]
fn wire_listeners_must_be_usable_and_uniquely_named(
    case: &str,
    listeners: Vec<TestListener<'static>>,
    expected: ListenerUsability,
) {
    let actual = if valid_wire_listeners(listeners.iter().map(|listener| ListenerAddress {
        name: listener.name,
        host: listener.host,
        port: listener.port.0,
    })) {
        ListenerUsability::Usable
    } else {
        ListenerUsability::Unusable
    };
    check!(actual == expected, "{case}");
}

#[bon::builder]
async fn add_response(
    request: AddRaftVoterRequest,
    engine: &KraftController,
    #[builder(into, default = VoterApiVersion (remove_raft_voter_request :: MAX_VERSION))] version: VoterApiVersion,
    #[builder(default = "response")] context: &'_ str,
) -> AddRaftVoterResponse {
    let setup = VoterResponseSetup { version, context };
    let version = setup.version.0;
    let mut body = BytesMut::new();
    krabka_protocol::Encode::encode(&request, &mut body, version).expect("encode");
    let response = add_raft_voter_response(version, &body.freeze(), engine)
        .await
        .expect("response");
    decoded::<AddRaftVoterResponse>(&response, version)
}

#[bon::builder]
async fn remove_response(
    request: &RemoveRaftVoterRequest,
    engine: &KraftController,
    #[builder(into, default = VoterApiVersion (remove_raft_voter_request :: MAX_VERSION))] version: VoterApiVersion,
    #[builder(default = "response")] context: &'_ str,
) -> RemoveRaftVoterResponse {
    let setup = VoterResponseSetup { version, context };
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
    struct Case {
        what: &'static str,
        request_cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: krabka_protocol::primitives::uuid::Uuid,
        expected: RemoveRaftVoterResponse,
    }

    const INVALID_REQUEST: i16 = 42;
    let version = remove_raft_voter_request::MAX_VERSION;
    let response_setup = voter_response_setup().version(version.into()).call();
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let cluster_id = engine.current_image().cluster_id().to_string();

    let real_directory = krabka_protocol::primitives::uuid::Uuid([7u8; 16]);
    let remove_request = |cluster_id, voter_id, voter_directory_id| RemoveRaftVoterRequest {
        cluster_id,
        voter_id,
        voter_directory_id,
        ..Default::default()
    };
    let invalid_voter = RemoveRaftVoterResponse {
        error_code: INVALID_REQUEST,
        error_message: Some("Remove voter request didn't include a valid voter".into()),
        ..Default::default()
    };
    let cases = vec![
        Case {
            what: "another cluster's id",
            request_cluster_id: Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            voter_id: 2,
            voter_directory_id: real_directory,
            expected: RemoveRaftVoterResponse {
                error_code: 104,
                error_message: Some(format!(
                    "The given id \"00000000-0000-0000-0000-0000000000ff\" doesn't match the \
                     cluster id \"{cluster_id}\""
                )),
                ..Default::default()
            },
        },
        Case {
            what: "a negative voter id",
            request_cluster_id: Some(cluster_id.clone()),
            voter_id: -1,
            voter_directory_id: real_directory,
            expected: invalid_voter.clone(),
        },
        Case {
            what: "a zero directory id",
            request_cluster_id: Some(cluster_id.clone()),
            voter_id: 2,
            voter_directory_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
            expected: invalid_voter,
        },
    ];

    for Case {
        what,
        request_cluster_id,
        voter_id,
        voter_directory_id,
        expected,
    } in cases
    {
        let request = remove_request(request_cluster_id, voter_id, voter_directory_id);
        let response = remove_response()
            .request(&request)
            .engine(&engine)
            .version(response_setup.version)
            .context("a refusal is still a response")
            .call()
            .await;
        check!(response == expected, "{what}");
    }

    // Omitting the cluster id entirely is allowed: the field is optional,
    // and only a mismatch is a refusal.
    let request = remove_request(None, 2, real_directory);
    let response = remove_response_for(
        &request,
        &engine,
        response_setup.version,
        response_setup.context,
    )
    .await;
    check!(
        response.error_code != INVALID_REQUEST,
        "a well-formed request reaches the quorum, got {}",
        response.error_code
    );

    // voter_id == 0 is valid (non-negative) and must not be rejected with INVALID_REQUEST
    let request = remove_request(None, 0, real_directory);
    let response = remove_response_for(
        &request,
        &engine,
        response_setup.version,
        response_setup.context,
    )
    .await;
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
    let response_setup = voter_response_setup().version(version.into()).call();
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
        ("a listener on port zero", 2, directory, vec![Listener {
            port: 0,
            ..good_listener()
        }]),
        ("a listener with no host", 2, directory, vec![Listener {
            host: String::new(),
            ..good_listener()
        }]),
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

    let build_candidate_request = |cluster_id: Option<String>| AddRaftVoterRequest {
        cluster_id,
        voter_id: 2,
        voter_directory_id: directory,
        listeners: vec![good_listener()],
        ..Default::default()
    };
    let response = add_response()
        .request(build_candidate_request(Some(
            "00000000-0000-0000-0000-0000000000ff".to_owned(),
        )))
        .engine(&engine)
        .version(response_setup.version)
        .context(response_setup.context)
        .call()
        .await;
    check!(response.error_code == 104);

    // Matching cluster_id is accepted (does not return INVALID_REQUEST)
    let request = build_candidate_request(Some(engine.current_image().cluster_id().to_string()));
    let response = add_response_for(
        request,
        &engine,
        response_setup.version,
        response_setup.context,
    )
    .await;
    check!(response.error_code != INVALID_REQUEST);

    // At kraft.version >= 1, AddRaftVoter probes the candidate listeners.
    // Unreachable candidate fails probe with code 7.
    activate_dynamic_membership(&engine).await;
    let request = build_candidate_request(None);
    let response = add_response_for(
        request,
        &engine,
        response_setup.version,
        response_setup.context,
    )
    .await;
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
    struct Case {
        what: &'static str,
        request_cluster_id: Option<String>,
        current_leader_epoch: i32,
        k_raft_version_feature: KRaftVersionFeature,
        want: i16,
    }

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

    let cases = vec![
        Case {
            what: "another cluster's id",
            request_cluster_id: Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            current_leader_epoch: epoch,
            k_raft_version_feature: range(0, 1),
            want: INCONSISTENT_CLUSTER_ID,
        },
        Case {
            what: "an epoch ahead of the quorum's",
            request_cluster_id: Some(cluster_id.clone()),
            current_leader_epoch: epoch + 1,
            k_raft_version_feature: range(0, 1),
            want: UNKNOWN_LEADER_EPOCH,
        },
        Case {
            what: "an epoch the quorum has left behind",
            request_cluster_id: Some(cluster_id.clone()),
            current_leader_epoch: epoch - 1,
            k_raft_version_feature: range(0, 1),
            want: FENCED_LEADER_EPOCH,
        },
        Case {
            what: "an inverted kraft.version range",
            request_cluster_id: Some(cluster_id.clone()),
            current_leader_epoch: epoch,
            k_raft_version_feature: range(2, 1),
            want: INVALID_REQUEST,
        },
    ];

    for Case {
        what,
        request_cluster_id,
        current_leader_epoch,
        k_raft_version_feature,
        want,
    } in cases
    {
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
