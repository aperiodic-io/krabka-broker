//! Behavior tests of [`TopicCreator`]: against a real in-process broker for
//! what the controller answers, and against a scripted controller listener
//! for the retry and version rules of the "forwarding" channel.

use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use bytes::{BufMut as _, Bytes, BytesMut};
use krabka_client_core::{MockBroker, MockReply};
use krabka_protocol::{
    Encode as _, UnknownTaggedFields,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_topics_request::CreatableTopic,
        create_topics_response::CreatableTopicResult,
        envelope_request,
        envelope_response::EnvelopeResponse,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use krabka_raft::NodeId;

use super::*;
use crate::test_support::{
    ControllerPeerAllowed, FakeMetadataSource, GrantsInPrincipalName, start_broker_no_audit,
    start_broker_no_audit_with,
};

/// A retry timeout short enough that a test sees [`TopicCreatorError::Timeout`]
/// without a wait of [`RETRY_TIMEOUT`].
const SHORT_RETRY_TIMEOUT: Time = millis(300);

fn request(name: &str) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_owned(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }
}

fn identity(principal_name: &str) -> ForwardedIdentity {
    ForwardedIdentity {
        principal_name: principal_name.to_owned(),
        client_address: IpAddr::from([127, 0, 0, 1]),
        client_id: "admin-client".to_owned(),
        correlation_id: 42,
    }
}

fn response(row: CreatableTopicResult) -> CreateTopicsResponse {
    CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![row],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// The row of a topic that the create handler refused before it minted a
/// topic id.
fn refused_row(name: &str, error_code: i16, error_message: &str) -> CreatableTopicResult {
    CreatableTopicResult {
        name: name.to_owned(),
        topic_id: ProtoUuid([0; 16]),
        error_code,
        error_message: Some(error_message.to_owned()),
        num_partitions: -1,
        replication_factor: -1,
        configs: Some(Vec::new()),
        topic_config_error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// The id the controller committed for `name`.
fn committed_topic_id(broker: &Broker, name: &str) -> ProtoUuid {
    let image = broker.controller.current_image();
    let record = image
        .topic(name)
        .unwrap_or_else(|| panic!("topic {name} is committed"));
    ProtoUuid(record.topic_id.into_bytes())
}

/// Which of the two entry points a case drives.
#[derive(Debug, Clone, Copy)]
enum Sender<'a> {
    /// `createTopicWithoutPrincipal`.
    Broker,
    /// `createTopicWithPrincipal` in the name of this principal.
    Principal(&'a str),
}

async fn create(
    creator: &TopicCreator,
    sender: Sender<'_>,
    request: CreateTopicsRequest,
) -> Result<CreateTopicsResponse, TopicCreatorError> {
    match sender {
        Sender::Broker => creator.create_topic_without_principal(request).await,
        Sender::Principal(name) => {
            creator
                .create_topic_with_principal(&identity(name), request)
                .await
        }
    }
}

/// `createTopicWithoutPrincipal` reaches the controller of a combined node
/// over its controller listener. The controller commits the topic, and a
/// second request for the same name gets Kafka's existence row.
#[tokio::test]
async fn without_principal_creates_a_topic_and_then_reports_that_it_exists() {
    let (handle, _dir) = start_broker_no_audit().await;
    let broker = handle.broker_arc_for_test();
    let creator = TopicCreator::new(&broker);

    let first = creator
        .create_topic_without_principal(request("fresh"))
        .await;

    // KIP-525 (v5+): the row carries the effective configs of the new topic.
    // The request set no overrides, so they are the defaults.
    let image = broker.controller.current_image();
    let configs = crate::test_support::described_creation_configs(
        &image,
        crate::test_support::DescribedCreationSetup {
            node: broker.config.node_id,
            topic: "fresh",
            overrides: &std::collections::BTreeMap::new(),
        },
    );
    let created = response(CreatableTopicResult {
        name: "fresh".to_owned(),
        topic_id: committed_topic_id(&broker, "fresh"),
        error_code: codes::NONE,
        error_message: None,
        num_partitions: 1,
        replication_factor: 1,
        configs: Some(configs),
        topic_config_error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    });
    check!(first == Ok(created));

    let second = creator
        .create_topic_without_principal(request("fresh"))
        .await;

    check!(
        second
            == Ok(response(refused_row(
                "fresh",
                codes::TOPIC_ALREADY_EXISTS,
                "Topic 'fresh' already exists.",
            )))
    );
    handle.shutdown().await;
}

/// The controller authorizes an enveloped request against the principal it
/// names, not against the broker that sent it. The broker's own identity
/// holds `ClusterAction` and `Create` on the `Cluster`, so it creates a topic
/// itself. A forwarded principal without `Create` still cannot create one
/// through it, and a principal that holds `Create` can.
#[tokio::test]
async fn the_controller_authorizes_the_forwarded_principal() {
    let (handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(ControllerPeerAllowed(GrantsInPrincipalName));
    })
    .await;
    let broker = handle.broker_arc_for_test();
    let creator = TopicCreator::new(&broker);
    let denied = |name: &str| {
        Some(response(refused_row(
            name,
            codes::TOPIC_AUTHORIZATION_FAILED,
            "Authorization failed.",
        )))
    };
    let cases = [
        // The broker's own identity may not describe the configs of the new
        // topic either, so KIP-525 withholds them too.
        (
            "the broker's own identity",
            Sender::Broker,
            "by-broker",
            None,
        ),
        (
            "a forwarded principal without Create",
            Sender::Principal("none"),
            "by-none",
            denied("by-none"),
        ),
        // The principal may not describe the configs of the new topic, so
        // KIP-525 withholds them and stamps `topicConfigErrorCode`.
        (
            "a forwarded principal with cluster Create",
            Sender::Principal("Cluster:Create"),
            "by-creator",
            None,
        ),
    ];

    for (case, sender, topic, want) in cases {
        let got = create(&creator, sender, request(topic)).await;

        let want = want.unwrap_or_else(|| {
            response(CreatableTopicResult {
                name: topic.to_owned(),
                topic_id: committed_topic_id(&broker, topic),
                error_code: codes::NONE,
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
                configs: Some(Vec::new()),
                topic_config_error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            })
        });
        check!(got == Ok(want), "case: {case}");
    }
    handle.shutdown().await;
}

/// The controller refuses the Envelope itself when the broker's own identity
/// lacks `ClusterAction`, and Kafka completes the request with that error. No
/// topic is created.
#[tokio::test]
async fn an_envelope_from_a_broker_without_cluster_action_is_refused() {
    let (handle, _dir) =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(GrantsInPrincipalName)).await;
    let broker = handle.broker_arc_for_test();
    let creator = TopicCreator::new(&broker);

    let got = creator
        .create_topic_with_principal(&identity("Cluster:Create"), request("refused"))
        .await;

    check!(
        got == Err(TopicCreatorError::Envelope(
            codes::CLUSTER_AUTHORIZATION_FAILED
        ))
    );
    check!(broker.controller.current_image().topic("refused").is_none());
    handle.shutdown().await;
}

/// A creator over `controller` that dials a plaintext controller listener at
/// the static quorum `voters`.
fn creator_over(controller: FakeMetadataSource, voters: Vec<(NodeId, String)>) -> TopicCreator {
    TopicCreator::from_parts(
        Arc::new(controller),
        ControllerDialer {
            outbound_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
            listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            server_name: "localhost".to_owned(),
            quorum_voters: voters,
        },
        "7".to_owned(),
    )
}

/// An address on which nothing listens.
async fn closed_address() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a probe listener");
    listener.local_addr().expect("probe address")
}

/// `NodeToControllerRequestThread` keeps a request while it finds no
/// controller, cannot connect, or gets no answer, and fails it with a timeout
/// once it is older than the retry timeout.
#[tokio::test]
async fn a_request_that_reaches_no_controller_times_out() {
    let closed = closed_address().await;
    let silent = FakeController::start(Script {
        answers: VecDeque::from([Answer::Silent]),
        create_topics_max: create_topics_request::LATEST_STABLE_VERSION,
    })
    .await;
    let cases = [
        ("no controller leader", None, Vec::new()),
        (
            "a leader without a known controller endpoint",
            Some(NodeId(42)),
            Vec::new(),
        ),
        (
            "a controller that refuses the connection",
            Some(NodeId(1)),
            vec![(NodeId(1), closed.to_string())],
        ),
        (
            "a controller that never answers",
            Some(NodeId(1)),
            vec![(NodeId(1), silent.addr().to_string())],
        ),
    ];

    for (case, leader, voters) in cases {
        for sender in [Sender::Broker, Sender::Principal("alice")] {
            silent.script(VecDeque::from([Answer::Silent; 8]));
            let creator = creator_over(
                FakeMetadataSource::builder().leader(leader).build(),
                voters.clone(),
            )
            .with_retry_timeout(SHORT_RETRY_TIMEOUT);

            let got = create(&creator, sender, request("late")).await;

            check!(
                got == Err(TopicCreatorError::Timeout),
                "case: {case}, {sender:?}"
            );
        }
    }
}

/// How the scripted controller answers one `CreateTopics` or `Envelope`.
#[derive(Debug, Clone, Copy)]
enum Answer {
    /// The topic is created.
    Created,
    /// `NOT_CONTROLLER` on the topic row, or on the Envelope itself.
    NotController,
    /// `NOT_CONTROLLER` on the topic row inside a served Envelope.
    EmbeddedNotController,
    /// The connection closes without an answer.
    Close,
    /// No answer.
    Silent,
}

/// What the scripted controller does.
struct Script {
    /// The answers to the next requests, in order. [`Answer::Created`] after
    /// the last one.
    answers: VecDeque<Answer>,
    /// The highest `CreateTopics` version the controller advertises.
    create_topics_max: i16,
}

/// What one `CreateTopics` request looked like on the wire of the scripted
/// controller.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Received {
    /// The client id of the connection.
    client_id: Option<String>,
    /// The embedded request of an Envelope, with its principal and client
    /// address. `None` for a plain `CreateTopics`.
    forwarded: Option<(ForwardedRequest, ForwardedPrincipal, IpAddr)>,
}

struct State {
    script: Script,
    connections: usize,
    received: Vec<Received>,
}

/// A controller listener that answers `ApiVersions`, `CreateTopics` and
/// `Envelope` from a script.
struct FakeController {
    broker: MockBroker,
    state: Arc<Mutex<State>>,
}

/// The created row the scripted controller answers with.
fn scripted_created_row(name: &str) -> CreatableTopicResult {
    CreatableTopicResult {
        name: name.to_owned(),
        topic_id: ProtoUuid([7; 16]),
        error_code: codes::NONE,
        error_message: None,
        num_partitions: 1,
        replication_factor: 1,
        configs: Some(Vec::new()),
        topic_config_error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn flexible_for(api_key: i16, version: i16) -> bool {
    match api_key {
        create_topics_request::API_KEY => create_topics_request::is_flexible(version),
        envelope_request::API_KEY => true,
        _ => false,
    }
}

use crate::test_support::scripted_response_body as encode_body;

/// The `CreateTopicsResponse` for `answer`.
fn create_topics_answer(answer: Answer, name: &str) -> CreateTopicsResponse {
    match answer {
        Answer::NotController | Answer::EmbeddedNotController => response(CreatableTopicResult {
            error_code: codes::NOT_CONTROLLER,
            ..scripted_created_row(name)
        }),
        _ => response(scripted_created_row(name)),
    }
}

impl FakeController {
    async fn start(script: Script) -> Self {
        let state = Arc::new(Mutex::new(State {
            script,
            connections: 0,
            received: Vec::new(),
        }));
        let handled = Arc::clone(&state);
        let broker =
            MockBroker::start_with_replies(move |api_key, version, correlation_id, body| {
                let mut state = handled.lock().expect("fake controller state");
                if api_key == api_versions_request::API_KEY {
                    state.connections += 1;
                    let advertised = [
                        (
                            api_versions_request::API_KEY,
                            0,
                            api_versions_request::MAX_VERSION,
                        ),
                        (
                            create_topics_request::API_KEY,
                            create_topics_request::MIN_VERSION,
                            state.script.create_topics_max,
                        ),
                        (envelope_request::API_KEY, 0, 0),
                    ];
                    let answer = ApiVersionsResponse {
                        api_keys: advertised
                            .into_iter()
                            .map(|(api_key, min_version, max_version)| ApiVersion {
                                api_key,
                                min_version,
                                max_version,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    };
                    // `ApiVersions` always answers with a v0 response header.
                    return MockReply::Respond(encode_body(&answer, version, false));
                }
                let answer = state.script.answers.pop_front().unwrap_or(Answer::Created);
                let mut frame = BytesMut::new();
                frame.put_i16(api_key);
                frame.put_i16(version);
                frame.put_i32(correlation_id);
                frame.put_slice(body);
                let parsed = crate::network::request::parse_request(&frame, flexible_for)
                    .expect("a request header");
                let client_id = parsed.client_id.map(ToOwned::to_owned);
                if api_key == envelope_request::API_KEY {
                    let outer =
                        envelope::decode_request(parsed.body, version).expect("an Envelope");
                    let forwarded = envelope::unwrap_request(
                        &outer.request_data,
                        flexible_for,
                        crate::api_catalog::UnstableApiVersions::Disabled,
                    )
                    .expect("an embedded request");
                    let principal =
                        envelope::deserialize_principal(outer.request_principal.as_deref())
                            .expect("a principal");
                    let address =
                        envelope::deserialize_client_host_address(&outer.client_host_address)
                            .expect("a client address");
                    let reply = envelope_answer(answer, &forwarded);
                    state.received.push(Received {
                        client_id,
                        forwarded: Some((forwarded, principal, address)),
                    });
                    reply
                } else {
                    state.received.push(Received {
                        client_id,
                        forwarded: None,
                    });
                    plain_answer(answer, version)
                }
            })
            .await;
        Self { broker, state }
    }

    fn addr(&self) -> SocketAddr {
        self.broker.addr
    }

    fn script(&self, answers: VecDeque<Answer>) {
        self.state
            .lock()
            .expect("fake controller state")
            .script
            .answers = answers;
    }

    fn connections(&self) -> usize {
        self.state
            .lock()
            .expect("fake controller state")
            .connections
    }

    fn received(&self) -> Vec<Received> {
        self.state
            .lock()
            .expect("fake controller state")
            .received
            .clone()
    }
}

fn plain_answer(answer: Answer, version: i16) -> MockReply {
    match answer {
        Answer::Close => MockReply::Close,
        Answer::Silent => MockReply::Silent,
        answer => MockReply::Respond(encode_body(
            &create_topics_answer(answer, "scripted"),
            version,
            create_topics_request::is_flexible(version),
        )),
    }
}

fn envelope_answer(answer: Answer, forwarded: &ForwardedRequest) -> MockReply {
    let envelope = match answer {
        Answer::Close => return MockReply::Close,
        Answer::Silent => return MockReply::Silent,
        Answer::NotController => EnvelopeResponse {
            response_data: None,
            error_code: codes::NOT_CONTROLLER,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        answer => {
            let mut body = BytesMut::new();
            create_topics_answer(answer, "scripted")
                .encode(&mut body, forwarded.api_version)
                .expect("encode the embedded answer");
            EnvelopeResponse {
                response_data: Some(envelope::wrap_response(forwarded, &body)),
                error_code: codes::NONE,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        }
    };
    MockReply::Respond(encode_body(&envelope, 0, true))
}

/// The embedded request that `identity("alice")` and `request("scripted")`
/// make at `version`.
fn forwarded_scripted(version: i16) -> (ForwardedRequest, ForwardedPrincipal, IpAddr) {
    let mut body = BytesMut::new();
    request("scripted")
        .encode(&mut body, version)
        .expect("encode the request");
    (
        ForwardedRequest {
            api_key: create_topics_request::API_KEY,
            api_version: version,
            correlation_id: 42,
            client_id: Some("admin-client".to_owned()),
            body: body.freeze(),
            body_flexible: create_topics_request::is_flexible(version),
        },
        ForwardedPrincipal {
            name: "alice".to_owned(),
            token_authenticated: false,
        },
        IpAddr::from([127, 0, 0, 1]),
    )
}

/// [`scripted_created_row`] as a response at `version` carries it. The topic
/// id arrived in v7, and the partition count, the replication factor and the
/// configs in v5, so an older response leaves them at their defaults.
fn created_row_at(version: i16) -> CreatableTopicResult {
    let row = scripted_created_row("scripted");
    match version {
        7.. => row,
        5..=6 => CreatableTopicResult {
            topic_id: ProtoUuid([0; 16]),
            ..row
        },
        _ => CreatableTopicResult {
            topic_id: ProtoUuid([0; 16]),
            num_partitions: -1,
            replication_factor: -1,
            ..row
        },
    }
}

/// `NodeToControllerRequestThread.handleResponse` sends a request again, on a
/// new connection, after a disconnect and after an answer that carries
/// `NOT_CONTROLLER`. An enveloped request carries the client's identity, and
/// its `CreateTopics` version is the highest one that both sides support.
#[tokio::test]
async fn a_request_goes_again_after_a_disconnect_or_not_controller() {
    /// The case, the sender, the scripted answers, the highest version the
    /// controller advertises, and what the controller received.
    type Case<'a> = (&'a str, Sender<'a>, &'a [Answer], i16, Vec<Received>);
    const LATEST: i16 = create_topics_request::LATEST_STABLE_VERSION;
    let plain = || Received {
        client_id: Some("7".to_owned()),
        forwarded: None,
    };
    let enveloped = |version: i16| Received {
        client_id: Some("7".to_owned()),
        forwarded: Some(forwarded_scripted(version)),
    };
    let cases: [Case<'_>; 7] = [
        (
            "plain, answered at once",
            Sender::Broker,
            &[],
            LATEST,
            vec![plain()],
        ),
        (
            "plain, after NOT_CONTROLLER",
            Sender::Broker,
            &[Answer::NotController],
            LATEST,
            vec![plain(), plain()],
        ),
        (
            "plain, after a disconnect",
            Sender::Broker,
            &[Answer::Close],
            LATEST,
            vec![plain(), plain()],
        ),
        (
            "enveloped, after NOT_CONTROLLER on the envelope",
            Sender::Principal("alice"),
            &[Answer::NotController],
            LATEST,
            vec![enveloped(LATEST), enveloped(LATEST)],
        ),
        (
            "enveloped, after NOT_CONTROLLER inside the envelope",
            Sender::Principal("alice"),
            &[Answer::EmbeddedNotController],
            LATEST,
            vec![enveloped(LATEST), enveloped(LATEST)],
        ),
        (
            "enveloped, after a disconnect",
            Sender::Principal("alice"),
            &[Answer::Close],
            LATEST,
            vec![enveloped(LATEST), enveloped(LATEST)],
        ),
        // Below v5 neither the embedded request nor its response carries a
        // tagged-fields section in its header.
        (
            "enveloped, to a controller that stops at v4",
            Sender::Principal("alice"),
            &[],
            4,
            vec![enveloped(4)],
        ),
    ];

    for (case, sender, answers, create_topics_max, want) in cases {
        let controller = FakeController::start(Script {
            answers: answers.iter().copied().collect(),
            create_topics_max,
        })
        .await;
        let creator = creator_over(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .build(),
            vec![(NodeId(1), controller.addr().to_string())],
        );

        let got = create(&creator, sender, request("scripted")).await;

        check!(
            got == Ok(response(created_row_at(create_topics_max))),
            "case: {case}"
        );
        check!(controller.connections() == want.len(), "case: {case}");
        check!(controller.received() == want, "case: {case}");
    }
}

/// Kafka's `latestUsableVersion(CREATE_TOPICS)`: the highest stable version
/// in both ranges, and no version at all when the ranges do not meet.
#[test]
fn the_create_topics_version_is_the_highest_that_both_sides_support() {
    let latest = create_topics_request::LATEST_STABLE_VERSION;
    let unsupported = || {
        Err(TopicCreatorError::Protocol(format!(
            "The controller does not support CREATE_TOPICS at a version in {}..={latest}",
            create_topics_request::MIN_VERSION,
        )))
    };
    let cases = [
        ("the same range", Some((0, latest)), Ok(latest)),
        (
            "a controller ahead of this codec",
            Some((0, latest + 2)),
            Ok(latest),
        ),
        ("a controller behind this codec", Some((0, 4)), Ok(4)),
        (
            "a controller that does not advertise it",
            None,
            unsupported(),
        ),
        (
            "a controller below this codec's minimum",
            Some((0, create_topics_request::MIN_VERSION - 1)),
            unsupported(),
        ),
        (
            "a controller above this codec's maximum",
            Some((latest + 1, latest + 2)),
            unsupported(),
        ),
    ];

    for (case, controller, want) in cases {
        check!(create_topics_version(controller) == want, "case: {case}");
    }
}

#[test]
fn each_error_reads_as_kafka_reports_it() {
    let cases = [
        (
            TopicCreatorError::Timeout,
            "CreateTopicsRequest to controller timed out",
        ),
        (
            TopicCreatorError::Envelope(codes::CLUSTER_AUTHORIZATION_FAILED),
            "Cluster authorization failed.",
        ),
        (
            TopicCreatorError::Envelope(codes::PRINCIPAL_DESERIALIZATION_FAILURE),
            "the controller refused the envelope with error code 97",
        ),
        (
            TopicCreatorError::Protocol("codec: bad".to_owned()),
            "codec: bad",
        ),
    ];

    for (error, want) in cases {
        check!(error.to_string() == want);
    }
}

/// `ForwardedIdentity::of` takes the principal name, the client address and
/// the client id and the correlation id of the request.
#[test]
fn a_forwarded_identity_is_the_identity_of_the_request() {
    let principal = crate::test_support::principal("alice");
    let peer: SocketAddr = "10.1.2.3:50000".parse().expect("literal address");
    let ctx = crate::test_support::request_context(&principal, &peer, "admin-client")
        .with_correlation_id(9);

    let got = ForwardedIdentity::of(&ctx);

    assert!(
        got == ForwardedIdentity {
            principal_name: "alice".to_owned(),
            client_address: IpAddr::from([10, 1, 2, 3]),
            client_id: "admin-client".to_owned(),
            correlation_id: 9,
        }
    );
}

/// The embedded response must echo the correlation id of the embedded
/// request, as Kafka's `AbstractResponse.parseResponse` checks.
#[test]
fn an_embedded_response_with_another_correlation_id_is_refused() {
    let forwarded = ForwardedRequest {
        correlation_id: 41,
        ..forwarded_scripted(create_topics_request::LATEST_STABLE_VERSION).0
    };
    let mut body = BytesMut::new();
    response(scripted_created_row("scripted"))
        .encode(&mut body, forwarded.api_version)
        .expect("encode");
    let data: Bytes = envelope::wrap_response(&forwarded, &body);

    check!(
        parse_embedded_response(&data, 42, forwarded.api_version)
            == Err(TopicCreatorError::Protocol(
                "Correlation id for response (41) does not match request (42)".to_owned()
            ))
    );
}
