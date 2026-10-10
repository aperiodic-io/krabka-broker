//! Behavior tests of the `AlterPartition` sender. Scripted listeners show
//! where the request goes and at which version. A real in-process controller
//! shows what a krabka controller listener answers.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use assert2::assert;
use bytes::{BufMut as _, BytesMut};
use krabka_client_core::{MockBroker, MockReply};
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::{
    Decode as _, UnknownTaggedFields,
    owned::{
        alter_partition_request::{self, BrokerState, PartitionData, TopicData},
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_raft::NodeId;

use super::*;
use crate::{
    codes,
    isr_maintenance::test_support::{fake_source, reg_at, topic},
    metadata_source::MetadataSource,
};

/// The node that leads the metadata quorum. As node 3001 of the Kafka system
/// tests, it is a controller only, and no image holds a broker registration
/// for it.
const ACTIVE_CONTROLLER: NodeId = NodeId(3001);

/// The id of the topic `orders`.
const TOPIC_ID: uuid::Uuid = uuid::Uuid::from_u128(0x000A_11CE);

fn plaintext_dialer(quorum_voters: Vec<(NodeId, String)>) -> ControllerDialer {
    ControllerDialer {
        outbound_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
        listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        server_name: "localhost".to_owned(),
        quorum_voters,
    }
}

/// Broker 1, the leader of `orders-0`, proposes the ISR {1, 2}.
fn change() -> IsrChange<'static> {
    IsrChange {
        topic: "orders",
        partition: 0,
        new_isr: vec![NodeId(1), NodeId(2)],
        leader_epoch: 7,
        partition_epoch: 4,
    }
}

/// A KIP-853 voter set with the one voter `id`, whose CONTROLLER endpoint is
/// `addr`.
fn voters_record(id: NodeId, addr: SocketAddr) -> MetadataRecord {
    MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
        voters: krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_owned(),
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]),
    })
}

/// What broker 1 sees of a cluster that [`ACTIVE_CONTROLLER`] leads: the
/// topic `orders`, and the brokers 1 and 2, which both advertise
/// `brokers_at`. When `voter_at` is `Some`, the image also holds the voter
/// set, with the CONTROLLER endpoint of the active controller at that address.
fn cluster_image(brokers_at: SocketAddr, voter_at: Option<SocketAddr>) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&topic("orders", TOPIC_ID));
    for id in [NodeId(1), NodeId(2)] {
        image.apply(&reg_at(id, &brokers_at.ip().to_string(), brokers_at.port()));
    }
    if let Some(addr) = voter_at {
        image.apply(&voters_record(ACTIVE_CONTROLLER, addr));
    }
    image
}

/// A metadata source over `image`, with `leader` as the controller leader.
fn source_of(image: MetadataImage, leader: Option<NodeId>) -> Arc<dyn MetadataSource> {
    Arc::new(fake_source(image, leader))
}

/// The ISR members of [`change`] with their broker epochs from
/// [`cluster_image`].
fn isr_with_epochs() -> Vec<BrokerState> {
    [(1, 1), (2, 2)]
        .into_iter()
        .map(|(broker_id, broker_epoch)| BrokerState {
            broker_id,
            broker_epoch,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect()
}

/// The `AlterPartition` that [`change`] becomes, as a controller decodes it.
/// Version 2 carries the ISR in `new_isr`, and version 3 carries it in
/// `new_isr_with_epochs`.
fn expected_request(
    new_isr: Vec<i32>,
    new_isr_with_epochs: Vec<BrokerState>,
) -> AlterPartitionRequest {
    AlterPartitionRequest {
        broker_id: 1,
        broker_epoch: 1,
        topics: vec![TopicData {
            topic_id: WireUuid(TOPIC_ID.into_bytes()),
            partitions: vec![PartitionData {
                partition_index: 0,
                leader_epoch: 7,
                new_isr,
                new_isr_with_epochs,
                leader_recovery_state: 0,
                partition_epoch: 4,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

use crate::test_support::scripted_response_body as encode_body;

/// What a scripted listener received.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Received {
    /// The connections that sent `ApiVersions`. A client sends it first on
    /// each connection.
    connections: usize,
    /// Each `AlterPartition`, with its version.
    requests: Vec<(i16, AlterPartitionRequest)>,
}

/// A Kafka listener that advertises `AlterPartition` up to `max_version`, or
/// does not advertise it when `max_version` is `None`. It answers each
/// `AlterPartition` with `answer`.
struct ScriptedListener {
    mock: MockBroker,
    received: Arc<Mutex<Received>>,
}

impl ScriptedListener {
    async fn start(max_version: Option<i16>, answer: AlterPartitionResponse) -> Self {
        let received = Arc::new(Mutex::new(Received::default()));
        let state = Arc::clone(&received);
        let mock = MockBroker::start_with_replies(move |api_key, version, correlation_id, body| {
            let mut recorded = state.lock().expect("scripted listener state");
            if api_key == api_versions_request::API_KEY {
                recorded.connections += 1;
                let advertised = std::iter::once((
                    api_versions_request::API_KEY,
                    0,
                    api_versions_request::MAX_VERSION,
                ))
                .chain(max_version.map(|max_version| {
                    (
                        alter_partition_request::API_KEY,
                        alter_partition_request::MIN_VERSION,
                        max_version,
                    )
                }));
                let api_versions = ApiVersionsResponse {
                    api_keys: advertised
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
                return MockReply::Respond(encode_body(&api_versions, version, false));
            }
            let mut frame = BytesMut::new();
            frame.put_i16(api_key);
            frame.put_i16(version);
            frame.put_i32(correlation_id);
            frame.put_slice(body);
            let parsed = crate::network::request::parse_request(&frame, |_, version| {
                alter_partition_request::is_flexible(version)
            })
            .expect("a request header");
            let mut request_body = parsed.body;
            let request = AlterPartitionRequest::decode(&mut request_body, version)
                .expect("an AlterPartition request");
            recorded.requests.push((version, request));
            MockReply::Respond(encode_body(&answer, version, true))
        })
        .await;
        Self { mock, received }
    }

    fn addr(&self) -> SocketAddr {
        self.mock.addr
    }

    fn received(&self) -> Received {
        self.received
            .lock()
            .expect("scripted listener state")
            .clone()
    }
}

/// The answer of a broker that is not the active controller.
fn not_controller() -> AlterPartitionResponse {
    AlterPartitionResponse {
        error_code: codes::NOT_CONTROLLER,
        ..Default::default()
    }
}

/// Where the sender finds the endpoint of the active controller.
#[derive(Debug, Clone, Copy)]
enum EndpointSource {
    /// The KIP-853 voter set in the image.
    VoterSet,
    /// Only `controller.quorum.voters`. A static quorum has no voter set in
    /// the image.
    ConfiguredQuorum,
}

/// One case of the routing test: the source of the endpoint, what the active
/// controller advertises and answers, what the sender returns, and the
/// request that the active controller receives.
struct RoutingCase {
    name: &'static str,
    source: EndpointSource,
    max_version: i16,
    answer: AlterPartitionResponse,
    want: Result<(), &'static str>,
    want_request: AlterPartitionRequest,
}

fn routing_cases() -> [RoutingCase; 4] {
    [
        RoutingCase {
            name: "voter set, version 3, accepted",
            source: EndpointSource::VoterSet,
            max_version: 3,
            answer: AlterPartitionResponse::default(),
            want: Ok(()),
            want_request: expected_request(Vec::new(), isr_with_epochs()),
        },
        RoutingCase {
            name: "voter set, version 2, accepted",
            source: EndpointSource::VoterSet,
            max_version: 2,
            answer: AlterPartitionResponse::default(),
            want: Ok(()),
            want_request: expected_request(vec![1, 2], Vec::new()),
        },
        RoutingCase {
            name: "configured quorum, version 3, accepted",
            source: EndpointSource::ConfiguredQuorum,
            max_version: 3,
            answer: AlterPartitionResponse::default(),
            want: Ok(()),
            want_request: expected_request(Vec::new(), isr_with_epochs()),
        },
        RoutingCase {
            name: "voter set, version 3, not controller",
            source: EndpointSource::VoterSet,
            max_version: 3,
            answer: not_controller(),
            want: Err("controller 3001 is not the active controller"),
            want_request: expected_request(Vec::new(), isr_with_epochs()),
        },
    ]
}

/// Kafka sends `AlterPartition` only to the active controller, on its
/// CONTROLLER listener. In an isolated-controller cluster that node has no
/// broker registration. So the request goes to the controller endpoint that
/// the voter set or the configured quorum names. It never goes to a
/// registered broker, which can only answer `NOT_CONTROLLER`.
///
/// A `NOT_CONTROLLER` answer from the controller does not send the request to
/// a broker either. The sender reports it, and the next scan sends the
/// proposal again.
#[tokio::test]
async fn send_alter_partition_goes_only_to_the_controller_listener_of_the_active_controller() {
    for case in routing_cases() {
        let active_controller = ScriptedListener::start(Some(case.max_version), case.answer).await;
        let brokers =
            ScriptedListener::start(Some(alter_partition_request::MAX_VERSION), not_controller())
                .await;
        let (voter_at, quorum_voters) = match case.source {
            EndpointSource::VoterSet => (Some(active_controller.addr()), Vec::new()),
            EndpointSource::ConfiguredQuorum => (
                None,
                vec![(ACTIVE_CONTROLLER, active_controller.addr().to_string())],
            ),
        };
        let metadata = source_of(
            cluster_image(brokers.addr(), voter_at),
            Some(ACTIVE_CONTROLLER),
        );
        let dialer = plaintext_dialer(quorum_voters);

        let sent = send_alter_partition(
            &ControllerLink {
                controller: &metadata,
                broker_id: 1,
                dialer: &dialer,
            },
            &change(),
        )
        .await;

        assert!(sent == case.want.map_err(str::to_owned), "{}", case.name);
        assert!(
            active_controller.received()
                == Received {
                    connections: 1,
                    requests: vec![(case.max_version, case.want_request)],
                },
            "{}",
            case.name
        );
        assert!(brokers.received() == Received::default(), "{}", case.name);
    }
}

/// With no endpoint for the active controller, the sender sends nothing. It
/// does not fall back to the registered brokers.
#[tokio::test]
async fn send_alter_partition_rejects_bad_controller_leader() {
    let cases = [
        // No controller leader elected at all.
        (None, "no controller leader"),
        // A leader that neither the voter set nor the configured quorum
        // carries an endpoint for.
        (
            Some(ACTIVE_CONTROLLER),
            "controller leader has no known controller endpoint",
        ),
    ];
    for (leader, want) in cases {
        let brokers =
            ScriptedListener::start(Some(alter_partition_request::MAX_VERSION), not_controller())
                .await;
        let metadata = source_of(cluster_image(brokers.addr(), None), leader);
        let dialer = plaintext_dialer(Vec::new());

        let sent = send_alter_partition(
            &ControllerLink {
                controller: &metadata,
                broker_id: 1,
                dialer: &dialer,
            },
            &change(),
        )
        .await;

        assert!(sent == Err(want.to_owned()), "leader={leader:?}");
        assert!(
            brokers.received() == Received::default(),
            "leader={leader:?}"
        );
    }
}

/// The sender takes its version from the `ApiVersions` table of the
/// controller listener. A peer that does not advertise `AlterPartition` gets
/// no request, as a Kafka broker fails the send with
/// `UnsupportedVersionException`.
#[tokio::test]
async fn send_alter_partition_sends_nothing_to_a_listener_that_does_not_advertise_it() {
    let active_controller = ScriptedListener::start(None, AlterPartitionResponse::default()).await;
    let brokers =
        ScriptedListener::start(Some(alter_partition_request::MAX_VERSION), not_controller()).await;
    let addr = active_controller.addr();
    let metadata = source_of(
        cluster_image(brokers.addr(), Some(addr)),
        Some(ACTIVE_CONTROLLER),
    );
    let dialer = plaintext_dialer(Vec::new());

    let sent = send_alter_partition(
        &ControllerLink {
            controller: &metadata,
            broker_id: 1,
            dialer: &dialer,
        },
        &change(),
    )
    .await;

    assert!(
        sent == Err(format!(
            "controller 3001 ({addr}): send: incompatible version: broker supports 0..=0, \
             client wants 2..=3 for api_key 56"
        ))
    );
    assert!(
        active_controller.received()
            == Received {
                connections: 1,
                requests: Vec::new(),
            }
    );
    assert!(brokers.received() == Received::default());
}

/// The request uses the channel of the controller listener. The dialer
/// refuses an SSL controller listener before it writes a byte when no client
/// TLS is configured. A plaintext dial would connect.
#[tokio::test]
async fn send_alter_partition_dials_with_the_controller_listener_security() {
    let active_controller = ScriptedListener::start(
        Some(alter_partition_request::MAX_VERSION),
        AlterPartitionResponse::default(),
    )
    .await;
    let addr = active_controller.addr();
    let metadata = source_of(cluster_image(addr, Some(addr)), Some(ACTIVE_CONTROLLER));
    let mut dialer = plaintext_dialer(Vec::new());
    dialer.listener_protocol = krabka_security::ListenerProtocol::Ssl;

    let sent = send_alter_partition(
        &ControllerLink {
            controller: &metadata,
            broker_id: 1,
            dialer: &dialer,
        },
        &change(),
    )
    .await;

    assert!(
        sent == Err(format!(
            "controller 3001 ({addr}): connect: config: TLS listener without TlsConnector"
        ))
    );
    assert!(active_controller.received() == Received::default());
}

#[tokio::test]
async fn send_alter_partition_to_reports_transport_error_for_closed_port() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let result = send_alter_partition_to(
        &plaintext_dialer(Vec::new()),
        1,
        &addr.ip().to_string(),
        addr.port(),
        AlterPartitionRequest::default(),
    )
    .await;

    assert!(let Err(AlterPartitionSendError::Transport(_)) = result);
}

/// Boots a single-node broker that leads the metadata quorum, and returns the
/// address of its controller listener.
async fn start_controller() -> (crate::BrokerHandle, SocketAddr, tempfile::TempDir) {
    crate::test_support::start_controller().await
}

/// A krabka controller advertises `AlterPartition` on its controller listener
/// and answers it there. The sender negotiates a version and reaches the
/// handler of the active controller. The image holds only the voter record of
/// the leader, which is what a broker-only node sees of an isolated
/// controller.
///
/// The request names broker 99, which the controller has not registered. So
/// the handler passes its leader check and then answers `STALE_BROKER_EPOCH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_alter_partition_reaches_the_handler_on_a_krabka_controller_listener() {
    let (broker, controller_addr, _dir) = start_controller().await;
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&voters_record(NodeId(1), controller_addr));
    let metadata = source_of(image, Some(NodeId(1)));
    let dialer = plaintext_dialer(Vec::new());

    let sent = send_alter_partition(
        &ControllerLink {
            controller: &metadata,
            broker_id: 99,
            dialer: &dialer,
        },
        &change(),
    )
    .await;

    assert!(
        sent == Err(format!(
            "AlterPartition rejected: global={} partition=0",
            codes::STALE_BROKER_EPOCH
        ))
    );
    broker.shutdown().await;
}

#[test]
fn not_controller_classification_covers_global_and_partition_codes() {
    let cases = [
        (codes::NOT_CONTROLLER, 0, true),
        (0, codes::NOT_CONTROLLER, true),
        (0, 0, false),
        (codes::UNKNOWN_SERVER_ERROR, 0, false),
    ];
    for (global_err, part_err, want) in cases {
        assert!(
            is_not_controller_response(global_err, part_err) == want,
            "global_err={global_err} part_err={part_err}"
        );
    }
}

#[test]
fn alter_partition_response_classifies_all_error_surfaces() {
    let cases = [
        (0, 0, Ok(())),
        (
            codes::NOT_CONTROLLER,
            0,
            Err(AlterPartitionSendError::NotController),
        ),
        (
            0,
            codes::NOT_CONTROLLER,
            Err(AlterPartitionSendError::NotController),
        ),
        (
            codes::UNKNOWN_SERVER_ERROR,
            0,
            Err(AlterPartitionSendError::Rejected {
                global_err: codes::UNKNOWN_SERVER_ERROR,
                part_err: 0,
            }),
        ),
        (
            0,
            codes::UNKNOWN_SERVER_ERROR,
            Err(AlterPartitionSendError::Rejected {
                global_err: 0,
                part_err: codes::UNKNOWN_SERVER_ERROR,
            }),
        ),
        (
            codes::UNKNOWN_SERVER_ERROR,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            Err(AlterPartitionSendError::Rejected {
                global_err: codes::UNKNOWN_SERVER_ERROR,
                part_err: codes::UNKNOWN_TOPIC_OR_PARTITION,
            }),
        ),
    ];
    for (global_err, part_err, want) in cases {
        assert!(
            classify_alter_partition_response(global_err, part_err) == want,
            "global_err={global_err} part_err={part_err}"
        );
    }
}
