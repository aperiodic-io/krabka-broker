//! The `ClusterAction` gate on the five share-state RPCs (#715).
//!
//! Kafka's `KafkaApis.handleInitializeShareGroupStateRequest`,
//! `handleReadShareGroupStateRequest`, `handleWriteShareGroupStateRequest`,
//! `handleDeleteShareGroupStateRequest` and
//! `handleReadShareGroupStateSummaryRequest` check `ClusterAction` on the
//! cluster first. A denial answers `toGlobalErrorResponse`:
//! `CLUSTER_AUTHORIZATION_FAILED` and Kafka's message on every requested
//! partition, and the share coordinator does not run.
//!
//! The steps run in order against one broker that leads every
//! `__share_group_state` partition. A summary read after each denied
//! mutation shows that the coordinator state did not change. Every request
//! goes through the dispatch registry.

use {
    super::test_support::StateReply,
    crate::{
        codes,
        coordinator::test_support::single_replica_partition,
        test_support::{
            ControllerPeerAllowed, GrantsInPrincipalName, StartedBroker, decode_response,
            dispatch_context, encode_request, peer, principal, request_context,
            start_broker_with_authorizer_no_audit,
        },
    },
    assert2::check,
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            delete_share_group_state_request::{
                self, DeleteShareGroupStateRequest, DeleteStateData,
            },
            delete_share_group_state_response::{self, DeleteShareGroupStateResponse},
            initialize_share_group_state_request::{
                self, InitializeShareGroupStateRequest, InitializeStateData,
            },
            initialize_share_group_state_response::{self, InitializeShareGroupStateResponse},
            read_share_group_state_request::{self, ReadShareGroupStateRequest, ReadStateData},
            read_share_group_state_response::{self, ReadShareGroupStateResponse},
            read_share_group_state_summary_request::{
                self, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
            },
            read_share_group_state_summary_response::{self, ReadShareGroupStateSummaryResponse},
            write_share_group_state_request::{self, WriteShareGroupStateRequest, WriteStateData},
            write_share_group_state_response::{self, WriteShareGroupStateResponse},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

const GROUP: &str = "authorized-share-group";

/// The two topics that every request names, each with two partitions.
const TOPICS: [WireUuid; 2] = [WireUuid([51; 16]), WireUuid([52; 16])];
const PARTITIONS: [i32; 2] = [0, 1];

/// The principal that holds `ClusterAction` on the cluster, as a broker does.
const BROKER: &str = "Cluster:ClusterAction";
/// A principal that holds every group and topic grant, and nothing on the
/// cluster.
const CLIENT: &str = "Group:Read+Group:Describe+Topic:Read+Topic:Describe";

const STATE_EPOCH: i32 = 4;
const INITIAL_START_OFFSET: i64 = 10;
const WRITTEN_START_OFFSET: i64 = 20;

/// One request of a step.
#[derive(Debug)]
enum Request {
    Initialize,
    Read,
    Write,
    Delete,
    ReadSummary,
}

/// One decoded response of a step.
#[derive(Debug, PartialEq)]
enum Response {
    Initialize(InitializeShareGroupStateResponse),
    Read(ReadShareGroupStateResponse),
    Write(WriteShareGroupStateResponse),
    Delete(DeleteShareGroupStateResponse),
    ReadSummary(ReadShareGroupStateSummaryResponse),
}

/// A request as the broker receives it.
struct EncodedRequest {
    api_key: i16,
    version: i16,
    body: Bytes,
}

macro_rules! encoded_request {
    ($api_key:expr, $version:ident, $request:ident) => {
        EncodedRequest {
            api_key: $api_key,
            version: $version,
            body: encode_request(&$request, $version),
        }
    };
}

impl Request {
    /// The api key, the version and the encoded body.
    fn encode(&self) -> EncodedRequest {
        match self {
            Self::Initialize => {
                let version = initialize_share_group_state_request::MAX_VERSION;
                let request = InitializeShareGroupStateRequest {
                    group_id: GROUP.to_string(),
                    topics: TOPICS
                        .map(|topic_id| InitializeStateData {
                            topic_id,
                            partitions: PARTITIONS
                                .map(|partition| {
                                    initialize_share_group_state_request::PartitionData {
                                        partition,
                                        state_epoch: STATE_EPOCH,
                                        start_offset: INITIAL_START_OFFSET,
                                        ..Default::default()
                                    }
                                })
                                .to_vec(),
                            ..Default::default()
                        })
                        .to_vec(),
                    ..Default::default()
                };
                encoded_request!(
                    initialize_share_group_state_request::API_KEY,
                    version,
                    request
                )
            }
            Self::Read => {
                let version = read_share_group_state_request::MAX_VERSION;
                let request = ReadShareGroupStateRequest {
                    group_id: GROUP.to_string(),
                    topics: TOPICS
                        .map(|topic_id| ReadStateData {
                            topic_id,
                            partitions: PARTITIONS
                                .map(|partition| read_share_group_state_request::PartitionData {
                                    partition,
                                    ..Default::default()
                                })
                                .to_vec(),
                            ..Default::default()
                        })
                        .to_vec(),
                    ..Default::default()
                };
                encoded_request!(read_share_group_state_request::API_KEY, version, request)
            }
            Self::Write => {
                let version = write_share_group_state_request::MAX_VERSION;
                let request = WriteShareGroupStateRequest {
                    group_id: GROUP.to_string(),
                    topics: TOPICS
                        .map(|topic_id| WriteStateData {
                            topic_id,
                            partitions: PARTITIONS
                                .map(|partition| write_share_group_state_request::PartitionData {
                                    partition,
                                    state_epoch: STATE_EPOCH,
                                    leader_epoch: 0,
                                    start_offset: WRITTEN_START_OFFSET,
                                    ..Default::default()
                                })
                                .to_vec(),
                            ..Default::default()
                        })
                        .to_vec(),
                    ..Default::default()
                };
                encoded_request!(write_share_group_state_request::API_KEY, version, request)
            }
            Self::Delete => {
                let version = delete_share_group_state_request::MAX_VERSION;
                let request = DeleteShareGroupStateRequest {
                    group_id: GROUP.to_string(),
                    topics: TOPICS
                        .map(|topic_id| DeleteStateData {
                            topic_id,
                            partitions: PARTITIONS
                                .map(
                                    |partition| delete_share_group_state_request::PartitionData {
                                        partition,
                                        ..Default::default()
                                    },
                                )
                                .to_vec(),
                            ..Default::default()
                        })
                        .to_vec(),
                    ..Default::default()
                };
                encoded_request!(delete_share_group_state_request::API_KEY, version, request)
            }
            Self::ReadSummary => {
                let version = read_share_group_state_summary_request::MAX_VERSION;
                let request = ReadShareGroupStateSummaryRequest {
                    group_id: GROUP.to_string(),
                    topics: TOPICS
                        .map(|topic_id| ReadStateSummaryData {
                            topic_id,
                            partitions: PARTITIONS
                                .map(|partition| {
                                    read_share_group_state_summary_request::PartitionData {
                                        partition,
                                        ..Default::default()
                                    }
                                })
                                .to_vec(),
                            ..Default::default()
                        })
                        .to_vec(),
                    ..Default::default()
                };
                encoded_request!(
                    read_share_group_state_summary_request::API_KEY,
                    version,
                    request
                )
            }
        }
    }

    fn decode(&self, version: i16, bytes: &Bytes) -> Response {
        match self {
            Self::Initialize => Response::Initialize(decode_response(bytes, version)),
            Self::Read => Response::Read(decode_response(bytes, version)),
            Self::Write => Response::Write(decode_response(bytes, version)),
            Self::Delete => Response::Delete(decode_response(bytes, version)),
            Self::ReadSummary => Response::ReadSummary(decode_response(bytes, version)),
        }
    }
}

/// Builds a row for every requested topic and partition, in request order.
fn every_partition<R: StateReply>(mut row: impl FnMut(i32) -> R::Partition) -> R {
    R::from_results(
        TOPICS
            .map(|topic| R::topic(topic, PARTITIONS.map(&mut row).into_iter().collect()))
            .into_iter()
            .collect(),
    )
}

fn refused_response<R: StateReply>() -> R {
    every_partition::<R>(|partition| {
        R::error_row(
            partition,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("Cluster authorization failed."),
        )
    })
}
fn refused(request: &Request) -> Response {
    match request {
        Request::Initialize => Response::Initialize(refused_response()),
        Request::Read => Response::Read(refused_response()),
        Request::Write => Response::Write(refused_response()),
        Request::Delete => Response::Delete(refused_response()),
        Request::ReadSummary => Response::ReadSummary(refused_response()),
    }
}

fn summary(state_epoch: i32, start_offset: i64, delivery_complete_count: i32) -> Response {
    Response::ReadSummary(every_partition::<ReadShareGroupStateSummaryResponse>(
        |partition| read_share_group_state_summary_response::PartitionResult {
            partition,
            error_code: codes::NONE,
            state_epoch,
            start_offset,
            delivery_complete_count,
            ..Default::default()
        },
    ))
}

fn read(start_offset: i64) -> Response {
    Response::Read(every_partition::<ReadShareGroupStateResponse>(
        |partition| read_share_group_state_response::PartitionResult {
            partition,
            error_code: codes::NONE,
            state_epoch: STATE_EPOCH,
            start_offset,
            ..Default::default()
        },
    ))
}

#[tokio::test]
async fn share_state_rpcs_need_cluster_action() {
    struct Step {
        caller: &'static str,
        request: Request,
        expected: Response,
    }

    let StartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_with_authorizer_no_audit(Arc::new(ControllerPeerAllowed(
        GrantsInPrincipalName,
    )))
    .await;
    let broker = handle.broker_arc_for_test();
    // The data topics that the requests name. The share coordinator refuses a
    // read or a write of a topic partition that the metadata image does not
    // hold.
    let mut data_topics = Vec::new();
    for (index, topic_id) in TOPICS.iter().enumerate() {
        let name = format!("share-authorized-{index}");
        data_topics.push(krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: name.clone(),
                topic_id: uuid::Uuid::from_bytes(topic_id.0),
                partitions: 2,
                replication_factor: 1,
            },
        ));
        for partition in PARTITIONS {
            data_topics.push(krabka_metadata::MetadataRecord::V1Partition(
                single_replica_partition(
                    &name,
                    krabka_ids::PartitionIndex(partition),
                    broker.config.node_id,
                ),
            ));
        }
    }
    broker
        .controller
        .submit_change(data_topics)
        .await
        .expect("create the data topics");
    // A real `__share_group_state` topic that this broker leads, loaded to
    // completion. The broker's metadata reconcile loop also refreshes the
    // share coordinator's leadership on every image change and does not wait
    // for the load it starts. A single explicit `refresh_leader_partitions`
    // call here can lose that race: the reconcile loop's own call can already
    // have claimed the leadership change and started the background load, so
    // this call's `finished()` sees no load of its own to wait for and
    // returns before the load ends. Poll for `LoadStatus::Active`, the way
    // every other live-broker share-coordinator test waits.
    super::test_support::lead_share_state_partitions(&broker).await;

    let initialized = || {
        Response::Initialize(every_partition::<InitializeShareGroupStateResponse>(
            |partition| initialize_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            },
        ))
    };
    let written = || {
        Response::Write(every_partition::<WriteShareGroupStateResponse>(
            |partition| write_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            },
        ))
    };
    let deleted = || {
        Response::Delete(every_partition::<DeleteShareGroupStateResponse>(
            |partition| delete_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            },
        ))
    };

    let steps: Vec<Step> = vec![
        Step {
            caller: "none",
            request: Request::Initialize,
            expected: refused(&Request::Initialize),
        },
        Step {
            caller: CLIENT,
            request: Request::Initialize,
            expected: refused(&Request::Initialize),
        },
        // Kafka's uninitialized summary: start offset and count -1.
        Step {
            caller: BROKER,
            request: Request::ReadSummary,
            expected: summary(0, -1, -1),
        },
        Step {
            caller: BROKER,
            request: Request::Initialize,
            expected: initialized(),
        },
        Step {
            caller: CLIENT,
            request: Request::Write,
            expected: refused(&Request::Write),
        },
        Step {
            caller: CLIENT,
            request: Request::Read,
            expected: refused(&Request::Read),
        },
        Step {
            caller: CLIENT,
            request: Request::ReadSummary,
            expected: refused(&Request::ReadSummary),
        },
        Step {
            caller: BROKER,
            request: Request::Read,
            expected: read(INITIAL_START_OFFSET),
        },
        Step {
            caller: CLIENT,
            request: Request::Delete,
            expected: refused(&Request::Delete),
        },
        Step {
            caller: BROKER,
            request: Request::ReadSummary,
            expected: summary(STATE_EPOCH, INITIAL_START_OFFSET, 0),
        },
        Step {
            caller: BROKER,
            request: Request::Write,
            expected: written(),
        },
        Step {
            caller: BROKER,
            request: Request::Read,
            expected: read(WRITTEN_START_OFFSET),
        },
        Step {
            caller: BROKER,
            request: Request::Delete,
            expected: deleted(),
        },
    ];

    let address = peer();
    for (
        step,
        Step {
            caller,
            request,
            expected,
        },
    ) in steps.into_iter().enumerate()
    {
        let user = principal(caller);
        let ctx = request_context(&user, &address, "share-state-authorization");
        let EncodedRequest {
            api_key,
            version,
            body,
        } = request.encode();
        let bytes = dispatch_context(&broker, api_key, version, &body, &ctx).await;
        check!(
            request.decode(version, &bytes) == expected,
            "step {step}: {caller} sends {request:?}"
        );
    }
    handle.shutdown().await;
}
