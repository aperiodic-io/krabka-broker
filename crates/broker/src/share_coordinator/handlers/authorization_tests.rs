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

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        delete_share_group_state_request::{self, DeleteShareGroupStateRequest, DeleteStateData},
        delete_share_group_state_response::{
            self, DeleteShareGroupStateResponse, DeleteStateResult,
        },
        initialize_share_group_state_request::{
            self, InitializeShareGroupStateRequest, InitializeStateData,
        },
        initialize_share_group_state_response::{
            self, InitializeShareGroupStateResponse, InitializeStateResult,
        },
        read_share_group_state_request::{self, ReadShareGroupStateRequest, ReadStateData},
        read_share_group_state_response::{self, ReadShareGroupStateResponse, ReadStateResult},
        read_share_group_state_summary_request::{
            self, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
        },
        read_share_group_state_summary_response::{
            self, ReadShareGroupStateSummaryResponse, ReadStateSummaryResult,
        },
        write_share_group_state_request::{self, WriteShareGroupStateRequest, WriteStateData},
        write_share_group_state_response::{self, WriteShareGroupStateResponse, WriteStateResult},
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    codes,
    test_support::{
        GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer, principal,
        request_context, start_broker_no_audit_with,
    },
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

impl Request {
    /// The api key, the version and the encoded body.
    fn encode(&self) -> (i16, i16, Bytes) {
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
                (
                    initialize_share_group_state_request::API_KEY,
                    version,
                    encode_request(&request, version),
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
                (
                    read_share_group_state_request::API_KEY,
                    version,
                    encode_request(&request, version),
                )
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
                (
                    write_share_group_state_request::API_KEY,
                    version,
                    encode_request(&request, version),
                )
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
                (
                    delete_share_group_state_request::API_KEY,
                    version,
                    encode_request(&request, version),
                )
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
                (
                    read_share_group_state_summary_request::API_KEY,
                    version,
                    encode_request(&request, version),
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

/// The partition row that Kafka's `toGlobalErrorResponse` builds.
macro_rules! refused_row {
    ($module:ident, $partition:expr) => {
        $module::PartitionResult {
            partition: $partition,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("Cluster authorization failed.".to_string()),
            ..Default::default()
        }
    };
}

/// Builds a response with one result row for each topic in [`TOPICS`], and
/// the row that `partition_row` builds for each partition in [`PARTITIONS`].
macro_rules! every_partition {
    ($response:ident, $result:ident, $partition_row:expr) => {
        $response {
            results: TOPICS
                .map(|topic_id| $result {
                    topic_id,
                    partitions: PARTITIONS.map($partition_row).to_vec(),
                    ..Default::default()
                })
                .to_vec(),
            ..Default::default()
        }
    };
}

fn refused(request: &Request) -> Response {
    match request {
        Request::Initialize => Response::Initialize(every_partition!(
            InitializeShareGroupStateResponse,
            InitializeStateResult,
            |partition| refused_row!(initialize_share_group_state_response, partition)
        )),
        Request::Read => Response::Read(every_partition!(
            ReadShareGroupStateResponse,
            ReadStateResult,
            |partition| refused_row!(read_share_group_state_response, partition)
        )),
        Request::Write => Response::Write(every_partition!(
            WriteShareGroupStateResponse,
            WriteStateResult,
            |partition| refused_row!(write_share_group_state_response, partition)
        )),
        Request::Delete => Response::Delete(every_partition!(
            DeleteShareGroupStateResponse,
            DeleteStateResult,
            |partition| refused_row!(delete_share_group_state_response, partition)
        )),
        Request::ReadSummary => Response::ReadSummary(every_partition!(
            ReadShareGroupStateSummaryResponse,
            ReadStateSummaryResult,
            |partition| refused_row!(read_share_group_state_summary_response, partition)
        )),
    }
}

fn summary(state_epoch: i32, start_offset: i64, delivery_complete_count: i32) -> Response {
    Response::ReadSummary(every_partition!(
        ReadShareGroupStateSummaryResponse,
        ReadStateSummaryResult,
        |partition| read_share_group_state_summary_response::PartitionResult {
            partition,
            error_code: codes::NONE,
            state_epoch,
            start_offset,
            delivery_complete_count,
            ..Default::default()
        }
    ))
}

fn read(start_offset: i64) -> Response {
    Response::Read(every_partition!(
        ReadShareGroupStateResponse,
        ReadStateResult,
        |partition| read_share_group_state_response::PartitionResult {
            partition,
            error_code: codes::NONE,
            state_epoch: STATE_EPOCH,
            start_offset,
            ..Default::default()
        }
    ))
}

#[tokio::test]
async fn share_state_rpcs_need_cluster_action() {
    let (handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
            GrantsInPrincipalName,
        ));
    })
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
                crate::coordinator::test_support::single_replica_partition(
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
        Response::Initialize(every_partition!(
            InitializeShareGroupStateResponse,
            InitializeStateResult,
            |partition| initialize_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            }
        ))
    };
    let written = || {
        Response::Write(every_partition!(
            WriteShareGroupStateResponse,
            WriteStateResult,
            |partition| write_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            }
        ))
    };
    let deleted = || {
        Response::Delete(every_partition!(
            DeleteShareGroupStateResponse,
            DeleteStateResult,
            |partition| delete_share_group_state_response::PartitionResult {
                partition,
                error_code: codes::NONE,
                ..Default::default()
            }
        ))
    };

    let steps: Vec<(&str, Request, Response)> = vec![
        ("none", Request::Initialize, refused(&Request::Initialize)),
        (CLIENT, Request::Initialize, refused(&Request::Initialize)),
        // Kafka's uninitialized summary: start offset and count -1.
        (BROKER, Request::ReadSummary, summary(0, -1, -1)),
        (BROKER, Request::Initialize, initialized()),
        (CLIENT, Request::Write, refused(&Request::Write)),
        (CLIENT, Request::Read, refused(&Request::Read)),
        (CLIENT, Request::ReadSummary, refused(&Request::ReadSummary)),
        (BROKER, Request::Read, read(INITIAL_START_OFFSET)),
        (CLIENT, Request::Delete, refused(&Request::Delete)),
        (
            BROKER,
            Request::ReadSummary,
            summary(STATE_EPOCH, INITIAL_START_OFFSET, 0),
        ),
        (BROKER, Request::Write, written()),
        (BROKER, Request::Read, read(WRITTEN_START_OFFSET)),
        (BROKER, Request::Delete, deleted()),
    ];

    let address = peer();
    for (step, (caller, request, expected)) in steps.into_iter().enumerate() {
        let user = principal(caller);
        let ctx = request_context(&user, &address, "share-state-authorization");
        let (api_key, version, body) = request.encode();
        let bytes = dispatch_context(&broker, api_key, version, &body, &ctx).await;
        check!(
            request.decode(version, &bytes) == expected,
            "step {step}: {caller} sends {request:?}"
        );
    }
    handle.shutdown().await;
}
