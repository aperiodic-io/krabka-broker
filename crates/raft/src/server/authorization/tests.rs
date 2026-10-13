use {
    super::{refusal, required_operation},
    crate::{
        ClusterGrants, ClusterOperation, ListenerLimits,
        kraft::{KraftController, transport::api_key},
        server::{
            ConnectionContext, Unstable, handle_conn,
            test_support::{encoded, single_voter_engine, wait_for_leader},
        },
        wire::{
            API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
            KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest, KrabkaSubmitChangeResponse,
        },
    },
    assert2::check,
    bytes::{BufMut as _, Bytes, BytesMut},
    krabka_metadata::MetadataRecord,
    krabka_protocol::{
        Decode as _, Encode as _,
        owned::{
            add_raft_voter_response::AddRaftVoterResponse,
            assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
            begin_quorum_epoch_response::BeginQuorumEpochResponse,
            controller_registration_response::ControllerRegistrationResponse,
            describe_cluster_request::DescribeClusterRequest,
            describe_cluster_response::DescribeClusterResponse,
            describe_quorum_request::{DescribeQuorumRequest, PartitionData, TopicData},
            describe_quorum_response::DescribeQuorumResponse,
            end_quorum_epoch_response::EndQuorumEpochResponse,
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::{self, FetchResponse, FetchableTopicResponse},
            fetch_snapshot_response::FetchSnapshotResponse,
            remove_raft_voter_response::RemoveRaftVoterResponse,
            update_raft_voter_response::UpdateRaftVoterResponse,
            vote_response::VoteResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{collections::BTreeSet, sync::Arc},
    tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
    tokio_util::sync::CancellationToken,
};

const MESSAGE: &str = "Cluster authorization failed.";

/// Kafka's `ControllerApis` operation for each api that the controller
/// listener answers itself, and no operation for the apis that a broker
/// handler authorizes.
#[test]
fn each_controller_api_needs_the_kafka_operation() {
    use ClusterOperation::{Alter, ClusterAction, Describe};

    let cases = [
        ("Fetch", api_key::FETCH, Some(ClusterAction)),
        ("Vote", api_key::VOTE, Some(ClusterAction)),
        (
            "BeginQuorumEpoch",
            api_key::BEGIN_QUORUM_EPOCH,
            Some(ClusterAction),
        ),
        (
            "EndQuorumEpoch",
            api_key::END_QUORUM_EPOCH,
            Some(ClusterAction),
        ),
        (
            "FetchSnapshot",
            api_key::FETCH_SNAPSHOT,
            Some(ClusterAction),
        ),
        ("ControllerRegistration", 70, Some(ClusterAction)),
        ("UpdateRaftVoter", 82, Some(ClusterAction)),
        ("AssignReplicasToDirs", 73, Some(ClusterAction)),
        ("SubmitChange", API_KEY_SUBMIT_CHANGE, Some(ClusterAction)),
        ("MetadataFetch", API_KEY_METADATA_FETCH, Some(ClusterAction)),
        (
            "DelegationTokenMutation",
            API_KEY_DELEGATION_TOKEN_MUTATION,
            Some(ClusterAction),
        ),
        ("AddRaftVoter", 80, Some(Alter)),
        ("RemoveRaftVoter", 81, Some(Alter)),
        ("DescribeCluster", 60, Some(Alter)),
        ("DescribeQuorum", 55, Some(Describe)),
        ("ApiVersions", 18, None),
        ("CreateTopics", 19, None),
        ("Envelope", 58, None),
        ("BrokerRegistration", 62, None),
        ("BrokerHeartbeat", 63, None),
    ];
    for (name, key, operation) in cases {
        check!(required_operation(key) == operation, "{name}");
    }
}

/// The refusal of a `SubmitChange` without `ClusterAction`.
fn submit_change_refusal() -> Refusal {
    Refusal::SubmitChange(KrabkaSubmitChangeResponse {
        error_code: 31,
        leader_hint: -1,
        result: Bytes::new(),
    })
}

/// One decoded refusal.
#[derive(Debug, PartialEq)]
enum Refusal {
    Fetch(FetchResponse),
    Vote(VoteResponse),
    BeginQuorumEpoch(BeginQuorumEpochResponse),
    AssignReplicasToDirs(AssignReplicasToDirsResponse),
    EndQuorumEpoch(EndQuorumEpochResponse),
    FetchSnapshot(FetchSnapshotResponse),
    ControllerRegistration(ControllerRegistrationResponse),
    UpdateRaftVoter(UpdateRaftVoterResponse),
    AddRaftVoter(AddRaftVoterResponse),
    RemoveRaftVoter(RemoveRaftVoterResponse),
    DescribeCluster(DescribeClusterResponse),
    DescribeQuorum(DescribeQuorumResponse),
    SubmitChange(KrabkaSubmitChangeResponse),
    MetadataFetch(KrabkaMetadataFetchResponse),
}

/// A request that the listener refuses, and the refusal it expects.
struct RefusalCase {
    name: &'static str,
    api: i16,
    version: i16,
    body: Bytes,
    expected: Refusal,
}

fn check_refusal_cases(cases: impl IntoIterator<Item = RefusalCase>) {
    for RefusalCase {
        name,
        api,
        version,
        body,
        expected,
    } in cases
    {
        let bytes = refusal(api, version, &body).expect("encode the refusal");
        check!(decode(api, version, &bytes) == expected, "{name}");
    }
}

fn decode(api: i16, version: i16, bytes: &[u8]) -> Refusal {
    let mut cursor = bytes;
    let refusal = match api {
        api_key::FETCH => Refusal::Fetch(FetchResponse::decode(&mut cursor, version).unwrap()),
        api_key::VOTE => Refusal::Vote(VoteResponse::decode(&mut cursor, version).unwrap()),
        api_key::BEGIN_QUORUM_EPOCH => Refusal::BeginQuorumEpoch(
            BeginQuorumEpochResponse::decode(&mut cursor, version).unwrap(),
        ),
        api_key::END_QUORUM_EPOCH => {
            Refusal::EndQuorumEpoch(EndQuorumEpochResponse::decode(&mut cursor, version).unwrap())
        }
        api_key::FETCH_SNAPSHOT => {
            Refusal::FetchSnapshot(FetchSnapshotResponse::decode(&mut cursor, version).unwrap())
        }
        73 => Refusal::AssignReplicasToDirs(
            AssignReplicasToDirsResponse::decode(&mut cursor, version).unwrap(),
        ),
        70 => Refusal::ControllerRegistration(
            ControllerRegistrationResponse::decode(&mut cursor, version).unwrap(),
        ),
        82 => {
            Refusal::UpdateRaftVoter(UpdateRaftVoterResponse::decode(&mut cursor, version).unwrap())
        }
        80 => Refusal::AddRaftVoter(AddRaftVoterResponse::decode(&mut cursor, version).unwrap()),
        81 => {
            Refusal::RemoveRaftVoter(RemoveRaftVoterResponse::decode(&mut cursor, version).unwrap())
        }
        60 => {
            Refusal::DescribeCluster(DescribeClusterResponse::decode(&mut cursor, version).unwrap())
        }
        55 => {
            Refusal::DescribeQuorum(DescribeQuorumResponse::decode(&mut cursor, version).unwrap())
        }
        API_KEY_SUBMIT_CHANGE | API_KEY_DELEGATION_TOKEN_MUTATION => {
            Refusal::SubmitChange(KrabkaSubmitChangeResponse::decode_v0(&mut cursor).unwrap())
        }
        API_KEY_METADATA_FETCH => {
            Refusal::MetadataFetch(KrabkaMetadataFetchResponse::decode_v0(&mut cursor).unwrap())
        }
        other => panic!("no refusal decoder for api {other}"),
    };
    check!(cursor.is_empty(), "api {api} v{version} decodes every byte");
    refusal
}

fn fetch_request(version: i16) -> Bytes {
    let request = FetchRequest {
        session_id: 7,
        topics: vec![FetchTopic {
            topic: "__cluster_metadata".to_string(),
            topic_id: WireUuid([1; 16]),
            partitions: vec![
                FetchPartition {
                    partition: 0,
                    ..Default::default()
                },
                FetchPartition {
                    partition: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request.encode(&mut body, version).unwrap();
    body.freeze()
}

/// Each refusal is the `getErrorResponse` of its Kafka api with
/// `CLUSTER_AUTHORIZATION_FAILED`, and the krabka-private apis refuse in
/// their own response.
#[test]
fn fetch_and_quorum_refusals_are_kafka_error_responses() {
    let fetch_row = |partition_index| fetch_response::PartitionData {
        partition_index,
        error_code: 31,
        high_watermark: -1,
        ..Default::default()
    };
    let cases = [
        RefusalCase {
            name: "Fetch before v13 names every partition",
            api: api_key::FETCH,
            version: 12,
            body: fetch_request(12),
            expected: Refusal::Fetch(FetchResponse {
                error_code: 31,
                session_id: 7,
                responses: vec![FetchableTopicResponse {
                    topic: "__cluster_metadata".to_string(),
                    // Fetch carries topic ids from v13 on.
                    topic_id: WireUuid([0; 16]),
                    partitions: vec![fetch_row(0), fetch_row(1)],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "Fetch at exactly v13 has only the top-level error",
            api: api_key::FETCH,
            version: 13,
            body: fetch_request(13),
            expected: Refusal::Fetch(FetchResponse {
                error_code: 31,
                session_id: 7,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "Fetch from v13 has only the top-level error",
            api: api_key::FETCH,
            version: 17,
            body: fetch_request(17),
            expected: Refusal::Fetch(FetchResponse {
                error_code: 31,
                session_id: 7,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "Vote",
            api: api_key::VOTE,
            version: 2,
            body: Bytes::new(),
            expected: Refusal::Vote(VoteResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "BeginQuorumEpoch",
            api: api_key::BEGIN_QUORUM_EPOCH,
            version: 1,
            body: Bytes::new(),
            expected: Refusal::BeginQuorumEpoch(BeginQuorumEpochResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "EndQuorumEpoch",
            api: api_key::END_QUORUM_EPOCH,
            version: 1,
            body: Bytes::new(),
            expected: Refusal::EndQuorumEpoch(EndQuorumEpochResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "FetchSnapshot",
            api: api_key::FETCH_SNAPSHOT,
            version: 1,
            body: Bytes::new(),
            expected: Refusal::FetchSnapshot(FetchSnapshotResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
    ];
    check_refusal_cases(cases);
}

#[test]
fn admin_and_private_refusals_are_kafka_error_responses() {
    let private_submit = KrabkaSubmitChangeResponse {
        error_code: 31,
        leader_hint: -1,
        result: Bytes::new(),
    };
    let cases = [
        RefusalCase {
            name: "AssignReplicasToDirs",
            api: 73,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::AssignReplicasToDirs(AssignReplicasToDirsResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "ControllerRegistration",
            api: 70,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::ControllerRegistration(ControllerRegistrationResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "UpdateRaftVoter",
            api: 82,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::UpdateRaftVoter(UpdateRaftVoterResponse {
                error_code: 31,
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "AddRaftVoter",
            api: 80,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::AddRaftVoter(AddRaftVoterResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "RemoveRaftVoter",
            api: 81,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::RemoveRaftVoter(RemoveRaftVoterResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "DescribeCluster",
            api: 60,
            version: 1,
            body: Bytes::new(),
            expected: Refusal::DescribeCluster(DescribeClusterResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "DescribeQuorum",
            api: 55,
            version: 2,
            body: Bytes::new(),
            expected: Refusal::DescribeQuorum(DescribeQuorumResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            }),
        },
        RefusalCase {
            name: "SubmitChange",
            api: API_KEY_SUBMIT_CHANGE,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::SubmitChange(private_submit.clone()),
        },
        RefusalCase {
            name: "DelegationTokenMutation",
            api: API_KEY_DELEGATION_TOKEN_MUTATION,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::SubmitChange(private_submit),
        },
        RefusalCase {
            name: "MetadataFetch",
            api: API_KEY_METADATA_FETCH,
            version: 0,
            body: Bytes::new(),
            expected: Refusal::MetadataFetch(KrabkaMetadataFetchResponse {
                error_code: 31,
                leader_hint: -1,
                leader_epoch: -1,
                log_start_offset: -1,
                high_watermark: -1,
                quorum_high_watermark: -1,
                snapshot_id: None,
                records: Bytes::new(),
            }),
        },
    ];
    check_refusal_cases(cases);
}

/// Grants that allow exactly the listed operations.
struct Grants(BTreeSet<&'static str>);

impl Grants {
    fn of(operations: &[&'static str]) -> Arc<Self> {
        Arc::new(Self(operations.iter().copied().collect()))
    }
}

impl ClusterGrants for Grants {
    fn allows(&self, operation: ClusterOperation) -> bool {
        let name = match operation {
            ClusterOperation::ClusterAction => "ClusterAction",
            ClusterOperation::Alter => "Alter",
            ClusterOperation::Describe => "Describe",
        };
        self.0.contains(name)
    }

    fn cluster_authorized_operations(&self) -> i32 {
        [("Alter", 7), ("Describe", 8), ("ClusterAction", 9)]
            .into_iter()
            .filter(|(name, _)| self.0.contains(name))
            .fold(0, |bits, (_, code)| bits | 1 << code)
    }
}

/// Sends one flexible-header request on `client` and returns the response
/// body after the flexible response header.
async fn exchange(
    client: &mut tokio::io::DuplexStream,
    api: i16,
    version: i16,
    body: &[u8],
) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i16(api);
    frame.put_i16(version);
    frame.put_i32(9);
    frame.put_i16(-1);
    frame.put_u8(0);
    frame.put_slice(body);
    client
        .write_all(&u32::try_from(frame.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    client.write_all(&frame).await.unwrap();

    let mut len = [0_u8; 4];
    client.read_exact(&mut len).await.unwrap();
    let mut response = vec![0_u8; usize::try_from(u32::from_be_bytes(len)).unwrap()];
    client.read_exact(&mut response).await.unwrap();
    check!(response[..5] == [0, 0, 0, 9, 0], "flexible response header");
    Bytes::copy_from_slice(&response[5..])
}

fn submit_change(topic: &str) -> Bytes {
    let records = vec![MetadataRecord::V1Topic(
        crate::test_support::single_partition_topic(topic, uuid::Uuid::new_v4()),
    )];
    let records =
        <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
            &records,
        )
        .expect("wincode");
    let mut out = Vec::new();
    KrabkaSubmitChangeRequest {
        records: Bytes::from(records),
    }
    .encode_v0(&mut out)
    .expect("submit request");
    Bytes::from(out)
}

/// One request of [`a_denied_request_never_reaches_the_engine`].
struct Case {
    name: &'static str,
    /// The grants of the connection.
    grants: &'static [&'static str],
    api: i16,
    version: i16,
    body: Bytes,
    /// The refusal it expects, or `None` when the request runs.
    refused: Option<Refusal>,
    /// A topic that the request commits.
    committed_topic: Option<&'static str>,
}

/// The listener checks each request against the grants of its connection and
/// answers a denial without running the request (#684).
///
/// A `SubmitChange` without `ClusterAction` commits no record. The same
/// request with `ClusterAction` commits it. `DescribeQuorum` needs `Describe`
/// and `DescribeCluster` needs `Alter`, so `ClusterAction` or `Describe` alone
/// does not open them.
#[tokio::test]
async fn a_denied_request_never_reaches_the_engine() {
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let shutdown = CancellationToken::new();

    let describe_quorum = encoded(
        &DescribeQuorumRequest {
            topics: vec![TopicData {
                topic_name: "__cluster_metadata".to_string(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        2,
    );
    let describe_cluster = encoded(
        &DescribeClusterRequest {
            endpoint_type: 2,
            ..Default::default()
        },
        1,
    );

    let cases = [
        Case {
            name: "SubmitChange without a grant",
            grants: &[],
            api: API_KEY_SUBMIT_CHANGE,
            version: 0,
            body: submit_change("denied-topic"),
            refused: Some(submit_change_refusal()),
            committed_topic: None,
        },
        Case {
            name: "SubmitChange with Alter and Describe only",
            grants: &["Alter", "Describe"],
            api: API_KEY_SUBMIT_CHANGE,
            version: 0,
            body: submit_change("denied-topic"),
            refused: Some(submit_change_refusal()),
            committed_topic: None,
        },
        Case {
            name: "SubmitChange with ClusterAction",
            grants: &["ClusterAction"],
            api: API_KEY_SUBMIT_CHANGE,
            version: 0,
            body: submit_change("allowed-topic"),
            refused: None,
            committed_topic: Some("allowed-topic"),
        },
        Case {
            name: "DescribeQuorum with ClusterAction only",
            grants: &["ClusterAction"],
            api: 55,
            version: 2,
            body: describe_quorum.clone(),
            refused: Some(Refusal::DescribeQuorum(DescribeQuorumResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            })),
            committed_topic: None,
        },
        Case {
            name: "DescribeCluster with Describe only",
            grants: &["Describe"],
            api: 60,
            version: 1,
            body: describe_cluster,
            refused: Some(Refusal::DescribeCluster(DescribeClusterResponse {
                error_code: 31,
                error_message: Some(MESSAGE.to_string()),
                ..Default::default()
            })),
            committed_topic: None,
        },
        Case {
            name: "DescribeQuorum with Describe",
            grants: &["Describe"],
            api: 55,
            version: 2,
            body: describe_quorum,
            refused: None,
            committed_topic: None,
        },
    ];

    for Case {
        name,
        grants,
        api,
        version,
        body,
        refused,
        committed_topic,
    } in cases
    {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let connection = tokio::spawn(handle_conn(
            server,
            engine.clone(),
            shutdown.clone(),
            None,
            None,
            ConnectionContext {
                peer: "127.0.0.1:9093".parse().unwrap(),
                principal: None,
                authenticated_via_token: false,
                expires_at_ms: None,
                grants: Grants::of(grants),
                unstable: Unstable::default(),
                limits: ListenerLimits::default(),
            },
            KraftController::wall_clock_ms,
        ));

        let response = exchange(&mut client, api, version, &body).await;
        let decoded = decode(api, version, &response);
        if let Some(expected) = refused {
            check!(decoded == expected, "{name}");
        } else {
            check!(
                !matches!(
                    &decoded,
                    Refusal::SubmitChange(KrabkaSubmitChangeResponse { error_code: 31, .. })
                        | Refusal::DescribeQuorum(DescribeQuorumResponse { error_code: 31, .. })
                ),
                "{name}: {decoded:?}"
            );
        }
        let image = engine.current_image();
        check!(image.topic("denied-topic").is_none(), "{name}");
        if let Some(topic) = committed_topic {
            check!(image.topic(topic).is_some(), "{name}");
        }

        drop(client);
        connection
            .await
            .expect("the connection task joins")
            .expect("the connection ends cleanly at EOF");
    }
}
