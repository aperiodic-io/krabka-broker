//! The authorization and partition checks of `AddPartitionsToTxn` (#686,
//! #849).
//!
//! Kafka's `KafkaApis.handleAddPartitionsToTxnRequest`:
//! - v4 and later come from brokers. The request needs `ClusterAction` on the
//!   cluster, else a top-level `CLUSTER_AUTHORIZATION_FAILED`. No
//!   transactional id or topic ACL is checked.
//! - v0 to v3 come from clients: `Write` on the transactional id, then
//!   `Write` on each non-internal topic. An internal topic is never
//!   authorized.
//! - A partition that is not authorized or does not exist fails the whole
//!   transaction: nothing is added, and the other partitions answer
//!   `OPERATION_NOT_ATTEMPTED`.
//!
//! Every case runs against one broker that coordinates every transactional
//! id and has one open transaction for each case.

use {
    super::{
        handle,
        test_support::{
            CoordinatorBroker, PartitionCode, PartitionSpec, enlisted, partition_set,
            seed_transaction, start_coordinator, topic, topic_result,
        },
    },
    crate::{
        authorizer::AuthorizationResult,
        codes,
        coordinator::bootstrap::OFFSETS_TOPIC,
        test_support::{ControllerPeerAllowed, peer, principal, request_context},
    },
    assert2::check,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
    },
    std::sync::Arc,
};

/// A broker principal: `ClusterAction` on the cluster and nothing else.
const BROKER: &str = "broker";
/// A client principal: `Write` on every transactional id, and `Write` on the
/// topics in [`CLIENT_TOPICS`] only.
const CLIENT: &str = "client";
/// The topics [`CLIENT`] may write, an internal topic included.
const CLIENT_TOPICS: [&str; 3] = ["a", "missing", OFFSETS_TOPIC];
/// Partition 0 of topic `a` and of topic `b`.
const A_AND_B: &[PartitionSpec] = &[
    PartitionSpec {
        topic: "a",
        partition: 0,
    },
    PartitionSpec {
        topic: "b",
        partition: 0,
    },
];

#[derive(Debug)]
struct Grants;

krabka_macros::test_authorizer! { Grants, request; {
    let allowed = match (
        request.principal.name.as_str(),
        request.resource_type,
        request.operation,
    ) {
        (BROKER, ResourceType::Cluster, AclOperation::ClusterAction)
        | (CLIENT, ResourceType::TransactionalId, AclOperation::Write) => true,
        (CLIENT, ResourceType::Topic, AclOperation::Write) => {
            CLIENT_TOPICS.contains(&request.resource_name)
        }
        _ => false,
    };
    if allowed {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
}}

/// What one case expects back.
#[derive(Debug)]
enum Expect {
    /// The top-level error code, and no rows.
    TopLevel(i16),
    /// These rows.
    Rows(&'static [RowSpec]),
}

/// One expected response row: the topic and partition it answers, and its code.
#[derive(Debug, Clone, Copy)]
struct RowSpec {
    topic: &'static str,
    partition: i32,
    code: i16,
}

struct Case {
    name: &'static str,
    version: i16,
    caller: &'static str,
    partitions: &'static [PartitionSpec],
    verify_only: bool,
    expect: Expect,
    /// The partitions in the transaction after the request.
    enlisted: &'static [PartitionSpec],
}

/// The partitions of one topic that a request names.
struct TopicGroup<'a> {
    name: &'a str,
    partitions: Vec<i32>,
}

/// The rows a response answers for one topic.
struct RowGroup<'a> {
    name: &'a str,
    rows: Vec<PartitionCode>,
}

fn request(case: &Case, tid: &str, producer_id: i64) -> AddPartitionsToTxnRequest {
    let mut topics: Vec<TopicGroup<'_>> = Vec::new();
    for &PartitionSpec {
        topic: name,
        partition,
    } in case.partitions
    {
        match topics.iter_mut().find(|group| group.name == name) {
            Some(group) => group.partitions.push(partition),
            None => topics.push(TopicGroup {
                name,
                partitions: vec![partition],
            }),
        }
    }
    let topics = topics
        .iter()
        .map(|TopicGroup { name, partitions }| topic(name, partitions))
        .collect();
    if case.version >= 4 {
        AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: tid.to_owned(),
                producer_id,
                producer_epoch: 2,
                verify_only: case.verify_only,
                topics,
                ..Default::default()
            }],
            ..Default::default()
        }
    } else {
        AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: tid.to_owned(),
            v3_and_below_producer_id: producer_id,
            v3_and_below_producer_epoch: 2,
            v3_and_below_topics: topics,
            ..Default::default()
        }
    }
}

fn expected(case: &Case, tid: &str) -> AddPartitionsToTxnResponse {
    let rows = match case.expect {
        Expect::TopLevel(error_code) => {
            return AddPartitionsToTxnResponse {
                error_code,
                ..Default::default()
            };
        }
        Expect::Rows(rows) => rows,
    };
    let mut topics: Vec<RowGroup<'_>> = Vec::new();
    for &RowSpec {
        topic: name,
        partition,
        code,
    } in rows
    {
        let row = PartitionCode { partition, code };
        match topics.iter_mut().find(|group| group.name == name) {
            Some(group) => group.rows.push(row),
            None => topics.push(RowGroup {
                name,
                rows: vec![row],
            }),
        }
    }
    let topic_results = topics
        .iter()
        .map(|RowGroup { name, rows }| topic_result(name, rows))
        .collect();
    if case.version >= 4 {
        AddPartitionsToTxnResponse {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: tid.to_owned(),
                topic_results,
                ..Default::default()
            }],
            ..Default::default()
        }
    } else {
        AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: topic_results,
            ..Default::default()
        }
    }
}

/// The table of cases: which principal sends which version, what it expects
/// back, and which partitions the transaction holds afterwards.
fn cases() -> [Case; 9] {
    use codes::{
        CLUSTER_AUTHORIZATION_FAILED as CLUSTER_DENIED, NONE,
        OPERATION_NOT_ATTEMPTED as NOT_ATTEMPTED, TOPIC_AUTHORIZATION_FAILED as TOPIC_DENIED,
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED as TID_DENIED, UNKNOWN_TOPIC_OR_PARTITION as UNKNOWN,
    };

    let missing_partition = |name, version, caller| Case {
        name,
        version,
        caller,
        partitions: &[
            PartitionSpec {
                topic: "a",
                partition: 0,
            },
            PartitionSpec {
                topic: "missing",
                partition: 0,
            },
        ],
        verify_only: false,
        expect: Expect::Rows(&[
            RowSpec {
                topic: "a",
                partition: 0,
                code: NOT_ATTEMPTED,
            },
            RowSpec {
                topic: "missing",
                partition: 0,
                code: UNKNOWN,
            },
        ]),
        enlisted: &[],
    };
    [
        Case {
            name: "v4, client grants, no ClusterAction",
            version: 4,
            caller: CLIENT,
            partitions: &[PartitionSpec {
                topic: "a",
                partition: 0,
            }],
            verify_only: false,
            expect: Expect::TopLevel(CLUSTER_DENIED),
            enlisted: &[],
        },
        Case {
            name: "v4, ClusterAction only",
            version: 4,
            caller: BROKER,
            partitions: A_AND_B,
            verify_only: false,
            expect: Expect::Rows(&[
                RowSpec {
                    topic: "a",
                    partition: 0,
                    code: NONE,
                },
                RowSpec {
                    topic: "b",
                    partition: 0,
                    code: NONE,
                },
            ]),
            enlisted: &[
                PartitionSpec {
                    topic: "a",
                    partition: 0,
                },
                PartitionSpec {
                    topic: "b",
                    partition: 0,
                },
            ],
        },
        Case {
            name: "v5 verify-only, ClusterAction only, the partition is enlisted",
            version: 5,
            caller: BROKER,
            partitions: &[PartitionSpec {
                topic: "a",
                partition: 0,
            }],
            verify_only: true,
            expect: Expect::Rows(&[RowSpec {
                topic: "a",
                partition: 0,
                code: NONE,
            }]),
            enlisted: &[
                PartitionSpec {
                    topic: "a",
                    partition: 0,
                },
                PartitionSpec {
                    topic: "b",
                    partition: 0,
                },
            ],
        },
        missing_partition(
            "v4, ClusterAction only, a partition that does not exist",
            4,
            BROKER,
        ),
        Case {
            name: "v3, ClusterAction only",
            version: 3,
            caller: BROKER,
            partitions: &[PartitionSpec {
                topic: "a",
                partition: 0,
            }],
            verify_only: false,
            expect: Expect::Rows(&[RowSpec {
                topic: "a",
                partition: 0,
                code: TID_DENIED,
            }]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on a and not on b",
            version: 3,
            caller: CLIENT,
            partitions: A_AND_B,
            verify_only: false,
            expect: Expect::Rows(&[
                RowSpec {
                    topic: "a",
                    partition: 0,
                    code: NOT_ATTEMPTED,
                },
                RowSpec {
                    topic: "b",
                    partition: 0,
                    code: TOPIC_DENIED,
                },
            ]),
            enlisted: &[],
        },
        missing_partition("v3, a partition that does not exist", 3, CLIENT),
        Case {
            name: "v3, an internal topic with Write on it",
            version: 3,
            caller: CLIENT,
            partitions: &[PartitionSpec {
                topic: OFFSETS_TOPIC,
                partition: 0,
            }],
            verify_only: false,
            expect: Expect::Rows(&[RowSpec {
                topic: OFFSETS_TOPIC,
                partition: 0,
                code: TOPIC_DENIED,
            }]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on the transactional id and the topic",
            version: 3,
            caller: CLIENT,
            partitions: &[PartitionSpec {
                topic: "a",
                partition: 0,
            }],
            verify_only: false,
            expect: Expect::Rows(&[RowSpec {
                topic: "a",
                partition: 0,
                code: NONE,
            }]),
            enlisted: &[PartitionSpec {
                topic: "a",
                partition: 0,
            }],
        },
    ]
}

#[tokio::test]
async fn add_partitions_to_txn_authorizes_by_version_and_fails_the_whole_transaction() {
    let cases = cases();

    let CoordinatorBroker {
        handle: handle_,
        dir: _dir,
    } = start_coordinator(Arc::new(ControllerPeerAllowed(Grants))).await;
    let broker = handle_.broker_arc_for_test();

    let address = peer();
    // The verify-only case reads the transaction that the case before it
    // filled, so the two share a transactional id.
    let mut tid = String::new();
    let mut producer_id = 0;
    for (index, case) in (0_i64..).zip(cases.iter()) {
        if !case.verify_only {
            tid = format!("tid-{index}");
            producer_id = 100 + index;
            seed_transaction(&broker, &tid, producer_id).await;
        }
        let user = principal(case.caller);
        let ctx = request_context(&user, &address, "add-partitions-authorization");
        let resp = handle(
            &broker,
            request(case, &tid, producer_id),
            case.version,
            &ctx,
        )
        .await
        .expect("handle");
        check!(resp == expected(case, &tid), "{}", case.name);
        let enlisted = enlisted(&broker, &tid).await;
        check!(enlisted == partition_set(case.enlisted), "{}", case.name);
    }
    handle_.shutdown().await;
}
