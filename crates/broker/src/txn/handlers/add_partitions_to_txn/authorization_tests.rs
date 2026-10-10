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

use std::{collections::BTreeSet, sync::Arc};

use assert2::check;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
};

use super::{
    handle,
    test_support::{enlisted, seed_transaction, start_coordinator, topic, topic_result},
};
use crate::{
    authorizer::AuthorizationResult,
    codes,
    coordinator::bootstrap::OFFSETS_TOPIC,
    test_support::{peer, principal, request_context},
};

/// A broker principal: `ClusterAction` on the cluster and nothing else.
const BROKER: &str = "broker";
/// A client principal: `Write` on every transactional id, and `Write` on the
/// topics in [`CLIENT_TOPICS`] only.
const CLIENT: &str = "client";
/// The topics [`CLIENT`] may write, an internal topic included.
const CLIENT_TOPICS: [&str; 3] = ["a", "missing", OFFSETS_TOPIC];

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
    /// These `(topic, partition, code)` rows.
    Rows(&'static [(&'static str, i32, i16)]),
}

struct Case {
    name: &'static str,
    version: i16,
    caller: &'static str,
    partitions: &'static [(&'static str, i32)],
    verify_only: bool,
    expect: Expect,
    /// The partitions in the transaction after the request.
    enlisted: &'static [(&'static str, i32)],
}

fn request(case: &Case, tid: &str, producer_id: i64) -> AddPartitionsToTxnRequest {
    let mut topics: Vec<(&str, Vec<i32>)> = Vec::new();
    for &(name, partition) in case.partitions {
        match topics.iter_mut().find(|(topic, _)| *topic == name) {
            Some((_, partitions)) => partitions.push(partition),
            None => topics.push((name, vec![partition])),
        }
    }
    let topics = topics
        .iter()
        .map(|(name, partitions)| topic(name, partitions))
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
    let mut topics: Vec<(&str, Vec<(i32, i16)>)> = Vec::new();
    for &(name, partition, code) in rows {
        match topics.iter_mut().find(|(topic, _)| *topic == name) {
            Some((_, partitions)) => partitions.push((partition, code)),
            None => topics.push((name, vec![(partition, code)])),
        }
    }
    let topic_results = topics
        .iter()
        .map(|(name, rows)| topic_result(name, rows))
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

#[tokio::test]
async fn add_partitions_to_txn_authorizes_by_version_and_fails_the_whole_transaction() {
    use codes::{
        CLUSTER_AUTHORIZATION_FAILED as CLUSTER_DENIED, NONE,
        OPERATION_NOT_ATTEMPTED as NOT_ATTEMPTED, TOPIC_AUTHORIZATION_FAILED as TOPIC_DENIED,
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED as TID_DENIED, UNKNOWN_TOPIC_OR_PARTITION as UNKNOWN,
    };

    let missing_partition = |name, version, caller| Case {
        name,
        version,
        caller,
        partitions: &[("a", 0), ("missing", 0)],
        verify_only: false,
        expect: Expect::Rows(&[("a", 0, NOT_ATTEMPTED), ("missing", 0, UNKNOWN)]),
        enlisted: &[],
    };
    let cases = [
        Case {
            name: "v4, client grants, no ClusterAction",
            version: 4,
            caller: CLIENT,
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::TopLevel(CLUSTER_DENIED),
            enlisted: &[],
        },
        Case {
            name: "v4, ClusterAction only",
            version: 4,
            caller: BROKER,
            partitions: &[("a", 0), ("b", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NONE), ("b", 0, NONE)]),
            enlisted: &[("a", 0), ("b", 0)],
        },
        Case {
            name: "v5 verify-only, ClusterAction only, the partition is enlisted",
            version: 5,
            caller: BROKER,
            partitions: &[("a", 0)],
            verify_only: true,
            expect: Expect::Rows(&[("a", 0, NONE)]),
            enlisted: &[("a", 0), ("b", 0)],
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
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, TID_DENIED)]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on a and not on b",
            version: 3,
            caller: CLIENT,
            partitions: &[("a", 0), ("b", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NOT_ATTEMPTED), ("b", 0, TOPIC_DENIED)]),
            enlisted: &[],
        },
        missing_partition("v3, a partition that does not exist", 3, CLIENT),
        Case {
            name: "v3, an internal topic with Write on it",
            version: 3,
            caller: CLIENT,
            partitions: &[(OFFSETS_TOPIC, 0)],
            verify_only: false,
            expect: Expect::Rows(&[(OFFSETS_TOPIC, 0, TOPIC_DENIED)]),
            enlisted: &[],
        },
        Case {
            name: "v3, Write on the transactional id and the topic",
            version: 3,
            caller: CLIENT,
            partitions: &[("a", 0)],
            verify_only: false,
            expect: Expect::Rows(&[("a", 0, NONE)]),
            enlisted: &[("a", 0)],
        },
    ];

    let (handle_, _dir) =
        start_coordinator(Arc::new(crate::test_support::ControllerPeerAllowed(Grants))).await;
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
        let want: BTreeSet<(String, i32)> = case
            .enlisted
            .iter()
            .map(|&(topic, partition)| (topic.to_owned(), partition))
            .collect();
        check!(enlisted == want, "{}", case.name);
    }
    handle_.shutdown().await;
}
