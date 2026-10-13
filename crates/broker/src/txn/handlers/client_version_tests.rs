//! #1180: what the coordinator does for a request follows the request's API
//! version, and the cluster's `transaction.version` only picks the log format.
//!
//! Kafka's `handleEndTxnRequest` and `handleAddPartitionsToTxnRequest` pass
//! `TransactionVersion.transactionVersionForEndTxn` and
//! `transactionVersionForAddPartitionsToTxn`: `EndTxn` v0 to v4 and
//! `AddPartitionsToTxn` v0 to v3 are `TV_0` on any cluster, and
//! `handleAddOffsetsToTxnRequest` always passes `TV_0`. The in-process broker
//! finalizes `transaction.version` 2, so a client of an older protocol runs its
//! next transaction at the epoch it holds, which is what it needs: an `EndTxn`
//! v4 response cannot carry a bumped epoch.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        codes,
        test_support::{
            decode_response, dispatch_context, encode_request, peer, principal, request_context,
        },
        txn::{
            handlers::add_partitions_to_txn::test_support::{
                CoordinatorBroker, seed_transaction, start_coordinator,
            },
            state::TxnState,
        },
    },
    assert2::check,
    krabka_protocol::owned::{
        add_partitions_to_txn_request::{
            self, AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction,
        },
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::{self, EndTxnRequest},
        end_txn_response::EndTxnResponse,
    },
    std::sync::Arc,
};

const TID: &str = "tid-client-version";
const PRODUCER_ID: i64 = 11;

/// One `AddPartitionsToTxn` of topic `a`, in the shape of `version`. It returns
/// the partition's error code.
async fn add_partition(broker: &Broker, version: i16, epoch: i16) -> i16 {
    let user = principal("ANONYMOUS");
    let address = peer();
    let ctx = request_context(&user, &address, "client-version");
    let topics = vec![AddPartitionsToTxnTopic {
        name: "a".into(),
        partitions: vec![0],
        ..Default::default()
    }];
    let request = AddPartitionsToTxnRequest {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: TID.into(),
            producer_id: PRODUCER_ID,
            producer_epoch: epoch,
            verify_only: false,
            topics: topics.clone(),
            ..Default::default()
        }],
        v3_and_below_transactional_id: TID.into(),
        v3_and_below_producer_id: PRODUCER_ID,
        v3_and_below_producer_epoch: epoch,
        v3_and_below_topics: topics,
        ..Default::default()
    };
    let bytes = dispatch_context(
        broker,
        add_partitions_to_txn_request::API_KEY,
        version,
        &encode_request(&request, version),
        &ctx,
    )
    .await;
    let response: AddPartitionsToTxnResponse = decode_response(&bytes, version);
    let rows = if version >= 4 {
        response.results_by_transaction[0].topic_results[0]
            .results_by_partition
            .clone()
    } else {
        response.results_by_topic_v3_and_below[0]
            .results_by_partition
            .clone()
    };
    rows[0].partition_error_code
}

/// One `EndTxn` in the shape of `version`.
async fn end_txn(broker: &Broker, version: i16, epoch: i16, committed: bool) -> EndTxnResponse {
    let user = principal("ANONYMOUS");
    let address = peer();
    let ctx = request_context(&user, &address, "client-version");
    let request = EndTxnRequest {
        transactional_id: TID.into(),
        producer_id: PRODUCER_ID,
        producer_epoch: epoch,
        committed,
        ..Default::default()
    };
    let bytes = dispatch_context(
        broker,
        end_txn_request::API_KEY,
        version,
        &encode_request(&request, version),
        &ctx,
    )
    .await;
    decode_response(&bytes, version)
}

/// The parts of the coordinator's entry that a client-version step changes.
#[derive(Debug, PartialEq, Eq)]
struct EntryView {
    state: TxnState,
    producer_epoch: i16,
    client_transaction_version: i16,
}

/// One step: its name, the code it answered, and the entry after it.
#[derive(Debug, PartialEq, Eq)]
struct Step {
    name: &'static str,
    code: i16,
    entry: EntryView,
}

/// The coordinator's entry as an [`EntryView`].
async fn entry(broker: &Broker) -> EntryView {
    let entry = broker
        .txn_coordinator
        .get(TID)
        .expect("the transaction")
        .lock()
        .await
        .clone();
    EntryView {
        state: entry.state,
        producer_epoch: entry.producer_epoch,
        client_transaction_version: entry.client_transaction_version,
    }
}

#[tokio::test]
async fn a_client_that_speaks_an_older_protocol_keeps_its_epoch_on_a_transaction_version_2_cluster()
{
    let CoordinatorBroker { handle, dir: _dir } =
        start_coordinator(Arc::new(AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    seed_transaction(&broker, TID, PRODUCER_ID).await;
    // The commit writes its marker to `a-0`, which has to be local by then.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while broker
        .partitions
        .get("a", krabka_ids::PartitionIndex(0))
        .is_none()
    {
        check!(std::time::Instant::now() < deadline, "a-0 becomes local");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    // `seed_transaction` opens the transaction at producer epoch 2.
    let epoch = 2;

    let mut actual = Vec::new();
    actual.push(Step {
        name: "AddPartitionsToTxn v3",
        code: add_partition(&broker, 3, epoch).await,
        entry: entry(&broker).await,
    });
    actual.push(Step {
        name: "EndTxn v4 commit",
        code: end_txn(&broker, 4, epoch, true).await.error_code,
        entry: entry(&broker).await,
    });
    // A v3 client uses the epoch it holds for its second transaction. A
    // completion that bumped it would answer PRODUCER_FENCED here.
    actual.push(Step {
        name: "AddPartitionsToTxn v3 again",
        code: add_partition(&broker, 3, epoch).await,
        entry: entry(&broker).await,
    });
    actual.push(Step {
        name: "EndTxn v4 abort",
        code: end_txn(&broker, 4, epoch, false).await.error_code,
        entry: entry(&broker).await,
    });
    actual.push(Step {
        name: "AddPartitionsToTxn v4",
        code: add_partition(&broker, 4, epoch).await,
        entry: entry(&broker).await,
    });
    // Only EndTxn v5 bumps the epoch, and it answers the new one.
    let committed = end_txn(&broker, 5, epoch, true).await;
    actual.push(Step {
        name: "EndTxn v5 commit",
        code: committed.error_code,
        entry: entry(&broker).await,
    });
    let expected = vec![
        Step {
            name: "AddPartitionsToTxn v3",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::Ongoing,
                producer_epoch: epoch,
                client_transaction_version: 0,
            },
        },
        Step {
            name: "EndTxn v4 commit",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::CompleteCommit,
                producer_epoch: epoch,
                client_transaction_version: 0,
            },
        },
        Step {
            name: "AddPartitionsToTxn v3 again",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::Ongoing,
                producer_epoch: epoch,
                client_transaction_version: 0,
            },
        },
        Step {
            name: "EndTxn v4 abort",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::CompleteAbort,
                producer_epoch: epoch,
                client_transaction_version: 0,
            },
        },
        // AddPartitionsToTxn v4 comes from a TV_2 client or a broker, and
        // records TV_2.
        Step {
            name: "AddPartitionsToTxn v4",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::Ongoing,
                producer_epoch: epoch,
                client_transaction_version: 2,
            },
        },
        Step {
            name: "EndTxn v5 commit",
            code: codes::NONE,
            entry: EntryView {
                state: TxnState::CompleteCommit,
                producer_epoch: epoch + 1,
                client_transaction_version: 2,
            },
        },
    ];
    check!(actual == expected);
    check!((committed.producer_id, committed.producer_epoch) == (PRODUCER_ID, epoch + 1));

    handle.shutdown().await;
}
