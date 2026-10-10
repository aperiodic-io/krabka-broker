// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-664 `ListTransactions` (`api_key` 66) and `DescribeTransactions`
//! (`api_key` 65). Both surface the broker's local `TxnCoordinator`
//! state. List returns a `(transactional_id, producer_id, state)` summary.
//! Describe returns the full per-tid detail: timeout, start time, and
//! partitions.

mod support;

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_broker::{BrokerHandle, codes};
use krabka_client_producer::{OwnedTransaction, Producer, ProducerRecord};
use krabka_protocol::owned::{
    describe_transactions_request::DescribeTransactionsRequest,
    describe_transactions_response::{TopicData, TransactionState as DescribedTransactionState},
    list_transactions_request::ListTransactionsRequest,
    list_transactions_response::{
        ListTransactionsResponse, TransactionState as ListedTransactionState,
    },
};
use tempfile::TempDir;

pub use crate::support::boot_single;
use crate::support::{
    client::connect_client,
    topics::{creatable_topic, create_topic_request},
};

/// `ListTransactions`, retried while the coordinator loads its state
/// partitions. Kafka answers `COORDINATOR_LOAD_IN_PROGRESS` (14) until every
/// partition it leads is loaded, and the admin client retries.
async fn list_transactions(
    client: &krabka_client_core::Client,
    request: &ListTransactionsRequest,
) -> ListTransactionsResponse {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(request.clone())
            .await
            .expect("ListTransactions");
        if response.error_code != codes::COORDINATOR_LOAD_IN_PROGRESS
            || std::time::Instant::now() >= deadline
        {
            return response;
        }
        // intentional: the coordinator load has no awaiter reachable from a
        // client; the answer is the signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `DescribeTransactions` for one id, after the coordinator of that id has
/// loaded its `__transaction_state` partition. `FindCoordinator` creates the
/// topic, and the row carries `COORDINATOR_LOAD_IN_PROGRESS` (14) or
/// `NOT_COORDINATOR` (16) until the load ends.
async fn describe_transaction(
    client: &krabka_client_core::Client,
    transactional_id: &str,
) -> DescribedTransactionState {
    let _ = client
        .send(
            krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
                key: transactional_id.into(),
                key_type: 1,
                coordinator_keys: vec![transactional_id.into()],
                ..Default::default()
            },
        )
        .await
        .expect("FindCoordinator");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![transactional_id.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = response
            .transaction_states
            .first()
            .expect("one row per requested id")
            .clone();
        if !matches!(
            row.error_code,
            codes::COORDINATOR_LOAD_IN_PROGRESS | codes::NOT_COORDINATOR
        ) || std::time::Instant::now() >= deadline
        {
            return row;
        }
        // intentional: the coordinator load has no awaiter reachable from a
        // client; the answer is the signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn create_topic(bootstrap: &str, name: &str) {
    let client = connect_client(bootstrap, None).await;
    let cr = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (name).into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

fn rec(topic: &str, v: &str) -> ProducerRecord {
    crate::support::producer::string_record(topic, v)
}

async fn admin_client(bootstrap: &str) -> krabka_client_core::Client {
    connect_client(bootstrap, None).await
}

/// Boot a broker, init a transactional producer with the given tid,
/// begin a txn, and produce one record to `topic`. It leaves the txn
/// in `Ongoing` so the admin APIs can see it. It returns the broker and the
/// producer, and the caller closes both.
async fn boot_with_ongoing_txn(
    tid: &str,
    topic: &str,
) -> (
    BrokerHandle,
    String,
    TempDir,
    Arc<Producer>,
    OwnedTransaction,
) {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&bootstrap, topic).await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    broker.wait_until_transaction_coordinator_ready().await;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(tid)
            .build()
            .await
            .unwrap(),
    );
    producer.init_transactions().await.unwrap();
    let transaction = Arc::clone(&producer)
        .begin_transaction_owned()
        .await
        .unwrap();
    // `enqueue` enqueues into the producer's local batch; without
    // `flush()` the AddPartitionsToTxn round-trip that registers
    // `(topic, partition)` on the coordinator's TxnEntry may not run
    // before the admin call. The drop pattern around `enqueue` is
    // intentional — it's a future-of-record-metadata handle we don't
    // need (commits will never fire on this test path).
    drop(
        producer
            .enqueue(rec(topic, "v"))
            .await
            .expect("record is queued"),
    );
    producer.flush().await.unwrap();
    // Don't commit/abort — we want the txn to stay Ongoing.

    // `flush()` returning does not mean the coordinator has finished the
    // AddPartitionsToTxn round-trip that `enqueue` triggered, so its `TxnEntry`
    // can still read `Empty` with no partitions for a moment afterwards. Wait
    // for it here rather than in each caller: every test below asserts against
    // the `Ongoing` state this helper's name promises, and the one that did not
    // wait raced it and read `Empty` on CI. `DescribeTransactions` is the
    // strongest signal available, because it reports the state and the
    // registered partition set together, and there is no metadata-image event
    // to wait on instead. The deadline is generous because macOS runners are
    // measurably slower than linux on the in-process transactional path.
    let admin = admin_client(&bootstrap).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let r = admin
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![tid.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let ready = matches!(
            r.transaction_states.as_slice(),
            [row] if row.error_code == 0
                && row.transaction_state == "Ongoing"
                && !row.topics.is_empty()
        );
        if ready {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "txn {tid} never reached Ongoing with its partitions: {r:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    (broker, bootstrap, dir, producer, transaction)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_returns_ongoing_txn() {
    let (broker, bootstrap, _dir, producer, transaction) =
        boot_with_ongoing_txn("my-tid", "t-list").await;

    let client = admin_client(&bootstrap).await;
    let resp = list_transactions(&client, &ListTransactionsRequest::default()).await;

    let row = resp
        .transaction_states
        .iter()
        .find(|r| r.transactional_id == "my-tid")
        .expect("my-tid not present");
    check!(row.producer_id >= 0);
    assert!(
        resp == ListTransactionsResponse {
            transaction_states: vec![ListedTransactionState {
                transactional_id: "my-tid".into(),
                producer_id: row.producer_id,
                transaction_state: "Ongoing".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    );

    abort_and_close(transaction, producer).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_state_filter_excludes_non_matching() {
    let (broker, bootstrap, _dir, producer, transaction) =
        boot_with_ongoing_txn("my-tid", "t-state-filter").await;

    let client = admin_client(&bootstrap).await;
    // Filter to "Empty" only — our txn is Ongoing, so the row should
    // be excluded.
    let r = list_transactions(
        &client,
        &ListTransactionsRequest {
            state_filters: vec!["Empty".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(
        r == ListTransactionsResponse::default(),
        "Ongoing txn must not match an Empty state filter: {r:?}",
    );

    abort_and_close(transaction, producer).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_reports_unknown_state_filters() {
    let (broker, bootstrap, _dir) = boot_single().await;

    let client = admin_client(&bootstrap).await;
    let r = list_transactions(
        &client,
        &ListTransactionsRequest {
            state_filters: vec!["Ongoing".into(), "BogusState".into(), "Empty".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(
        r == ListTransactionsResponse {
            unknown_state_filters: vec!["BogusState".to_string()],
            ..Default::default()
        }
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_full_state_for_known_tid() {
    let (broker, bootstrap, _dir, producer, transaction) =
        boot_with_ongoing_txn("describe-tid", "t-describe").await;

    let client = admin_client(&bootstrap).await;
    let r = client
        .send(DescribeTransactionsRequest {
            transactional_ids: vec!["describe-tid".into()],
            ..Default::default()
        })
        .await
        .expect("DescribeTransactions");
    assert!(r.transaction_states.len() == 1);
    let row = r.transaction_states[0].clone();

    check!(row.producer_id >= 0);
    check!(row.transaction_start_time_ms > 0);
    assert!(
        row == DescribedTransactionState {
            transactional_id: "describe-tid".into(),
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: 60_000,
            transaction_start_time_ms: row.transaction_start_time_ms,
            producer_id: row.producer_id,
            topics: vec![TopicData {
                topic: "t-describe".into(),
                partitions: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        }
    );

    abort_and_close(transaction, producer).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_not_found_for_unknown_tid() {
    let (broker, bootstrap, _dir) = boot_single().await;

    let client = admin_client(&bootstrap).await;
    let row = describe_transaction(&client, "ghost-tid").await;
    assert!(
        row == DescribedTransactionState {
            error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
            transactional_id: "ghost-tid".into(),
            ..Default::default()
        }
    );

    broker.shutdown().await;
}

/// Abort before closing, proving the guard released its owning producer reference.
async fn abort_and_close(transaction: OwnedTransaction, producer: Arc<Producer>) {
    transaction.abort().await.unwrap();
    Arc::into_inner(producer)
        .expect("transaction guard released its producer reference")
        .close()
        .await
        .unwrap();
}
