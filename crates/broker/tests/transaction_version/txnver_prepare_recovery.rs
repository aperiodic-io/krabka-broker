//! A transaction whose `PrepareCommit` or `PrepareAbort` record is durable
//! must complete, and nothing may overwrite it while it completes.
//!
//! Kafka's `TransactionCoordinator` answers `EndTxn` with `NONE` once the
//! `Prepare*` record is in the transaction log, and its
//! `TransactionMarkerChannelManager` then writes the markers and the
//! `Complete*` record. `TransactionStateManager` does the same for every
//! `Prepare*` transaction it loads. `InitProducerId` answers
//! `CONCURRENT_TRANSACTIONS` until the transaction is complete.
//!
//! Each case stops or fails the marker fan-out with the broker's test gate,
//! so the durable state is `Prepare*` at a known point.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerHandle, MarkerFanoutMode};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    end_txn_request::EndTxnRequest, end_txn_response::EndTxnResponse,
    init_producer_id_response::InitProducerIdResponse, produce_request::ProduceRequest,
};
use tempfile::TempDir;

use crate::{
    support::transactions::{end_transaction_request, init_producer_request},
    txnver_harness::{
        admin_client, config, create_topic, downgrade_transaction_version, find_coordinator,
        topic_id,
    },
};

const CONCURRENT_TRANSACTIONS: i16 = 51;
const PRODUCER_FENCED: i16 = 90;

/// The time a retried request may take to see a transaction complete.
const SETTLE: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy)]
struct Identity {
    producer_id: i64,
    epoch: i16,
}

fn coordinator_is_loading(code: i16) -> bool {
    crate::support::transaction_wire::coordinator_loading(code)
}

async fn init_producer_id(
    client: &Client,
    transactional_id: &str,
    identity: (i64, i16),
) -> InitProducerIdResponse {
    client
        .send(init_producer_request(
            crate::support::transactions::InitProducerSetup {
                transactional_id: Some(transactional_id.into()),
                producer: crate::support::transactions::ProducerIdentity::from_wire((
                    identity.0, identity.1,
                )),
                ..Default::default()
            },
        ))
        .await
        .expect("InitProducerId")
}

async fn init_producer(client: &Client, transactional_id: &str) -> Identity {
    find_coordinator(client, transactional_id).await;
    let response = crate::support::transaction_wire::initialize_producer(
        || init_producer_id(client, transactional_id, (-1, -1)),
        coordinator_is_loading,
        SETTLE,
    )
    .await;
    Identity {
        producer_id: response.producer_id,
        epoch: response.producer_epoch,
    }
}

async fn add_partition(client: &Client, transactional_id: &str, producer: Identity, topic: &str) {
    crate::support::transaction_wire::partition_added(client.send(
        crate::support::transaction_wire::add_partition_request(
            transactional_id,
            topic,
            (producer.producer_id, producer.epoch),
        ),
    ))
    .await;
}

/// Produce `values` in one batch. `transaction` is `None` for a plain batch,
/// and otherwise names the transactional id the request carries, as a Kafka
/// producer's request does.
async fn produce(
    client: &Client,
    topic: &str,
    transaction: Option<(&str, Identity)>,
    values: &[&'static str],
) {
    let producer = transaction.map(|(_, producer)| producer);
    let batch = crate::support::transaction_wire::records_batch(
        producer.map(|producer| (producer.producer_id, producer.epoch)),
        values,
    );
    let request = ProduceRequest {
        transactional_id: transaction.map(|(transactional_id, _)| transactional_id.to_owned()),
        ..crate::support::produce::batch_request(
            batch,
            crate::support::produce::SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id: topic_id(client, topic).await,
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        )
    };
    let response = crate::support::transaction_wire::settled_produce(client, request, SETTLE).await;
    let code = response.responses[0].partition_responses[0].error_code;
    assert!(code == 0, "Produce: {response:?}");
}

fn end_txn_request(
    transactional_id: &str,
    producer: Identity,
    outcome: crate::support::transactions::TransactionOutcome,
) -> EndTxnRequest {
    end_transaction_request(
        transactional_id,
        crate::support::transactions::EndTransactionSetup {
            producer: crate::support::transactions::ProducerIdentity::from_wire((
                producer.producer_id,
                producer.epoch,
            )),
            outcome,
        },
    )
}

/// Retry `EndTxn` the way a Kafka producer does after a lost answer, until
/// the answer is not retriable or the time is up.
async fn end_txn_until_answered(
    client: &Client,
    transactional_id: &str,
    producer: Identity,
    outcome: crate::support::transactions::TransactionOutcome,
) -> EndTxnResponse {
    find_coordinator(client, transactional_id).await;
    crate::support::transaction_wire::retry_coordinator(
        || async {
            client
                .send(end_txn_request(transactional_id, producer, outcome))
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        |code| coordinator_is_loading(code) || code == CONCURRENT_TRANSACTIONS,
        SETTLE,
    )
    .await
}

/// The values a `read_committed` consumer reads, up to and including `last`.
async fn read_committed_through(bootstrap: &str, topic: &str, last: &str) -> Vec<String> {
    crate::support::transaction_wire::read_committed_through(bootstrap, topic, last, SETTLE).await
}

struct Started {
    broker: BrokerHandle,
    client: Client,
    bootstrap: String,
}

async fn start(log_dir: &std::path::Path, bootstrap_mode: Option<BootstrapMode>) -> Started {
    let broker = Broker::start(config(log_dir.to_path_buf(), bootstrap_mode))
        .await
        .expect("start broker");
    let bootstrap = broker.listen_addr().to_string();
    let client = admin_client(&bootstrap).await;
    Started {
        broker,
        client,
        bootstrap,
    }
}

/// Start a transaction with three records on `topic`, after a downgrade to
/// `transaction.version` `downgrade_to` when it is set.
async fn open_transaction(
    client: &Client,
    transactional_id: &str,
    topic: &str,
    downgrade_to: Option<i16>,
) -> Identity {
    create_topic(
        client,
        topic,
        crate::support::topics::TopicPartitionCount(1),
    )
    .await;
    if let Some(level) = downgrade_to {
        downgrade_transaction_version(client, level).await;
    }
    let producer = init_producer(client, transactional_id).await;
    add_partition(client, transactional_id, producer, topic).await;
    produce(
        client,
        topic,
        Some((transactional_id, producer)),
        &["a", "b", "c"],
    )
    .await;
    producer
}

async fn fanout_transaction(
    log_dir: &std::path::Path,
    transactional_id: &str,
    topic: &str,
    downgrade_to: Option<i16>,
    mode: MarkerFanoutMode,
) -> (Started, Identity) {
    let started = start(log_dir, None).await;
    let producer = open_transaction(&started.client, transactional_id, topic, downgrade_to).await;
    started.broker.set_transaction_marker_fanout_for_test(mode);
    (started, producer)
}

/// The `EndTxn` v5 answer for a completed transaction. A v5 request is a `TV_2`
/// client whatever `transaction.version` the cluster finalized, so completion
/// bumps the epoch (`epoch_bump` 1) at every level. A client below v5 would keep
/// it.
fn completed(producer: Identity, epoch_bump: i16) -> EndTxnResponse {
    EndTxnResponse {
        producer_id: producer.producer_id,
        producer_epoch: producer.epoch + epoch_bump,
        ..Default::default()
    }
}

struct CutCase {
    name: &'static str,
    topic: &'static str,
    outcome: crate::support::transactions::TransactionOutcome,
    downgrade_to: Option<i16>,
    epoch_bump: i16,
    /// What a `read_committed` consumer reads through the record that the
    /// test writes after the transaction completes.
    visible: &'static [&'static str],
}

/// Stop the broker while `EndTxn` waits between its `Prepare*` append and its
/// markers, start it again, and retry `EndTxn`.
async fn end_txn_cut_by_shutdown(case: &CutCase) -> (Identity, EndTxnResponse, Vec<String>) {
    let directory = TempDir::new().expect("tempdir");
    let (first, producer) = fanout_transaction(
        directory.path(),
        case.name,
        case.topic,
        case.downgrade_to,
        MarkerFanoutMode::Hold,
    )
    .await;
    let cut_client = admin_client(&first.bootstrap).await;
    let request = end_txn_request(case.name, producer, case.outcome);
    let cut = tokio::spawn(async move { cut_client.send(request).await });
    first
        .broker
        .wait_for_transaction_marker_fanouts_for_test(1)
        .await;
    first.broker.shutdown().await;
    let cut_answer = cut.await.expect("cut EndTxn task");
    assert!(
        cut_answer.is_err(),
        "{}: a shutdown must cut the held EndTxn without an answer: {cut_answer:?}",
        case.name
    );

    let second = start(directory.path(), Some(BootstrapMode::Rejoin)).await;
    let retried = end_txn_until_answered(&second.client, case.name, producer, case.outcome).await;
    produce(&second.client, case.topic, None, &["z"]).await;
    let seen = read_committed_through(&second.bootstrap, case.topic, "z").await;
    second.broker.shutdown().await;
    (producer, retried, seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_cut_between_prepare_and_complete_completes_after_restart() {
    let cases = [
        CutCase {
            name: "cut-commit",
            topic: "cut-commit",
            outcome: crate::support::transactions::TransactionOutcome::Commit,
            downgrade_to: None,
            epoch_bump: 1,
            visible: &["a", "b", "c", "z"],
        },
        CutCase {
            name: "cut-abort",
            topic: "cut-abort",
            outcome: crate::support::transactions::TransactionOutcome::Abort,
            downgrade_to: None,
            epoch_bump: 1,
            visible: &["z"],
        },
        CutCase {
            name: "cut-commit-tv1",
            topic: "cut-commit-tv1",
            outcome: crate::support::transactions::TransactionOutcome::Commit,
            downgrade_to: Some(1),
            epoch_bump: 1,
            visible: &["a", "b", "c", "z"],
        },
    ];
    for case in &cases {
        let (producer, retried, seen) = end_txn_cut_by_shutdown(case).await;
        assert!(
            retried == completed(producer, case.epoch_bump),
            "{}: retried EndTxn",
            case.name
        );
        assert!(seen == case.visible, "{}: read_committed", case.name);
    }
}

async fn assert_visible_suffix_and_shutdown(started: Started, topic: &str, expected: &[&str]) {
    produce(&started.client, topic, None, &["z"]).await;
    let seen = read_committed_through(&started.bootstrap, topic, "z").await;
    assert!(seen == expected);
    started.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_producer_id_during_prepare_commit_is_concurrent_transactions() {
    let transactional_id = "init-during-prepare";
    let topic = "init-during-prepare";
    let directory = TempDir::new().expect("tempdir");
    let (started, producer) = fanout_transaction(
        directory.path(),
        transactional_id,
        topic,
        None,
        MarkerFanoutMode::Hold,
    )
    .await;
    let end_client = admin_client(&started.bootstrap).await;
    let request = end_txn_request(
        transactional_id,
        producer,
        crate::support::transactions::TransactionOutcome::Commit,
    );
    let end = tokio::spawn(async move { end_client.send(request).await });
    started
        .broker
        .wait_for_transaction_marker_fanouts_for_test(1)
        .await;

    // Kafka `prepareInitProducerIdTransit`: a producer ID that is not the
    // entry's is fenced, and any other request waits for the transition.
    let cases = [
        ("no identity", (-1, -1), CONCURRENT_TRANSACTIONS),
        (
            "the transaction's identity",
            (producer.producer_id, producer.epoch),
            CONCURRENT_TRANSACTIONS,
        ),
        (
            "another producer ID",
            (producer.producer_id + 1_000, 0),
            PRODUCER_FENCED,
        ),
    ];
    for (name, identity, error_code) in cases {
        let response = init_producer_id(&started.client, transactional_id, identity).await;
        let expected = InitProducerIdResponse {
            error_code,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        assert!(response == expected, "InitProducerId with {name}");
    }

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
    let answer = end.await.expect("EndTxn task").expect("EndTxn answer");
    assert!(answer == completed(producer, 1));
    assert_visible_suffix_and_shutdown(started, topic, &["a", "b", "c", "z"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_txn_with_a_failed_marker_fanout_answers_none_and_completes_later() {
    let transactional_id = "failed-fanout";
    let topic = "failed-fanout";
    let directory = TempDir::new().expect("tempdir");
    let (started, producer) = fanout_transaction(
        directory.path(),
        transactional_id,
        topic,
        None,
        MarkerFanoutMode::Fail,
    )
    .await;
    let answer = started
        .client
        .send(end_txn_request(
            transactional_id,
            producer,
            crate::support::transactions::TransactionOutcome::Commit,
        ))
        .await
        .expect("EndTxn");
    assert!(
        answer == completed(producer, 1),
        "EndTxn after a durable PrepareCommit"
    );

    started
        .broker
        .set_transaction_marker_fanout_for_test(MarkerFanoutMode::Open);
    let retried = end_txn_until_answered(
        &started.client,
        transactional_id,
        producer,
        crate::support::transactions::TransactionOutcome::Commit,
    )
    .await;
    assert!(retried == completed(producer, 1), "retried EndTxn");
    assert_visible_suffix_and_shutdown(started, topic, &["a", "b", "c", "z"]).await;
}
