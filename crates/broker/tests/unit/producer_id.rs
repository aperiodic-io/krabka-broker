//! `InitProducerId` with and without a transactional id.
//!
//! A plain call hands out a pooled producer id, and a transactional one needs
//! the `__transaction_state` topic to exist first. Only `FindCoordinator` with
//! `key_type` 1 asks for that topic. Its first answer is
//! `COORDINATOR_NOT_AVAILABLE`, and the client retries until the lookup names
//! a broker. A repeated transactional id bumps the epoch.

use assert2::{assert, check};
use krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;

use crate::support;

#[tokio::test]
async fn init_producer_id_returns_fresh_pid() {
    let p = support::start().await;
    // A null transactional id asks for an idempotent producer; the schema default is empty.
    let r = crate::support::transactions::claim_idempotent_producer(&p.client).await;
    check!(r.error_code == 0);
    check!(r.producer_id == 0);
    check!(r.producer_epoch == 0);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_without_coordinator_bootstrap_returns_not_coordinator() {
    // Without a prior FindCoordinator(TRANSACTION) call, __transaction_state
    // does not exist. Kafka's `TransactionStateManager
    // .getAndMaybeAddTransactionState` then answers NOT_COORDINATOR (16), and
    // InitProducerId does not create the topic.
    // A valid timeout isolates that path: `InitProducerId` now validates
    // transaction.timeout.ms before the coordinator lookup, and the wire
    // default of 0 is itself invalid, which would otherwise answer
    // INVALID_TRANSACTION_TIMEOUT (50) instead of the case this test names.
    let p = support::start().await;
    let r = p
        .client
        .send(crate::support::transactions::new_producer_request(
            crate::support::transactions::InitProducerSetup {
                transactional_id: Some("tx-1".into()),
                ..Default::default()
            },
        ))
        .await
        .expect("InitProducerId");
    assert!(r.error_code == 16); // NOT_COORDINATOR
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_txn_creates_topic_then_returns_local_broker() {
    let p = support::start().await; // single-voter broker
    // Use coordinator_keys (v4+ style) so the transaction-id reaches the
    // broker on the wire. key_type=1 selects the TRANSACTION branch.
    let first = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["my-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(TRANSACTION)");
    // Kafka's `KafkaApis.getCoordinator`: the lookup finds no
    // __transaction_state, asks for it, and answers COORDINATOR_NOT_AVAILABLE
    // (15) with `Node.noNode()`.
    crate::check_expected_coordinator!(response, first => ("my-tid", -1, String::new(), -1, 15, None));

    // The retried lookup resolves the partition leader, which is this broker
    // (the only broker in the cluster).
    let retried =
        support::find_coordinator(&p.client, support::KEY_TYPE_TRANSACTION, "my-tid").await;
    let listen = p.broker.listen_addr();
    crate::check_expected_coordinator!(row, retried => ("my-tid", 1, listen.ip().to_string(), i32::from(listen.port()), 0, None));
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_transactional_id_returns_real_pid() {
    let p = support::start().await;
    // Bootstrap __transaction_state via FindCoordinator (key_type=1), and
    // retry until the lookup names a coordinator, as a client does.
    support::find_coordinator(&p.client, support::KEY_TYPE_TRANSACTION, "my-tid").await;

    let r = p
        .client
        .send(crate::support::transactions::new_producer_request(
            crate::support::transactions::InitProducerSetup {
                transactional_id: Some("my-tid".into()),
                ..Default::default()
            },
        ))
        .await
        .expect("InitProducerId");
    check!(r.error_code == 0, "error_code should be NONE");
    check!(
        r.producer_id >= 0,
        "producer_id should come from txn coordinator's pool"
    );
    check!(r.producer_epoch == 0, "first allocation → epoch 0");
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_same_tid_bumps_epoch() {
    let p = support::start().await;
    // Bootstrap __transaction_state for stable-tid, and retry until the
    // lookup names a coordinator, as a client does.
    support::find_coordinator(&p.client, support::KEY_TYPE_TRANSACTION, "stable-tid").await;

    let r1 = p
        .client
        .send(crate::support::transactions::new_producer_request(
            crate::support::transactions::InitProducerSetup {
                transactional_id: Some("stable-tid".into()),
                ..Default::default()
            },
        ))
        .await
        .expect("InitProducerId 1");
    assert!(r1.error_code == 0, "r1 error_code");

    let r2 = p
        .client
        .send(crate::support::transactions::new_producer_request(
            crate::support::transactions::InitProducerSetup {
                transactional_id: Some("stable-tid".into()),
                ..Default::default()
            },
        ))
        .await
        .expect("InitProducerId 2");
    check!(r2.error_code == 0, "r2 error_code");
    check!(r1.producer_id == r2.producer_id, "same pid for same tid");
    check!(
        r2.producer_epoch == r1.producer_epoch + 1,
        "second call bumps epoch by 1"
    );
    p.broker.shutdown().await;
}
