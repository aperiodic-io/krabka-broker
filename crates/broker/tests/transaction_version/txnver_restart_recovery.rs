//! Restart recovery of `__transaction_state`: the decode / recover-from-disk
//! path that a live broker never exercises.
//!
//! Each case persists an `Ongoing` entry, restarts the broker on the same data
//! directory, and commits the recovered transaction through `EndTxn`. A commit
//! succeeds only if `TxnCoordinator::recover` decoded the persisted record with
//! the original producer identity, so the `EndTxn` response is the proof.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{BootstrapMode, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    end_txn_response::EndTxnResponse, find_coordinator_request::FindCoordinatorRequest,
};
use tempfile::TempDir;

use crate::{
    support::transactions::{end_transaction_request, init_producer_request},
    txnver_harness::{NONE, create_topic, downgrade_transaction_version},
};

/// Re-open the broker on the SAME data dir. A populated dir replays the raft
/// log and checkpoint instead of a re-bootstrap, so the restart uses
/// `BootstrapMode::Rejoin`. This is the same pattern as
/// `consumer_group_next_gen_persistence.rs`.
fn recovery_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    // Recovery fails closed if any locally led state partition is missing.
    // This single-transaction fixture materializes and replays one partition.
    cfg.transaction_state_num_partitions = 1;
    cfg.transaction_state_replication_factor = 1;
    cfg
}

fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = recovery_config(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

async fn find_ready_coordinator(client: &Client, tid: &str) {
    let fc = client
        .send(FindCoordinatorRequest {
            key: tid.into(),
            key_type: 1, // TRANSACTION
            coordinator_keys: vec![tid.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        fc.error_code == 0 || fc.coordinators.iter().all(|c| c.error_code == 0),
        "FindCoordinator: {fc:?}"
    );
}

/// `InitProducerId` for `tid`. It retries while the coordinator is still
/// loading, that is, on `COORDINATOR_NOT_AVAILABLE(15)` or
/// `NOT_COORDINATOR(16)`. Returns the assigned
/// `(producer_id, producer_epoch)`.
async fn init_producer_id(client: &Client, tid: &str) -> (i64, i16) {
    // FindCoordinator locates and triggers loading of the coordinator for tid;
    // on a single-broker cluster the coordinator load can lag broker boot.
    find_ready_coordinator(client, tid).await;

    let mut init = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let resp = client
            .send(init_producer_request(
                crate::support::transactions::InitProducerSetup {
                    transactional_id: Some(tid.into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("InitProducerId");
        if resp.error_code == 0 {
            init = Some(resp);
            break;
        }
        assert!(
            resp.error_code == 15 || resp.error_code == 16,
            "InitProducerId failed: {resp:?}"
        );
        // intentional: txn-coordinator load state is not in the metadata image and
        // has no metric/awaiter; only InitProducerId's 15/16 code signals it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let init = init.expect("InitProducerId did not become ready within 10s");
    (init.producer_id, init.producer_epoch)
}

/// `AddPartitionsToTxn` to add `(topic, partition)` to the ongoing txn for
/// `tid`/`pid`/`epoch`. This transitions the coordinator entry to `Ongoing`
/// and PERSISTS a `TransactionLogValue` record to `__transaction_state`. It
/// does not commit the record. Asserts success.
async fn add_partition_ongoing(
    client: &Client,
    tid: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
) {
    let added_topic = crate::support::transaction_wire::transaction_topic(topic, vec![partition]);
    let add = client
        .send(crate::support::transaction_wire::partitions_request(
            crate::support::transaction_wire::TransactionPartitionsSetup {
                transactional_id: tid,
                producer: crate::support::transactions::ProducerIdentity::from_wire((pid, epoch)),
                topics: vec![added_topic],
                ..Default::default()
            },
        ))
        .await
        .expect("AddPartitionsToTxn add");
    let expected = crate::txnver_harness::expected_partitions(tid, topic, &[(partition, NONE)]);
    assert!(
        add == expected,
        "adding ({topic},{partition}) returned an unexpected response: {add:?}"
    );
}

/// Wait for the transaction coordinator for `tid` to finish loading after a
/// (re)boot, then commit the in-flight transaction through `EndTxn`. The
/// commit succeeds only if the coordinator already holds an `Ongoing` entry
/// whose `(producer_id, producer_epoch)` match. On a freshly-rebooted broker
/// that entry can come only from a decode of the persisted
/// `__transaction_state` record. Returns the complete `EndTxn` response.
async fn commit_via_end_txn(client: &Client, tid: &str, pid: i64, epoch: i16) -> EndTxnResponse {
    // FindCoordinator both locates and triggers loading of the coordinator.
    find_ready_coordinator(client, tid).await;

    // Retry while the coordinator is still loading state from disk.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = client
            .send(end_transaction_request(
                tid,
                crate::support::transactions::EndTransactionSetup {
                    producer: crate::support::transactions::ProducerIdentity::from_wire((
                        pid, epoch,
                    )),
                    ..Default::default()
                },
            ))
            .await
            .expect("EndTxn");
        // 15/16: coordinator still loading — keep retrying until the deadline.
        if (resp.error_code == 15 || resp.error_code == 16) && std::time::Instant::now() < deadline
        {
            // intentional: coordinator recover/load state after restart is not in the
            // metadata image and has no metric/awaiter; only EndTxn's 15/16 code
            // signals it. Bounded RPC-response poll, not a materialization wait.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return resp;
    }
}

struct RecoveryCase {
    name: &'static str,
    topic: &'static str,
    tid: &'static str,
    downgrade_to: Option<i16>,
    completion_epoch_delta: i16,
}

/// Persist an `Ongoing` transaction, restart on the same data directory, and
/// compare the complete `EndTxn` response after recovery. Success proves that
/// the broker decoded the selected transaction-log codec with the original
/// producer identity. The expected completion epoch also checks KIP-890: the
/// `EndTxn` v5 request that these tests send is a `TV_2` client whatever
/// `transaction.version` the cluster finalized, so it bumps the epoch at every
/// level, and the level only picks the log codec.
async fn assert_ongoing_txn_survives_restart(case: &RecoveryCase) {
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let (pid, epoch);
    {
        let (broker, _bootstrap, client) = crate::support::client::start_client(
            recovery_config(log_dir.clone()),
            Some("krabka-txnv-test"),
        )
        .await;
        create_topic(
            &client,
            case.topic,
            crate::support::topics::TopicPartitionCount(1),
        )
        .await;
        if let Some(level) = case.downgrade_to {
            downgrade_transaction_version(&client, level).await;
        }

        (pid, epoch) = init_producer_id(&client, case.tid).await;
        add_partition_ongoing(&client, case.tid, pid, epoch, case.topic, 0).await;
        // Deliberately do NOT commit: the entry stays Ongoing on disk.

        broker.shutdown().await;
    }

    // Re-boot on the same dir: triggers TxnCoordinator::recover + decode.
    {
        let (broker, _bootstrap, client) =
            crate::support::client::start_client(rejoin_config(log_dir), Some("krabka-txnv-test"))
                .await;

        let response = commit_via_end_txn(&client, case.tid, pid, epoch).await;
        let expected = EndTxnResponse {
            producer_id: pid,
            producer_epoch: epoch + case.completion_epoch_delta,
            ..Default::default()
        };
        assert!(
            response == expected,
            "{} recovery returned an unexpected EndTxn response: {response:?}",
            case.name
        );

        broker.shutdown().await;
    }
}

/// Primary durability matrix for the v1 (flexible, `TV_2` default) and v0
/// (classic, `TV_0`) transaction-log codecs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_ongoing_transactions_survive_restart_and_decode_recovery() {
    let cases = [
        RecoveryCase {
            name: "v1/TV_2",
            topic: "rec1",
            tid: "recover-v1-tid",
            downgrade_to: None,
            completion_epoch_delta: 1,
        },
        RecoveryCase {
            name: "v0/TV_0",
            topic: "rec0",
            tid: "recover-v0-tid",
            downgrade_to: Some(0),
            completion_epoch_delta: 1,
        },
    ];

    for case in &cases {
        Box::pin(assert_ongoing_txn_survives_restart(case)).await;
    }
}
