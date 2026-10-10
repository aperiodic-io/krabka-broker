//! Share fetches of a group whose `share.isolation.level` is
//! `read_committed`. The acquire window is clamped to the last stable offset,
//! so the records of an open transaction stay invisible until that
//! transaction commits, and the broker surfaces them afterwards rather than
//! losing them. The data of an aborted transaction is archived and never
//! acquired.
//!
//! Kafka has no broker isolation key, so each test sets the group override,
//! and a group without one reads `read_uncommitted`.

use std::{collections::BTreeSet, time::Duration};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_producer::Producer;
use krabka_metadata::{GroupConfigRecord, MetadataRecord};

use crate::{
    harness::{bootstrap_share_state, produce_n},
    share_rpc::{acquired_count, share_fetch},
};

/// Kafka's `GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG` values.
const READ_COMMITTED: &str = "read_committed";
const READ_UNCOMMITTED: &str = "read_uncommitted";

/// Sets the `share.isolation.level` of `group` in the metadata image, beside
/// the `share.auto.offset.reset=earliest` that `bootstrap_share_state` sets,
/// in the record `IncrementalAlterConfigs` writes for a group config.
///
/// The record replaces the group's whole override map, so it carries both
/// keys, and it goes after `bootstrap_share_state`.
async fn set_isolation_level(broker: &BrokerHandle, group: &str, level: &str) {
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group.into(),
            configs: [
                ("share.auto.offset.reset".to_owned(), "earliest".to_owned()),
                ("share.isolation.level".to_owned(), level.to_owned()),
            ]
            .into(),
        }))
        .await
        .expect("set share.isolation.level");
}

/// Prepare a transaction-ready empty share group with its exact isolation override.
async fn isolation_fixture(
    level: Option<&str>,
) -> (
    tokio::sync::OwnedSemaphorePermit,
    BrokerHandle,
    std::sync::Arc<krabka_client_core::Client>,
    tempfile::TempDir,
    uuid::Uuid,
    String,
) {
    let (permit, broker, client, dir, tid) =
        Box::pin(crate::support::share::permitted_topic_fixture(
            crate::support::share::ShareTopicSetup::default(),
            |_| {},
        ))
        .await;
    let bootstrap = broker.listen_addr().to_string();
    bootstrap_share_state(&broker, &client, "g1").await;
    if let Some(level) = level {
        set_isolation_level(&broker, "g1", level).await;
    }
    broker.wait_until_transaction_coordinator_ready().await;
    (permit, broker, client, dir, tid, bootstrap)
}

/// F2 (`read_committed`): with `share.isolation.level = read_committed`, a
/// share fetch never surfaces records from an OPEN transaction (offsets past
/// the LSO).
///
/// A transactional producer begins a txn and sends 3 records but does NOT
/// commit. The partition's HWM is then 3 while the LSO stays at 0. A
/// `read_committed` share fetch clamps its read window to `min(LSO, HWM) = 0`,
/// so it acquires nothing. After the txn commits, the LSO advances to 3 and the
/// same group then acquires all 3. This proves the clamp tracked the LSO, and
/// that the broker merely deferred the records and did not lose them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_skips_open_txn_then_sees_committed() {
    let (_permit, broker, client, _dir, tid, bootstrap) =
        isolation_fixture(Some(READ_COMMITTED)).await;

    // Open a transaction and send 3 records WITHOUT committing: HWM=3, LSO=0.
    let producer =
        crate::support::producer::transactional_producer(bootstrap.clone(), "share-rc-tid").await;
    let txn = producer.begin_transaction().await.unwrap();
    enqueue_transaction_values(&producer).await;
    // Flush the records to the log (advances HWM) but keep the txn OPEN (LSO=0).
    producer.flush().await.unwrap();

    let (member, _member_epoch) =
        crate::support::share::join_consume_member(&broker, &client, tid).await;

    // A read_committed share fetch must acquire NOTHING: every record is past
    // the LSO (still 0). Poll a few times to be sure it never spuriously acquires.
    for epoch in 0..6 {
        crate::share_rpc::fetch_empty(
            &client,
            crate::support::share::ShareFetchSetup::at_epoch(
                &member,
                tid,
                crate::support::share::ShareSessionEpoch(epoch),
            ),
        )
        .await;
        // intentional: deliberately observe that nothing is acquired across a
        // window while the txn stays open (behavior under test, not a
        // state-settle guess).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Commit the transaction → the LSO advances past the records (a commit
    // control marker is appended, so HWM == LSO). The same group now acquires
    // the committed records (proving they were deferred, not dropped). The
    // acquired window also covers the control-marker offset, whose bytes the
    // read path filters out — so we assert on the surfaced record VALUES.
    txn.commit().await.unwrap();
    let mut values: Vec<String> = Vec::new();
    for epoch in 6..30 {
        let row = share_fetch(
            &client,
            crate::support::share::ShareFetchSetup::at_epoch(
                &member,
                tid,
                crate::support::share::ShareSessionEpoch(epoch),
            ),
        )
        .await;
        if acquired_count(&row) > 0
            && let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2())
        {
            values = crate::support::share::record_values(batches);
            values.sort();
            if values == vec!["a", "b", "c"] {
                break;
            }
        }
        // intentional: bounded RPC poll for the post-commit LSO advance
        // (transaction-coordinator state, not in the metadata image) to surface
        // via ShareFetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        values == vec!["a", "b", "c"],
        "after commit the read_committed fetch must surface the 3 committed \
         records, got {values:?}"
    );

    producer.close().await.unwrap();
}

/// What a share group sees of one transaction followed by one plain record:
/// the offsets it acquired and the record values that it got.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    acquired: BTreeSet<i64>,
    values: BTreeSet<String>,
}

impl Seen {
    fn expected(offsets: &[i64], values: &[&str]) -> Self {
        Self {
            acquired: offsets.iter().copied().collect(),
            values: values.iter().map(|value| (*value).to_string()).collect(),
        }
    }
}

/// Kafka's `SharePartition.filterAbortedTransactionalAcquiredRecords`: under
/// `read_committed`, a share group never acquires the data of an aborted
/// transaction. The share consumer cannot drop it itself, because a
/// `ShareFetch` response has no aborted-transactions field.
///
/// Each row writes a transaction of three records at offsets 0-2 and ends it,
/// which puts the marker at offset 3. It then produces one plain record `v0`
/// at offset 4. The marker is never acquired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_transaction_data_is_archived_under_read_committed() {
    let cases = [
        (
            "read_committed, commit",
            Some(READ_COMMITTED),
            true,
            Seen::expected(&[0, 1, 2, 4], &["a", "b", "c", "v0"]),
        ),
        (
            "read_committed, abort",
            Some(READ_COMMITTED),
            false,
            Seen::expected(&[4], &["v0"]),
        ),
        (
            "read_uncommitted, abort",
            Some(READ_UNCOMMITTED),
            false,
            Seen::expected(&[0, 1, 2, 4], &["a", "b", "c", "v0"]),
        ),
        (
            "no override reads uncommitted, abort",
            None,
            false,
            Seen::expected(&[0, 1, 2, 4], &["a", "b", "c", "v0"]),
        ),
    ];

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, isolation_level, commit, want) in cases {
        actual.push((
            name,
            Box::pin(transaction_then_record(isolation_level, commit)).await,
        ));
        expected.push((name, want));
    }
    assert!(actual == expected);
}

/// `isolation_level` is the group's `share.isolation.level`, or `None` for a
/// group with no override.
async fn transaction_then_record(isolation_level: Option<&str>, commit: bool) -> Seen {
    let (_permit, broker, client, _dir, tid, bootstrap) = isolation_fixture(isolation_level).await;

    let producer =
        crate::support::producer::transactional_producer(bootstrap.clone(), "share-aborted-tid")
            .await;
    let txn = producer.begin_transaction().await.unwrap();
    enqueue_transaction_values(&producer).await;
    producer.flush().await.unwrap();
    if commit {
        txn.commit().await.unwrap();
    } else {
        txn.abort().await.unwrap();
    }
    produce_n(
        &client,
        crate::support::share::ShareRecordsSetup {
            topic_id: tid,
            ..Default::default()
        },
    )
    .await;

    let (member, _member_epoch) =
        crate::support::share::join_consume_member(&broker, &client, tid).await;

    // The member never acknowledges, so each offset is acquired at most once
    // while its lock holds. Fetch until the plain record at offset 4 arrives.
    let mut seen = Seen {
        acquired: BTreeSet::new(),
        values: BTreeSet::new(),
    };
    for epoch in 0..30 {
        let row = share_fetch(
            &client,
            crate::support::share::ShareFetchSetup::at_epoch(
                &member,
                tid,
                crate::support::share::ShareSessionEpoch(epoch),
            ),
        )
        .await;
        let row_acquired: BTreeSet<i64> = row
            .acquired_records
            .iter()
            .flat_map(|range| range.first_offset..=range.last_offset)
            .collect();
        if let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2()) {
            for batch in batches {
                for record in &batch.records {
                    let offset = batch.base_offset + i64::from(record.offset_delta);
                    if let Some(value) = record.value.as_ref()
                        && row_acquired.contains(&offset)
                    {
                        seen.values
                            .insert(String::from_utf8_lossy(value).into_owned());
                    }
                }
            }
        }
        seen.acquired.extend(row_acquired);
        if seen.acquired.contains(&4) {
            break;
        }
        // intentional: bounded RPC poll for the LSO to pass the marker and the
        // plain record to become acquirable; no image or metric signals it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    producer.close().await.unwrap();
    broker.shutdown().await;
    seen
}

async fn enqueue_transaction_values(producer: &Producer) {
    for v in ["a", "b", "c"] {
        drop(
            producer
                .enqueue(crate::support::producer::producer_record(
                    crate::support::producer::ProducerRecordSetup {
                        topic: ("t").into(),
                        value: Some(bytes::Bytes::from(v.to_string())),
                        ..Default::default()
                    },
                ))
                .await
                .expect("record is queued"),
        );
    }
}
