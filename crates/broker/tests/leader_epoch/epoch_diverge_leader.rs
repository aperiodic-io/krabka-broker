//! The leader side of KIP-320: a follower `Fetch` that advertises a
//! `last_fetched_epoch` whose epoch ended before the requested `fetch_offset`
//! is answered with a `diverging_epoch` and no records.
//!
//! The leader-side answer is computed from the epoch cache alone, so this test
//! needs only the single-broker fixture. The follower that acts on the answer
//! is covered in `epoch_diverge_follower`.

use assert2::{assert, check};
use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, ReplicaState};

use crate::{
    epoch_harness::{boot_single, create_topic, record, topic_id_for},
    support::{
        client::connect_client,
        fetch::{fetch_partition, single_partition_fetch},
    },
};

/// KIP-320 leader side. A Fetch from an assigned follower that
/// advertises a stale `last_fetched_epoch` whose epoch ends *before* the
/// requested `fetch_offset` must get a `diverging_epoch` that points at the
/// epoch boundary, and NO records.
///
/// The test builds the leader's epoch history deterministically:
///   * produce `k = 2` records at epoch 0, which gives checkpoint `0 -> 0`,
///   * commit leader epoch 1 in metadata, then produce 2 more, which gives
///     checkpoint `1 -> 2`.
///
/// The cache is then `e0 -> [0, 2)` and `e1 -> [2, 4)`, and the log end is 4.
///
/// A follower fetch at `fetch_offset = 4` with `last_fetched_epoch = 0` says
/// "my last record was in epoch 0, give me offset 4". That is divergent,
/// because epoch 0 on this leader ends at offset 2, not 4. The leader's
/// `epoch_and_offset_for(0, 4)` returns `(0, 2)`. The recorded end, 2, is below
/// the fetch offset, 4, so the handler answers with
/// `diverging_epoch { epoch: 0, end_offset: 2 }` and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diverging_epoch_returned_on_stale_last_fetched_epoch() {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "diverge").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "diverge").await;

    // Helper: produce one single-record batch, stamped with whatever leader
    // epoch the partition currently holds.
    let produce_one = |value: &'static str| {
        let client = &client;
        async move {
            client
                .send(crate::support::produce::batch_request(
                    record(value),
                    crate::support::produce::SinglePartitionProduceSetup {
                        topic: ("diverge").into(),
                        topic_id,
                        ..Default::default()
                    },
                ))
                .await
                .expect("produce");
        }
    };

    // Epoch 0: produce k = 2 records → checkpoint row `0 0`, LEO = 2.
    let e0: i32 = 0;
    let k: i64 = 2;
    produce_one("e0-a").await;
    produce_one("e0-b").await;

    // Keep metadata and the local epoch in agreement: a later supervisor
    // reconcile must not reinstall epoch 0 and erase this history. Declare
    // follower 7 in the replica set so the Fetch exercises follower admission.
    let mut partition = broker
        .partition_record_for_test("diverge", 0)
        .expect("partition metadata");
    partition.leader_epoch = krabka_metadata::LeaderEpoch(1);
    partition.replicas.push(krabka_broker::NodeId(7));
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(partition))
        .await
        .expect("promote to epoch 1");
    let checkpoint_path = dir.path().join("diverge-0").join("leader-epoch-checkpoint");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if std::fs::read_to_string(&checkpoint_path)
                .is_ok_and(|checkpoint| checkpoint.lines().any(|line| line == "1 2"))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("supervisor records epoch 1 at offset 2");

    // Produce 2 more → LEO = 4.
    produce_one("e1-a").await;
    produce_one("e1-b").await;
    let n: i64 = 4;

    // Sanity: the leader really advanced to LEO == n.
    let leo = broker
        .local_log_end_offset("diverge", 0)
        .expect("local leo");
    assert!(leo == n, "expected leader LEO == {n}, got {leo}");

    // Follower Fetch at offset n claiming last_fetched_epoch == e0. Leave
    // `current_leader_epoch` at its -1 default so we don't trip the KIP-101
    // fence and actually reach the KIP-320 divergence check.
    let resp = client
        .send(FetchRequest {
            replica_id: 7,
            replica_state: ReplicaState {
                replica_id: 7,
                ..Default::default()
            },
            ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
                topic: "diverge".into(),
                topic_id,
                partition: FetchPartition {
                    last_fetched_epoch: e0,
                    ..fetch_partition(crate::support::fetch::FetchPartitionSetup {
                        offset: krabka_ids::Offset(n),
                        ..Default::default()
                    })
                },
                limits: crate::support::fetch::FetchLimits::one_mebibyte(
                    crate::support::fetch::RequestWaitMillis(100),
                ),
            })
        })
        .await
        .expect("fetch");

    let part = &resp.responses[0].partitions[0];
    // NONE error code: divergence is reported in-band, not as an error.
    check!(
        part.error_code == 0,
        "expected NONE, got {}",
        part.error_code
    );
    check!(
        part.diverging_epoch.end_offset == k,
        "diverging_epoch.end_offset should be the epoch-0 boundary {k}, got {}",
        part.diverging_epoch.end_offset
    );
    check!(
        part.diverging_epoch.epoch == e0,
        "diverging_epoch.epoch should be {e0}, got {}",
        part.diverging_epoch.epoch
    );
    // No records are served alongside a divergence signal.
    check!(
        part.records.is_none()
            || part
                .records
                .as_ref()
                .and_then(|r| r.as_v2())
                .is_none_or(<[_]>::is_empty),
        "diverging fetch must serve no records"
    );

    broker.shutdown().await;
}
