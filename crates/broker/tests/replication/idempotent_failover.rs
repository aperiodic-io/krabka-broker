//! Idempotent-producer state across a leadership change.
//!
//! A Kafka follower appends every replicated batch through
//! `UnifiedLog.appendAsFollower`, and that append updates the producer state
//! exactly as a leader append does. A follower that becomes leader therefore
//! holds the last five batches of each producer up to its log end.
//! `ProducerStateEntry.findDuplicateBatch` finds a retry among them, and
//! `UnifiedLog.append` answers the retry with the offsets of the first append.
//!
//! The `ReplicationTest.test_replication_with_broker_failure` system test
//! (`clean_bounce`, `broker_type=leader`, `enable_idempotence=true`) found two
//! symptoms of one fault. The new leader appended a retry of a batch it had
//! already replicated, so the log held the batch twice. A broker that became
//! leader again refused the next batch with `OUT_OF_ORDER_SEQUENCE_NUMBER`,
//! because its producer state still held the sequence of its previous term.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BrokerConfig, BrokerHandle, codes};
use krabka_protocol::{
    owned::produce_response::{LeaderIdAndEpoch, PartitionProduceResponse},
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use tempfile::TempDir;

use crate::{
    support,
    support::{
        records::{batch_from_records, value_record},
        topics::create_topic_request,
    },
};

/// The idempotent producer of every batch in this module.
const PRODUCER_ID: i64 = 7_331;

/// The `max_timestamp` of every batch. The topic is a `CreateTime` topic, so a
/// duplicate answers with this value in `log_append_time_ms`: Kafka's
/// `UnifiedLog.append` copies `BatchMetadata.timestamp` there.
const BATCH_MAX_TIMESTAMP: i64 = 1_700_000_000_000;

/// The number of records in every batch.
const RECORDS_PER_BATCH: i32 = 3;

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// The batch at producer sequence `base_sequence`, with
/// [`RECORDS_PER_BATCH`] records. A retry sends the same batch again.
fn idempotent_batch(base_sequence: i32) -> RecordBatch {
    RecordBatch {
        last_offset_delta: RECORDS_PER_BATCH - 1,
        base_timestamp: BATCH_MAX_TIMESTAMP,
        max_timestamp: BATCH_MAX_TIMESTAMP,
        producer_id: PRODUCER_ID,
        producer_epoch: 0,
        base_sequence,
        ..batch_from_records(
            (0..RECORDS_PER_BATCH)
                .map(|offset_delta| {
                    value_record(
                        offset_delta,
                        Some(Bytes::from(format!("seq-{}", base_sequence + offset_delta))),
                    )
                })
                .collect(),
        )
    }
}

/// The row of a batch that the leader appended at `base_offset`.
fn appended(base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset: 0,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// The row of a retry that the leader recognized as a duplicate of the batch
/// at `base_offset`.
fn duplicate_of(base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        log_append_time_ms: BATCH_MAX_TIMESTAMP,
        ..appended(base_offset)
    }
}

/// Create `topic` with one partition on `replicas`, the first one leading, and
/// wait until every broker of `cluster` has it.
async fn create_topic(cluster: &Cluster, topic: &str, replicas: &[i32]) -> WireUuid {
    let admin = crate::support::client::connect_with_context(
        cluster[0].1.listen_addr.to_string(),
        None,
        "admin client",
    )
    .await;
    let response = admin
        .send(create_topic_request(support::topic_on(topic, &[replicas])))
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE);
    for (handle, _, _) in cluster {
        handle.wait_until_partition_present(topic, 0).await;
    }
    response.topics[0].topic_id
}

/// Send `batch` to partition 0 of `topic` with `acks=all`, and return the whole
/// partition row.
async fn produce(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let client = crate::support::client::connect_with_context(
        broker.listen_addr().to_string(),
        None,
        "producer client",
    )
    .await;
    let response = crate::support::produce::send_batch(
        &client,
        batch,
        crate::support::produce::SinglePartitionProduceSetup {topic: topic.to_owned(), topic_id, ..crate::support::produce::SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()},
    )
    .await;
    response.responses[0].partition_responses[0].clone()
}

/// Make broker `to` the leader of partition 0 of `topic` at the next leader
/// epoch, as an election does, and wait until it accepts produces.
async fn elect(cluster: &Cluster, topic: &str, to: u64) {
    let mut record = cluster[0]
        .0
        .partition_record_for_test(topic, 0)
        .expect("partition record");
    record.leader = krabka_metadata::NodeId(to);
    record.leader_epoch = record.leader_epoch.next();
    record.partition_epoch += 1;
    cluster[0]
        .0
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(record))
        .await
        .expect("elect a new leader");
    broker(cluster, to)
        .wait_until_local_partition_leader(topic, 0, krabka_raft::NodeId(to))
        .await;
}

/// The handle of broker `node_id`.
fn broker(cluster: &Cluster, node_id: u64) -> &BrokerHandle {
    &cluster
        .iter()
        .find(|(_, config, _)| config.node_id.0 == node_id)
        .expect("broker in cluster")
        .0
}

/// The base offset of every batch in the local log of each broker of
/// `cluster`, once each log ends at `log_end_offset`.
async fn batch_base_offsets(
    cluster: &Cluster,
    topic: &str,
    log_end_offset: i64,
) -> Vec<Option<Vec<i64>>> {
    let mut logs = Vec::new();
    for (handle, _, _) in cluster {
        handle
            .wait_until_local_log_end_offset(topic, 0, log_end_offset)
            .await;
        logs.push(handle.local_batch_base_offsets_for_test(topic, 0));
    }
    logs
}

/// The `clean_bounce` case of the system test. The leader appends a batch,
/// every follower replicates it, and then the leader stops with a controlled
/// shutdown before the producer reads the answer. The producer sends the batch
/// again to the new leader. The new leader must answer with the offset of the
/// first append and must not append the batch a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_elected_by_a_controlled_shutdown_answers_a_replicated_retry_as_a_duplicate() {
    let _g = crate::cluster_lock().lock().await;
    let mut cluster = crate::support::registered_cluster(3).await;

    // Do not stop the controller leader: it holds the only replica of an
    // internal partition, which a controlled shutdown cannot move.
    let controller = cluster[0].0.wait_until_controller_leader().await;
    let controller_idx = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == controller)
        .expect("controller leader in cluster");
    let leader_idx = (controller_idx + 1) % cluster.len();
    let leader = i32::try_from(cluster[leader_idx].1.node_id.0).expect("node id");
    let mut replicas = vec![leader];
    replicas.extend(
        cluster
            .iter()
            .map(|(_, config, _)| i32::try_from(config.node_id.0).expect("node id"))
            .filter(|node| *node != leader),
    );
    let topic = "idempotent-bounce";
    let topic_id = create_topic(&cluster, topic, &replicas).await;
    cluster[leader_idx]
        .0
        .wait_until_local_partition_leader(topic, 0, cluster[leader_idx].1.node_id)
        .await;

    let first = produce(&cluster[leader_idx].0, topic, topic_id, idempotent_batch(0)).await;
    let unanswered = produce(&cluster[leader_idx].0, topic, topic_id, idempotent_batch(3)).await;
    assert!((first, unanswered) == (appended(0), appended(3)));
    batch_base_offsets(&cluster, topic, 6).await;

    let (stopped, _, _stopped_dir) = cluster.remove(leader_idx);
    stopped
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("controlled shutdown drains the leader");
    let stopped_id = krabka_metadata::NodeId(u64::try_from(leader).expect("node id"));
    cluster[0]
        .0
        .wait_for_image(|image| {
            image
                .partition(topic, 0)
                .is_some_and(|record| record.leader != stopped_id)
        })
        .await;
    let new_leader = cluster[0]
        .0
        .partition_leader_for_test(topic, 0)
        .expect("new leader");
    let new_leader = broker(&cluster, new_leader);
    new_leader
        .wait_until_local_partition_leader(topic, 0, krabka_raft::NodeId(new_leader.node_id()))
        .await;

    let retry = produce(new_leader, topic, topic_id, idempotent_batch(3)).await;
    let older_retry = produce(new_leader, topic, topic_id, idempotent_batch(0)).await;
    let next = produce(new_leader, topic, topic_id, idempotent_batch(6)).await;
    let logs = batch_base_offsets(&cluster, topic, 9).await;

    assert!(
        (retry, older_retry, next, logs)
            == (
                duplicate_of(3),
                duplicate_of(0),
                appended(6),
                vec![Some(vec![0, 3, 6]); 2]
            )
    );

    crate::support::shutdown_cluster(cluster).await;
}

/// A broker that leads, follows, and leads again. While it follows, it
/// replicates a batch that another leader appended. When it leads again, a
/// retry of that batch is a duplicate, and the next batch follows it in
/// sequence: it is not out of order against the sequence of the first term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_elected_again_continues_from_the_batches_it_replicated() {
    let _g = crate::cluster_lock().lock().await;
    let cluster = crate::support::registered_cluster(3).await;
    let topic = "idempotent-reelection";
    let topic_id = create_topic(&cluster, topic, &[1, 2, 3]).await;
    cluster[0]
        .0
        .wait_until_local_partition_leader(topic, 0, krabka_raft::NodeId(1))
        .await;

    let first_term = produce(broker(&cluster, 1), topic, topic_id, idempotent_batch(0)).await;
    elect(&cluster, topic, 2).await;
    let other_term = produce(broker(&cluster, 2), topic, topic_id, idempotent_batch(3)).await;
    batch_base_offsets(&cluster, topic, 6).await;
    elect(&cluster, topic, 1).await;

    let next = produce(broker(&cluster, 1), topic, topic_id, idempotent_batch(6)).await;
    let retry = produce(broker(&cluster, 1), topic, topic_id, idempotent_batch(3)).await;
    let logs = batch_base_offsets(&cluster, topic, 9).await;

    assert!(
        (first_term, other_term, next, retry, logs)
            == (
                appended(0),
                appended(3),
                appended(6),
                duplicate_of(3),
                vec![Some(vec![0, 3, 6]); 3]
            )
    );

    crate::support::shutdown_cluster(cluster).await;
}
