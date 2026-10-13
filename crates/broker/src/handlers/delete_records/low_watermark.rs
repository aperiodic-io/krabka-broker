//! The follower half of a `DeleteRecords` trim: the low watermark a row
//! answers, and the wait for the followers to reach the trim point.
//!
//! Kafka moves the log start offset on every replica. The leader trims its own
//! log, each follower raises its log start to the leader's from its next Fetch
//! response, and reports the new start in the Fetch after that. The leader's
//! `ReplicaManager.deleteRecords` parks the request in the `DeleteRecords`
//! purgatory until `Partition.lowWatermarkIfLeader` (the lowest log start of
//! the leader and every live follower) reaches the requested offset. A row
//! that does not get there within the request's `timeout_ms` answers
//! `REQUEST_TIMED_OUT` with the low watermark it had when the trim ran.
//!
//! Without the wait a client is told its records are gone while a follower
//! still holds them, and a leader change serves them again.

use {
    super::response::{error_partition_result, partition_result},
    crate::{
        broker::Broker, codes, handlers::offline_replicas::live_brokers, partition::Partition,
    },
    krabka_log::Offset,
    krabka_protocol::owned::delete_records_response::DeleteRecordsTopicResult,
    std::time::Duration,
};

/// How often a waiting request reads the followers' progress again. A
/// follower reports its new log start in its next Fetch, which a caught-up
/// follower sends at least every `replica.fetch.wait.max.ms`, so a finer poll
/// would only spend CPU.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The position of a response row: the index of its topic, then the index of
/// its partition within that topic.
#[derive(Debug, Clone, Copy)]
pub(super) struct RowPosition {
    pub(super) topic: usize,
    pub(super) partition: usize,
}

/// One response row that waits for the followers.
#[derive(Debug)]
pub(super) struct Waiting {
    /// The row's position: the topic index, then the partition index, in the
    /// response.
    pub(super) row: RowPosition,
    pub(super) topic: String,
    pub(super) partition: i32,
    /// The offset the low watermark has to reach.
    pub(super) required: i64,
}

/// Kafka's `Partition.lowWatermarkIfLeader` for `part`, which this broker
/// leads.
///
/// A follower is live when its broker is registered and not fenced, which is
/// Kafka's `MetadataCache.hasAliveBroker`.
pub(super) async fn current(broker: &Broker, part: &Partition) -> Offset {
    let image = broker.controller.current_image();
    let alive = live_brokers(broker, &image).await;
    // The assignment comes from the image: a broker that just became leader
    // publishes its leadership before it installs the replica set in
    // `ReplicaState`, and a trim in that gap must still wait for the
    // followers.
    let replicas = image
        .partition(&part.topic, part.index.get())
        .map(|record| record.replicas.clone())
        .unwrap_or_default();
    let leader_log_start = part.log_start_offset();
    part.replica_state.lock().await.low_watermark(
        broker.config.node_id,
        leader_log_start,
        &replicas,
        &alive,
    )
}

/// Wait until every row in `waiting` reaches its required offset, or until
/// `timeout_ms` passes, and write each row's final answer into `topics`.
///
/// A row that completes answers `NONE` and the low watermark it reached. A row
/// whose partition this broker no longer leads answers
/// `NOT_LEADER_OR_FOLLOWER`, and one whose partition is gone answers
/// `UNKNOWN_TOPIC_OR_PARTITION`, both with Kafka's `-1`. A row still waiting at
/// the deadline keeps the `REQUEST_TIMED_OUT` answer the trim gave it.
pub(super) async fn await_followers(
    broker: &Broker,
    topics: &mut [DeleteRecordsTopicResult],
    mut waiting: Vec<Waiting>,
    timeout_ms: i32,
) {
    let timeout = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
    let deadline = tokio::time::Instant::now() + timeout;
    while !waiting.is_empty() {
        let mut still_waiting = Vec::with_capacity(waiting.len());
        for row in waiting {
            let slot = &mut topics[row.row.topic].partitions[row.row.partition];
            let Some(part) = broker
                .partitions
                .get(&row.topic, krabka_ids::PartitionIndex(row.partition))
            else {
                *slot = error_partition_result(row.partition, codes::UNKNOWN_TOPIC_OR_PARTITION);
                continue;
            };
            if part
                .current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                != broker.config.node_id
            {
                *slot = error_partition_result(row.partition, codes::NOT_LEADER_OR_FOLLOWER);
                continue;
            }
            let low_watermark = current(broker, &part).await;
            if low_watermark.0 >= row.required {
                *slot = partition_result(row.partition, low_watermark.0, codes::NONE);
            } else {
                still_waiting.push(row);
            }
        }
        waiting = still_waiting;
        let now = tokio::time::Instant::now();
        if waiting.is_empty() || now >= deadline {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
}
