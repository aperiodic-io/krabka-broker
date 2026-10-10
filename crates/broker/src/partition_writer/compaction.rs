//! The writer's Compact arm.
//!
//! A pass keeps the last record of each active producer, so that the state of
//! the producer survives the rewrite. [`krabka_log::Log::compact`] reads those records
//! from the producer state of the log, which a leader and a follower both
//! update for each batch that they append. This is Kafka's
//! `Cleaner.cleanSegments`, which reads `UnifiedLog.lastRecordsOfActiveProducers`
//! on every replica. The tracker of the produce path does not take part: on a
//! follower it does not hold the replicated data batches.

use super::storage::{LogStorage, MaintenanceAck, maintain_log};
use crate::replica_state::ReplicaState;

pub(super) async fn handle_compact(
    storage: LogStorage<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: MaintenanceAck,
) {
    // The pass is bounded at the last stable offset, not the high watermark:
    // Kafka's `LogCleanerManager.cleanableOffsets` takes `lastStableOffset`,
    // because the records between the LSO and the high watermark belong to
    // transactions with no marker yet. Rewriting them before the commit or
    // abort is known would leave the output segment's aborted-transaction
    // index unable to learn about a marker that arrives later. The watermark
    // is read inside the writer actor, which is the only task that moves the
    // log, so no append or truncation can land between the read and the
    // rewrite.
    maintain_log(
        storage,
        replica_state,
        ack,
        "compact task panicked",
        |log, now, high_watermark| {
            let context = krabka_log::CompactionContext {
                now,
                last_stable_offset: log.last_stable_offset(high_watermark),
            };
            log.compact(&context)
        },
    )
    .await;
}
