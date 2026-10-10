//! One compaction sweep over the partition registry.
//!
//! The sweep tests every partition this broker hosts for a cleanup policy
//! containing `compact`, for Kafka's cleanable test over the partition's dirty
//! region, and for a KFC-9 write freeze. It dispatches
//! [`Partition::compact_log`] for the partitions that pass all three tests,
//! and it accounts each completed compaction, each failed one, and the sweep
//! itself in the metrics.
//!
//! Leadership is not one of the tests. Kafka's `LogCleanerManager` walks
//! every log the `LogManager` holds, exactly as `LogManager.cleanupLogs`
//! does, because compaction is a local operation on a replica: it rewrites
//! sealed segments in place and preserves every surviving record's offset, so
//! a follower that runs a pass stays byte-comparable with the leader at the
//! offsets that remain. Skipping followers left a compacted topic's follower
//! replicas with no bound on their segment count at all, since
//! `cleanup.policy=compact` sets no `retention.ms` for the local-retention
//! sweep to act on.
//!
//! A failure is accounted rather than logged and forgotten. Compaction and
//! local retention stop together on a compacted topic, so a cleaner that
//! fails every partition is a disk filling with no other signal: the failure
//! counter carries the partition and the reason, the uncleanable set carries
//! the partitions the cleaner has lost across sweeps, and the sweep counter
//! is left where it was so a failing pass never reports success.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use krabka_log::LogError;
use krabka_metadata::MetadataImage;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use krabka_verified::FreezeMutationKind;
use tracing::warn;

use crate::{
    error::BrokerError,
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    metrics::BrokerMetrics,
    partition::Partition,
    partition_registry::PartitionRegistry,
};

/// What keeps a partition on the uncleanable list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    /// A failure that a later pass may not repeat, such as a disk or a dead
    /// writer: the sweep runs the partition again.
    Retryable,
    /// A compressed record above `max.decompressed.message.bytes`. The cleaner
    /// refuses the same batch on every pass while the limit stays what it was,
    /// and each pass reads and decodes the whole consumed range before it gets
    /// there, so the sweep leaves the partition alone until the limit changes.
    /// `limit` is the one that refused it.
    RecordTooLarge { limit: usize },
}

impl Cause {
    fn of(error: &BrokerError) -> Self {
        match error {
            BrokerError::Log(LogError::RecordTooLarge { limit, .. }) => {
                Self::RecordTooLarge { limit: *limit }
            }
            _ => Self::Retryable,
        }
    }
}

/// The partitions whose most recent compaction attempt failed, carried from
/// one sweep to the next.
///
/// Kafka's `LogCleanerManager` keeps the same set, and for the same reason: a
/// per-sweep count would report zero on the next sweep that finds the
/// partition ineligible. The set is keyed by the partition's identity rather
/// than by an `Arc<Partition>` so a partition this broker stops hosting leaves
/// it.
///
/// Trunk's cleaner never runs a marked partition again until it leaves the log
/// manager, whatever failed. krabka does that for the one failure that cannot
/// change without an operator, a record above the decompressed-size limit: the
/// sweep skips the partition while the limit is the one that refused it, and
/// the first sweep after the operator changes the limit runs it again, where
/// trunk needs a broker restart. Every other failure stays marked until a pass
/// succeeds, and the sweep retries it each time, because a disk that comes
/// back needs no operator step.
#[derive(Debug, Default)]
pub(crate) struct UncleanablePartitions {
    inner: BTreeMap<(String, i32), Cause>,
}

impl UncleanablePartitions {
    /// How many partitions the cleaner currently cannot clean.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the sweep must leave `key` alone: a pass on it would refuse the
    /// same record again, because `record_limit` is still the limit that
    /// refused it.
    fn is_held(&self, key: &(String, i32), record_limit: Option<ByteSize>) -> bool {
        matches!(
            self.inner.get(key),
            Some(Cause::RecordTooLarge { limit })
                if record_limit.map(ByteSizeExt::bytes_usize) == Some(*limit)
        )
    }

    /// Drop every entry that `swept` does not name, which is what releases a
    /// partition this broker no longer hosts or no longer compacts.
    fn retain_swept(&mut self, swept: &BTreeSet<(String, i32)>) {
        self.inner.retain(|key, _| swept.contains(key));
    }
}

use crate::metrics::cleaner_failure_reason as failure_reason;

#[cfg(test)]
mod tests;

/// Whether a KFC-9 write freeze stops this sweep from compacting `topic`.
///
/// Compaction removes records, and the KFC's rule refuses every operation that
/// removes data from a frozen topic's log. A disaster-recovery promotion needs
/// the frozen prefix byte-identical between the two sites, and one cleaner run
/// on one side leaves the same offsets holding different bytes.
///
/// `image` is `None` for a sweep with no metadata authority to ask, which
/// resolves no freeze.
fn freeze_stops_compaction(image: Option<&MetadataImage>, topic: &str) -> bool {
    image.is_some_and(|image| {
        matches!(
            resolve_freeze_mutation(image, topic, true, FreezeMutationKind::Compaction),
            FreezeMutationResolution::Frozen(_)
        )
    })
}

pub(crate) async fn tick_all(
    partitions: &PartitionRegistry,
    image: Option<&MetadataImage>,
    metrics: &BrokerMetrics,
    uncleanable: &mut UncleanablePartitions,
) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    // Every partition this sweep considered its work, whether or not it was
    // due a pass. What is not in it is not this cleaner's any more, so it
    // leaves the uncleanable set at the end of the sweep.
    let mut swept: BTreeSet<(String, i32)> = BTreeSet::new();
    let mut failed = false;
    for partition in snapshot {
        // Kafka bounds a pass at the last stable offset, not the high
        // watermark: the records between the LSO and the high watermark
        // belong to transactions with no marker yet, and rewriting them
        // early would leave the output segment's aborted-transaction index
        // unable to learn about a marker that arrives later. A follower
        // learns the high watermark from the leader's Fetch response, so the
        // LSO computed from it is one every replica can derive.
        let high_watermark = partition.high_watermark().await;
        let (compacted, record_limit, due) = {
            // Recover the guard if the mutex was poisoned by a panic
            // elsewhere rather than killing the (discarded-JoinHandle)
            // cleaner task. The config snapshot stays readable.
            let mut log = partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // A policy list containing `compact` is what makes a partition the
            // cleaner's work, so `compact,delete` is swept exactly as `compact`
            // is; the delete half is the local-retention sweep's, in
            // `crate::log_retention`, which runs on its own interval and over
            // every hosted log rather than only the led ones.
            let config = log.config_snapshot();
            let compacted = config.cleanup_policy.contains_compact();
            let last_stable_offset = log.last_stable_offset(high_watermark);
            (
                compacted,
                config.max_decompressed_record,
                // Kafka's `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`
                // and `max.compaction.lag.ms`, which say whether this
                // partition has earned a pass yet.
                compacted && log.compaction_due(std::time::SystemTime::now(), last_stable_offset),
            )
        };
        if !compacted {
            continue;
        }
        // The cleanup policy is what makes a partition this cleaner's work.
        // A partition that is merely not due yet keeps the uncleanable mark
        // an earlier failure left on it, because the failure is still there;
        // only a pass that succeeds clears it.
        let key = (partition.topic.clone(), partition.index.get());
        swept.insert(key.clone());
        if !due {
            continue;
        }
        // KFC-9, beside the policy test and skipping in exactly the same way.
        // A skip is the right shape here rather than an error: the cleaner is
        // a background loop with no caller to refuse, so a frozen topic simply
        // has no work for it, and the partition becomes eligible again on the
        // first sweep after the thaw leaves the image, with no operator step.
        if freeze_stops_compaction(image, &partition.topic) {
            continue;
        }
        // A record above the decompressed-size limit refuses the same batch on
        // every pass, so a partition it already refused waits for the limit to
        // change. The refusal was accounted when it happened, the gauge still
        // counts the partition, and a sweep that skips it has failed nothing.
        if uncleanable.is_held(&key, record_limit) {
            continue;
        }
        match partition.compact_log().await {
            Ok(()) => {
                metrics.record_compaction(&partition.topic, partition.index.get());
                uncleanable.inner.remove(&key);
            }
            Err(e) => {
                let reason = failure_reason(&e);
                warn!(
                    topic = %partition.topic,
                    partition_id = partition.index.get(),
                    reason = reason.as_str(),
                    error = %e,
                    "compaction failed for partition",
                );
                metrics.record_cleaner_failure(&partition.topic, partition.index.get(), reason);
                uncleanable.inner.insert(key, Cause::of(&e));
                failed = true;
            }
        }
    }
    // A partition the sweep no longer considers its work — this broker
    // stopped hosting the replica, or the topic stopped being compacted — is
    // no longer uncleanable, so the gauge falls without waiting for a pass to
    // succeed on a partition that will never get one.
    uncleanable.retain_swept(&swept);
    metrics.set_uncleanable_partitions(uncleanable.len());
    if failed {
        // The sweep ran and did not do what it is for. Counting it as a pass
        // is what made a cleaner failing every partition on a dying disk
        // report the same rate as a healthy one.
        return;
    }
    // One increment per clean sweep, whether or not any partition was
    // eligible, so a test that seals a segment can poll this counter to
    // confirm a full pass ran after the seal (see `wait_for_metrics`).
    metrics.record_cleaner_run();
}
