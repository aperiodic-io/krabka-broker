//! Per-broker log-compaction ticker.
//!
//! Every `interval`, it walks the partitions registry and dispatches
//! [`Partition::compact_log`](crate::partition::Partition::compact_log) for
//! every partition where all of these hold:
//!
//!   - the topic's `cleanup.policy` is `compact`,
//!   - Kafka's cleanable test says a pass is due, and
//!   - no KFC-9 write freeze covers the topic.
//!
//! Leadership is not among them. Kafka's `LogCleanerManager` cleans every log
//! the broker hosts, so a follower replica of a compacted topic compacts its
//! own copy rather than growing without bound until it is elected.
//!
//! The compaction itself runs on the partition's writer actor, so appends,
//! replication and compaction run in sequence.
//!
//! This file holds the ticker and the configuration it reads. The sweep that
//! those three conditions describe lives in [`sweep`].

use std::{sync::Arc, time::Duration};

use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use self::sweep::{UncleanablePartitions, tick_all};
use crate::{metrics::BrokerMetrics, partition_registry::PartitionRegistry, time_util};

mod sweep;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

/// Tunables for [`run`], shared with the other local log-maintenance sweep.
pub(crate) type CleanerConfig = time_util::MetadataSweepConfig;

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
    metrics: BrokerMetrics,
) {
    // Drive the sweep cadence through the injected `Timer` (production: real
    // time; tests: a controlled manual timeline). A zero-duration first sleep
    // reproduces `tokio::time::interval`'s immediate t=0 tick, so the first sweep
    // fires at startup; each subsequent sleep is re-armed to `cfg.interval` only
    // after the sweep completes (`MissedTickBehavior::Delay` semantics — a slow
    // sweep never triggers a catch-up burst). Arming and completing a deadline
    // are both fallible, and a cleaner whose cadence is gone has nothing left to
    // do, so either failure ends the task rather than spinning on a timer that
    // cannot be armed. The timer is cloned into a local only to leave `cfg` free
    // for the sweep body; the tick future itself is `'static` and borrows
    // nothing.
    const TASK: &str = "log cleaner";
    let timer = Arc::clone(&cfg.timer);
    // Failed partitions persist across sweeps until compaction succeeds.
    let mut uncleanable = UncleanablePartitions::default();
    time_util::run_sweeps!(
        &*timer,
        (Duration::ZERO, cfg.interval.to_std()),
        &shutdown,
        TASK,
        {
            // Read one image at the start of a sweep, including its current freezes.
            let image = cfg.current_image();
            tick_all(&partitions, image.as_deref(), &metrics, &mut uncleanable).await;
        },
        {
            debug!("cleaner task shutting down");
        },
    );
}
