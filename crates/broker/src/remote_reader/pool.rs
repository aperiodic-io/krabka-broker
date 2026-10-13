//! The bounded reader pool that fronts every cold-tier read.
//!
//! Kafka gives `RemoteLogManager` a `remoteStorageReaderThreadPool` of
//! `remote.log.reader.threads` threads (10 by default) behind a queue of
//! `remote.log.reader.max.pending.tasks` (100 by default). When the queue is
//! full the executor throws `RejectedExecutionException` and
//! `ReplicaManager.processRemoteFetches` answers that partition with an error
//! instead of parking the fetch. That cap is what keeps a burst of cold-tier
//! consumers from consuming the resources local produce and fetch depend on.
//!
//! krabka's remote SPI calls run on the tokio blocking pool, which the WAL
//! fsync, the local fetch and the replica IO share, so the same cap is needed
//! here for the same reason. [`ReaderPool`] is that cap: a
//! [`Semaphore`](tokio::sync::Semaphore) of `threads` permits, plus a count of
//! the readers waiting for one. A read that would push the waiting count past
//! `max_pending_tasks` is rejected rather than queued.

use {
    std::sync::atomic::{AtomicUsize, Ordering},
    tokio::sync::{Semaphore, SemaphorePermit},
};

/// A read the pool refused because its pending queue was already full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReaderPoolRejected;

/// The concurrency cap and pending-task queue for cold-tier reads.
#[derive(Debug)]
pub(crate) struct ReaderPool {
    permits: Semaphore,
    /// Reads waiting for a permit. Not the same as `Semaphore::available_permits`:
    /// this is the queue Kafka bounds, and it excludes the running reads.
    pending: AtomicUsize,
    /// How many reads may run at once.
    threads: usize,
    /// How many reads may wait for a permit at once.
    max_pending_tasks: usize,
    /// Reads refused since startup, for the broker's rejection counter.
    rejected: AtomicUsize,
}

/// A permit held for the duration of one cold-tier read. Dropping it releases
/// the slot.
pub(crate) struct ReaderPermit<'pool> {
    _permit: SemaphorePermit<'pool>,
}

impl ReaderPool {
    /// A pool of `threads` concurrent reads with room for `max_pending_tasks`
    /// waiting ones.
    ///
    /// `threads` of zero would refuse every read, so it is raised to one; the
    /// config layer rejects zero before it reaches here, and this keeps a pool
    /// built in code from deadlocking a broker.
    pub(crate) fn new(threads: usize, max_pending_tasks: usize) -> Self {
        let threads = threads.max(1);
        Self {
            permits: Semaphore::new(threads),
            pending: AtomicUsize::new(0),
            threads,
            max_pending_tasks,
            rejected: AtomicUsize::new(0),
        }
    }

    /// A pool that never refuses and never waits, for the in-process tests
    /// that are not about the cap itself.
    #[cfg(test)]
    pub(crate) fn unbounded() -> Self { Self::new(Semaphore::MAX_PERMITS, usize::MAX) }

    /// Waits for a slot, or refuses when the pending queue is full.
    ///
    /// The pending count is incremented before the wait and decremented when
    /// the permit arrives, so it reads as the queue depth Kafka's
    /// `RemoteLogReaderTaskQueueSize` reports.
    pub(crate) async fn acquire(&self) -> Result<ReaderPermit<'_>, ReaderPoolRejected> {
        // Take the queue slot first: two readers that both see room must not
        // both get in when only one slot is left.
        let taken = self
            .pending
            .try_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending < self.max_pending_tasks).then_some(pending + 1)
            });
        if taken.is_err() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ReaderPoolRejected);
        }
        let permit = self.permits.acquire().await;
        self.pending.fetch_sub(1, Ordering::AcqRel);
        // The semaphore is never closed: nothing calls `close`, and the pool
        // outlives every read it hands a permit to. Refusing is the safe
        // answer if that ever changes.
        let Ok(permit) = permit else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(ReaderPoolRejected);
        };
        Ok(ReaderPermit { _permit: permit })
    }

    /// Reads waiting for a permit right now
    /// (Kafka's `RemoteLogReaderTaskQueueSize`).
    pub(crate) fn queue_size(&self) -> usize { self.pending.load(Ordering::Acquire) }

    /// The share of the pool's read slots that are free, as a percentage
    /// (Kafka's `RemoteLogReaderAvgIdlePercent`).
    ///
    /// Kafka reports a moving average of the pool's idle ratio. This is the
    /// instantaneous reading instead, because Prometheus averages a gauge over
    /// whatever window the query asks for and a second moving average inside
    /// the broker would only blur it.
    pub(crate) fn idle_percent(&self) -> f64 {
        let available =
            u32::try_from(self.permits.available_permits().min(self.threads)).unwrap_or(u32::MAX);
        // `new` raised `threads` to at least one, so this cannot divide by zero.
        let threads = u32::try_from(self.threads).unwrap_or(u32::MAX);
        f64::from(available) / f64::from(threads) * 100.0
    }

    /// Reads refused since startup because the pending queue was full.
    pub(crate) fn rejected(&self) -> usize { self.rejected.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests;
