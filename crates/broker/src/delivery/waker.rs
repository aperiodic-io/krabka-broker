//! The re-arm handle of the broker-wide delivery scheduler.
//!
//! The scheduler sleeps until the earliest activation deadline it knows about.
//! A produce can land a batch that comes due before that instant, and the task
//! would then wake too late. It publishes the instant it sleeps on here, and the
//! partition writer compares each new deadline against that instant. Only a
//! sooner deadline wakes the task, so an ordinary append pays one atomic load.
//!
//! A missed wake costs promptness and never correctness. A fetch recomputes the
//! watermark under the log mutex, so it never serves a batch early and never
//! holds one back once its deadline has passed.

use {
    std::sync::atomic::{AtomicI64, Ordering},
    tokio::sync::Notify,
};

/// Lets a produce re-arm the delivery scheduler before its current sleep ends.
pub(crate) struct DeliveryWaker {
    /// Epoch-millisecond instant the scheduler next wakes on its own.
    /// [`i64::MAX`] until the task arms for the first time.
    wakes_at_ms: AtomicI64,
    notify: Notify,
}

impl DeliveryWaker {
    pub(crate) fn new() -> Self {
        Self {
            wakes_at_ms: AtomicI64::new(i64::MAX),
            notify: Notify::new(),
        }
    }

    /// Publish the instant the scheduler is about to sleep until.
    ///
    /// The task calls this before it arms the sleep, so a poke that arrives in
    /// between is compared against the value that is about to hold, and a
    /// [`Notify`] permit outlives the gap either way.
    pub(crate) fn arm(&self, wakes_at_ms: i64) {
        self.wakes_at_ms.store(wakes_at_ms, Ordering::Release);
    }

    /// The instant the scheduler wakes on its own now.
    pub(crate) fn wakes_at_ms(&self) -> i64 { self.wakes_at_ms.load(Ordering::Acquire) }

    /// Wake the scheduler when `deadline_ms` comes due before it would wake by
    /// itself. Reports whether it woke the task.
    pub(crate) fn wake_for(&self, deadline_ms: i64) -> bool {
        if deadline_ms >= self.wakes_at_ms() {
            return false;
        }
        self.notify.notify_one();
        true
    }

    /// Wait until a produce asks for an earlier deadline.
    pub(crate) async fn woken(&self) { self.notify.notified().await; }
}

impl Default for DeliveryWaker {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, std::sync::Arc};

    #[test]
    fn an_unarmed_waker_takes_every_deadline() {
        let waker = DeliveryWaker::new();
        check!(waker.wakes_at_ms() == i64::MAX);
        check!(waker.wake_for(i64::MAX - 1));
    }

    #[test]
    fn only_a_sooner_deadline_wakes_the_task() {
        let waker = DeliveryWaker::new();
        waker.arm(1_000);
        let cases = [(999, true), (1_000, false), (1_001, false)];
        for (deadline_ms, expected) in cases {
            check!(
                waker.wake_for(deadline_ms) == expected,
                "deadline {deadline_ms}"
            );
        }
    }

    #[tokio::test]
    async fn a_poke_that_lands_before_the_wait_still_wakes_it() {
        let waker = Arc::new(DeliveryWaker::new());
        waker.arm(1_000);
        check!(waker.wake_for(500));
        // The permit outlives the gap between the poke and the wait, so this
        // returns instead of parking forever.
        waker.woken().await;
    }
}
