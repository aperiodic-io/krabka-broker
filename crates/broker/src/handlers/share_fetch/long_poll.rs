//! The long poll that a `ShareFetch` runs when its first acquire pass took
//! nothing and the client asked to wait.
//!
//! It parks on the append and HW-advance notifies of the partitions this
//! broker leads, so a second pass only runs once there is something new to
//! see, or the client's `max_wait_ms` expired.

use {
    super::pending::PendingPartition,
    crate::broker::Broker,
    std::{sync::Arc, time::Duration},
    tokio::sync::Notify,
};

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Parks on the append and HW-advance notifies of the partitions that this
/// broker can lead, under a single timeout. It mirrors the wait construction
/// in `fetch::long_poll_then_reread`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LongPollOutcome {
    NoPartitions,
    Notified,
    TimedOut,
}

/// Arms one waiter on every notify a `ShareFetch` can be woken by, before the
/// first acquire pass runs.
///
/// The arming is the whole point of the split: every producer path signals
/// with `notify_waiters`, which wakes only the waiters already registered and
/// leaves no permit behind. A waiter armed after the acquire pass would miss
/// a record produced during it, and the request would then sleep out its whole
/// `max_wait_ms` with records sitting in the log.
pub(super) fn arm_waits(broker: &Broker, pending: &[PendingPartition]) -> Vec<WaitFut> {
    let notifies = pending
        .iter()
        .filter(|partition| partition.leadable)
        .filter(|partition| partition.fetchable)
        .filter_map(|partition| {
            partition.topic_name.as_deref().and_then(|name| {
                broker
                    .partitions
                    .get(name, krabka_ids::PartitionIndex(partition.partition_index))
            })
        })
        .flat_map(|partition| {
            [
                partition.append_notify.clone(),
                partition.hw_advance_notify.clone(),
            ]
        })
        .collect();
    armed(notifies)
}

fn armed(notifies: Vec<Arc<Notify>>) -> Vec<WaitFut> {
    notifies
        .into_iter()
        .map(|notify| {
            let mut wait = Box::pin(notify.notified_owned());
            wait.as_mut().enable();
            wait as WaitFut
        })
        .collect()
}

pub(super) async fn long_poll(waits: Vec<WaitFut>, max_wait_ms: i32) -> LongPollOutcome {
    let max_wait = Duration::from_millis(u64::from(u32::try_from(max_wait_ms).unwrap_or(0)));
    wait_for_notifications(waits, max_wait).await
}

async fn wait_for_notifications(waits: Vec<WaitFut>, max_wait: Duration) -> LongPollOutcome {
    if waits.is_empty() {
        return LongPollOutcome::NoPartitions;
    }
    match tokio::time::timeout(max_wait, futures_util::future::select_all(waits)).await {
        Ok(_) => LongPollOutcome::Notified,
        Err(_) => LongPollOutcome::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[tokio::test(start_paused = true)]
    async fn notification_wait_reports_empty_wakeup_and_timeout() {
        assert!(
            wait_for_notifications(Vec::new(), Duration::from_secs(1)).await
                == LongPollOutcome::NoPartitions
        );

        let notified = Arc::new(Notify::new());
        notified.notify_one();
        assert!(
            wait_for_notifications(armed(vec![notified]), Duration::from_secs(1)).await
                == LongPollOutcome::Notified
        );

        assert!(
            wait_for_notifications(armed(vec![Arc::new(Notify::new())]), Duration::from_secs(1))
                .await
                == LongPollOutcome::TimedOut
        );
    }

    /// `notify_waiters` wakes only the waiters that are already registered, so
    /// a `ShareFetch` that arms its waiters after the acquire pass misses a
    /// record produced during it. Arming before the pass is what makes the
    /// notification that lands in that window still count.
    #[tokio::test(start_paused = true)]
    async fn a_notification_between_arming_and_parking_still_wakes_the_poll() {
        let notify = Arc::new(Notify::new());
        let waits = armed(vec![Arc::clone(&notify)]);

        // The window the arming closes: this fires while the acquire pass is
        // still running, with nothing parked on the notify yet.
        notify.notify_waiters();

        assert!(
            wait_for_notifications(waits, Duration::from_secs(30)).await
                == LongPollOutcome::Notified
        );
    }
}
