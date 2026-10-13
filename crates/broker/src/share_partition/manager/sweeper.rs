//! The background acquisition-lock-timeout sweeper.
//!
//! The sweeper is the only caller that walks every live cell, so it is also the
//! one that has to snapshot the `DashMap` before it awaits. It sits apart from
//! the request path because it is the sole detached task the manager spawns.

use {
    super::{LeaderKey, SharePartitionLeaderManager},
    crate::share_partition::{group_settings::GroupShareSettings, state::AcquisitionState},
    std::{sync::Arc, time::Duration},
    tokio::sync::Mutex,
};

impl SharePartitionLeaderManager {
    /// Spawns the background acquisition-lock-timeout sweeper.
    ///
    /// The sweeper runs every `record_lock_duration / 2` of the broker
    /// setting, with a minimum of 100ms. A group whose
    /// `share.record.lock.duration.ms` is shorter still sees its locks run out
    /// on time for every request, because each acquire pass expires locks
    /// first; the sweeper only bounds how long an idle partition keeps them.
    /// A lock that runs out at the group's `share.delivery.count.limit`
    /// archives its records. On each run it snapshots the live cells. It clones their `Arc`s
    /// out of the `DashMap`, so it holds no guard across an `.await`. It then
    /// expires each timed-out lock and persists again the cells that changed.
    /// The sweeper runs detached for the lifetime of the broker.
    pub(crate) fn spawn_lock_sweeper(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let period = (mgr.config.record_lock_duration / 2).max(Duration::from_millis(100));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                // Snapshot keys + cells, releasing all DashMap guards first.
                let cells: Vec<(LeaderKey, Arc<Mutex<AcquisitionState>>)> = mgr
                    .leaders
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();
                let now = std::time::Instant::now();
                let image = mgr.controller.current_image();
                for ((group, topic_id, partition), cell) in cells {
                    let settings = GroupShareSettings::resolve(&image, &group, &mgr.config);
                    let mut st = cell.lock().await;
                    st.set_dlq_enabled(settings.dlq_enabled);
                    st.expire_locks(now, settings.delivery_count_limit);
                    if st.dirty {
                        // A failed write keeps the state dirty for the next tick.
                        let _ = mgr
                            .persist_if_dirty(&group, topic_id, partition, Some(&cell), &mut st)
                            .await;
                    }
                }
            }
        });
    }
}
