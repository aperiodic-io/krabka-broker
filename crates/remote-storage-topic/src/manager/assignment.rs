//! The metadata-partition assignment reconciler and the read gate it feeds.
//!
//! The broker drives which `__remote_log_metadata` partitions this manager
//! consumes, and every gated read consults the readiness state that the same
//! reconciler records. Both halves share the `ready_targets` map and the
//! HWM-unknown sentinel, so they belong in one module.

use {
    super::TopicBasedRemoteLogMetadataManager, crate::log::PartitionStart,
    krabka_remote_storage::RemoteStorageError, tracing::warn,
};

#[cfg(test)]
mod tests;

/// Sentinel target HWM that means "this partition is assigned but its real
/// high-water mark is not yet known". That happens when the
/// `high_water_marks` RPC failed, or when the partition had no entry in the
/// returned index. The gate treats the sentinel as `NotReady`, because a real
/// applied offset can never reach `i64::MAX`. The next `reconcile_assignment`
/// retries the HWM fetch and replaces the sentinel with the real target.
const HWM_UNKNOWN: i64 = i64::MAX;

/// Outcome of the per-metadata-partition readiness check that gates the
/// gated [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager)
/// read methods
/// ([`RemoteLogMetadataManager::remote_log_segment_metadata`](krabka_remote_storage::RemoteLogMetadataManager::remote_log_segment_metadata),
/// [`RemoteLogMetadataManager::list_remote_log_segments`](krabka_remote_storage::RemoteLogMetadataManager::list_remote_log_segments),
/// [`RemoteLogMetadataManager::list_remote_log_segments_by_epoch`](krabka_remote_storage::RemoteLogMetadataManager::list_remote_log_segments_by_epoch),
/// [`RemoteLogMetadataManager::highest_offset_for_epoch`](krabka_remote_storage::RemoteLogMetadataManager::highest_offset_for_epoch)).
pub enum ReadGate {
    /// This broker does not consume the metadata partition, because it
    /// neither leads nor follows any covered user-partition. Answer
    /// a miss (`Ok(None)` or an empty list).
    Unassigned,
    /// Assigned, but the consumer pump has not reached the assignment-time
    /// HWM. Answer with the retryable `Err(NotReady)`.
    NotReady,
    /// Assigned and caught up. Delegate to the inner cache.
    Ready,
}

impl TopicBasedRemoteLogMetadataManager {
    /// The read decision for metadata partition `mp`, used to gate
    /// [`RemoteLogMetadataManager::remote_log_segment_metadata`](krabka_remote_storage::RemoteLogMetadataManager::remote_log_segment_metadata).
    pub(super) fn metadata_partition_gate(&self, mp: i32) -> ReadGate {
        let target = {
            let guard = self.ready_targets.lock().expect("ready_targets poisoned");
            match guard.get(&mp) {
                Some(&t) => t,
                // Not assigned: this broker neither leads nor follows any
                // user-partition in `mp`, so it must not answer from any
                // stale cache it happened to consume earlier. A genuine
                // miss — `Ok(None)`, NOT `NotReady`.
                None => return ReadGate::Unassigned,
            }
        };
        if target == 0 {
            return ReadGate::Ready; // empty partition: nothing to catch up to
        }
        // A sentinel target means the real HWM is not yet known (the
        // assignment-time fetch failed); the partition is assigned but the
        // answer is unknown → retryable, never a false `Ok(None)`.
        if target == HWM_UNKNOWN {
            return ReadGate::NotReady;
        }
        let Ok(idx) = usize::try_from(mp) else {
            // Defensive: a metadata partition index that doesn't fit in
            // usize is nonsensical, but if it ever happens we must NOT
            // fail open into `Ready` (which would serve a possibly-stale
            // or false-miss answer). Treat it as still catching up.
            return ReadGate::NotReady;
        };
        let applied = self.applied.lock().expect("applied mutex poisoned");
        if idx < applied.len() && applied[idx] >= target - 1 {
            ReadGate::Ready
        } else {
            ReadGate::NotReady
        }
    }

    /// `true` when metadata partition `mp` is assigned and caught up to its
    /// assignment-time HWM. Tests use this to poll for catch-up.
    #[cfg(test)]
    pub(super) fn metadata_partition_ready(&self, mp: i32) -> bool {
        matches!(self.metadata_partition_gate(mp), ReadGate::Ready)
    }

    /// The metadata partitions this manager is currently assigned (tracked
    /// for readiness). Sorted ascending.
    #[must_use]
    /// # Panics
    /// Panics if the assignment-readiness mutex is poisoned.
    pub fn assigned_metadata_partitions(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }

    /// Diff `desired` against the current assignment and drive the
    /// [`AssignmentHandle`](crate::log::AssignmentHandle).
    ///
    /// This method adds newly-needed partitions, seeded from the snapshot
    /// committed offset + 1, or from 0 when there is no committed event. It
    /// removes partitions that are no longer needed. It records each added
    /// partition's assignment-time HWM, so reads gate on `NotReady` until the
    /// pump catches up.
    ///
    /// An HWM-fetch failure fails CLOSED. This method records a partition
    /// whose real high-water mark it could not get with the `HWM_UNKNOWN`
    /// sentinel target, so the gate returns the retryable `NotReady` and
    /// never a false `Ok(None)`. Every subsequent reconcile retries such
    /// partitions, and the broker drives a reconcile on each image change and
    /// each reconciler tick. A transient `high_water_marks` failure therefore
    /// self-heals: the real target replaces the sentinel as soon as the fetch
    /// succeeds.
    ///
    /// A SINGLE task MUST drive this method. It is not internally serialized.
    /// It interleaves `.await` points with reads and writes of the
    /// `ready_targets` map under short, non-overlapping locks, so two
    /// concurrent callers could race the add, remove, and refresh logic.
    /// Correctness depends on the broker invoking it from exactly one
    /// reconciler task.
    ///
    /// It is async because it reads the log's high-water marks. The broker
    /// calls it from its reconciler task on the runtime, never from a
    /// `spawn_blocking` thread.
    /// # Panics
    /// Panics if the assignment-readiness mutex is poisoned.
    pub async fn reconcile_assignment(&self, desired: &[i32]) {
        use std::collections::HashSet;
        let want: HashSet<i32> = desired.iter().copied().collect();
        // Snapshot the current per-partition targets so we can both diff the
        // assigned set and find partitions still carrying the HWM-unknown
        // sentinel (which need a refresh). Lock released before the `.await`.
        let current: std::collections::HashMap<i32, i64> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .clone();
        let have: HashSet<i32> = current.keys().copied().collect();

        let needs_add = want.difference(&have).copied().collect::<Vec<_>>();
        // Partitions still assigned (in want) whose recorded target is the
        // sentinel: their HWM is still unknown, so re-attempt the fetch.
        let needs_refresh = want
            .iter()
            .copied()
            .filter(|mp| current.get(mp) == Some(&HWM_UNKNOWN))
            .collect::<Vec<_>>();

        // One HWM snapshot covers both additions and sentinel refreshes.
        let needs_hwm = !needs_add.is_empty() || !needs_refresh.is_empty();
        let hwms = if needs_hwm {
            match self.log.high_water_marks().await {
                Ok(h) => Some(h),
                Err(e) => {
                    warn!(error = ?e, "topic-based RLMM: high_water_marks fetch failed; \
                          assigned partitions gate NotReady until a later reconcile refreshes");
                    None
                }
            }
        } else {
            None
        };

        // Resolve a partition's target HWM from the (maybe-missing) snapshot.
        // A failed fetch (`None`) or a missing per-partition entry both yield
        // the sentinel so the gate stays NotReady — never fail open to 0.
        let target_for = |mp: i32| -> i64 {
            match &hwms {
                Some(h) => usize::try_from(mp)
                    .ok()
                    .and_then(|i| h.get(i).copied())
                    .unwrap_or(HWM_UNKNOWN),
                None => HWM_UNKNOWN,
            }
        };

        for mp in needs_add {
            // `committed_offset` is `-1` when there is no committed event
            // (full replay), so `+ 1` lands on the resume start offset (0).
            let start_offset = Self::resume_start_offset(self.committed_offset(mp))
                .expect("snapshot committed offsets were validated before manager startup");
            self.assignment.add(PartitionStart {
                partition: mp,
                start_offset,
            });
            // Assign-but-NotReady when the HWM is unknown: the broker DOES
            // own this partition, so leaving it Unassigned would wrongly
            // return Ok(None). The sentinel makes the gate return NotReady.
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .insert(mp, target_for(mp));
        }
        // Replace the sentinel for already-assigned partitions whose HWM is
        // now known (the partition stays assigned; only its target changes).
        for mp in needs_refresh {
            let target = target_for(mp);
            if target != HWM_UNKNOWN {
                let mut guard = self.ready_targets.lock().expect("ready_targets poisoned");
                // Only refresh if still assigned with the sentinel (a
                // concurrent remove would have dropped it — see the
                // single-task contract above).
                if guard.get(&mp) == Some(&HWM_UNKNOWN) {
                    guard.insert(mp, target);
                }
            }
        }
        for mp in have.difference(&want).copied() {
            let mut waiters = self.mutation_waiters.lock().await;
            self.assignment.remove(mp);
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .remove(&mp);
            let keys: Vec<_> = waiters
                .keys()
                .copied()
                .filter(|key| key.partition == mp)
                .collect();
            for key in keys {
                if let Some(reply) = waiters.remove(&key) {
                    let _ = reply.send(Err(RemoteStorageError::NotReady { partition: mp }));
                }
            }
        }
    }
}
