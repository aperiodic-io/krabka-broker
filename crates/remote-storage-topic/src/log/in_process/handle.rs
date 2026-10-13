//! The [`AssignmentHandle`] that [`InProcessMetadataEventLog::subscribe`]
//! returns, which mutates one live subscription's assigned partition set.
//!
//! Adding a partition mid-stream injects that partition's backlog into the
//! subscription's FIFO and marks the cursor `via_inject`, so the live records
//! that follow queue behind the backlog rather than race it. Dropping the
//! handle evicts the subscription state from the log.
//!
//! [`InProcessMetadataEventLog::subscribe`]: super::InProcessMetadataEventLog

use {
    super::{InProcessInner, PartitionCursor},
    crate::log::{AssignmentHandle, PartitionStart},
    std::sync::Arc,
};

pub(super) struct InProcessAssignmentHandle {
    pub(super) inner: Arc<InProcessInner>,
    pub(super) sub_id: u64,
}

impl Drop for InProcessAssignmentHandle {
    fn drop(&mut self) {
        // Evict this subscription's state so the map does not grow
        // without bound as subscriptions come and go. The stream's live
        // filter holds its own `Arc<SubscriptionState>`, so dropping the
        // map entry never affects an in-flight stream — only `add`/
        // `remove`/`assigned` (which go through the handle) stop working,
        // and the handle is gone.
        if let Ok(mut subs) = self.inner.subscriptions.lock() {
            subs.remove(&self.sub_id);
        }
    }
}

impl AssignmentHandle for InProcessAssignmentHandle {
    fn add(&self, start: PartitionStart) {
        let subs = self
            .inner
            .subscriptions
            .lock()
            .expect("metadata-log subscriptions mutex poisoned");
        let Some(state) = subs.get(&self.sub_id).cloned() else {
            return;
        };
        drop(subs);
        // Hold the log lock so the backlog snapshot + the assigned
        // insert bracket every concurrent publish exactly once: a
        // publish either lands in the snapshot we inject here, or it is
        // forwarded live (because `assigned` already contains it).
        let log = self.inner.log.lock().expect("metadata-log mutex poisoned");
        let mut assigned = state.assigned.lock().expect("assigned mutex poisoned");
        if assigned.contains_key(&start.partition) {
            return; // already assigned: no-op
        }
        let idx = match usize::try_from(start.partition) {
            Ok(i) if i < log.len() => i,
            _ => return, // out of range: ignore
        };
        let records = &log[idx];
        let begin = usize::try_from(start.start_offset.max(0)).unwrap_or(usize::MAX);
        for record in records.iter().skip(begin) {
            let _ = state.inject.send(record.clone());
        }
        // Live records at or after the current end are forwarded by the
        // broadcast path once `assigned` contains the partition. They are
        // routed through `inject` (via_inject) so they queue *behind* the
        // backlog we just pushed above, preserving per-partition publish
        // order: stream::select must not interleave a live record ahead of
        // undrained backlog.
        let next_live = i64::try_from(records.len()).expect("len fits in i64");
        assigned.insert(start.partition, PartitionCursor {
            next: next_live,
            via_inject: true,
        });
    }

    fn remove(&self, partition: i32) {
        let subs = self
            .inner
            .subscriptions
            .lock()
            .expect("metadata-log subscriptions mutex poisoned");
        if let Some(state) = subs.get(&self.sub_id) {
            state
                .assigned
                .lock()
                .expect("assigned mutex poisoned")
                .remove(&partition);
        }
    }

    fn assigned(&self) -> Vec<i32> {
        let subs = self
            .inner
            .subscriptions
            .lock()
            .expect("metadata-log subscriptions mutex poisoned");
        let Some(state) = subs.get(&self.sub_id) else {
            return Vec::new();
        };
        let mut v: Vec<i32> = state
            .assigned
            .lock()
            .expect("assigned mutex poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::log::{InProcessMetadataEventLog, MetadataEventLog},
        assert2::{assert, check},
        bytes::Bytes,
        futures_util::StreamExt,
    };

    #[tokio::test]
    async fn add_mid_stream_delivers_backlog_then_live() {
        let log = InProcessMetadataEventLog::new(2);
        // Three backlog records on partition 1 (offsets 0,1,2).
        for v in [b"old0".as_slice(), b"old1", b"old2"] {
            log.publish(1, Bytes::copy_from_slice(v)).await.unwrap();
        }
        let (mut stream, handle) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        // Add partition 1 from offset 0, then publish a live append
        // IMMEDIATELY — without first draining the injected backlog.
        //
        // This is the ordering trap. The merged stream is
        // `stream::select(inject_stream, live)`, which round-robins
        // between its two inputs when both have a ready item. Under the
        // OLD behavior the live "new" (offset 3) is emitted by the `live`
        // input directly, so select interleaves it between backlog items:
        // old0(inject), new(live), old1(inject), old2(inject) — "new"
        // jumps ahead of old1/old2, violating per-partition publish
        // order. The fix routes a just-added partition's live records
        // through the SAME inject FIFO, so they queue strictly behind the
        // backlog: old0, old1, old2, new.
        handle.add(PartitionStart {
            partition: 1,
            start_offset: 0,
        });
        log.publish(1, Bytes::from_static(b"new")).await.unwrap();

        let mut got = Vec::new();
        for _ in 0..4 {
            let r = stream.next().await.unwrap();
            got.push((r.partition, r.offset, r.payload.to_vec()));
        }
        assert!(
            got == vec![
                (1, 0, b"old0".to_vec()),
                (1, 1, b"old1".to_vec()),
                (1, 2, b"old2".to_vec()),
                (1, 3, b"new".to_vec()),
            ],
            "backlog must drain fully (in offset order) before the live append"
        );
        assert!(handle.assigned().contains(&1));
    }

    #[tokio::test]
    async fn dropping_handle_evicts_subscription_state() {
        let log = InProcessMetadataEventLog::new(1);
        let (_stream, handle) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        assert!(log.inner.subscriptions.lock().unwrap().len() == 1);
        drop(handle);
        assert!(
            log.inner.subscriptions.lock().unwrap().len() == 0,
            "subscription state must be evicted when the handle drops"
        );
    }

    #[tokio::test]
    async fn remove_stops_delivery() {
        let log = InProcessMetadataEventLog::new(2);
        let (mut stream, handle) = log.subscribe(vec![
            PartitionStart {
                partition: 0,
                start_offset: 0,
            },
            PartitionStart {
                partition: 1,
                start_offset: 0,
            },
        ]);
        handle.remove(1);
        assert!(handle.assigned() == vec![0]);
        log.publish(1, Bytes::from_static(b"gone")).await.unwrap();
        log.publish(0, Bytes::from_static(b"here")).await.unwrap();
        let r = stream.next().await.unwrap();
        check!(r.partition == 0);
        check!(r.payload.as_ref() == b"here");
    }
}
