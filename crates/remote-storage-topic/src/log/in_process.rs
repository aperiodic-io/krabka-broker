//! [`InProcessMetadataEventLog`], the in-memory broadcast-channel fixture that
//! implements [`MetadataEventLog`] without a Kafka cluster behind it.
//!
//! This module holds the fixture's shared state, which is the per-partition
//! record vectors and the per-subscription cursors, together with the
//! [`MetadataEventLog`] implementation over them. The two child modules act on
//! that state: [`self::handle`] carries the [`AssignmentHandle`] the fixture
//! hands back, and [`self::live`] carries the broadcast filter that decides
//! which live record a subscription sees. They are descendants of this module
//! so the state can stay private to this file.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::AtomicU64},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream, stream::unfold};
use tokio::sync::{broadcast, mpsc};

use self::{handle::InProcessAssignmentHandle, live::filtered_broadcast};
use super::{
    AssignmentHandle, MetadataEventLog, MetadataEventRecord, MetadataEventStream, PartitionStart,
    RangeVisitor,
};
use crate::error::MetadataLogError;

mod handle;
mod live;

/// Single-process [`MetadataEventLog`] for unit tests and for the
/// multi-broker fixture. Multiple manager instances that clone the same `Arc`
/// observe each other's writes.
pub struct InProcessMetadataEventLog {
    inner: Arc<InProcessInner>,
}

/// Live-assignment cursor for one partition within a subscription.
#[derive(Debug, Clone, Copy)]
struct PartitionCursor {
    /// Next offset that the backlog or live path has NOT yet delivered. The
    /// log filters out records below this offset.
    next: i64,
    /// When set, the log forwards live records for this partition through
    /// the `inject` FIFO rather than emits them directly on the broadcast
    /// path. A partition added mid-stream sets this, so its live appends
    /// queue *behind* its already-injected backlog. Without it,
    /// `stream::select` could interleave a live record ahead of undrained
    /// backlog and violate per-partition publish order. Initially-assigned
    /// partitions leave this `false`. Their backlog goes through the chained
    /// snapshot stream, which fully drains before any live record.
    via_inject: bool,
}

/// Per-subscription live assignment, plus a sender to inject backlog when a
/// partition is added mid-stream. A monotonically-increasing subscription id
/// keys it, so multiple subscribers stay independent.
struct SubscriptionState {
    /// partition -> cursor. Presence in the map means the partition is
    /// assigned.
    assigned: Mutex<HashMap<i32, PartitionCursor>>,
    /// Inject backlog records in FIFO publish order. For `add`-ed partitions
    /// this also injects live records.
    inject: mpsc::UnboundedSender<MetadataEventRecord>,
}

struct InProcessInner {
    /// `log[partition][offset] = event`.
    log: Mutex<Vec<Vec<MetadataEventRecord>>>,
    /// Notify subscribers of new writes.
    tx: broadcast::Sender<MetadataEventRecord>,
    /// Constant for the life of the log.
    partition_count: i32,
    /// Live subscriptions, keyed by id, for assignment filtering and
    /// mid-stream backlog injection.
    subscriptions: Mutex<HashMap<u64, Arc<SubscriptionState>>>,
    /// Allocates subscription ids.
    next_sub_id: AtomicU64,
}

impl InProcessMetadataEventLog {
    /// Construct an empty log with `partition_count` partitions.
    ///
    /// # Panics
    ///
    /// Panics when `partition_count <= 0`.
    #[must_use]
    pub fn new(partition_count: i32) -> Arc<Self> {
        assert2::assert!(partition_count > 0, "partition_count must be positive");
        let cap = usize::try_from(partition_count).expect("partition_count fits in usize");
        let (tx, _rx) = broadcast::channel(1024);
        Arc::new(Self {
            inner: Arc::new(InProcessInner {
                log: Mutex::new(vec![Vec::new(); cap]),
                tx,
                partition_count,
                subscriptions: Mutex::new(HashMap::new()),
                next_sub_id: AtomicU64::new(0),
            }),
        })
    }

    fn append(
        &self,
        partition: i32,
        key: Option<Bytes>,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        if partition < 0 || partition >= self.inner.partition_count {
            return Err(MetadataLogError::PartitionOutOfRange {
                partition,
                count: self.inner.partition_count,
            });
        }
        // Hold the partition lock across the broadcast.send so that any
        // concurrent subscribe() observes either the appended record in
        // its snapshot or as a forwarded broadcast — never both.
        let mut guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        let idx = usize::try_from(partition).expect("partition non-negative");
        let log_for_p = &mut guard[idx];
        let offset = i64::try_from(log_for_p.len()).expect("offset fits in i64");
        let tombstone = event.is_none();
        let record = MetadataEventRecord {
            partition,
            offset,
            key,
            payload: event.unwrap_or_default(),
            tombstone,
        };
        log_for_p.push(record.clone());
        // `send` only errors when there are no active receivers; that
        // is fine — the record is still durable in the in-memory log
        // and any later subscriber's snapshot will see it.
        let _ = self.inner.tx.send(record);
        Ok(offset)
    }
}

#[async_trait]
impl MetadataEventLog for InProcessMetadataEventLog {
    fn partition_count(&self) -> i32 {
        self.inner.partition_count
    }

    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.append(partition, None, Some(event))
    }

    async fn publish_keyed(
        &self,
        partition: i32,
        key: Bytes,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        self.append(partition, Some(key), event)
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        use std::sync::atomic::Ordering;

        // Bracket snapshot + broadcast subscribe under the log lock so each
        // published record is seen exactly once (snapshot xor live).
        let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        let rx = self.inner.tx.subscribe();

        // Initial assigned set: partition -> next live offset (= current
        // len), so the broadcast path forwards only records published after
        // subscribe; everything earlier comes from the snapshot below.
        let mut assigned: HashMap<i32, PartitionCursor> = HashMap::new();
        let mut snapshot: Vec<MetadataEventRecord> = Vec::new();
        for ps in &assignment {
            let Ok(idx) = usize::try_from(ps.partition) else {
                continue;
            };
            if idx >= guard.len() {
                continue;
            }
            let records = &guard[idx];
            let begin = usize::try_from(ps.start_offset.max(0)).unwrap_or(usize::MAX);
            for record in records.iter().skip(begin) {
                snapshot.push(record.clone());
            }
            assigned.insert(
                ps.partition,
                PartitionCursor {
                    next: i64::try_from(records.len()).expect("len fits in i64"),
                    // Initially-assigned: backlog rides the chained
                    // snapshot stream, so live records can go direct.
                    via_inject: false,
                },
            );
        }

        let (inject_tx, inject_rx) = mpsc::unbounded_channel::<MetadataEventRecord>();
        let state = Arc::new(SubscriptionState {
            assigned: Mutex::new(assigned),
            inject: inject_tx,
        });
        let sub_id = self.inner.next_sub_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .subscriptions
            .lock()
            .expect("metadata-log subscriptions mutex poisoned")
            .insert(sub_id, Arc::clone(&state));
        drop(guard);

        let snapshot_stream = stream::iter(snapshot);
        let inject_stream = unfold(inject_rx, |mut rx| async move {
            rx.recv().await.map(|r| (r, rx))
        });
        let live = filtered_broadcast(rx, state);
        // Snapshot first (subscribe-time backlog), then a merge of injected
        // backlog (from `add`) and assignment-filtered live records.
        let merged = stream::select(inject_stream, live);
        let stream = snapshot_stream.chain(merged).boxed();

        let handle: Arc<dyn AssignmentHandle> = Arc::new(InProcessAssignmentHandle {
            inner: Arc::clone(&self.inner),
            sub_id,
        });
        (stream, handle)
    }

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
        Ok(guard
            .iter()
            .map(|v| i64::try_from(v.len()).expect("hwm fits in i64"))
            .collect())
    }

    async fn visit_range(
        &self,
        partition: i32,
        start: i64,
        end: i64,
        visit: &mut RangeVisitor<'_>,
    ) -> Result<(), MetadataLogError> {
        let out_of_range = || MetadataLogError::PartitionOutOfRange {
            partition,
            count: self.inner.partition_count,
        };
        let idx = usize::try_from(partition).map_err(|_| out_of_range())?;
        // Copy the range out, so `visit` never runs under the log lock.
        let range: Vec<MetadataEventRecord> = {
            let guard = self.inner.log.lock().expect("metadata-log mutex poisoned");
            let records = guard.get(idx).ok_or_else(out_of_range)?;
            let high_water_mark = i64::try_from(records.len()).expect("hwm fits in i64");
            if end > high_water_mark {
                return Err(MetadataLogError::Other(format!(
                    "partition {partition} ends at {high_water_mark}, before {end}"
                )));
            }
            records
                .iter()
                .filter(|record| start <= record.offset && record.offset < end)
                .cloned()
                .collect()
        };
        range.into_iter().try_for_each(visit)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use futures_util::StreamExt;

    use super::*;

    #[tokio::test]
    async fn publish_assigns_monotonic_offsets() {
        let log = InProcessMetadataEventLog::new(2);
        check!(log.publish(0, Bytes::from_static(b"a")).await.unwrap() == 0);
        check!(log.publish(0, Bytes::from_static(b"b")).await.unwrap() == 1);
        check!(log.publish(1, Bytes::from_static(b"c")).await.unwrap() == 0);
        let hwms = log.high_water_marks().await.unwrap();
        assert!(hwms == vec![2, 1]);
    }

    /// Every record `visit_range` hands over, in the order it hands them.
    async fn visited(
        log: &InProcessMetadataEventLog,
        partition: i32,
        start: i64,
        end: i64,
    ) -> Result<Vec<MetadataEventRecord>, MetadataLogError> {
        let mut records = Vec::new();
        log.visit_range(partition, start, end, &mut |record| {
            records.push(record);
            Ok(())
        })
        .await?;
        Ok(records)
    }

    #[tokio::test]
    async fn visit_range_hands_over_the_half_open_range_without_subscribing() {
        let log = InProcessMetadataEventLog::new(2);
        for value in [b"a", b"b", b"c"] {
            log.publish(0, Bytes::from_static(value)).await.unwrap();
        }

        let record = |offset, payload| MetadataEventRecord {
            partition: 0,
            offset,
            key: None,
            payload: Bytes::from_static(payload),
            tombstone: false,
        };
        check!(visited(&log, 0, 1, 3).await.unwrap() == vec![record(1, b"b"), record(2, b"c")]);
        check!(visited(&log, 1, 0, 0).await.unwrap().is_empty());
        check!(
            visited(&log, 0, 0, 4).await.unwrap_err().to_string()
                == "metadata log error: partition 0 ends at 3, before 4"
        );
        for partition in [-1, 2] {
            check!(matches!(
                visited(&log, partition, 0, 0).await,
                Err(MetadataLogError::PartitionOutOfRange { partition: got, count: 2 })
                    if got == partition
            ));
        }
        check!(log.inner.subscriptions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn visit_range_stops_at_the_first_visitor_error() {
        let log = InProcessMetadataEventLog::new(1);
        for value in [b"a", b"b", b"c"] {
            log.publish(0, Bytes::from_static(value)).await.unwrap();
        }

        let mut seen = Vec::new();
        let error = log
            .visit_range(0, 0, 3, &mut |record| {
                seen.push(record.offset);
                if record.offset == 1 {
                    Err(MetadataLogError::Closed)
                } else {
                    Ok(())
                }
            })
            .await
            .unwrap_err();

        check!(matches!(error, MetadataLogError::Closed));
        check!(seen == vec![0, 1]);
    }

    #[tokio::test]
    async fn keyed_tombstones_survive_replay() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish_keyed(
            0,
            Bytes::from_static(b"range"),
            Some(Bytes::from_static(b"object")),
        )
        .await
        .unwrap();
        log.publish_keyed(0, Bytes::from_static(b"range"), None)
            .await
            .unwrap();
        let (mut stream, _) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);

        let value = stream.next().await.unwrap();
        let tombstone = stream.next().await.unwrap();
        assert!(value.key.as_deref() == Some(b"range".as_slice()));
        assert!(!value.tombstone);
        assert!(tombstone.key.as_deref() == Some(b"range".as_slice()));
        assert!(tombstone.tombstone);
    }

    #[tokio::test]
    async fn subscribe_replays_history_then_forwards_new_writes() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish(0, Bytes::from_static(b"a")).await.unwrap();
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        let a = stream.next().await.unwrap();
        let b = stream.next().await.unwrap();
        assert!(a.payload.as_ref() == b"a");
        assert!(b.payload.as_ref() == b"b");
        log.publish(0, Bytes::from_static(b"c")).await.unwrap();
        let c = stream.next().await.unwrap();
        check!(c.payload.as_ref() == b"c");
        check!(c.partition == 0);
        check!(c.offset == 2);
    }

    #[tokio::test]
    async fn subscribe_attached_after_history_still_sees_history() {
        let log = InProcessMetadataEventLog::new(1);
        for i in 0..5 {
            log.publish(0, Bytes::copy_from_slice(&[i])).await.unwrap();
        }
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        for i in 0..5 {
            let r = stream.next().await.unwrap();
            assert!(r.payload.as_ref() == &[i]);
            assert!(r.offset == i64::from(i));
        }
    }

    #[tokio::test]
    async fn publish_out_of_range_is_rejected() {
        let log = InProcessMetadataEventLog::new(2);
        let err = log.publish(5, Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, MetadataLogError::PartitionOutOfRange { .. }));
    }

    #[tokio::test]
    async fn two_subscribers_see_the_same_history() {
        let log = InProcessMetadataEventLog::new(1);
        log.publish(0, Bytes::from_static(b"a")).await.unwrap();
        let (mut s1, _h1) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        let (mut s2, _h2) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        log.publish(0, Bytes::from_static(b"b")).await.unwrap();
        for s in [&mut s1, &mut s2] {
            assert!(s.next().await.unwrap().payload.as_ref() == b"a");
            assert!(s.next().await.unwrap().payload.as_ref() == b"b");
        }
    }

    #[tokio::test]
    async fn subscribe_delivers_only_assigned_partitions_from_start_offset() {
        let log = InProcessMetadataEventLog::new(3);
        // partition 0: a,b,c ; partition 1: x,y ; partition 2: z
        for p0 in [b"a".as_slice(), b"b", b"c"] {
            log.publish(0, Bytes::copy_from_slice(p0)).await.unwrap();
        }
        for p1 in [b"x".as_slice(), b"y"] {
            log.publish(1, Bytes::copy_from_slice(p1)).await.unwrap();
        }
        log.publish(2, Bytes::from_static(b"z")).await.unwrap();

        // Assign partition 0 from offset 1 and partition 1 from offset 0;
        // partition 2 is NOT assigned.
        let (mut stream, _h) =
            log.subscribe(vec![PartitionStart::new(0, 1), PartitionStart::new(1, 0)]);

        let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
        for _ in 0..3 {
            let r = stream.next().await.unwrap();
            got.push((r.partition, r.offset, r.payload.to_vec()));
        }
        got.sort();
        assert!(
            got == vec![
                (0, 1, b"b".to_vec()),
                (0, 2, b"c".to_vec()),
                (1, 0, b"x".to_vec()),
            ]
        );
        // partition 1 offset 1 ("y") is the only remaining assigned record.
        let r = stream.next().await.unwrap();
        check!(r.partition == 1);
        check!(r.offset == 1);
        check!(r.payload.as_ref() == b"y");
    }

    #[tokio::test]
    async fn live_appends_only_for_assigned_partitions() {
        let log = InProcessMetadataEventLog::new(2);
        let (mut stream, _h) = log.subscribe(vec![PartitionStart {
            partition: 0,
            start_offset: 0,
        }]);
        // Unassigned partition write must not appear.
        log.publish(1, Bytes::from_static(b"skip")).await.unwrap();
        log.publish(0, Bytes::from_static(b"keep")).await.unwrap();
        let r = stream.next().await.unwrap();
        check!(r.partition == 0);
        check!(r.payload.as_ref() == b"keep");
    }
}
