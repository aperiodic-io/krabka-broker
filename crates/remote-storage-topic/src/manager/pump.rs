//! The consumer pump, which applies every record the metadata event log
//! delivers to the local cache.
//!
//! The pump is the single writer of the inner cache and of the per-partition
//! applied offsets, so it lives apart from the manager that spawns it. It
//! applies an event to the cache before it advances the applied offset, which
//! is the ordering the snapshot capture relies on.

use std::sync::Arc;

use futures_util::StreamExt;
use krabka_remote_storage::{InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{log::MetadataEventStream, serde::MetadataEvent};

pub async fn pump_loop(
    mut stream: MetadataEventStream,
    inner: Arc<InmemoryRemoteLogMetadataManager>,
    applied: Arc<std::sync::Mutex<Vec<i64>>>,
    applied_tx: watch::Sender<u64>,
    shutdown: CancellationToken,
) {
    let mut version: u64 = 0;
    loop {
        let next = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            n = stream.next() => n,
        };
        let Some(record) = next else { return };
        if !record.tombstone {
            match MetadataEvent::decode(&record.payload) {
                Ok(MetadataEvent::AddSegment(md)) => {
                    if let Err(e) = inner.add_remote_log_segment_metadata(md) {
                        warn!(error = ?e, partition = record.partition, offset = record.offset,
                              "topic-based RLMM: add replay rejected");
                    }
                }
                Ok(MetadataEvent::UpdateSegment(u)) => {
                    if let Err(e) = inner.update_remote_log_segment_metadata(u) {
                        warn!(error = ?e, partition = record.partition, offset = record.offset,
                              "topic-based RLMM: update replay rejected");
                    }
                }
                Ok(MetadataEvent::PartitionDelete(d)) => {
                    if let Err(e) = inner.put_remote_partition_delete_metadata(d) {
                        warn!(error = ?e, partition = record.partition, offset = record.offset,
                              "topic-based RLMM: partition-delete replay rejected");
                    }
                }
                Err(e) => {
                    warn!(error = ?e, partition = record.partition, offset = record.offset,
                          "topic-based RLMM: failed to decode event");
                }
            }
        }
        if let Ok(idx) = usize::try_from(record.partition) {
            let mut a = applied.lock().expect("applied mutex poisoned");
            if idx < a.len() && record.offset > a[idx] {
                a[idx] = record.offset;
            }
        }
        version = version.wrapping_add(1);
        let _ = applied_tx.send(version);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::LeaderEpoch;
    use uuid::Uuid;

    use super::*;
    use crate::{
        log::{InProcessMetadataEventLog, MetadataEventLog},
        manager::test_support::{
            finish, on_blocking, seed_finished, start_manager_all, started, tp,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn two_managers_sharing_a_log_converge() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let a = start_manager_all(log.clone()).await;
        let b = start_manager_all(log.clone()).await;

        let a2 = a.clone();
        on_blocking(move || {
            a2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let a2 = a.clone();
        on_blocking(move || a2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

        // `b` must observe `a`'s writes once its pump has applied
        // them. Poll up to 2s for the in-process broadcast to fan out.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while b.highest_offset_for_epoch(&tp(), LeaderEpoch(0)).unwrap() != Some(99) {
            assert!(
                std::time::Instant::now() < deadline,
                "manager B did not converge within 2s"
            );
            tokio::task::yield_now().await;
        }
        assert!(b.highest_offset_for_epoch(&tp(), LeaderEpoch(0)).unwrap() == Some(99));
        let got = b
            .remote_log_segment_metadata(&tp(), LeaderEpoch(0), 50)
            .unwrap()
            .unwrap();
        assert!(got.remote_log_segment_id().id == Uuid::from_u128(10));

        a.shutdown();
        b.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_rehydrates_from_log() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        {
            let m = start_manager_all(log.clone()).await;
            seed_finished(&m, &[(10, 0, 99), (11, 100, 199), (12, 200, 299)]).await;
            m.shutdown();
        }

        // Fresh manager against the same log: assigning all partitions
        // replays the full history before the read below.
        let fresh = start_manager_all(log).await;
        let listed = fresh.list_remote_log_segments(&tp()).unwrap();
        let ranges: Vec<(i64, i64)> = listed
            .iter()
            .map(|s| (s.start_offset(), s.end_offset()))
            .collect();
        assert!(ranges == [(0, 99), (100, 199), (200, 299)]);
        assert!(
            fresh
                .highest_offset_for_epoch(&tp(), LeaderEpoch(0))
                .unwrap()
                == Some(299)
        );
        fresh.shutdown();
    }
}
