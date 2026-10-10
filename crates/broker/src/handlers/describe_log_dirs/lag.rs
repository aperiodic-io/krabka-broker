//! The two `offset_lag` values that a `DescribeLogDirs` partition entry
//! carries.
//!
//! Kafka's `ReplicaManager.getLogEndOffsetLag`: a current log reports
//! `max(HW − LEO, 0)`, which is 0 because the high watermark never passes the
//! log end offset, while a KIP-113 future log reports
//! `current_log.LEO − future_log.LEO` so that an operator can watch an
//! intra-broker move drain. Both report [`INVALID_OFFSET_LAG`] when the broker
//! has no local current log for the partition. Both readings reach into the
//! partition registry and the future-log registry, which is why they live
//! together and away from the directory scan.

/// `DescribeLogDirsResponse.INVALID_OFFSET_LAG`: the lag is not available
/// because the replica is not created or is offline.
pub(super) const INVALID_OFFSET_LAG: i64 = -1;

/// `max(HW − LEO, 0)` for a loaded current log.
///
/// Returns [`INVALID_OFFSET_LAG`] when the partition is not materialized on
/// this broker.
pub(super) async fn offset_lag_for(
    partitions: &crate::partition_registry::PartitionRegistry,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, krabka_ids::PartitionIndex(partition)) else {
        return INVALID_OFFSET_LAG;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Lag is a record-count delta between two offsets, not an offset.
    (hw.0 - leo.0).max(0)
}

/// `current_log.LEO − future_log.LEO` for an in-progress KIP-113 move, with no
/// clamp.
///
/// Returns [`INVALID_OFFSET_LAG`] if the partition is not materialized
/// locally. The future LEO counts as 0 if the future-log registry has no
/// entry. The registry has no entry when the broker has just started and the
/// resume task has not opened the future log yet.
pub(super) fn future_offset_lag(
    partitions: &crate::partition_registry::PartitionRegistry,
    future_logs: &dashmap::DashMap<
        (String, krabka_ids::PartitionIndex),
        std::sync::Arc<crate::future_log::FutureLogState>,
    >,
    topic: &str,
    partition: krabka_ids::PartitionIndex,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return INVALID_OFFSET_LAG;
    };
    let current_leo = part.log_end_offset();
    let future_leo =
        future_logs
            .get(&(topic.to_string(), partition))
            .map_or(krabka_log::Offset(0), |e| {
                e.value()
                    .future_log
                    .lock()
                    .expect("future log mutex poisoned")
                    .log_end_offset()
            });
    // Lag is a record-count delta between two offsets, not an offset.
    current_leo.0 - future_leo.0
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::test_support::RecordCount;

    /// Builds a `Partition` rooted at `<log_dir>/<topic>-<partition>`.
    ///
    /// The function uses the real `spawn_partition` path and mirrors the
    /// `future_log` and registry test fixtures. It appends `count` records, so
    /// the LEO of the partition advances to `count`.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct PartitionLogSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        partition: krabka_ids::PartitionIndex,
        #[default(RecordCount(0))]
        count: RecordCount,
    }

    fn partition_with_leo(
        log_dir: &std::path::Path,
        setup: PartitionLogSetup<'_>,
    ) -> std::sync::Arc<crate::partition::Partition> {
        let PartitionLogSetup {
            topic,
            partition,
            count,
        } = setup;
        let part = crate::test_support::open_partition(
            log_dir,
            crate::test_support::StandalonePartitionSetup {
                topic,
                partition: krabka_ids::PartitionIndex(partition.get()),
                ..Default::default()
            },
        );
        if count.0 > 0 {
            append_n(&part.log, count);
        }
        part
    }

    /// Appends one batch of `count` records to a `Log` behind a mutex.
    ///
    /// The LEO of the log advances by `count`.
    fn append_n(log: &std::sync::Mutex<krabka_log::Log>, count: RecordCount) {
        let mut batch = crate::test_support::repeated_records_batch(
            crate::test_support::RepeatedRecordsSetup {
                count,
                timestamp: crate::test_support::UnixMillis(1_700_000_000),
            },
        );
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut batch)
            .expect("append records");
    }

    /// `ReplicaManager.getLogEndOffsetLag` for a current log: `-1` with no
    /// local partition, and `max(HW − LEO, 0)`, so 0, for a leader whose five
    /// records are not replicated yet (a fresh HW is 0).
    #[tokio::test]
    async fn offset_lag_matches_kafka() {
        let dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(
            dir.path(),
            PartitionLogSetup {
                topic: "t",
                count: RecordCount(5),
                ..Default::default()
            },
        );
        assert!(part.log_end_offset() == krabka_log::Offset(5));
        reg.insert("t".into(), krabka_ids::PartitionIndex(0), part);
        for (topic, expected) in [("ghost", INVALID_OFFSET_LAG), ("t", 0)] {
            assert2::check!(offset_lag_for(&reg, topic, 0).await == expected, "{topic}");
        }
    }

    /// Builds a `FutureLogState` whose future log has LEO `future_count`.
    fn future_state_with_leo(
        dir: &std::path::Path,
        future_count: RecordCount,
    ) -> std::sync::Arc<crate::future_log::FutureLogState> {
        let future_path = dir.join("future");
        std::fs::create_dir_all(&future_path).unwrap();
        let flog = krabka_log::Log::open(&future_path, krabka_log::LogConfig::default()).unwrap();
        let future_log = std::sync::Arc::new(std::sync::Mutex::new(flog));
        if future_count.0 > 0 {
            append_n(&future_log, future_count);
        }
        std::sync::Arc::new(crate::future_log::FutureLogState {
            target_log_dir: dir.to_path_buf(),
            future_path,
            future_log,
            cancel: tokio_util::sync::CancellationToken::new(),
            task: std::sync::Mutex::new(None::<tokio::task::JoinHandle<()>>),
        })
    }

    /// `ReplicaManager.getLogEndOffsetLag` for a future log:
    /// `current LEO − future LEO` with no clamp, and `-1` with no local
    /// current log.
    #[tokio::test]
    async fn future_offset_lag_matches_kafka() {
        let cur_dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(
            cur_dir.path(),
            PartitionLogSetup {
                topic: "t",
                partition: krabka_ids::PartitionIndex(3),
                count: RecordCount(5),
            },
        );
        assert!(part.log_end_offset() == krabka_log::Offset(5));
        reg.insert("t".into(), krabka_ids::PartitionIndex(3), part);

        for (name, topic, future_leo, expected) in [
            ("no local current log", "ghost", 2, INVALID_OFFSET_LAG),
            ("future three records behind", "t", 2, 3),
            ("future ahead is not clamped", "t", 7, -2),
        ] {
            let fut_dir = tempfile::tempdir().unwrap();
            let future_logs = dashmap::DashMap::new();
            future_logs.insert(
                (topic.to_string(), krabka_ids::PartitionIndex(3)),
                future_state_with_leo(fut_dir.path(), RecordCount(future_leo)),
            );
            let lag = future_offset_lag(&reg, &future_logs, topic, krabka_ids::PartitionIndex(3));
            assert2::check!(lag == expected, "case {name}");
        }
    }
}
