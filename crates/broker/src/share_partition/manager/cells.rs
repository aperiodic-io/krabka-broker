//! The lazily loaded acquisition-state cells: the load-on-miss path, the
//! test-only peek, and the invalidation the admin offset RPCs use.
//!
//! This is the only module that inserts into or removes from the `leaders`
//! map, so the rule that no `DashMap` guard is held across an `.await` is
//! checkable by reading one file.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use tokio::sync::Mutex;
use tracing::warn;

use super::{
    SharePartitionLeaderManager,
    persistence::{fences_the_partition, persister_error_code},
};
use crate::{
    coordinator::unified::streams::config::ShareAutoOffsetReset,
    share_partition::state::AcquisitionState,
    time_util::{duration_millis, now_ms},
};

impl SharePartitionLeaderManager {
    /// Gets the acquisition-state cell for `(group, topic_id, partition)`, and
    /// loads it lazily on a miss.
    ///
    /// On a cache miss the method reads the durable state from the persister
    /// and folds it into a fresh [`AcquisitionState`]. The group coordinator
    /// initializes the state of every assigned share partition first (Kafka's
    /// Initialize-first flow). When that state has no start offset yet
    /// (`-1`), the group's `share.auto.offset.reset` decides where the empty
    /// window starts, and the method persists that decision so a later leader
    /// does not resolve it again against a moved log or a moved clock. The
    /// persister refuses a read of a key that has no state. The
    /// method drops the `DashMap` guard before the load `.await`. A concurrent
    /// loader that loses the insert race adopts the cell of the winner.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    ///
    /// # Errors
    ///
    /// Returns the mapped error code
    /// ([`persister_error_code`](super::persistence::persister_error_code))
    /// when the state read fails, and leaves no cell behind, so the next
    /// request reads again. Kafka's `SharePartition.maybeInitialize` fails the
    /// same way, and `SharePartitionManager` removes the partition from its
    /// cache. A read error never becomes a start offset.
    pub(crate) async fn get_or_load(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Arc<Mutex<AcquisitionState>>, i16> {
        let key = (group.to_string(), topic_id, partition);
        if let Some(cell) = self.leaders.get(&key) {
            return Ok(cell.value().clone());
        }

        // Miss: load from the persister WITHOUT holding any DashMap guard.
        let leader_epoch = self.leader_epoch_for(topic_id, partition);
        let mut loaded = match self
            .persister
            .read_state(group, topic_id, partition, leader_epoch)
            .await
        {
            // Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET` is -1: the
            // record the group coordinator writes when it registers a share
            // partition, before any fetch has resolved where the group starts.
            // It is not a start offset, so it takes the strategy path below.
            Ok(persisted) if persisted.start_offset.0 >= 0 => {
                let mut st = AcquisitionState::new(persisted.start_offset);
                st.load_from(
                    persisted.start_offset,
                    persisted.state_epoch,
                    leader_epoch,
                    &persisted.state_batches,
                );
                st
            }
            Ok(registered) => {
                let start = match self.initial_start_offset(group, topic_id, partition).await {
                    Ok(start) => start,
                    Err(code) => {
                        warn!(
                            group,
                            %topic_id, partition, code,
                            "share-partition start offset not available"
                        );
                        return Err(code);
                    }
                };
                let mut st = AcquisitionState::new(start);
                // The strategy decides only where the window starts. The state
                // epoch stays the coordinator's: it is the fencing token the
                // group coordinator stamped when it registered the partition,
                // and a write-back carrying a lower one is refused with
                // FENCED_STATE_EPOCH, which would strand every later SPSO
                // advance in memory.
                st.state_epoch = registered.state_epoch;
                st.leader_epoch = leader_epoch;
                // The resolved start is durable state: persist it now so the
                // next leader inherits it instead of re-resolving a `latest`
                // or `by_duration` strategy against a log that has moved on.
                st.dirty = true;
                st
            }
            Err(e) => {
                let code = persister_error_code(&e);
                warn!(
                    group,
                    %topic_id, partition, error = %e, code,
                    "share-partition state load failed"
                );
                return Err(code);
            }
        };

        // The resolved start of a partition with no state is best-effort
        // durable: a failed write keeps `dirty` set for a retry. A fenced
        // write means another writer owns the state, so no cell is cached.
        if let Err(code) = self
            .persist_if_dirty(group, topic_id, partition, None, &mut loaded)
            .await
            && fences_the_partition(code)
        {
            return Err(code);
        }
        let cell = Arc::new(Mutex::new(loaded));
        // Adopt the winner if another task loaded the same key concurrently.
        let cell = self
            .leaders
            .entry(key.clone())
            .or_insert(cell)
            .value()
            .clone();
        // A run that the last leader left in `Archiving` resumes its
        // dead-letter write here (KIP-1191). Whichever loader takes the runs
        // first starts them, so a race starts each once.
        let resumed = cell.lock().await.take_pending_dlq();
        self.dispatch_dead_letters(&key, &cell, resumed);
        Ok(cell)
    }

    /// Where a share partition with no persisted state starts.
    ///
    /// The group's `share.auto.offset.reset` picks the offset: `earliest` the
    /// log start, `latest` the high watermark, and `by_duration:<d>` the first
    /// record at or after `now - d`. Every answer is clamped to the log start,
    /// so a retention-truncated partition never starts below it.
    ///
    /// # Errors
    ///
    /// `OFFSET_NOT_AVAILABLE` when `by_duration` finds no record at or after
    /// the target time, as Kafka's `ShareFetchUtils.offsetForTimestamp` throws
    /// `OffsetNotAvailableException` for a log with no such record. The load
    /// then fails and caches nothing, so the next request resolves again once
    /// a record in the window exists. `INVALID_RECORD` when the lookup reads a
    /// compressed record above the topic's Kafka trunk
    /// `max.decompressed.message.bytes`.
    ///
    /// A partition this broker does not hold, or a topic id the image does not
    /// know, yields offset 0: there is no log to resolve against, and the next
    /// load re-resolves once the partition is materialized.
    async fn initial_start_offset(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Offset, i16> {
        let image = self.controller.current_image();
        let strategy = image
            .group_config(group)
            .map_or_else(ShareAutoOffsetReset::default, |overrides| {
                ShareAutoOffsetReset::from_group_overrides(overrides)
            });
        let local = image
            .topic_name_by_id(&topic_id)
            .and_then(|topic| self.partitions.get(topic, PartitionIndex(partition)));
        let Some(local) = local else {
            return Ok(Offset(0));
        };
        let log_start = local.log_start_offset();
        match strategy {
            ShareAutoOffsetReset::Earliest => Ok(log_start),
            ShareAutoOffsetReset::Latest => Ok(local.high_watermark().await.max(log_start)),
            ShareAutoOffsetReset::ByDuration(duration) => {
                let target = now_ms().saturating_sub(duration_millis(duration));
                let found = {
                    let log = local.log.lock().expect("log mutex poisoned");
                    log.offset_for_timestamp_checked(target)
                };
                // A compressed record above the topic's trunk
                // `max.decompressed.message.bytes` fails the lookup:
                // `UnifiedLog.fetchOffsetByTimestamp` throws
                // `InvalidRecordException` through
                // `ReplicaManager.fetchOffsetForTimestamp` into the share
                // partition's initialization.
                let found = found.map_err(|error| {
                    crate::codes::from_broker_error(&crate::error::BrokerError::from(error))
                })?;
                // Every record predates the window: Kafka's
                // `ShareFetchUtils.offsetForTimestamp` throws, and the
                // initialization of the share partition fails.
                found
                    .map(|(offset, _)| offset.max(log_start))
                    .ok_or(crate::codes::OFFSET_NOT_AVAILABLE)
            }
        }
    }

    /// The cached acquisition cell of `(group, topic_id, partition)`, with no
    /// persister load.
    ///
    /// An acknowledgement goes only to a share partition that a fetch on this
    /// broker loaded: Kafka's `SharePartitionManager.acknowledge` answers
    /// `UNKNOWN_TOPIC_OR_PARTITION` for one that is not in its cache.
    pub(crate) fn cached(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<Arc<Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|cell| cell.value().clone())
    }

    /// Test-only: borrows the live acquisition cell without a persister load.
    ///
    /// Returns `None` if this node does not currently lead the partition or has
    /// not loaded the cell.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn peek_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|c| c.value().clone())
    }

    /// Drops `cell` from the cache when it is still the cached cell for
    /// `(group, topic_id, partition)`.
    ///
    /// A request that held a fenced cell can finish after another request
    /// loaded a replacement. Removing by key alone would then drop the
    /// replacement while it is in use, and a later load would create a second
    /// machine for the same share partition.
    pub(crate) fn invalidate_cell(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        cell: &Arc<Mutex<AcquisitionState>>,
    ) {
        self.leaders
            .remove_if(&(group.to_string(), topic_id, partition), |_, cached| {
                Arc::ptr_eq(cached, cell)
            });
    }

    /// Drops every share session and every cached acquisition-state cell.
    ///
    /// Kafka's `SharePartitionManager.onShareVersionToggle` does this when the
    /// finalized `share.version` drops below 1: it removes all sessions and
    /// all cached share partitions. Acquired records need no release, because
    /// the durable state stores them as available, and a later load reads the
    /// SPSO again.
    pub(crate) fn clear(&self) {
        self.sessions.clear();
        self.leaders.clear();
    }

    /// Test-only: caches `state` as the live cell, with no persister read.
    #[cfg(test)]
    pub(crate) fn insert_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state: AcquisitionState,
    ) -> Arc<Mutex<AcquisitionState>> {
        let cell = Arc::new(Mutex::new(state));
        self.leaders
            .insert((group.to_string(), topic_id, partition), Arc::clone(&cell));
        cell
    }

    /// Drops the cached acquisition-state cell for
    /// `(group, topic_id, partition)`.
    ///
    /// The next `get_or_load` then re-reads the durable SPSO. The admin offset
    /// RPCs call this method after `AlterShareGroupOffsets` or
    /// `DeleteShareGroupOffsets` rewrites the persister state. A later
    /// `ShareFetch` on this broker thus sees an in-flight reset. A cell on
    /// another broker refreshes on its own next load, which matches the classic
    /// offset-reset behavior.
    pub(crate) fn invalidate(&self, group: &str, topic_id: uuid::Uuid, partition: i32) {
        self.leaders
            .remove(&(group.to_string(), topic_id, partition));
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use assert2::assert;
    use krabka_log::Offset;
    use krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord, NodeId, TopicRecord};

    use crate::{
        codes,
        coordinator::unified::streams::config::KEY_SHARE_AUTO_OFFSET_RESET,
        share_partition::{
            manager::test_support::{
                manager, manager_with_image_and_partitions, open_data_partition,
            },
            state::AcquisitionState,
        },
    };

    krabka_macros::single_replica_partition_fixture!(partition_record);

    /// Every `share.auto.offset.reset` strategy, resolved against one real
    /// log: two records stamped three hours ago at offsets 0-1, two stamped
    /// ninety minutes ago at offsets 2-3, and a high watermark of 4.
    ///
    /// The resolution is exercised where `get_or_load` calls it. The load
    /// itself cannot reach it over this fixture, because a metadata image with
    /// no brokers cannot bootstrap `__share_group_state`, so `read_state`
    /// fails before it can report that the partition has no state.
    #[tokio::test]
    async fn fresh_cell_starts_where_the_group_strategy_says() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tid = uuid::Uuid::from_bytes([41; 16]);
        let now = crate::time_util::now_ms();
        let hour = 60 * 60 * 1_000;
        let reg = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        open_data_partition(
            &reg,
            dir.path(),
            crate::share_partition::manager::test_support::DataPartitionSetup {
                batches: vec![
                    crate::share_partition::manager::test_support::TimedValues {
                        timestamp: crate::test_support::UnixMillis(now - 3 * hour),
                        values: &[b"stale-0", b"stale-1"],
                    },
                    crate::share_partition::manager::test_support::TimedValues {
                        timestamp: crate::test_support::UnixMillis(now - hour - hour / 2),
                        values: &[b"recent-0", b"recent-1"],
                    },
                ],
                high_watermark: Offset(4),
                ..Default::default()
            },
        )
        .await;

        // `default` carries no override at all, so the broker default decides.
        let strategies = [
            ("default", None, Ok(Offset(4))),
            ("latest", Some("latest"), Ok(Offset(4))),
            ("earliest", Some("earliest"), Ok(Offset(0))),
            // The window reaches back past the second batch only.
            ("in-window", Some("by_duration:PT2H"), Ok(Offset(2))),
            // No record is inside the window: Kafka's `offsetForTimestamp`
            // throws, and the initialization fails.
            (
                "past-the-end",
                Some("by_duration:PT1H"),
                Err(codes::OFFSET_NOT_AVAILABLE),
            ),
        ];
        let mgr = manager_with_image_and_partitions(
            image_with_strategies(
                tid,
                strategies
                    .iter()
                    .filter_map(|(group, value, _)| value.map(|value| (*group, value))),
            ),
            reg,
        );

        for (group, value, want) in strategies {
            let got = mgr.initial_start_offset(group, tid, 0).await;
            assert!(
                got == want,
                "{KEY_SHARE_AUTO_OFFSET_RESET}={value:?}: got {got:?}, want {want:?}"
            );
        }
    }

    /// A metadata image holding topic `t` (one partition, led here) under
    /// `tid`, and a `share.auto.offset.reset` for each `(group, value)`.
    fn image_with_strategies<'a>(
        tid: uuid::Uuid,
        strategies: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Arc<MetadataImage> {
        let mut records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: tid,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(partition_record(
                "t",
                krabka_ids::PartitionIndex(0),
                NodeId(1),
            )),
        ];
        for (group, value) in strategies {
            records.push(MetadataRecord::V1GroupConfig(GroupConfigRecord {
                group_id: group.to_string(),
                configs: maplit::btreemap! {
                    KEY_SHARE_AUTO_OFFSET_RESET.to_owned() => value.to_owned()
                },
            }));
        }
        Arc::new(MetadataImage::from_records(uuid::Uuid::nil(), &records))
    }

    /// `by_duration` resolves through the same lookup as `ListOffsets` by
    /// timestamp, so a compressed record above the topic's Kafka trunk
    /// `max.decompressed.message.bytes` that the lookup has to read fails the
    /// load with `INVALID_RECORD`: `UnifiedLog.fetchOffsetByTimestamp` throws
    /// `InvalidRecordException` into the share partition's initialization.
    ///
    /// Two gzip batches, a small record stamped three hours ago at offset 0 and
    /// one with a 1000-byte value stamped ninety minutes ago at offset 1.
    #[tokio::test]
    async fn by_duration_start_refuses_a_record_above_the_decompressed_limit() {
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let dir = tempfile::tempdir().expect("tempdir");
        let tid = uuid::Uuid::from_bytes([42; 16]);
        let now = crate::time_util::now_ms();
        let hour = 60 * 60 * 1_000;
        let reg = Arc::new(crate::partition_registry::PartitionRegistry::new());
        open_data_partition(
            &reg,
            dir.path(),
            crate::share_partition::manager::test_support::DataPartitionSetup {
                high_watermark: Offset(2),
                ..Default::default()
            },
        )
        .await;
        let partition = reg
            .get("t", krabka_ids::PartitionIndex(0))
            .expect("partition");
        for (timestamp, value_len) in [(now - 3 * hour, 10), (now - hour - hour / 2, 1_000)] {
            let mut batch = RecordBatch {
                base_timestamp: timestamp,
                max_timestamp: timestamp,
                attributes: Attributes::default()
                    .with_compression(krabka_compression::CompressionType::Gzip),
                records: vec![Record {
                    value: Some(bytes::Bytes::from(vec![7_u8; value_len])),
                    ..Default::default()
                }],
                ..Default::default()
            };
            crate::test_support::append_partition_batch(&partition, &mut batch);
        }
        let mgr = manager_with_image_and_partitions(
            image_with_strategies(
                tid,
                [
                    ("two-hours", "by_duration:PT2H"),
                    ("four-hours", "by_duration:PT4H"),
                ],
            ),
            reg,
        );

        for (name, group, limit, want) in [
            // The first batch tops out three hours ago, so the window lands in
            // the second, which holds the oversized record.
            (
                "the match is oversized",
                "two-hours",
                Some(krabka_units::bytes(100)),
                Err(codes::INVALID_RECORD),
            ),
            // The first batch matches, and the oversized one is never read.
            (
                "the match precedes the oversized batch",
                "four-hours",
                Some(krabka_units::bytes(100)),
                Ok(Offset(0)),
            ),
            ("no limit", "two-hours", None, Ok(Offset(1))),
        ] {
            {
                let log = partition.log.lock().expect("partition log lock");
                let config = krabka_log::LogConfig {
                    max_decompressed_record: limit,
                    ..log.config_snapshot()
                };
                log.set_config(config);
            }
            let got = mgr.initial_start_offset(group, tid, 0).await;
            assert!(got == want, "{name}: got {got:?}, want {want:?}");
        }
    }

    /// A state read that fails never becomes a start offset. Over a
    /// broker-less image the persister cannot create a share-state topic, so
    /// no coordinator can serve the read: the load answers
    /// `COORDINATOR_NOT_AVAILABLE` and caches no cell, so the next request
    /// reads again.
    #[tokio::test]
    async fn a_failed_state_read_answers_an_error_and_caches_nothing() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([21; 16]);

        let loads = [
            mgr.get_or_load("g1", tid, 0).await.err(),
            mgr.get_or_load("g1", tid, 0).await.err(),
        ];

        assert!(
            (loads, mgr.peek_for_test("g1", tid, 0).is_none())
                == ([Some(codes::COORDINATOR_NOT_AVAILABLE); 2], true)
        );
    }

    /// Only the cell that a fenced request held leaves the cache.
    #[tokio::test]
    async fn invalidate_cell_keeps_a_replacement() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([23; 16]);
        let stale = Arc::new(tokio::sync::Mutex::new(AcquisitionState::new(Offset(0))));
        let replacement = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));

        mgr.invalidate_cell("g1", tid, 0, &stale);
        let kept = mgr
            .peek_for_test("g1", tid, 0)
            .is_some_and(|cached| Arc::ptr_eq(&cached, &replacement));
        mgr.invalidate_cell("g1", tid, 0, &replacement);

        assert!((kept, mgr.peek_for_test("g1", tid, 0).is_none()) == (true, true));
    }

    #[tokio::test]
    async fn invalidate_removes_cached_cell() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([24; 16]);

        let cell = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        let cached = mgr
            .peek_for_test("g1", tid, 0)
            .is_some_and(|peeked| Arc::ptr_eq(&cell, &peeked));
        mgr.invalidate("g1", tid, 0);

        assert!((cached, mgr.peek_for_test("g1", tid, 0).is_none()) == (true, true));
    }
}
