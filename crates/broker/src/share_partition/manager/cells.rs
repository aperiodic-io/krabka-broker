//! The lazily loaded acquisition-state cells: the load-on-miss path, the
//! test-only peek, and the invalidation the admin offset RPCs use.
//!
//! This is the only module that inserts into or removes from the `leaders`
//! map, so the rule that no `DashMap` guard is held across an `.await` is
//! checkable by reading one file.

use {
    super::{
        SharePartitionLeaderManager,
        persistence::{fences_the_partition, persister_error_code},
    },
    crate::{
        codes::{KAFKA_STORAGE_ERROR, OFFSET_NOT_AVAILABLE, REQUEST_TIMED_OUT, from_broker_error},
        config_keys::resolve_remote_list_offsets_timeout,
        coordinator::unified::streams::config::ShareAutoOffsetReset,
        handlers::list_offsets::timestamp::resolve_timestamp_with_sources,
        share_partition::{source::diskless_logical_start, state::AcquisitionState},
        time_util::{duration_millis, now_ms},
    },
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_remote_storage::TopicIdPartition,
    std::sync::Arc,
    tokio::sync::Mutex,
    tracing::warn,
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
        let config = local
            .log
            .lock()
            .expect("log mutex poisoned")
            .config_snapshot();
        let log_start = if local.diskless
            && let Some(reader) = self.diskless_read.get()
        {
            diskless_logical_start(reader, &local, topic_id)
                .await
                .map_err(|error| from_broker_error(&error))?
        } else if config.remote_storage_enable
            && let Some(reader) = self.remote_reader.get()
        {
            let tp = TopicIdPartition::new(topic_id, local.topic.clone(), partition);
            let remote = tokio::time::timeout(
                resolve_remote_list_offsets_timeout(&image, self.node_id),
                reader.earliest_offset(&tp),
            )
            .await
            .map_err(|_| REQUEST_TIMED_OUT)?
            .map_err(|_| KAFKA_STORAGE_ERROR)?;
            let log = local.log.lock().expect("log mutex poisoned");
            Offset(remote.map_or(log.log_start_offset().0, |remote| {
                remote.min(log.log_start_offset().0)
            }))
            .max(log.established_log_start().unwrap_or(Offset(0)))
        } else {
            local.log_start_offset()
        };
        match strategy {
            ShareAutoOffsetReset::Earliest => Ok(log_start),
            ShareAutoOffsetReset::Latest => Ok(local.high_watermark().await.max(log_start)),
            ShareAutoOffsetReset::ByDuration(duration) => {
                let target = now_ms().saturating_sub(duration_millis(duration));
                let tp = TopicIdPartition::new(topic_id, local.topic.clone(), partition);
                let found = resolve_timestamp_with_sources(
                    &local,
                    &tp,
                    target,
                    resolve_remote_list_offsets_timeout(&image, self.node_id),
                    self.remote_reader
                        .get()
                        .filter(|_| config.remote_storage_enable)
                        .map(Arc::as_ref),
                    self.diskless_read
                        .get()
                        .filter(|_| local.diskless)
                        .map(Arc::as_ref),
                )
                .await?;
                // A compressed record above the topic's trunk
                // `max.decompressed.message.bytes` fails the lookup:
                // `UnifiedLog.fetchOffsetByTimestamp` throws
                // `InvalidRecordException` through
                // `ReplicaManager.fetchOffsetForTimestamp` into the share
                // partition's initialization.
                // Every record predates the window: Kafka's
                // `ShareFetchUtils.offsetForTimestamp` throws, and the
                // initialization of the share partition fails.
                (found.0 >= 0)
                    .then_some(Offset(found.0).max(log_start))
                    .ok_or(OFFSET_NOT_AVAILABLE)
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

    use {
        crate::{
            codes,
            coordinator::unified::streams::config::KEY_SHARE_AUTO_OFFSET_RESET,
            handlers::list_offsets::timestamp::resolve_timestamp_with_sources,
            partition::test_support::{test_partition, update_log_config},
            partition_registry::PartitionRegistry,
            remote_reader::test_support::{sparse_remote_segment_reader_with_max_timestamp, tp},
            share_partition::{
                manager::test_support::{
                    manager, manager_with_image_and_partitions, open_data_partition,
                },
                source::test_support::diskless_partition,
                state::AcquisitionState,
            },
            test_support::append_partition_batch,
            time_util::now_ms,
        },
        assert2::assert,
        bytes::Bytes,
        krabka_ids::PartitionIndex,
        krabka_log::{LogConfig, Offset},
        krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord, NodeId, TopicRecord},
        krabka_protocol::records::{Record, RecordBatch},
        krabka_remote_storage::TopicIdPartition,
        krabka_units::bytes,
        std::{sync::Arc, time::Duration},
    };

    krabka_macros::single_replica_partition_fixture!(partition_record);

    #[tokio::test]
    async fn diskless_initialization_resolves_earliest_and_duration_from_wal_history() {
        let record_time = now_ms() - 30 * 60 * 1_000;
        let (partition, reader, tid, _dir) = diskless_partition(record_time).await;
        // This is the real flusher's trim seam, not tiered segment eviction:
        // its established local checkpoint must not hide readable WAL bytes.
        assert!(partition.log.lock().unwrap().established_log_start() == Some(Offset(2)));
        assert!(reader.index.lock().await.delete_floor(tid, 0) == 0);
        let registry = Arc::new(PartitionRegistry::new());
        registry.insert("t".into(), PartitionIndex(0), Arc::clone(&partition));
        let mgr = manager_with_image_and_partitions(
            image_with_strategies(tid, [
                ("earliest", "earliest"),
                ("duration", "by_duration:PT1H"),
            ]),
            registry,
        );
        mgr.set_read_sources(None, Some(reader.clone()));
        for floor in [0, 1] {
            reader.index.lock().await.raise_delete_floor(tid, 0, floor);
            assert!(
                resolve_timestamp_with_sources(
                    &partition,
                    &TopicIdPartition::new(tid, "t", 0),
                    record_time - 1,
                    Duration::from_secs(1),
                    None,
                    Some(&reader),
                )
                .await
                    == Ok((floor, record_time))
            );
            for group in ["earliest", "duration"] {
                assert!(
                    mgr.initial_start_offset(group, tid, 0).await == Ok(Offset(floor)),
                    "{group} floor {floor}"
                );
            }
        }
    }

    #[tokio::test]
    async fn duration_initialization_selects_an_archived_match_before_local_data() {
        let (reader, _archive) = sparse_remote_segment_reader_with_max_timestamp(2_400);
        let tid = tp().topic_id;
        let (partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        {
            let mut log = partition.log.lock().unwrap();
            log.set_config(LogConfig {
                segment_size: bytes(1),
                remote_storage_enable: true,
                ..log.config_snapshot()
            });
            for _ in 0..18 {
                log.append(&mut RecordBatch {
                    base_timestamp: 3_000,
                    max_timestamp: 3_000,
                    records: vec![Record {
                        value: Some(Bytes::from_static(b"local")),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .unwrap();
            }
            log.delete_local_segments_through(Offset(17)).unwrap();
        }
        partition.replica_state.lock().await.hw = Offset(18);
        let registry = Arc::new(PartitionRegistry::new());
        registry.insert("t".into(), PartitionIndex(0), Arc::new(partition));
        let mgr = manager_with_image_and_partitions(
            image_with_strategies(tid, [("duration", "by_duration:P100000D")]),
            registry,
        );
        mgr.set_read_sources(Some(Arc::new(reader)), None);
        assert!(mgr.initial_start_offset("duration", tid, 0).await == Ok(Offset(10)));
    }

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
        let now = now_ms();
        let hour = 60 * 60 * 1_000;
        let reg = std::sync::Arc::new(PartitionRegistry::new());
        open_data_partition()
            .reg(&reg)
            .log_dir(dir.path())
            .batches(vec![
                crate::share_partition::manager::test_support::TimedValues {
                    timestamp: crate::test_support::UnixMillis(now - 3 * hour),
                    values: &[b"stale-0", b"stale-1"],
                },
                crate::share_partition::manager::test_support::TimedValues {
                    timestamp: crate::test_support::UnixMillis(now - hour - hour / 2),
                    values: &[b"recent-0", b"recent-1"],
                },
            ])
            .high_watermark(4)
            .call()
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
        let now = now_ms();
        let hour = 60 * 60 * 1_000;
        let reg = Arc::new(PartitionRegistry::new());
        open_data_partition()
            .reg(&reg)
            .log_dir(dir.path())
            .high_watermark(2)
            .call()
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
            append_partition_batch(&partition, &mut batch);
        }
        let mgr = manager_with_image_and_partitions(
            image_with_strategies(tid, [
                ("two-hours", "by_duration:PT2H"),
                ("four-hours", "by_duration:PT4H"),
            ]),
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
            update_log_config(&partition, |config| {
                config.max_decompressed_record = limit;
            });
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
