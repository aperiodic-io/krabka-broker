//! Creation of a KIP-113 future log: the entry points that stage a
//! `<topic>-<partition>-future` directory and start its replicator task.
//!
//! `start_move` serves an `AlterReplicaLogDirs` request and `resume_move`
//! picks up a move that a crash interrupted. Both end in `spawn_move`, which
//! registers the `FutureLogState` and spawns the replicator.

use {
    super::{
        FutureLogState, MoveError, MovePolicy, canonicalize_or_self,
        cleanup::cancel_move,
        configured_log_dir, future_dir_name_too_long,
        replicator::{ReplicatorTask, replicator_loop},
    },
    crate::{
        error::BrokerError, log_dir, log_dir_status::LogDirRegistry, partition::Partition,
        partition_registry::PartitionRegistry,
    },
    dashmap::DashMap,
    krabka_ids::PartitionIndex,
    krabka_log::{Log, LogConfig},
    std::{
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
    tokio_util::sync::CancellationToken,
};

/// Start a move of `(topic, partition)` to `target_log_dir`, or confirm an
/// identical move as a no-op. Returns immediately after it spawns the
/// replicator task, so the `AlterReplicaLogDirs` handler can then ack success.
///
/// Idempotency: if a move with the same target is already in flight,
/// returns `Ok(())` without spawning a second task. If its target differs,
/// the old task and future log are removed before the replacement starts.
#[allow(clippy::too_many_arguments)] // KIP-113 move inputs are independent runtime resources.
pub(crate) async fn start_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    all_log_dirs: &[PathBuf],
    log_dir_status: &LogDirRegistry,
    log_config: &LogConfig,
    topic_partition: (&str, PartitionIndex),
    topic_id: Option<uuid::Uuid>,
    target_log_dir: &Path,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let (topic, partition) = topic_partition;
    // (0) Kafka refuses a move whose future directory name would pass 255
    //     characters before it looks at the destination.
    if future_dir_name_too_long(topic, partition.get()) {
        return Err(MoveError::TopicNameTooLong);
    }
    // (1) Validate the target is a configured log.dir, by Kafka's string
    //     equality with the configured absolute path.
    let Some(target_log_dir) = configured_log_dir(all_log_dirs, target_log_dir) else {
        return Err(MoveError::LogDirNotFound);
    };
    if log_dir_status.is_offline(&target_log_dir) {
        return Err(MoveError::Storage(BrokerError::Io(std::io::Error::other(
            "target log directory is offline",
        ))));
    }
    // (1b) KIP-1066: a cordoned directory takes no new replica.
    if partitions.cordoned_log_dirs().is_cordoned(&target_log_dir) {
        return Err(MoveError::Cordoned);
    }

    // (2) Partition must be hosted on this broker.
    let key = (topic.to_string(), partition);
    let part = if let Some(part) = partitions.get(topic, partition) {
        part
    } else {
        if let Some(topic_id) = topic_id {
            partitions.set_preferred_log_dir(topic_id, partition, target_log_dir.clone());
        }
        let Some(part) = partitions.get(topic, partition) else {
            return Err(MoveError::ReplicaNotAvailable);
        };
        // Materialization won the race before it could consume the
        // preference. Continue as an ordinary live-replica move without
        // leaving stale placement state behind.
        if let Some(topic_id) = topic_id {
            partitions.clear_preferred_log_dir(topic_id, partition);
        }
        part
    };

    // (3) Already moving? Keep a same-target request idempotent. Kafka stops
    //     and removes a future replica when a later request changes its
    //     destination, including when the new destination is the current dir.
    if let Some(existing) = future_logs.get(&key).map(|e| e.value().clone()) {
        if canonicalize_or_self(&existing.target_log_dir) == canonicalize_or_self(&target_log_dir) {
            return Ok(());
        }
        cancel_move(future_logs, &key, existing).await?;
    }

    // (4) Already in the target dir? No-op success. This check must follow
    //     cancellation so redirecting a move back to its source stops it.
    let current_log_dir = part.log_dir.load_full();
    if canonicalize_or_self(&current_log_dir) == canonicalize_or_self(&target_log_dir) {
        return Ok(());
    }

    // (5) Open the future log at <target>/<topic>-<partition>-future.
    let future_path = log_dir::future_partition_dir(&target_log_dir, topic, partition.get());
    std::fs::create_dir_all(&future_path)?;
    let future_log = open_future_log(partitions, &future_path, log_config)?;

    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir,
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

/// Recover an interrupted move discovered on disk at broker startup
/// (a `<topic>-<partition>-future` directory in a configured log.dir
/// whose corresponding partition exists). Re-opens the future log
/// and re-spawns the replicator, picking up at whatever offset the
/// future log already holds.
pub(crate) fn resume_move(
    partitions: &Arc<PartitionRegistry>,
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: &Path,
    log_config: &LogConfig,
    topic: &str,
    partition: PartitionIndex,
    policy: MovePolicy,
) -> Result<(), MoveError> {
    let part = partitions
        .get(topic, partition)
        .ok_or(MoveError::ReplicaNotAvailable)?;
    let future_path = log_dir::future_partition_dir(target_log_dir, topic, partition.get());
    let future_log = open_future_log(partitions, &future_path, log_config)?;
    spawn_move(MoveTask {
        partitions: partitions.clone(),
        future_logs: future_logs.clone(),
        target_log_dir: target_log_dir.to_path_buf(),
        future_path,
        future_log,
        topic: topic.to_string(),
        partition,
        part,
        policy,
    });
    Ok(())
}

fn open_future_log(
    partitions: &PartitionRegistry,
    path: &Path,
    log_config: &LogConfig,
) -> Result<Arc<Mutex<Log>>, MoveError> {
    let mut log = Log::open(path, log_config.clone())?;
    if let Some(stamp_source) = partitions.stamp_source() {
        log.set_stamp_source(stamp_source)?;
    }
    Ok(Arc::new(Mutex::new(log)))
}

/// Shared by [`start_move`] and [`resume_move`]. It builds the
/// `FutureLogState`, inserts it into the registry, and spawns the
/// per-move replicator task.
struct MoveTask {
    partitions: Arc<PartitionRegistry>,
    future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    target_log_dir: PathBuf,
    future_path: PathBuf,
    future_log: Arc<Mutex<Log>>,
    topic: String,
    partition: PartitionIndex,
    part: Arc<Partition>,
    policy: MovePolicy,
}

fn spawn_move(task: MoveTask) {
    let cancel = CancellationToken::new();
    let target_partition_path =
        log_dir::partition_dir(&task.target_log_dir, &task.topic, task.partition.get());
    let replicator = tokio::spawn(replicator_loop(ReplicatorTask {
        part: task.part,
        future_log: task.future_log.clone(),
        future_path: task.future_path.clone(),
        target_partition_path,
        target_log_dir: task.target_log_dir.clone(),
        cancel: cancel.clone(),
        _partitions: task.partitions,
        future_logs: task.future_logs.clone(),
        topic: task.topic.clone(),
        partition: task.partition,
        policy: task.policy,
    }));
    let state = Arc::new(FutureLogState {
        target_log_dir: task.target_log_dir,
        future_path: task.future_path,
        future_log: task.future_log,
        cancel,
        task: std::sync::Mutex::new(Some(replicator)),
    });
    task.future_logs.insert((task.topic, task.partition), state);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::future_log::test_support::{
            PlantedFutureLog, fixture_partition, plant_future_log, registered_source_fixture,
            test_policy,
        },
        assert2::assert,
        std::time::Duration,
        tempfile::tempdir,
    };

    // Move t-0 using the common log config and policy, with caller-owned registries/status.

    async fn move_with_defaults(
        partitions: &Arc<PartitionRegistry>,
        future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
        dirs: &[PathBuf],
        status: &LogDirRegistry,
        topic_id: Option<uuid::Uuid>,
        target: &Path,
    ) -> Result<(), MoveError> {
        // The existing start_move API takes one topic/partition pair.
        start_move(
            partitions,
            future_logs,
            dirs,
            status,
            &LogConfig::default(),
            ("t", PartitionIndex(0)),
            topic_id,
            target,
            test_policy(),
        )
        .await
    }

    fn assert_precreation_preference(partitions: &PartitionRegistry, expected: &Path) {
        assert!(
            partitions
                .preferred_log_dir(uuid::Uuid::from_u128(1), PartitionIndex(0))
                .as_deref()
                == Some(expected)
        );
    }

    /// The log dirs, the partitions and the future logs of a move.
    struct MoveRegistries {
        log_dirs: Vec<PathBuf>,
        partitions: Arc<PartitionRegistry>,
        future_logs: Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    }

    fn empty_move_registries(log_dirs: Vec<PathBuf>) -> MoveRegistries {
        MoveRegistries {
            log_dirs,
            partitions: Arc::new(PartitionRegistry::new()),
            future_logs: Arc::new(DashMap::new()),
        }
    }

    fn registered_move(primary: &Path, target: &Path) -> MoveRegistries {
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            "t".into(),
            PartitionIndex(0),
            fixture_partition(primary, "t", PartitionIndex(0)),
        );
        MoveRegistries {
            log_dirs: vec![primary.to_path_buf(), target.to_path_buf()],
            partitions,
            future_logs: Arc::new(DashMap::new()),
        }
    }

    #[tokio::test]
    async fn move_error_log_dir_not_found_when_target_unknown() {
        // Empty broker — no partitions, no log dirs. `start_move`
        // returns LogDirNotFound before it ever looks at the
        // partition map.
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let log_dirs: Vec<PathBuf> = vec![];
        let bogus = tempdir().unwrap();
        let err = move_with_defaults(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogDirRegistry::default(),
            None,
            bogus.path(),
        )
        .await
        .expect_err("expected LogDirNotFound");
        assert!(matches!(err, MoveError::LogDirNotFound));
    }

    #[tokio::test]
    async fn move_error_replica_not_available_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let dir = tempdir().unwrap();
        let err = move_with_defaults(
            &partitions,
            &future_logs,
            &[dir.path().to_path_buf()],
            &LogDirRegistry::default(),
            Some(uuid::Uuid::from_u128(1)),
            dir.path(),
        )
        .await
        .expect_err("expected ReplicaNotAvailable");
        assert!(matches!(err, MoveError::ReplicaNotAvailable));
        assert_precreation_preference(&partitions, dir.path());
    }

    #[tokio::test]
    async fn invalid_target_does_not_replace_precreation_preference() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let valid = tempdir().unwrap();
        let invalid = tempdir().unwrap();
        partitions.set_preferred_log_dir(
            uuid::Uuid::from_u128(1),
            PartitionIndex(0),
            valid.path().to_path_buf(),
        );

        let err = move_with_defaults(
            &partitions,
            &future_logs,
            &[valid.path().to_path_buf()],
            &LogDirRegistry::default(),
            Some(uuid::Uuid::from_u128(1)),
            invalid.path(),
        )
        .await
        .expect_err("invalid target");

        assert!(matches!(err, MoveError::LogDirNotFound));
        assert_precreation_preference(&partitions, valid.path());
    }

    #[tokio::test]
    async fn a_later_valid_precreation_request_replaces_the_preference() {
        let first = tempdir().unwrap();
        let second = tempdir().unwrap();
        let MoveRegistries {
            log_dirs,
            partitions,
            future_logs,
        } = empty_move_registries(vec![
            first.path().to_path_buf(),
            second.path().to_path_buf(),
        ]);

        for target in [&log_dirs[0], &log_dirs[1]] {
            let error = move_with_defaults(
                &partitions,
                &future_logs,
                &log_dirs,
                &LogDirRegistry::default(),
                Some(uuid::Uuid::from_u128(1)),
                target,
            )
            .await
            .expect_err("the replica is not created yet");
            assert!(matches!(error, MoveError::ReplicaNotAvailable));
        }

        assert_precreation_preference(&partitions, &log_dirs[1]);
    }

    #[tokio::test]
    async fn an_offline_target_does_not_replace_the_preference() {
        let first = tempdir().unwrap();
        let offline = tempdir().unwrap();
        let MoveRegistries {
            log_dirs,
            partitions,
            future_logs,
        } = empty_move_registries(vec![
            first.path().to_path_buf(),
            offline.path().to_path_buf(),
        ]);
        let status = LogDirRegistry::default();
        status.mark_offline(offline.path(), "test failure");
        partitions.set_preferred_log_dir(
            uuid::Uuid::from_u128(1),
            PartitionIndex(0),
            first.path().to_path_buf(),
        );

        let error = move_with_defaults(
            &partitions,
            &future_logs,
            &log_dirs,
            &status,
            Some(uuid::Uuid::from_u128(1)),
            offline.path(),
        )
        .await
        .expect_err("offline target");

        assert!(matches!(error, MoveError::Storage(_)));
        assert_precreation_preference(&partitions, first.path());
    }

    #[tokio::test]
    async fn start_move_to_current_dir_is_noop() {
        // Asking to move a partition to the directory it already
        // lives in returns success without touching `future_logs`.
        let (primary, extra) = (tempdir().unwrap(), tempdir().unwrap());
        let MoveRegistries {
            log_dirs,
            partitions,
            future_logs,
        } = registered_move(primary.path(), extra.path());

        move_with_defaults(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogDirRegistry::default(),
            None,
            primary.path(),
        )
        .await
        .expect("noop should succeed");
        assert!(
            future_logs.is_empty(),
            "noop must not register a future log"
        );
    }

    #[test]
    fn resume_move_errors_when_partition_missing() {
        let partitions = Arc::new(PartitionRegistry::new());
        let future_logs = Arc::new(DashMap::new());
        let target = tempdir().unwrap();

        let err = resume_move(
            &partitions,
            &future_logs,
            target.path(),
            &LogConfig::default(),
            "missing",
            PartitionIndex(0),
            test_policy(),
        )
        .expect_err("missing partition must reject resume");

        assert!(matches!(err, MoveError::ReplicaNotAvailable));
        assert!(future_logs.is_empty());
    }

    #[tokio::test]
    async fn start_move_idempotent_for_same_target() {
        // Two ARLD calls with the same target while the first move is
        // still in flight collapse to one — second call returns Ok(())
        // and the registry still has one entry.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let MoveRegistries {
            log_dirs,
            partitions,
            future_logs,
        } = registered_move(primary.path(), extra.path());

        // Plant a registry entry as if a prior ARLD already kicked off
        // a move — exercises the "already moving, same target" branch
        // without racing the replicator's swap-and-remove.
        plant_future_log(&future_logs, extra.path());

        move_with_defaults(
            &partitions,
            &future_logs,
            &log_dirs,
            &LogDirRegistry::default(),
            None,
            extra.path(),
        )
        .await
        .expect("same-target alter must be idempotent");
        assert!(future_logs.len() == 1);
    }

    #[tokio::test]
    async fn start_move_redirects_conflicting_target() {
        // Kafka stops and removes the old future replica before it starts a
        // replacement toward the new destination.
        let primary = tempdir().unwrap();
        let extra = tempdir().unwrap();
        let third = tempdir().unwrap();
        let log_dirs = vec![
            primary.path().to_path_buf(),
            extra.path().to_path_buf(),
            third.path().to_path_buf(),
        ];
        let source = registered_source_fixture().primary(primary.path()).call();

        // Plant a registry entry pointing at `extra`.
        let PlantedFutureLog {
            path: future_path,
            cancel: old_cancel,
        } = plant_future_log(&source.future_logs, extra.path());

        move_with_defaults(
            &source.partitions,
            &source.future_logs,
            &log_dirs,
            &LogDirRegistry::default(),
            None,
            third.path(),
        )
        .await
        .expect("conflicting-target alter must redirect");

        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let moved = canonicalize_or_self(&source.partition.log_dir.load_full())
                    == canonicalize_or_self(third.path());
                if moved && source.future_logs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement move should complete");

        assert!(old_cancel.is_cancelled());
        assert!(!future_path.exists());
        assert!(source.partition.log_end_offset() == 3);
    }
}
