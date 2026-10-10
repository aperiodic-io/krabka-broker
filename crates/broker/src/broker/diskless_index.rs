//! Bring-up of the diskless WAL index projection and the object flusher that
//! rides on it. The retry loop mirrors the topic-backed RLMM bootstrap beside
//! it but targets a different topic and hands off to the flusher, so it is a
//! module of its own.

use std::sync::{Arc, atomic::AtomicBool};

use krabka_remote_storage_topic::MetadataEventLog;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::rlmm::{KafkaSwapKickoff, metadata_log_config, rlmm_bootstrap_backoff},
    error::BrokerError,
    partition_registry::PartitionRegistry,
};

/// Opens the index event log for one bootstrap attempt.
///
/// Every attempt opens a *fresh* log, which is the point: a
/// `KafkaMetadataEventLog` partition whose fetch loop died while connecting
/// stays silent for the life of its subscription, so a stalled replay only
/// recovers on new connections. Tests substitute an in-process log to reach
/// that retry.
#[async_trait::async_trait]
trait IndexLogSource: Send + Sync {
    async fn open(&self) -> Result<Arc<dyn MetadataEventLog>, BrokerError>;
}

struct KafkaIndexLogSource {
    log_config: krabka_remote_storage_topic::KafkaMetadataLogConfig,
}

#[async_trait::async_trait]
impl IndexLogSource for KafkaIndexLogSource {
    async fn open(&self) -> Result<Arc<dyn MetadataEventLog>, BrokerError> {
        krabka_remote_storage_topic::KafkaMetadataEventLog::start(self.log_config.clone())
            .await
            .map(|log| log as Arc<dyn MetadataEventLog>)
            .map_err(|error| BrokerError::Txn(format!("diskless WAL index log start: {error}")))
    }
}

pub(super) struct DisklessFlusherStartup {
    /// Where each new index log is published so `DeleteRecords` can write its
    /// floor through whichever incarnation is live. Cleared while the
    /// bootstrap is rebuilding one.
    pub(super) index_log_slot:
        Arc<arc_swap::ArcSwapOption<crate::diskless::index_log::DisklessIndexLog>>,
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) image_rx: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    pub(super) object_store: Arc<dyn object_store::ObjectStore>,
    pub(super) node_id: krabka_metadata::NodeId,
    pub(super) broker_id: i32,
    pub(super) metrics: crate::metrics::BrokerMetrics,
    pub(super) flush_config: crate::diskless::flusher::FlushConfig,
    pub(super) ready: Arc<AtomicBool>,
}

pub(super) async fn bootstrap_diskless_index_log(
    cache: Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
    config: KafkaSwapKickoff,
    flusher: DisklessFlusherStartup,
    shutdown: CancellationToken,
) {
    let source = KafkaIndexLogSource {
        log_config: index_log_config(&config.cfg, config.broker_id),
    };
    bootstrap_from_source(&source, cache, &config, &flusher, &shutdown).await;
}

fn index_event_max_bytes(config: &crate::config::KafkaRlmmConfig) -> usize {
    // Leave the other half of the frame for Kafka's request envelope.
    (config.frame_max.bytes() / 2).max(1)
}

fn index_log_config(
    config: &crate::config::KafkaRlmmConfig,
    broker_id: i32,
) -> krabka_remote_storage_topic::KafkaMetadataLogConfig {
    let mut log_config = metadata_log_config(
        config,
        crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC.to_owned(),
        format!("krabka-diskless-index-broker-{broker_id}"),
    );
    let fetch_max =
        ByteSize::from_bytes(u64::try_from(index_event_max_bytes(config)).unwrap_or(u64::MAX));
    if log_config.fetch_max_bytes > fetch_max {
        log_config.fetch_max_bytes = fetch_max;
    }
    log_config.compacted = true;
    // `min_isr` is Kafka's setting for `__remote_log_metadata`. The index topic
    // keeps the cluster default.
    log_config.min_isr = None;
    log_config
}

async fn bootstrap_from_source(
    source: &dyn IndexLogSource,
    cache: Arc<tokio::sync::Mutex<crate::diskless::wal_index::WalIndexCache>>,
    config: &KafkaSwapKickoff,
    flusher: &DisklessFlusherStartup,
    shutdown: &CancellationToken,
) {
    let mut backoff = config.bootstrap_backoff_initial;
    loop {
        let opened = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            result = source.open() => result,
        };
        let index_log = match opened {
            Ok(log) => {
                crate::diskless::index_log::DisklessIndexLog::start_with_cache(
                    log,
                    Arc::clone(&cache),
                    flusher.metrics.clone(),
                )
                .await
            }
            Err(error) => Err(error),
        };
        match index_log {
            Ok(index_log) => {
                tracing::info!(
                    topic = crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC,
                    "diskless WAL index projection started; flusher waits for its replay"
                );
                flusher
                    .index_log_slot
                    .store(Some(Arc::new(index_log.clone())));
                // `flusher.ready` flips inside `run`, once the projection has
                // replayed the index topic and the first tick may fire.
                let exit = crate::diskless::flusher::run(
                    crate::diskless::flusher::FlusherContext {
                        partitions: Arc::clone(&flusher.partitions),
                        image_rx: flusher.image_rx.clone(),
                        object_store: Arc::clone(&flusher.object_store),
                        index_log: index_log.clone(),
                        node_id: flusher.node_id,
                        broker_id: flusher.broker_id,
                        metrics: flusher.metrics.clone(),
                        ready: Arc::clone(&flusher.ready),
                    },
                    flusher.flush_config.clone(),
                    shutdown.clone(),
                )
                .await;
                // Keep an invalid projection visible so cold reads fail closed
                // while the next attempt opens. A merely stalled connection is
                // cleared so writers cannot retain it during reconnect.
                if index_log.is_valid() {
                    flusher.index_log_slot.store(None);
                }
                match exit {
                    crate::diskless::flusher::FlusherExit::ShutDown => return,
                    crate::diskless::flusher::FlusherExit::ReplayStalled => {
                        tracing::warn!(
                            topic = crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC,
                            backoff_ms = backoff.as_millis(),
                            "diskless WAL index replay stalled; rebuilding the index log"
                        );
                    }
                    crate::diskless::flusher::FlusherExit::ProjectionUnavailable => {
                        tracing::error!(
                            topic = crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC,
                            backoff_ms = backoff.as_millis(),
                            "diskless WAL index projection became unavailable; rebuilding it"
                        );
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, backoff_ms = backoff.as_millis(),
                    "diskless WAL index log start failed; retrying");
            }
        }
        if !rlmm_bootstrap_backoff(&mut backoff, config.bootstrap_backoff_max, shutdown).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    fn test_startup(ready: Arc<AtomicBool>) -> DisklessFlusherStartup {
        let (_image_tx, image_rx) = tokio::sync::watch::channel(Arc::new(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
        ));
        DisklessFlusherStartup {
            index_log_slot: Arc::new(arc_swap::ArcSwapOption::empty()),
            partitions: Arc::new(PartitionRegistry::new()),
            image_rx,
            object_store: Arc::new(object_store::memory::InMemory::new()),
            node_id: krabka_raft::NodeId(7),
            broker_id: 1,
            metrics: crate::metrics::BrokerMetrics::new(),
            flush_config: crate::diskless::flusher::FlushConfig {
                interval: std::time::Duration::from_millis(1),
                index_projection_timeout: std::time::Duration::from_millis(50),
                ..crate::diskless::flusher::FlushConfig::default()
            },
            ready,
        }
    }

    fn test_kickoff() -> KafkaSwapKickoff {
        KafkaSwapKickoff {
            cfg: crate::config::KafkaRlmmConfig::default(),
            broker_id: 1,
            bootstrap_backoff_initial: std::time::Duration::from_millis(1),
            bootstrap_backoff_max: std::time::Duration::from_millis(5),
            reconcile_tick: std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn index_replay_fetch_stays_within_the_event_frame_budget() {
        let mut config = crate::config::KafkaRlmmConfig {
            frame_max: krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
                .unwrap(),
            ..crate::config::KafkaRlmmConfig::default()
        };

        assert!(index_log_config(&config, 1).fetch_max_bytes == krabka_units::kibibytes(16));

        config.fetch_max_bytes = krabka_units::kibibytes(4);
        assert!(index_log_config(&config, 1).fetch_max_bytes == krabka_units::kibibytes(4));
    }

    #[test]
    fn index_topic_keeps_the_cluster_default_min_isr() {
        let config = crate::config::KafkaRlmmConfig::default();

        assert!(index_log_config(&config, 1).min_isr.is_none());
    }

    /// Hands out a silent-but-open replay first, then a healthy one, and
    /// counts the attempts.
    struct StallThenHealthySource {
        inner: Arc<dyn MetadataEventLog>,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl IndexLogSource for StallThenHealthySource {
        async fn open(&self) -> Result<Arc<dyn MetadataEventLog>, BrokerError> {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                return Ok(
                    crate::diskless::index_log::test_support::PacedReplayLog::new(
                        Arc::clone(&self.inner),
                        crate::diskless::index_log::test_support::ReplayPace::Never,
                    ),
                );
            }
            Ok(Arc::clone(&self.inner))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn diskless_index_bootstrap_rebuilds_the_log_after_a_stalled_replay() {
        // A populated index topic, so the first attempt has a backlog to
        // replay and its silent stream really does stall.
        let event_log: Arc<dyn MetadataEventLog> =
            krabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let seed = crate::diskless::index_log::DisklessIndexLog::start(Arc::clone(&event_log))
            .await
            .unwrap();
        seed.publish_flush(&crate::diskless::wal_index::WalFlushRecord {
            object_key: "diskless-wal/1/seed.ckwl".into(),
            format_version: crate::diskless::wal_index::WalFlushRecord::FORMAT_VERSION,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id: uuid::Uuid::from_u128(11),
                partition: 0,
                first_offset: 0,
                last_offset: 2,
                byte_start: 0,
                byte_len: 10,
                max_timestamp_ms: 0,
            }],
        })
        .await
        .unwrap();

        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = StallThenHealthySource {
            inner: event_log,
            attempts: Arc::clone(&attempts),
        };
        let ready = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let kickoff = test_kickoff();
        let startup = test_startup(Arc::clone(&ready));
        let bootstrap = bootstrap_from_source(
            &source,
            Arc::new(tokio::sync::Mutex::new(
                crate::diskless::wal_index::WalIndexCache::default(),
            )),
            &kickoff,
            &startup,
            &shutdown,
        );
        tokio::pin!(bootstrap);

        // The stalled attempt must not end the bootstrap: it reopens the log,
        // and only the second, healthy replay lets the flusher start.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    () = &mut bootstrap => panic!("bootstrap gave up instead of rebuilding"),
                    () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
                }
                if ready.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
            }
        })
        .await
        .expect("a stalled replay is retried on a fresh log");

        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 2);
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), bootstrap)
            .await
            .expect("cancelled diskless index bootstrap returns promptly");
    }

    #[tokio::test]
    async fn diskless_index_bootstrap_retries_until_cancelled() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = listener.local_addr().unwrap().to_string();
        drop(listener);

        let snapshot_dir = tempdir().unwrap();
        let kickoff = crate::broker::rlmm::test_kickoff(crate::config::KafkaRlmmConfig {
            bootstrap,
            num_partitions: 1,
            replication: 1,
            snapshot_dir: snapshot_dir.path().to_path_buf(),
            ..crate::config::KafkaRlmmConfig::default()
        });
        let flusher = DisklessFlusherStartup {
            flush_config: crate::diskless::flusher::FlushConfig::default(),
            ..test_startup(Arc::new(AtomicBool::new(false)))
        };
        let shutdown = CancellationToken::new();
        let bootstrap = bootstrap_diskless_index_log(
            Arc::new(tokio::sync::Mutex::new(
                crate::diskless::wal_index::WalIndexCache::default(),
            )),
            kickoff,
            flusher,
            shutdown.clone(),
        );
        tokio::pin!(bootstrap);

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut bootstrap)
                .await
                .is_err()
        );
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), bootstrap)
            .await
            .expect("cancelled diskless index bootstrap returns promptly");
    }
}
