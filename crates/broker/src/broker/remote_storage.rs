//! KIP-405 tiered-storage bring-up: the object-store backend, the remote-log
//! metadata manager (topic-backed or in-memory), the diskless cold-read
//! handle, and the remote-log-manager copy task. It is its own module because
//! the backend selection and its WORM layering are self-contained.

use std::sync::Arc;

use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{BrokerConfig, RemoteStorageBackend, RlmmKind},
    diskless::{read::DisklessReadHandle, wal_index::WalIndexCache},
    error::BrokerError,
    metadata_source::MetadataSource,
    metrics::{BrokerMetrics, RemoteReaderLevels, RemoteReaderTotals},
    partition_registry::PartitionRegistry,
    remote_log_manager::{
        ArchiveMode, RemoteLogManagerConfig, RemoteLogManagerContext, SweepConcurrency, run,
    },
    remote_reader::{ReaderPool, RemoteReader, RemoteReaderLimits},
};

#[derive(Default)]
pub(super) struct RemoteStorageStartup {
    pub(super) reader: Option<Arc<RemoteReader>>,
    pub(super) swap_target: Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    pub(super) diskless_read: Option<Arc<DisklessReadHandle>>,
}

fn build_diskless_read_handle(
    backend: &RemoteStorageBackend,
) -> Result<Arc<DisklessReadHandle>, BrokerError> {
    let store_config = match backend {
        RemoteStorageBackend::Local { dir } => {
            krabka_object_store::ObjectStoreConfig::Local { root: dir.clone() }
        }
        RemoteStorageBackend::S3(config) => {
            krabka_object_store::ObjectStoreConfig::S3(config.clone())
        }
        RemoteStorageBackend::Gcs(config) => {
            krabka_object_store::ObjectStoreConfig::Gcs(config.clone())
        }
    };
    let store = krabka_object_store::build_object_store(&store_config).map_err(|error| {
        BrokerError::Startup(format!("diskless object store builder failed: {error}"))
    })?;
    Ok(Arc::new(DisklessReadHandle::new(
        Arc::new(tokio::sync::Mutex::new(WalIndexCache::default())),
        store,
    )))
}

pub(super) fn start_remote_storage(
    config: &BrokerConfig,
    partitions: &Arc<PartitionRegistry>,
    controller: &Arc<dyn MetadataSource>,
    metrics: &BrokerMetrics,
    shutdown: &CancellationToken,
) -> Result<RemoteStorageStartup, BrokerError> {
    let Some(backend) = config.remote_storage_backend.clone() else {
        return Ok(RemoteStorageStartup::default());
    };
    let diskless_read = Some(build_diskless_read_handle(&backend)?);
    // WORM layers over whichever object store was selected; it is not a
    // backend of its own. `BrokerConfig::validate` already rejects the
    // local pairing, so the arm below only fires for a config built in code.
    let worm = config.remote_storage_worm.as_ref();
    let archive = ArchiveMode::from_worm(worm);
    let with_worm = |store: krabka_remote_storage::S3RemoteStorage| match worm {
        Some(worm) => store.with_worm(worm).map_err(|error| {
            BrokerError::Startup(format!("remote_storage.worm setup failed: {error}"))
        }),
        None => Ok(store),
    };
    let storage: Arc<dyn krabka_remote_storage::RemoteStorageManager> = match backend {
        RemoteStorageBackend::Local { dir } => {
            // Fail loudly rather than archive without manifests: a local
            // directory cannot enforce write-once, so a WORM deployment that
            // silently ran on one would produce an archive no auditor can
            // trust and nobody would find out until the audit.
            if worm.is_some() {
                return Err(BrokerError::Startup(
                    "remote_storage.worm requires an object-store backend; a local storage \
                     directory cannot enforce write-once"
                        .into(),
                ));
            }
            Arc::new(krabka_remote_storage::LocalTieredStorage::new(dir))
        }
        RemoteStorageBackend::S3(s3) => Arc::new(with_worm(
            krabka_remote_storage::S3RemoteStorage::from_s3_config(&s3).map_err(|error| {
                BrokerError::Startup(format!("remote_storage.s3 builder failed: {error}"))
            })?,
        )?),
        RemoteStorageBackend::Gcs(gcs) => Arc::new(with_worm(
            krabka_remote_storage::S3RemoteStorage::from_gcs_config(&gcs).map_err(|error| {
                BrokerError::Startup(format!("remote_storage.gcs builder failed: {error}"))
            })?,
        )?),
    };
    let (metadata, swap_target): (
        Arc<dyn krabka_remote_storage::RemoteLogMetadataManager>,
        Option<Arc<krabka_remote_storage_topic::SwappableRlmm>>,
    ) = match &config.remote_log_metadata {
        RlmmKind::TopicBacked(_) => {
            let not_ready = Arc::new(krabka_remote_storage_topic::NotReadyRlmm::new());
            let swap = Arc::new(krabka_remote_storage_topic::SwappableRlmm::new(not_ready));
            (swap.clone(), Some(swap))
        }
        RlmmKind::InMemory => (
            Arc::new(krabka_remote_storage::InmemoryRemoteLogMetadataManager::new()),
            None,
        ),
    };
    // KIP-405's reader bounds. A cache directory that cannot be opened is not
    // a reason to refuse to serve the tier: the reader falls back to
    // downloading each index per fetch, which is what it did before the cache
    // existed, and the startup warning says so.
    let index_cache = match krabka_remote_storage::RemoteIndexCache::new(
        &config.log_dir,
        config.remote_index_cache_size.bytes_u64(),
    ) {
        Ok(cache) => cache,
        Err(error) => {
            tracing::warn!(
                error = %error,
                dir = %config.log_dir.display(),
                "remote index cache unavailable; every cold fetch will re-download its indexes"
            );
            krabka_remote_storage::RemoteIndexCache::disabled()
        }
    };
    let index_cache = Arc::new(index_cache);
    tokio::spawn(run(
        RemoteLogManagerContext {
            partitions: Arc::clone(partitions),
            controller: Arc::clone(controller),
            archive,
            rsm: Arc::clone(&storage),
            rlmm: Arc::clone(&metadata),
            index_cache: Arc::clone(&index_cache),
            metrics: metrics.clone(),
            node_id: config.node_id,
            broker_id: config.broker_id,
        },
        RemoteLogManagerConfig {
            interval: config.remote_log_manager_interval,
            copy_timeout: config.remote_copy_timeout,
            concurrency: SweepConcurrency {
                copier: config.remote_copier_threads,
                expiration: config.remote_expiration_threads,
            },
            unstable_api_versions: config.features.unstable_api_versions,
        },
        shutdown.child_token(),
    ));
    let reader = Arc::new(RemoteReader::with_limits(
        storage,
        metadata,
        RemoteReaderLimits {
            index_cache,
            pool: ReaderPool::new(
                config.remote_reader_threads,
                config.remote_reader_max_pending_tasks,
            ),
        },
    ));
    spawn_remote_reader_gauges(
        Arc::clone(&reader),
        metrics.clone(),
        config.gauge_poll_interval,
        shutdown.child_token(),
    );
    Ok(RemoteStorageStartup {
        reader: Some(reader),
        swap_target,
        diskless_read,
    })
}

/// Publishes the reader pool's depth and idle share and the index cache's
/// hit / miss counters on the broker's gauge cadence.
///
/// The pool and the cache keep their own totals, so the sampler turns each
/// monotonic total into the increment its counter has not seen yet. It is its
/// own task rather than a branch of the broker gauge updater because the
/// reader does not exist until this module has built the backend, which is
/// after that updater is spawned.
fn spawn_remote_reader_gauges(
    reader: Arc<RemoteReader>,
    metrics: BrokerMetrics,
    poll_interval: krabka_units::Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(poll_interval.to_std());
        let mut reported = RemoteReaderTotals::default();
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                () = shutdown.cancelled() => return,
            }
            let stats = reader.index_cache.stats();
            let current = RemoteReaderTotals {
                index_cache_hits: stats.hits,
                index_cache_misses: stats.misses,
                index_cache_evictions: stats.evictions,
                rejected_reads: u64::try_from(reader.pool.rejected()).unwrap_or(u64::MAX),
            };
            metrics.observe_remote_reader(
                &reported,
                current,
                RemoteReaderLevels {
                    task_queue_size: u64::try_from(reader.pool.queue_size()).unwrap_or(u64::MAX),
                    idle_percent: reader.pool.idle_percent(),
                    index_cache_bytes: stats.bytes,
                    index_cache_entries: stats.entries,
                },
            );
            reported = current;
        }
    });
}
