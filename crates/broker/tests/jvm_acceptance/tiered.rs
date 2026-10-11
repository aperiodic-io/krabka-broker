//! Broker bring-up with the S3 tiered-storage backend pointed at `MinIO`.
//!
//! The helper shortens the `RemoteLogManager` tick so a copy and a local
//! eviction happen inside the test's wall clock rather than at the production
//! default.

use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use super::{
    ports::{broker0_advertised, broker0_listen},
    tiered_workload::TIERED_SEGMENT_SIZE,
};
use crate::{jvm_acceptance::host_broker_config, support::init_jvm_tracing};

/// Same shape as [`start_host_broker`] but with the S3 tiered-storage
/// backend wired in and a lower `RemoteLogManager` tick, so the acceptance
/// loop completes in seconds rather than at the 30s production default.
///
/// `rlmm` selects the [`krabka_broker::RlmmKind`]. Pass
/// `RlmmKind::InMemory` for tests that only need a single-run round-trip.
/// Pass `RlmmKind::TopicBacked(…)` when the test needs durable metadata that
/// survives a broker restart.
///
/// Returns the broker handle, the temp dir, and the `BrokerConfig` so the
/// caller can reuse it for a restart. The caller must keep the temp dir
/// alive.
pub(crate) fn start_host_broker_with_minio_tier(
    s3: krabka_remote_storage::S3Config,
    rlmm: krabka_broker::RlmmKind,
) -> impl std::future::Future<
    Output = (
        krabka_broker::BrokerHandle,
        tempfile::TempDir,
        krabka_broker::BrokerConfig,
    ),
> {
    init_jvm_tracing("krabka_broker=debug,info");
    let dir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig {
        // The tiered topics these suites create override no segment size —
        // no old JVM `TopicCommand` can name a sub-1-MiB one — so they
        // inherit this broker default. See `TIERED_SEGMENT_SIZE`.
        log_config: LogConfig {
            segment_size: TIERED_SEGMENT_SIZE,
            ..LogConfig::default()
        },
        remote_storage_backend: Some(krabka_broker::RemoteStorageBackend::S3(s3)),
        // 1s tick so the producer's sealed segments reach S3 (and the
        // local-retention pass evicts them) within the test's wall clock.
        remote_log_manager_interval: krabka_units::secs(1),
        remote_log_metadata: rlmm,
        ..host_broker_config(dir.path(), "static addr")
    };
    Box::pin(async move {
        let handle = Broker::start(config.clone()).await.expect("start broker");
        eprintln!(
            "KRABKA[test] broker started listen={listen} advertised={bootstrap} (tiered S3 backend)",
            bootstrap = broker0_advertised(),
            listen = broker0_listen()
        );
        (handle, dir, config)
    })
}
