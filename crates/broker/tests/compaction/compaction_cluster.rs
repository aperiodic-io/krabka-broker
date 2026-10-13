//! The one broker this suite runs against, configured with a one-second
//! cleaner interval.
//!
//! The interval is the whole reason the boot is not the shared `support`
//! helper: compaction is only observable here because the cleaner wakes far
//! more often than it does by default.

use {
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    std::net::SocketAddr,
    tempfile::TempDir,
};

/// Start a single PLAINTEXT broker with a 1s cleaner interval.
pub(crate) async fn start_broker_with_fast_cleaner() -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.cleaner_interval_override = Some(krabka_units::secs(1));
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}
