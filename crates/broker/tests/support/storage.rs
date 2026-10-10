//! Log-directory fixtures shared by JBOD integration suites.
use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

/// Preserve the primary directory and the one extra directory in configured order.
pub fn two_dir_config(primary: &std::path::Path, extra: &std::path::Path) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(primary.to_path_buf());
    config.extra_log_dirs = vec![extra.to_path_buf()];
    config
}

/// Starts a broker with two fixture log directories.
///
/// # Panics
///
/// Panics if either temporary directory cannot be created or the broker cannot start.
pub fn start_two_dir_broker()
-> impl std::future::Future<Output = (BrokerHandle, TempDir, TempDir, SocketAddr)> {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let cfg = crate::support::storage::two_dir_config(primary.path(), extra.path());
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker start");
        let addr = handle.listen_addr();
        (handle, primary, extra, addr)
    })
}

/// Current segment objects, including legacy objects named `log`, in walk order.
pub fn remote_log_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("log")
                || path.file_name().and_then(|name| name.to_str()) == Some("log")
            {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(root, &mut found);
    found
}

/// Segment objects under root's immediate directories whose names begin with topic.
pub fn topic_remote_log_files(root: &std::path::Path, topic: &str) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let is_topic_dir = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(topic));
        if path.is_dir() && is_topic_dir {
            files.extend(remote_log_files(&path));
        }
    }
    files
}

/// Count immediate topic partition directories, optionally including future moves.
pub fn count_partition_dirs(entries: std::fs::ReadDir, topic: &str, include_future: bool) -> usize {
    let prefix = format!("{topic}-");
    entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with(&prefix) && (include_future || !name.ends_with("-future"))
            })
        })
        .count()
}

/// The owner identity Docker must use for a writable host directory.
///
/// # Panics
/// Panics if the host directory's metadata cannot be read.
#[cfg(unix)]
pub fn host_directory_user(path: &std::path::Path) -> String {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = std::fs::metadata(path).expect("stat the host data directory");
    format!("{}:{}", metadata.uid(), metadata.gid())
}

/// The readiness a storage scenario requires before it reads partition state.
#[derive(Clone, Copy, Default)]
pub(crate) enum PartitionReadiness {
    #[default]
    MetadataPublished,
    LocalWriterPresent,
}
