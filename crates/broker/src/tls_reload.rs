//! Background TLS hot-reload watcher.
//!
//! The watcher polls the cert / key / client-CA paths configured on the
//! broker's [`krabka_security::TlsConfig`]. On any mtime change it rebuilds
//! the `ServerConfig` and swaps it into the shared
//! [`krabka_security::DynamicServerConfig`]. New TLS handshakes get the swap
//! on the next `accept`. The swap does not affect in-flight handshakes.
//!
//! If a rebuild fails, the watcher logs the error at `warn` and the previous
//! config stays in place. This keeps the broker in service with the old cert
//! instead of dropping connections.

use {
    krabka_security::{DynamicServerConfig, TlsConfig},
    krabka_units::{Time, convert::TimeExt},
    std::{path::Path, sync::Arc, time::SystemTime},
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PathMtimes {
    cert: Option<SystemTime>,
    key: Option<SystemTime>,
    client_ca: Option<SystemTime>,
}

fn read_mtime(p: &Path) -> Option<SystemTime> { std::fs::metadata(p).ok()?.modified().ok() }

fn snapshot_mtimes(cfg: &TlsConfig) -> PathMtimes {
    PathMtimes {
        cert: read_mtime(&cfg.cert_chain_path),
        key: read_mtime(&cfg.private_key_path),
        client_ca: cfg.client_ca_path.as_deref().and_then(read_mtime),
    }
}

/// Spawned task entry point. The task polls every `interval` and stops on the
/// `shutdown` token.
pub(crate) async fn run(
    dynamic: Arc<DynamicServerConfig>,
    cfg: TlsConfig,
    interval: Time,
    shutdown: CancellationToken,
) {
    if interval <= <Time as TimeExt>::ZERO {
        info!("tls hot-reload watcher disabled (interval == 0)");
        return;
    }
    let mut last = snapshot_mtimes(&cfg);
    let mut ticker = tokio::time::interval(interval.to_std());
    // First tick fires immediately; skip it so we don't double-load on
    // startup (the broker already built the initial ServerConfig).
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                info!("tls hot-reload watcher shutting down");
                return;
            }
        }
        let now = snapshot_mtimes(&cfg);
        if now == last {
            debug!("tls hot-reload watcher: no change");
            continue;
        }
        match dynamic.reload_from(&cfg) {
            Ok(()) => {
                info!("tls hot-reload watcher: server config swapped");
                last = now;
            }
            Err(e) => {
                warn!(error = %e, "tls hot-reload watcher: reload failed; keeping prior config");
                // Don't update `last` — if the next tick succeeds, we
                // want it to retry against the same input.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::localhost_ecdsa_pair,
        assert2::assert,
        krabka_security::ClientAuthMode,
        krabka_units::secs,
        std::{
            fs,
            path::{Path, PathBuf},
            time::Duration,
        },
    };

    fn install_provider() { let _ = rustls::crypto::ring::default_provider().install_default(); }

    /// A certificate and a key, both PEM encoded.
    struct PemPair {
        cert: String,
        key: String,
    }

    fn generated_pair() -> PemPair {
        let (cert, key) = localhost_ecdsa_pair();
        PemPair {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }

    fn write_file(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, body).unwrap();
        path
    }

    /// The paths of a certificate file and a key file.
    struct PairPaths {
        cert: PathBuf,
        key: PathBuf,
    }

    fn write_pair(dir: &Path, cert: &str, key: &str) -> PairPaths {
        PairPaths {
            cert: write_file(dir, "cert.pem", cert),
            key: write_file(dir, "key.pem", key),
        }
    }

    fn tls_config(cert_chain_path: PathBuf, private_key_path: PathBuf) -> TlsConfig {
        TlsConfig {
            cert_chain_path,
            private_key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        }
    }

    fn bump_mtime(path: &Path, delta: Duration) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_modified(SystemTime::now() + delta).unwrap();
    }

    #[test]
    fn read_mtime_reports_existing_files_and_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "cert.pem", "not a real cert");

        assert!(read_mtime(&path).is_some());
        assert!(read_mtime(&dir.path().join("missing.pem")).is_none());
    }

    #[test]
    fn snapshot_mtimes_captures_cert_key_and_client_ca() {
        let dir = tempfile::tempdir().unwrap();
        let cert = write_file(dir.path(), "cert.pem", "cert");
        let key = write_file(dir.path(), "key.pem", "key");
        let ca = write_file(dir.path(), "client-ca.pem", "ca");
        let cfg = TlsConfig {
            cert_chain_path: cert.clone(),
            private_key_path: key.clone(),
            trust_roots_path: None,
            client_ca_path: Some(ca.clone()),
            client_auth: ClientAuthMode::Required,
        };

        let snapshot = snapshot_mtimes(&cfg);

        let expected = PathMtimes {
            cert: read_mtime(&cert),
            key: read_mtime(&key),
            client_ca: read_mtime(&ca),
        };
        assert!(snapshot != PathMtimes::default());
        assert!(snapshot == expected);
    }

    #[tokio::test(start_paused = true)]
    async fn run_reloads_after_mtime_change_and_skips_unchanged_ticks() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let PemPair {
            cert: cert_a,
            key: key_a,
        } = generated_pair();
        let PairPaths { cert, key } = write_pair(dir.path(), &cert_a, &key_a);
        let cfg = tls_config(cert.clone(), key.clone());
        let dynamic = DynamicServerConfig::from_tls_config(&cfg).unwrap();
        let before = dynamic.current();
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(run(dynamic.clone(), cfg.clone(), secs(1), shutdown.clone()));
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let unchanged = dynamic.current();
        assert!(
            Arc::ptr_eq(&before, &unchanged),
            "unchanged mtimes must not reload"
        );

        let PemPair {
            cert: cert_b,
            key: key_b,
        } = generated_pair();
        fs::write(&cert, cert_b).unwrap();
        fs::write(&key, key_b).unwrap();
        bump_mtime(&cert, Duration::from_mins(1));
        bump_mtime(&key, Duration::from_secs(61));

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let after = dynamic.current();
        assert!(
            !Arc::ptr_eq(&before, &after),
            "changed mtimes must reload the server config"
        );

        shutdown.cancel();
        task.await.unwrap();
    }
}
