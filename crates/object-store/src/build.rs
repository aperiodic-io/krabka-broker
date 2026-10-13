//! Construction of an `object_store::ObjectStore` handle from a config.

#[cfg(not(target_family = "wasm"))]
use crate::config::{GcsConfig, S3Config};
#[cfg(target_family = "wasm")]
use crate::unavailable::unavailable;
#[cfg(not(target_family = "wasm"))]
use object_store::{ClientOptions, RetryConfig};
use {
    super::{error::ObjectStoreError, object_store_api as object_store},
    crate::config::ObjectStoreConfig,
    object_store::ObjectStore,
    std::sync::Arc,
};

/// The retry budget `cfg` asks for.
///
/// The backoff curve is `object_store`'s: what an operator turns is how many
/// attempts a request gets and how long the whole sequence may take, not the
/// shape of the sleeps between them.
#[cfg(not(target_family = "wasm"))]
fn retry_config(max_retries: usize, retry_timeout: std::time::Duration) -> RetryConfig {
    RetryConfig {
        backoff: object_store::BackoffConfig::default(),
        max_retries,
        retry_timeout,
    }
}

/// The HTTP-client bounds `cfg` asks for, on top of `base`.
///
/// Both timeouts are always set rather than left to the crate's defaults: a
/// store that stalls has to hit a bound the operator chose, and a value that
/// only exists inside a dependency is not one they can turn.
#[cfg(not(target_family = "wasm"))]
fn client_options(
    base: ClientOptions,
    allow_http: bool,
    request_timeout: std::time::Duration,
    connect_timeout: std::time::Duration,
) -> ClientOptions {
    base.with_allow_http(allow_http)
        .with_timeout(request_timeout)
        .with_connect_timeout(connect_timeout)
}

/// Build an `object_store` handle for `cfg`.
///
/// The builder wiring covers the credential chains, the endpoints, and
/// `allow_http`. It is identical to the wiring in the per-crate constructors.
///
/// # Errors
///
/// Returns [`ObjectStoreError::InvalidConfig`] if the backend builder rejects
/// the combination of bucket, region, endpoint, and credentials. On
/// `wasm32-wasip1`, also for every backend other than
/// [`ObjectStoreConfig::InMemory`].
pub fn build_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    match cfg {
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::S3(s3) => build_s3(s3),
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::Gcs(gcs) => Ok(Arc::new(build_gcs_store(gcs)?)),
        #[cfg(not(target_family = "wasm"))]
        ObjectStoreConfig::Local { root } => {
            let store = object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(|e| ObjectStoreError::InvalidConfig(format!("local: {e}")))?;
            Ok(Arc::new(store))
        }
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::S3(_) => Err(unavailable("S3")),
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::Gcs(_) => Err(unavailable("GCS")),
        #[cfg(target_family = "wasm")]
        ObjectStoreConfig::Local { .. } => Err(unavailable("local filesystem")),
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
    }
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn build_s3_store(
    cfg: &S3Config,
) -> Result<object_store::aws::AmazonS3, ObjectStoreError> {
    let mut builder = krabka_macros::connection_policy!(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_allow_http(cfg.allow_http),
        cfg
    );
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let (Some(k), Some(s)) = (&cfg.access_key_id, &cfg.secret_access_key) {
        builder = builder.with_access_key_id(k).with_secret_access_key(s);
    }
    if cfg.conditional_put {
        builder = builder.with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);
    }
    if cfg.checksum_sha256 {
        builder = builder.with_checksum_algorithm(object_store::aws::Checksum::SHA256);
    }
    builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("S3 builder: {e}")))
}

#[cfg(not(target_family = "wasm"))]
fn build_s3(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    Ok(Arc::new(build_s3_store(cfg)?))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn build_gcs_store(
    cfg: &GcsConfig,
) -> Result<object_store::gcp::GoogleCloudStorage, ObjectStoreError> {
    let mut builder =
        object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(&cfg.bucket);
    if let Some(path) = &cfg.service_account_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = &cfg.service_account_key {
        builder = builder.with_service_account_key(key);
    }
    if let Some(adc) = &cfg.application_credentials_path {
        builder = builder.with_application_credentials(adc);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    builder = krabka_macros::connection_policy!(builder, cfg);
    let store = builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("GCS builder: {e}")))?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        crate::{
            config::{GcsConfig, ObjectStoreConfig, S3Config},
            test_support::{HttpListener, http_listener, round_trip, s3_config},
        },
        assert2::{assert, check},
        object_store::ObjectStoreExt as _,
    };

    #[test]
    fn inmemory_builds() {
        assert!(build_object_store(&ObjectStoreConfig::InMemory).is_ok());
    }

    #[tokio::test]
    async fn inmemory_round_trips() {
        let store = build_object_store(&ObjectStoreConfig::InMemory).unwrap();
        let path = object_store::path::Path::from("t/x");
        let got = round_trip(&*store, &path, b"hi").await;
        assert!(&got[..] == b"hi");
    }

    #[test]
    fn local_builds_against_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ObjectStoreConfig::Local {
            root: dir.path().to_path_buf(),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn s3_builds_with_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio:9000".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// The integrity knobs are opt-out. Turning both off must still yield a
    /// usable builder: the flags select request headers, not a different
    /// backend.
    #[test]
    fn s3_builds_with_integrity_knobs_off() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            conditional_put: false,
            checksum_sha256: false,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// The default config turns both knobs on, so the common path must build
    /// with conditional put and SHA-256 checksums wired in.
    #[test]
    fn s3_builds_with_integrity_knobs_on() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from crates/remote-storage/src/gcs.rs tests: with every credential
    // field None, the builder selects Workload Identity / ADC and constructs.
    #[test]
    fn gcs_workload_identity_builds() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from gcs.rs tests: a custom endpoint + allow_http builds.
    #[test]
    fn gcs_honors_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            endpoint: Some("http://fake-gcs:4443".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    /// A store that accepts the connection and then answers nothing must not
    /// hang the caller: `request_timeout` is the bound, and it is the operator
    /// who set it.
    #[tokio::test]
    async fn s3_request_timeout_bounds_a_stalled_store() {
        let HttpListener { listener, endpoint } = http_listener().await;
        // Hold the accepted sockets open for the length of the test: dropping
        // them would answer the request with a reset, which is not a stall.
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                held.push(socket);
            }
        });

        let store = build_object_store(&ObjectStoreConfig::S3(S3Config {
            max_retries: 0,
            request_timeout: std::time::Duration::from_millis(200),
            ..s3_config("b", endpoint)
        }))
        .unwrap();

        let started = std::time::Instant::now();
        let result = store.get(&object_store::path::Path::from("stalled")).await;
        let elapsed = started.elapsed();
        server.abort();

        assert!(result.is_err());
        check!(
            elapsed < std::time::Duration::from_secs(5),
            "a 200ms request timeout let a stalled store block for {elapsed:?}"
        );
    }

    /// `max_retries` is the attempt budget, not a suggestion: a store that
    /// answers `503 SlowDown` to everything must be given exactly
    /// `max_retries + 1` attempts before the error reaches the caller.
    #[tokio::test]
    async fn s3_max_retries_bounds_the_attempts_against_a_throttling_store() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let HttpListener { listener, endpoint } = http_listener().await;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&chunk[..read]),
                    }
                }
                if request.is_empty() {
                    continue;
                }
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = "<Error><Code>SlowDown</Code><Message>Please reduce your \
                            request rate.</Message></Error>";
                let response = format!(
                    "HTTP/1.1 503 Slow Down\r\nContent-Length: {}\r\nConnection: \
                     close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });

        let store = build_object_store(&ObjectStoreConfig::S3(S3Config {
            max_retries: 2,
            retry_timeout: std::time::Duration::from_secs(30),
            request_timeout: std::time::Duration::from_secs(5),
            ..s3_config("b", endpoint)
        }))
        .unwrap();

        let result = store
            .get(&object_store::path::Path::from("throttled"))
            .await;
        server.abort();

        assert!(result.is_err());
        check!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 3);
    }
}
