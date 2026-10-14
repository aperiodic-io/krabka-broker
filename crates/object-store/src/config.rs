//! Object-store connection config types shared across Krabka.

use std::time::Duration;

/// Default threshold above which a segment upload changes from a single PUT to
/// a streaming multipart upload. The threshold is 100 MiB, well below AWS's
/// 5 GiB single-PUT cap.
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 100 * 1024 * 1024;

/// Default per-part size for multipart uploads. The size is 16 MiB. AWS
/// requires >= 5 MiB per non-final part and caps the parts at 10 000, so 16 MiB
/// scales past any real segment.
pub const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Default number of times a single object-store request is retried before the
/// error reaches the caller. It matches `object_store`'s own default of 10.
pub const DEFAULT_MAX_RETRIES: usize = 10;

/// Default ceiling on the wall-clock time one request may spend across all of
/// its retries. It is 3 minutes, `object_store`'s default, which stays under
/// the 5-minute mark past which a request's credentials may expire mid-retry.
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_mins(3);

/// Default ceiling on one HTTP request, connect phase included. It is 30
/// seconds, `object_store`'s default, chosen because an object-store request
/// may transfer a whole segment.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default ceiling on the connect phase alone. It is 5 seconds, matching
/// `object_store`.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Selects and parameterises the object-store backend to construct.
#[derive(Clone, Debug)]
pub enum ObjectStoreConfig {
    /// Any S3-compatible endpoint, such as AWS S3, `MinIO`, or Cloudflare R2.
    S3(S3Config),
    /// Native Google Cloud Storage. It supports keyless GKE Workload Identity.
    Gcs(GcsConfig),
    /// Local filesystem rooted at `root`, for development and test.
    Local { root: std::path::PathBuf },
    /// In-process store, for tests.
    InMemory,
}

/// Connection and bucket parameters for an S3-compatible backend.
///
/// The credentials come either from `access_key_id` and `secret_access_key` or
/// from the standard AWS credential chain. When both fields are `None`,
/// `object_store` falls back to the environment-variable chain.
///
/// `Debug` redacts the credential fields, so a stray `{:?}` call or tracing
/// call never leaks them.
#[krabka_macros::s3_fields(runtime)]
#[krabka_macros::object_store_config]
#[derive(Clone, PartialEq, Eq, derive_more::Debug, krabka_macros::FieldDefaults)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket. No leading slash and no trailing
    /// slash.
    pub prefix: Option<String>,
    /// AWS region. AWS S3 requires it. Use the placeholder `"us-east-1"` for
    /// `MinIO` and R2.
    pub region: String,
    /// Allow plaintext HTTP. `MinIO` without TLS requires it.
    pub allow_http: bool,
    /// Files of at least this size upload with multipart. Defaults to
    /// [`DEFAULT_MULTIPART_THRESHOLD`].
    #[default(DEFAULT_MULTIPART_THRESHOLD)]
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    #[default(DEFAULT_MULTIPART_CHUNK_SIZE)]
    pub multipart_chunk_size: usize,
    /// Enable conditional puts (`If-None-Match`) so a `PutMode::Create` write
    /// fails instead of silently overwriting. Defaults to `true`.
    #[default(true)]
    pub conditional_put: bool,
    /// Send `x-amz-checksum-sha256` so the server verifies each object on
    /// ingest. Defaults to `true`.
    #[default(true)]
    pub checksum_sha256: bool,
}

/// Connection and bucket parameters for native Google Cloud Storage.
///
/// If every credential field is `None`, the config selects Workload Identity or
/// ADC through the metadata server. This is the keyless GKE production path.
///
/// `Debug` redacts the credential fields, so a stray `{:?}` call or tracing
/// call never leaks them.
#[krabka_macros::gcs_fields(runtime)]
#[krabka_macros::object_store_config]
#[derive(Clone, PartialEq, Eq, derive_more::Debug, krabka_macros::FieldDefaults)]
pub struct GcsConfig {
    /// Allow plaintext HTTP. Emulators without TLS require it.
    pub allow_http: bool,
    /// Files of at least this size upload with resumable multipart. Defaults
    /// to [`DEFAULT_MULTIPART_THRESHOLD`].
    #[default(DEFAULT_MULTIPART_THRESHOLD)]
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    #[default(DEFAULT_MULTIPART_CHUNK_SIZE)]
    pub multipart_chunk_size: usize,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[test]
    fn multipart_size_constants() {
        assert!(DEFAULT_MULTIPART_THRESHOLD == 100 * 1024 * 1024);
        assert!(DEFAULT_MULTIPART_CHUNK_SIZE == 16 * 1024 * 1024);
    }

    #[test]
    fn s3_config_default_uses_multipart_constants() {
        let cfg = S3Config::default();
        assert!(cfg.multipart_threshold == DEFAULT_MULTIPART_THRESHOLD);
        assert!(cfg.multipart_chunk_size == DEFAULT_MULTIPART_CHUNK_SIZE);
    }

    #[test]
    fn s3_config_default_enables_integrity_knobs() {
        let cfg = S3Config::default();
        assert!(cfg.conditional_put);
        assert!(cfg.checksum_sha256);
    }

    #[test]
    fn s3_config_debug_shows_integrity_knobs() {
        let dbg = format!("{:?}", S3Config::default());
        assert!(dbg.contains("conditional_put: true"));
        assert!(dbg.contains("checksum_sha256: true"));
    }

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let cfg = S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: Some("AKIASECRET".into()),
            secret_access_key: Some("supersecret".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("AKIASECRET"));
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("***"));
    }

    #[test]
    fn gcs_config_default_uses_multipart_constants() {
        let cfg = GcsConfig::default();
        assert!(cfg.multipart_threshold == DEFAULT_MULTIPART_THRESHOLD);
        assert!(cfg.multipart_chunk_size == DEFAULT_MULTIPART_CHUNK_SIZE);
    }

    #[test]
    fn gcs_config_debug_redacts_credentials() {
        let cfg = GcsConfig {
            bucket: "b".into(),
            service_account_key: Some("{\"private_key\":\"leak\"}".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("leak"));
        assert!(dbg.contains("***"));
    }

    #[test]
    fn object_store_config_debug_redacts_via_inner() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            secret_access_key: Some("supersecret".into()),
            ..Default::default()
        });
        assert!(!format!("{cfg:?}").contains("supersecret"));
    }

    /// The four durability knobs must default to the values documented in
    /// `docs/config-reference.md`, on both backends: an operator who sets
    /// none of them gets exactly this.
    #[yare::parameterized(
        s3 = { "s3", false },
        gcs = { "gcs", true },
    )]
    fn backend_defaults_carry_the_documented_bounds(name: &str, gcs: bool) {
        let (retries, retry, request, connect) = if gcs {
            let cfg = GcsConfig::default();
            (
                cfg.max_retries,
                cfg.retry_timeout,
                cfg.request_timeout,
                cfg.connect_timeout,
            )
        } else {
            let cfg = S3Config::default();
            (
                cfg.max_retries,
                cfg.retry_timeout,
                cfg.request_timeout,
                cfg.connect_timeout,
            )
        };
        check!(retries == 10, "{name}");
        check!(retry == Duration::from_secs(180), "{name}");
        check!(request == Duration::from_secs(30), "{name}");
        check!(connect == Duration::from_secs(5), "{name}");
    }

    /// The bounds are not credential material, so a redacting `Debug` must
    /// still print them: an operator reading a startup log is looking for the
    /// value that is timing their store out.
    #[yare::parameterized(
        s3_retries = { false, "max_retries: 10" },
        s3_retry_timeout = { false, "retry_timeout: 180s" },
        s3_request_timeout = { false, "request_timeout: 30s" },
        s3_connect_timeout = { false, "connect_timeout: 5s" },
        gcs_retries = { true, "max_retries: 10" },
        gcs_retry_timeout = { true, "retry_timeout: 180s" },
        gcs_request_timeout = { true, "request_timeout: 30s" },
        gcs_connect_timeout = { true, "connect_timeout: 5s" },
    )]
    fn debug_prints_the_bounds_on_both_backends(gcs: bool, field: &str) {
        let dbg = if gcs {
            format!("{:?}", GcsConfig::default())
        } else {
            format!("{:?}", S3Config::default())
        };
        check!(dbg.contains(field), "{field} missing from {dbg}");
    }
}
