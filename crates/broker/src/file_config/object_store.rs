//! The object-store TOML shapes: `[remote_storage.s3]`,
//! `[remote_storage.gcs]`, and `[remote_storage.worm]`.
//!
//! The two backend tables carry credential material, so each derives a
//! redacting `Debug`. The WORM table sits beside them because write-once mode
//! layers over whichever of the two backends is selected and cannot be used
//! with the local filesystem backend.

use krabka_units::Time;
use schemars::JsonSchema;
use serde::Deserialize;

/// TOML shape of `[remote_storage.s3]`. Maps to
/// [`krabka_remote_storage::S3Config`].
#[krabka_macros::human_units]
#[derive(Clone, Deserialize, JsonSchema, PartialEq, derive_more::Debug)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageS3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region. Required even for non-AWS endpoints (use any value).
    pub region: String,
    /// Optional key prefix inside the bucket (lets multiple clusters
    /// share a bucket).
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (e.g. `MinIO` or Cloudflare R2).
    pub endpoint: Option<String>,
    /// Explicit access key id. Falls back to the AWS credential chain
    /// (env vars, instance profile, …) when omitted.
    #[debug("{:?}", access_key_id.as_ref().map(|_| "***"))]
    pub access_key_id: Option<String>,
    /// Explicit secret access key. Falls back to the AWS credential chain
    /// when omitted.
    #[debug("{:?}", secret_access_key.as_ref().map(|_| "***"))]
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP (off-by-default; required by `MinIO` running
    /// without TLS).
    #[serde(default)]
    pub allow_http: bool,
    /// Optional override of the multipart-upload threshold (bytes). When
    /// `None`, [`krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`]
    /// applies. Operators typically leave this alone; lower it to force
    /// multipart on smaller segments for testing.
    pub multipart_threshold: Option<u64>,
    /// Optional override of the per-part multipart chunk size (bytes).
    /// When `None`, [`krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`]
    /// applies. AWS requires parts ≥ 5 MiB except the last; `MinIO`
    /// tolerates smaller values.
    pub multipart_chunk_size: Option<usize>,
    /// Optional override of conditional puts (`If-None-Match`), which make a
    /// create-mode write fail on an existing key instead of overwriting it.
    /// When `None`, the [`krabka_remote_storage::S3Config`] default of `true`
    /// applies. Turn it off only for an S3-compatible store that mishandles
    /// the header; WORM archive mode relies on it.
    #[serde(default)]
    pub conditional_put: Option<bool>,
    /// Optional override of the `x-amz-checksum-sha256` header, which has the
    /// server verify each object on ingest. When `None`, the
    /// [`krabka_remote_storage::S3Config`] default of `true` applies.
    #[serde(default)]
    pub checksum_sha256: Option<bool>,
    /// How many times one object-store request is retried before the error
    /// reaches the broker. When `None`, [`krabka_object_store::DEFAULT_MAX_RETRIES`]
    /// (10) applies; `0` disables retries. Under a store that answers
    /// `503 SlowDown` this is what bounds how long one call holds its
    /// caller before the failure is counted and the sweep moves on.
    pub max_retries: Option<usize>,
    /// Ceiling on the wall-clock time one request may spend across all of
    /// its retries. When `None`,
    /// [`krabka_object_store::DEFAULT_RETRY_TIMEOUT`] (3m) applies. Keep it
    /// under 5 minutes: retries reuse the original request's credentials.
    pub retry_timeout: Option<Time>,
    /// Ceiling on one HTTP request, connect phase included. When `None`,
    /// [`krabka_object_store::DEFAULT_REQUEST_TIMEOUT`] (30s) applies. This
    /// is the bound on a store that accepts the connection and then
    /// answers nothing.
    pub request_timeout: Option<Time>,
    /// Ceiling on the connect phase alone. When `None`,
    /// [`krabka_object_store::DEFAULT_CONNECT_TIMEOUT`] (5s) applies.
    pub connect_timeout: Option<Time>,
}

/// TOML shape of `[remote_storage.gcs]`. Maps to
/// [`krabka_remote_storage::GcsConfig`].
///
/// Omitting all credential fields (`service_account_path`,
/// `service_account_key`, `application_credentials_path`) selects GKE
/// Workload Identity / Application Default Credentials (keyless) — the
/// primary production path.
#[krabka_macros::gcs_fields(file)]
#[krabka_macros::human_units]
#[derive(Clone, Deserialize, JsonSchema, PartialEq, derive_more::Debug)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageGcsConfig {
    /// Allow plaintext HTTP (off-by-default; required by emulators
    /// running without TLS).
    #[serde(default)]
    pub allow_http: bool,
    /// Optional override of the multipart-upload threshold (bytes). When
    /// `None`, [`krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`]
    /// applies. Operators typically leave this alone; lower it to force
    /// multipart on smaller segments for testing.
    pub multipart_threshold: Option<u64>,
    /// Optional override of the per-part multipart chunk size (bytes).
    /// When `None`, [`krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`]
    /// applies.
    pub multipart_chunk_size: Option<usize>,
    /// How many times one object-store request is retried before the error
    /// reaches the broker. When `None`, [`krabka_object_store::DEFAULT_MAX_RETRIES`]
    /// (10) applies; `0` disables retries. Under a store that answers
    /// `503 SlowDown` this is what bounds how long one call holds its
    /// caller before the failure is counted and the sweep moves on.
    pub max_retries: Option<usize>,
    /// Ceiling on the wall-clock time one request may spend across all of
    /// its retries. When `None`,
    /// [`krabka_object_store::DEFAULT_RETRY_TIMEOUT`] (3m) applies. Keep it
    /// under 5 minutes: retries reuse the original request's credentials.
    pub retry_timeout: Option<Time>,
    /// Ceiling on one HTTP request, connect phase included. When `None`,
    /// [`krabka_object_store::DEFAULT_REQUEST_TIMEOUT`] (30s) applies. This
    /// is the bound on a store that accepts the connection and then
    /// answers nothing.
    pub request_timeout: Option<Time>,
    /// Ceiling on the connect phase alone. When `None`,
    /// [`krabka_object_store::DEFAULT_CONNECT_TIMEOUT`] (5s) applies.
    pub connect_timeout: Option<Time>,
}

/// TOML shape of `[remote_storage.worm]`. Maps to
/// [`krabka_remote_storage::WormConfig`]. Presence of the table enables WORM
/// archive mode.
///
/// Unlike [`FileRemoteStorageS3Config`] this derives `Debug` plainly, and that
/// is deliberate: it holds a *path* to a signing key and the key's public id,
/// neither of which is credential material, and an operator debugging a chain
/// needs to see which key signed it. Do not "fix" this into a redacting impl.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileWormConfig {
    /// Path to the `PKCS#8` Ed25519 key that signs each segment manifest.
    ///
    /// Default: unset. Manifests then carry no signature, and the archive
    /// keeps only the per-object digests and the hash chain. Setting this
    /// requires `signing_key_id` as well.
    #[serde(default)]
    pub signing_key_path: Option<String>,
    /// Stable identifier recorded in every manifest signature, so a chain
    /// stays verifiable across a key rotation.
    ///
    /// Default: unset. Setting this requires `signing_key_path` as well.
    #[serde(default)]
    pub signing_key_id: Option<String>,
    /// Refuse every remote fetch from this archive.
    ///
    /// Default: `false`. When `true`, remote fetch is unavailable: a consumer
    /// that asks for an offset whose local segment has already been evicted
    /// gets an error, not a slow read. The archive is then a compliance sink,
    /// not a storage tier.
    #[serde(default)]
    pub write_only: bool,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::file_config::{FileConfig, FileConfigError};

    fn configured_s3(source: &str) -> krabka_remote_storage::S3Config {
        let config = crate::file_config::test_support::configured_unwrap_parse(source).unwrap();
        match config.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => s3,
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let cfg = FileRemoteStorageS3Config {
            bucket: "logs".to_string(),
            region: "us-east-1".to_string(),
            prefix: None,
            endpoint: None,
            access_key_id: Some("AKIAEXAMPLEKEYID".to_string()),
            secret_access_key: Some("super-secret-key-value".to_string()),
            allow_http: false,
            multipart_threshold: None,
            multipart_chunk_size: None,
            conditional_put: None,
            checksum_sha256: None,
            max_retries: None,
            retry_timeout: None,
            request_timeout: None,
            connect_timeout: None,
        };
        let dbg = format!("{cfg:?}");
        // Secrets are redacted; non-secret fields are still printed.
        let cases = [
            ("super-secret-key-value", false),
            ("AKIAEXAMPLEKEYID", false),
            ("***", true),
            ("logs", true),
            ("us-east-1", true),
        ];
        for (needle, want) in cases {
            assert!(dbg.contains(needle) == want, "needle {needle:?} in: {dbg}");
        }
    }
    #[test]
    fn remote_storage_s3_section_parses() {
        let toml = r#"
[remote_storage.s3]
bucket = "krabka-prod"
region = "us-east-1"
prefix = "cluster-a"
endpoint = "http://minio:9000"
allow_http = true
"#;
        let s3 = configured_s3(toml);
        // Credentials default to None and the multipart knobs default
        // when the TOML omits them.
        check!(s3.bucket.as_str() == "krabka-prod");
        check!(s3.region.as_str() == "us-east-1");
        check!(s3.prefix.as_deref() == Some("cluster-a"));
        check!(s3.endpoint.as_deref() == Some("http://minio:9000"));
        check!(s3.allow_http);
        check!(s3.access_key_id.is_none());
        check!(s3.secret_access_key.is_none());
        check!(s3.multipart_threshold == krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD);
        check!(s3.multipart_chunk_size == krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE);
    }
    #[test]
    fn remote_storage_s3_section_round_trips_multipart_overrides() {
        let toml = r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"
multipart_threshold = 8192
multipart_chunk_size = 5242880
"#;
        let s3 = configured_s3(toml);
        assert!(s3.multipart_threshold == 8192);
        assert!(s3.multipart_chunk_size == 5_242_880);
    }
    #[test]
    fn remote_storage_gcs_section_parses() {
        let toml = r#"
[remote_storage.gcs]
bucket = "krabka-prod"
prefix = "cluster-a"
endpoint = "http://fake-gcs:4443"
allow_http = true
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Gcs(g)) => {
                // Leaving all credential fields unset selects Workload
                // Identity / ADC; multipart knobs default when the TOML
                // omits them.
                check!(g.bucket.as_str() == "krabka-prod");
                check!(g.prefix.as_deref() == Some("cluster-a"));
                check!(g.endpoint.as_deref() == Some("http://fake-gcs:4443"));
                check!(g.allow_http);
                check!(g.service_account_path.is_none());
                check!(g.service_account_key.is_none());
                check!(g.application_credentials_path.is_none());
                check!(g.multipart_threshold == krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD);
                check!(
                    g.multipart_chunk_size == krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE
                );
            }
            other => panic!("expected Gcs backend, got {other:?}"),
        }
    }
    #[test]
    fn remote_storage_gcs_credentials_parse() {
        let toml = r#"
[remote_storage.gcs]
bucket = "b"
service_account_path = "/etc/gcs/key.json"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Gcs(g)) => {
                assert!(g.bucket == "b");
                assert!(g.service_account_path.as_deref() == Some("/etc/gcs/key.json"));
            }
            other => panic!("expected Gcs backend, got {other:?}"),
        }
    }
    #[test]
    fn remote_storage_gcs_config_debug_redacts_credentials() {
        let gcs = FileRemoteStorageGcsConfig {
            bucket: "krabka-prod".into(),
            prefix: None,
            service_account_path: Some("/etc/gcs/sa-path.json".into()),
            service_account_key: Some("super-secret-inline-key".into()),
            application_credentials_path: Some("/etc/gcs/adc.json".into()),
            endpoint: None,
            allow_http: false,
            multipart_threshold: None,
            multipart_chunk_size: None,
            max_retries: None,
            retry_timeout: None,
            request_timeout: None,
            connect_timeout: None,
        };
        let rendered = format!("{gcs:?}");
        // All three credential fields are redacted; non-secret fields are
        // still printed.
        let cases = [
            ("/etc/gcs/sa-path.json", false),
            ("super-secret-inline-key", false),
            ("/etc/gcs/adc.json", false),
            ("***", true),
            ("krabka-prod", true),
        ];
        for (needle, want) in cases {
            assert!(
                rendered.contains(needle) == want,
                "needle {needle:?} in: {rendered}"
            );
        }
    }
    #[test]
    fn worm_table_maps_to_broker_config() {
        let toml = r#"
[remote_storage.s3]
bucket = "krabka-archive"
region = "us-east-1"

[remote_storage.worm]
signing_key_path = "/etc/krabka/worm-signing.pk8"
signing_key_id = "worm-2026-q3"
write_only = true
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        check!(
            cfg.remote_storage_worm
                == Some(krabka_remote_storage::WormConfig {
                    signing_key_path: Some(std::path::PathBuf::from(
                        "/etc/krabka/worm-signing.pk8"
                    )),
                    signing_key_id: Some("worm-2026-q3".to_string()),
                    write_only: true,
                })
        );
    }
    #[test]
    fn worm_table_defaults_to_unsigned_readable_archive() {
        let toml = r#"
[remote_storage.gcs]
bucket = "krabka-archive"

[remote_storage.worm]
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        // An empty table still enables WORM; every knob takes its default.
        check!(cfg.remote_storage_worm == Some(krabka_remote_storage::WormConfig::default()));
    }
    #[test]
    fn worm_rejects_invalid_combinations() {
        for (label, source, needle) in [
            (
                "local backend cannot enforce write-once",
                "[remote_storage]\nstorage_dir = \"/tmp/tier\"\n\
                 [remote_storage.worm]\n",
                "storage_dir",
            ),
            (
                "worm with a local backend and a key set is still rejected",
                "[remote_storage]\nstorage_dir = \"/tmp/tier\"\n\
                 [remote_storage.worm]\nsigning_key_path = \"/k.pk8\"\n\
                 signing_key_id = \"k1\"\n",
                "storage_dir",
            ),
            (
                "no backend at all",
                "[remote_storage.worm]\nwrite_only = true\n",
                "[remote_storage.s3]",
            ),
            (
                "key path without an id",
                "[remote_storage.s3]\nbucket = \"b\"\nregion = \"us-east-1\"\n\
                 [remote_storage.worm]\nsigning_key_path = \"/k.pk8\"\n",
                "signing_key_id",
            ),
            (
                "key id without a path",
                "[remote_storage.s3]\nbucket = \"b\"\nregion = \"us-east-1\"\n\
                 [remote_storage.worm]\nsigning_key_id = \"k1\"\n",
                "signing_key_path",
            ),
        ] {
            let file: FileConfig = toml::from_str(source).expect("parse worm syntax");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("invalid worm config must fail");
            check!(
                matches!(error, FileConfigError::InvalidConfig(_)),
                "{label}: expected InvalidConfig, got {error:?}"
            );
            let rendered = error.to_string();
            check!(
                rendered.contains(needle),
                "{label}: message must name {needle:?}, got: {rendered}"
            );
        }
    }
    #[test]
    fn worm_absent_leaves_config_none() {
        let toml = r#"
[remote_storage.s3]
bucket = "krabka-prod"
region = "us-east-1"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        check!(cfg.remote_storage_worm.is_none());
    }
    #[test]
    fn worm_config_debug_shows_the_key_id_and_path() {
        // Deliberately NOT redacted: neither field is credential material,
        // and an operator auditing a chain must be able to tell which key
        // signed it. A `***` here would remove the only answer to that.
        let worm = FileWormConfig {
            signing_key_path: Some("/etc/krabka/worm-signing.pk8".into()),
            signing_key_id: Some("worm-2026-q3".into()),
            write_only: true,
        };
        let rendered = format!("{worm:?}");
        check!(rendered.contains("/etc/krabka/worm-signing.pk8"));
        check!(rendered.contains("worm-2026-q3"));
        check!(!rendered.contains("***"));
    }

    /// The four durability bounds reach `S3Config` from the TOML, and each
    /// one that the TOML omits falls back to the backend's own default
    /// rather than to a value restated here.
    #[test]
    fn remote_storage_s3_bounds_parse_and_default() {
        let toml = r#"
[remote_storage.s3]
bucket = "krabka-prod"
region = "us-east-1"
max_retries = 3
request_timeout = "10s"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        let defaults = krabka_remote_storage::S3Config::default();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => {
                check!(s3.max_retries == 3);
                check!(s3.request_timeout == std::time::Duration::from_secs(10));
                check!(s3.retry_timeout == defaults.retry_timeout);
                check!(s3.connect_timeout == defaults.connect_timeout);
            }
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    /// The same four bounds on the GCS table.
    #[test]
    fn remote_storage_gcs_bounds_parse_and_default() {
        let toml = r#"
[remote_storage.gcs]
bucket = "krabka-prod"
retry_timeout = "45s"
connect_timeout = "2s"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        let defaults = krabka_remote_storage::GcsConfig::default();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Gcs(gcs)) => {
                check!(gcs.retry_timeout == std::time::Duration::from_secs(45));
                check!(gcs.connect_timeout == std::time::Duration::from_secs(2));
                check!(gcs.max_retries == defaults.max_retries);
                check!(gcs.request_timeout == defaults.request_timeout);
            }
            other => panic!("expected GCS backend, got {other:?}"),
        }
    }

    /// `0` disables retries, and it has to survive the `Option` plumbing:
    /// `unwrap_or(default)` on a `Some(0)` would otherwise be indistinguishable
    /// from an omitted key if the mapping ever collapsed to `unwrap_or_default`.
    #[test]
    fn zero_max_retries_disables_retries_rather_than_defaulting() {
        let toml = r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"
max_retries = 0
"#;
        let s3 = configured_s3(toml);
        check!(s3.max_retries == 0);
    }
}
