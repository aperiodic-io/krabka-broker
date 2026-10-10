//! The `[remote_storage]` TOML shape and the KIP-405 backend selection.
//!
//! [`FileRemoteStorageConfig`] names exactly one tiered-storage backend —
//! a local directory, `[remote_storage.s3]`, or `[remote_storage.gcs]` — and
//! `apply_remote_storage` resolves that choice, layers WORM archive mode over
//! an object store, and builds the remote-log metadata manager's policy.

use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt},
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::{
    FileConfigError,
    object_store::{FileRemoteStorageGcsConfig, FileRemoteStorageS3Config, FileWormConfig},
    validate::invalid_runtime_value,
};

/// TOML shape of `[remote_storage]`. Maps to
/// [`crate::BrokerConfig::remote_storage_backend`].
///
/// Exactly one of `storage_dir` (local filesystem), `[remote_storage.s3]`
/// (S3-compatible object store), or `[remote_storage.gcs]` (native Google
/// Cloud Storage) should be set. Setting more than one errors at load time.
#[krabka_macros::human_units]
#[krabka_macros::config_table(strict)]
pub struct FileRemoteStorageConfig {
    /// Root directory for the local `LocalTieredStorage` backend.
    pub storage_dir: Option<String>,
    /// S3-compatible backend parameters. Omit to use `storage_dir`.
    pub s3: Option<FileRemoteStorageS3Config>,
    /// Native Google Cloud Storage backend parameters. Omit to use
    /// `storage_dir` or `[remote_storage.s3]`.
    pub gcs: Option<FileRemoteStorageGcsConfig>,
    /// WORM archive mode for whichever object-store backend is selected.
    /// Presence of the table turns it on; omit it (the default) for
    /// ordinary mutable tiered storage. Requires `[remote_storage.s3]` or
    /// `[remote_storage.gcs]` — `storage_dir` cannot enforce write-once.
    pub worm: Option<FileWormConfig>,
    /// Opt-in to the topic-backed `RemoteLogMetadataManager`.
    /// When absent, the broker uses the in-memory fixture.
    pub kafka_metadata: Option<FileKafkaRlmmConfig>,
    /// How many partitions may be copying segments to the remote tier at
    /// once. Kafka's `remote.log.manager.copier.thread.pool.size`; defaults
    /// to 10. Raising it is the remedy for a copy backlog on an object store
    /// with bandwidth to spare.
    #[schemars(range(min = 1))]
    pub copier_threads: Option<usize>,
    /// How many partitions may be running their local- and remote-retention
    /// passes at once. Kafka's
    /// `remote.log.manager.expiration.thread.pool.size`; defaults to 10.
    #[schemars(range(min = 1))]
    pub expiration_threads: Option<usize>,
    /// How many cold-tier reads may run at once. Kafka's
    /// `remote.log.reader.threads`; defaults to 10.
    #[schemars(range(min = 1))]
    pub reader_threads: Option<usize>,
    /// How many cold-tier reads may wait for a reader slot before one is
    /// refused. Kafka's `remote.log.reader.max.pending.tasks`; defaults to
    /// 100.
    #[schemars(range(min = 1))]
    pub reader_max_pending_tasks: Option<usize>,
    /// Byte budget of the on-disk cache of remote segment indexes under
    /// `<log_dir>/remote-log-index-cache`. Kafka's
    /// `remote.log.index.file.cache.total.size.bytes`; defaults to 1 GiB.
    pub index_cache_size: Option<ByteSize>,
    /// Deadline on one segment copy to the remote tier. Defaults to 10
    /// minutes.
    ///
    /// A partition holds one copier slot for the whole of its copy pass, so a
    /// copy that hangs on a stalled object store spends that slot on a
    /// partition moving no bytes. Past this deadline the copy is abandoned
    /// and retried on the next tick; the segment stays in
    /// `CopySegmentStarted`, which local retention refuses to delete
    /// against.
    pub copy_timeout: Option<Time>,
}

/// TOML shape of `[remote_storage.kafka_metadata]`. Maps to
/// [`crate::config::KafkaRlmmConfig`].
#[krabka_macros::human_units]
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileKafkaRlmmConfig {
    /// `host:port` the manager dials to reach its own broker.
    /// May be empty; the broker derives the address from the inter-broker
    /// listener at startup.
    #[serde(default)]
    pub bootstrap: String,
    /// Partition count for `__remote_log_metadata` on first creation.
    /// Defaults to 50 (Kafka's
    /// `remote.log.metadata.topic.num.partitions`).
    pub num_partitions: Option<i32>,
    /// Replication factor for `__remote_log_metadata` on first
    /// creation. Defaults to 3 (Kafka's
    /// `remote.log.metadata.topic.replication.factor`).
    pub replication: Option<i32>,
    /// `min.insync.replicas` for `__remote_log_metadata` on first creation.
    /// Defaults to 2 (Kafka's `remote.log.metadata.topic.min.isr`). Keep it at
    /// or below `replication`: a topic whose `min.insync.replicas` exceeds its
    /// replica count rejects every `acks=all` write, so a single-broker setup
    /// that sets `replication = 1` sets this to 1 as well.
    #[schemars(range(min = 1))]
    pub min_isr: Option<i32>,
    /// Timeout for provisioning each internal metadata topic.
    pub topic_create_timeout: Option<Time>,
    /// Maximum wait for each per-partition metadata fetch.
    pub fetch_max_wait: Option<Time>,
    /// Maximum bytes returned by each per-partition metadata fetch.
    pub fetch_max_bytes: Option<ByteSize>,
    /// Backoff after a failed metadata fetch.
    pub fetch_retry_backoff: Option<Time>,
    /// Capacity of the shared metadata-event delivery queue.
    #[schemars(range(min = 1))]
    pub event_queue_capacity: Option<usize>,
    /// RLMM cache snapshot cadence.
    pub snapshot_interval: Option<Time>,
    /// Explicit opt-out: run the non-durable in-memory RLMM instead of the
    /// topic-backed default. Tests / single-node dev only.
    #[serde(default)]
    pub in_memory: bool,
}
/// The `S3Config` `[remote_storage.s3]` describes.
///
/// Every key the TOML omits falls back to the value `S3Config::default()`
/// carries rather than to one restated here, so a changed default cannot
/// silently disagree with the TOML layer.
fn s3_config(s3: &FileRemoteStorageS3Config) -> krabka_remote_storage::S3Config {
    let defaults = krabka_remote_storage::S3Config::default();
    krabka_remote_storage::S3Config {
        bucket: s3.bucket.clone(),
        region: s3.region.clone(),
        prefix: s3.prefix.clone(),
        endpoint: s3.endpoint.clone(),
        access_key_id: s3.access_key_id.clone(),
        secret_access_key: s3.secret_access_key.clone(),
        allow_http: s3.allow_http,
        multipart_threshold: s3
            .multipart_threshold
            .unwrap_or(defaults.multipart_threshold),
        multipart_chunk_size: s3
            .multipart_chunk_size
            .unwrap_or(defaults.multipart_chunk_size),
        conditional_put: s3.conditional_put.unwrap_or(defaults.conditional_put),
        checksum_sha256: s3.checksum_sha256.unwrap_or(defaults.checksum_sha256),
        max_retries: s3.max_retries.unwrap_or(defaults.max_retries),
        retry_timeout: s3
            .retry_timeout
            .map_or(defaults.retry_timeout, TimeExt::to_std),
        request_timeout: s3
            .request_timeout
            .map_or(defaults.request_timeout, TimeExt::to_std),
        connect_timeout: s3
            .connect_timeout
            .map_or(defaults.connect_timeout, TimeExt::to_std),
    }
}

/// The `GcsConfig` `[remote_storage.gcs]` describes, on the same terms as
/// [`s3_config`].
fn gcs_config(gcs: &FileRemoteStorageGcsConfig) -> krabka_remote_storage::GcsConfig {
    let defaults = krabka_remote_storage::GcsConfig::default();
    krabka_remote_storage::GcsConfig {
        bucket: gcs.bucket.clone(),
        prefix: gcs.prefix.clone(),
        service_account_path: gcs.service_account_path.clone(),
        service_account_key: gcs.service_account_key.clone(),
        application_credentials_path: gcs.application_credentials_path.clone(),
        endpoint: gcs.endpoint.clone(),
        allow_http: gcs.allow_http,
        multipart_threshold: gcs
            .multipart_threshold
            .unwrap_or(defaults.multipart_threshold),
        multipart_chunk_size: gcs
            .multipart_chunk_size
            .unwrap_or(defaults.multipart_chunk_size),
        max_retries: gcs.max_retries.unwrap_or(defaults.max_retries),
        retry_timeout: gcs
            .retry_timeout
            .map_or(defaults.retry_timeout, TimeExt::to_std),
        request_timeout: gcs
            .request_timeout
            .map_or(defaults.request_timeout, TimeExt::to_std),
        connect_timeout: gcs
            .connect_timeout
            .map_or(defaults.connect_timeout, TimeExt::to_std),
    }
}

pub(super) fn apply_remote_storage(
    remote: Option<&FileRemoteStorageConfig>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(rs) = remote else { return Ok(()) };
    if let Some(threads) = rs.reader_threads {
        if threads == 0 {
            return Err(invalid_runtime_value(
                "remote_storage.reader_threads",
                "must be at least 1: a reader pool with no slots would refuse every cold read",
            ));
        }
        cfg.remote_reader_threads = threads;
    }
    if let Some(threads) = rs.copier_threads {
        if threads == 0 {
            return Err(invalid_runtime_value(
                "remote_storage.copier_threads",
                "must be at least 1: a copier bound of zero would let no partition copy a \
                 segment, so nothing would ever reach the tier",
            ));
        }
        cfg.remote_copier_threads = threads;
    }
    if let Some(threads) = rs.expiration_threads {
        if threads == 0 {
            return Err(invalid_runtime_value(
                "remote_storage.expiration_threads",
                "must be at least 1: an expiration bound of zero would stop every retention \
                 pass, so no segment would ever be evicted locally or deleted remotely",
            ));
        }
        cfg.remote_expiration_threads = threads;
    }
    if let Some(pending) = rs.reader_max_pending_tasks {
        if pending == 0 {
            return Err(invalid_runtime_value(
                "remote_storage.reader_max_pending_tasks",
                "must be at least 1: a queue with no room would refuse every cold read that \
                 arrives while another is running",
            ));
        }
        cfg.remote_reader_max_pending_tasks = pending;
    }
    if let Some(size) = rs.index_cache_size {
        if size.bytes_u64() == 0 {
            return Err(invalid_runtime_value(
                "remote_storage.index_cache_size",
                "must be positive: set it above the largest index object, or the cache stores \
                 nothing and every cold fetch re-downloads its indexes",
            ));
        }
        cfg.remote_index_cache_size = size;
    }
    if let Some(timeout) = rs.copy_timeout {
        if timeout <= <Time as TimeExt>::ZERO {
            return Err(invalid_runtime_value(
                "remote_storage.copy_timeout",
                "must be positive: a zero deadline would abandon every copy before it started, \
                 so no segment would ever reach the tier",
            ));
        }
        cfg.remote_copy_timeout = timeout;
    }
    let set_count = usize::from(rs.storage_dir.is_some())
        + usize::from(rs.s3.is_some())
        + usize::from(rs.gcs.is_some());
    if set_count > 1 {
        return Err(FileConfigError::InvalidConfig(
            "[remote_storage] cannot set both/more than one of `storage_dir` \
                     (local), `[remote_storage.s3]` (object store), and \
                     `[remote_storage.gcs]` (Google Cloud Storage)"
                .into(),
        ));
    }
    if let Some(dir) = &rs.storage_dir {
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: std::path::PathBuf::from(dir),
        });
    } else if let Some(s3) = &rs.s3 {
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::S3(s3_config(s3)));
    } else if let Some(gcs) = &rs.gcs {
        cfg.remote_storage_backend =
            Some(crate::config::RemoteStorageBackend::Gcs(gcs_config(gcs)));
    }

    // WORM archive mode layers over whichever object store was just
    // selected. It is a sibling of the backend, not a fourth backend: the
    // same S3 / GCS store is used, with write-once semantics on top.
    if let Some(worm) = &rs.worm {
        // Keyed off the resolved backend, not off `rs`, so a `storage_dir`
        // inherited from the caller's `BrokerConfig` is caught too.
        match &cfg.remote_storage_backend {
            Some(
                crate::config::RemoteStorageBackend::S3(_)
                | crate::config::RemoteStorageBackend::Gcs(_),
            ) => {}
            Some(crate::config::RemoteStorageBackend::Local { .. }) => {
                return Err(FileConfigError::InvalidConfig(
                    "[remote_storage.worm] requires an object-store backend \
                     (`[remote_storage.s3]` or `[remote_storage.gcs]`); \
                     `storage_dir` (local) cannot enforce write-once"
                        .into(),
                ));
            }
            None => {
                return Err(FileConfigError::InvalidConfig(
                    "[remote_storage.worm] requires an object-store backend; \
                     set `[remote_storage.s3]` or `[remote_storage.gcs]`"
                        .into(),
                ));
            }
        }
        match (&worm.signing_key_path, &worm.signing_key_id) {
            (Some(_), None) => {
                return Err(FileConfigError::InvalidConfig(
                    "[remote_storage.worm] cannot set `signing_key_path` \
                     without `signing_key_id`: a key with no id cannot be \
                     selected at verify time, so the signatures it writes are \
                     unverifiable after a rotation"
                        .into(),
                ));
            }
            (None, Some(_)) => {
                return Err(FileConfigError::InvalidConfig(
                    "[remote_storage.worm] cannot set `signing_key_id` \
                     without `signing_key_path`: an unsigned chain proves \
                     continuity but not authorship, and the id alone signs \
                     nothing"
                        .into(),
                ));
            }
            (Some(_), Some(_)) | (None, None) => {}
        }
        cfg.remote_storage_worm = Some(krabka_remote_storage::WormConfig {
            signing_key_path: worm
                .signing_key_path
                .as_deref()
                .map(std::path::PathBuf::from),
            signing_key_id: worm.signing_key_id.clone(),
            write_only: worm.write_only,
        });
    }

    // KIP-405: topic-backed RLMM is the default whenever tiered storage
    // is enabled. `[remote_storage.kafka_metadata]` only overrides the
    // topic knobs; `in_memory = true` is the explicit opt-out.
    if cfg.remote_storage_backend.is_some() {
        let km = rs.kafka_metadata.as_ref();
        if km.is_some_and(|k| k.in_memory) {
            cfg.remote_log_metadata = crate::config::RlmmKind::InMemory;
        } else {
            let mut policy = crate::config::KafkaRlmmConfig {
                bootstrap: km.map(|k| k.bootstrap.clone()).unwrap_or_default(),
                num_partitions: km
                    .and_then(|k| k.num_partitions)
                    .unwrap_or(crate::config::DEFAULT_RLMM_TOPIC_NUM_PARTITIONS),
                replication: km
                    .and_then(|k| k.replication)
                    .unwrap_or(crate::config::DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR),
                min_isr: km
                    .and_then(|k| k.min_isr)
                    .unwrap_or(crate::config::DEFAULT_RLMM_TOPIC_MIN_ISR),
                snapshot_dir: cfg.log_dir.join("remote-log-metadata"),
                ..crate::config::KafkaRlmmConfig::default()
            };
            if let Some(km) = km {
                policy.topic_create_timeout = km
                    .topic_create_timeout
                    .unwrap_or(policy.topic_create_timeout);
                policy.fetch_max_wait = km.fetch_max_wait.unwrap_or(policy.fetch_max_wait);
                policy.fetch_max_bytes = km.fetch_max_bytes.unwrap_or(policy.fetch_max_bytes);
                policy.fetch_retry_backoff =
                    km.fetch_retry_backoff.unwrap_or(policy.fetch_retry_backoff);
                if let Some(capacity) = km.event_queue_capacity {
                    policy.event_queue_capacity =
                        krabka_remote_storage_topic::MetadataEventQueueCapacity::new(capacity)
                            .map_err(|error| {
                                invalid_runtime_value("event_queue_capacity", error)
                            })?;
                }
                policy.snapshot_interval = km.snapshot_interval.unwrap_or(policy.snapshot_interval);
            }
            policy
                .validate()
                .map_err(|error| invalid_runtime_value("remote_storage.kafka_metadata", error))?;
            cfg.remote_log_metadata = crate::config::RlmmKind::TopicBacked(policy);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{gibibytes, mebibytes, millis, secs};

    use crate::file_config::FileConfig;

    #[test]
    fn remote_storage_section_enables_and_sets_dir() {
        let toml = r#"
[remote_storage]
storage_dir = "/var/lib/krabka/tier"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Local { dir }) => {
                assert!(dir == std::path::PathBuf::from("/var/lib/krabka/tier"));
            }
            other => panic!("expected Local backend, got {other:?}"),
        }
    }
    #[test]
    fn no_remote_storage_section_leaves_backend_none() {
        let cfg =
            crate::file_config::test_support::configured_unwrap_parse("broker_id = 1").unwrap();
        assert!(cfg.remote_storage_backend.is_none());
        // No remote_storage section: RLMM stays at the production default (TopicBacked).
        assert!(matches!(
            cfg.remote_log_metadata,
            crate::config::RlmmKind::TopicBacked(_)
        ));
    }
    fn parsed_kafka_metadata(toml: &str) -> crate::config::KafkaRlmmConfig {
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        match cfg.remote_log_metadata {
            crate::config::RlmmKind::TopicBacked(config) => config,
            crate::config::RlmmKind::InMemory => panic!("expected TopicBacked"),
        }
    }

    #[test]
    fn kafka_metadata_section_parses_with_defaults() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "127.0.0.1:9092"
"#;
        let km = parsed_kafka_metadata(toml);
        check!(km.bootstrap.as_str() == "127.0.0.1:9092");
        check!(km.num_partitions == 50);
        check!(km.replication == 3);
        check!(km.min_isr == 2);
        check!(km.topic_create_timeout == secs(30));
        check!(km.fetch_max_wait == millis(500));
        check!(km.fetch_max_bytes == mebibytes(1));
        check!(km.fetch_retry_backoff == millis(200));
        check!(km.event_queue_capacity.capacity() == 1024);
        check!(km.snapshot_interval == secs(60));
    }
    #[test]
    fn kafka_metadata_section_honors_overrides() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "broker-0:9094"
num_partitions = 8
replication = 1
min_isr = 1
topic_create_timeout = "45s"
fetch_max_wait = "750ms"
fetch_max_bytes = "2MiB"
fetch_retry_backoff = "300ms"
event_queue_capacity = 2048
snapshot_interval = "90s"
"#;
        let km = parsed_kafka_metadata(toml);
        check!(km.bootstrap.as_str() == "broker-0:9094");
        check!(km.num_partitions == 8);
        check!(km.replication == 1);
        check!(km.min_isr == 1);
        check!(km.topic_create_timeout == secs(45));
        check!(km.fetch_max_wait == millis(750));
        check!(km.fetch_max_bytes == mebibytes(2));
        check!(km.fetch_retry_backoff == millis(300));
        check!(km.event_queue_capacity.capacity() == 2048);
        check!(km.snapshot_interval == secs(90));
    }
    #[test]
    fn kafka_metadata_section_rejects_invalid_policy() {
        for (field, value) in [
            ("min_isr", "0"),
            ("topic_create_timeout", "\"0ms\""),
            ("topic_create_timeout", "\"0.5ms\""),
            ("topic_create_timeout", "\"2147483648ms\""),
            ("fetch_max_wait", "\"0ms\""),
            ("fetch_max_bytes", "\"0B\""),
            ("fetch_max_bytes", "\"0.5B\""),
            ("fetch_max_bytes", "\"2147483648B\""),
            ("fetch_retry_backoff", "\"0ms\""),
            ("event_queue_capacity", "0"),
            ("snapshot_interval", "\"0s\""),
        ] {
            let source = format!(
                "[remote_storage]\nstorage_dir = \"/tmp/tier\"\n\
                 [remote_storage.kafka_metadata]\n{field} = {value}\n"
            );
            let file: FileConfig = toml::from_str(&source).expect("parse policy syntax");
            let mut config = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut config)
                .expect_err("invalid metadata policy must fail");
            assert!(error.to_string().contains(field), "{field}: {error}");
        }
    }
    #[test]
    fn reader_bounds_and_index_cache_size_default_to_kafkas_values() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        check!(cfg.remote_reader_threads == 10);
        check!(cfg.remote_reader_max_pending_tasks == 100);
        check!(cfg.remote_index_cache_size == gibibytes(1));
    }

    #[test]
    fn reader_bounds_and_index_cache_size_are_overridable() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"
reader_threads = 4
reader_max_pending_tasks = 32
index_cache_size = "256MiB"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        check!(cfg.remote_reader_threads == 4);
        check!(cfg.remote_reader_max_pending_tasks == 32);
        check!(cfg.remote_index_cache_size == mebibytes(256));
    }

    /// The two KIP-405 sweep bounds default to Kafka's thread-pool sizes,
    /// and an operator who names them gets what they named.
    #[test]
    fn sweep_bounds_default_to_kafkas_thread_pool_sizes_and_are_overridable() {
        for (case, toml, copier, expiration) in [
            (
                "defaults",
                "[remote_storage]\nstorage_dir = \"/tmp/tier\"\n",
                10,
                10,
            ),
            (
                "overridden",
                "[remote_storage]\nstorage_dir = \"/tmp/tier\"\n\
                 copier_threads = 32\nexpiration_threads = 4\n",
                32,
                4,
            ),
        ] {
            let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
            check!(cfg.remote_copier_threads == copier, "{case}");
            check!(cfg.remote_expiration_threads == expiration, "{case}");
        }
    }

    #[test]
    fn a_zero_reader_bound_sweep_bound_or_cache_size_is_rejected() {
        for (field, value) in [
            ("reader_threads", "0"),
            ("reader_max_pending_tasks", "0"),
            ("copier_threads", "0"),
            ("expiration_threads", "0"),
            ("index_cache_size", "\"0B\""),
        ] {
            let source =
                format!("[remote_storage]\nstorage_dir = \"/tmp/tier\"\n{field} = {value}\n");
            let file: FileConfig = toml::from_str(&source).expect("parse syntax");
            let mut config = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut config)
                .expect_err("a zero bound must fail");
            assert!(error.to_string().contains(field), "{field}: {error}");
        }
    }

    #[test]
    fn remote_storage_backends_are_mutually_exclusive() {
        for (case, source, expected) in [
            (
                "remote_storage_local_and_s3_together_rejected",
                r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.s3]
bucket = "b"
region = "us-east-1"
"#,
                "cannot set both",
            ),
            (
                "remote_storage_local_and_gcs_together_rejected",
                r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.gcs]
bucket = "b"
"#,
                "cannot set",
            ),
            (
                "remote_storage_s3_and_gcs_together_rejected",
                r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"

[remote_storage.gcs]
bucket = "b"
"#,
                "cannot set",
            ),
        ] {
            let error =
                crate::file_config::test_support::configured_unwrap_parse(source).unwrap_err();
            let rendered = error.to_string();
            assert!(
                rendered.contains(expected),
                "{case}: expected backend-conflict error, got: {rendered}"
            );
        }
    }

    #[test]
    fn kafka_metadata_in_memory_true_opts_out_to_in_memory_rlmm() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
in_memory = true
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap();
        assert!(
            matches!(cfg.remote_log_metadata, crate::config::RlmmKind::InMemory),
            "in_memory = true must opt out to RlmmKind::InMemory, got {:?}",
            cfg.remote_log_metadata
        );
    }

    /// The copy deadline reaches `BrokerConfig`, and it defaults to the
    /// production constant when the TOML omits it.
    #[test]
    fn copy_timeout_parses_and_defaults() {
        let cases = [
            ("", crate::config::DEFAULT_REMOTE_COPY_TIMEOUT),
            ("copy_timeout = \"90s\"\n", secs(90)),
        ];
        for (line, want) in cases {
            let toml = format!("[remote_storage]\nstorage_dir = \"/tmp/tier\"\n{line}");
            let cfg = crate::file_config::test_support::configured_unwrap_parse(&toml).unwrap();
            check!(cfg.remote_copy_timeout == want, "{line:?}");
        }
    }

    /// A zero deadline would abandon every copy before it started, so it is
    /// refused at load time rather than silently tiering nothing.
    #[test]
    fn a_zero_copy_timeout_is_refused() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"
copy_timeout = "0s"
"#;
        let err = crate::file_config::test_support::configured_unwrap_parse(toml).unwrap_err();
        check!(err.to_string().contains("remote_storage.copy_timeout"));
    }
}
