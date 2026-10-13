//! KIP-405 tiered storage: which object store backs the remote log, which
//! remote-log metadata manager runs beside it, and the WORM archive pairing
//! the broker refuses.

use {
    crate::{
        BrokerError,
        config::{
            BrokerConfig, DEFAULT_RLMM_SNAPSHOT_INTERVAL, DEFAULT_RLMM_TOPIC_MIN_ISR,
            DEFAULT_RLMM_TOPIC_NUM_PARTITIONS, DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR,
        },
    },
    krabka_client_core::security::ClientSecurity,
    krabka_units::{ByteSize, Time, convert::TimeExt},
    std::path::PathBuf,
};

/// Parameters for the topic-backed
/// [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager).
///
/// Does not derive `PartialEq`/`Eq`: the `security` field holds
/// rustls-adjacent types (a `ClientConfig` connector) that are not
/// comparable, and nothing compares this config by value.
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub struct KafkaRlmmConfig {
    /// Capacity of every Kafka metadata-log client dispatch queue.
    pub dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size for every Kafka metadata-log client.
    pub frame_max: krabka_client_core::ClientFrameMax,
    /// `host:port` the manager dials to reach its own broker. It is loopback
    /// in a single-broker setup, and the inter-broker listener in a
    /// multi-broker setup.
    pub bootstrap: String,
    /// Partition count to create `__remote_log_metadata` with on first
    /// startup. Ignored when the topic already exists.
    #[default(DEFAULT_RLMM_TOPIC_NUM_PARTITIONS)]
    pub num_partitions: i32,
    /// Replication factor to create `__remote_log_metadata` with on
    /// first startup. Ignored when the topic already exists.
    #[default(DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR)]
    pub replication: i32,
    /// `min.insync.replicas` to create `__remote_log_metadata` with on first
    /// startup. Maps to Kafka's `remote.log.metadata.topic.min.isr`. Ignored
    /// when the topic already exists. Default
    /// [`DEFAULT_RLMM_TOPIC_MIN_ISR`].
    #[default(DEFAULT_RLMM_TOPIC_MIN_ISR)]
    pub min_isr: i32,
    /// How often the topic-backed manager flushes its RLMM cache
    /// snapshot to disk. Maps to Kafka's
    /// `remote.log.metadata.snapshot.interval`. Default
    /// [`DEFAULT_RLMM_SNAPSHOT_INTERVAL`].
    #[default(DEFAULT_RLMM_SNAPSHOT_INTERVAL)]
    pub snapshot_interval: Time,
    /// Timeout for provisioning each internal metadata topic.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT)]
    pub topic_create_timeout: Time,
    /// Maximum wait for each per-partition metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_MAX_WAIT)]
    pub fetch_max_wait: Time,
    /// Maximum bytes returned by each per-partition metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_MAX_BYTES)]
    pub fetch_max_bytes: ByteSize,
    /// Backoff after a failed metadata fetch.
    #[default(krabka_remote_storage_topic::DEFAULT_METADATA_FETCH_RETRY_BACKOFF)]
    pub fetch_retry_backoff: Time,
    /// Capacity of the shared metadata-event delivery queue.
    pub event_queue_capacity: krabka_remote_storage_topic::MetadataEventQueueCapacity,
    /// Directory the manager writes the RLMM cache snapshot to, as one
    /// `snapshot` file. The broker derives the path from `log.dir`.
    pub snapshot_dir: std::path::PathBuf,
    /// Client TLS/SASL security for the metadata client. `None` means
    /// plaintext loopback, which suits single-broker and fully-plaintext
    /// clusters. The broker overrides this at runtime in
    /// `bootstrap_topic_rlmm` from the inter-broker listener. The TOML path
    /// always supplies `None`.
    ///
    /// The field is boxed to keep `KafkaRlmmConfig` and the enclosing
    /// `BrokerConfig` small, because `Broker::start` moves `BrokerConfig` by
    /// value into a large future.
    pub security: Option<Box<ClientSecurity>>,
}

/// Which `RemoteLogMetadataManager` the broker runs when tiered storage is enabled.
///
/// Topic-backed is the production default. It matches Kafka's
/// `TopicBasedRemoteLogMetadataManager`, the only production RLMM. In-memory
/// is an explicit opt-out for in-process integration tests that have no real
/// listener to loop the metadata client back to. The broker ignores this enum
/// entirely when [`BrokerConfig::remote_storage_backend`] is `None`.
#[derive(Debug, Clone)]
pub enum RlmmKind {
    /// Durable `__remote_log_metadata`-backed manager. `cfg.bootstrap` and
    /// `cfg.snapshot_dir` may be empty; the broker derives them at start from
    /// the inter-broker listener and `log.dir` respectively.
    TopicBacked(KafkaRlmmConfig),
    /// Non-durable in-process manager. Tests only.
    InMemory,
}

impl KafkaRlmmConfig {
    /// Validates the shared metadata transport and RLMM snapshot policy.
    ///
    /// # Errors
    ///
    /// Returns an invalid-runtime error when a value cannot safely reach its
    /// runtime consumer.
    pub fn validate(&self) -> Result<(), BrokerError> {
        let transport = krabka_remote_storage_topic::KafkaMetadataLogConfig {
            dispatch_queue_capacity: self.dispatch_queue_capacity,
            frame_max: self.frame_max,
            min_isr: Some(self.min_isr),
            topic_create_timeout: self.topic_create_timeout,
            fetch_max_wait: self.fetch_max_wait,
            fetch_max_bytes: self.fetch_max_bytes,
            fetch_retry_backoff: self.fetch_retry_backoff,
            event_queue_capacity: self.event_queue_capacity,
            ..krabka_remote_storage_topic::KafkaMetadataLogConfig::new(&self.bootstrap)
        };
        transport.validate().map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!("remote_storage.kafka_metadata: {error}"))
        })?;

        let snapshot_interval = std::time::Duration::try_from_secs_f64(
            self.snapshot_interval.secs_f64(),
        )
        .map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!(
                "remote_storage.kafka_metadata.snapshot_interval: {error}"
            ))
        })?;
        if snapshot_interval.is_zero() {
            return Err(BrokerError::InvalidRuntimeConfig(
                "remote_storage.kafka_metadata.snapshot_interval must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// What backs the broker's `RemoteStorageManager` when tiered storage is on.
#[derive(Debug, Clone)]
pub enum RemoteStorageBackend {
    /// Filesystem-backed `LocalTieredStorage`. Useful for tests, single-
    /// node dev setups, and shared-filesystem multi-broker deployments.
    Local {
        /// Root directory for the segment store.
        dir: PathBuf,
    },
    /// S3-compatible `S3RemoteStorage`. This is a production backend. It
    /// works with AWS S3, `MinIO`, Cloudflare R2, and GCS through S3
    /// compatibility.
    S3(krabka_remote_storage::S3Config),
    /// Native Google Cloud Storage `S3RemoteStorage` engine. This is the
    /// production backend for GKE deployments. It supports keyless Workload
    /// Identity and ADC auth; leave all credential fields unset for that.
    Gcs(krabka_remote_storage::GcsConfig),
}

impl BrokerConfig {
    /// Rejects WORM archive mode over anything but an object store.
    ///
    /// The TOML layer rejects the same pairing, but a `BrokerConfig` built in
    /// code never passes through it.
    pub(super) fn validate_remote_storage_worm(&self) -> Result<(), BrokerError> {
        if self.remote_storage_worm.is_some()
            && !matches!(
                self.remote_storage_backend,
                Some(RemoteStorageBackend::S3(_) | RemoteStorageBackend::Gcs(_))
            )
        {
            return Err(BrokerError::InvalidRuntimeConfig(
                "remote storage WORM mode requires an S3 or GCS backend; a local storage \
                 directory cannot enforce write-once"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        krabka_units::{convert::ByteSizeExt, mebibytes, millis, minutes, secs},
    };

    #[test]
    fn production_default_selects_topic_backed_rlmm() {
        let c = BrokerConfig::default();
        assert!(matches!(c.remote_log_metadata, RlmmKind::TopicBacked(_)));
    }

    #[test]
    fn test_default_selects_in_memory_rlmm() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(matches!(c.remote_log_metadata, RlmmKind::InMemory));
    }

    #[test]
    fn kafka_rlmm_config_default_has_sane_topic_settings() {
        let c = KafkaRlmmConfig::default();
        check!(c.num_partitions == 50);
        check!(c.replication == 3);
        check!(c.min_isr == 2);
        check!(c.bootstrap.is_empty());
        check!(c.snapshot_dir == std::path::PathBuf::new());
        check!(c.snapshot_interval == DEFAULT_RLMM_SNAPSHOT_INTERVAL);
        check!(c.topic_create_timeout == secs(30));
        check!(c.fetch_max_wait == millis(500));
        check!(c.fetch_max_bytes == mebibytes(1));
        check!(c.fetch_retry_backoff == millis(200));
        check!(c.event_queue_capacity.capacity() == 1024);
        check!(c.security.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn kafka_rlmm_config_validates_transport_and_snapshot_policy() {
        let valid = KafkaRlmmConfig {
            topic_create_timeout: secs(45),
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            event_queue_capacity: krabka_remote_storage_topic::MetadataEventQueueCapacity::new(
                2048,
            )
            .unwrap(),
            snapshot_interval: secs(90),
            min_isr: 1,
            ..KafkaRlmmConfig::default()
        };
        valid.validate().unwrap();

        for (field, config) in [
            ("fetch_max_wait", KafkaRlmmConfig {
                fetch_max_wait: Time::ZERO,
                ..KafkaRlmmConfig::default()
            }),
            ("min_isr", KafkaRlmmConfig {
                min_isr: 0,
                ..KafkaRlmmConfig::default()
            }),
            ("snapshot_interval", KafkaRlmmConfig {
                snapshot_interval: Time::ZERO,
                ..KafkaRlmmConfig::default()
            }),
            ("snapshot_interval", KafkaRlmmConfig {
                snapshot_interval: Time::from_secs_f64(f64::INFINITY),
                ..KafkaRlmmConfig::default()
            }),
        ] {
            let error = config.validate().expect_err("invalid policy must fail");
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    #[test]
    fn broker_validation_rejects_invalid_topic_metadata_policy() {
        let config = BrokerConfig {
            remote_log_metadata: RlmmKind::TopicBacked(KafkaRlmmConfig {
                fetch_max_bytes: ByteSize::ZERO,
                ..KafkaRlmmConfig::default()
            }),
            ..BrokerConfig::default()
        };

        let error = config
            .validate()
            .expect_err("invalid embedded metadata policy must fail");
        assert!(error.to_string().contains("fetch_max_bytes"), "{error}");
    }

    #[test]
    fn kafka_rlmm_config_carries_snapshot_settings() {
        let c = KafkaRlmmConfig {
            bootstrap: "127.0.0.1:9092".into(),
            snapshot_interval: minutes(1),
            snapshot_dir: std::path::PathBuf::from("/data/remote-log-metadata"),
            ..KafkaRlmmConfig::default()
        };
        assert!(c.snapshot_interval == minutes(1));
        assert!(c.snapshot_dir == std::path::PathBuf::from("/data/remote-log-metadata"));
    }

    #[test]
    fn kafka_rlmm_config_carries_optional_security() {
        let c = KafkaRlmmConfig {
            bootstrap: "127.0.0.1:9092".into(),
            num_partitions: 1,
            replication: 1,
            snapshot_dir: std::path::PathBuf::from("/data/remote-log-metadata"),
            ..KafkaRlmmConfig::default()
        };
        assert!(c.security.is_none());
    }

    #[test]
    fn worm_without_an_object_store_backend_is_rejected_by_validate() {
        // The TOML layer already rejects these, but a `BrokerConfig` built in
        // code never passes through it.
        let cases = [
            (
                "a local storage directory cannot enforce write-once",
                Some(RemoteStorageBackend::Local {
                    dir: PathBuf::from("/var/lib/krabka-remote"),
                }),
            ),
            ("tiered storage is off entirely", None),
        ];
        for (name, backend) in cases {
            let c = BrokerConfig {
                remote_storage_backend: backend,
                remote_storage_worm: Some(krabka_remote_storage::WormConfig::default()),
                ..BrokerConfig::default()
            };
            check!(
                matches!(c.validate(), Err(BrokerError::InvalidRuntimeConfig(_))),
                "case {name}"
            );
        }
    }

    #[test]
    fn worm_over_an_object_store_backend_is_accepted_by_validate() {
        let c = BrokerConfig {
            remote_storage_backend: Some(RemoteStorageBackend::S3(
                krabka_remote_storage::S3Config {
                    bucket: "archive".into(),
                    region: "us-east-1".into(),
                    ..krabka_remote_storage::S3Config::default()
                },
            )),
            remote_storage_worm: Some(krabka_remote_storage::WormConfig::default()),
            ..BrokerConfig::default()
        };

        c.validate()
            .expect("WORM over an S3 backend is the supported pairing");
    }
}
