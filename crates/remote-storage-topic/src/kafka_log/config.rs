//! Construction-time configuration for the Kafka-topic-backed metadata event
//! log.
//!
//! This module holds the topic name, the Apache Kafka defaults for partition
//! count and replication factor, the transport tunables of the producer and of
//! the per-partition fetch loops, and the validation that keeps every tunable
//! inside the `int32` millisecond and byte ranges the Kafka wire carries.

use {
    krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity},
    krabka_macros::RefinedNewtype,
    krabka_units::prelude::{
        ByteSize, ByteSizeExt as _, Time, TimeExt as _, mebibytes, millis, secs,
    },
    refined_type::rule::GreaterUsize,
};

/// Default name of the internal metadata topic.
pub const METADATA_TOPIC: &str = "__remote_log_metadata";

/// Default partition count for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.num.partitions`.
pub const DEFAULT_NUM_PARTITIONS: i32 = 50;

/// Default replication factor for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.replication.factor`.
pub const DEFAULT_REPLICATION: i32 = 3;

/// Default `min.insync.replicas` for `__remote_log_metadata`, matching
/// Apache Kafka's `remote.log.metadata.topic.min.isr`
/// (`TopicBasedRemoteLogMetadataManagerConfig.DEFAULT_REMOTE_LOG_METADATA_TOPIC_MIN_ISR`).
pub const DEFAULT_MIN_ISR: i32 = 2;

/// How long `CreateTopics` may take to provision `__remote_log_metadata`
/// before the broker abandons the round-trip.
pub const DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT: Time = secs(30);

/// `max_wait_ms` for the per-partition metadata `Fetch`. It is long enough
/// that an idle partition costs one RPC per interval rather than a spin, and
/// short enough that cancellation on reassignment is prompt.
pub const DEFAULT_METADATA_FETCH_MAX_WAIT: Time = millis(500);

/// Per-partition budget for the metadata `Fetch`. Metadata events are small,
/// so one mebibyte is many thousands of them per round-trip.
pub const DEFAULT_METADATA_FETCH_MAX_BYTES: ByteSize = mebibytes(1);

/// Pause before a retry of a failed metadata `Fetch`, so a broker that is down
/// does not turn the fetch loop into a busy spin.
pub const DEFAULT_METADATA_FETCH_RETRY_BACKOFF: Time = millis(200);

/// Default capacity of the shared metadata-event delivery queue.
pub const DEFAULT_METADATA_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Positive capacity of the shared metadata-event delivery queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterUsize<0>),
    string_error,
    label = "metadata event queue capacity",
    getter = capacity,
    default = DEFAULT_METADATA_EVENT_QUEUE_CAPACITY
)]
pub struct MetadataEventQueueCapacity(usize);

/// Construction-time configuration for
/// [`KafkaMetadataEventLog`](super::KafkaMetadataEventLog).
#[derive(Debug, Clone)]
pub struct KafkaMetadataLogConfig {
    /// `host:port` for the Kafka client to bootstrap from. The TBRLMM in a
    /// broker connects over loopback to its own listener.
    pub bootstrap: String,
    /// Internal topic name. Production deployments keep the default. The
    /// field exists so multiple isolated clusters can share an environment in
    /// tests.
    pub topic: String,
    /// Number of partitions to create the topic with on first startup.
    /// The log ignores this value when the topic already exists, and the
    /// existing count wins. The log does not support re-bucketing on
    /// partition growth.
    pub num_partitions: i32,
    /// Replication factor to create the topic with on first startup.
    /// The log ignores this value when the topic already exists.
    pub replication: i32,
    /// `min.insync.replicas` to create the topic with on first startup.
    /// `None` leaves the setting to the cluster default. The log ignores this
    /// value when the topic already exists.
    pub min_isr: Option<i32>,
    /// `client_id` for the producer and consumer. It is diagnostic only.
    pub client_id: String,
    /// Provision and maintain this internal topic with log compaction.
    pub compacted: bool,
    /// Create the topic when it is absent. Read-only tooling disables this.
    pub provision_topic: bool,
    /// Client TLS/SASL security applied to the producer, the raw client,
    /// the admin client, and every per-partition fetch connection.
    /// `None` is plaintext loopback, and it is the default.
    pub security: Option<krabka_client_core::security::ClientSecurity>,
    /// Timeout for provisioning the internal topic.
    pub topic_create_timeout: Time,
    /// Maximum wait for each per-partition metadata fetch.
    pub fetch_max_wait: Time,
    /// Maximum bytes returned by each per-partition metadata fetch.
    pub fetch_max_bytes: ByteSize,
    /// Backoff after a failed metadata fetch.
    pub fetch_retry_backoff: Time,
    /// Capacity of the shared metadata-event delivery queue.
    pub event_queue_capacity: MetadataEventQueueCapacity,
    /// Capacity of every outbound Kafka connection's dispatch queue.
    pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    /// Maximum frame size for every outbound Kafka connection.
    pub frame_max: ClientFrameMax,
}

impl KafkaMetadataLogConfig {
    /// Construct a config with the conventional Kafka defaults.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            topic: METADATA_TOPIC.to_string(),
            num_partitions: DEFAULT_NUM_PARTITIONS,
            replication: DEFAULT_REPLICATION,
            min_isr: Some(DEFAULT_MIN_ISR),
            client_id: "krabka-rlmm".to_string(),
            compacted: false,
            provision_topic: true,
            security: None,
            topic_create_timeout: DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT,
            fetch_max_wait: DEFAULT_METADATA_FETCH_MAX_WAIT,
            fetch_max_bytes: DEFAULT_METADATA_FETCH_MAX_BYTES,
            fetch_retry_backoff: DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
            event_queue_capacity: MetadataEventQueueCapacity::default(),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
        }
    }

    /// Validate values that cross into the Kafka wire client.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive, non-finite, fractional, or
    /// out-of-range wire values, and for a `min_isr` below one, as Kafka's
    /// `remote.log.metadata.topic.min.isr` does (`atLeast(1)`).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(min_isr) = self.min_isr {
            refined_type::rule::GreaterI32::<0>::new(min_isr)
                .map_err(|error| format!("min_isr: {error}"))?;
        }
        validate_positive_whole_millis_i32("topic_create_timeout", self.topic_create_timeout)?;
        validate_positive_whole_millis_i32("fetch_max_wait", self.fetch_max_wait)?;
        validate_positive_whole_bytes_i32("fetch_max_bytes", self.fetch_max_bytes)?;
        validate_positive_duration("fetch_retry_backoff", self.fetch_retry_backoff)
    }
}

fn validate_positive_whole_millis_i32(name: &str, value: Time) -> Result<(), String> {
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        ));
    }
    let millis = i32::try_from(millis).map_err(|_| {
        format!("{name} must be a positive whole number of milliseconds within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(millis)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_whole_bytes_i32(name: &str, value: ByteSize) -> Result<(), String> {
    let bytes = value.bytes_i64();
    if !value.bytes_f64().is_finite() || ByteSize::from_bytes_i64(bytes) != value {
        return Err(format!(
            "{name} must be a positive whole number of bytes within 1..=i32::MAX"
        ));
    }
    let bytes = i32::try_from(bytes).map_err(|_| {
        format!("{name} must be a positive whole number of bytes within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(bytes)
        .map(|_| ())
        .map_err(|error| format!("{name}: {error}"))
}

fn validate_positive_duration(name: &str, value: Time) -> Result<(), String> {
    let duration = std::time::Duration::try_from_secs_f64(value.secs_f64())
        .map_err(|error| format!("{name}: {error}"))?;
    if duration.is_zero() {
        return Err(format!("{name} must be positive"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn configured(configure: impl FnOnce(&mut KafkaMetadataLogConfig)) -> KafkaMetadataLogConfig {
        let mut cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        configure(&mut cfg);
        cfg
    }

    #[test]
    fn config_defaults_match_kafka() {
        let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        check!(cfg.topic == METADATA_TOPIC);
        check!(cfg.num_partitions == 50);
        check!(cfg.replication == 3);
        check!(cfg.min_isr == Some(2));
        check!(!cfg.compacted);
        check!(cfg.provision_topic);
        check!(cfg.bootstrap == "127.0.0.1:9092");
        check!(cfg.security.is_none());
        check!(cfg.topic_create_timeout == secs(30));
        check!(cfg.fetch_max_wait == millis(500));
        check!(cfg.fetch_max_bytes == mebibytes(1));
        check!(cfg.fetch_retry_backoff == millis(200));
        check!(cfg.event_queue_capacity.capacity() == 1024);
        cfg.validate().unwrap();
    }

    #[test]
    fn config_accepts_custom_transport_policy() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: secs(45),
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            event_queue_capacity: MetadataEventQueueCapacity::new(2048).unwrap(),
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };

        cfg.validate().unwrap();
        check!(cfg.topic_create_timeout == secs(45));
        check!(cfg.fetch_max_wait == millis(750));
        check!(cfg.fetch_max_bytes == mebibytes(2));
        check!(cfg.fetch_retry_backoff == millis(300));
        check!(cfg.event_queue_capacity.capacity() == 2048);
    }

    #[yare::parameterized(
        create_timeout_zero = {
            "topic_create_timeout",
            configured(|cfg| cfg.topic_create_timeout = Time::ZERO),
        },
        create_timeout_submillisecond = {
            "topic_create_timeout",
            configured(|cfg| cfg.topic_create_timeout = Time::from_micros(500)),
        },
        create_timeout_infinite = {
            "topic_create_timeout",
            configured(|cfg| cfg.topic_create_timeout = Time::from_secs_f64(f64::INFINITY)),
        },
        create_timeout_too_large = {
            "topic_create_timeout",
            configured(|cfg| cfg.topic_create_timeout = Time::from_millis(i64::from(i32::MAX) + 1)),
        },
        fetch_wait_zero = { "fetch_max_wait", configured(|cfg| cfg.fetch_max_wait = Time::ZERO) },
        fetch_wait_submillisecond = {
            "fetch_max_wait",
            configured(|cfg| cfg.fetch_max_wait = Time::from_micros(500)),
        },
        fetch_wait_infinite = {
            "fetch_max_wait",
            configured(|cfg| cfg.fetch_max_wait = Time::from_secs_f64(f64::INFINITY)),
        },
        fetch_wait_too_large = {
            "fetch_max_wait",
            configured(|cfg| cfg.fetch_max_wait = Time::from_millis(i64::from(i32::MAX) + 1)),
        },
        fetch_bytes_zero = {
            "fetch_max_bytes",
            configured(|cfg| cfg.fetch_max_bytes = ByteSize::ZERO),
        },
        fetch_bytes_fractional = {
            "fetch_max_bytes",
            configured(|cfg| cfg.fetch_max_bytes = ByteSize::from_bytes_f64(0.5)),
        },
        fetch_bytes_infinite = {
            "fetch_max_bytes",
            configured(|cfg| cfg.fetch_max_bytes = ByteSize::from_bytes_f64(f64::INFINITY)),
        },
        fetch_bytes_too_large = {
            "fetch_max_bytes",
            configured(|cfg| cfg.fetch_max_bytes = ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1)),
        },
        retry_backoff_zero = {
            "fetch_retry_backoff",
            configured(|cfg| cfg.fetch_retry_backoff = Time::ZERO),
        },
        retry_backoff_infinite = {
            "fetch_retry_backoff",
            configured(|cfg| cfg.fetch_retry_backoff = Time::from_secs_f64(f64::INFINITY)),
        },
    )]
    fn config_rejects_invalid_transport_policy(field: &str, cfg: KafkaMetadataLogConfig) {
        let error = cfg.validate().expect_err("invalid policy must fail");
        assert2::assert!(error.contains(field), "field={field}, error={error}");
    }

    #[yare::parameterized(
        one = { Some(1), true },
        two = { Some(2), true },
        zero = { Some(0), false },
        negative = { Some(-1), false },
        unset_uses_cluster_default = { None, true },
    )]
    fn min_isr_must_be_at_least_one(min_isr: Option<i32>, valid: bool) {
        let cfg = KafkaMetadataLogConfig {
            min_isr,
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };
        let result = cfg.validate();
        assert2::assert!(result.is_ok() == valid, "min_isr={min_isr:?}: {result:?}");
        if let Err(error) = result {
            assert2::assert!(error.contains("min_isr"), "{error}");
        }
    }

    #[test]
    fn metadata_event_queue_capacity_rejects_zero() {
        assert!(MetadataEventQueueCapacity::new(0).is_err());
        check!(MetadataEventQueueCapacity::new(1).unwrap().capacity() == 1);
    }

    /// The metadata client's tunables are quantities, but they reach Kafka as
    /// raw `int32` milliseconds and bytes. This test pins the wire images. A
    /// wrong scale here is invisible in the types. It would show up only as a
    /// metadata consumer that spins, when `max_wait` is too short, or that
    /// truncates batches.
    #[test]
    fn client_tunables_convert_to_their_kafka_wire_images() {
        let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
        check!(cfg.topic_create_timeout.millis_i32() == 30_000);
        check!(cfg.fetch_max_wait.millis_i32() == 500);
        check!(cfg.fetch_max_bytes.bytes_i32() == 1 << 20);
        check!(cfg.fetch_retry_backoff.to_std() == std::time::Duration::from_millis(200));
    }

    #[test]
    fn config_carries_client_resource_policy() {
        let cfg = KafkaMetadataLogConfig {
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::new(7).unwrap(),
            frame_max: ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };
        check!(cfg.dispatch_queue_capacity.get() == 7);
        check!(cfg.frame_max.size() == krabka_units::kibibytes(32));
    }

    #[test]
    fn config_carries_security() {
        use {
            krabka_client_core::security::{ClientSecurity, SaslCredentials},
            krabka_security::ListenerProtocol,
        };
        let cfg = KafkaMetadataLogConfig {
            bootstrap: "127.0.0.1:9092".into(),
            topic: METADATA_TOPIC.into(),
            num_partitions: 1,
            replication: 1,
            client_id: "x".into(),
            security: Some(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: "u".into(),
                    password: "p".into(),
                }),
                sasl_host: None,
            }),
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };
        assert!(cfg.security.is_some());
    }
}
