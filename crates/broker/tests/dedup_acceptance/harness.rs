//! The fixtures the KFC-10 acceptance cases share.
//!
//! A [`Topic`] is one topic on one in-process broker, addressed the way
//! Produce v13 and Fetch v13 address it. Its methods produce hand-built
//! batches with [`keyed_batch`], read the partition back with Fetch, change
//! the topic config through `IncrementalAlterConfigs`, and read the KFC-10
//! series of the partition.

use {
    crate::support::{
        InProcess,
        client::create_topic_with,
        configs::{ConfigMutationOutcome, ConfigOperation, alter_topic_config},
        dedup::{
            PartitionContents, Rec, StoredRecord, broker_metric, create_dedup_topic, keyed_batch,
            partition_metric, produce, read_partition,
        },
        produce::expected_partition_success,
        topic_id_for,
        topics::{CreateTopicSetup, TopicPartitionCount},
    },
    assert2::assert,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_log::LogConfig,
    krabka_protocol::{
        owned::{
            describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
            describe_configs_response::DescribeConfigsResourceResult,
            produce_response::PartitionProduceResponse,
        },
        primitives::uuid::Uuid,
        records::RecordBatch,
    },
    std::time::{Duration, Instant},
};

/// `ConfigResource.Type.TOPIC` on the wire.
pub(crate) const TOPIC_RESOURCE: i8 = 2;

/// How long a poll waits for a config or a gauge to reach this broker.
const SETTLE: Duration = Duration::from_secs(10);

/// One topic of the broker that `p` runs.
#[derive(Clone, Copy)]
pub(crate) struct Topic<'a> {
    pub(crate) p: &'a InProcess,
    pub(crate) name: &'a str,
    pub(crate) id: Uuid,
}

/// Create a topic that deduplicates within `window_ms`, or that does not
/// deduplicate when `window_ms` is `None`, and wait until the broker leads
/// every partition.
///
/// A topic created with `window_ms: None` stores no dedup override at all,
/// so `DescribeConfigs` reports both keys at their defaults.
#[bon::builder]
pub(crate) async fn topic<'a>(
    p: &'a InProcess,
    name: &'a str,
    #[builder(required, default = Some(60_000))] window_ms: Option<i64>,
    key_header: Option<&str>,
    #[builder(default = 1)] partitions: i32,
    #[builder(default = &[])] extra_configs: &[(&str, &str)],
) -> Topic<'a> {
    let id = if let Some(window_ms) = window_ms {
        create_dedup_topic()
            .broker(&p.broker)
            .client(&p.client)
            .topic(name)
            .window_ms(window_ms)
            .maybe_key_header(key_header)
            .partitions(partitions)
            .extra_configs(extra_configs)
            .call()
            .await
    } else {
        create_topic_with(&p.client, CreateTopicSetup {
            topic: name,
            configs: extra_configs,
            num_partitions: TopicPartitionCount(partitions),
            ..Default::default()
        })
        .await;
        for partition in 0..partitions {
            p.broker.wait_until_partition_present(name, partition).await;
        }
        topic_id_for(&p.client, name).await
    };
    Topic { p, name, id }
}

impl Topic<'_> {
    /// Produce `records` to partition 0 as one uncompressed batch, `acks=-1`.
    pub(crate) async fn send(self, records: &[Rec]) -> PartitionProduceResponse {
        self.send_batch(keyed_batch().records(records).call()).await
    }

    /// Produce `records` to `partition` as one uncompressed batch.
    pub(crate) async fn send_to(self, partition: i32, records: &[Rec]) -> PartitionProduceResponse {
        produce()
            .client(&self.p.client)
            .topic(self.name)
            .topic_id(self.id)
            .partition(partition)
            .batch(keyed_batch().records(records).call())
            .call()
            .await
    }

    /// Produce one prepared batch to partition 0, `acks=-1`.
    pub(crate) async fn send_batch(self, batch: RecordBatch) -> PartitionProduceResponse {
        produce()
            .client(&self.p.client)
            .topic(self.name)
            .topic_id(self.id)
            .batch(batch)
            .call()
            .await
    }

    /// Everything partition 0 holds, as a consumer reads it.
    pub(crate) async fn contents(self) -> PartitionContents { self.contents_of(0).await }

    pub(crate) async fn contents_of(self, partition: i32) -> PartitionContents {
        read_partition(&self.p.client, self.name, self.id, partition).await
    }

    /// One per-partition KFC-10 series of partition 0.
    pub(crate) async fn metric(self, name: &str) -> u64 { self.metric_of(name, 0).await }

    pub(crate) async fn metric_of(self, name: &str, partition: i32) -> u64 {
        integral(partition_metric(&self.p.broker, name, self.name, partition).await)
    }

    /// Every per-partition KFC-10 series of partition `partition`.
    pub(crate) async fn metrics_of(self, partition: i32) -> DedupMetrics {
        DedupMetrics {
            records_dropped: self
                .metric_of("dedup_records_dropped_total", partition)
                .await,
            records_unindexed: self
                .metric_of("dedup_records_unindexed_total", partition)
                .await,
            records_skew_clamped: self
                .metric_of("dedup_records_skew_clamped_total", partition)
                .await,
            capacity_evicted_entries: self
                .metric_of("dedup_capacity_evicted_entries_total", partition)
                .await,
            batches_rebuilt: self
                .metric_of("dedup_batches_rebuilt_total", partition)
                .await,
            index_rebuilds: self
                .metric_of("dedup_index_rebuilds_total", partition)
                .await,
            index_entries: self.metric_of("dedup_index_entries", partition).await,
        }
    }

    /// Poll a per-partition series of partition 0 until it reads `want`, and
    /// return what it read last.
    pub(crate) async fn settled_metric(self, name: &str, want: u64) -> u64 {
        let deadline = Instant::now() + SETTLE;
        loop {
            let value = self.metric(name).await;
            if value == want || Instant::now() > deadline {
                return value;
            }
            // Intentional: a config push reaches the writer with no waiter a
            // client can reach, and the gauge is the signal.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Run one `IncrementalAlterConfigs` operation on this topic.
    pub(crate) async fn alter(
        self,
        name: &str,
        operation: ConfigOperation,
        value: Option<&str>,
    ) -> ConfigMutationOutcome {
        alter_topic_config()
            .client(&self.p.client)
            .topic(self.name)
            .name(name)
            .operation(operation)
            .maybe_value(value)
            .call()
            .await
    }

    /// Set a config and wait until this broker's metadata image holds it.
    pub(crate) async fn set_config(self, name: &str, value: &str) {
        let outcome = self.alter(name, ConfigOperation::Set, Some(value)).await;
        assert!(outcome == accepted_mutation(), "set {name}={value}");
        self.await_config(name, |entry| entry.value.as_deref() == Some(value))
            .await;
    }

    /// Delete a config and wait until this broker reports its default.
    pub(crate) async fn delete_config(self, name: &str) {
        let outcome = self.alter(name, ConfigOperation::Delete, None).await;
        assert!(outcome == accepted_mutation(), "delete {name}");
        self.await_config(name, |entry| entry.config_source == CONFIG_SOURCE_DEFAULT)
            .await;
    }

    /// Wait until partition 0's own `LogConfig` is `ready`: a log setting such
    /// as `message.timestamp.type` reaches the partition writer after the
    /// metadata image, and the append reads it there.
    pub(crate) async fn await_log_config(self, ready: impl Fn(&LogConfig) -> bool) {
        let deadline = Instant::now() + SETTLE;
        while !self
            .p
            .broker
            .partition_log_config_for_test(self.name, 0)
            .is_some_and(|config| ready(&config))
        {
            assert!(
                Instant::now() < deadline,
                "{} log config never settled",
                self.name
            );
            // Intentional: the config push to the partition writer has no
            // waiter a client can reach.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait until this broker's `DescribeConfigs` entry for `name` is `ready`.
    pub(crate) async fn await_config(
        self,
        name: &str,
        ready: impl Fn(&DescribeConfigsResourceResult) -> bool,
    ) {
        let deadline = Instant::now() + SETTLE;
        loop {
            let entries = describe_topic(&self.p.client, self.name, &[name]).await;
            if entries.iter().any(&ready) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{name} on {} never settled: {entries:?}",
                self.name
            );
            // Intentional: the metadata image has no waiter a client can
            // reach, and DescribeConfigs reads the same image.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`: a stored topic override.
pub(crate) const CONFIG_SOURCE_TOPIC: i8 = 1;

/// `ConfigSource.DEFAULT_CONFIG`: no override at any level.
pub(crate) const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// The outcome of a config mutation the broker accepted.
pub(crate) fn accepted_mutation() -> ConfigMutationOutcome {
    ConfigMutationOutcome {
        error_code: codes::NONE,
        error_message: None,
    }
}

/// Every KFC-10 series of one partition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DedupMetrics {
    pub(crate) records_dropped: u64,
    pub(crate) records_unindexed: u64,
    pub(crate) records_skew_clamped: u64,
    pub(crate) capacity_evicted_entries: u64,
    pub(crate) batches_rebuilt: u64,
    pub(crate) index_rebuilds: u64,
    pub(crate) index_entries: u64,
}

/// A broker-wide KFC-10 gauge.
pub(crate) async fn broker_gauge(p: &InProcess, name: &str) -> u64 {
    integral(broker_metric(&p.broker, name).await)
}

/// Poll a broker-wide gauge until it reads `want`, and return what it read
/// last.
pub(crate) async fn settled_broker_gauge(p: &InProcess, name: &str, want: u64) -> u64 {
    let deadline = Instant::now() + SETTLE;
    loop {
        let value = broker_gauge(p, name).await;
        if value == want || Instant::now() > deadline {
            return value;
        }
        // Intentional: see `Topic::settled_metric`.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A Prometheus sample as the whole number every KFC-10 series is.
fn integral(value: f64) -> u64 {
    value
        .to_string()
        .parse()
        .unwrap_or_else(|_| panic!("a KFC-10 series is a whole number, got {value}"))
}

/// The `DescribeConfigs` entries of `keys` on `topic`, with documentation,
/// in the order of `keys`.
pub(crate) async fn describe_topic(
    client: &Client,
    topic: &str,
    keys: &[&str],
) -> Vec<DescribeConfigsResourceResult> {
    let response = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: TOPIC_RESOURCE,
                resource_name: topic.to_owned(),
                configuration_keys: Some(keys.iter().map(|key| (*key).to_owned()).collect()),
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: true,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let result = &response.results[0];
    assert!(
        result.error_code == codes::NONE,
        "DescribeConfigs({topic}): {result:?}"
    );
    // Kafka answers in its own order, not the request's, so the entries are
    // put in the order of `keys`. A missing entry shortens the list.
    keys.iter()
        .filter_map(|key| result.configs.iter().find(|entry| entry.name == *key))
        .cloned()
        .collect()
}

/// The partition row of a successful append at `base_offset` to a log that
/// starts at 0.
pub(crate) fn appended_at(base_offset: i64) -> PartitionProduceResponse {
    expected_partition_success(base_offset, 0)
}

/// A record with `key` and `value`.
pub(crate) fn rec(key: &str, value: &str) -> Rec { Rec::key(key).value(value) }

/// The records `Rec::key` builds for each key, in order.
pub(crate) fn recs(keys: &[&str]) -> Vec<Rec> { keys.iter().map(|key| Rec::key(key)).collect() }

/// A record a consumer reads at `offset`.
pub(crate) fn stored(offset: i64, key: &str, value: &str) -> StoredRecord {
    StoredRecord {
        offset,
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
    }
}

/// A record that `Rec::key(key)` produced, as a consumer reads it at
/// `offset`.
pub(crate) fn stored_key(offset: i64, key: &str) -> StoredRecord {
    stored(offset, key, &format!("value-of-{key}"))
}
