//! [`KafkaMetadataEventLog`]: the production [`MetadataEventLog`]
//! adapter that persists events in the internal `__remote_log_metadata`
//! Kafka topic.
//!
//! Writes flow through a [`krabka_client_producer::Producer`] with
//! explicit per-record partition pinning. Reads come back through one
//! cancellable manual-`Fetch` task per assigned partition. Each task
//! drives its own dedicated [`krabka_client_core::Connection`] and emits
//! [`MetadataEventRecord`](crate::MetadataEventRecord)s into a shared
//! mpsc. There is **no consumer group and no broker-side offset commit**.
//! The RLMM owns the read position. The manager assigns all partitions from
//! offset 0 today, then resumes from snapshot offsets and restricts the
//! consumed set.
//!
//! A dedicated connection per partition is necessary because the broker is
//! serial per-connection. A long-`max_wait_ms` fetch would
//! head-of-line-block any other RPC that shares the socket.
//!
//! Topic provisioning runs once at [`KafkaMetadataEventLog::start`] through
//! the [`krabka_client_admin::AdminClient`]. It reuses an existing topic, and
//! the topic's actual partition count then overrides the configured
//! `num_partitions`. It creates an absent topic with the configured cleanup
//! policy and `retention.ms=-1`. The same admin round-trip surfaces the topic's
//! `Uuid`, which the manual `Fetch` path needs, because Fetch v≥13 carries
//! `topic_id` and not the name.
//!
//! One `ListOffsets(timestamp=-1)` over the raw
//! [`krabka_client_core::Client`] pulls the high-water marks, rather than a
//! consumer. [`MetadataEventLog::high_water_marks`] therefore does not need
//! any fetch task to have made progress.
//!
//! [`KafkaMetadataEventLog::open_read_only`] opens the same log without a
//! producer and without provisioning. It sends no `InitProducerId` and no
//! `Produce`, so a principal that holds only `READ` and `DESCRIBE` on the topic
//! can use it. [`MetadataEventLog::visit_range`] then reads a bounded offset
//! range from the partition leader one page at a time, and follows the leader
//! when it moves. The `range` module holds that loop.

use {
    async_trait::async_trait,
    bytes::Bytes,
    futures_util::stream::{StreamExt, unfold},
    krabka_client_core::{
        BrokerInfo, BrokerPool, Client, ClientError, ClientFrameMax, Connection,
        ConnectionDispatchQueueCapacity, ConnectionOptions, FetchMinBytes, FetchPartitionResult,
        IsolatedFetch, MetadataScope, connection_target_host,
        fetch_partition_with_isolation_progress,
    },
    krabka_client_producer::{Acks, Producer, ProducerRecord},
    krabka_protocol::{
        owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        primitives::uuid::Uuid as WireUuid,
    },
    krabka_units::prelude::{ByteSize, Time, TimeExt as _},
    std::{
        collections::{BTreeMap, HashMap},
        sync::{Arc, Mutex as StdMutex},
    },
    tracing::{instrument, warn},
};

mod config;
mod consumer;
mod range;
mod topic;

pub use self::config::{
    DEFAULT_METADATA_EVENT_QUEUE_CAPACITY, DEFAULT_METADATA_FETCH_MAX_BYTES,
    DEFAULT_METADATA_FETCH_MAX_WAIT, DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
    DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT, DEFAULT_MIN_ISR, DEFAULT_NUM_PARTITIONS,
    DEFAULT_REPLICATION, KafkaMetadataLogConfig, METADATA_TOPIC, MetadataEventQueueCapacity,
};
use {
    self::{
        consumer::{ConsumerState, KafkaAssignmentHandle, metadata_event_channel},
        range::{RangeFetcher, RangeReadFailure, RangeTopic, visit_range_pages},
        topic::ensure_topic,
    },
    crate::{
        error::MetadataLogError,
        log::{
            AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart, RangeVisitor,
        },
    },
};

/// Whether an opened log may write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Access {
    /// Provision the topic when configured to, and build the idempotent
    /// producer.
    ReadWrite,
    /// Neither provision nor produce.
    ReadOnly,
}

/// Production [`MetadataEventLog`] backed by an internal Kafka topic.
pub struct KafkaMetadataEventLog {
    /// `None` for a log from [`KafkaMetadataEventLog::open_read_only`].
    producer: Option<Producer>,
    client: Client,
    /// Leader connections for [`MetadataEventLog::visit_range`].
    readers: BrokerPool,
    topic: String,
    topic_id: WireUuid,
    partition_count: i32,
    bootstrap: String,
    client_id: String,
    security: Option<krabka_client_core::security::ClientSecurity>,
    fetch_max_wait: Time,
    fetch_max_bytes: ByteSize,
    fetch_retry_backoff: Time,
    event_queue_capacity: MetadataEventQueueCapacity,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
    subscriptions: tokio::sync::Mutex<Vec<Arc<ConsumerState>>>,
}

impl KafkaMetadataEventLog {
    /// Provision the topic if it is missing, connect the producer and the
    /// raw client, learn the topic id, and return the log.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::Other`] on admin / producer /
    /// client construction failures.
    #[instrument(skip_all, fields(topic = %cfg.topic, bootstrap = %cfg.bootstrap), err)]
    pub async fn start(cfg: KafkaMetadataLogConfig) -> Result<Arc<Self>, MetadataLogError> {
        Box::pin(Self::open(cfg, Access::ReadWrite)).await
    }

    /// Open an existing topic for reads only, and return the log.
    ///
    /// The log neither provisions the topic nor changes its cleanup policy,
    /// whatever `cfg.provision_topic` and `cfg.compacted` say, and it builds
    /// no producer. Its reads therefore need only `DESCRIBE` and `READ` on the
    /// topic. [`MetadataEventLog::publish`] and
    /// [`MetadataEventLog::publish_keyed`] return
    /// [`MetadataLogError::Publish`] without contacting the broker.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::Other`] when the topic does not exist, and
    /// on admin or client construction failures.
    #[instrument(skip_all, fields(topic = %cfg.topic, bootstrap = %cfg.bootstrap), err)]
    pub async fn open_read_only(
        mut cfg: KafkaMetadataLogConfig,
    ) -> Result<Arc<Self>, MetadataLogError> {
        cfg.provision_topic = false;
        cfg.compacted = false;
        Box::pin(Self::open(cfg, Access::ReadOnly)).await
    }

    async fn open(
        cfg: KafkaMetadataLogConfig,
        access: Access,
    ) -> Result<Arc<Self>, MetadataLogError> {
        cfg.validate()
            .map_err(|error| MetadataLogError::Other(format!("invalid config: {error}")))?;

        // 1. Provision the topic, learn its partition count and id. The
        //    manual Fetch path needs the topic Uuid (Fetch v≥13 carries
        //    topic_id, not the name).
        let (partition_count, topic_id) = ensure_topic(&cfg).await?;

        // 2. Producer with acks=All and idempotence on. Read-your-writes
        //    depends on the broker durably acking the publish.
        //    A read-only log builds none, so it never sends InitProducerId.
        let producer = match access {
            Access::ReadWrite => Some(
                Producer::builder()
                    .bootstrap(cfg.bootstrap.clone())
                    .client_id(format!("{}-producer", cfg.client_id))
                    .dispatch_queue_capacity(cfg.dispatch_queue_capacity.get())
                    .frame_max(cfg.frame_max.size())
                    .acks(Acks::All)
                    .enable_idempotence(true)
                    .maybe_security(cfg.security.clone())
                    .build()
                    .await
                    .map_err(|e| MetadataLogError::Other(format!("producer build failed: {e}")))?,
            ),
            Access::ReadOnly => None,
        };

        // 3. Raw client for ListOffsets and any future low-level queries.
        let client = Client::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-client", cfg.client_id))
            .dispatch_queue_capacity(cfg.dispatch_queue_capacity.get())
            .frame_max(cfg.frame_max.size())
            .maybe_security(cfg.security.clone())
            .metadata_scope(METADATA_TOPIC_SCOPE)
            .build()
            .await
            .map_err(|e| MetadataLogError::Other(format!("client build failed: {e}")))?;
        client.metadata_topics().set([cfg.topic.as_str()]);

        // 4. Leader connections for bounded range reads.
        let reader_options = ConnectionOptions {
            client_id: format!("{}-reader", cfg.client_id),
            dispatch_queue_capacity: cfg.dispatch_queue_capacity,
            frame_max: cfg.frame_max,
            security: cfg.security.clone().map(Box::new),
            ..ConnectionOptions::default()
        };
        let readers = BrokerPool::new_with_server_names(
            resolve_bootstrap(&cfg.bootstrap, &reader_options).await?,
            reader_options,
        );

        Ok(Arc::new(Self {
            producer,
            client,
            readers,
            topic: cfg.topic,
            topic_id,
            partition_count,
            bootstrap: cfg.bootstrap,
            client_id: cfg.client_id,
            security: cfg.security,
            fetch_max_wait: cfg.fetch_max_wait,
            fetch_max_bytes: cfg.fetch_max_bytes,
            fetch_retry_backoff: cfg.fetch_retry_backoff,
            event_queue_capacity: cfg.event_queue_capacity,
            dispatch_queue_capacity: cfg.dispatch_queue_capacity,
            frame_max: cfg.frame_max,
            subscriptions: tokio::sync::Mutex::new(Vec::new()),
        }))
    }

    /// Cancel the fetch tasks of every active subscription. A drop also
    /// cancels them.
    pub async fn shutdown(&self) {
        let mut subs = self.subscriptions.lock().await;
        for state in subs.drain(..) {
            state.cancel_all();
        }
    }

    async fn list_offsets(&self, timestamp: i64) -> Result<Vec<i64>, MetadataLogError> {
        let metadata = self
            .client
            .refresh_metadata()
            .await
            .map_err(|e| MetadataLogError::Other(format!("Metadata failed: {e}")))?;
        let mut by_leader: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
        for partition in 0..self.partition_count {
            let leader = partition_leader(&metadata, &self.topic, partition).ok_or_else(|| {
                MetadataLogError::Other(format!(
                    "{} partition {partition} has no available leader",
                    self.topic
                ))
            })?;
            by_leader.entry(leader).or_default().push(partition);
        }
        let mut offsets = vec![0i64; usize_count(self.partition_count)?];
        for (leader, partitions) in by_leader {
            let req = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopic {
                    name: self.topic.clone(),
                    partitions: partitions
                        .into_iter()
                        .map(|partition_index| ListOffsetsPartition {
                            partition_index,
                            current_leader_epoch: -1,
                            timestamp,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let resp = self.client.broker(leader).send(req).await.map_err(|e| {
                MetadataLogError::Other(format!("ListOffsets from broker {leader} failed: {e}"))
            })?;
            for p in resp.topics.iter().flat_map(|topic| &topic.partitions) {
                if p.error_code != 0 {
                    return Err(MetadataLogError::Other(format!(
                        "ListOffsets partition {} error {}",
                        p.partition_index, p.error_code
                    )));
                }
                if let Ok(idx) = usize::try_from(p.partition_index)
                    && idx < offsets.len()
                {
                    offsets[idx] = p.offset;
                }
            }
        }
        Ok(offsets)
    }
}

/// Resolve every `host:port` of a bootstrap list inside the DNS deadline,
/// keeping each host name for TLS.
async fn resolve_bootstrap(
    bootstrap: &str,
    options: &ConnectionOptions,
) -> Result<Vec<(std::net::SocketAddr, String)>, MetadataLogError> {
    let mut resolved = Vec::new();
    for address in bootstrap.split(',').map(str::trim) {
        if address.is_empty() {
            continue;
        }
        let lookup = tokio::time::timeout(
            options.dns_timeout.time().to_std(),
            krabka_client_core::transport::resolve(address),
        )
        .await
        .map_err(|_elapsed| MetadataLogError::Other(format!("DNS lookup of {address} timed out")))?
        .map_err(|error| {
            MetadataLogError::Other(format!("DNS lookup of {address} failed: {error}"))
        })?;
        resolved.extend(
            lookup
                .into_iter()
                .map(|socket| (socket, connection_target_host(address).to_owned())),
        );
    }
    if resolved.is_empty() {
        return Err(MetadataLogError::Other(format!(
            "bootstrap {bootstrap:?} resolved to no address"
        )));
    }
    Ok(resolved)
}

/// The brokers a `Metadata` response names, in the pool's shape.
fn brokers_of(
    metadata: &krabka_protocol::owned::metadata_response::MetadataResponse,
) -> Vec<BrokerInfo> {
    metadata
        .brokers
        .iter()
        .map(|broker| BrokerInfo {
            id: broker.node_id,
            host: broker.host.clone(),
            port: broker.port,
            rack: broker.rack.clone(),
        })
        .collect()
}

/// The metadata scope of every RLMM client: only the metadata topic.
///
/// Kafka's `TopicBasedRemoteLogMetadataManager` reads through a consumer that
/// `assign`s the metadata topic partitions, and its `ConsumerMetadata` names
/// exactly the assigned topics. The RLMM creates the topic itself, so no
/// request may auto-create it with broker defaults.
pub(super) const METADATA_TOPIC_SCOPE: MetadataScope = MetadataScope::Topics {
    allow_auto_topic_creation: false,
};

pub(super) fn partition_leader(
    metadata: &krabka_protocol::owned::metadata_response::MetadataResponse,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    metadata
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic) && entry.error_code == 0)?
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition && entry.error_code == 0)
        .map(|entry| entry.leader_id)
        .filter(|leader| *leader >= 0)
}

impl Drop for KafkaMetadataEventLog {
    fn drop(&mut self) {
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            for state in subs.drain(..) {
                state.cancel_all();
            }
        }
    }
}

#[async_trait]
impl MetadataEventLog for KafkaMetadataEventLog {
    fn partition_count(&self) -> i32 { self.partition_count }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition, len = event.len()),
        err
    )]
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.publish_record(partition, None, Some(event)).await
    }

    async fn publish_keyed(
        &self,
        partition: i32,
        key: Bytes,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        self.publish_record(partition, Some(key), event).await
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (tx, rx) = metadata_event_channel(self.event_queue_capacity);
        let state = Arc::new(ConsumerState {
            bootstrap: self.bootstrap.clone(),
            client_id: format!("{}-consumer", self.client_id),
            security: self.security.clone(),
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            tx,
            fetch_max_wait: self.fetch_max_wait,
            fetch_max_bytes: self.fetch_max_bytes,
            fetch_retry_backoff: self.fetch_retry_backoff,
            dispatch_queue_capacity: self.dispatch_queue_capacity,
            frame_max: self.frame_max,
            tasks: StdMutex::new(HashMap::new()),
        });
        for ps in assignment {
            state.spawn_partition(ps);
        }
        if let Ok(mut subs) = self.subscriptions.try_lock() {
            subs.push(Arc::clone(&state));
        } else {
            warn!("KafkaMetadataEventLog: could not track subscription state");
        }
        let stream = unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) }).boxed();
        let handle: Arc<dyn AssignmentHandle> = Arc::new(KafkaAssignmentHandle { state });
        (stream, handle)
    }

    async fn low_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        self.list_offsets(-2).await // EARLIEST
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition_count = self.partition_count),
        err
    )]
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        self.list_offsets(-1).await // LATEST
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(topic = %self.topic, partition, start, end),
        err
    )]
    async fn visit_range(
        &self,
        partition: i32,
        start: i64,
        end: i64,
        visit: &mut RangeVisitor<'_>,
    ) -> Result<(), MetadataLogError> {
        let topic = RangeTopic {
            name: &self.topic,
            partition_count: self.partition_count,
            retry_backoff: self.fetch_retry_backoff.to_std(),
        };
        visit_range_pages(self, &topic, partition, start, end, visit).await
    }
}

impl RangeFetcher for KafkaMetadataEventLog {
    type Leader = Arc<Connection>;

    // cargo-mutants: needs a live leader; `range` tests the loop around it
    #[cfg_attr(test, mutants::skip)]
    async fn leader(&self, partition: i32) -> Result<Arc<Connection>, RangeReadFailure> {
        self.leader_connection(partition).await
    }

    // cargo-mutants: needs a live leader; `range` tests the loop around it
    #[cfg_attr(test, mutants::skip)]
    async fn fetch(
        &self,
        leader: &Arc<Connection>,
        partition: i32,
        offset: i64,
    ) -> Result<FetchPartitionResult, ClientError> {
        fetch_partition_with_isolation_progress(leader, IsolatedFetch {
            topic: &self.topic,
            topic_id: self.topic_id,
            partition,
            fetch_offset: offset,
            max_wait: self.fetch_max_wait,
            max: krabka_client_core::DEFAULT_FETCH_RESPONSE_MAX,
            partition_max: self.fetch_max_bytes,
            fetch_min: FetchMinBytes::default(),
            isolation_level: 0,
        })
        .await
    }
}

impl KafkaMetadataEventLog {
    async fn publish_record(
        &self,
        partition: i32,
        key: Option<Bytes>,
        event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        let record = producer_record(&self.topic, self.partition_count, partition, key, event)?;
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| MetadataLogError::Publish("the metadata log is read-only".into()))?;
        let meta = producer
            .send(record)
            .await
            .map_err(|e| MetadataLogError::Publish(e.to_string()))?;
        Ok(meta.offset)
    }

    /// A connection to the current leader of `partition`.
    ///
    /// A single-broker cluster that advertises port `0` leaves its broker out
    /// of the pool's registry. On such a cluster the bootstrap broker is the
    /// leader, so the read falls back to the bootstrap connection.
    async fn leader_connection(&self, partition: i32) -> Result<Arc<Connection>, RangeReadFailure> {
        let metadata = self
            .client
            .refresh_metadata()
            .await
            .map_err(|error| RangeReadFailure::client("Metadata failed", &error))?;
        let leader = partition_leader(&metadata, &self.topic, partition).ok_or_else(|| {
            RangeReadFailure::retriable(format!(
                "{} partition {partition} has no available leader",
                self.topic
            ))
        })?;
        self.readers.refresh_brokers(&brokers_of(&metadata)).await;
        match self.readers.get(leader).await {
            Err(ClientError::Disconnected) if !self.readers.knows_broker(leader) => {
                self.readers.bootstrap_connection().await
            }
            connection => connection,
        }
        .map_err(|error| {
            RangeReadFailure::client(&format!("connect to broker {leader} failed"), &error)
        })
    }
}

fn producer_record(
    topic: &str,
    partition_count: i32,
    partition: i32,
    key: Option<Bytes>,
    event: Option<Bytes>,
) -> Result<ProducerRecord, MetadataLogError> {
    if partition < 0 || partition >= partition_count {
        return Err(MetadataLogError::PartitionOutOfRange {
            partition,
            count: partition_count,
        });
    }
    Ok(ProducerRecord {
        topic: topic.to_owned(),
        partition: Some(partition),
        key,
        value: event,
        ..Default::default()
    })
}

fn usize_count(n: i32) -> Result<usize, MetadataLogError> {
    usize::try_from(n).map_err(|_| MetadataLogError::Other(format!("partition_count {n} negative")))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[test]
    fn metadata_requests_name_only_the_metadata_topic_without_auto_creation() {
        use {
            krabka_client_core::MetadataTopics,
            krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic},
        };

        let topics = MetadataTopics::new(METADATA_TOPIC_SCOPE);
        topics.set([config::METADATA_TOPIC]);

        check!(
            topics.request()
                == MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: Some(config::METADATA_TOPIC.to_owned()),
                        ..Default::default()
                    }]),
                    allow_auto_topic_creation: false,
                    ..Default::default()
                }
        );
    }

    #[tokio::test]
    async fn open_read_only_rejects_invalid_policy_before_connecting() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: Time::ZERO,
            ..KafkaMetadataLogConfig::new("not a socket address")
        };

        let error = KafkaMetadataEventLog::open_read_only(cfg)
            .await
            .err()
            .expect("invalid policy must fail before network I/O");
        assert!(error.to_string().contains("topic_create_timeout"));
    }

    #[tokio::test]
    async fn bootstrap_resolution_names_the_address_it_could_not_use() {
        let options = ConnectionOptions::default();
        let cases = [
            ("", "bootstrap \"\" resolved to no address"),
            (" , ", "bootstrap \" , \" resolved to no address"),
            ("no-port", "DNS lookup of no-port failed"),
        ];
        for (bootstrap, expected) in cases {
            let error = resolve_bootstrap(bootstrap, &options).await.unwrap_err();
            check!(
                error.to_string().contains(expected),
                "{bootstrap:?}: {error}"
            );
        }

        let resolved = resolve_bootstrap(" 127.0.0.1:9092, ,localhost:9093", &options)
            .await
            .unwrap();
        check!(
            resolved.first() == Some(&("127.0.0.1:9092".parse().unwrap(), "127.0.0.1".to_owned()))
        );
        check!(
            resolved
                .iter()
                .skip(1)
                .all(|(address, host)| address.port() == 9093 && host == "localhost")
        );
    }

    #[tokio::test]
    async fn start_rejects_invalid_policy_before_connecting() {
        let cfg = KafkaMetadataLogConfig {
            topic_create_timeout: Time::ZERO,
            ..KafkaMetadataLogConfig::new("not a socket address")
        };

        let Err(error) = KafkaMetadataEventLog::start(cfg).await else {
            panic!("invalid policy must fail before network I/O");
        };
        assert!(error.to_string().contains("topic_create_timeout"));
    }

    #[test]
    fn keyed_tombstone_record_is_partitioned_and_preserves_the_null_value() {
        let record = producer_record(
            "__diskless_wal_index",
            3,
            2,
            Some(Bytes::from_static(b"range")),
            None,
        )
        .unwrap();

        assert!(record.topic == "__diskless_wal_index");
        assert!(record.partition == Some(2));
        assert!(record.key.as_deref() == Some(b"range".as_slice()));
        assert!(record.value.is_none());
    }

    #[test]
    fn keyed_record_rejects_out_of_range_partitions() {
        for partition in [-1, 3] {
            let error = producer_record("index", 3, partition, None, None).unwrap_err();
            assert!(matches!(
                error,
                MetadataLogError::PartitionOutOfRange {
                    partition: got,
                    count: 3
                } if got == partition
            ));
        }
    }
}
