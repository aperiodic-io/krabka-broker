//! The dead-letter write: Kafka's `ShareGroupDLQStateManager`.
//!
//! [`DlqWriter`] validates the group's topic, creates it when the cluster
//! allows and it is missing, and produces the dead-letter records to the
//! leader of the partition that the source partition maps to. Kafka sends
//! both requests through an inter-broker `NetworkClient`, to a random broker
//! for the topic creation and to the partition leader for the produce, and
//! the leader authorizes and appends like any other produce. This does the
//! same over the inter-broker listener, so the append takes the ordinary
//! produce path with its checks, its throttles and its replication.
//!
//! Each request retries as Kafka's does: up to 5 attempts, backing off from
//! 1 s to 30 s. A write that runs out of attempts, or that a broker refuses
//! for good, fails, and the leader manager archives the records regardless.
//!
//! The produce requests to one leader share one connection, opened when the
//! first needs it and kept for the ones after it, as Kafka's send thread keeps
//! one `NetworkClient` for every dead-letter produce. A write that pays for a
//! TCP, TLS and SASL setup each time would turn a consumer that rejects at a
//! high rate into a storm of connections.
//!
//! The rounds of every write for one leader are coalesced, as Kafka's send
//! thread coalesces its handlers ([`super::coalesce`]): while a request is in
//! flight to a leader, the rounds that arrive for it wait, and go out in as
//! few requests as `max.message.bytes` allows. A round is answered with the
//! response to the request that carried it, so a write ends when its own
//! records are written, and it retries on its own.
//!
//! Each round counts on the `DeadLetterQueue*` meters of Kafka's
//! `ShareGroupMetrics`: an attempt to produce it, the records of a round that
//! is written, and a write that fails.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use exponential_backoff::Backoff;
use krabka_compression::RecordDecompressionPolicy;
use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::{
    owned::{
        create_topics_response::CreateTopicsResponse, produce_request::ProduceRequest,
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_security::ListenerProtocol;
use krabka_units::convert::ByteSizeExt as _;

use super::{
    DlqError, DlqRequest, DlqSink,
    coalesce::{Admitted, Coalescer, Produce, ProduceTransport},
    record::{RangeContext, RoundBounds, build_round, destination_partition},
    source::{Fetched, SourceTier, fetch},
    validate::{ClusterSettings, GroupSettings, TopicState, validate},
};
use crate::{
    auto_topic_creation::AutoTopicCreation,
    codes,
    config::BrokerConfig,
    config_keys::{BrokerLogDefaults, resolve_max_message_bytes},
    metadata_source::MetadataSource,
    metrics::BrokerMetrics,
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    topic_creator::TopicCreatorError,
};

/// The attempts of one request, and the pauses between them: Kafka's
/// `MAX_REQUEST_ATTEMPTS`, `REQUEST_BACKOFF_MS` and `REQUEST_BACKOFF_MAX_MS`.
const MAX_ATTEMPTS: u32 = 5;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The result of one attempt of a request.
enum Attempt<T> {
    Done(T),
    /// Try again after a pause: a leader that is not there yet, a metadata
    /// image that lags a topic creation, a connection that dropped.
    Retry(String),
    /// A broker refused for good.
    Fatal(DlqError),
}

/// Where the dead-letter records go: the leader of one partition of the
/// dead-letter topic.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    topic: String,
    topic_id: uuid::Uuid,
    partition: i32,
    leader: NodeId,
    /// The topic's `max.message.bytes`.
    max_message_bytes: i32,
}

/// The dead-letter topic of `topic` as `image` knows it, for the records of
/// `source_partition`: Kafka's `populateDLQTopicData`.
///
/// # Errors
///
/// Why the topic is not usable yet. The topic may be a moment old, so the
/// caller tries again.
fn resolve_target(
    image: &MetadataImage,
    topic: &str,
    source_partition: i32,
    max_message_bytes: i32,
) -> Result<Target, String> {
    let record = image
        .topic(topic)
        .ok_or_else(|| format!("DLQ topic {topic} is not in the metadata image"))?;
    // The count of the partition records is the authoritative one: a
    // `TopicRecord` carries none.
    let partitions = image.topic_partition_count(topic);
    if partitions <= 0 {
        return Err(format!("DLQ topic {topic} has no partitions yet"));
    }
    let partition = destination_partition(source_partition, partitions);
    let leader = image
        .partition(topic, partition)
        .map(|partition| partition.leader)
        .filter(|leader| image.broker(*leader).is_some())
        .ok_or_else(|| format!("DLQ topic {topic}-{partition} has no leader that is up yet"))?;
    Ok(Target {
        topic: topic.to_owned(),
        topic_id: record.topic_id,
        partition,
        leader,
        max_message_bytes,
    })
}

/// What one produce answer means: Kafka's `handleProduceResponse`.
///
/// The answer to a coalesced request holds a row for every partition of every
/// topic in it, so the row is the one for the topic of `target` and its
/// partition, as Kafka finds it by topic id.
fn classify_produce(response: &ProduceResponse, target: &Target) -> Attempt<()> {
    let topic_id = WireUuid(*target.topic_id.as_bytes());
    let row = response
        .responses
        .iter()
        .filter(|topic| topic.topic_id == topic_id || topic.name == target.topic)
        .flat_map(|topic| &topic.partition_responses)
        .find(|row| row.index == target.partition);
    let Some(row) = row else {
        return Attempt::Fatal(DlqError::Write(format!(
            "Received empty partition produce response from the DLQ topic {}-{}.",
            target.topic, target.partition
        )));
    };
    match row.error_code {
        codes::NONE => Attempt::Done(()),
        codes::NOT_LEADER_OR_FOLLOWER => Attempt::Retry(format!(
            "{}-{} is not led by node {}",
            target.topic, target.partition, target.leader
        )),
        code => Attempt::Fatal(DlqError::Write(format!(
            "Unable to produce to the DLQ topic {}-{}: error {code}{}.",
            target.topic,
            target.partition,
            row.error_message
                .as_deref()
                .map(|message| format!(", {message}"))
                .unwrap_or_default()
        ))),
    }
}

/// What one topic creation answer means: Kafka's `handleCreateTopicsResponse`.
fn classify_create(
    result: Result<CreateTopicsResponse, TopicCreatorError>,
    topic: &str,
) -> Attempt<()> {
    match result {
        Ok(response) => match response.topics.first().map(|row| row.error_code) {
            Some(codes::NONE) => Attempt::Done(()),
            // The topic was created by a request that was in flight, or the
            // controller is throttling: the next attempt looks at the image
            // again (see `ensure_topic`), and asks again only if the topic is
            // still missing.
            Some(code @ (codes::TOPIC_ALREADY_EXISTS | codes::THROTTLING_QUOTA_EXCEEDED)) => {
                Attempt::Retry(format!("creating {topic}: error {code}"))
            }
            Some(code) => Attempt::Fatal(DlqError::Write(format!(
                "Unable to create the DLQ topic {topic}: error {code}."
            ))),
            None => Attempt::Fatal(DlqError::Write(format!(
                "The DLQ topic {topic} is not in the create topics response."
            ))),
        },
        Err(TopicCreatorError::Timeout) => Attempt::Retry(format!("creating {topic} timed out")),
        Err(error) => Attempt::Fatal(DlqError::Write(format!(
            "Unable to create the DLQ topic {topic}: {error}."
        ))),
    }
}

/// Runs `attempt` under Kafka's retry rule: up to [`MAX_ATTEMPTS`] tries with
/// a growing pause between them.
async fn with_backoff<T, F, Fut>(what: &str, mut attempt: F) -> Result<T, DlqError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Attempt<T>>,
{
    let backoff = Backoff::new(MAX_ATTEMPTS, BACKOFF_MIN, Some(BACKOFF_MAX));
    for delay in backoff {
        match attempt().await {
            Attempt::Done(value) => return Ok(value),
            Attempt::Fatal(error) => return Err(error),
            Attempt::Retry(reason) => match delay {
                Some(delay) => {
                    tracing::debug!(%reason, what, "dead-letter request will retry");
                    tokio::time::sleep(delay).await;
                }
                None => {
                    return Err(DlqError::Write(format!(
                        "Exhausted max retries to {what}: {reason}."
                    )));
                }
            },
        }
    }
    Err(DlqError::Write(format!("No attempt was made to {what}.")))
}

/// Gets the dead-letter `topic` made: `state` says how the topic stands in
/// the metadata image, and `create` sends one `CreateTopics` request.
///
/// A retry of Kafka's topic creation starts over from the top. The timer task
/// puts the handler back through `enqueue`, which validates the topic, and
/// `dlqTopicExists`, which reads the metadata cache, before a `CreateTopics`
/// goes out again. So a request that answers `TOPIC_ALREADY_EXISTS`, because
/// a concurrent write made the topic, ends at the next attempt, when the image
/// shows the topic. The request only repeats while the topic is still missing.
async fn ensure_topic<S, C, Fut>(topic: &str, state: S, create: C) -> Result<(), DlqError>
where
    S: Fn() -> Result<TopicState, DlqError>,
    C: Fn() -> Fut,
    Fut: Future<Output = Result<CreateTopicsResponse, TopicCreatorError>>,
{
    with_backoff(&format!("create the DLQ topic {topic}"), || async {
        match state() {
            Err(error) => Attempt::Fatal(error),
            Ok(TopicState::Exists) => Attempt::Done(()),
            Ok(TopicState::Missing) => classify_create(create().await, topic),
        }
    })
    .await
}

/// The inter-broker listener that the requests go over.
struct Listener {
    protocol: ListenerProtocol,
    name: String,
    server_name: String,
}

/// A connection that a pool can tell has closed.
trait Pooled: Clone {
    fn is_open(&self) -> bool;
}

impl Pooled for krabka_client_core::Connection {
    fn is_open(&self) -> bool {
        !self.is_closed()
    }
}

/// One connection for each leader, dialled when a write first needs it and
/// shared by every write that follows, until it closes.
///
/// A [`krabka_client_core::Connection`] pipelines the requests of its clones,
/// so writers that reach one leader at once use the one connection. Writers
/// that need a leader with no open connection wait for a single dial.
struct LeaderConnections<C> {
    slots: std::sync::Mutex<HashMap<NodeId, Arc<tokio::sync::Mutex<Option<C>>>>>,
}

impl<C> Default for LeaderConnections<C> {
    fn default() -> Self {
        Self {
            slots: std::sync::Mutex::default(),
        }
    }
}

impl<C: Pooled> LeaderConnections<C> {
    /// The open connection to `leader`, dialled with `dial` when there is
    /// none.
    async fn get<F, Fut>(&self, leader: NodeId, dial: F) -> Result<C, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<C, String>>,
    {
        let slot = {
            let mut slots = self.slots.lock().expect("connection pool lock");
            Arc::clone(slots.entry(leader).or_default())
        };
        let mut held = slot.lock().await;
        if let Some(open) = held.as_ref().filter(|connection| connection.is_open()) {
            return Ok(open.clone());
        }
        let dialled = dial().await?;
        *held = Some(dialled.clone());
        Ok(dialled)
    }
}

/// Sends the produce requests over the inter-broker listener, on one
/// connection for each leader.
struct InterBrokerProduce {
    controller: Arc<dyn MetadataSource>,
    client: Arc<InterBrokerClient>,
    listener: Listener,
    /// The connection to each leader that a produce goes to.
    connections: LeaderConnections<krabka_client_core::Connection>,
    client_id: String,
}

#[async_trait]
impl ProduceTransport for InterBrokerProduce {
    /// One produce request, over the connection to `node` that the writes
    /// share.
    async fn send(&self, node: NodeId, request: ProduceRequest) -> Result<ProduceResponse, String> {
        let image = self.controller.current_image();
        let broker = image
            .broker(node)
            .ok_or_else(|| format!("node {node} is not in the metadata image"))?;
        let (host, port) = crate::broker::registered_listener_endpoint(broker, &self.listener.name);
        let host = host.to_owned();
        let connection = self
            .connections
            .get(node, || async {
                let options = krabka_client_core::ConnectionOptions {
                    client_id: self.client_id.clone(),
                    ..krabka_client_core::ConnectionOptions::default()
                };
                self.client
                    .connect_as_connection(
                        &host,
                        port,
                        self.listener.protocol,
                        &self.listener.server_name,
                        options,
                    )
                    .await
                    .map_err(|error| format!("connect to {host}:{port}: {error}"))
            })
            .await?;
        connection.send(request).await.map_err(|error| {
            // A connection that failed a request may be half dead: closing it
            // makes the next attempt dial afresh.
            connection.clone().close();
            format!("produce to {host}:{port}: {error}")
        })
    }
}

/// Writes dead-letter records: the [`DlqSink`] of a running broker.
pub struct DlqWriter {
    node_id: NodeId,
    controller: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    topics: Arc<AutoTopicCreation>,
    /// The produce requests, coalesced for each destination leader.
    sender: Coalescer<InterBrokerProduce>,
    /// The static log settings, under the dynamic broker defaults: the
    /// `message.max.bytes` of a topic that sets no `max.message.bytes`.
    base_log: LogConfig,
    decompression: RecordDecompressionPolicy,
    /// Where the `DeadLetterQueue*` meters are counted.
    metrics: BrokerMetrics,
    /// The remote tier, which a copy reads for a source offset that only the
    /// tier holds (KIP-405). Empty on a broker with no tiered storage, and
    /// until the broker has built its reader.
    remote_reader: std::sync::OnceLock<Arc<crate::remote_reader::RemoteReader>>,
}

impl DlqWriter {
    pub fn new(
        config: &BrokerConfig,
        controller: Arc<dyn MetadataSource>,
        partitions: Arc<PartitionRegistry>,
        topics: Arc<AutoTopicCreation>,
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        metrics: BrokerMetrics,
    ) -> Self {
        let node_id = NodeId(config.node_id.0);
        Self {
            node_id,
            controller: Arc::clone(&controller),
            partitions,
            topics,
            sender: Coalescer::new(InterBrokerProduce {
                controller,
                client,
                listener: Listener {
                    protocol: listener_protocol,
                    name: config.inter_broker_listener_name.clone(),
                    server_name: config.inter_broker_server_name.clone(),
                },
                connections: LeaderConnections::default(),
                client_id: format!("krabka-broker-dlq-{node_id}"),
            }),
            base_log: config.log_config.clone(),
            decompression: config.record_decompression_policy().unwrap_or_default(),
            metrics,
            remote_reader: std::sync::OnceLock::new(),
        }
    }

    /// Lets a copy read the source records that only the remote tier holds.
    ///
    /// The broker builds its remote reader after its coordinators, so it hands
    /// the reader to the writer here once the reader exists. A second call
    /// changes nothing.
    pub(crate) fn set_remote_reader(&self, reader: Arc<crate::remote_reader::RemoteReader>) {
        let _ = self.remote_reader.set(reader);
    }

    /// The `max.message.bytes` of `topic`, as this node runs it.
    fn max_message_bytes(&self, image: &MetadataImage, topic: &str) -> i32 {
        let broker_default =
            BrokerLogDefaults::resolve(image, self.node_id, &self.base_log, (None, None))
                .max_message_size;
        i32::try_from(resolve_max_message_bytes(image, topic, broker_default).bytes_usize())
            .unwrap_or(i32::MAX)
    }

    /// Creates the dead-letter topic of `settings`, unless the image shows it
    /// by the time an attempt looks.
    async fn create_topic(&self, group: &str, settings: &GroupSettings) -> Result<(), DlqError> {
        let topic = settings.topic.as_str();
        ensure_topic(
            topic,
            || {
                let image = self.controller.current_image();
                let cluster = ClusterSettings::resolve(&image, self.node_id);
                validate(&image, group, Some(settings.clone()), &cluster).map(|(_, state)| state)
            },
            || self.topics.create_dead_letter_topic(topic),
        )
        .await
    }

    /// The dead-letter partition for `source_partition`, once the image shows
    /// it. It takes a moment for a new topic.
    async fn target(&self, topic: &str, source_partition: i32) -> Result<Target, DlqError> {
        with_backoff(&format!("find the DLQ topic {topic}"), || async {
            let image = self.controller.current_image();
            let max_message_bytes = self.max_message_bytes(&image, topic);
            match resolve_target(&image, topic, source_partition, max_message_bytes) {
                Ok(target) => Attempt::Done(target),
                Err(reason) => Attempt::Retry(reason),
            }
        })
        .await
    }

    /// Produces `batch` to the leader of `target`, with all replicas
    /// acknowledging, in a request that the rounds of other writes to the
    /// same leader share.
    async fn produce(
        &self,
        group: &str,
        target: &Target,
        batch: &RecordBatch,
    ) -> Result<(), DlqError> {
        produce_round(
            &self.sender,
            &self.metrics,
            group,
            target,
            // The leader can move between attempts, so it is read afresh.
            || {
                self.controller
                    .current_image()
                    .partition(&target.topic, target.partition)
                    .map_or(target.leader, |partition| partition.leader)
            },
            batch,
        )
        .await
    }
}

/// Produces one round, `batch` for `target`, to the node that `leader` names,
/// and counts it on the group's meters: Kafka's `ProduceRequestHandler`, which
/// retries a round on its own, and marks `recordDLQProduce` when a request
/// takes it, `recordDLQRecordWrite` when its records are written, and
/// `recordDLQProduceFailed` when the write is given up.
///
/// A produce answer that has no row for the round's partition is a failed
/// write here. Kafka's `handleProduceResponse` fails the write without marking
/// `recordDLQProduceFailed` in that case, which only a broker that breaks the
/// protocol can bring about, so the meter here says what happened to the
/// records.
async fn produce_round<T: ProduceTransport>(
    sender: &Coalescer<T>,
    metrics: &BrokerMetrics,
    group: &str,
    target: &Target,
    leader: impl Fn() -> NodeId,
    batch: &RecordBatch,
) -> Result<(), DlqError> {
    let admitted: Admitted = {
        let (metrics, group) = (metrics.clone(), group.to_owned());
        Arc::new(move || metrics.record_share_dlq_produce(&group))
    };
    let written = with_backoff(
        &format!(
            "produce to the DLQ topic {}-{}",
            target.topic, target.partition
        ),
        || async {
            let produce = Produce {
                topic: target.topic.clone(),
                topic_id: target.topic_id,
                partition: target.partition,
                max_message_bytes: target.max_message_bytes,
                batch: batch.clone(),
                admitted: Arc::clone(&admitted),
            };
            match sender.produce(leader(), produce).await {
                Ok(response) => classify_produce(&response, target),
                Err(reason) => Attempt::Retry(reason),
            }
        },
    )
    .await;
    match &written {
        Ok(()) => metrics.record_share_dlq_records(group, batch.records.len()),
        Err(_) => metrics.record_share_dlq_produce_failed(group),
    }
    written
}

#[async_trait]
impl DlqSink for DlqWriter {
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
        let image = self.controller.current_image();
        let cluster = ClusterSettings::resolve(&image, self.node_id);
        let (settings, state) = validate(
            &image,
            &request.group,
            GroupSettings::resolve(&image, &request.group),
            &cluster,
        )?;
        if state == TopicState::Missing {
            self.create_topic(&request.group, &settings).await?;
        }
        // Kafka puts the id of a topic whose name it cannot find in the header.
        let source_topic = image
            .topic_name_by_id(&request.topic_id)
            .map_or_else(|| request.topic_id.to_string(), str::to_owned);
        let source = self
            .partitions
            .get(&source_topic, PartitionIndex(request.source_partition));
        let tier = self.remote_reader.get().map(|reader| SourceTier {
            reader,
            metrics: &self.metrics,
            tp: krabka_remote_storage::TopicIdPartition::new(
                request.topic_id,
                source_topic.clone(),
                request.source_partition,
            ),
        });
        let context = RangeContext {
            group: &request.group,
            source_topic: &source_topic,
            source_partition: request.source_partition,
            delivery_count: request.delivery_count,
            cause: request.cause,
        };
        let (first, last) = (request.first.0, request.last.0);
        let mut next = first;
        while next <= last {
            let target = self
                .target(&settings.topic, request.source_partition)
                .await?;
            let fetched = if settings.copy_record {
                let budget = usize::try_from(target.max_message_bytes).unwrap_or(usize::MAX);
                fetch(
                    source.as_ref(),
                    tier.as_ref(),
                    (next, last),
                    budget,
                    self.decompression,
                )
                .await
            } else {
                Fetched {
                    records: std::collections::BTreeMap::new(),
                    last_resolved: last,
                }
            };
            let round = build_round(
                &context,
                &fetched.records,
                RoundBounds {
                    next,
                    last,
                    last_resolved: fetched.last_resolved,
                    max_message_bytes: target.max_message_bytes,
                },
                crate::time_util::now_ms(),
            );
            self.produce(&request.group, &target, &round.batch).await?;
            next = round.last_offset + 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, PartitionRecord, TopicRecord};
    use krabka_protocol::owned::{
        create_topics_response::CreatableTopicResult,
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    };

    use super::{super::coalesce::test_support::FakeBroker, *};

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum BrokerRegistration {
        Registered,
        AwaitingRegistration,
    }

    #[derive(Clone, Copy)]
    struct FixtureTopicId(u8);

    #[derive(Clone, Copy)]
    struct RoundRecordCount(usize);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ProduceRoundSetup {
        #[default(FixtureTopicId(1))]
        id: FixtureTopicId,
        #[default(RoundRecordCount(1))]
        records: RoundRecordCount,
    }

    /// An image with the topic `dlq.g`, one partition for each of `leaders`,
    /// as `(leader, whether that broker is registered)`.
    fn image_with_topic(leaders: &[(NodeId, BrokerRegistration)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "dlq.g".into(),
            topic_id: uuid::Uuid::from_bytes([7; 16]),
            partitions: i32::try_from(leaders.len()).unwrap(),
            replication_factor: 1,
        }));
        for (index, (leader, registered)) in leaders.iter().enumerate() {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "dlq.g".into(),
                partition: i32::try_from(index).unwrap(),
                leader: *leader,
                replicas: vec![*leader],
                isr: vec![*leader],
                ..Default::default()
            }));
            if *registered == BrokerRegistration::Registered {
                image.apply(&MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        host: "h".into(),
                        ..crate::test_support::broker_registration(*leader)
                    },
                ));
            }
        }
        image
    }

    fn target(partition: PartitionIndex, leader: NodeId) -> Target {
        Target {
            topic: "dlq.g".into(),
            topic_id: uuid::Uuid::from_bytes([7; 16]),
            partition: partition.0,
            leader,
            max_message_bytes: 1_048_588,
        }
    }

    /// Kafka's `populateDLQTopicData`: the partition is the source partition
    /// modulo the count, and the topic is not usable until the image holds it
    /// with a leader that is registered.
    #[test]
    fn the_target_follows_the_source_partition_and_waits_for_a_leader() {
        let ready = image_with_topic(&[
            (NodeId(1), BrokerRegistration::Registered),
            (NodeId(2), BrokerRegistration::Registered),
            (NodeId(3), BrokerRegistration::Registered),
        ]);
        // Partition 1 is led by node 9, which has not registered.
        let no_leader = image_with_topic(&[
            (NodeId(1), BrokerRegistration::Registered),
            (NodeId(9), BrokerRegistration::AwaitingRegistration),
            (NodeId(3), BrokerRegistration::Registered),
        ]);

        let actual = [
            resolve_target(&ready, "dlq.g", 4, 1_048_588),
            resolve_target(&ready, "dlq.g", 3, 1_048_588),
            resolve_target(&no_leader, "dlq.g", 4, 1_048_588),
            resolve_target(&ready, "missing", 0, 1_048_588),
        ];

        assert!(
            actual
                == [
                    Ok(target(PartitionIndex(1), NodeId(2))),
                    Ok(target(PartitionIndex(0), NodeId(1))),
                    Err("DLQ topic dlq.g-1 has no leader that is up yet".to_owned()),
                    Err("DLQ topic missing is not in the metadata image".to_owned()),
                ]
        );
    }

    fn produce_response(
        index: PartitionIndex,
        error_code: crate::test_support::KafkaErrorCode,
    ) -> ProduceResponse {
        ProduceResponse {
            responses: vec![TopicProduceResponse {
                topic_id: WireUuid([7; 16]),
                partition_responses: vec![PartitionProduceResponse {
                    index: index.0,
                    error_code: error_code.0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn outcome<T>(attempt: &Attempt<T>) -> &'static str {
        match attempt {
            Attempt::Done(_) => "done",
            Attempt::Retry(_) => "retry",
            Attempt::Fatal(_) => "fatal",
        }
    }

    /// Kafka's `handleProduceResponse`: success is done, a leader that has
    /// moved is tried again, any other error is final, and so is an answer with
    /// no row for the partition.
    #[test]
    fn a_produce_answer_is_done_retried_or_final() {
        let target = target(PartitionIndex(1), NodeId(2));
        let cases = [
            (
                produce_response(
                    PartitionIndex(1),
                    crate::test_support::KafkaErrorCode(codes::NONE),
                ),
                "done",
            ),
            (
                produce_response(
                    PartitionIndex(1),
                    crate::test_support::KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER),
                ),
                "retry",
            ),
            (
                produce_response(
                    PartitionIndex(1),
                    crate::test_support::KafkaErrorCode(codes::MESSAGE_TOO_LARGE),
                ),
                "fatal",
            ),
            (
                produce_response(
                    PartitionIndex(1),
                    crate::test_support::KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED),
                ),
                "fatal",
            ),
            (
                produce_response(
                    PartitionIndex(0),
                    crate::test_support::KafkaErrorCode(codes::NONE),
                ),
                "fatal",
            ),
            (ProduceResponse::default(), "fatal"),
        ];

        assert!(
            cases
                .iter()
                .map(|(response, _)| outcome(&classify_produce(response, &target)))
                .collect::<Vec<_>>()
                == cases.iter().map(|(_, want)| *want).collect::<Vec<_>>()
        );
    }

    /// The answer to a coalesced request has a row for each partition of each
    /// topic in it, and a round reads the row of its own topic: the row of
    /// another topic's partition of the same index is not its answer.
    #[test]
    fn a_produce_answer_is_read_from_the_row_of_the_topic() {
        let other_topic = ProduceResponse {
            responses: vec![TopicProduceResponse {
                topic_id: WireUuid([8; 16]),
                partition_responses: vec![PartitionProduceResponse {
                    index: 1,
                    error_code: codes::NONE,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let both = ProduceResponse {
            responses: [other_topic.responses.clone(), {
                produce_response(
                    PartitionIndex(1),
                    crate::test_support::KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER),
                )
                .responses
            }]
            .concat(),
            ..Default::default()
        };

        assert!(
            [
                outcome(&classify_produce(
                    &other_topic,
                    &target(PartitionIndex(1), NodeId(2))
                )),
                outcome(&classify_produce(
                    &both,
                    &target(PartitionIndex(1), NodeId(2))
                )),
            ] == ["fatal", "retry"]
        );
    }

    /// A produce round of the tests: `records` records for partition 0 of the
    /// topic `dlq.<id>`.
    fn round_of(setup: ProduceRoundSetup) -> (Target, RecordBatch) {
        let ProduceRoundSetup { id, records } = setup;
        let id = id.0;
        let records = records.0;
        let target = Target {
            topic: format!("dlq.{id}"),
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            partition: 0,
            leader: NodeId(1),
            max_message_bytes: 1_048_588,
        };
        let batch = RecordBatch {
            last_offset_delta: i32::try_from(records).unwrap() - 1,
            records: (0..records)
                .map(|delta| krabka_protocol::records::Record {
                    offset_delta: i32::try_from(delta).unwrap(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        (target, batch)
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct CoalescedWriteSetup {
        #[default(RoundRecordCount(2))]
        first_records: RoundRecordCount,
        #[default(RoundRecordCount(1))]
        second_records: RoundRecordCount,
    }

    struct CoalescedWrites {
        sender: Coalescer<FakeBroker>,
        metrics: BrokerMetrics,
        first: Target,
        first_batch: RecordBatch,
        second: Target,
        second_batch: RecordBatch,
    }

    impl CoalescedWrites {
        async fn first_write(&self) -> Result<(), DlqError> {
            produce_round(
                &self.sender,
                &self.metrics,
                "g1",
                &self.first,
                || NodeId(1),
                &self.first_batch,
            )
            .await
        }
    }

    fn coalesced_writes(broker: FakeBroker, setup: CoalescedWriteSetup) -> CoalescedWrites {
        let (first, first_batch) = round_of(ProduceRoundSetup {
            records: setup.first_records,
            ..Default::default()
        });
        let (second, second_batch) = round_of(ProduceRoundSetup {
            id: FixtureTopicId(2),
            records: setup.second_records,
        });
        CoalescedWrites {
            sender: Coalescer::new(broker),
            metrics: BrokerMetrics::new(),
            first,
            first_batch,
            second,
            second_batch,
        }
    }

    /// The answer of a broker that gives every partition the code that
    /// `code` says for the request's index, the topic and the partition.
    fn answer_rows(
        request: &ProduceRequest,
        index: usize,
        code: impl Fn(usize, &str, i32) -> i16,
    ) -> ProduceResponse {
        ProduceResponse {
            responses: request
                .topic_data
                .iter()
                .map(|topic| TopicProduceResponse {
                    name: topic.name.clone(),
                    topic_id: topic.topic_id,
                    partition_responses: topic
                        .partition_data
                        .iter()
                        .map(|partition| PartitionProduceResponse {
                            index: partition.index,
                            error_code: code(index, &topic.name, partition.index),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The topics of each request that the broker was sent, and its node.
    fn topics_sent(broker: &FakeBroker) -> Vec<(NodeId, Vec<String>)> {
        broker
            .sent()
            .into_iter()
            .map(|(node, request)| {
                (
                    node,
                    request
                        .topic_data
                        .into_iter()
                        .map(|topic| topic.name)
                        .collect(),
                )
            })
            .collect()
    }

    krabka_macros::share_dlq_meters! {
    /// What the meters of `group` count: the records written, the attempts to
    /// produce, and the writes that failed.
        meters, BrokerMetrics, crate::metrics::ShareGroupIdLabel
    }

    /// Two writes for the same leader go out in one request, and each write
    /// ends with its own records: the one whose partition the leader accepted
    /// is done, and the one it refused for good has failed, whichever request
    /// carried them. Each counts on its own group's meters.
    #[tokio::test(start_paused = true)]
    async fn each_write_in_a_coalesced_request_ends_with_its_own_records() {
        let broker = FakeBroker::answering(|index, request| {
            Ok(answer_rows(request, index, |_, topic, _| {
                if topic == "dlq.2" {
                    codes::MESSAGE_TOO_LARGE
                } else {
                    codes::NONE
                }
            }))
        });
        let writes = coalesced_writes(broker.clone(), CoalescedWriteSetup::default());

        let (done, refused) = futures_util::future::join(
            writes.first_write(),
            produce_round(
                &writes.sender,
                &writes.metrics,
                "g2",
                &writes.second,
                || NodeId(1),
                &writes.second_batch,
            ),
        )
        .await;

        assert!(
            (
                done,
                refused,
                topics_sent(&broker),
                meters(&writes.metrics, "g1"),
                meters(&writes.metrics, "g2"),
            ) == (
                Ok(()),
                Err(DlqError::Write(format!(
                    "Unable to produce to the DLQ topic dlq.2-0: error {}.",
                    codes::MESSAGE_TOO_LARGE
                ))),
                vec![(NodeId(1), vec!["dlq.1".to_owned(), "dlq.2".to_owned()])],
                (2, 1, 0),
                (0, 1, 1),
            )
        );
    }

    /// A write that the leader answers with `NOT_LEADER_OR_FOLLOWER` retries on
    /// its own, after the backoff, to the leader the image names by then, and
    /// the write that shared its first request is not sent again. Each attempt
    /// counts as a produce, and the records count once, when they are written.
    #[tokio::test(start_paused = true)]
    async fn a_write_retries_on_its_own_to_the_leader_of_the_moment() {
        let broker = FakeBroker::answering(|index, request| {
            Ok(answer_rows(request, index, |index, topic, _| {
                if index == 0 && topic == "dlq.2" {
                    codes::NOT_LEADER_OR_FOLLOWER
                } else {
                    codes::NONE
                }
            }))
        });
        let writes = coalesced_writes(
            broker.clone(),
            CoalescedWriteSetup {
                first_records: RoundRecordCount(1),
                second_records: RoundRecordCount(3),
            },
        );
        let leaders = std::cell::Cell::new(0_u64);

        let (done, retried) = futures_util::future::join(
            writes.first_write(),
            produce_round(
                &writes.sender,
                &writes.metrics,
                "g2",
                &writes.second,
                || {
                    // The leader moves to node 2 after the first attempt.
                    leaders.set(leaders.get() + 1);
                    NodeId(leaders.get())
                },
                &writes.second_batch,
            ),
        )
        .await;

        assert!(
            (
                done,
                retried,
                topics_sent(&broker),
                meters(&writes.metrics, "g1"),
                meters(&writes.metrics, "g2"),
            ) == (
                Ok(()),
                Ok(()),
                vec![
                    (NodeId(1), vec!["dlq.1".to_owned(), "dlq.2".to_owned()]),
                    (NodeId(2), vec!["dlq.2".to_owned()]),
                ],
                (1, 1, 0),
                (3, 2, 0),
            )
        );
    }

    /// A write that no attempt gets through runs out of attempts: five, as in
    /// Kafka's `MAX_REQUEST_ATTEMPTS`. Whether the leader keeps answering that
    /// it is not the leader or the request never gets a response, it counts one
    /// failed write and an attempt for each try.
    #[tokio::test(start_paused = true)]
    async fn a_write_that_runs_out_of_attempts_counts_one_failure() {
        let not_leader = |index, request: &ProduceRequest| {
            Ok(answer_rows(request, index, |_, _, _| {
                codes::NOT_LEADER_OR_FOLLOWER
            }))
        };
        let no_response = |_, _: &ProduceRequest| Err("connection refused".to_owned());
        let cases: [(FakeBroker, &str); 2] = [
            (
                FakeBroker::answering(not_leader),
                "dlq.1-0 is not led by node 1",
            ),
            (FakeBroker::answering(no_response), "connection refused"),
        ];

        for (broker, reason) in cases {
            let sender = Coalescer::new(broker.clone());
            let metrics = BrokerMetrics::new();
            let (target, batch) = round_of(ProduceRoundSetup::default());

            let written =
                produce_round(&sender, &metrics, "g1", &target, || NodeId(1), &batch).await;

            assert!(
                (written, broker.sent().len(), meters(&metrics, "g1"))
                    == (
                        Err(DlqError::Write(format!(
                            "Exhausted max retries to produce to the DLQ topic dlq.1-0: {reason}."
                        ))),
                        5,
                        (0, 5, 1),
                    ),
                "{reason}"
            );
        }
    }

    /// The produce is counted when a request takes the round, as Kafka's
    /// `coalesceProduceRequests` marks `recordDLQProduce`, and not when the
    /// response arrives: a request that is still in flight counts, and the
    /// records do not, until they are written.
    #[tokio::test(start_paused = true)]
    async fn a_produce_counts_while_its_request_is_in_flight() {
        let broker = FakeBroker::held_answering(|index, request| {
            Ok(answer_rows(request, index, |_, _, _| codes::NONE))
        });
        let sender = Coalescer::new(broker.clone());
        let metrics = BrokerMetrics::new();
        let (target, batch) = round_of(ProduceRoundSetup {
            records: RoundRecordCount(2),
            ..Default::default()
        });
        let write = produce_round(&sender, &metrics, "g1", &target, || NodeId(1), &batch);
        tokio::pin!(write);

        tokio::select! {
            _ = &mut write => unreachable!("the request is held"),
            () = async {
                for _ in 0..20 {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
        let in_flight = (broker.sent().len(), meters(&metrics, "g1"));
        broker.release();
        let written = write.await;

        assert!(
            (in_flight, written, meters(&metrics, "g1")) == ((1, (0, 1, 0)), Ok(()), (2, 1, 0))
        );
    }

    /// A connection that records whether it is open, and which dial made it.
    #[derive(Clone)]
    struct FakeConnection {
        dial: usize,
        open: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Pooled for FakeConnection {
        fn is_open(&self) -> bool {
            self.open.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A dialler that counts its dials, and lets other tasks run mid-dial.
    #[derive(Default)]
    struct Dialler {
        dials: std::sync::atomic::AtomicUsize,
        fail_first: bool,
    }

    impl Dialler {
        async fn dial(&self) -> Result<FakeConnection, String> {
            let dial = self.dials.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            if self.fail_first && dial == 0 {
                return Err("connection refused".to_owned());
            }
            Ok(FakeConnection {
                dial,
                open: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            })
        }

        fn dials(&self) -> usize {
            self.dials.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// The produce requests to one leader share a connection, another leader
    /// has its own, and a connection that has closed is dialled again.
    #[tokio::test]
    async fn writes_to_one_leader_share_a_connection_until_it_closes() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler::default();

        let first = pool.get(NodeId(1), || dialler.dial()).await.unwrap();
        let again = pool.get(NodeId(1), || dialler.dial()).await.unwrap();
        let other = pool.get(NodeId(2), || dialler.dial()).await.unwrap();
        first.open.store(false, std::sync::atomic::Ordering::SeqCst);
        let redialled = pool.get(NodeId(1), || dialler.dial()).await.unwrap();

        assert!(
            (
                first.dial,
                again.dial,
                other.dial,
                redialled.dial,
                dialler.dials()
            ) == (0, 0, 1, 2, 3)
        );
    }

    /// Writers that need a leader at the same moment wait for one dial rather
    /// than each opening a connection, which is the storm a consumer that
    /// rejects at a high rate would otherwise cause.
    #[tokio::test]
    async fn concurrent_writes_to_one_leader_dial_once() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler::default();

        let connections =
            futures_util::future::join_all((0..16).map(|_| pool.get(NodeId(1), || dialler.dial())))
                .await;

        assert!(
            (
                connections
                    .iter()
                    .map(|connection| connection.as_ref().map(|c| c.dial))
                    .collect::<Vec<_>>(),
                dialler.dials(),
            ) == (vec![Ok(0); 16], 1)
        );
    }

    /// A dial that fails is not remembered: the next write dials again.
    #[tokio::test]
    async fn a_failed_dial_is_tried_again_by_the_next_write() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler {
            fail_first: true,
            ..Dialler::default()
        };

        let failed = pool.get(NodeId(1), || dialler.dial()).await.map(|c| c.dial);
        let retried = pool.get(NodeId(1), || dialler.dial()).await.map(|c| c.dial);

        assert!((failed, retried) == (Err("connection refused".to_owned()), Ok(1)));
    }

    fn created(error_code: i16) -> CreateTopicsResponse {
        CreateTopicsResponse {
            topics: vec![CreatableTopicResult {
                name: "dlq.g".into(),
                error_code,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Kafka's `handleCreateTopicsResponse`: a topic that was created is done,
    /// an existing one or a throttle is tried again, and any other refusal is
    /// final.
    #[test]
    fn a_topic_creation_answer_is_done_retried_or_final() {
        let cases = [
            (Ok(created(codes::NONE)), "done"),
            (Ok(created(codes::TOPIC_ALREADY_EXISTS)), "retry"),
            (Ok(created(codes::THROTTLING_QUOTA_EXCEEDED)), "retry"),
            (Ok(created(codes::TOPIC_AUTHORIZATION_FAILED)), "fatal"),
            (Ok(created(codes::INVALID_CONFIG)), "fatal"),
            (Ok(CreateTopicsResponse::default()), "fatal"),
            (Err(TopicCreatorError::Timeout), "retry"),
            (
                Err(TopicCreatorError::Envelope(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                )),
                "fatal",
            ),
        ];
        let expected: Vec<&str> = cases.iter().map(|(_, want)| *want).collect();

        let actual: Vec<&str> = cases
            .into_iter()
            .map(|(result, _)| outcome(&classify_create(result, "dlq.g")))
            .collect();

        assert!(actual == expected);
    }

    /// Runs `ensure_topic` for `dlq.g` with a scripted image and a scripted
    /// controller. `states` is what the image shows at each attempt, and its
    /// last entry holds from then on; each `CreateTopics` gets `answer`.
    /// Returns the outcome and how many requests went out.
    async fn ensure(
        states: &[Result<TopicState, DlqError>],
        answer: i16,
    ) -> (Result<(), DlqError>, usize) {
        let looks = std::cell::Cell::new(0_usize);
        let requests = std::cell::Cell::new(0_usize);
        let outcome = ensure_topic(
            "dlq.g",
            || {
                let look = looks.get();
                looks.set(look + 1);
                states[look.min(states.len() - 1)].clone()
            },
            || {
                requests.set(requests.get() + 1);
                std::future::ready(Ok(created(answer)))
            },
        )
        .await;
        (outcome, requests.get())
    }

    /// Kafka's retry of a topic creation goes back through `enqueue` and
    /// `dlqTopicExists`: a request that gets `TOPIC_ALREADY_EXISTS` because a
    /// concurrent write made the topic ends when the image shows the topic,
    /// rather than asking until the retries run out. The request repeats only
    /// while the topic is missing, and a topic that turns up without
    /// `errors.deadletterqueue.group.enable` is refused, as `validateDlqTopic`
    /// refuses it.
    #[tokio::test(start_paused = true)]
    async fn a_topic_that_a_concurrent_write_created_ends_the_creation() {
        let not_enabled = DlqError::Config("DLQ is not enabled on configured DLQ topic".to_owned());
        let cases = [
            // The image shows the topic already: nothing is asked.
            (vec![Ok(TopicState::Exists)], codes::NONE, (Ok(()), 0)),
            (vec![Ok(TopicState::Missing)], codes::NONE, (Ok(()), 1)),
            (
                vec![Ok(TopicState::Missing), Ok(TopicState::Exists)],
                codes::TOPIC_ALREADY_EXISTS,
                (Ok(()), 1),
            ),
            (
                vec![
                    Ok(TopicState::Missing),
                    Ok(TopicState::Missing),
                    Ok(TopicState::Exists),
                ],
                codes::THROTTLING_QUOTA_EXCEEDED,
                (Ok(()), 2),
            ),
            (
                vec![Ok(TopicState::Missing)],
                codes::TOPIC_ALREADY_EXISTS,
                (
                    Err(DlqError::Write(format!(
                        "Exhausted max retries to create the DLQ topic dlq.g: \
                         creating dlq.g: error {}.",
                        codes::TOPIC_ALREADY_EXISTS
                    ))),
                    5,
                ),
            ),
            (
                vec![Ok(TopicState::Missing), Err(not_enabled.clone())],
                codes::TOPIC_ALREADY_EXISTS,
                (Err(not_enabled), 1),
            ),
            (
                vec![Ok(TopicState::Missing)],
                codes::TOPIC_AUTHORIZATION_FAILED,
                (
                    Err(DlqError::Write(format!(
                        "Unable to create the DLQ topic dlq.g: error {}.",
                        codes::TOPIC_AUTHORIZATION_FAILED
                    ))),
                    1,
                ),
            ),
        ];
        let expected: Vec<_> = cases.iter().map(|(_, _, want)| want.clone()).collect();

        let mut actual = Vec::new();
        for (states, answer, _) in &cases {
            actual.push(ensure(states, *answer).await);
        }

        assert!(actual == expected);
    }
}
