//! The broker's auto topic creation: Kafka 4.3.1's
//! `DefaultAutoTopicCreationManager`.
//!
//! Three paths ask for a topic that does not exist:
//!
//! - A `Metadata` request that allows auto-creation
//!   ([`AutoTopicCreation::create_topics`] with the identity of the client).
//! - The first request that needs a coordinator topic: `FindCoordinator`, the
//!   share persister and the barrier coordinator
//!   ([`AutoTopicCreation::request`], with no identity). No broker creates
//!   `__consumer_offsets`, `__transaction_state`, `__share_group_state` or
//!   `__barrier_state` when it starts.
//! - A streams group heartbeat whose topology needs internal topics
//!   ([`AutoTopicCreation::create_streams_internal_topics`]).
//!
//! Each path sends one `CreateTopics` request to the active controller
//! through [`TopicCreator`], as Kafka's `KRaftTopicCreator` does, and does not
//! wait for the answer. The controller runs the checks of a client's
//! `CreateTopics`: the ACLs, the controller-mutation quota, the replica
//! placement, the topic policy and the config validation. A request with an
//! identity goes in a KIP-590 Envelope, so the controller authorizes the
//! client. A request with no identity goes as this broker's own controller
//! connection, so the controller authorizes the broker.
//!
//! A coordinator topic gets its configured partition count, replication
//! factor and topic configs. The replication factor is never lowered to fit
//! the cluster: with fewer live brokers than the factor, the controller
//! refuses the creation with `INVALID_REPLICATION_FACTOR`, and a later
//! request tries again. The caller of a coordinator lookup gets
//! `COORDINATOR_NOT_AVAILABLE` and retries, as Kafka's
//! `KafkaApis.getCoordinator` answers it.
//!
//! One set of in-flight names serves every path (Kafka's `inflightTopics`).
//! A name that a creation holds is skipped until the answer arrives. A failed
//! creation of a streams internal topic goes into an error cache. The cache
//! holds the name back from a new creation until the entry expires, and the
//! next heartbeats report the error.

use {
    crate::{
        barrier::{STATE_TOPIC, bootstrap::topic_configs as barrier_topic_configs},
        broker::Broker,
        codes,
        config::BrokerConfig,
        config_keys::{
            ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, MESSAGE_TIMESTAMP_TYPE,
            MESSAGE_TIMESTAMP_TYPE_CREATE,
        },
        coordinator::bootstrap::{OFFSETS_TOPIC, offsets_topic_configs},
        internal_topics::is_internal_topic,
        share_coordinator::bootstrap::{
            TOPIC as SHARE_TOPIC, topic_configs as share_topic_configs,
        },
        time_util::now_ms,
        topic_creator::{ForwardedIdentity, TopicCreator, TopicCreatorError},
        txn::bootstrap::{TOPIC as TRANSACTION_TOPIC, topic_configs as transaction_topic_configs},
    },
    dashmap::DashMap,
    krabka_log::topic_name::validate_topic_name,
    krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
            create_topics_response::CreateTopicsResponse,
            metadata_response::MetadataResponseTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BinaryHeap, HashMap},
        sync::{
            Arc, Mutex, OnceLock, PoisonError, Weak,
            atomic::{AtomicUsize, Ordering},
        },
    },
};

/// The `timeout_ms` of each `CreateTopics` request. Kafka's
/// `makeCreateTopicsRequestBuilder` sets `config.requestTimeoutMs`, the
/// broker's `request.timeout.ms`. krabka has no such setting, so it uses the
/// Kafka default of 30000.
const REQUEST_TIMEOUT_MS: i32 = 30_000;

/// The capacity of the error cache: the default `topicErrorCacheCapacity` of
/// `DefaultAutoTopicCreationManager`.
const ERROR_CACHE_CAPACITY: usize = 1_000;

/// Kafka's `Errors` entries for the codes that a `CreateTopics` answer can
/// carry: the code, `Errors.name()` and `Errors.message()`, from `Errors.java`
/// of Kafka 4.3.1. The first entry is the fallback of `Errors.forCode` for a
/// code that is not in the table.
const CREATE_TOPICS_ERRORS: [(i16, &str, &str); 15] = [
    (
        codes::UNKNOWN_SERVER_ERROR,
        "UNKNOWN_SERVER_ERROR",
        "The server experienced an unexpected error when processing the request.",
    ),
    (
        codes::REQUEST_TIMED_OUT,
        "REQUEST_TIMED_OUT",
        "The request timed out.",
    ),
    (
        codes::INVALID_TOPIC_EXCEPTION,
        "INVALID_TOPIC_EXCEPTION",
        "The request attempted to perform an operation on an invalid topic.",
    ),
    (
        codes::TOPIC_AUTHORIZATION_FAILED,
        "TOPIC_AUTHORIZATION_FAILED",
        "Topic authorization failed.",
    ),
    (
        codes::CLUSTER_AUTHORIZATION_FAILED,
        "CLUSTER_AUTHORIZATION_FAILED",
        "Cluster authorization failed.",
    ),
    (
        codes::UNSUPPORTED_VERSION,
        "UNSUPPORTED_VERSION",
        "The version of API is not supported.",
    ),
    (
        codes::TOPIC_ALREADY_EXISTS,
        "TOPIC_ALREADY_EXISTS",
        "Topic with this name already exists.",
    ),
    (
        codes::INVALID_PARTITIONS,
        "INVALID_PARTITIONS",
        "Number of partitions is below 1.",
    ),
    (
        codes::INVALID_REPLICATION_FACTOR,
        "INVALID_REPLICATION_FACTOR",
        "Replication factor is below 1 or larger than the number of available brokers.",
    ),
    (
        codes::INVALID_REPLICA_ASSIGNMENT,
        "INVALID_REPLICA_ASSIGNMENT",
        "Replica assignment is invalid.",
    ),
    (
        codes::INVALID_CONFIG,
        "INVALID_CONFIG",
        "Configuration is invalid.",
    ),
    (
        codes::NOT_CONTROLLER,
        "NOT_CONTROLLER",
        "This is not the correct controller for this cluster.",
    ),
    (
        codes::INVALID_REQUEST,
        "INVALID_REQUEST",
        "This most likely occurs because of a request being malformed by the client library or \
         the message was sent to an incompatible broker. See the broker logs for more details.",
    ),
    (
        codes::POLICY_VIOLATION,
        "POLICY_VIOLATION",
        "Request parameters do not satisfy the configured policy.",
    ),
    (
        codes::THROTTLING_QUOTA_EXCEEDED,
        "THROTTLING_QUOTA_EXCEEDED",
        "The throttling quota has been exceeded.",
    ),
];

/// Kafka's `Errors.forCode(code)`: the name and the default message of
/// `code`, or those of `UNKNOWN_SERVER_ERROR` for a code that is not in
/// [`CREATE_TOPICS_ERRORS`].
fn kafka_error(code: i16) -> (&'static str, &'static str) {
    let &(_, name, message) = CREATE_TOPICS_ERRORS
        .iter()
        .find(|(known, ..)| *known == code)
        .unwrap_or(&CREATE_TOPICS_ERRORS[0]);
    (name, message)
}

/// The broker's auto topic creation.
///
/// The coordinators that need a topic start before the [`Broker`] exists, so
/// the broker binds itself here once it is built. A [`Self::request`] made
/// before then creates nothing, and its caller retries as it does for any
/// other `COORDINATOR_NOT_AVAILABLE`.
#[derive(Debug, Default)]
pub struct AutoTopicCreation {
    /// A weak pointer, so the broker that owns this component is not kept
    /// alive by it.
    broker: OnceLock<Weak<Broker>>,
    /// The names whose creation is in flight (Kafka's `inflightTopics`).
    in_flight: DashMap<String, ()>,
    /// The failed creations of streams internal topics (Kafka's
    /// `topicCreationErrorCache`).
    errors: ExpiringErrorCache,
    /// The number of `CreateTopics` requests this component has sent.
    started: AtomicUsize,
}

impl AutoTopicCreation {
    /// Binds the broker that [`Self::request`] creates topics on.
    ///
    /// # Errors
    ///
    /// Returns an error when a broker is bound already.
    pub fn bind(&self, broker: &Arc<Broker>) -> Result<(), &'static str> {
        self.broker
            .set(Arc::downgrade(broker))
            .map_err(|_| "auto topic creation already bound")
    }

    /// Marks `name` in flight. Returns `false` when another creation of the
    /// name is in flight already.
    fn begin(&self, name: &str) -> bool { self.in_flight.insert(name.to_owned(), ()).is_none() }

    /// Kafka's `clearInflightRequests`: clears the in-flight mark of each of
    /// `names`.
    fn end(&self, names: &[String]) {
        for name in names {
            self.in_flight.remove(name);
        }
    }

    /// Test-only: marks `name` in flight, as a creation that has not answered
    /// yet holds it.
    #[cfg(test)]
    pub(crate) fn hold_for_test(&self, name: &str) -> bool { self.begin(name) }

    /// Test-only: clears the in-flight mark of `name`.
    #[cfg(test)]
    pub(crate) fn release_for_test(&self, name: &str) { self.end(&[name.to_owned()]); }

    /// Test-only: whether a creation of `name` is in flight.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_in_flight(&self, name: &str) -> bool { self.in_flight.contains_key(name) }

    /// Test-only: the number of `CreateTopics` requests this component has
    /// sent.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn started(&self) -> usize { self.started.load(Ordering::Relaxed) }

    /// Asks for the topic `name` with no request context, as
    /// `KafkaApis.getCoordinator` calls `createTopics(Set(name), quota,
    /// None)`.
    ///
    /// The call returns at once. It does nothing when no broker is bound yet.
    /// A failed creation is logged, and the next request tries again.
    pub fn request(self: &Arc<Self>, name: &str) {
        let Some(broker) = self.broker.get().and_then(Weak::upgrade) else {
            tracing::debug!(topic = name, "auto topic creation is not bound yet");
            return;
        };
        drop(self.create_topics(&broker, &[name], None));
    }

    /// KIP-1191: creates the share-group dead-letter topic `name` as this
    /// broker, and waits for the controller's answer.
    ///
    /// It sends what Kafka's `ShareGroupDLQStateManager.createTopicBuilder`
    /// sends: the cluster's default partition count and replication factor,
    /// `errors.deadletterqueue.group.enable=true`, which marks the topic as
    /// one a group may write to, and `message.timestamp.type=CreateTime`. The
    /// dead-letter records carry the time of the write, and a cluster-wide
    /// `LogAppendTime` default would overwrite it.
    ///
    /// # Errors
    ///
    /// [`TopicCreatorError`] when no broker is bound yet, or the controller
    /// gave no answer.
    pub(crate) async fn create_dead_letter_topic(
        &self,
        name: &str,
    ) -> Result<CreateTopicsResponse, TopicCreatorError> {
        let broker = self.broker.get().and_then(Weak::upgrade).ok_or_else(|| {
            TopicCreatorError::Protocol("auto topic creation is not bound".into())
        })?;
        TopicCreator::new(&broker)
            .create_topic_without_principal(create_topics_request(vec![dead_letter_topic(name)]))
            .await
    }

    /// Kafka's `createTopics`: sends one `CreateTopics` request for the names
    /// that can be created, and answers a `Metadata` row for each of `names`.
    ///
    /// `filterCreatableTopics` answers a name that `Topic.validate` refuses
    /// with `INVALID_TOPIC_EXCEPTION`, and a name whose creation is in flight
    /// with `UNKNOWN_TOPIC_OR_PARTITION`. `sendCreateTopicRequest` sends the
    /// other names without waiting for the answer, and answers each of them
    /// with `UNKNOWN_TOPIC_OR_PARTITION`. The uncreatable rows come first.
    ///
    /// With `identity`, the request goes in an Envelope that names the
    /// client (`createTopicWithPrincipal`). Without it, the request goes as
    /// this broker (`createTopicWithoutPrincipal`).
    pub(crate) fn create_topics(
        self: &Arc<Self>,
        broker: &Broker,
        names: &[&str],
        identity: Option<ForwardedIdentity>,
    ) -> Vec<MetadataResponseTopic> {
        let row = |error_code, name: &str| MetadataResponseTopic {
            error_code,
            name: Some(name.to_owned()),
            topic_id: WireUuid::ZERO,
            is_internal: is_internal_topic(&broker.config, name),
            ..Default::default()
        };
        let mut uncreatable = Vec::new();
        let mut creatable = Vec::new();
        for name in names {
            if validate_topic_name(name).is_err() {
                uncreatable.push(row(codes::INVALID_TOPIC_EXCEPTION, name));
            } else if self.begin(name) {
                creatable.push(creatable_topic(&broker.config, name));
            } else {
                uncreatable.push(row(codes::UNKNOWN_TOPIC_OR_PARTITION, name));
            }
        }
        if creatable.is_empty() {
            return uncreatable;
        }
        let rows = creatable
            .iter()
            .map(|topic| row(codes::UNKNOWN_TOPIC_OR_PARTITION, &topic.name));
        uncreatable.extend(rows);
        self.send_create_topic_request(broker, creatable, identity, None);
        uncreatable
    }

    /// Kafka's `sendCreateTopicRequest`: sends `topics` in the background.
    /// When the answer arrives, the in-flight marks clear and each failure is
    /// logged. The send stops when the broker shuts down, as Kafka's channel
    /// manager stops with the broker. A streams request caches its failures
    /// for `cache_ttl_ms` instead of logging the ordinary completion.
    fn send_create_topic_request(
        self: &Arc<Self>,
        broker: &Broker,
        topics: Vec<CreatableTopic>,
        identity: Option<ForwardedIdentity>,
        cache_ttl_ms: Option<i64>,
    ) {
        let names: Vec<String> = topics.iter().map(|topic| topic.name.clone()).collect();
        let request = create_topics_request(topics);
        let creator = TopicCreator::new(broker);
        let shutdown = broker.supervisor_shutdown.clone();
        let this = Arc::clone(self);
        self.started.fetch_add(1, Ordering::Relaxed);
        if cache_ttl_ms.is_none() {
            tracing::info!(
                topics = ?names,
                "Sent auto-creation request for {names:?} to the active controller."
            );
        }
        tokio::spawn(async move {
            let send = async {
                match identity {
                    Some(identity) => {
                        creator
                            .create_topic_with_principal(&identity, request)
                            .await
                    }
                    None => creator.create_topic_without_principal(request).await,
                }
            };
            let result = shutdown.run_until_cancelled(send).await;
            if let (Some(ttl_ms), Some(result)) = (cache_ttl_ms, &result) {
                this.finish_streams_creation(&names, result, ttl_ms, now_ms());
            } else {
                this.end(&names);
                match result {
                    Some(Ok(response)) => log_failed_rows(&response),
                    Some(Err(error)) => log_error(&names, &error),
                    None => {}
                }
            }
        });
    }

    /// Kafka's `createStreamsInternalTopics`: sends `topics` with the
    /// identity of the heartbeat, in the background.
    ///
    /// A topic whose last failure has not expired, or whose creation is in
    /// flight, is skipped. When the answer arrives, the in-flight marks clear
    /// and each failure goes into the error cache for `ttl_ms`.
    pub(crate) fn create_streams_internal_topics(
        self: &Arc<Self>,
        broker: &Broker,
        topics: Vec<CreatableTopic>,
        identity: ForwardedIdentity,
        ttl_ms: i64,
    ) {
        let now_ms = now_ms();
        let topics: Vec<CreatableTopic> = topics
            .into_iter()
            .filter(|topic| !self.errors.has_error(&topic.name, now_ms) && self.begin(&topic.name))
            .collect();
        if topics.is_empty() {
            return;
        }
        self.send_create_topic_request(broker, topics, Some(identity), Some(ttl_ms));
    }

    /// The completion of `sendCreateTopicRequestWithErrorCaching`: clears the
    /// in-flight marks of `names` and caches the failures of `result`.
    ///
    /// A request that got no answer caches its error message for every one of
    /// `names` (`cacheTopicCreationErrors`). An answer caches each row whose
    /// code is not `NONE`, `TOPIC_ALREADY_EXISTS` included
    /// (`cacheTopicCreationErrorsFromResponse`). A row with a null or empty
    /// message caches the default message of its code.
    fn finish_streams_creation(
        &self,
        names: &[String],
        result: &Result<CreateTopicsResponse, TopicCreatorError>,
        ttl_ms: i64,
        now_ms: i64,
    ) {
        self.end(names);
        match result {
            Ok(response) => {
                tracing::debug!(
                    topics = ?names,
                    "Auto topic creation completed for {names:?} with response {response:?}."
                );
                for topic in response
                    .topics
                    .iter()
                    .filter(|topic| topic.error_code != codes::NONE)
                {
                    let message = topic
                        .error_message
                        .clone()
                        .filter(|message| !message.is_empty())
                        .unwrap_or_else(|| kafka_error(topic.error_code).1.to_owned());
                    tracing::debug!(
                        topic = %topic.name,
                        "Cached topic creation error for {}: {message}",
                        topic.name
                    );
                    self.errors.put(&topic.name, message, ttl_ms, now_ms);
                }
            }
            Err(error) => {
                log_error(names, error);
                let message = error.to_string();
                for name in names {
                    self.errors.put(name, message.clone(), ttl_ms, now_ms);
                }
            }
        }
    }

    /// Kafka's `getStreamsInternalTopicCreationErrors`: the unexpired cached
    /// error of each of `names` that has one, in name order.
    pub(crate) fn streams_internal_topic_creation_errors<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
        now_ms: i64,
    ) -> BTreeMap<String, String> {
        self.errors.errors_for_topics(names, now_ms)
    }
}

/// KIP-1191: the `CreateTopics` row for the dead-letter topic `name`, as
/// Kafka's `ShareGroupDLQStateManager.createTopicBuilder` builds it. The
/// cluster's defaults give the partition count and the replication factor
/// (-1), and two configs are set: `errors.deadletterqueue.group.enable=true`
/// opts the topic in, and `message.timestamp.type=CreateTime` keeps the
/// timestamp of each dead-letter record against a `LogAppendTime` default.
fn dead_letter_topic(name: &str) -> CreatableTopic {
    let config = |name: &str, value: &str| CreatableTopicConfig {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        ..Default::default()
    };
    CreatableTopic {
        name: name.to_owned(),
        num_partitions: -1,
        replication_factor: -1,
        configs: vec![
            config(ERRORS_DEADLETTERQUEUE_GROUP_ENABLE, "true"),
            config(MESSAGE_TIMESTAMP_TYPE, MESSAGE_TIMESTAMP_TYPE_CREATE),
        ],
        ..Default::default()
    }
}

/// Kafka's `makeCreateTopicsRequestBuilder`.
fn create_topics_request(topics: Vec<CreatableTopic>) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics,
        timeout_ms: REQUEST_TIMEOUT_MS,
        ..Default::default()
    }
}

/// The `whenComplete` log of `sendCreateTopicRequest` for an answer: one
/// warning for each row whose code is not `NONE`.
fn log_failed_rows(response: &CreateTopicsResponse) {
    for topic in response
        .topics
        .iter()
        .filter(|topic| topic.error_code != codes::NONE)
    {
        let (error_name, _) = kafka_error(topic.error_code);
        tracing::warn!(
            topic = %topic.name,
            "Auto topic creation failed for {} with error '{error_name}': {}",
            topic.name,
            topic.error_message.as_deref().unwrap_or("null")
        );
    }
}

/// Kafka's `logError`: a timeout logs at debug level, and any other error
/// logs a warning.
fn log_error(names: &[String], error: &TopicCreatorError) {
    match error {
        TopicCreatorError::Timeout => {
            tracing::debug!(topics = ?names, "Auto topic creation timed out for {names:?}.");
        }
        error => {
            tracing::warn!(
                topics = ?names,
                "Auto topic creation failed for {names:?} with exception: {error}"
            );
        }
    }
}

/// Kafka's `DefaultAutoTopicCreationManager.creatableTopic`: the
/// `CreateTopics` row that auto-creation sends for `name`.
///
/// A coordinator topic gets its configured partition count, replication
/// factor and topic configs. `__barrier_state` has no Kafka counterpart and
/// follows the same rule. Any other name gets `num.partitions` and
/// `default.replication.factor` only when the operator supplied them, and
/// else -1, which asks the controller for its own defaults.
#[must_use]
pub fn creatable_topic(config: &BrokerConfig, name: &str) -> CreatableTopic {
    let (num_partitions, replication_factor, configs) = match name {
        OFFSETS_TOPIC => (
            config.offsets_topic_num_partitions,
            config.offsets_topic_replication_factor,
            offsets_topic_configs(config),
        ),
        TRANSACTION_TOPIC => (
            config.transaction_state_num_partitions,
            config.transaction_state_replication_factor,
            transaction_topic_configs(
                config.transaction_state_segment_bytes,
                config.transaction_state_min_isr,
            ),
        ),
        SHARE_TOPIC => (
            config.share_coordinator.state_topic_num_partitions,
            config.share_coordinator.state_topic_replication_factor,
            share_topic_configs(&config.share_coordinator),
        ),
        STATE_TOPIC => (
            config.barrier_state_num_partitions,
            config.barrier_state_replication_factor,
            barrier_topic_configs(),
        ),
        _ => {
            let supplied = config.static_config_origins.topic_creation;
            return CreatableTopic {
                name: name.to_owned(),
                num_partitions: if supplied.num_partitions {
                    config.num_partitions
                } else {
                    -1
                },
                replication_factor: if supplied.default_replication_factor {
                    config.default_replication_factor
                } else {
                    -1
                },
                ..Default::default()
            };
        }
    };
    CreatableTopic {
        name: name.to_owned(),
        num_partitions,
        replication_factor,
        configs: configs
            .into_iter()
            .map(|(name, value)| CreatableTopicConfig {
                name,
                value: Some(value),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Kafka's `ExpiringErrorCache`: the error of each topic, with an expiry
/// time for each entry.
///
/// A min-heap on the expiry time drops the expired entries and keeps the
/// cache at its capacity: when the cache is full, the entry that expires
/// first goes, not the one used least recently. An entry that a newer
/// [`Self::put`] replaced stays in the heap, and its sequence number tells
/// that it is stale.
#[derive(Debug)]
struct ExpiringErrorCache {
    capacity: usize,
    inner: Mutex<ErrorCacheInner>,
}

#[derive(Debug, Default)]
struct ErrorCacheInner {
    /// Topic name -> its current entry.
    by_topic: HashMap<String, ErrorEntry>,
    /// (expiry time, sequence number, topic name), earliest expiry first.
    expiry_queue: BinaryHeap<Reverse<(i64, u64, String)>>,
    /// The sequence number of the next entry.
    next_sequence: u64,
}

#[derive(Debug)]
struct ErrorEntry {
    message: String,
    expires_at_ms: i64,
    sequence: u64,
}

impl Default for ExpiringErrorCache {
    fn default() -> Self { Self::new(ERROR_CACHE_CAPACITY) }
}

impl ExpiringErrorCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ErrorCacheInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Caches `message` for `topic` until `now_ms + ttl_ms`. Then it drops
    /// the expired entries, and the entries that expire first while the
    /// cache holds more than its capacity.
    fn put(&self, topic: &str, message: String, ttl_ms: i64, now_ms: i64) {
        let mut inner = self.lock();
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let sequence = inner.next_sequence;
        inner.next_sequence += 1;
        inner.by_topic.insert(topic.to_owned(), ErrorEntry {
            message,
            expires_at_ms,
            sequence,
        });
        inner
            .expiry_queue
            .push(Reverse((expires_at_ms, sequence, topic.to_owned())));
        while let Some(Reverse((earliest_ms, ..))) = inner.expiry_queue.peek() {
            if *earliest_ms > now_ms && inner.by_topic.len() <= self.capacity {
                break;
            }
            let Some(Reverse((_, sequence, topic))) = inner.expiry_queue.pop() else {
                break;
            };
            if inner
                .by_topic
                .get(&topic)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                inner.by_topic.remove(&topic);
            }
        }
    }

    /// Whether `topic` has an unexpired entry.
    fn has_error(&self, topic: &str, now_ms: i64) -> bool {
        self.lock()
            .by_topic
            .get(topic)
            .is_some_and(|entry| entry.expires_at_ms > now_ms)
    }

    /// The unexpired entry of each of `topics` that has one.
    fn errors_for_topics<'a>(
        &self,
        topics: impl IntoIterator<Item = &'a str>,
        now_ms: i64,
    ) -> BTreeMap<String, String> {
        let inner = self.lock();
        topics
            .into_iter()
            .filter_map(|topic| {
                inner
                    .by_topic
                    .get(topic)
                    .filter(|entry| entry.expires_at_ms > now_ms)
                    .map(|entry| (topic.to_owned(), entry.message.clone()))
            })
            .collect()
    }

    /// The number of entries, the expired ones included.
    #[cfg(test)]
    fn len(&self) -> usize { self.lock().by_topic.len() }
}

#[cfg(test)]
mod tests;
