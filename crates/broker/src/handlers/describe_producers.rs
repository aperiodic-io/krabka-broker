//! `DescribeProducers` (`api_key=61`, KIP-664) shows the producer state of a
//! set of partitions.
//!
//! This admin RPC returns the producer state of the partition log for a set
//! of `(topic, partition)` pairs. JVM `Admin.describeProducers` and
//! `kafka-transactions --describe-producers` use it to debug stuck idempotent
//! or transactional producers.
//!
//! ## ACL
//!
//! Kafka's `KafkaApis.checkValidTopic` validates the topic name with
//! `Topic.validate` before it authorizes anything: a malformed name (empty,
//! too long, or holding a character other than ASCII alphanumerics, `.`,
//! `_` and `-`) gives every requested partition `INVALID_TOPIC_EXCEPTION
//! (17)` with `Topic.validate`'s message, regardless of the principal's
//! ACLs. Only a name that passes that check goes on to the `Read` check on
//! `Topic(name)`, which mirrors `Fetch`, per KIP-664. On a Deny, every
//! partition of that topic carries `TOPIC_AUTHORIZATION_FAILED (29)`. An
//! unknown topic or an out-of-range partition gives a per-partition
//! `UNKNOWN_TOPIC_OR_PARTITION (3)`.
//!
//! ## Replicas
//!
//! Kafka's `ReplicaManager.activeProducerState` answers on every replica that
//! hosts the partition, the leader and each follower. A partition that the
//! metadata holds and this broker does not host gives
//! `NOT_LEADER_OR_FOLLOWER (6)`, and a partition in an offline log directory
//! gives `KAFKA_STORAGE_ERROR (56)`.
//!
//! ## Field semantics
//!
//! Every field comes from the producer state of the partition log,
//! [`krabka_log::Log::active_producers`], which is Kafka's
//! `UnifiedLog.activeProducers`. A leader updates that state for each batch
//! that it appends, and a follower for each batch that it replicates, so a
//! follower answers as its leader does at the same log end. The produce-path
//! tracker in `crate::producer_state` does not hold the data batches that a
//! follower replicates, so this handler does not read it.
//! `coordinator_epoch` is `-1` before the first transaction marker of the
//! producer, and `current_txn_start_offset` is `-1` when the producer has no
//! open transaction.

use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{RequestContext, allowed_topics},
    },
    krabka_log::topic_name::validate_topic_name,
    krabka_metadata::AclOperation,
    krabka_protocol::owned::{
        describe_producers_request::{DescribeProducersRequest, TopicRequest},
        describe_producers_response::{
            DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
        },
    },
};

pub(crate) fn handle(
    broker: &Broker,
    req: &DescribeProducersRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> Result<DescribeProducersResponse, BrokerError> {
    let image = broker.controller.current_image();

    // Kafka's `checkValidTopic` runs `Topic.validate` before it authorizes
    // anything, so a malformed name never reaches the authorizer. Only the
    // names that pass go into the batch `Read` check below.
    let allowed = allowed_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        AclOperation::Read,
        req.topics
            .iter()
            .filter(|t| validate_topic_name(t.name.as_str()).is_ok())
            .map(|t| t.name.as_str()),
    );

    let mut topics_out: Vec<TopicResponse> = Vec::with_capacity(req.topics.len());
    for topic_req in &req.topics {
        if let Err(invalid) = validate_topic_name(topic_req.name.as_str()) {
            // Kafka answers INVALID_TOPIC_EXCEPTION on every requested
            // partition of a malformed name, regardless of the principal's
            // ACLs, before it even looks the topic up.
            topics_out.push(refused_topic(
                topic_req,
                codes::INVALID_TOPIC_EXCEPTION,
                &invalid.to_string(),
            ));
            continue;
        }

        // KIP-664: per-partition TOPIC_AUTHORIZATION_FAILED on every
        // requested partition of a denied topic. Every valid name was
        // authorized above, so one missing from `allowed` was denied.
        if !allowed.contains(topic_req.name.as_str()) {
            topics_out.push(refused_topic(
                topic_req,
                codes::TOPIC_AUTHORIZATION_FAILED,
                "Topic authorization failed.",
            ));
            continue;
        }

        let mut parts_out: Vec<PartitionResponse> =
            Vec::with_capacity(topic_req.partition_indexes.len());

        // Topic-existence + per-partition-bounds check. The image
        // exposes `partition(name, idx) -> Option<&PartitionRecord>`
        // which combines both checks in one lookup.
        for &idx in &topic_req.partition_indexes {
            if image.partition(topic_req.name.as_str(), idx).is_none() {
                // Kafka's `handleDescribeProducersRequest` gives the message
                // only for an unknown topic, which it answers itself. A bad
                // partition index of a known topic reaches
                // `ReplicaManager.activeProducerState`, whose answer is a
                // bare code.
                let error_message = image
                    .topic(topic_req.name.as_str())
                    .is_none()
                    .then(|| "This server does not host this topic-partition.".to_owned());
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message,
                    active_producers: Vec::new(),
                    ..Default::default()
                });
                continue;
            }

            parts_out.push(hosted_partition_producers(
                broker,
                topic_req.name.as_str(),
                idx,
            )?);
        }

        topics_out.push(TopicResponse {
            name: topic_req.name.clone(),
            partitions: parts_out,
            ..Default::default()
        });
    }

    Ok(DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        ..Default::default()
    })
}

/// The row of a topic refused before any lookup: every requested partition
/// carries `error_code` and `error_message`, and no producers.
fn refused_topic(topic: &TopicRequest, error_code: i16, error_message: &str) -> TopicResponse {
    TopicResponse {
        name: topic.name.clone(),
        partitions: topic
            .partition_indexes
            .iter()
            .map(|&partition_index| PartitionResponse {
                partition_index,
                error_code,
                error_message: Some(error_message.to_owned()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The row of a partition that the metadata holds: Kafka's
/// `ReplicaManager.activeProducerState`.
///
/// Kafka answers from the log of any replica that the broker hosts, leader or
/// follower, through `Partition.activeProducerState`. A partition that the
/// broker does not host is `NOT_LEADER_OR_FOLLOWER`, and a partition in an
/// offline log directory is `KAFKA_STORAGE_ERROR`. Both rows carry only the
/// code.
///
/// # Errors
///
/// Returns [`BrokerError::Replication`] when a panic poisoned the log lock of
/// the partition.
fn hosted_partition_producers(
    broker: &Broker,
    topic: &str,
    partition_index: i32,
) -> Result<PartitionResponse, BrokerError> {
    let refused = |error_code| PartitionResponse {
        partition_index,
        error_code,
        ..Default::default()
    };
    let Some(partition) = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(partition_index))
    else {
        return Ok(refused(codes::NOT_LEADER_OR_FOLLOWER));
    };
    if broker.log_dir_status.is_offline(&partition.log_dir.load()) {
        return Ok(refused(codes::KAFKA_STORAGE_ERROR));
    }
    let log = partition.log.lock().map_err(|_| {
        BrokerError::Replication(format!(
            "DescribeProducers: log lock poisoned for {topic}-{partition_index}"
        ))
    })?;
    Ok(PartitionResponse {
        partition_index,
        error_code: codes::NONE,
        active_producers: log
            .active_producers()
            .into_iter()
            .map(producer_state)
            .collect(),
        ..Default::default()
    })
}

/// The wire row of one producer: Kafka's `UnifiedLog.activeProducers`, which
/// widens the epoch to an `int32` and puts `-1` in
/// `current_txn_start_offset` when no transaction is open.
fn producer_state(producer: krabka_log::ActiveProducer) -> ProducerState {
    ProducerState {
        producer_id: producer.producer_id.get(),
        producer_epoch: i32::from(producer.producer_epoch),
        last_sequence: producer.last_sequence,
        last_timestamp: producer.last_timestamp,
        coordinator_epoch: producer.coordinator_epoch,
        current_txn_start_offset: producer
            .current_txn_start_offset
            .map_or(-1, |offset| offset.0),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::{AllowAllAuthorizer, Authorizer},
            broker::Broker,
            handlers::RequestContext,
            partition::Partition,
            test_support::{DenyAll, KafkaErrorCode, peer, principal},
            txn::marker::{MarkerType, build_marker_batch},
        },
        assert2::assert,
        bytes::Bytes,
        krabka_metadata::NodeId,
        krabka_protocol::owned::describe_producers_request::TopicRequest,
        std::sync::Arc,
    };

    const VERSION: i16 = 0;

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn request(topic: &str, partition_indexes: &[i32]) -> DescribeProducersRequest {
        DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: topic.into(),
                partition_indexes: partition_indexes.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn drive(
        broker: &Broker,
        req: &DescribeProducersRequest,
        ctx: &RequestContext<'_>,
    ) -> DescribeProducersResponse {
        handle(broker, req, VERSION, ctx).expect("handle")
    }

    /// Kafka's `checkValidTopic` answers `INVALID_TOPIC_EXCEPTION` (17) for
    /// a malformed topic name on every requested partition, before it ever
    /// consults the authorizer, and with `Topic.validate`'s message. A
    /// well-formed but missing topic still falls through to the existence
    /// check, and gets `UNKNOWN_TOPIC_OR_PARTITION` (3) with Kafka's message.
    /// This holds under both a deny-everything and an allow-everything
    /// authorizer, since an invalid name never reaches either.
    #[tokio::test]
    async fn handle_validates_topic_name_before_authorizing() {
        /// A topic name, the authorizer, and the error code and message the
        /// partition row must carry.
        struct NameCase<'a> {
            name: &'a str,
            authorizer: Arc<dyn Authorizer>,
            expected_code: i16,
            expected_message: Option<String>,
        }

        let too_long = "a".repeat(krabka_log::topic_name::MAX_TOPIC_NAME_LENGTH + 1);
        let cases: Vec<NameCase<'_>> = vec![
            NameCase {
                name: "",
                authorizer: Arc::new(AllowAllAuthorizer),
                expected_code: codes::INVALID_TOPIC_EXCEPTION,
                expected_message: Some(krabka_log::topic_name::InvalidTopicName::Empty.to_string()),
            },
            NameCase {
                name: "",
                authorizer: Arc::new(DenyAll),
                expected_code: codes::INVALID_TOPIC_EXCEPTION,
                expected_message: Some(krabka_log::topic_name::InvalidTopicName::Empty.to_string()),
            },
            NameCase {
                name: "a/b",
                authorizer: Arc::new(AllowAllAuthorizer),
                expected_code: codes::INVALID_TOPIC_EXCEPTION,
                expected_message: Some(
                    krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into())
                        .to_string(),
                ),
            },
            NameCase {
                name: "a/b",
                authorizer: Arc::new(DenyAll),
                expected_code: codes::INVALID_TOPIC_EXCEPTION,
                expected_message: Some(
                    krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into())
                        .to_string(),
                ),
            },
            NameCase {
                name: too_long.as_str(),
                authorizer: Arc::new(AllowAllAuthorizer),
                expected_code: codes::INVALID_TOPIC_EXCEPTION,
                expected_message: Some(
                    krabka_log::topic_name::InvalidTopicName::TooLong(too_long.clone()).to_string(),
                ),
            },
            NameCase {
                name: "missing-but-valid",
                authorizer: Arc::new(AllowAllAuthorizer),
                expected_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                expected_message: Some("This server does not host this topic-partition.".into()),
            },
            NameCase {
                name: "missing-but-valid",
                authorizer: Arc::new(DenyAll),
                expected_code: codes::TOPIC_AUTHORIZATION_FAILED,
                expected_message: Some("Topic authorization failed.".into()),
            },
        ];

        for NameCase {
            name,
            authorizer,
            expected_code,
            expected_message,
        } in cases
        {
            broker_fixture!((broker_handle, _dir, broker), start_broker(authorizer));
            request_identity!((p, peer), principal("alice"));
            let req = request(name, &[0]);

            let resp = drive(&broker, &req, &test_context(&p, &peer));

            let expected = empty_producer_response()
                .topic(name)
                .partition(0)
                .error_code(expected_code)
                .maybe_message(expected_message.clone())
                .call();
            assert!(resp == expected, "topic {name:?}");
            broker_handle.shutdown().await;
        }
    }

    #[derive(krabka_macros::FieldDefaults)]
    struct EmptyProducerResponseSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        partition: krabka_ids::PartitionIndex,
        error_code: KafkaErrorCode,
        message: Option<String>,
    }

    /// Independent wire oracle for a partition that exposes no producer state.
    #[bon::builder]
    fn empty_producer_response(
        #[builder(default = "orders")] topic: &'_ str,
        #[builder(into, default)] partition: krabka_ids::PartitionIndex,
        #[builder(into, default)] error_code: KafkaErrorCode,
        message: Option<String>,
    ) -> DescribeProducersResponse {
        let setup = EmptyProducerResponseSetup {
            topic,
            partition,
            error_code,
            message,
        };
        DescribeProducersResponse {
            topics: vec![TopicResponse {
                name: setup.topic.to_owned(),
                partitions: vec![PartitionResponse {
                    partition_index: setup.partition.0,
                    error_code: setup.error_code.0,
                    error_message: setup.message,
                    active_producers: Vec::new(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Kafka's `handleDescribeProducersRequest` answers an unknown topic itself,
    /// with the error's message. A partition index outside a known topic
    /// reaches `ReplicaManager.activeProducerState`, which answers the same code
    /// with no message.
    #[tokio::test]
    async fn handle_gives_the_message_only_for_a_topic_the_image_lacks() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(AllowAllAuthorizer))
        );
        broker
            .controller
            .submit_change(vec![
                krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                    name: "known".into(),
                    topic_id: uuid::Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                }),
                krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                    topic: "known".into(),
                    partition: 0,
                    leader: broker.config.node_id,
                    replicas: vec![broker.config.node_id],
                    isr: vec![broker.config.node_id],
                    ..Default::default()
                }),
            ])
            .await
            .expect("seed the known topic");
        request_identity!((p, peer), principal("alice"));

        // (topic, expected error message)
        let cases = [
            ("known", None),
            (
                "missing",
                Some("This server does not host this topic-partition.".to_owned()),
            ),
        ];
        for (name, error_message) in cases {
            let resp = drive(&broker, &request(name, &[5]), &test_context(&p, &peer));
            let expected = empty_producer_response()
                .topic(name)
                .partition(5)
                .error_code(codes::UNKNOWN_TOPIC_OR_PARTITION)
                .maybe_message(error_message)
                .call();
            assert!(resp == expected, "topic {name:?}");
        }
        broker_handle.shutdown().await;
    }

    /// An invalid name gives `INVALID_TOPIC_EXCEPTION` on every requested
    /// partition, not only the first.
    #[tokio::test]
    async fn handle_invalid_topic_name_marks_every_requested_partition() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(AllowAllAuthorizer))
        );
        request_identity!((p, peer), principal("alice"));
        let req = request("a/b", &[0, 1, 4]);

        let resp = drive(&broker, &req, &test_context(&p, &peer));

        let message =
            krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into()).to_string();
        let expected = DescribeProducersResponse {
            throttle_time_ms: 0,
            topics: vec![TopicResponse {
                name: "a/b".into(),
                partitions: [0, 1, 4]
                    .into_iter()
                    .map(|partition_index| PartitionResponse {
                        partition_index,
                        error_code: codes::INVALID_TOPIC_EXCEPTION,
                        error_message: Some(message.clone()),
                        active_producers: Vec::new(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Put `topic` with one partition on `replicas`, led by `leader`, in the
    /// metadata image. Return the partition once this broker hosts it in its
    /// role, or `None` when this broker is not a replica.
    #[derive(krabka_macros::FieldDefaults)]
    struct HostedProducerPartitionSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        #[default(krabka_metadata::NodeId(1))]
        leader: krabka_metadata::NodeId,
        #[default(vec![krabka_metadata::NodeId(1)])]
        replicas: Vec<NodeId>,
    }

    #[bon::builder]
    async fn seed_partition(
        broker: &Broker,
        #[builder(default = "orders")] topic: &'_ str,
        #[builder(into, default = krabka_metadata :: NodeId (1))] leader: krabka_metadata::NodeId,
        #[builder(default = vec ! [krabka_metadata :: NodeId (1)])] replicas: Vec<NodeId>,
    ) -> Option<Arc<Partition>> {
        let setup = HostedProducerPartitionSetup {
            topic,
            leader,
            replicas,
        };
        let HostedProducerPartitionSetup {
            topic,
            leader,
            replicas,
        } = setup;
        let hosted = replicas.contains(&broker.config.node_id);
        broker
            .controller
            .submit_change(vec![
                krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                    name: topic.into(),
                    topic_id: uuid::Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: i16::try_from(replicas.len()).expect("replica count"),
                }),
                krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                    topic: topic.into(),
                    partition: 0,
                    leader,
                    isr: replicas.clone(),
                    replicas,
                    ..Default::default()
                }),
            ])
            .await
            .expect("seed the topic");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let in_role = broker
                    .partitions
                    .get(topic, krabka_ids::PartitionIndex(0))
                    .filter(|partition| {
                        partition
                            .current_leader
                            .load(std::sync::atomic::Ordering::Acquire)
                            == leader.0
                    });
                let image_holds = broker
                    .controller
                    .current_image()
                    .partition(topic, 0)
                    .is_some();
                match (hosted, in_role) {
                    (true, Some(partition)) => return Some(partition),
                    (false, _) if image_holds => return None,
                    _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("the broker applies the topic")
    }

    krabka_macros::producer_batch_fixture!(producer_batch, Bytes::from_static(b"v"));

    /// A data batch of `producer` (`(id, epoch)`) with `records` records from
    /// sequence 0, at `base_offset`, whose max timestamp is `max_timestamp`.
    /// `DescribeProducers` widens the persisted producer epoch to an i32.
    #[derive(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct ReportedProducerEpoch(i32);

    #[derive(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct ProducerCoordinatorEpoch(i32);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ExpectedProducerSetup {
        #[default(krabka_ids::ProducerId(7))]
        producer_id: krabka_ids::ProducerId,
        producer_epoch: ReportedProducerEpoch,
        last_sequence: BatchSequence,
        #[default(BatchTimestamp(1_000))]
        last_timestamp: BatchTimestamp,
        #[default(ProducerCoordinatorEpoch(-1))]
        coordinator_epoch: ProducerCoordinatorEpoch,
        #[default(krabka_log::Offset(-1))]
        current_txn_start_offset: krabka_log::Offset,
    }

    #[bon::builder]
    fn producer_row(
        #[builder(into, default = krabka_ids :: ProducerId (7))]
        producer_id: krabka_ids::ProducerId,
        #[builder(into, default)] producer_epoch: ReportedProducerEpoch,
        #[builder(into, default)] last_sequence: BatchSequence,
        #[builder(into, default = BatchTimestamp (1_000))] last_timestamp: BatchTimestamp,
        #[builder(into, default = ProducerCoordinatorEpoch (- 1))]
        coordinator_epoch: ProducerCoordinatorEpoch,
        #[builder(into, default = krabka_log :: Offset (- 1))]
        current_txn_start_offset: krabka_log::Offset,
    ) -> ProducerState {
        let setup = ExpectedProducerSetup {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        };
        let ExpectedProducerSetup {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        } = setup;
        ProducerState {
            producer_id: producer_id.0,
            producer_epoch: producer_epoch.0,
            last_sequence: last_sequence.0,
            last_timestamp: last_timestamp.0,
            coordinator_epoch: coordinator_epoch.0,
            current_txn_start_offset: current_txn_start_offset.0,
            ..Default::default()
        }
    }

    fn partition_row(error_code: i16, active_producers: Vec<ProducerState>) -> PartitionResponse {
        PartitionResponse {
            partition_index: 0,
            error_code,
            error_message: None,
            active_producers,
            ..Default::default()
        }
    }

    /// Kafka's `ReplicaManager.activeProducerState` answers from the log of
    /// every replica that the broker hosts, leader or follower, and
    /// `NOT_LEADER_OR_FOLLOWER` for a partition that the metadata holds and the
    /// broker does not host. The batches here reach the logs only through the
    /// leader's log and the follower's replication path. The produce-path
    /// tracker holds none of them, so a handler that read it would answer no
    /// producers. A partition in an offline log directory is
    /// `KAFKA_STORAGE_ERROR`.
    #[tokio::test]
    async fn handle_answers_from_the_log_of_every_hosted_replica() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(AllowAllAuthorizer))
        );
        let local = broker.config.node_id;
        let remote = krabka_metadata::NodeId(local.0 + 1);

        let leads = seed_partition()
            .broker(&broker)
            .topic("leads")
            .leader(local)
            .replicas(vec![local])
            .call()
            .await
            .expect("hosted leader");
        let follows = seed_partition()
            .broker(&broker)
            .topic("follows")
            .leader(remote)
            .replicas(vec![remote, local])
            .call()
            .await
            .expect("hosted follower");
        assert!(
            seed_partition()
                .broker(&broker)
                .topic("moved")
                .leader(remote)
                .replicas(vec![remote])
                .call()
                .await
                .is_none()
        );

        leads
            .log
            .lock()
            .expect("log lock")
            .append(
                &mut producer_batch()
                    .producer(BatchProducer::from_wire((10, 0)))
                    .records(3)
                    .call(),
            )
            .expect("append on the leader");
        follows
            .replicate_batch(
                producer_batch()
                    .producer(BatchProducer::from_wire((20, 3)))
                    .records(2)
                    .max_timestamp(2_000)
                    .transaction(BatchTransactionMode::Transactional)
                    .call(),
            )
            .await
            .expect("replicate a transactional batch");
        let mut marker = build_marker_batch(
            krabka_log::ProducerId(20),
            3,
            krabka_log::Offset(2),
            MarkerType::Commit,
            9,
        );
        marker.base_timestamp = 3_000;
        marker.max_timestamp = 3_000;
        follows
            .replicate_batch(marker)
            .await
            .expect("replicate the commit marker");
        follows
            .replicate_batch(
                producer_batch()
                    .producer(BatchProducer::from_wire((21, 0)))
                    .max_timestamp(4_000)
                    .transaction(BatchTransactionMode::Transactional)
                    .base_offset(3)
                    .call(),
            )
            .await
            .expect("replicate an open transaction");

        request_identity!((p, peer), principal("alice"));
        let request = DescribeProducersRequest {
            topics: ["leads", "follows", "moved"]
                .into_iter()
                .map(|name| TopicRequest {
                    name: name.into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let topic = |name: &str, row: PartitionResponse| TopicResponse {
            name: name.into(),
            partitions: vec![row],
            ..Default::default()
        };

        let resp = drive(&broker, &request, &test_context(&p, &peer));

        let expected = DescribeProducersResponse {
            throttle_time_ms: 0,
            topics: vec![
                topic(
                    "leads",
                    partition_row(codes::NONE, vec![
                        producer_row().producer_id(10).last_sequence(2).call(),
                    ]),
                ),
                topic(
                    "follows",
                    partition_row(codes::NONE, vec![
                        producer_row()
                            .producer_id(20)
                            .producer_epoch(3)
                            .last_sequence(1)
                            .last_timestamp(3_000)
                            .coordinator_epoch(9)
                            .call(),
                        producer_row()
                            .producer_id(21)
                            .last_timestamp(4_000)
                            .current_txn_start_offset(3)
                            .call(),
                    ]),
                ),
                topic(
                    "moved",
                    partition_row(codes::NOT_LEADER_OR_FOLLOWER, Vec::new()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);

        // Every partition of this broker shares its one log directory.
        broker
            .log_dir_status
            .mark_offline(&leads.log_dir.load(), "test: EIO");
        let offline = drive(&broker, &request, &test_context(&p, &peer));
        let expected = DescribeProducersResponse {
            topics: vec![
                topic(
                    "leads",
                    partition_row(codes::KAFKA_STORAGE_ERROR, Vec::new()),
                ),
                topic(
                    "follows",
                    partition_row(codes::KAFKA_STORAGE_ERROR, Vec::new()),
                ),
                topic(
                    "moved",
                    partition_row(codes::NOT_LEADER_OR_FOLLOWER, Vec::new()),
                ),
            ],
            ..Default::default()
        };
        assert!(offline == expected);
        broker_handle.shutdown().await;
    }
}
