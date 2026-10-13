//! The checks and the commit of one `CreateTopics` row.
//!
//! This is the part of Kafka's `ReplicationControlManager.createTopic` that
//! a topic goes through after authorization: the name, the existence check,
//! the configs, the shape, the replica placement, the topic policy, the
//! controller-mutation quota and the metadata commit. `CreateTopics` runs it
//! for each row that it gets, from a client or from the auto topic creation
//! of a broker ([`crate::auto_topic_creation::AutoTopicCreation`]).

use {
    super::{
        THROTTLING_QUOTA_EXCEEDED_MESSAGE, automatic_leaderships, diskless_wal_placement_error,
        inactive_brokers, invalid_topic_shape, manual_leaderships,
        name::topic_name_error,
        placement::{automatic_placement_exclusions, resolve_assignments},
        placement_failure_message,
        records::{null_config_error, topic_config_overrides, topic_records},
        resolve_default,
        response::topic_error_result,
        site_broker_views, topic_exists_result,
    },
    crate::{
        broker::Broker,
        codes,
        config_keys::{self, resolve_preferred_leader_site},
        handlers::{
            offline_replicas::unavailable_brokers,
            partition_materialization::PartitionMaterialization,
        },
        quota::ControllerMutationQuota,
        site_placement::PlacementRng,
        topic_policy::check,
    },
    krabka_protocol::owned::{
        create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    },
    krabka_raft::RaftError,
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// A topic that passed every check, and was committed unless the request is
/// validate-only.
pub struct NewTopic {
    pub name: String,
    pub topic_id: Uuid,
    /// The replicas of each partition, in partition order.
    pub assignments: Vec<Vec<krabka_raft::NodeId>>,
    /// The canonical config overrides the topic was created with.
    pub overrides: BTreeMap<String, String>,
}

/// The state that the checks of every row in one request share: the metadata
/// image the request reads, and what the broker derives from it once.
pub struct TopicCreation<'a> {
    broker: &'a Broker,
    image: &'a krabka_metadata::MetadataImage,
    preferred_site: Option<&'a str>,
    topic_defaults: config_keys::TopicDefaults,
    /// KIP-108: a validate-only request runs every check and commits nothing.
    validate_only: bool,
}

impl<'a> TopicCreation<'a> {
    pub fn new(
        broker: &'a Broker,
        image: &'a krabka_metadata::MetadataImage,
        validate_only: bool,
    ) -> Self {
        Self {
            broker,
            image,
            preferred_site: resolve_preferred_leader_site(image),
            topic_defaults: config_keys::TopicDefaults::from_image(
                image,
                broker.config.features.unstable_api_versions,
            ),
            validate_only,
        }
    }

    /// Runs the checks of one row and, unless the request is validate-only,
    /// commits the topic and opens its local partitions.
    ///
    /// `quota` is the KIP-599 controller-mutation quota of the request.
    ///
    /// # Errors
    ///
    /// Returns the refused row, in the shape Kafka answers it.
    pub async fn create(
        &self,
        topic_req: CreatableTopic,
        quota: &mut ControllerMutationQuota,
    ) -> Result<NewTopic, Box<CreatableTopicResult>> {
        let mut topic_req = topic_req;
        let broker = self.broker;
        let image = self.image;
        let node_id = broker.config.node_id;
        let name = topic_req.name.clone();

        // Kafka checks the name before anything else. The name becomes part
        // of the partition directory path, so no later step may see a name
        // that this check refuses.
        if let Some((code, message)) = topic_name_error(image, &name) {
            return Err(Box::new(topic_error_result(name, code, Some(message))));
        }

        // Kafka answers an existing topic TOPIC_ALREADY_EXISTS before it
        // looks at the configs, the counts, the placement or the policy, so
        // `kafka-topics --create --if-not-exists` succeeds whatever else the
        // request carries.
        if image.topic(&name).is_some() {
            return Err(Box::new(topic_exists_result(name)));
        }

        // Kafka's `computeConfigChanges` refuses a config with a null value
        // before it validates the others.
        if let Some(message) = null_config_error(&topic_req) {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(message),
            )));
        }

        // Kafka validates a topic's configs before it looks at placement, so a
        // rejected config wins over INVALID_PARTITIONS on the same topic.
        // The stored map carries each value in the form Kafka reports it, so
        // every reader of it parses ` TRUE ` as it parses `true`.
        let config_overrides = match config_keys::canonical_topic_config_map(
            &topic_config_overrides(&topic_req),
            &self.topic_defaults,
            broker.config.remote_storage_backend.is_some(),
            broker.config.features.unstable_api_versions,
        ) {
            Ok(canonical) => canonical,
            Err(reason) => {
                return Err(Box::new(topic_error_result(
                    name,
                    codes::INVALID_CONFIG,
                    Some(reason),
                )));
            }
        };

        // `canonical_topic_config_map` checks tiered storage against the
        // broker's backend, but not the diskless flag. A diskless topic needs
        // the same thing from the broker: an object-store backend. Without
        // `remote_storage_backend` there is no `DisklessReadHandle`, so the
        // broker starts neither the WAL index projection nor the object
        // flusher. The topic would still accept writes through its WAL
        // quorum, but nothing would ever move them to the object tier and
        // nothing would ever trim the local logs, so the durability model the
        // flag advertises would not exist and local storage would grow without
        // bound. Refuse at creation rather than accept a topic that cannot
        // work.
        //
        // This reads *this* broker's configuration, and the topic is
        // cluster-wide, so it is a guard against the common
        // one-configuration-fleet mistake rather than a cluster-wide
        // guarantee: a fleet where only some brokers carry a backend can still
        // create a topic that some of them cannot serve. Catching the default
        // configuration is the case worth having.
        let diskless = config_keys::resolve_diskless(Some(&config_overrides));
        if diskless && broker.config.remote_storage_backend.is_none() {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(format!(
                    "{}=true requires an object-store tier, but this broker has no \
                     `remote_storage_backend` configured; the diskless WAL could never flush \
                     or trim",
                    config_keys::DISKLESS
                )),
            )));
        }

        // Kafka's `ReplicationControlManager.createTopic` checks the
        // replication factor first and the partition count second. Each may
        // be -1, which KIP-464 resolves to the broker's `num.partitions` or
        // `default.replication.factor`. A manual assignment requires -1 for
        // both, and `resolve_assignments` checks that.
        if topic_req.assignments.is_empty() {
            if let Some((code, message)) = invalid_topic_shape(&topic_req) {
                return Err(Box::new(topic_error_result(
                    name,
                    code,
                    Some(message.to_owned()),
                )));
            }
            topic_req.num_partitions =
                resolve_default(topic_req.num_partitions, broker.config.num_partitions);
            topic_req.replication_factor = resolve_default(
                topic_req.replication_factor,
                broker.config.default_replication_factor,
            );
        }

        // Read the current broker set from the controller's image, with the
        // site and the witness role of each broker. `site_broker_views` sorts
        // by node id for determinism, and it covers the race in which the
        // self-registration record has not reached the local image yet.
        // The automatic placement leaves out a broker that is in controlled
        // shutdown and not fenced, and one whose log directories are all
        // cordoned (KIP-1066). It takes a fenced broker only as a last resort,
        // as Kafka's placer does. A broker that a controlled shutdown stopped
        // is fenced, so it is such a last resort. The ISR below leaves that
        // broker out. A manual assignment may name any registered broker,
        // because Kafka 4.3.1 checks only that the broker is registered, and
        // the ISR again leaves out the inactive ones.
        let unavailable = unavailable_brokers(broker, image).await;
        let inactive = inactive_brokers(image, &unavailable);
        let manual = !topic_req.assignments.is_empty();
        let no_exclusion = std::collections::HashSet::new();
        let unusable = automatic_placement_exclusions(image, &unavailable);
        let brokers = site_broker_views(
            image,
            broker.config.is_broker().then_some(node_id),
            if manual { &no_exclusion } else { &unusable },
            &unavailable,
        );

        let assignments = match resolve_assignments(
            &topic_req,
            &brokers,
            self.preferred_site,
            &mut PlacementRng::from_entropy(),
        ) {
            Ok(assignments) => assignments,
            Err((code, message)) => {
                return Err(Box::new(topic_error_result(name, code, Some(message))));
            }
        };

        if assignments.is_empty() {
            // The placement cannot satisfy the request. RF above the broker
            // count, fenced brokers included, and a cluster with no unfenced
            // broker are the common causes. Surface INVALID_REPLICATION_FACTOR
            // with the message of Kafka's replica placer.
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_REPLICATION_FACTOR,
                Some(placement_failure_message(
                    topic_req.replication_factor,
                    &brokers,
                )),
            )));
        }

        let leaderships = if manual {
            match manual_leaderships(
                &assignments,
                &inactive,
                &config_keys::witness_node_ids(image),
                0,
            ) {
                Ok(leaderships) => leaderships,
                Err(message) => {
                    return Err(Box::new(topic_error_result(
                        name,
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        Some(message),
                    )));
                }
            }
        } else {
            automatic_leaderships(&assignments, &inactive)
        };

        if diskless
            && let Some(reason) =
                diskless_wal_placement_error(image, &broker.config, 0, &leaderships)
        {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(reason),
            )));
        }

        // KIP-108: the operator-declared topic policy, on the effective
        // partition count and replication factor the placement resolved and
        // on the topic's own config overrides. Kafka calls
        // `CreateTopicPolicy.validate` here too: after config validation, and
        // before the records are generated.
        if let Err(reason) = check(
            &broker.config.topic_policy,
            &name,
            Some(assignments.len()),
            assignments.first().map(Vec::len),
            &config_overrides,
        ) {
            return Err(Box::new(topic_error_result(
                name,
                codes::POLICY_VIOLATION,
                Some(reason),
            )));
        }

        // KIP-599: charge the partitions this topic creates.
        let partition_count = u64::try_from(assignments.len()).unwrap_or(u64::MAX);
        if quota.record(partition_count).is_err() {
            return Err(Box::new(topic_error_result(
                name,
                codes::THROTTLING_QUOTA_EXCEEDED,
                Some(THROTTLING_QUOTA_EXCEEDED_MESSAGE.into()),
            )));
        }

        // Kafka's `Uuid.randomUuid`: never a leading dash.
        let topic_id = krabka_format::random_uuid();

        // A validate-only request has now passed every check the committing
        // path runs, and commits nothing.
        if !self.validate_only {
            let placement = Placement {
                topic_id,
                assignments: &assignments,
                leaderships: &leaderships,
                overrides: &config_overrides,
                diskless,
            };
            self.commit(&topic_req, &placement).await?;
        }
        Ok(NewTopic {
            name,
            topic_id,
            assignments,
            overrides: config_overrides,
        })
    }

    /// Commits the records of a checked topic, and opens the partitions this
    /// broker holds.
    async fn commit(
        &self,
        topic_req: &CreatableTopic,
        placement: &Placement<'_>,
    ) -> Result<(), Box<CreatableTopicResult>> {
        let broker = self.broker;
        let name = &topic_req.name;
        // Build the batch: one TopicRecord + N PartitionRecords.
        let records = topic_records(
            topic_req,
            placement.topic_id,
            placement.assignments,
            placement.leaderships,
            placement.overrides,
        );

        let failure = match broker.controller.submit_change(records).await {
            Ok(_) => {
                PartitionMaterialization {
                    broker,
                    log_dirs: &broker.config.all_log_dirs(),
                    diskless: placement.diskless,
                    topic_id: placement.topic_id,
                }
                .materialize(
                    "CreateTopics",
                    name,
                    (0..placement.assignments.len()).map(|index| i32::try_from(index).unwrap_or(0)),
                    placement.assignments,
                    placement.leaderships,
                )
                .await;
                return Ok(());
            }
            // Another request created the name after this one read the
            // image. The quorum decides that race, and the row is the one
            // Kafka's existence check answers.
            Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {
                topic_exists_result(name.clone())
            }
            Err(RaftError::Metadata(krabka_metadata::MetadataError::InvalidRecord(_))) => {
                // E.g., `partitions <= 0` rejected by image::validate.
                topic_error_result(name.clone(), codes::INVALID_PARTITIONS, None)
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                topic_error_result(name.clone(), codes::NOT_CONTROLLER, None)
            }
            Err(e) => {
                tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                topic_error_result(name.clone(), codes::UNKNOWN_SERVER_ERROR, None)
            }
        };
        Err(Box::new(failure))
    }
}

/// The placement and configs a checked topic is committed with.
struct Placement<'a> {
    topic_id: Uuid,
    assignments: &'a [Vec<krabka_raft::NodeId>],
    leaderships: &'a [super::InitialLeadership],
    overrides: &'a BTreeMap<String, String>,
    diskless: bool,
}
