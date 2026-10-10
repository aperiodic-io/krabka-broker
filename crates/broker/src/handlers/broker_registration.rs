//! `BrokerRegistration` (`api_key=62`). KIP-631/KIP-903 broker registration.

use std::collections::HashSet;

use krabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId};
use krabka_protocol::owned::{
    broker_registration_request::{BrokerRegistrationRequest, Listener},
    broker_registration_response::BrokerRegistrationResponse,
};
use krabka_raft::RaftError;

use crate::{broker::Broker, codes};

context_handler! {
    BrokerRegistrationRequest => BrokerRegistrationResponse,
    (broker, req, version, ctx),
    {
        let image = broker.controller.current_image();

        // ClusterAction is checked before the active-controller requirement.
        if let Some(code) = super::registration_listeners::registration_refusal(broker, &image, ctx) {
            return Ok(response(code, -1));
        }
        // Kafka's controller decides one registration at a time on its event
        // thread. Hold the registration turn from the session check to the
        // session replacement, so two registrations of one broker id, or of one
        // log directory, cannot both pass against the same image.
        let turn = broker.liveness.registration_turn().await;
        let image = broker.controller.current_image();

        let node_id = match u64::try_from(req.broker_id) {
            Ok(id) => NodeId(id),
            Err(_) => return Ok(response(codes::INVALID_REGISTRATION, -1)),
        };
        // The checks run in the order of `ClusterControlManager.registerBroker`,
        // so a request with more than one fault gets Kafka's error code.
        if !crate::cluster_id::matches(&req.cluster_id, image.cluster_id()) {
            return Ok(response(codes::INCONSISTENT_CLUSTER_ID, -1));
        }
        let incarnation_id = uuid::Uuid::from_bytes(req.incarnation_id.0);
        let existing = image.broker(node_id);
        // A new incarnation is refused only while the previous one still holds a
        // heartbeat session. A restarted broker has a new incarnation id, and it
        // registers once the session of the process it replaced expires.
        if let Some(existing) = existing
            && existing.incarnation_id != incarnation_id
            && broker.liveness.has_valid_session(node_id.0).await
        {
            return Ok(response(codes::DUPLICATE_BROKER_REGISTRATION, -1));
        }
        if req.is_migrating_zk_broker {
            return Ok(response(codes::BROKER_ID_NOT_REGISTERED, -1));
        }
        let directory_assignment = image.finalized_metadata_version().is_some_and(|level| {
            level >= krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL
        });
        if directory_assignment && let Err(code) = validate_log_dirs(&req, &image, node_id) {
            return Ok(response(code, -1));
        }
        let endpoints = match decode_listeners(&req.listeners) {
            Ok(endpoints) => endpoints,
            Err(code) => return Ok(response(code, -1)),
        };
        if let Err(code) = validate_features(&req, &image) {
            return Ok(response(code, -1));
        }

        let first = &endpoints[0];
        let features = req
            .features
            .iter()
            .map(|feature| {
                (
                    feature.name.clone(),
                    (feature.min_supported_version, feature.max_supported_version),
                )
            })
            .collect();
        let log_dirs = if directory_assignment {
            req.log_dirs
                .iter()
                .map(|directory| uuid::Uuid::from_bytes(directory.0))
                .collect()
        } else {
            Vec::new()
        };
        let amended = existing.filter(|existing| existing.incarnation_id == incarnation_id);
        let record = BrokerRegistrationRecord {
            // `ClusterControlManager.registerBroker`: a new registration keeps the
            // schema default `Fenced = true` and is not in controlled shutdown,
            // and an amend copies both from the registration it amends.
            fenced: amended.is_none_or(|existing| existing.fenced),
            in_controlled_shutdown: amended.is_some_and(|existing| existing.in_controlled_shutdown),
            cordoned_log_dirs: None,
            node_id,
            // An amend keeps the epoch it registered at, and the controller
            // applies it only while the broker is still registered at that epoch.
            // Any other registration carries no epoch (-1), and the controller
            // stamps the offset it commits at.
            broker_epoch: amended.map_or(-1, |existing| existing.broker_epoch),
            incarnation_id,
            host: first.host.clone(),
            port: first.port,
            rack: req.rack.clone(),
            endpoints,
            log_dirs,
            features,
        };
        if amended.is_some() {
            // The same process registered again, after a lost response or a
            // controller change. Kafka rewrites the record with the listeners and
            // features the request carries and keeps the epoch; nothing about the
            // broker's log changed, so no restart handling runs.
            if let Err(error) = broker
                .controller
                .submit_change(vec![MetadataRecord::V1BrokerRegistration(record)])
                .await
            {
                return Ok(response(raft_error_code(&error), -1));
            }
            return Ok(registered_response(broker, node_id, incarnation_id));
        }
        let clean_restart = clean_shutdown_proven(&req, version, &image, node_id);
        // KIP-966: a broker that cannot prove it stopped gracefully may have lost
        // an unflushed log tail, so nothing the cluster still believes about that
        // log holds -- not its ELR membership, and not its ISR seat either.
        // `ClusterControlManager.registerBroker` calls
        // `handleBrokerShutdown(id, isCleanShutdown, records)` before it appends
        // the `RegisterBrokerRecord`, and the branch that boolean picks is the
        // whole difference: `isElrFeatureEnabled() && !isCleanShutdown` runs two
        // `generateLeaderAndIsrUpdates` calls, which is what
        // `compute_unclean_restart_changes` is. A restart that proves itself
        // clean takes none of that.
        let restart = if clean_restart {
            crate::leader_election::FailoverPlan::default()
        } else {
            crate::leader_election::compute_unclean_restart_changes(
                &image,
                node_id,
                &broker.liveness,
                &broker.metrics,
            )
            .await
        };
        for (topic, partition) in &restart.unavailable {
            tracing::warn!(
                %topic, partition, node_id = node_id.0,
                "returning broker led this partition and no live ISR replica can take it; partition unavailable"
            );
        }
        if let Err(error) = broker
            .controller
            .submit_change(registration_records(restart.changes, record))
            .await
        {
            return Ok(response(raft_error_code(&error), -1));
        }
        // The session of the previous incarnation, if there was one, belongs to
        // a process that is gone. `ClusterControlManager.registerBroker` removes
        // it and registers the new incarnation fenced. It happens at once, inside
        // the registration turn, so no heartbeat of the new process can come
        // before it.
        broker.liveness.replace_incarnation(node_id.0).await;
        let answer = registered_response(broker, node_id, incarnation_id);
        drop(turn);
        // KIP-966: a partition whose topic opted into an offset-aware recovery
        // strategy is handed to the Unclean Recovery Manager, the same way the
        // dead-broker failover hands one over. Fire and forget.
        for (topic, partition, strategy) in restart.recoveries {
            broker
                .unclean_recovery
                .enqueue(crate::unclean_recovery::RecoveryJob {
                    topic,
                    partition,
                    strategy,
                    reply: None,
                    // Nobody asked for this recovery, so there is no proposal to
                    // name and nobody to refuse; `break_glass` decides whether the
                    // URM runs it.
                    proposal: None,
                })
                .await;
        }

        Ok(answer)
    }
}

/// The answer to an accepted registration: the epoch the image now holds for
/// `node_id`, if the registration it holds is this incarnation's.
fn registered_response(
    broker: &Broker,
    node_id: NodeId,
    incarnation_id: uuid::Uuid,
) -> BrokerRegistrationResponse {
    let epoch = broker
        .controller
        .current_image()
        .broker(node_id)
        .filter(|registration| registration.incarnation_id == incarnation_id)
        .map_or(-1, |registration| registration.broker_epoch);
    response(
        if epoch < 0 {
            codes::UNKNOWN_SERVER_ERROR
        } else {
            0
        },
        epoch,
    )
}

/// The records one accepted registration writes, in the order they apply.
///
/// `restart` is what a restart that could not prove itself clean costs, empty
/// for one that did: the ELR withdrawals and ISR removals
/// [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
/// decided. They go ahead of the registration, so a replay that stops between
/// the two has already stopped trusting the returning log rather than not yet
/// started.
fn registration_records(
    mut restart: Vec<MetadataRecord>,
    record: BrokerRegistrationRecord,
) -> Vec<MetadataRecord> {
    restart.push(MetadataRecord::V1BrokerRegistration(record));
    restart
}

/// Whether this registration proves the broker stopped gracefully last time.
///
/// `previous_broker_epoch` reaches the wire only at v3, and Kafka's
/// `QuorumController` mirrors that by passing
/// `cleanShutdownDetectionEnabled = requestApiVersion >= 3` into
/// `ClusterControlManager.registerBroker`, which forces the comparison to
/// `false` for anything older. A broker that cannot say what epoch it last
/// held is not trusted to have held one.
fn clean_shutdown_proven(
    req: &BrokerRegistrationRequest,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    node_id: NodeId,
) -> bool {
    version >= 3
        && crate::clean_shutdown::restart_was_clean(image, node_id, req.previous_broker_epoch)
}

fn decode_listeners(listeners: &[Listener]) -> Result<Vec<BrokerEndpoint>, i16> {
    crate::handlers::registration_listeners::decode!(
        listeners,
        codes::INVALID_REGISTRATION,
        codes::INVALID_REGISTRATION,
        codes::INVALID_REGISTRATION
    )
}

/// Kafka's directory checks in `ClusterControlManager.registerBroker`, which
/// apply from `metadata.version` `3.7-IV2` (KIP-858): at least one directory,
/// none of the hundred reserved ids, no id twice, and no id that another
/// broker already registered.
fn validate_log_dirs(
    req: &BrokerRegistrationRequest,
    image: &krabka_metadata::MetadataImage,
    node_id: NodeId,
) -> Result<(), i16> {
    let directories: Vec<uuid::Uuid> = req
        .log_dirs
        .iter()
        .map(|directory| uuid::Uuid::from_bytes(directory.0))
        .collect();
    if directories.is_empty() || directories.iter().copied().any(reserved_directory_id) {
        return Err(codes::INVALID_REGISTRATION);
    }
    let distinct: HashSet<uuid::Uuid> = directories.iter().copied().collect();
    if distinct.len() != directories.len() {
        return Err(codes::INVALID_REGISTRATION);
    }
    let owned_by_another = image
        .brokers()
        .filter(|registered| registered.node_id != node_id)
        .any(|registered| registered.log_dirs.iter().any(|id| distinct.contains(id)));
    if owned_by_another {
        return Err(codes::INVALID_REGISTRATION);
    }
    Ok(())
}

/// Kafka's `DirectoryId.reserved`: the first hundred ids, which include
/// `MIGRATING`, `UNASSIGNED` and `LOST`.
fn reserved_directory_id(id: uuid::Uuid) -> bool {
    id.as_u128() < 100
}

/// Kafka's feature checks in `ClusterControlManager.registerBroker`, in its
/// order.
///
/// Every feature the broker names must support the level the cluster
/// finalized, and a feature the cluster has not finalized is at level 0
/// (`UNSUPPORTED_VERSION`). The broker must name `metadata.version`
/// (`INVALID_REGISTRATION`). Last, every feature the cluster finalized above
/// level 0 must be one the broker names (`UNSUPPORTED_VERSION`).
fn validate_features(
    req: &BrokerRegistrationRequest,
    image: &krabka_metadata::MetadataImage,
) -> Result<(), i16> {
    let finalized = |name: &str| image.finalized_feature(name).unwrap_or(0);
    let unsupported = req.features.iter().any(|feature| {
        let level = finalized(&feature.name);
        !(feature.min_supported_version..=feature.max_supported_version).contains(&level)
    });
    if unsupported {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    let names_metadata_version = req
        .features
        .iter()
        .any(|feature| feature.name == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE);
    if !names_metadata_version {
        return Err(codes::INVALID_REGISTRATION);
    }
    let missing = image.finalized_features().iter().any(|(name, level)| {
        *level != 0 && !req.features.iter().any(|feature| feature.name == *name)
    });
    if missing {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    Ok(())
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        RaftError::Metadata(_) => codes::INVALID_REGISTRATION,
        _ => codes::UNKNOWN_SERVER_ERROR,
    }
}

fn response(error_code: i16, broker_epoch: i64) -> BrokerRegistrationResponse {
    BrokerRegistrationResponse {
        error_code,
        broker_epoch,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::broker_registration_request::Feature;

    use super::*;

    #[test]
    fn listener_validation_rejects_duplicates_and_unknown_protocol() {
        let valid = Listener {
            name: "PLAINTEXT".into(),
            host: "broker".into(),
            port: 9092,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&valid)).is_ok());
        assert2::assert!(decode_listeners(&[valid.clone(), valid.clone()]).is_err());
        assert2::assert!(
            decode_listeners(&[Listener {
                security_protocol: 99,
                ..valid
            }])
            .is_err()
        );
    }

    /// krabka-io/krabka-broker#822: the feature checks of
    /// `ClusterControlManager.registerBroker`, in Kafka's order. The cluster
    /// has finalized `metadata.version` 25 and `group.version` 1.
    #[test]
    fn features_are_checked_as_kafka_checks_them() {
        type Case<'a> = (&'a str, &'a [(&'a str, i16, i16)], Result<(), i16>);
        let cases: &[Case<'_>] = &[
            (
                "both finalized features in range",
                &[("metadata.version", 7, 25), ("group.version", 0, 1)],
                Ok(()),
            ),
            (
                "metadata.version below the finalized level",
                &[("metadata.version", 7, 24), ("group.version", 0, 1)],
                Err(codes::UNSUPPORTED_VERSION),
            ),
            (
                "a feature the cluster left at level 0 that the broker cannot run at 0",
                &[
                    ("metadata.version", 7, 25),
                    ("group.version", 0, 1),
                    ("share.version", 1, 1),
                ],
                Err(codes::UNSUPPORTED_VERSION),
            ),
            (
                "an unfinalized feature that includes level 0",
                &[
                    ("metadata.version", 7, 25),
                    ("group.version", 0, 1),
                    ("share.version", 0, 1),
                ],
                Ok(()),
            ),
            (
                "no metadata.version",
                &[("group.version", 0, 1)],
                Err(codes::INVALID_REGISTRATION),
            ),
            ("no features at all", &[], Err(codes::INVALID_REGISTRATION)),
            (
                "a finalized feature the broker does not name",
                &[("metadata.version", 7, 25)],
                Err(codes::UNSUPPORTED_VERSION),
            ),
            (
                "an unsupported feature wins over a missing metadata.version",
                &[("group.version", 2, 3)],
                Err(codes::UNSUPPORTED_VERSION),
            ),
        ];
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (name, level) in [("metadata.version", 25), ("group.version", 1)] {
            image.apply(&MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: name.into(),
                    level,
                },
            ));
        }
        for (what, features, expected) in cases {
            let req = BrokerRegistrationRequest {
                features: features
                    .iter()
                    .map(|&(name, min, max)| Feature {
                        name: name.into(),
                        min_supported_version: min,
                        max_supported_version: max,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            assert2::check!(validate_features(&req, &image) == *expected, "{what}");
        }
    }

    /// krabka-io/krabka-broker#822: the KIP-858 directory checks of
    /// `ClusterControlManager.registerBroker`. Broker 1 already registered
    /// directory 500, and broker 2 is registering.
    #[test]
    fn log_dirs_are_checked_as_kafka_checks_them() {
        let cases: &[(&str, &[u128], Result<(), i16>)] = &[
            ("one fresh directory", &[1000], Ok(())),
            ("two fresh directories", &[1000, 1001], Ok(())),
            ("no directory", &[], Err(codes::INVALID_REGISTRATION)),
            ("MIGRATING", &[0], Err(codes::INVALID_REGISTRATION)),
            (
                "the last reserved id",
                &[99],
                Err(codes::INVALID_REGISTRATION),
            ),
            ("the first id past the reserved range", &[100], Ok(())),
            (
                "one id twice",
                &[1000, 1000],
                Err(codes::INVALID_REGISTRATION),
            ),
            (
                "a directory another broker registered",
                &[1000, 500],
                Err(codes::INVALID_REGISTRATION),
            ),
            ("a directory it registered itself", &[600], Ok(())),
        ];
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node, directory) in [(1, 500), (2, 600)] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    broker_epoch: 10,
                    incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
                    host: "broker".into(),
                    log_dirs: vec![uuid::Uuid::from_u128(directory)],
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(node))
                },
            ));
        }
        for (what, directories, expected) in cases {
            let req = BrokerRegistrationRequest {
                broker_id: 2,
                log_dirs: directories
                    .iter()
                    .map(|&id| {
                        krabka_protocol::primitives::uuid::Uuid(
                            *uuid::Uuid::from_u128(id).as_bytes(),
                        )
                    })
                    .collect(),
                ..Default::default()
            };
            assert2::check!(
                validate_log_dirs(&req, &image, NodeId(2)) == *expected,
                "{what}"
            );
        }
    }
}

/// KIP-966 on the wire: an external broker's registration carries the
/// clean-shutdown proof as `previousBrokerEpoch`, and the controller withdraws
/// its ELR membership when the proof does not hold.
///
/// This is the path a JVM broker takes.
/// [`crate::broker::registration`] holds the same rule for krabka's own
/// self-registration, which is a different route to the same records.
#[cfg(test)]
mod wire_tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{
        BrokerRegistrationRecord, LeaderEpoch, NodeId, PartitionRecord, TopicConfigRecord,
        TopicRecord,
    };

    use super::*;
    use crate::{
        config_keys::MIN_INSYNC_REPLICAS,
        elr::{TopicElr, state::PartitionElr},
        handlers::forward_to_controller::is_active_controller,
        test_support::{peer, start_broker_with_authorizer},
    };

    const TOPIC: &str = "orders";
    const REGISTERED: NodeId = NodeId(2);
    /// `BrokerRegistration` v3 is where `previousBrokerEpoch` enters the
    /// schema, and `QuorumController` passes
    /// `cleanShutdownDetectionEnabled = requestApiVersion >= 3`.
    const V3: i16 = 3;
    const V2: i16 = 2;

    /// What the restarting broker offers as its clean-shutdown proof.
    #[derive(Debug, Clone, Copy)]
    enum Offer {
        /// The epoch the cluster still holds for it -- what a graceful stop
        /// leaves behind.
        HeldEpoch,
        /// The `-1` a `BrokerRegistrationRequest` defaults to, which is all a
        /// crashed broker has.
        Unproven,
    }

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(NodeId).collect()
    }

    /// Node 2 registered, and one partition of `TOPIC` holding `isr` under
    /// `min_isr`, published with `elr`.
    ///
    /// All three are fixture knobs because the halves of the restart rule read
    /// different columns: the ELR withdrawal fires on a partition node 2 has
    /// already left, and the ISR removal only on one it is still in.
    fn seed_records(isr: &[u64], min_isr: &str, elr: Option<&str>) -> Vec<MetadataRecord> {
        let mut records = vec![
            // KIP-966 ELR maintenance is gated on the feature, whose release
            // default is 0, so the seed finalizes it the way an operator's
            // `kafka-features upgrade` would.
            MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
                name: crate::features::ELR_VERSION.into(),
                level: 1,
            }),
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                // A new registration carries no epoch; the controller stamps
                // the offset it commits at.
                broker_epoch: -1,
                incarnation_id: uuid::Uuid::from_u128(0xdead),
                host: "broker-2".into(),
                endpoints: vec![crate::test_support::plaintext_broker_endpoint(
                    "broker-2", 9092,
                )],
                log_dirs: vec![uuid::Uuid::from_u128(1011)],
                features: krabka_metadata::supported_feature_ranges(),
                ..crate::test_support::broker_registration(REGISTERED)
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.into(),
                topic_id: uuid::Uuid::from_u128(9),
                partitions: 1,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: TOPIC.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: nodes(&[1, 2, 3]),
                isr: nodes(isr),
                leader_epoch: LeaderEpoch(7),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); 3],
                partition_epoch: 4,
            }),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), min_isr.to_string())]
                    .into_iter()
                    .collect(),
            }),
        ];
        if let Some(elr) = elr {
            records.extend(crate::elr::state::test_records(TOPIC, elr));
        }
        records
    }

    /// The partition of `TOPIC` as it stands after a restart: the two columns
    /// the restart rule can move.
    #[derive(Debug, PartialEq, Eq)]
    struct Restarted {
        isr: Vec<NodeId>,
        elr: PartitionElr,
    }

    /// [`restart`] against the seed the ELR tests use: node 2 already out of
    /// the ISR, and published as eligible.
    async fn reregister(version: i16, offer: Offer) -> PartitionElr {
        restart(version, offer, seed_records(&[1], "2", Some("0:2,3:")))
            .await
            .elr
    }

    /// Re-register node 2 from a fresh process -- a new incarnation id, the
    /// way a JVM broker generates one per boot -- against `seed`, offering
    /// `previous_broker_epoch` as its proof, and return what the partition
    /// looks like afterwards.
    async fn restart(version: i16, offer: Offer, seed: Vec<MetadataRecord>) -> Restarted {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer))
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !is_active_controller(&broker) {
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        broker.controller.submit_change(seed).await.expect("seed");
        // This fixture represents a stopped old broker, whose heartbeat
        // session is over. End it once the liveness ticker has seeded this
        // term, so the seeding cannot open it again afterwards.
        crate::test_support::end_heartbeat_session(&broker, REGISTERED.0).await;

        let image = broker.controller.current_image();
        let previous_broker_epoch = match offer {
            Offer::HeldEpoch => image
                .broker_epoch(REGISTERED)
                .expect("node 2 is registered"),
            Offer::Unproven => crate::clean_shutdown::UNPROVEN,
        };
        let request = BrokerRegistrationRequest {
            broker_id: 2,
            cluster_id: image.cluster_id().to_string(),
            incarnation_id: krabka_protocol::primitives::uuid::Uuid(
                uuid::Uuid::from_u128(0xbeef).into_bytes(),
            ),
            listeners: vec![Listener {
                name: "PLAINTEXT".into(),
                host: "broker-2".into(),
                port: 9092,
                security_protocol: 0,
                ..Default::default()
            }],
            features: crate::test_support::finalized_broker_features(&image),
            log_dirs: vec![krabka_protocol::primitives::uuid::Uuid(
                uuid::Uuid::from_u128(1011).into_bytes(),
            )],
            previous_broker_epoch,
            ..Default::default()
        };
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("broker"),
            client_id = "broker-client"
        );
        // Through the dispatch registry: `previous_broker_epoch` only rides
        // the wire from version 3.
        let response: BrokerRegistrationResponse = crate::test_support::dispatch_wire(
            &broker,
            krabka_protocol::api_key::ApiKey::BrokerRegistration as i16,
            version,
            &request,
            &ctx,
        )
        .await;
        assert!(
            response.error_code == 0,
            "registration was refused: {response:?}"
        );

        let image = broker.controller.current_image();
        let restarted = Restarted {
            isr: image
                .partition(TOPIC, 0)
                .expect("seeded partition")
                .isr
                .clone(),
            elr: TopicElr::of_topic(&image, TOPIC).partition(0),
        };
        drop(image);
        drop(broker);
        broker_handle.shutdown().await;
        restarted
    }

    /// A broker offering the epoch the cluster still holds for it restarted
    /// cleanly and keeps its membership.
    #[tokio::test]
    async fn a_proven_clean_restart_keeps_its_elr_membership() {
        assert!(
            reregister(V3, Offer::HeldEpoch).await
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                }
        );
    }

    /// A broker offering nothing -- the `-1` a `BrokerRegistrationRequest`
    /// defaults to, which is what a crashed broker has to offer -- loses it.
    #[tokio::test]
    async fn an_unproven_restart_loses_its_elr_membership() {
        assert!(
            reregister(V3, Offer::Unproven).await
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![],
                }
        );
    }

    /// A request older than v3 has no `previousBrokerEpoch` field to carry a
    /// proof, so the controller cannot detect a clean shutdown and assumes
    /// unclean -- Kafka enables `cleanShutdownDetectionEnabled` only when
    /// `requestApiVersion >= 3`. The epoch on the struct is ignored because it
    /// never reaches the wire.
    #[tokio::test]
    async fn a_pre_v3_registration_cannot_prove_anything() {
        assert!(
            reregister(V2, Offer::HeldEpoch).await
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![],
                }
        );
    }

    /// The seed the ISR half needs: node 2 in a healthy ISR that sits exactly
    /// at `min.insync.replicas`, and nothing published about it.
    ///
    /// This is the case an ELR withdrawal on its own cannot see. There is no
    /// membership to withdraw, and yet the next eligibility is derived from
    /// `old_isr`, which still names node 2 -- so whether node 2 stays in that
    /// ISR is the whole of the difference between the two branches.
    fn healthy_isr_seed() -> Vec<MetadataRecord> {
        seed_records(&[1, 2, 3], "3", None)
    }

    /// A restart that cannot prove itself clean loses its ISR seat as well as
    /// its eligibility, and the batch that takes the seat away does not hand
    /// eligibility back on the way out.
    ///
    /// Dropping node 2 leaves the ISR under `min.insync.replicas`, so the
    /// recompute that rides the same batch has an ELR to publish and
    /// `old_isr ∪ eligible_before` still names node 2. Kafka's
    /// `uncleanShutdownReplicas` is what keeps it out of the eligible column,
    /// and it lands nowhere else: the partition has a leader, so its
    /// last-known ELR is empty.
    #[tokio::test]
    async fn an_unproven_restart_loses_its_isr_seat_without_regaining_eligibility() {
        assert!(
            restart(V3, Offer::Unproven, healthy_isr_seed()).await
                == Restarted {
                    isr: nodes(&[1, 3]),
                    elr: PartitionElr {
                        eligible_leader_replicas: vec![],
                        last_known_elr: vec![],
                    },
                }
        );
    }

    /// A restart that proves itself clean keeps the seat, from the same seed.
    ///
    /// Kafka's `handleBrokerShutdown` reaches the two-call unclean path only
    /// under `isElrFeatureEnabled() && !isCleanShutdown`, so the proof is what
    /// decides, and a broker that stopped gracefully still holds the log its
    /// ISR membership claims. Nothing moves: same ISR, still no ELR.
    #[tokio::test]
    async fn a_proven_clean_restart_keeps_its_isr_seat() {
        assert!(
            restart(V3, Offer::HeldEpoch, healthy_isr_seed()).await
                == Restarted {
                    isr: nodes(&[1, 2, 3]),
                    elr: PartitionElr::default(),
                }
        );
    }
}
