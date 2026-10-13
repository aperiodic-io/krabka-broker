//! `AlterReplicaLogDirs` (`api_key=34`, KIP-113) moves a replica between the
//! log directories of one broker.
//!
//! This intra-broker log-directory reassignment moves a hosted replica from
//! one of the configured `log.dirs` of this broker to another, without a new
//! replication from the leader. It backs `kafka-log-dirs --alter` and the
//! `--reassignment-json-file` log-dir overrides of
//! `kafka-reassign-partitions`.
//!
//! The handler validates the inputs immediately, starts a background
//! replicator task for each move with [`crate::future_log::start_move`], and
//! returns success. The data copy and the atomic dir-rename occur in the
//! background. Clients poll `DescribeLogDirs` and watch `is_future_key`
//! change from `true` to `false` to find the end of the move.
//!
//! [`crate::network::dispatch::handle_alter_replica_log_dirs_frame`] enforces
//! authorisation (Cluster.Alter). This handler runs only after the principal
//! has been authorized.

#[cfg(test)]
use crate::producer_state::ProducerState;
use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        future_log::{self, MoveError},
        handlers::encode_response,
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_protocol::{
        Decode,
        owned::{
            alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
        },
    },
    std::{collections::BTreeMap, path::PathBuf},
};

/// A partition addressed by topic name and index, ordered by topic and then index.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let all_log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let controller = broker.controller.clone();
    let move_policy = future_log::MovePolicy {
        retry_backoff: broker.config.future_log_move_retry_backoff,
        read_chunk: broker.config.future_log_move_read_chunk,
        throttle: broker.throttle_state.alter_log_dirs.clone(),
    };
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
        let image = controller.current_image();

        // The wire format lets a client list the same partition under several
        // target dirs. Kafka's `AlterReplicaLogDirsRequest.partitionDirs()`
        // collects them into a map that keeps the LAST destination, and
        // `ReplicaManager.alterReplicaLogDirs` handles each partition once, so
        // a superseded destination is neither validated nor started.
        let mut destinations: BTreeMap<TopicPartitionKey, PathBuf> = BTreeMap::new();
        for dir in req.dirs {
            let target_path = PathBuf::from(&dir.path);
            for topic in dir.topics {
                for partition_index in topic.partitions {
                    destinations.insert(
                        TopicPartitionKey {
                            topic: topic.name.clone(),
                            partition: partition_index,
                        },
                        target_path.clone(),
                    );
                }
            }
        }

        // (topic, partition) → error code.
        let mut per_partition: BTreeMap<TopicPartitionKey, i16> = BTreeMap::new();
        for (
            TopicPartitionKey {
                topic,
                partition: partition_index,
            },
            target_path,
        ) in destinations
        {
            let code = match future_log::start_move(
                &partitions,
                &future_logs,
                &all_log_dirs,
                &log_dir_status,
                &log_config,
                (&topic, krabka_ids::PartitionIndex(partition_index)),
                image.topic(&topic).map(|topic| topic.topic_id),
                &target_path,
                move_policy.clone(),
            )
            .await
            {
                Ok(()) => codes::NONE,
                Err(MoveError::TopicNameTooLong) => codes::INVALID_TOPIC_EXCEPTION,
                Err(MoveError::LogDirNotFound) => codes::LOG_DIR_NOT_FOUND,
                Err(MoveError::Cordoned) => codes::INVALID_REPLICA_ASSIGNMENT,
                Err(MoveError::ReplicaNotAvailable) => codes::REPLICA_NOT_AVAILABLE,
                Err(MoveError::Storage(error)) => {
                    tracing::warn!(
                        topic = %topic,
                        partition = partition_index,
                        target = %target_path.display(),
                        error = %error,
                        "replica log-dir move failed"
                    );
                    codes::KAFKA_STORAGE_ERROR
                }
            };
            per_partition.insert(
                TopicPartitionKey {
                    topic,
                    partition: partition_index,
                },
                code,
            );
        }

        // Group per-partition results back into the response's
        // per-topic shape.
        let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
            BTreeMap::new();
        for (TopicPartitionKey { topic, partition }, code) in per_partition {
            by_topic
                .entry(topic)
                .or_default()
                .push(AlterReplicaLogDirPartitionResult {
                    partition_index: partition,
                    error_code: code,
                    ..Default::default()
                });
        }

        let results: Vec<_> = by_topic
            .into_iter()
            .map(|(name, partitions)| AlterReplicaLogDirTopicResult {
                topic_name: name,
                partitions,
                ..Default::default()
            })
            .collect();

        let resp = AlterReplicaLogDirsResponse {
            results,
            ..Default::default()
        };
        encode_response(&resp, version)
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::spawn_partition, handlers::test_support as handler_fixture_support,
            log_dir::partition_dir, test_support as broker_fixture_support,
        },
        assert2::assert,
        krabka_protocol::owned::alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic,
        },
    };

    krabka_macros::wire_codec_helpers! {
        encode_request(AlterReplicaLogDirsRequest);
        decode_response(AlterReplicaLogDirsResponse);
    }

    fn expected_partition_response(
        topic: &str,
        partition_index: i32,
        error_code: i16,
    ) -> AlterReplicaLogDirsResponse {
        krabka_macros::unthrottled_wire!(AlterReplicaLogDirsResponse {
            results: vec![krabka_macros::tagged_wire!(AlterReplicaLogDirTopicResult {
                topic_name: topic.to_string(),
                partitions: vec![krabka_macros::tagged_wire!(
                    AlterReplicaLogDirPartitionResult {
                        partition_index,
                        error_code,
                    }
                )],
            })],
        })
    }

    async fn assert_partition_response(
        broker: &Broker,
        version: i16,
        request: AlterReplicaLogDirsRequest,
        topic: &str,
        error_code: i16,
        label: &str,
    ) {
        let bytes = handle(broker, version, 1, &encode_request(&request, version))
            .await
            .expect("handle");
        let expected = expected_partition_response(topic, 0, error_code);
        assert!(decode_response(&bytes, version) == expected, "{label}");
    }

    #[tokio::test]
    async fn handle_preserves_unknown_target_response_shape() {
        let version = 2;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [broker_fixture_support::start_broker_with(|_| {})];
        };
        let req = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: "/tmp/krabka-missing-log-dir".into(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".into(),
                    partitions: vec![7],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, version);

        let bytes = handle(&broker, version, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = krabka_macros::unthrottled_wire!(AlterReplicaLogDirsResponse {
            results: vec![krabka_macros::tagged_wire!(AlterReplicaLogDirTopicResult {
                topic_name: "orders".to_string(),
                partitions: vec![krabka_macros::tagged_wire!(
                    AlterReplicaLogDirPartitionResult {
                        partition_index: 7,
                        error_code: codes::LOG_DIR_NOT_FOUND,
                    }
                )],
            })],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka matches the destination against the configured absolute paths by
    /// string equality (`LogManager.isLogDirOnline`) and refuses a partition
    /// whose future directory name passes 255 characters before it reads the
    /// destination (`ReplicaManager.alterReplicaLogDirs`). A destination that
    /// names a configured directory reaches the hosted-replica check, which
    /// answers `REPLICA_NOT_AVAILABLE` for a partition this broker does not
    /// host.
    // The cases spell POSIX paths and one is a symbolic link.
    #[cfg(unix)]
    #[tokio::test]
    async fn destination_and_topic_name_validation_matches_kafka() {
        let version = 2;
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = std::fs::canonicalize(extra.path()).expect("canonical extra dir");
        let links = tempfile::tempdir().expect("link dir");
        let link = links.path().join("link");
        std::os::unix::fs::symlink(&extra_dir, &link).expect("symlink to the extra dir");
        let extra_str = extra_dir.display().to_string();
        let name = extra_dir
            .file_name()
            .expect("tempdir has a name")
            .to_string_lossy()
            .to_string();
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_with_extra_log_dir(&extra_dir)];
        };

        // A topic of 213 characters gives Kafka's future directory name,
        // `<topic>-0.<32 hex>-future`, exactly 255 characters.
        let longest = "t".repeat(213);
        let too_long = "t".repeat(214);
        let cases = [
            (
                "the configured path",
                extra_str.clone(),
                "orders",
                codes::REPLICA_NOT_AVAILABLE,
            ),
            (
                "a trailing slash",
                format!("{extra_str}/"),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a trailing dot",
                format!("{extra_str}/."),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a parent hop",
                format!("{extra_str}/../{name}"),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a relative path",
                extra_str.trim_start_matches('/').to_string(),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "a symbolic link",
                link.display().to_string(),
                "orders",
                codes::LOG_DIR_NOT_FOUND,
            ),
            (
                "the longest topic name",
                extra_str.clone(),
                longest.as_str(),
                codes::REPLICA_NOT_AVAILABLE,
            ),
            (
                "a topic name one too long",
                extra_str.clone(),
                too_long.as_str(),
                codes::INVALID_TOPIC_EXCEPTION,
            ),
            (
                "a too-long name before an unknown path",
                format!("{extra_str}/"),
                too_long.as_str(),
                codes::INVALID_TOPIC_EXCEPTION,
            ),
        ];
        for (label, path, topic, error_code) in cases {
            let req = AlterReplicaLogDirsRequest {
                dirs: vec![AlterReplicaLogDir {
                    path,
                    topics: vec![AlterReplicaLogDirTopic {
                        name: topic.into(),
                        partitions: vec![0],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert_partition_response(&broker, version, req, topic, error_code, label).await;
        }
        broker_handle.shutdown().await;
    }

    /// Kafka collects the request into `partitionDirs()`, a map that keeps the
    /// LAST destination of a partition listed twice, and handles each
    /// partition once. A superseded destination neither answers nor starts a
    /// move, whatever the last one does.
    #[tokio::test]
    async fn a_partition_listed_twice_moves_to_its_last_destination_only() {
        let version = 2;
        let extra = tempfile::tempdir().expect("extra log dir");
        let extra_dir = extra.path().to_path_buf();
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_with_extra_log_dir(&extra_dir)];
        };
        let primary = broker.config.log_dir.clone();
        let partition_dir = partition_dir(&primary, "orders", 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let hosted = spawn_partition(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            primary.clone(),
            krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default())
                .expect("open log"),
            broker.log_dir_status.clone(),
            std::sync::Arc::new(ProducerState::new()),
            false,
        );
        broker.partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            hosted.clone(),
        );
        let valid = extra_dir.display().to_string();
        let unknown = "/krabka-missing-log-dir".to_string();

        // (name, destinations in request order, expected code, whether the
        // partition ends up in the extra directory)
        let cases = [
            (
                "a valid destination superseded by an unknown one",
                [valid.clone(), unknown.clone()],
                codes::LOG_DIR_NOT_FOUND,
                false,
            ),
            (
                "an unknown destination superseded by a valid one",
                [unknown, valid],
                codes::NONE,
                true,
            ),
        ];
        for (name, destinations, error_code, moves) in cases {
            let req = AlterReplicaLogDirsRequest {
                dirs: destinations
                    .into_iter()
                    .map(|path| AlterReplicaLogDir {
                        path,
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };

            assert_partition_response(&broker, version, req, "orders", error_code, name).await;
            // An empty partition moves within milliseconds once a move starts,
            // so a move that never starts is given a short look.
            let wait = if moves {
                std::time::Duration::from_secs(20)
            } else {
                std::time::Duration::from_millis(500)
            };
            let deadline = tokio::time::Instant::now() + wait;
            let mut in_extra_dir = false;
            while !in_extra_dir && tokio::time::Instant::now() < deadline {
                in_extra_dir = std::fs::canonicalize(hosted.log_dir.load_full().as_path()).ok()
                    == std::fs::canonicalize(&extra_dir).ok();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(in_extra_dir == moves, "{name}");
        }
        broker_handle.shutdown().await;
    }
}
