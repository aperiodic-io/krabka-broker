//! Formatting the target log directory and seeding it with the topics the
//! archive scan recovered.
//!
//! A restore drives `krabka_format::run_from_args_with_records` in process,
//! forwarding the target-side flags and handing it one `TopicRecord` per topic
//! and one `PartitionRecord` per partition, so the restored cluster boots with
//! its topics already present. This runs once, before any segment is written.

#[cfg(test)]
use krabka_metadata::FeatureLevelRecord;
use {
    crate::{
        args::RestoreArgs,
        discover::{ArchiveInventory, load_snapshot},
        error::RestoreError,
        report::MetadataRestoreReport,
    },
    krabka_ids::LeaderEpoch,
    krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord},
    krabka_remote_storage::{
        ArchiveVerifyReport, ChainHead, ChainStamp, RemoteLogSegmentState, WormChainRecord,
        parse_partition_dir_name,
    },
    krabka_remote_storage_topic::Snapshot,
    std::{
        collections::{HashMap, HashSet},
        path::Path,
    },
    uuid::Uuid,
};

/// The formatter outcome needed by the final restore report.
pub struct FormatTargetOutcome {
    /// Cluster id written to the target.
    pub cluster_id: Uuid,
    /// Metadata recovered from the optional controller snapshot.
    pub metadata: MetadataRestoreReport,
}

#[cfg(test)]
fn feature_level_records<'a>(excluded: &'a [&str]) -> impl Iterator<Item = MetadataRecord> + 'a {
    krabka_metadata::feature_registry()
        .iter()
        .filter(|registered| !excluded.contains(&registered.name()))
        .map(|registered| {
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: registered.name().to_owned(),
                level: 0,
            })
        })
}

/// Format the target log directory, seed it with the recovered topics, and
/// return the cluster id it was formatted with.
///
/// The formatter generates a cluster id when none is given and does not report
/// it back, so this passes an explicit `--cluster-id` and keeps it for the
/// report. An operator who restores a cluster has to know its identity.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when `--node-id` is absent: every restored partition names the target node as leader and sole replica, and defaulting that identity to node 0 could silently name a node the operator never said exists. Returns [`RestoreError::Format`] when the formatter rejects the target-side flags, and [`RestoreError::Io`] when the target cannot be written.
pub async fn format_target(
    args: &RestoreArgs,
    inventory: &ArchiveInventory,
) -> Result<FormatTargetOutcome, RestoreError> {
    format_target_with_snapshot(args, inventory, None).await
}

/// Format from retained authenticated bytes when supplied, without reopening the source path.
pub(crate) async fn format_target_with_snapshot(
    args: &RestoreArgs,
    inventory: &ArchiveInventory,
    authenticated_snapshot: Option<&[u8]>,
) -> Result<FormatTargetOutcome, RestoreError> {
    let node_id = args.target.node_id.ok_or_else(|| {
        RestoreError::InvalidArgument(
            "--node-id is required: every restored partition names the target node as leader \
             and sole replica, so a restore cannot default that identity to node 0"
                .to_owned(),
        )
    })?;
    let cluster_id = args.target.cluster_id.unwrap_or_else(Uuid::new_v4);

    let mut format_argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        args.target.log_dir.to_string_lossy().into_owned(),
        "--cluster-id".to_owned(),
        cluster_id.to_string(),
        "--node-id".to_owned(),
        node_id.to_string(),
    ];
    if args.target.standalone {
        format_argv.push("--standalone".to_owned());
    }
    if !args.target.initial_controllers.is_empty() {
        format_argv.push("--initial-controllers".to_owned());
        format_argv.push(args.target.initial_controllers.join(","));
    }
    if args.target.no_initial_controllers {
        format_argv.push("--no-initial-controllers".to_owned());
    }
    if let Some(listener) = &args.target.controller_listener {
        format_argv.push("--controller-listener".to_owned());
        format_argv.push(listener.clone());
    }

    let snapshot_records = read_metadata_snapshot(
        args.archive.metadata_snapshot.as_deref(),
        authenticated_snapshot,
    )
    .await?;
    let SeededMetadata {
        records: extra,
        report: metadata,
    } = seed_metadata_records(
        inventory,
        node_id,
        args.archive.metadata_snapshot.clone(),
        snapshot_records.as_deref(),
    )?;
    let code = krabka_format::run_from_args_with_records(format_argv, extra).await;
    if code == 0 {
        Ok(FormatTargetOutcome {
            cluster_id,
            metadata,
        })
    } else {
        Err(RestoreError::Format { code })
    }
}

/// Seed the restored broker's local RLMM cache while resetting its Kafka
/// cursors for the new `__remote_log_metadata` topic.
///
/// # Errors
/// Returns [`RestoreError`] when the source snapshot cannot be read, its
/// authenticated chain tips cannot be established, or the target snapshot
/// cannot be written.
pub fn seed_rlmm_snapshot(
    args: &RestoreArgs,
    authenticated: Option<&ArchiveVerifyReport>,
) -> Result<(), RestoreError> {
    seed_rlmm_snapshot_from(args, authenticated, None)
}

/// Seed from the decoded authenticated snapshot when supplied, without reopening its path.
pub(crate) fn seed_rlmm_snapshot_from(
    args: &RestoreArgs,
    authenticated: Option<&ArchiveVerifyReport>,
    authenticated_snapshot: Option<&Snapshot>,
) -> Result<(), RestoreError> {
    let Some(path) = args.archive.rlmm_snapshot.as_ref() else {
        return Ok(());
    };
    let mut snapshot = match authenticated_snapshot {
        Some(snapshot) => snapshot.clone(),
        None => load_snapshot(path)?,
    };
    snapshot.committed_offsets.fill(-1);
    snapshot
        .dump
        .partitions
        .retain(|partition| args.selects_topic(&partition.topic_id_partition.topic));
    if let Some(report) = authenticated {
        seed_authenticated_chain_tips(&mut snapshot.dump, report)?;
    }
    if args.dry_run {
        return Ok(());
    }
    snapshot
        .write_atomic(
            &args
                .target
                .log_dir
                .join("remote-log-metadata")
                .join(krabka_remote_storage_topic::snapshot::SNAPSHOT_FILE_NAME),
        )
        .map_err(|error| {
            RestoreError::Io(std::io::Error::other(format!(
                "cannot seed restored RLMM snapshot: {error}"
            )))
        })
}

fn seed_authenticated_chain_tips(
    dump: &mut krabka_remote_storage::RlmmCacheDump,
    report: &ArchiveVerifyReport,
) -> Result<(), RestoreError> {
    for partition in &mut dump.partitions {
        let tp = &partition.topic_id_partition;
        let verified = report
            .partitions
            .iter()
            .find(|candidate| {
                candidate
                    .partition_dir
                    .rsplit('/')
                    .next()
                    .and_then(parse_partition_dir_name)
                    .is_some_and(|parsed| {
                        parsed.topic_id == tp.topic_id && parsed.partition == tp.partition
                    })
            })
            .and_then(|partition| partition.epochs.last())
            .ok_or_else(|| RestoreError::Authenticity {
                reason: format!(
                    "RLMM snapshot partition {}-{} has no authenticated WORM chain tip",
                    tp.topic, tp.partition
                ),
            })?;
        let latest = partition
            .segments
            .iter_mut()
            .filter(|segment| {
                !matches!(
                    segment.state(),
                    RemoteLogSegmentState::DeleteSegmentStarted
                        | RemoteLogSegmentState::DeleteSegmentFinished
                )
            })
            .max_by_key(|segment| segment.start_offset())
            .ok_or_else(|| RestoreError::Authenticity {
                reason: format!(
                    "RLMM snapshot partition {}-{} has no live segment for its authenticated WORM chain tip",
                    tp.topic, tp.partition
                ),
            })?;
        *latest = latest.clone().with_custom_metadata(
            WormChainRecord::request(ChainStamp {
                epoch_id: verified.epoch_id,
                seq: verified.last_seq,
                prev_head: ChainHead::GENESIS,
            })
            .with_head(verified.head)
            .to_custom_metadata(),
        );
    }
    Ok(())
}

async fn read_metadata_snapshot(
    path: Option<&Path>,
    authenticated_snapshot: Option<&[u8]>,
) -> Result<Option<Vec<MetadataRecord>>, RestoreError> {
    let Some(path) = path else {
        return Ok(None);
    };
    let loaded;
    let bytes = if let Some(bytes) = authenticated_snapshot {
        bytes
    } else {
        loaded = tokio::fs::read(path).await.map_err(|error| {
            RestoreError::Io(std::io::Error::new(
                error.kind(),
                format!("cannot read metadata snapshot {}: {error}", path.display()),
            ))
        })?;
        &loaded
    };
    krabka_raft::deserialize_metadata_snapshot(bytes)
        .map(Some)
        .map_err(|error| RestoreError::MetadataSnapshot {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })
}

/// The records a restore seeds into the target formatter, with the report of what they restore.
struct SeededMetadata {
    records: Vec<MetadataRecord>,
    report: MetadataRestoreReport,
}

/// How many partitions the archive holds for one topic, and the topic's id.
#[derive(Debug, Clone, Copy)]
struct TopicTally {
    topic_id: Uuid,
    partitions: i32,
}

/// Build the topic and partition records a restore seeds into the target formatter, from what the archive scan recovered.
///
/// Every topic's [`MetadataRecord::V1Topic`] precedes every [`MetadataRecord::V1Partition`], which is the ordering `krabka_format::run_with_records`'s own doc requires: a `MetadataImage` derives a topic's partition count from the partition records that apply after it, so a partition can only follow its own topic.
///
/// Pulled out as a pure function, separate from [`format_target`]'s formatter call, so a test can check exactly what gets seeded without running the formatter at all.
fn seed_metadata_records(
    inventory: &ArchiveInventory,
    node_id: NodeId,
    snapshot: Option<std::path::PathBuf>,
    snapshot_records: Option<&[MetadataRecord]>,
) -> Result<SeededMetadata, RestoreError> {
    let mut topic_order: Vec<&str> = Vec::new();
    let mut topics: HashMap<&str, TopicTally> = HashMap::new();
    for entry in &inventory.partitions {
        let topic = entry.partition.topic.as_str();
        let counted = topics.entry(topic).or_insert_with(|| {
            topic_order.push(topic);
            TopicTally {
                topic_id: entry.partition.topic_id,
                partitions: 0,
            }
        });
        counted.partitions += 1;
    }

    let mut metadata = MetadataRestoreReport {
        snapshot,
        topic_configs: 0,
        access_control_entries: 0,
        client_quotas: 0,
        scram_credentials: 0,
        feature_levels: 0,
        topics_without_configuration: Vec::new(),
    };
    if snapshot_records.is_none() {
        metadata.topics_without_configuration = topic_order
            .iter()
            .map(|topic| (*topic).to_owned())
            .collect();
    }

    let mut features = Vec::new();
    let mut feature_epoch = Vec::new();
    let mut topic_configs = Vec::new();
    let mut scram = Vec::new();
    let mut acls = Vec::new();
    let mut quotas = Vec::new();
    let snapshot_topics: HashMap<&str, Uuid> = snapshot_records
        .into_iter()
        .flatten()
        .filter_map(|record| match record {
            MetadataRecord::V1Topic(topic) => Some((topic.name.as_str(), topic.topic_id)),
            _ => None,
        })
        .collect();
    let mut snapshot_features = HashSet::new();
    for record in snapshot_records.into_iter().flatten() {
        match record {
            MetadataRecord::V1FeatureLevel(feature) => {
                metadata.feature_levels += 1;
                snapshot_features.insert(feature.name.as_str());
                features.push(record.clone());
            }
            MetadataRecord::V1FeaturesEpoch(_) => feature_epoch.push(record.clone()),
            MetadataRecord::V1TopicConfig(config) => {
                if let Some(TopicTally {
                    topic_id: archive_topic_id,
                    ..
                }) = topics.get(config.topic.as_str())
                {
                    match snapshot_topics.get(config.topic.as_str()) {
                        Some(snapshot_topic_id) if snapshot_topic_id == archive_topic_id => {
                            metadata.topic_configs += 1;
                            topic_configs.push(record.clone());
                        }
                        snapshot_topic_id => {
                            return Err(RestoreError::MetadataSnapshotTopicMismatch {
                                topic: config.topic.clone(),
                                archive_topic_id: *archive_topic_id,
                                snapshot_topic_id: snapshot_topic_id
                                    .map_or_else(|| "absent".to_owned(), ToString::to_string),
                            });
                        }
                    }
                }
            }
            MetadataRecord::V1ScramCredential(_) => {
                metadata.scram_credentials += 1;
                scram.push(record.clone());
            }
            MetadataRecord::V1AccessControlEntry(_) => {
                metadata.access_control_entries += 1;
                acls.push(record.clone());
            }
            MetadataRecord::V1ClientQuota(_) => {
                metadata.client_quotas += 1;
                quotas.push(record.clone());
            }
            _ => {}
        }
    }
    if snapshot_records.is_some() {
        // A feature that the snapshot does not finalize is disabled in the
        // restored cluster. `metadata.version` is never disabled: Kafka has no
        // `MetadataVersion` at level 0, and the active controller refuses
        // bootstrap records that remove it. So a snapshot without one keeps
        // the level of the format.
        features.extend(
            krabka_metadata::feature_registry()
                .iter()
                .filter(|feature| {
                    !matches!(
                        feature.name(),
                        krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                            | krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                    ) && !snapshot_features.contains(feature.name())
                })
                .map(|feature| {
                    MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
                        name: feature.name().to_owned(),
                        level: 0,
                    })
                }),
        );
    }

    let mut records = Vec::with_capacity(
        features.len()
            + feature_epoch.len()
            + topic_order.len()
            + inventory.partitions.len()
            + topic_configs.len()
            + scram.len()
            + acls.len()
            + quotas.len(),
    );
    records.append(&mut features);
    records.append(&mut feature_epoch);
    for topic in &topic_order {
        let TopicTally {
            topic_id,
            partitions,
        } = topics
            .get(topic)
            .copied()
            .expect("every topic in topic_order was inserted into topics above");
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: (*topic).to_owned(),
            topic_id,
            partitions,
            replication_factor: 1,
        }));
    }
    for entry in &inventory.partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: entry.partition.topic.clone(),
            partition: entry.partition.partition,
            leader: node_id,
            replicas: vec![node_id],
            isr: vec![node_id],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: Vec::new(),
            // KIP-631: 0 on creation. `PartitionRecord::default()`'s
            // `partition_epoch` is -1, the on-disk deserialization default for
            // a record written before this field existed; a freshly restored
            // partition is neither, so this is set explicitly rather than
            // pulled from `..Default::default()`.
            partition_epoch: 0,
        }));
    }
    records.append(&mut topic_configs);
    records.append(&mut scram);
    records.append(&mut acls);
    records.append(&mut quotas);
    Ok(SeededMetadata {
        records,
        report: metadata,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authenticate_captured_snapshot,
            discover::UnrecognizedKeys,
            materialize::test_support::{args_from, partition_inventory},
        },
        assert2::check,
        krabka_metadata::{
            AclEntry, AclOperation, ClientQuotaRecord, FeatureLevelRecord, FeaturesEpochRecord,
            MetadataImage, PatternType, PermissionType, QuotaEntity, ResourceType,
            ScramCredentialRecord, TopicConfigRecord,
        },
        krabka_remote_storage::{
            PartitionDump, RemotePartitionDeleteState, RlmmCacheDump, Sha256Digest,
            TopicIdPartition,
        },
        krabka_security::SaslMechanism,
    };

    fn delete_marked_dump() -> RlmmCacheDump {
        RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: TopicIdPartition::new(uuid::Uuid::from_u128(7), "orders", 0),
                segments: Vec::new(),
                delete_state: Some(RemotePartitionDeleteState::DeletePartitionMarked),
            }],
        }
    }

    #[test]
    fn seed_metadata_records_emits_every_topic_before_any_partition() {
        let orders_id = Uuid::new_v4();
        let payments_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", orders_id, 0),
                partition_inventory("orders", orders_id, 1),
                partition_inventory("payments", payments_id, 0),
            ],
            unrecognized: UnrecognizedKeys::default(),
        };

        let SeededMetadata {
            records,
            report: metadata,
        } = seed_metadata_records(&inventory, NodeId(7), None, None).expect("seed metadata");

        let partition_record = |topic: &str, partition: i32| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition,
                leader: NodeId(7),
                replicas: vec![NodeId(7)],
                isr: vec![NodeId(7)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            })
        };
        let expected = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id: orders_id,
                partitions: 2,
                replication_factor: 1,
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "payments".to_owned(),
                topic_id: payments_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("orders", 0),
            partition_record("orders", 1),
            partition_record("payments", 0),
        ];
        check!(records == expected);
        check!(
            metadata.topics_without_configuration
                == vec!["orders".to_owned(), "payments".to_owned()]
        );
    }

    #[test]
    fn restored_rlmm_snapshot_keeps_cached_state_and_replays_the_new_topic_from_zero() {
        let source = tempfile::tempdir().expect("source tempdir");
        let target = tempfile::tempdir().expect("target tempdir");
        let source_path = source.path().join("snapshot");
        let dump = delete_marked_dump();
        Snapshot {
            committed_offsets: vec![19],
            dump: dump.clone(),
        }
        .write_atomic(&source_path)
        .expect("write source snapshot");
        let args = args_from(
            &["--rlmm-snapshot", &source_path.display().to_string()],
            target.path(),
        );

        seed_rlmm_snapshot(&args, None).expect("seed restored snapshot");

        let restored = Snapshot::load(
            &target
                .path()
                .join("remote-log-metadata")
                .join(krabka_remote_storage_topic::snapshot::SNAPSHOT_FILE_NAME),
        )
        .expect("read restored snapshot")
        .expect("restored snapshot exists");
        check!(restored.committed_offsets == vec![-1]);
        check!(restored.dump == dump);
    }

    #[tokio::test]
    async fn authenticated_snapshots_are_consumed_after_the_source_paths_change() {
        let source = tempfile::tempdir().expect("source");
        let target = tempfile::tempdir().expect("target");
        let metadata_path = source.path().join("metadata.checkpoint");
        let rlmm_path = source.path().join("snapshot");
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_owned(),
            level: 25,
        }));
        let metadata_bytes =
            krabka_raft::serialize_metadata_snapshot(&image, 42).expect("metadata");
        let snapshot = Snapshot {
            committed_offsets: vec![19],
            dump: delete_marked_dump(),
        };
        let rlmm_bytes = snapshot.encode();
        std::fs::write(&metadata_path, &metadata_bytes).unwrap();
        std::fs::write(&rlmm_path, &rlmm_bytes).unwrap();
        let retained_metadata = authenticate_captured_snapshot(
            Some(&metadata_path),
            Some(Sha256Digest::of(&metadata_bytes)),
            true,
            "metadata",
        )
        .await
        .unwrap()
        .unwrap();
        let retained_rlmm = authenticate_captured_snapshot(
            Some(&rlmm_path),
            Some(Sha256Digest::of(&rlmm_bytes)),
            true,
            "RLMM",
        )
        .await
        .unwrap()
        .unwrap();
        std::fs::write(&metadata_path, b"changed metadata").unwrap();
        std::fs::write(&rlmm_path, b"changed RLMM").unwrap();
        check!(
            read_metadata_snapshot(Some(&metadata_path), Some(&retained_metadata))
                .await
                .unwrap()
                == Some(krabka_raft::deserialize_metadata_snapshot(&metadata_bytes).unwrap())
        );
        let args = args_from(
            &["--rlmm-snapshot", &rlmm_path.display().to_string()],
            target.path(),
        );
        seed_rlmm_snapshot_from(
            &args,
            None,
            Some(&Snapshot::decode(&retained_rlmm).unwrap()),
        )
        .unwrap();
        let restored = Snapshot::load(&target.path().join("remote-log-metadata/snapshot"))
            .unwrap()
            .unwrap();
        check!(
            restored
                == Snapshot {
                    committed_offsets: vec![-1],
                    ..snapshot
                }
        );
    }

    #[test]
    fn authenticated_dry_run_still_validates_the_rlmm_snapshot() {
        let source = tempfile::tempdir().expect("source tempdir");
        let target = tempfile::tempdir().expect("target tempdir");
        let source_path = source.path().join("snapshot");
        std::fs::write(&source_path, b"not a snapshot").expect("write malformed snapshot");
        let args = args_from(
            &[
                "--rlmm-snapshot",
                &source_path.display().to_string(),
                "--dry-run",
            ],
            target.path(),
        );

        check!(seed_rlmm_snapshot(&args, None).is_err());
        check!(!target.path().join("remote-log-metadata/snapshot").exists());
    }

    #[test]
    fn seed_metadata_records_restores_supported_snapshot_families_in_dependency_order() {
        let topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", topic_id, 0)],
            unrecognized: UnrecognizedKeys::default(),
        };
        let feature = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_owned(),
            level: 25,
        });
        let epoch = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 });
        let config = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".to_owned(),
            overrides: maplit::btreemap! {"cleanup.policy".to_owned() => "compact".to_owned()},
        });
        let ignored_config = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "not-restored".to_owned(),
            overrides: maplit::btreemap! {"retention.ms".to_owned() => "1".to_owned()},
        });
        let scram = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".to_owned(),
            mechanism: SaslMechanism::ScramSha256,
            salt: vec![1; 16],
            stored_key: vec![2; 32],
            server_key: vec![3; 32],
            iterations: 4096,
        });
        let acl = MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".to_owned(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        });
        let quota = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".to_owned(),
                entity_name: Some("alice".to_owned()),
            }],
            config_key: "producer_byte_rate".to_owned(),
            config_value: Some(1024.0),
        });
        let snapshot_records = vec![
            quota.clone(),
            ignored_config,
            acl.clone(),
            scram.clone(),
            config.clone(),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            epoch.clone(),
            feature.clone(),
        ];
        let snapshot = std::path::PathBuf::from("/backup/metadata.checkpoint");

        let SeededMetadata { records, report } = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some(snapshot.clone()),
            Some(&snapshot_records),
        )
        .expect("seed metadata");

        let mut expected = vec![feature];
        expected.extend(feature_level_records(&[
            "metadata.version",
            krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE,
        ]));
        expected.extend([
            epoch,
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".to_owned(),
                partition: 0,
                leader: NodeId(7),
                replicas: vec![NodeId(7)],
                isr: vec![NodeId(7)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
            config,
            scram,
            acl,
            quota,
        ]);
        check!(records == expected);
        check!(
            report
                == MetadataRestoreReport {
                    snapshot: Some(snapshot),
                    topic_configs: 1,
                    access_control_entries: 1,
                    client_quotas: 1,
                    scram_credentials: 1,
                    feature_levels: 1,
                    topics_without_configuration: vec![],
                }
        );
    }

    /// A feature that the snapshot does not finalize is disabled, except
    /// `metadata.version`: no `MetadataVersion` has level 0, so a snapshot
    /// without one keeps the level the format gives.
    #[test]
    fn a_snapshot_without_a_metadata_version_keeps_the_formatted_level() {
        let inventory = ArchiveInventory {
            partitions: Vec::new(),
            unrecognized: UnrecognizedKeys::default(),
        };
        let group_version = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".to_owned(),
            level: 1,
        });

        let SeededMetadata { records, .. } = seed_metadata_records(
            &inventory,
            NodeId(7),
            None,
            Some(std::slice::from_ref(&group_version)),
        )
        .expect("seed metadata");

        let mut expected = vec![group_version];
        expected.extend(feature_level_records(&[
            "metadata.version",
            "group.version",
            krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE,
        ]));
        check!(records == expected);
    }

    #[test]
    fn snapshot_topic_config_must_match_the_archived_topic_id() {
        let archive_topic_id = Uuid::new_v4();
        let snapshot_topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", archive_topic_id, 0)],
            unrecognized: UnrecognizedKeys::default(),
        };
        let snapshot_records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id: snapshot_topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".to_owned(),
                overrides: maplit::btreemap! {
                    "cleanup.policy".to_owned() => "delete".to_owned()
                },
            }),
        ];

        let result = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some("metadata.checkpoint".into()),
            Some(&snapshot_records),
        );

        check!(matches!(
            result,
            Err(RestoreError::MetadataSnapshotTopicMismatch {
                topic,
                archive_topic_id: archive,
                snapshot_topic_id: snapshot,
            }) if topic == "orders"
                && archive == archive_topic_id
                && snapshot == snapshot_topic_id.to_string()
        ));

        let missing_topic = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some("metadata.checkpoint".into()),
            Some(&snapshot_records[1..]),
        );
        check!(matches!(
            missing_topic,
            Err(RestoreError::MetadataSnapshotTopicMismatch {
                snapshot_topic_id,
                ..
            }) if snapshot_topic_id == "absent"
        ));
    }

    #[tokio::test]
    async fn unreadable_metadata_snapshot_is_an_io_failure() {
        let missing = tempfile::tempdir()
            .expect("temp dir")
            .path()
            .join("missing.checkpoint");

        let result = read_metadata_snapshot(Some(&missing), None).await;

        let error = result.unwrap_err();
        check!(error.exit_code() == crate::error::EXIT_MATERIALIZE);
        check!(matches!(error, RestoreError::Io(_)));
    }

    #[tokio::test]
    async fn corrupt_metadata_snapshot_is_an_integrity_failure() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("metadata.checkpoint");
        std::fs::write(&path, b"not a metadata snapshot").expect("write corrupt snapshot");

        let error = read_metadata_snapshot(Some(&path), None).await.unwrap_err();

        check!(error.exit_code() == crate::error::EXIT_INTEGRITY);
        check!(matches!(
            error,
            RestoreError::MetadataSnapshot {
                path: error_path,
                ..
            } if error_path == path
        ));
    }

    #[tokio::test]
    async fn format_target_requires_node_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: UnrecognizedKeys::default(),
        };

        let result = format_target(&args, &inventory).await;
        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn format_target_formats_the_target_and_returns_the_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
            ],
            target.path(),
        );
        let topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", topic_id, 0),
                partition_inventory("orders", topic_id, 1),
            ],
            unrecognized: UnrecognizedKeys::default(),
        };

        let outcome = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(
            args.target.cluster_id.is_none() || Some(outcome.cluster_id) == args.target.cluster_id
        );
        check!(target.path().join("bootstrap.json").exists());
        check!(target.path().join("bootstrap.records.bin").exists());
    }

    #[tokio::test]
    async fn format_target_honors_an_explicit_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let fixed = Uuid::new_v4();
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
                "--cluster-id",
                &fixed.to_string(),
            ],
            target.path(),
        );
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: UnrecognizedKeys::default(),
        };

        let outcome = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(outcome.cluster_id == fixed);
    }
}
