//! What each subcommand does, and the one place that talks to a cluster.
//!
//! Every function here returns a [`BackupError`], and only [`crate::run()`] turns
//! one into an exit code. A runbook that wraps the tool gets the code; a test
//! that drives the library keeps the error.

use std::collections::{BTreeMap, BTreeSet};

use krabka_audit::FileEd25519Signer;
use krabka_client_admin::{
    AdminClient, AdminError, ConfigResource, DescribeConfigsOptions, TopicMetadata,
    groups::ListGroupsOptions,
};
use krabka_client_core::{
    Client, CoordinatorKeyType, build_find_coordinator, coordinator_endpoint,
    security::ClientSecurity,
};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_remote_storage::{ObjectEntry, Sha256Digest};
use krabka_remote_storage_topic::{KafkaMetadataEventLog, KafkaMetadataLogConfig};

use crate::{
    archive::{Archive, ArchiveArgs},
    capture::{RLMM_SNAPSHOT_RELATIVE, capture_id, capture_key, newest_metadata_checkpoint},
    cli::{CaptureSigningArgs, CaptureTrustArgs, LATEST},
    connection::SecurityArgs,
    diskless::capture_projection,
    error::BackupError,
    manifest::{
        Artifact, CAPTURE_ROOT, DISKLESS_WAL_INDEX, GROUP_OFFSETS, MANIFEST, MANIFEST_VERSION,
        METADATA_CHECKPOINT, Manifest, RLMM_SNAPSHOT, artifact_problem, sha256_hex,
    },
    offsets::{GroupOffsetsFile, commit_refusals, commit_request, group_offsets},
};

const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_GROUP_OFFSETS_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;
const MAX_DISKLESS_INDEX_BYTES: u64 = 256 * 1024 * 1024;

fn artifact_byte_limit(name: &str) -> u64 {
    match name {
        MANIFEST => MAX_MANIFEST_BYTES,
        GROUP_OFFSETS => MAX_GROUP_OFFSETS_BYTES,
        METADATA_CHECKPOINT | RLMM_SNAPSHOT => MAX_SNAPSHOT_BYTES,
        _ => MAX_DISKLESS_INDEX_BYTES,
    }
}

fn check_capture_size(name: &str, size: u64, max_bytes: u64) -> Result<(), BackupError> {
    if size > max_bytes {
        return Err(BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "capture artifact `{name}` is {size} bytes, exceeds restore limit of {max_bytes} bytes"
            ),
        )));
    }
    Ok(())
}

async fn read_capture_file(path: &std::path::Path, name: &str) -> Result<Vec<u8>, BackupError> {
    check_capture_size(
        name,
        tokio::fs::metadata(path).await?.len(),
        artifact_byte_limit(name),
    )?;
    let bytes = tokio::fs::read(path).await?;
    // The file can grow between its metadata check and the read.
    check_capture_size(name, bytes.len() as u64, artifact_byte_limit(name))?;
    Ok(bytes)
}

/// Milliseconds since the Unix epoch, saturating at zero on a clock set before
/// it. The value names a capture directory and nothing branches on it, so a
/// nonsensical clock produces an odd name rather than a failure.
fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis()),
    )
    .unwrap_or(0)
}

/// Copy this node's restore inputs into the archive.
///
/// # Errors
///
/// Returns [`BackupError::Io`] when the capture found nothing to take or an
/// artifact exceeds its restore size limit, and the archive's or the cluster's
/// own error otherwise.
pub async fn capture(
    log_dir: Option<&std::path::Path>,
    bootstrap_server: Option<&str>,
    archive: &ArchiveArgs,
) -> Result<String, BackupError> {
    capture_secured(
        log_dir,
        bootstrap_server,
        None,
        &CaptureSigningArgs::default(),
        archive,
    )
    .await
}

/// Load command-config security and capture this node's restore inputs.
///
/// # Errors
///
/// Returns the same errors as [`capture`], plus command-config read or parse
/// failures.
pub async fn capture_configured(
    log_dir: Option<&std::path::Path>,
    bootstrap_server: Option<&str>,
    security: &SecurityArgs,
    signing: &CaptureSigningArgs,
    archive: &ArchiveArgs,
) -> Result<String, BackupError> {
    let client_security = match bootstrap_server {
        Some(_) => security.load().await?,
        None => None,
    };
    capture_secured(log_dir, bootstrap_server, client_security, signing, archive).await
}

async fn capture_secured(
    log_dir: Option<&std::path::Path>,
    bootstrap_server: Option<&str>,
    security: Option<ClientSecurity>,
    signing: &CaptureSigningArgs,
    archive: &ArchiveArgs,
) -> Result<String, BackupError> {
    capture_secured_at(
        log_dir,
        bootstrap_server,
        security,
        signing,
        archive,
        now_ms(),
    )
    .await
}

async fn capture_secured_at(
    log_dir: Option<&std::path::Path>,
    bootstrap_server: Option<&str>,
    security: Option<ClientSecurity>,
    signing: &CaptureSigningArgs,
    archive: &ArchiveArgs,
    captured_at_ms: u64,
) -> Result<String, BackupError> {
    let store = Archive::open(archive)?;
    let id = capture_id(captured_at_ms);
    let mut artifacts: Vec<Artifact> = Vec::new();
    let mut absent: Vec<String> = Vec::new();
    let mut metadata_snapshot_sha256 = None;
    let mut rlmm_snapshot_sha256 = None;

    if let Some(log_dir) = log_dir {
        let rlmm = log_dir.join(RLMM_SNAPSHOT_RELATIVE);
        match read_capture_file(&rlmm, RLMM_SNAPSHOT).await {
            Ok(bytes) => {
                rlmm_snapshot_sha256 = Some(Sha256Digest::of(&bytes));
                artifacts.push(
                    upload(
                        &store,
                        &id,
                        RLMM_SNAPSHOT,
                        &rlmm.display().to_string(),
                        bytes,
                    )
                    .await?,
                );
            }
            Err(BackupError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                absent.push(format!("{} is absent", rlmm.display()));
            }
            Err(error) => return Err(error),
        }

        match newest_metadata_checkpoint(log_dir) {
            Some(path) => {
                let bytes = read_capture_file(&path, METADATA_CHECKPOINT).await?;
                metadata_snapshot_sha256 = Some(Sha256Digest::of(&bytes));
                artifacts.push(
                    upload(
                        &store,
                        &id,
                        METADATA_CHECKPOINT,
                        &path.display().to_string(),
                        bytes,
                    )
                    .await?,
                );
            }
            None => absent.push(format!(
                "{} holds no controller or observer metadata checkpoint",
                log_dir.display()
            )),
        }
    }

    if let Some(bootstrap) = bootstrap_server {
        let offsets = fetch_group_offsets(bootstrap, security.clone()).await?;
        let bytes = serde_json::to_vec_pretty(&offsets).map_err(|source| BackupError::Json {
            context: GROUP_OFFSETS.to_owned(),
            source,
        })?;
        let group_offsets_sha256 = Some(Sha256Digest::of(&bytes));
        artifacts.push(upload(&store, &id, GROUP_OFFSETS, bootstrap, bytes).await?);
        if let Some(bytes) = capture_diskless_index(
            &store,
            bootstrap,
            security.clone(),
            signing,
            metadata_snapshot_sha256,
            rlmm_snapshot_sha256,
            group_offsets_sha256,
        )
        .await?
        {
            artifacts.push(upload(&store, &id, DISKLESS_WAL_INDEX, bootstrap, bytes).await?);
        }
    }

    if artifacts.is_empty() {
        return Err(BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "captured nothing: {}",
                if absent.is_empty() {
                    "pass --log-dir, --bootstrap-server, or both".to_owned()
                } else {
                    absent.join("; ")
                }
            ),
        )));
    }

    let manifest = Manifest {
        version: MANIFEST_VERSION,
        capture_id: id.clone(),
        captured_at_ms,
        log_dir: log_dir.map(|dir| dir.display().to_string()),
        bootstrap_server: bootstrap_server.map(ToOwned::to_owned),
        artifacts,
    };
    let encoded = serde_json::to_vec_pretty(&manifest).map_err(|source| BackupError::Json {
        context: MANIFEST.to_owned(),
        source,
    })?;
    check_capture_size(MANIFEST, encoded.len() as u64, MAX_MANIFEST_BYTES)?;
    store.put_new(&capture_key(&id, MANIFEST), encoded).await?;

    println!("capture {id}");
    for artifact in &manifest.artifacts {
        println!(
            "  {} <- {} ({} bytes, sha256 {})",
            artifact.name, artifact.source, artifact.size_bytes, artifact.sha256
        );
    }
    for missing in &absent {
        println!("  WARNING: {missing}");
    }
    Ok(id)
}

async fn capture_diskless_index(
    store: &Archive,
    bootstrap: &str,
    security: Option<ClientSecurity>,
    signing: &CaptureSigningArgs,
    metadata_snapshot_sha256: Option<Sha256Digest>,
    rlmm_snapshot_sha256: Option<Sha256Digest>,
    group_offsets_sha256: Option<Sha256Digest>,
) -> Result<Option<Vec<u8>>, BackupError> {
    const TOPIC: &str = "__diskless_wal_index";
    const MAX_WAL_OBJECT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    let mut admin = connect_admin(bootstrap, security.clone()).await?;
    let index_metadata = admin
        .metadata(&[TOPIC])
        .await
        .map_err(|error| BackupError::Cluster(format!("diskless WAL index Metadata: {error}")))?;
    if !diskless_index_present(&index_metadata, TOPIC)? {
        return Ok(None);
    }
    let metadata = admin
        .metadata(&[])
        .await
        .map_err(|error| BackupError::Cluster(format!("Metadata: {error}")))?;
    let topic_names = metadata
        .topics
        .iter()
        .filter(|topic| topic.name != TOPIC)
        .map(|topic| topic.name.as_str())
        .collect::<Vec<_>>();
    let resources = topic_names
        .iter()
        .map(|name| ConfigResource::topic(*name))
        .collect::<Vec<_>>();
    let configs = admin
        .describe_configs(&resources, DescribeConfigsOptions::default())
        .await
        .map_err(|error| BackupError::Cluster(format!("DescribeConfigs: {error}")))?
        .into_iter()
        .map(|(resource, config)| {
            // The client used to fail the whole call on a resource error, as
            // this `Broker` error; keep that text.
            let config = config.map_err(|error| {
                let error = AdminError::Broker {
                    api: "DescribeConfigs",
                    code: error.code,
                    name: error.name,
                    message: error.message,
                };
                BackupError::Cluster(format!("DescribeConfigs: {error}"))
            })?;
            let overrides = config.dynamic_overrides(&resource);
            Ok((resource.name, overrides))
        })
        .collect::<Result<BTreeMap<_, _>, BackupError>>()?;
    let topics = diskless_topic_topology(&metadata, &configs, TOPIC)?;
    let mut config = KafkaMetadataLogConfig::new(bootstrap);
    TOPIC.clone_into(&mut config.topic);
    "krabka-backup-diskless-capture".clone_into(&mut config.client_id);
    config.security = security;
    // Capture only reads. The broker owns provisioning and compaction policy,
    // and Kafka refuses a client Produce to an internal topic, so the log
    // opens without a producer and needs only READ and DESCRIBE on the index.
    let log = KafkaMetadataEventLog::open_read_only(config)
        .await
        .map_err(|error| BackupError::Cluster(format!("diskless WAL index: {error}")))?;
    let mut capture = capture_projection(log.clone(), &topics, now_ms())
        .await
        .map_err(BackupError::Integrity)?;
    capture.metadata_snapshot_sha256 = metadata_snapshot_sha256;
    capture.rlmm_snapshot_sha256 = rlmm_snapshot_sha256;
    capture.group_offsets_sha256 = group_offsets_sha256;
    log.shutdown().await;
    if let (Some(key_id), Some(key_path)) = (
        signing.worm_signing_key_id.as_ref(),
        signing.worm_signing_key.as_ref(),
    ) {
        let signer = FileEd25519Signer::from_pkcs8_file(key_path, key_id.clone())
            .map_err(|error| BackupError::InvalidArgument(error.to_string()))?;
        let keys = capture
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.clone())
            .collect::<BTreeSet<_>>();
        let mut claims = Vec::with_capacity(keys.len());
        for key in keys {
            let bytes = store
                .get_absolute_capped(&key, MAX_WAL_OBJECT_BYTES)
                .await?;
            claims.push(ObjectEntry {
                suffix: std::path::Path::new(&key)
                    .extension()
                    .map_or_else(String::new, |extension| {
                        format!(".{}", extension.to_string_lossy())
                    }),
                key,
                size_bytes: bytes.len() as u64,
                sha256: Sha256Digest::of(&bytes),
                e_tag: None,
                version_id: None,
                create_precondition: false,
            });
        }
        let head = capture
            .seal(claims, &signer)
            .map_err(BackupError::Integrity)?;
        println!("  diskless-capture WORM head {head}");
    }
    serde_json::to_vec_pretty(&capture)
        .map(Some)
        .map_err(|source| BackupError::Json {
            context: DISKLESS_WAL_INDEX.to_owned(),
            source,
        })
}

fn diskless_topic_topology(
    metadata: &TopicMetadata,
    configs: &BTreeMap<String, BTreeMap<String, String>>,
    index_topic: &str,
) -> Result<std::collections::HashMap<uuid::Uuid, (String, i32)>, BackupError> {
    let mut topics = std::collections::HashMap::new();
    for topic in metadata
        .topics
        .iter()
        .filter(|topic| topic.name != index_topic)
    {
        if let Some(error) = &topic.error {
            return Err(BackupError::Cluster(format!(
                "Metadata for `{}`: code={} ({})",
                topic.name, error.code, error.name
            )));
        }
        let config = configs.get(topic.name.as_str()).ok_or_else(|| {
            BackupError::Cluster(format!(
                "DescribeConfigs omitted metadata topic `{}`",
                topic.name
            ))
        })?;
        if config
            .get("krabka.diskless")
            .is_none_or(|value| value != "true")
        {
            continue;
        }
        let topic_id = topic.topic_id.ok_or_else(|| {
            BackupError::Cluster(format!("diskless topic `{}` has no topic id", topic.name))
        })?;
        topics.insert(
            uuid::Uuid::from_bytes(topic_id.into_bytes()),
            (topic.name.clone(), topic.partition_count),
        );
    }
    Ok(topics)
}

fn diskless_index_present(metadata: &TopicMetadata, name: &str) -> Result<bool, BackupError> {
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name == name)
        .ok_or_else(|| {
            BackupError::Cluster(format!(
                "diskless WAL index Metadata omitted requested topic `{name}`"
            ))
        })?;
    match &topic.error {
        None => Ok(true),
        Some(error) if error.code == 3 => Ok(false),
        Some(error) => Err(BackupError::Cluster(format!(
            "diskless WAL index Metadata: code={} ({}){}",
            error.code,
            error.name,
            error
                .message
                .as_deref()
                .map(|message| format!(" {message:?}"))
                .unwrap_or_default()
        ))),
    }
}

#[cfg(test)]
mod diskless_tests {
    use assert2::check;
    use krabka_client_admin::{KafkaError, TopicMetadataEntry};

    use super::*;

    fn metadata(error: Option<KafkaError>) -> TopicMetadata {
        TopicMetadata {
            topics: vec![TopicMetadataEntry {
                name: "__diskless_wal_index".into(),
                topic_id: None,
                partition_count: 0,
                replication_factor: 0,
                error,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn diskless_index_discovery_distinguishes_absence_from_denial() {
        check!(diskless_index_present(&metadata(None), "__diskless_wal_index").unwrap());
        check!(
            !diskless_index_present(
                &metadata(Some(KafkaError {
                    code: 3,
                    name: "UNKNOWN_TOPIC_OR_PARTITION",
                    message: None,
                })),
                "__diskless_wal_index",
            )
            .unwrap()
        );
        check!(
            diskless_index_present(
                &metadata(Some(KafkaError {
                    code: 29,
                    name: "TOPIC_AUTHORIZATION_FAILED",
                    message: None,
                })),
                "__diskless_wal_index",
            )
            .is_err()
        );
        check!(diskless_index_present(&TopicMetadata::default(), "__diskless_wal_index").is_err());
    }

    #[test]
    fn diskless_topology_keeps_every_partition_including_empty_ones() {
        let metadata = TopicMetadata {
            topics: vec![TopicMetadataEntry {
                name: "orders".into(),
                topic_id: Some(uuid::Uuid::from_u128(7)),
                partition_count: 3,
                replication_factor: 1,
                error: None,
            }],
            ..Default::default()
        };
        let configs = BTreeMap::from([(
            "orders".to_owned(),
            BTreeMap::from([("krabka.diskless".into(), "true".into())]),
        )]);

        check!(
            diskless_topic_topology(&metadata, &configs, "__diskless_wal_index").unwrap()
                == std::collections::HashMap::from([(
                    uuid::Uuid::from_u128(7),
                    ("orders".to_owned(), 3)
                )])
        );
    }
}

/// Put one artifact into the capture and record what was written.
async fn upload(
    store: &Archive,
    id: &str,
    name: &str,
    source: &str,
    bytes: Vec<u8>,
) -> Result<Artifact, BackupError> {
    upload_capped(store, id, name, source, bytes, artifact_byte_limit(name)).await
}

async fn upload_capped(
    store: &Archive,
    id: &str,
    name: &str,
    source: &str,
    bytes: Vec<u8>,
    max_bytes: u64,
) -> Result<Artifact, BackupError> {
    check_capture_size(name, bytes.len() as u64, max_bytes)?;
    let artifact = Artifact {
        name: name.to_owned(),
        source: source.to_owned(),
        size_bytes: bytes.len() as u64,
        sha256: sha256_hex(&bytes),
    };
    store.put_new(&capture_key(id, name), bytes).await?;
    Ok(artifact)
}

/// Print every capture in the archive, newest last.
///
/// # Errors
///
/// Returns the archive's own error when the listing or a manifest read fails.
pub async fn list(archive: &ArchiveArgs) -> Result<Vec<String>, BackupError> {
    let store = Archive::open(archive)?;
    let ids = store.child_directories(CAPTURE_ROOT).await?;
    if ids.is_empty() {
        println!("no captures under {CAPTURE_ROOT}");
    }
    for id in &ids {
        match read_manifest(&store, id).await {
            Ok(manifest) => {
                let names: Vec<&str> = manifest
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.name.as_str())
                    .collect();
                println!("{id}  {}", names.join(" "));
            }
            Err(error) => println!("{id}  UNREADABLE: {error}"),
        }
    }
    Ok(ids.into_iter().collect())
}

/// Re-read a capture and check every artifact against its recorded digest.
///
/// # Errors
///
/// Returns [`BackupError::Integrity`] when an artifact's bytes are not the
/// bytes the manifest recorded, and the archive's own error when the capture
/// cannot be read at all.
pub async fn verify(capture: &str, archive: &ArchiveArgs) -> Result<(), BackupError> {
    let (store, id, manifest) = open_capture(archive, capture).await?;

    let mut problems: Vec<String> = Vec::new();
    for artifact in &manifest.artifacts {
        let bytes = store
            .get_capped(
                &capture_key(&id, &artifact.name),
                artifact_byte_limit(&artifact.name),
            )
            .await?;
        match artifact_problem(artifact, &bytes) {
            Some(problem) => problems.push(problem),
            None => println!("  {} ok ({} bytes)", artifact.name, artifact.size_bytes),
        }
    }
    if problems.is_empty() {
        println!(
            "capture {id} verified: {} artifacts",
            manifest.artifacts.len()
        );
        return Ok(());
    }
    Err(BackupError::Integrity(format!(
        "capture {id}: {}",
        problems.join("; ")
    )))
}

/// Commit a capture's group offsets into a restored cluster.
///
/// # Errors
///
/// Returns [`BackupError::Integrity`] when the captured offsets do not match
/// their recorded digest, [`BackupError::Cluster`] when the cluster refuses a
/// commit, and the archive's own error when the capture cannot be read.
pub async fn restore_offsets(
    capture: &str,
    bootstrap_server: &str,
    dry_run: bool,
    archive: &ArchiveArgs,
) -> Result<usize, BackupError> {
    restore_offsets_secured(
        capture,
        bootstrap_server,
        None,
        &CaptureTrustArgs::default(),
        dry_run,
        archive,
    )
    .await
}

/// Load command-config security and restore one capture's group offsets.
///
/// # Errors
///
/// Returns the same errors as [`restore_offsets`], plus command-config read or
/// parse failures.
pub async fn restore_offsets_configured(
    capture: &str,
    bootstrap_server: &str,
    security: &SecurityArgs,
    trust: &CaptureTrustArgs,
    dry_run: bool,
    archive: &ArchiveArgs,
) -> Result<usize, BackupError> {
    restore_offsets_secured(
        capture,
        bootstrap_server,
        security.load().await?,
        trust,
        dry_run,
        archive,
    )
    .await
}

async fn restore_offsets_secured(
    capture: &str,
    bootstrap_server: &str,
    security: Option<ClientSecurity>,
    trust: &CaptureTrustArgs,
    dry_run: bool,
    archive: &ArchiveArgs,
) -> Result<usize, BackupError> {
    let (store, id, manifest) = open_capture(archive, capture).await?;
    let recorded = manifest.artifact(GROUP_OFFSETS).ok_or_else(|| {
        BackupError::NoSuchCapture(format!("capture {id} holds no {GROUP_OFFSETS}"))
    })?;
    let bytes = store
        .get_capped(&capture_key(&id, GROUP_OFFSETS), MAX_GROUP_OFFSETS_BYTES)
        .await?;
    if let Some(problem) = artifact_problem(recorded, &bytes) {
        return Err(BackupError::Integrity(format!("capture {id}: {problem}")));
    }
    verify_offsets_boundary(&store, &id, &manifest, &bytes, trust).await?;
    let offsets = GroupOffsetsFile::from_slice(&bytes, &capture_key(&id, GROUP_OFFSETS))?;

    if dry_run {
        for group in &offsets.groups {
            for offset in &group.offsets {
                println!(
                    "would commit {}: {}-{} = {}",
                    group.group, offset.topic, offset.partition, offset.offset
                );
            }
        }
        return Ok(offsets.offset_count());
    }

    let topic_ids = topic_ids(bootstrap_server, &offsets, security.clone()).await?;
    let client = Client::builder()
        .bootstrap(bootstrap_server)
        .client_id("krabka-backup")
        .maybe_security(security)
        .build()
        .await
        .map_err(|error| {
            BackupError::Cluster(format!("cannot reach {bootstrap_server}: {error}"))
        })?;

    let mut committed = 0;
    for group in &offsets.groups {
        if group.offsets.is_empty() {
            continue;
        }
        let found = client
            .send(build_find_coordinator(
                &group.group,
                CoordinatorKeyType::Group,
            ))
            .await
            .map_err(|error| {
                BackupError::Cluster(format!("FindCoordinator for {}: {error}", group.group))
            })?;
        let coordinator = coordinator_endpoint(&group.group, found).map_err(|error| {
            BackupError::Cluster(format!("FindCoordinator for {}: {error}", group.group))
        })?;
        let response = client
            .broker(coordinator.node_id)
            .send(commit_request(group, &topic_ids))
            .await
            .map_err(|error| {
                BackupError::Cluster(format!("OffsetCommit for {}: {error}", group.group))
            })?;
        let refusals = commit_refusals(&response);
        if !refusals.is_empty() {
            return Err(BackupError::Cluster(format!(
                "OffsetCommit for {}: {}",
                group.group,
                refusals.join("; ")
            )));
        }
        committed += group.offsets.len();
        println!("{}: {} offsets committed", group.group, group.offsets.len());
    }
    Ok(committed)
}

async fn verify_offsets_boundary(
    store: &Archive,
    id: &str,
    manifest: &Manifest,
    offsets: &[u8],
    trust: &CaptureTrustArgs,
) -> Result<(), BackupError> {
    let configured = trust.worm_key_id.is_some()
        || trust.worm_public_key.is_some()
        || trust.worm_expect_head.is_some();
    let Some(recorded) = manifest.artifact(DISKLESS_WAL_INDEX) else {
        if configured {
            return Err(BackupError::Integrity(format!(
                "capture {id} holds no signed diskless boundary for group offsets"
            )));
        }
        return Ok(());
    };
    let bytes = store
        .get_capped(
            &capture_key(id, DISKLESS_WAL_INDEX),
            MAX_DISKLESS_INDEX_BYTES,
        )
        .await?;
    if let Some(problem) = artifact_problem(recorded, &bytes) {
        return Err(BackupError::Integrity(format!("capture {id}: {problem}")));
    }
    let capture = krabka_remote_storage::diskless::DisklessWalCapture::from_slice(&bytes)
        .map_err(BackupError::Integrity)?;
    if capture.authentication.is_none() && !configured {
        return Ok(());
    }
    let key_id = trust.worm_key_id.as_ref().ok_or_else(|| {
        BackupError::Integrity("signed offset restore requires --worm-key-id".to_owned())
    })?;
    let key_path = trust.worm_public_key.as_ref().ok_or_else(|| {
        BackupError::Integrity("signed offset restore requires --worm-public-key".to_owned())
    })?;
    let expected_head = trust.worm_expect_head.as_deref().ok_or_else(|| {
        BackupError::Integrity("signed offset restore requires --worm-expect-head".to_owned())
    })?;
    let public_key = tokio::fs::read(key_path).await?;
    let trusted = krabka_remote_storage::TrustedManifestKeys::single(key_id.clone(), public_key);
    capture
        .authenticate(&trusted, Some(expected_head))
        .map_err(BackupError::Integrity)?;
    if capture.group_offsets_sha256 != Some(Sha256Digest::of(offsets)) {
        return Err(BackupError::Integrity(
            "group offsets do not match the signed diskless capture boundary".to_owned(),
        ));
    }
    Ok(())
}

/// The topic id the restored cluster gave each topic the capture names.
///
/// `OffsetCommit` v10 puts the topic id on the wire instead of the name, so a
/// commit that carried names only would silently commit nothing there.
async fn topic_ids(
    bootstrap_server: &str,
    offsets: &GroupOffsetsFile,
    security: Option<ClientSecurity>,
) -> Result<BTreeMap<String, WireUuid>, BackupError> {
    let names: BTreeSet<&str> = offsets
        .groups
        .iter()
        .flat_map(|group| group.offsets.iter().map(|offset| offset.topic.as_str()))
        .collect();
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut admin = connect_admin(bootstrap_server, security).await?;
    let wanted: Vec<&str> = names.into_iter().collect();
    let metadata = admin
        .metadata(&wanted)
        .await
        .map_err(|error| BackupError::Cluster(format!("Metadata: {error}")))?;
    Ok(metadata
        .topics
        .into_iter()
        .filter_map(|topic| {
            topic
                .topic_id
                .map(|id| (topic.name, WireUuid(id.into_bytes())))
        })
        .collect())
}

/// Read every group's committed offsets from a live cluster.
async fn fetch_group_offsets(
    bootstrap_server: &str,
    security: Option<ClientSecurity>,
) -> Result<GroupOffsetsFile, BackupError> {
    let mut admin = connect_admin(bootstrap_server, security).await?;
    let groups = admin
        .list_groups(&ListGroupsOptions::default())
        .await
        .map_err(|error| BackupError::Cluster(format!("ListGroups: {error}")))?
        .all()
        .map_err(|failure| {
            BackupError::Cluster(format!(
                "ListGroups on broker {}: {} ({})",
                failure.node_id, failure.error.name, failure.error.code
            ))
        })?;
    let mut captured = GroupOffsetsFile::default();
    for group in groups.into_iter().map(|listing| listing.group_id) {
        let fetched = admin
            .list_consumer_group_offsets(&group)
            .await
            .map_err(|error| BackupError::Cluster(format!("OffsetFetch for {group}: {error}")))?;
        if fetched.is_empty() {
            continue;
        }
        captured.groups.push(group_offsets(&group, &fetched));
    }
    Ok(captured)
}

async fn connect_admin(
    bootstrap_server: &str,
    security: Option<ClientSecurity>,
) -> Result<AdminClient, BackupError> {
    AdminClient::connect_secured(&[bootstrap_server.to_owned()], security)
        .await
        .map_err(|error| BackupError::Cluster(format!("cannot reach {bootstrap_server}: {error}")))
}

/// Resolve a capture selector and load its manifest from the opened archive.
async fn open_capture(
    archive: &ArchiveArgs,
    capture: &str,
) -> Result<(Archive, String, Manifest), BackupError> {
    let store = Archive::open(archive)?;
    let id = resolve_capture(&store, capture).await?;
    let manifest = read_manifest(&store, &id).await?;
    Ok((store, id, manifest))
}

/// Select the newest committed capture for `latest`, or check that a named directory exists.
async fn resolve_capture(store: &Archive, capture: &str) -> Result<String, BackupError> {
    let ids = store.child_directories(CAPTURE_ROOT).await?;
    if capture == LATEST {
        for id in ids.into_iter().rev() {
            match read_manifest(store, &id).await {
                Ok(_) => return Ok(id),
                // Artifacts can outlive an interrupted capture; its manifest
                // is published last and commits the completed capture.
                Err(BackupError::ObjectStore(krabka_object_store::ObjectStoreError::NotFound(
                    _,
                ))) => {}
                Err(error) => return Err(error),
            }
        }
        return Err(BackupError::NoSuchCapture(format!(
            "the archive holds no completed capture under {CAPTURE_ROOT}"
        )));
    }
    if ids.contains(capture) {
        return Ok(capture.to_owned());
    }
    Err(BackupError::NoSuchCapture(format!(
        "the archive holds no capture {capture}"
    )))
}

async fn read_manifest(store: &Archive, id: &str) -> Result<Manifest, BackupError> {
    let key = capture_key(id, MANIFEST);
    let bytes = store.get_capped(&key, MAX_MANIFEST_BYTES).await?;
    Manifest::from_slice(&bytes, &key)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{
        BackupError, DISKLESS_WAL_INDEX, FileEd25519Signer, GROUP_OFFSETS, MANIFEST,
        METADATA_CHECKPOINT, RLMM_SNAPSHOT, Sha256Digest, capture, list, restore_offsets, verify,
        verify_offsets_boundary,
    };
    use crate::{
        archive::{Archive, ArchiveArgs},
        capture::capture_key,
        cli::CaptureTrustArgs,
        manifest::{Artifact, MANIFEST_VERSION, Manifest, sha256_hex},
        offsets::{CommittedOffset, GROUP_OFFSETS_VERSION, GroupOffsets, GroupOffsetsFile},
    };

    /// The RLMM snapshot bytes a fixture node holds. The capture copies bytes
    /// and decodes nothing, so any content proves the same thing.
    const RLMM_BYTES: &[u8] = b"rlmm snapshot bytes";

    /// The newest checkpoint's bytes.
    const CHECKPOINT_BYTES: &[u8] = b"the newest controller checkpoint";

    fn archive_args(root: &std::path::Path) -> ArchiveArgs {
        ArchiveArgs {
            local: Some(root.to_path_buf()),
            ..ArchiveArgs::default()
        }
    }

    krabka_macros::snapshot_node_fixture!(node_with_rlmm);

    #[test]
    fn capture_size_limits_match_readers_at_the_boundary() {
        for (name, expected) in [
            (MANIFEST, 8 * 1024 * 1024),
            (GROUP_OFFSETS, 64 * 1024 * 1024),
            (METADATA_CHECKPOINT, 512 * 1024 * 1024),
            (RLMM_SNAPSHOT, 512 * 1024 * 1024),
            (DISKLESS_WAL_INDEX, 256 * 1024 * 1024),
        ] {
            let limit = super::artifact_byte_limit(name);
            check!(limit == expected);
            check!(super::check_capture_size(name, limit, limit).is_ok());
            check!(super::check_capture_size(name, limit + 1, limit).is_err());
            check!(super::check_capture_size(name, u64::MAX, limit).is_err());
        }
    }

    #[tokio::test]
    async fn capture_upload_caps_accept_equality_and_reject_overflow_before_put() {
        for name in [
            RLMM_SNAPSHOT,
            METADATA_CHECKPOINT,
            GROUP_OFFSETS,
            DISKLESS_WAL_INDEX,
        ] {
            for size in [8, 9] {
                let (_root, _args, store) = archive_fixture();
                let id = "0000000000000042";
                // Earlier artifacts may remain orphaned after a later failure.
                store
                    .put_new(&capture_key(id, "earlier"), b"valid".to_vec())
                    .await
                    .unwrap();
                let result =
                    super::upload_capped(&store, id, name, "node", vec![b'x'; size], 8).await;
                if size == 8 {
                    check!(
                        result.unwrap()
                            == Artifact {
                                name: name.to_owned(),
                                source: "node".to_owned(),
                                size_bytes: 8,
                                sha256: sha256_hex(&[b'x'; 8]),
                            }
                    );
                    check!(store.get(&capture_key(id, name)).await.unwrap() == vec![b'x'; size]);
                } else {
                    let error = result.unwrap_err();
                    check!(
                        matches!(&error, BackupError::Io(error) if error.kind() == std::io::ErrorKind::InvalidData)
                    );
                    check!(error.to_string().contains(name));
                    check!(matches!(
                        store.get(&capture_key(id, name)).await,
                        Err(BackupError::ObjectStore(
                            krabka_object_store::ObjectStoreError::NotFound(_)
                        ))
                    ));
                    check!(matches!(
                        store.get(&capture_key(id, MANIFEST)).await,
                        Err(BackupError::ObjectStore(
                            krabka_object_store::ObjectStoreError::NotFound(_)
                        ))
                    ));
                    check!(store.get(&capture_key(id, "earlier")).await.unwrap() == b"valid");
                }
            }
        }
    }

    #[tokio::test]
    async fn capture_rejects_oversized_local_snapshots_without_committing() {
        for name in [RLMM_SNAPSHOT, METADATA_CHECKPOINT] {
            let node = node_with_both_inputs();
            let path = if name == RLMM_SNAPSHOT {
                node.path().join(super::RLMM_SNAPSHOT_RELATIVE)
            } else {
                super::newest_metadata_checkpoint(node.path()).unwrap()
            };
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(super::MAX_SNAPSHOT_BYTES + 1)
                .unwrap();
            let (_root, args, store) = archive_fixture();
            let error = capture(Some(node.path()), None, &args).await.unwrap_err();
            check!(error.to_string().contains(name));
            let ids = store.child_directories(super::CAPTURE_ROOT).await.unwrap();
            check!(ids.len() == usize::from(name == METADATA_CHECKPOINT));
            for id in ids {
                check!(store.get(&capture_key(&id, RLMM_SNAPSHOT)).await.unwrap() == RLMM_BYTES);
                for absent in [name, MANIFEST] {
                    check!(matches!(
                        store.get(&capture_key(&id, absent)).await,
                        Err(BackupError::ObjectStore(
                            krabka_object_store::ObjectStoreError::NotFound(_)
                        ))
                    ));
                }
            }
        }
    }

    #[tokio::test]
    async fn concurrent_captures_at_the_same_timestamp_keep_both_nodes_bytes() {
        let first = node_with_rlmm();
        let second = node_with_rlmm();
        let snapshot = second.path().join(super::RLMM_SNAPSHOT_RELATIVE);
        std::fs::write(snapshot, b"second node snapshot").unwrap();
        let (_root, archive, store) = archive_fixture();
        let signing = super::CaptureSigningArgs::default();
        let (left, right) = tokio::join!(
            super::capture_secured_at(Some(first.path()), None, None, &signing, &archive, 42),
            super::capture_secured_at(Some(second.path()), None, None, &signing, &archive, 42),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        check!(left != right);
        check!(list(&archive).await.unwrap().len() == 2);
        for (id, expected) in [
            (&left, RLMM_BYTES),
            (&right, b"second node snapshot".as_slice()),
        ] {
            verify(id, &archive).await.unwrap();
            check!(store.get(&capture_key(id, RLMM_SNAPSHOT)).await.unwrap() == expected);
            check!(capture_manifest(&store, id).await.captured_at_ms == 42);
        }
    }

    #[tokio::test]
    async fn latest_ignores_newer_capture_artifacts_without_a_committed_manifest() {
        let (_root, args, store) = archive_fixture();
        let complete = "0000000000000001";
        let orphan = "0000000000000002-0123456789abcdef0123456789abcdef";
        write_offsets_capture(&store, complete, b"offsets", b"offsets").await;
        store
            .put(&capture_key(orphan, GROUP_OFFSETS), b"orphan".to_vec())
            .await
            .unwrap();
        check!(super::resolve_capture(&store, "latest").await.unwrap() == complete);
        check!(verify("latest", &args).await.is_ok());
        check!(super::resolve_capture(&store, orphan).await.unwrap() == orphan);
        check!(matches!(
            verify(orphan, &args).await,
            Err(BackupError::ObjectStore(
                krabka_object_store::ObjectStoreError::NotFound(_)
            ))
        ));
    }

    #[tokio::test]
    async fn document_reads_reject_independent_limits_before_buffering() {
        let (root, archive, store) = archive_fixture();
        let id = "0000000000000001";
        let key = capture_key(id, MANIFEST);
        store.put(&key, b"{}".to_vec()).await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(root.path().join(&key))
            .unwrap()
            .set_len(super::MAX_MANIFEST_BYTES + 1)
            .unwrap();
        let error = super::read_manifest(&store, id).await.unwrap_err();
        check!(
            matches!(error, BackupError::ObjectStore(krabka_object_store::ObjectStoreError::TooLarge { max_bytes, .. }) if max_bytes == super::MAX_MANIFEST_BYTES)
        );

        write_offsets_capture(&store, id, b"offsets", b"offsets").await;
        let key = capture_key(id, GROUP_OFFSETS);
        std::fs::OpenOptions::new()
            .write(true)
            .open(root.path().join(&key))
            .unwrap()
            .set_len(super::MAX_GROUP_OFFSETS_BYTES + 1)
            .unwrap();
        let error = verify(id, &archive).await.unwrap_err();
        check!(
            matches!(error, BackupError::ObjectStore(krabka_object_store::ObjectStoreError::TooLarge { max_bytes, .. }) if max_bytes == super::MAX_GROUP_OFFSETS_BYTES)
        );
    }

    fn archive_fixture() -> (tempfile::TempDir, ArchiveArgs, Archive) {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = Archive::open(&args).expect("open the archive");
        (archive_root, args, store)
    }

    async fn captured_node() -> (tempfile::TempDir, tempfile::TempDir, ArchiveArgs, String) {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let id = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");
        (node, archive_root, args, id)
    }

    async fn capture_manifest(store: &Archive, id: &str) -> Manifest {
        serde_json::from_slice(
            &store
                .get(&capture_key(id, MANIFEST))
                .await
                .expect("read the manifest"),
        )
        .expect("decode the manifest")
    }

    fn check_unreachable(error: &BackupError, address: &str) {
        assert!(let BackupError::Cluster(_) = error, "got: {error}");
        check!(error.to_string().contains(address), "got: {error}");
    }

    /// A log directory holding both of the files a capture takes off a node.
    fn node_with_both_inputs() -> tempfile::TempDir {
        let log_dir = node_with_rlmm();
        write_checkpoint(log_dir.path());
        log_dir
    }

    /// The newest controller checkpoint, in the directory a controller writes.
    fn write_checkpoint(log_dir: &std::path::Path) {
        let metadata = log_dir.join("__cluster_metadata-0");
        std::fs::create_dir_all(&metadata).expect("create the metadata dir");
        std::fs::write(
            metadata.join("00000000000000000042-0000000001.checkpoint"),
            CHECKPOINT_BYTES,
        )
        .expect("write the newest checkpoint");
    }

    /// An address on the loopback interface that nothing is listening on, so
    /// a connection attempt is refused rather than timing out.
    fn closed_address() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe listener");
        let port = listener.local_addr().expect("the probe's address").port();
        drop(listener);
        format!("127.0.0.1:{port}")
    }

    /// Write a capture that no [`capture`] call produced: a manifest naming one
    /// group-offsets artifact, and whatever bytes the caller wants under it.
    /// It is how the read paths are driven over inputs a healthy capture cannot
    /// leave behind.
    async fn write_offsets_capture(store: &Archive, id: &str, bytes: &[u8], recorded: &[u8]) {
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            capture_id: id.to_owned(),
            captured_at_ms: 1_700_000_000_000,
            log_dir: None,
            bootstrap_server: Some("broker-1:9092".to_owned()),
            artifacts: vec![Artifact {
                name: GROUP_OFFSETS.to_owned(),
                source: "broker-1:9092".to_owned(),
                size_bytes: recorded.len() as u64,
                sha256: sha256_hex(recorded),
            }],
        };
        store
            .put(
                &capture_key(id, MANIFEST),
                serde_json::to_vec(&manifest).expect("encode the manifest"),
            )
            .await
            .expect("write the manifest");
        store
            .put(&capture_key(id, GROUP_OFFSETS), bytes.to_vec())
            .await
            .expect("write the offsets");
    }

    /// One group with one committed offset, as JSON bytes.
    fn offsets_json() -> Vec<u8> {
        serde_json::to_vec(&GroupOffsetsFile {
            version: GROUP_OFFSETS_VERSION,
            groups: vec![GroupOffsets {
                group: "analytics".to_owned(),
                offsets: vec![CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 0,
                    offset: 42,
                }],
            }],
        })
        .expect("encode the offsets")
    }

    #[tokio::test]
    async fn signed_capture_boundary_rejects_forged_group_offsets() {
        const PKCS8: &[u8] = &[
            0x30, 0x53, 0x02, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20, 0x10, 0xb4, 0x98, 0x61, 0xa2, 0x6e, 0xcb, 0x9b, 0x94, 0x33, 0x41, 0xa3,
            0x7f, 0xb7, 0x7d, 0x80, 0x95, 0x6a, 0xa7, 0xda, 0x95, 0x25, 0x62, 0xc6, 0x9b, 0x61,
            0xfd, 0xb6, 0x63, 0x77, 0xe5, 0xba, 0xa1, 0x23, 0x03, 0x21, 0x00, 0x13, 0x57, 0x2a,
            0x6e, 0xa1, 0xd1, 0xe1, 0x9d, 0x61, 0x77, 0x0e, 0xcb, 0xe3, 0x12, 0x91, 0xe9, 0xac,
            0x86, 0x0f, 0x0b, 0x93, 0xf5, 0x60, 0x09, 0xfe, 0xa8, 0xcc, 0xc9, 0x0e, 0xfe, 0x2a,
            0x87,
        ];
        let offsets = offsets_json();
        let signer = FileEd25519Signer::from_pkcs8_bytes(PKCS8, "capture-key".into()).unwrap();
        let mut capture = krabka_remote_storage::diskless::DisklessWalCapture {
            format_version: krabka_remote_storage::diskless::DisklessWalCapture::FORMAT_VERSION,
            captured_at_ms: 42,
            source_cutoffs: Vec::new(),
            partitions: Vec::new(),
            metadata_snapshot_sha256: None,
            rlmm_snapshot_sha256: None,
            group_offsets_sha256: Some(Sha256Digest::of(&offsets)),
            authentication: None,
        };
        let head = capture.seal(Vec::new(), &signer).unwrap();
        let capture_bytes = serde_json::to_vec(&capture).unwrap();
        let root = tempfile::tempdir().unwrap();
        let public_key = root.path().join("capture.pub");
        std::fs::write(&public_key, signer.public_key()).unwrap();
        let archive = Archive::open(&archive_args(root.path())).unwrap();
        let id = "0000000000000042";
        archive
            .put(&capture_key(id, DISKLESS_WAL_INDEX), capture_bytes.clone())
            .await
            .unwrap();
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            capture_id: id.into(),
            captured_at_ms: 42,
            log_dir: None,
            bootstrap_server: Some("broker:9092".into()),
            artifacts: vec![
                Artifact {
                    name: GROUP_OFFSETS.into(),
                    source: "broker:9092".into(),
                    size_bytes: offsets.len() as u64,
                    sha256: sha256_hex(&offsets),
                },
                Artifact {
                    name: DISKLESS_WAL_INDEX.into(),
                    source: "broker:9092".into(),
                    size_bytes: capture_bytes.len() as u64,
                    sha256: sha256_hex(&capture_bytes),
                },
            ],
        };
        let trust = CaptureTrustArgs {
            worm_key_id: Some("capture-key".into()),
            worm_public_key: Some(public_key),
            worm_expect_head: Some(head.to_string()),
        };

        verify_offsets_boundary(&archive, id, &manifest, &offsets, &trust)
            .await
            .expect("authentic offsets");
        check!(
            verify_offsets_boundary(&archive, id, &manifest, b"forged", &trust)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_records_both_snapshots_with_their_digests() {
        let (_node, _archive_root, args, id) = captured_node().await;

        let store = Archive::open(&args).expect("open the archive");
        let manifest = capture_manifest(&store, &id).await;
        check!(manifest.capture_id == id);
        check!(manifest.bootstrap_server == None);
        check!(
            manifest.artifact(RLMM_SNAPSHOT).map(|a| a.sha256.clone())
                == Some(sha256_hex(RLMM_BYTES))
        );
        check!(
            manifest
                .artifact(METADATA_CHECKPOINT)
                .map(|a| a.sha256.clone())
                == Some(sha256_hex(CHECKPOINT_BYTES))
        );
        check!(manifest.artifact(GROUP_OFFSETS) == None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_reports_each_input_it_did_not_find_and_takes_the_other() {
        let log_dir = tempfile::tempdir().expect("log dir");
        write_checkpoint(log_dir.path());
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let id = capture(Some(log_dir.path()), None, &args)
            .await
            .expect("a capture that found one input succeeds");

        let store = Archive::open(&args).expect("open the archive");
        let manifest = capture_manifest(&store, &id).await;
        check!(manifest.artifact(RLMM_SNAPSHOT) == None);
        check!(manifest.artifact(METADATA_CHECKPOINT).is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_finds_nothing_names_the_flags_that_would_have_helped() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let message = capture(None, None, &args)
            .await
            .expect_err("a capture with no source fails")
            .to_string();
        check!(message.contains("pass --log-dir"), "got: {message}");

        let empty = tempfile::tempdir().expect("an empty log dir");
        let message = capture(Some(empty.path()), None, &args)
            .await
            .expect_err("a capture that found nothing fails")
            .to_string();
        check!(message.contains("is absent"), "got: {message}");
        check!(message.contains("checkpoint"), "got: {message}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_snapshot_is_reported_rather_than_treated_as_absent() {
        let log_dir = tempfile::tempdir().expect("log dir");
        // A directory where the snapshot file belongs: the read fails with
        // something other than `NotFound`, which is not "the node has none".
        std::fs::create_dir_all(log_dir.path().join("remote-log-metadata/snapshot"))
            .expect("create a directory in the snapshot's place");
        let archive_root = tempfile::tempdir().expect("archive root");

        let error = capture(
            Some(log_dir.path()),
            None,
            &archive_args(archive_root.path()),
        )
        .await
        .expect_err("an unreadable snapshot fails the capture");
        assert!(let BackupError::Io(_) = &error, "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_of_group_offsets_reports_the_cluster_it_could_not_reach() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let address = closed_address();

        let error = capture(None, Some(&address), &archive_args(archive_root.path()))
            .await
            .expect_err("a capture cannot read offsets from a cluster that is not there");
        check_unreachable(&error, &address);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_names_every_capture_and_says_so_when_there_are_none() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        check!(list(&args).await.expect("list an empty archive").is_empty());

        let node = node_with_both_inputs();
        let first = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");
        // A second capture id that sorts after the first, written by hand so
        // the two do not depend on the clock ticking between them.
        let store = Archive::open(&args).expect("open the archive");
        let second = "9999999999999999";
        write_offsets_capture(&store, second, &offsets_json(), &offsets_json()).await;

        check!(list(&args).await.expect("list the captures") == vec![first, second.to_owned()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_reports_a_capture_whose_manifest_cannot_be_read_and_keeps_going() {
        let (_archive_root, args, store) = archive_fixture();
        store
            .put(&capture_key("0000000000000001", MANIFEST), b"{".to_vec())
            .await
            .expect("write a truncated manifest");

        check!(
            list(&args)
                .await
                .expect("an unreadable manifest is listed, not fatal")
                == vec!["0000000000000001".to_owned()]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verify_accepts_a_capture_it_just_wrote_under_either_selector() {
        let (_node, _archive_root, args, id) = captured_node().await;

        check!(verify("latest", &args).await.is_ok());
        check!(verify(&id, &args).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verify_reports_an_artifact_the_archive_no_longer_holds_whole() {
        let (_node, archive_root, args, id) = captured_node().await;

        // What a half-finished upload leaves behind, and exactly what a
        // restore must not be handed.
        std::fs::write(
            archive_root.path().join(capture_key(&id, RLMM_SNAPSHOT)),
            b"rlmm snapshot byt",
        )
        .expect("truncate the captured snapshot");

        let error = verify("latest", &args)
            .await
            .expect_err("a truncated artifact fails verification");
        assert!(let BackupError::Integrity(_) = &error, "got: {error}");
        check!(error.to_string().contains(RLMM_SNAPSHOT), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_is_not_in_the_archive_is_named_rather_than_guessed_at() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let error = verify("latest", &args)
            .await
            .expect_err("an empty archive has no newest capture");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");

        let node = node_with_both_inputs();
        capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");
        let error = verify("0000000000000007", &args)
            .await
            .expect_err("a capture the archive does not hold");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");
        check!(
            error.to_string().contains("0000000000000007"),
            "got: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dry_run_reports_every_offset_and_commits_nothing() {
        let (_archive_root, args, store) = archive_fixture();
        let offsets = offsets_json();
        write_offsets_capture(&store, "0000000000000001", &offsets, &offsets).await;

        // The cluster address is one nothing is listening on: a dry run must
        // not reach for it at all.
        check!(
            restore_offsets("latest", &closed_address(), true, &args)
                .await
                .expect("a dry run reads the capture alone")
                == 1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_without_offsets_cannot_restore_them() {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("a capture of the two files holds no offsets");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");
        check!(error.to_string().contains(GROUP_OFFSETS), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offsets_that_do_not_match_their_digest_are_never_committed() {
        let (_archive_root, args, store) = archive_fixture();
        write_offsets_capture(&store, "0000000000000001", b"{}", &offsets_json()).await;

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("offsets that do not match the manifest are an integrity failure");
        assert!(let BackupError::Integrity(_) = &error, "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offsets_that_are_not_the_captured_shape_name_the_object_that_holds_them() {
        let (_archive_root, args, store) = archive_fixture();
        let bytes = b"[]";
        write_offsets_capture(&store, "0000000000000001", bytes, bytes).await;

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("a JSON array is not the captured shape");
        assert!(let BackupError::Json { .. } = &error, "got: {error}");
        check!(error.to_string().contains(GROUP_OFFSETS), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offsets_or_a_manifest_from_before_1_0_are_refused_as_an_unsupported_version() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = Archive::open(&args).expect("open the archive");
        let offsets = br#"{"groups":[]}"#;
        write_offsets_capture(&store, "0000000000000001", offsets, offsets).await;

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("an offsets file with no version is refused");
        check!(
            let BackupError::UnsupportedVersion { found: None, expected: GROUP_OFFSETS_VERSION, .. } = &error,
            "got: {error}"
        );
        check!(error.to_string().contains(GROUP_OFFSETS), "got: {error}");
        check!(
            error.to_string().contains("predates krabka 1.0"),
            "got: {error}"
        );

        let mut manifest: serde_json::Value = serde_json::from_slice(
            &store
                .get(&capture_key("0000000000000001", MANIFEST))
                .await
                .expect("read the manifest"),
        )
        .expect("decode the manifest");
        manifest["version"] = serde_json::json!(MANIFEST_VERSION + 1);
        store
            .put(
                &capture_key("0000000000000001", MANIFEST),
                serde_json::to_vec(&manifest).expect("encode the manifest"),
            )
            .await
            .expect("rewrite the manifest");

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("a manifest of a later version is refused");
        assert2::assert!(
            let BackupError::UnsupportedVersion {
                found: Some(found),
                expected: MANIFEST_VERSION,
                ..
            } = &error,
            "got: {error}"
        );
        check!(found == "1");
        check!(error.to_string().contains(MANIFEST), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_reports_the_cluster_it_could_not_reach() {
        let (_archive_root, args, store) = archive_fixture();
        let offsets = offsets_json();
        write_offsets_capture(&store, "0000000000000001", &offsets, &offsets).await;
        let address = closed_address();

        // The topic ids come first, so this is the Metadata lookup failing.
        let error = restore_offsets("latest", &address, false, &args)
            .await
            .expect_err("a commit cannot reach a cluster that is not there");
        check_unreachable(&error, &address);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_holds_no_offsets_commits_nothing_and_asks_the_cluster_nothing() {
        let (_archive_root, args, store) = archive_fixture();
        let empty = serde_json::to_vec(&GroupOffsetsFile::default()).expect("encode the offsets");
        write_offsets_capture(&store, "0000000000000001", &empty, &empty).await;

        // A capture taken while no group had committed anything names no
        // topic, so there is no metadata to look up and no commit to send:
        // the restore succeeds having asked the cluster for nothing, which is
        // why the address below is one nothing is listening on.
        check!(
            restore_offsets("latest", &closed_address(), false, &args)
                .await
                .expect("an empty capture restores nothing")
                == 0
        );
    }
}
