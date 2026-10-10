//! The files a format leaves in a log directory.
//!
//! Every formatted directory holds Kafka's `meta.properties`. The metadata log
//! directory also holds the bootstrap manifest with its binary record stream,
//! and for a dynamic KIP-853 format the offset-zero metadata checkpoint. Each
//! writer serializes records the run has already resolved, so the encoding and
//! the I/O sit together here, apart from the flag handling that decides what
//! goes in them.

use std::{ffi::OsString, io, path::Path};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use krabka_metadata::MetadataRecord;
use serde::Serialize;
use serde_wincode::SerdeCompat;
use wincode::Serialize as _;

use crate::{ids::ClusterId, meta_properties::MetaProperties};

pub(super) const ZERO_CHECKPOINT_NAME: &str = "00000000000000000000-0000000000.checkpoint";

/// The environment variable that makes a run fail after it writes the file of
/// the given name. It is a test seam: it is how the tests interrupt a run
/// partway through, to show that the next run recovers.
pub const FAIL_AFTER_ENV: &str = "KRABKA_FORMAT_FAIL_AFTER";

/// A failure injected through [`FAIL_AFTER_ENV`], or `None` in normal use.
#[derive(Debug, Clone, Default)]
pub(super) struct Fault(Option<OsString>);

impl Fault {
    /// Reads [`FAIL_AFTER_ENV`].
    pub(super) fn from_env() -> Self {
        Self(std::env::var_os(FAIL_AFTER_ENV))
    }

    /// Fails when `path` is the file the fault names.
    fn after(&self, path: &Path) -> Result<(), String> {
        match (&self.0, path.file_name()) {
            (Some(name), Some(written)) if name == written => Err(format!(
                "injected failure after {} ({FAIL_AFTER_ENV})",
                path.display()
            )),
            _ => Ok(()),
        }
    }
}

/// Writes Kafka's `meta.properties`: the marker of a formatted directory, and
/// the file the broker reads its cluster, node, and directory ids from on
/// every boot.
///
/// The file is written under a temporary name, synced, and renamed into
/// place, as Kafka's `PropertiesUtils.writePropertiesFile` does. A run that
/// stops partway therefore never leaves a truncated marker. A failure gets
/// the message of the `FormatterException` that `Formatter.doFormat` throws.
#[tracing::instrument(
    level = "debug",
    name = "cli.write_meta_properties",
    skip_all,
    fields(log_dir = %log_dir.display(), cluster_id = %meta.cluster_id, node_id = meta.node_id),
    err
)]
pub(super) fn write_meta_properties(
    log_dir: &Path,
    meta: &MetaProperties,
    fault: &Fault,
) -> Result<(), String> {
    meta.write_observed(log_dir, |path| fault.after(path).map_err(io::Error::other))
        .map_err(|e| {
            format!(
                "Error while writing meta.properties file {}: {e}",
                log_dir.display()
            )
        })
}

/// The `bootstrap.records.bin` format version this build writes: a
/// big-endian `i16` at the front of the file, before the first record.
///
/// It is part of the 1.x on-disk contract. The broker's reader,
/// `krabka_broker::bootstrap::load_bootstrap_records`, refuses a missing or
/// unknown version, and its `BOOTSTRAP_RECORDS_VERSION` is the same number.
pub(super) const BOOTSTRAP_RECORDS_VERSION: i16 = 0;

/// The `bootstrap.json` format version this build writes: the required
/// top-level `version` field.
///
/// It is part of the 1.x on-disk contract. No reader exists: the broker never
/// reads `bootstrap.json`, which only mirrors `bootstrap.records.bin` for an
/// operator. A reader added later refuses a missing or unknown version. The
/// number is 1 because the field carried 1 before it was named `version`.
pub(super) const BOOTSTRAP_MANIFEST_VERSION: u32 = 1;

/// Human-readable manifest written to `<log_dir>/bootstrap.json`.
#[derive(Debug, Serialize)]
struct BootstrapManifest {
    /// Always [`BOOTSTRAP_MANIFEST_VERSION`].
    version: u32,
    /// Kafka's 22-character base64 form.
    cluster_id: ClusterId,
    record_count: usize,
    /// Base64-encoded `SerdeCompat<MetadataRecord>` payloads, one per
    /// seed record. Mirrors the contents of `bootstrap.records.bin` so
    /// operators can inspect the file without a hex editor.
    records_b64: Vec<String>,
}

/// Write the KIP-630/KIP-853 offset-zero bootstrap checkpoint for a
/// dynamically formatted controller into
/// `<metadata_log_dir>/__cluster_metadata-0/`, where Kafka writes it.
///
/// The checkpoint holds the control state of `control_records`, then
/// `metadata_records` in their own order, as Kafka's
/// `Formatter.writeBoostrapSnapshot` writes them. The active controller
/// writes those records to the metadata log.
pub(super) fn write_dynamic_checkpoint(
    metadata_log_dir: &Path,
    cluster_id: ClusterId,
    control_records: &[MetadataRecord],
    metadata_records: &[MetadataRecord],
    fault: &Fault,
) -> Result<(), String> {
    let mut controls = krabka_metadata::MetadataImage::new(cluster_id.into());
    for record in control_records {
        controls.apply(record);
    }
    let bytes = krabka_raft::serialize_bootstrap_snapshot(
        controls.kraft_version(),
        controls.voters(),
        metadata_records,
        0,
    )
    .map_err(|e| format!("serialize offset-zero checkpoint: {e}"))?;
    let checkpoint_dir = krabka_raft::metadata_partition_dir(metadata_log_dir);
    std::fs::create_dir_all(&checkpoint_dir)
        .map_err(|e| format!("create checkpoint directory: {e}"))?;
    let path = checkpoint_dir.join(ZERO_CHECKPOINT_NAME);
    std::fs::write(&path, bytes).map_err(|e| format!("write offset-zero checkpoint: {e}"))?;
    fault.after(&path)
}

/// Serialize the manifest + records to disk under `log_dir`. Returns the
/// first I/O or encoding error encountered.
#[tracing::instrument(
    level = "debug",
    name = "cli.write_bootstrap_files",
    skip_all,
    fields(log_dir = %log_dir.display(), record_count = records.len()),
    err
)]
pub(super) fn write_bootstrap_files(
    log_dir: &Path,
    cluster_id: ClusterId,
    records: &[MetadataRecord],
    fault: &Fault,
) -> Result<(), String> {
    // 1. Per-record `SerdeCompat<MetadataRecord>` payloads.
    let mut record_blobs: Vec<Vec<u8>> = Vec::with_capacity(records.len());
    for rec in records {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec)
            .map_err(|e| format!("serialize record: {e}"))?;
        record_blobs.push(bytes);
    }

    // 2. Binary stream: the version header, then length-prefixed (u32 LE)
    //    blobs, concatenated.
    let mut bin = BOOTSTRAP_RECORDS_VERSION.to_be_bytes().to_vec();
    for blob in &record_blobs {
        let len: u32 = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bin.extend_from_slice(&len.to_le_bytes());
        bin.extend_from_slice(blob);
    }
    let bin_path = log_dir.join("bootstrap.records.bin");
    std::fs::write(&bin_path, &bin).map_err(|e| format!("write bootstrap.records.bin: {e}"))?;
    fault.after(&bin_path)?;

    // 3. Manifest JSON (cluster id + base64 mirrors of each blob).
    let records_b64: Vec<String> = record_blobs.iter().map(|b| STANDARD.encode(b)).collect();
    let manifest = BootstrapManifest {
        version: BOOTSTRAP_MANIFEST_VERSION,
        cluster_id,
        record_count: records.len(),
        records_b64,
    };
    let json =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    let json_path = log_dir.join("bootstrap.json");
    std::fs::write(&json_path, json).map_err(|e| format!("write bootstrap.json: {e}"))?;
    fault.after(&json_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The records the golden files hold.
    fn golden_records() -> Vec<MetadataRecord> {
        vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 30,
            },
        )]
    }

    /// `golden_records` in `bootstrap.records.bin`: the version header, then
    /// one `u32` little-endian length and the wincode record. The broker's
    /// reader pins the same bytes.
    const GOLDEN_RECORDS_BIN: &[u8] = include_bytes!("../../tests/fixtures/bootstrap.records.bin");

    /// `golden_records` in `bootstrap.json`, for the cluster id below.
    const GOLDEN_MANIFEST: &str = r#"{
  "version": 1,
  "cluster_id": "AQIDBAUGBwgJCgsMDQ4PEA",
  "record_count": 1,
  "records_b64": [
    "EQAAABAAAAAAAAAAbWV0YWRhdGEudmVyc2lvbh4A"
  ]
}"#;

    /// Both bootstrap files are laid out byte for byte as the 1.x contract
    /// fixes them.
    #[test]
    fn the_bootstrap_files_match_their_golden_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId(uuid::Uuid::from_u128(
            0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
        ));
        write_bootstrap_files(dir.path(), cluster_id, &golden_records(), &Fault::default())
            .unwrap();
        assert2::assert!(
            std::fs::read(dir.path().join("bootstrap.records.bin")).unwrap() == GOLDEN_RECORDS_BIN
        );
        assert2::assert!(
            std::fs::read_to_string(dir.path().join("bootstrap.json")).unwrap() == GOLDEN_MANIFEST
        );
    }
}
