//! What a capture writes beside its artifacts, and the check that reads it
//! back.
//!
//! A capture is a directory of opaque bytes: an operator cannot look at a
//! controller checkpoint and tell whether it arrived whole. The manifest is
//! what makes the copy checkable — it names every artifact with the size and
//! the SHA-256 of the bytes that were uploaded, so `krabka-backup verify` can
//! answer the only question that matters before a disaster, which is whether
//! this copy would restore.

use {
    crate::error::BackupError,
    serde::{Deserialize, Serialize, de::DeserializeOwned},
    sha2::{Digest as _, Sha256},
};

/// Version of `manifest.json`, its required top-level `"version"` field.
///
/// Part of the 1.x on-disk contract: a 1.x `krabka-backup` reads every
/// manifest that any earlier 1.x build wrote.
pub const MANIFEST_VERSION: i16 = 0;

/// Directory the captures live under, inside the archive.
pub const CAPTURE_ROOT: &str = "restore-inputs";

/// Object name of the manifest inside one capture.
pub const MANIFEST: &str = "manifest.json";

/// Object name of the captured RLMM snapshot.
pub const RLMM_SNAPSHOT: &str = "rlmm-snapshot";

/// Object name of the captured controller metadata checkpoint.
pub const METADATA_CHECKPOINT: &str = "cluster-metadata.checkpoint";

/// Object name of the captured committed group offsets.
pub const GROUP_OFFSETS: &str = "group-offsets.json";

/// Object name of the committed diskless-WAL projection.
pub const DISKLESS_WAL_INDEX: &str = "diskless-wal-index.json";

/// One captured artifact, as it was uploaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    /// Object name inside the capture, one of the four constants above.
    pub name: String,
    /// Where the bytes came from: a path on the node, or the cluster address
    /// they were read from.
    pub source: String,
    /// Bytes uploaded.
    pub size_bytes: u64,
    /// Lowercase hex SHA-256 of the uploaded bytes.
    pub sha256: String,
}

/// The record of one capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Always [`MANIFEST_VERSION`] when this build writes it.
    pub version: i16,
    /// The capture's own id, which is also its directory name.
    pub capture_id: String,
    /// When the capture ran, in milliseconds since the Unix epoch.
    pub captured_at_ms: u64,
    /// The log directory the file artifacts were read from, when a capture
    /// took any.
    pub log_dir: Option<String>,
    /// The cluster the group offsets were read from, when a capture took them.
    pub bootstrap_server: Option<String>,
    /// Every artifact this capture wrote, in the order it wrote them.
    pub artifacts: Vec<Artifact>,
}

impl Manifest {
    /// Decode a manifest, refusing a missing or unknown `"version"`.
    ///
    /// # Errors
    ///
    /// [`BackupError::UnsupportedVersion`] for a manifest with no version or
    /// one other than [`MANIFEST_VERSION`], and [`BackupError::Json`] for bytes
    /// that are not a manifest. `context` names the object in either error.
    pub fn from_slice(bytes: &[u8], context: &str) -> Result<Self, BackupError> {
        decode_versioned(bytes, context, MANIFEST_VERSION)
    }

    /// The artifact of this name, if the capture took one.
    #[must_use]
    pub fn artifact(&self, name: &str) -> Option<&Artifact> {
        self.artifacts.iter().find(|entry| entry.name == name)
    }
}

/// Just the version of a capture document, read before the rest of it.
#[derive(Deserialize)]
struct VersionProbe {
    version: Option<serde_json::Value>,
}

/// Decode a JSON capture document whose top-level `"version"` must be
/// `expected`.
///
/// The version is read on its own first, so a document of another version is
/// reported as that, and not as whatever field its shape lacks.
pub(crate) fn decode_versioned<T: DeserializeOwned>(
    bytes: &[u8],
    context: &str,
    expected: i16,
) -> Result<T, BackupError> {
    let json = |source| BackupError::Json {
        context: context.to_owned(),
        source,
    };
    let probe: VersionProbe = serde_json::from_slice(bytes).map_err(json)?;
    if !matches!(&probe.version, Some(version) if *version == expected) {
        return Err(BackupError::UnsupportedVersion {
            context: context.to_owned(),
            found: probe.version.map(|version| version.to_string()),
            expected,
        });
    }
    serde_json::from_slice(bytes).map_err(json)
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// What is wrong with one artifact's bytes, or `None` when they are the bytes
/// the manifest recorded.
///
/// The size is checked as well as the digest, because a size mismatch names
/// the failure an operator can act on — a truncated upload — while a digest
/// mismatch alone only says the bytes differ.
#[must_use]
pub fn artifact_problem(expected: &Artifact, actual: &[u8]) -> Option<String> {
    let actual_size = actual.len() as u64;
    if actual_size != expected.size_bytes {
        return Some(format!(
            "{}: archive holds {actual_size} bytes, manifest recorded {}",
            expected.name, expected.size_bytes
        ));
    }
    let actual_sha = sha256_hex(actual);
    (actual_sha != expected.sha256).then(|| {
        format!(
            "{}: archive holds sha256 {actual_sha}, manifest recorded {}",
            expected.name, expected.sha256
        )
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{Artifact, MANIFEST_VERSION, Manifest, artifact_problem, sha256_hex},
        crate::error::BackupError,
        assert2::check,
    };

    fn artifact(bytes: &[u8]) -> Artifact {
        Artifact {
            name: "rlmm-snapshot".to_owned(),
            source: "/var/lib/krabka/remote-log-metadata/snapshot".to_owned(),
            size_bytes: bytes.len() as u64,
            sha256: sha256_hex(bytes),
        }
    }

    #[test]
    fn the_empty_input_hashes_to_the_published_sha256_of_the_empty_string() {
        check!(
            sha256_hex(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn bytes_that_match_the_manifest_are_no_problem() {
        check!(artifact_problem(&artifact(b"snapshot bytes"), b"snapshot bytes") == None);
    }

    #[test]
    fn a_short_read_is_reported_as_a_size_mismatch() {
        let problem = artifact_problem(&artifact(b"snapshot bytes"), b"snapshot")
            .expect("a short read is a problem");
        check!(problem.contains("8 bytes"), "got: {problem}");
        check!(problem.contains("14"), "got: {problem}");
    }

    #[test]
    fn same_length_different_bytes_are_reported_as_a_digest_mismatch() {
        let problem = artifact_problem(&artifact(b"snapshot bytes"), b"snapshot bytez")
            .expect("different bytes are a problem");
        check!(problem.contains("sha256"), "got: {problem}");
    }

    fn golden_manifest() -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            capture_id: "0001700000000000".to_owned(),
            captured_at_ms: 1_700_000_000_000,
            log_dir: Some("/var/lib/krabka".to_owned()),
            bootstrap_server: Some("broker-1:9092".to_owned()),
            artifacts: vec![artifact(b"snapshot bytes")],
        }
    }

    /// The exact bytes this build writes for [`golden_manifest`]. A change
    /// here is a change to the 1.x capture format.
    const GOLDEN_MANIFEST: &str = concat!(
        r#"{"version":0,"capture_id":"0001700000000000","captured_at_ms":1700000000000,"#,
        r#""log_dir":"/var/lib/krabka","bootstrap_server":"broker-1:9092","artifacts":"#,
        r#"[{"name":"rlmm-snapshot","source":"/var/lib/krabka/remote-log-metadata/snapshot","#,
        r#""size_bytes":14,"#,
        r#""sha256":"ee36ef8194c4dd1e734e6d64f62653008e6566dbd5e2cd7f289f0f4f3e4467a4"}]}"#,
    );

    #[test]
    fn a_manifest_encodes_to_the_golden_bytes_and_decodes_back() {
        let manifest = golden_manifest();
        let encoded = serde_json::to_string(&manifest).expect("encode the manifest");
        check!(encoded == GOLDEN_MANIFEST);
        let decoded = Manifest::from_slice(GOLDEN_MANIFEST.as_bytes(), "manifest.json")
            .expect("decode the manifest");
        check!(decoded == manifest);
        check!(decoded.artifact("rlmm-snapshot").is_some());
        check!(decoded.artifact("group-offsets.json").is_none());
    }

    #[test]
    fn a_manifest_with_a_missing_or_unknown_version_is_refused() {
        for (name, json, found) in [
            (
                "pre-1.0 manifest",
                r#"{"capture_id":"1","captured_at_ms":1,"log_dir":null,"bootstrap_server":null,"artifacts":[]}"#,
                None,
            ),
            (
                "future version",
                r#"{"version":1,"capture_id":"1","captured_at_ms":1,"log_dir":null,"bootstrap_server":null,"artifacts":[]}"#,
                Some("1"),
            ),
        ] {
            let error = Manifest::from_slice(json.as_bytes(), "restore-inputs/1/manifest.json")
                .expect_err(name);
            assert2::assert!(
                let BackupError::UnsupportedVersion {
                    context,
                    found: actual,
                    expected: MANIFEST_VERSION
                } = &error,
                "case {name}: {error}"
            );
            check!(context == "restore-inputs/1/manifest.json", "case {name}");
            check!(actual.as_deref() == found, "case {name}");
        }
    }
}
