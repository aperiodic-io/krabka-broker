//! On-disk record of the WAL quorum voter set for one shard.
//!
//! `QuorumWalStore` writes this descriptor when it first creates a shard, and
//! reads it back on every reopen, so a voter set that changed under the broker
//! is refused instead of silently re-bootstrapped. The replace is crash-safe:
//! the new bytes go to a temporary file, the previous descriptor moves aside as
//! a backup, and a failed rename puts that backup back.

use std::{fs, io::Write as _};

use krabka_kraft_core::NodeId;

use crate::error::BrokerError;

pub(super) const QUORUM_STATE_FILE: &str = "quorum-state.json";
pub(super) const QUORUM_STATE_BACKUP_FILE: &str = "quorum-state.json.bak";

/// Version of `quorum-state.json` and its `.bak`, the required top-level
/// `"version"` field.
///
/// Part of the 1.x on-disk contract: a 1.x broker reads every descriptor that
/// any earlier 1.x broker wrote.
pub(super) const QUORUM_MEMBERSHIP_VERSION: i16 = 0;

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct PersistedQuorumMembership {
    version: i16,
    voters: Vec<u64>,
}

/// Just the version of a descriptor, read before the rest of it.
#[derive(serde::Deserialize)]
struct PersistedQuorumMembershipVersion {
    version: Option<serde_json::Value>,
}

/// Why a WAL quorum membership descriptor could not be read.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum QuorumMembershipDecodeError {
    /// The descriptor has no `"version"`, as one written before 1.0 does.
    #[error(
        "the descriptor has no \"version\": it predates krabka 1.0, whose data a 1.x broker \
         does not read; reformat this node"
    )]
    MissingVersion,
    /// The descriptor carries a version this build does not read.
    #[error(
        "unsupported descriptor version {found}: this build reads version \
         {QUORUM_MEMBERSHIP_VERSION}"
    )]
    UnsupportedVersion {
        /// The `"version"` as JSON text.
        found: String,
    },
    /// The bytes are not a descriptor at all.
    #[error("{0}")]
    Malformed(String),
}

fn decode_quorum_membership(
    bytes: &[u8],
) -> Result<PersistedQuorumMembership, QuorumMembershipDecodeError> {
    let malformed =
        |err: serde_json::Error| QuorumMembershipDecodeError::Malformed(err.to_string());
    let probe: PersistedQuorumMembershipVersion =
        serde_json::from_slice(bytes).map_err(malformed)?;
    let version = probe
        .version
        .ok_or(QuorumMembershipDecodeError::MissingVersion)?;
    if version != QUORUM_MEMBERSHIP_VERSION {
        return Err(QuorumMembershipDecodeError::UnsupportedVersion {
            found: version.to_string(),
        });
    }
    serde_json::from_slice(bytes).map_err(malformed)
}

pub(super) fn load_or_prepare_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<bool, BrokerError> {
    fs::create_dir_all(root)?;
    let path = root.join(QUORUM_STATE_FILE);
    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    let existing = if path.exists() {
        Some(&path)
    } else if backup.exists() {
        Some(&backup)
    } else {
        None
    };
    if let Some(existing) = existing {
        let bytes = fs::read(existing)?;
        // The backup is read under the same rules as the primary: a backup
        // of a version this build does not read is an error, never a reason
        // to fall back further or to bootstrap the shard again.
        let persisted = decode_quorum_membership(&bytes).map_err(|err| {
            BrokerError::Replication(format!(
                "decode WAL quorum membership {}: {err}",
                existing.display()
            ))
        })?;
        let persisted_ids = persisted.voters.into_iter().map(NodeId).collect::<Vec<_>>();
        if persisted_ids != voter_ids {
            return Err(BrokerError::Replication(format!(
                "WAL quorum voter set changed for {}: persisted {:?}, configured {:?}",
                existing.display(),
                persisted_ids,
                voter_ids
            )));
        }
        return Ok(false);
    }

    Ok(true)
}

pub(super) fn persist_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<(), BrokerError> {
    let path = root.join(QUORUM_STATE_FILE);
    let persisted = PersistedQuorumMembership {
        version: QUORUM_MEMBERSHIP_VERSION,
        voters: voter_ids.iter().map(|id| id.0).collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted).map_err(|err| {
        BrokerError::Replication(format!(
            "encode WAL quorum membership {}: {err}",
            path.display()
        ))
    })?;
    let temporary = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    if backup.exists() {
        if path.exists() {
            fs::remove_file(&backup)?;
        } else {
            fs::rename(&backup, &path)?;
        }
    }
    if path.exists() {
        fs::rename(&path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, &path) {
        restore_membership_backup(&backup, &path);
        return Err(error.into());
    }
    if backup.exists() {
        fs::remove_file(&backup)?;
    }

    // A durable file is not enough on filesystems where the rename itself is
    // only stable after the parent directory is synced. Rust does not expose
    // directory handles that can be flushed on Windows; the file sync above is
    // the strongest portable guarantee there, matching `krabka-log`.
    #[cfg(unix)]
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn restore_membership_backup(backup: &std::path::Path, path: &std::path::Path) {
    if let (Ok(true), Ok(false)) = (backup.try_exists(), path.try_exists()) {
        let _ = fs::rename(backup, path);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    fn prepared_three_voter_root() -> (tempfile::TempDir, Vec<NodeId>) {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        assert!(load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
        (root, voter_ids)
    }

    #[test]
    fn quorum_membership_descriptor_survives_reopen() {
        let (root, voter_ids) = prepared_three_voter_root();
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let is_new = load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap();
        let persisted =
            decode_quorum_membership(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();

        assert!(!is_new);
        assert!(
            persisted
                == PersistedQuorumMembership {
                    version: QUORUM_MEMBERSHIP_VERSION,
                    voters: vec![0, 1, 2],
                }
        );
        assert!(root.path().join(QUORUM_STATE_FILE).exists());
        assert!(
            !root
                .path()
                .join(QUORUM_STATE_FILE)
                .with_extension("json.tmp")
                .exists()
        );
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_persist_replaces_a_stale_temporary_file() {
        let (root, voter_ids) = prepared_three_voter_root();
        let temporary = root
            .path()
            .join(QUORUM_STATE_FILE)
            .with_extension("json.tmp");
        fs::write(&temporary, b"incomplete").unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        assert!(!temporary.exists());
        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_persist_replaces_an_existing_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        fs::create_dir_all(root.path()).unwrap();
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            serde_json::to_vec(&serde_json::json!({
                "version": 0,
                "voters": [0, 1, 2],
                "leader_epoch": 4,
                "leader_id": 1,
            }))
            .unwrap(),
        )
        .unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();
        assert!(persisted == serde_json::json!({"version": 0, "voters": [0, 1, 2]}));
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_loads_backup_left_between_replace_renames() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();
        fs::rename(
            root.path().join(QUORUM_STATE_FILE),
            root.path().join(QUORUM_STATE_BACKUP_FILE),
        )
        .unwrap();

        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_restore_only_uses_a_backup_when_the_primary_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let primary = root.path().join(QUORUM_STATE_FILE);
        let backup = root.path().join(QUORUM_STATE_BACKUP_FILE);
        fs::write(&backup, b"backup-only").unwrap();

        restore_membership_backup(&backup, &primary);

        assert!(fs::read(&primary).unwrap() == b"backup-only");
        assert!(!backup.exists());

        fs::write(&primary, b"current").unwrap();
        fs::write(&backup, b"stale-backup").unwrap();
        restore_membership_backup(&backup, &primary);
        assert!(fs::read(&primary).unwrap() == b"current");
        assert!(fs::read(&backup).unwrap() == b"stale-backup");
    }

    /// The exact bytes a 1.x broker writes for voters `[0, 1, 2]`. A change
    /// here is a change to the 1.x on-disk contract.
    const GOLDEN_DESCRIPTOR: &str =
        "{\n  \"version\": 0,\n  \"voters\": [\n    0,\n    1,\n    2\n  ]\n}";

    #[test]
    fn quorum_membership_descriptor_matches_the_golden_bytes() {
        let root = tempfile::tempdir().unwrap();
        persist_quorum_membership(root.path(), &[NodeId(0), NodeId(1), NodeId(2)]).unwrap();

        let written = fs::read_to_string(root.path().join(QUORUM_STATE_FILE)).unwrap();

        assert!(written == GOLDEN_DESCRIPTOR);
        assert!(
            decode_quorum_membership(GOLDEN_DESCRIPTOR.as_bytes())
                == Ok(PersistedQuorumMembership {
                    version: QUORUM_MEMBERSHIP_VERSION,
                    voters: vec![0, 1, 2],
                })
        );
    }

    #[test]
    fn quorum_membership_decode_rejects_a_missing_or_unknown_version() {
        for (name, descriptor, expected) in [
            (
                "pre-1.0 descriptor with no version",
                serde_json::json!({"voters": [0, 1, 2]}),
                QuorumMembershipDecodeError::MissingVersion,
            ),
            (
                "pre-1.0 descriptor with election fields",
                serde_json::json!({
                    "cluster_id": Uuid::from_u128(17),
                    "voters": [0, 1, 2],
                    "kraft_version": 1,
                    "leader_epoch": 7,
                }),
                QuorumMembershipDecodeError::MissingVersion,
            ),
            (
                "future version",
                serde_json::json!({"version": 1, "voters": [0, 1, 2]}),
                QuorumMembershipDecodeError::UnsupportedVersion {
                    found: "1".to_owned(),
                },
            ),
            (
                "version that is not a number",
                serde_json::json!({"version": "0", "voters": [0, 1, 2]}),
                QuorumMembershipDecodeError::UnsupportedVersion {
                    found: "\"0\"".to_owned(),
                },
            ),
        ] {
            let bytes = serde_json::to_vec(&descriptor).unwrap();
            assert!(
                decode_quorum_membership(&bytes) == Err(expected),
                "case {name}"
            );
        }
    }

    #[test]
    fn quorum_membership_refuses_a_primary_or_backup_of_an_unknown_version() {
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        for file in [QUORUM_STATE_FILE, QUORUM_STATE_BACKUP_FILE] {
            let root = tempfile::tempdir().unwrap();
            fs::write(
                root.path().join(file),
                serde_json::json!({"version": 1, "voters": [0, 1, 2]}).to_string(),
            )
            .unwrap();

            let error = load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("unsupported descriptor version 1"),
                "{file}: {error}"
            );
        }
    }

    #[test]
    fn quorum_membership_rejects_changed_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let changed = vec![NodeId(0), NodeId(1), NodeId(3)];
        assert!(load_or_prepare_quorum_membership(root.path(), &changed).is_err());
    }
}
