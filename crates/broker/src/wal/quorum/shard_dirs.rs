//! Layout and lifecycle of the `__diskless_wal_quorum` directory tree.
//!
//! A shard owns one directory per log dir, named from the topic, its topic id,
//! and the partition, so a recreated topic never adopts the replica logs of its
//! predecessor. The removal helpers are the counterpart: they delete a whole
//! shard, delete only the leader-owned state below it, or prune the
//! follower-only roots that the current metadata image no longer assigns to
//! this broker.

use std::fs;

use krabka_ids::PartitionIndex;
use uuid::Uuid;

use super::registry;

fn sanitize_topic(topic: &str) -> String {
    topic
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[must_use]
pub(crate) fn shard_dir(
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Option<Uuid>,
    partition: PartitionIndex,
) -> std::path::PathBuf {
    let identity = topic_id.map_or_else(
        || sanitize_topic(topic),
        |topic_id| format!("{}-{topic_id}", sanitize_topic(topic)),
    );
    log_dir
        .join("__diskless_wal_quorum")
        .join(format!("{identity}-{}", partition.0))
}

fn unregister_shard(
    registry: &registry::WalShardRegistry,
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Uuid,
    partition: PartitionIndex,
) -> std::path::PathBuf {
    registry.remove(registry::ShardId {
        topic_id,
        partition,
    });
    shard_dir(log_dir, topic, Some(topic_id), partition)
}

#[derive(Clone, Copy)]
pub(crate) enum ShardRemoval {
    All,
    Leader,
}

pub(crate) fn remove_shard(
    registry: &registry::WalShardRegistry,
    log_dir: &std::path::Path,
    topic: &str,
    topic_id: Uuid,
    partition: PartitionIndex,
    scope: ShardRemoval,
) -> std::io::Result<()> {
    let root = unregister_shard(registry, log_dir, topic, topic_id, partition);
    if matches!(scope, ShardRemoval::All) {
        return match fs::remove_dir_all(root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result,
        };
    }
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("voter-") {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        }?;
    }
    Ok(())
}

/// Remove follower-only WAL shard directories that the current metadata image
/// no longer assigns to this broker.
///
/// Reconciliation calls this after obsolete WAL-follower tasks have stopped.
/// A root is eligible only when every child is a `voter-*` follower directory;
/// roots carrying any leader/runtime state are left to the owner-aware
/// partition prune path. This closes the offline-delete/reassignment case that
/// an in-memory task reconciliation cannot observe after a process restart.
pub(crate) fn prune_orphaned_shard_dirs(
    log_dirs: &[std::path::PathBuf],
    keep: &std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<()> {
    for log_dir in log_dirs {
        let root = log_dir.join("__diskless_wal_quorum");
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if keep.contains(&path) {
                continue;
            }
            if !path.is_dir() {
                continue;
            }
            let follower_only = fs::read_dir(&path)?.all(|child| {
                child.is_ok_and(|child| {
                    child.path().is_dir()
                        && child.file_name().to_string_lossy().starts_with("voter-")
                })
            });
            if follower_only {
                fs::remove_dir_all(path)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_kraft_core::NodeId;

    use super::*;
    use crate::wal::quorum::membership::QUORUM_STATE_FILE;

    #[test]
    fn shard_directory_distinguishes_same_name_topic_recreations() {
        let root = std::path::Path::new("data");
        let first_id = Uuid::from_u128(1);
        let second_id = Uuid::from_u128(2);

        let first = shard_dir(root, "orders", Some(first_id), PartitionIndex(3));
        let second = shard_dir(root, "orders", Some(second_id), PartitionIndex(3));

        assert!(first != second);
        assert!(first.ends_with(format!("orders-{first_id}-3")));
        assert!(second.ends_with(format!("orders-{second_id}-3")));
    }

    #[test]
    fn remove_shard_is_idempotent_but_reports_other_io_errors() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry::WalShardRegistry::new(NodeId(0));
        let topic_id = Uuid::from_u128(3);
        let partition = PartitionIndex(4);

        remove_shard(
            &registry,
            root.path(),
            "orders",
            topic_id,
            partition,
            ShardRemoval::All,
        )
        .unwrap();

        let path = shard_dir(root.path(), "orders", Some(topic_id), partition);
        fs::create_dir_all(path.join("voter-2")).unwrap();
        fs::write(path.join("voter-2/checkpoint"), b"durable").unwrap();
        remove_shard(
            &registry,
            root.path(),
            "orders",
            topic_id,
            partition,
            ShardRemoval::All,
        )
        .unwrap();
        assert!(!path.exists());

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not a directory").unwrap();
        assert!(
            remove_shard(
                &registry,
                root.path(),
                "orders",
                topic_id,
                partition,
                ShardRemoval::All
            )
            .is_err()
        );
    }

    #[test]
    fn remove_leader_shard_preserves_follower_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry::WalShardRegistry::new(NodeId(2));
        let topic_id = Uuid::from_u128(4);
        let partition = PartitionIndex(0);
        let shard = shard_dir(root.path(), "orders", Some(topic_id), partition);
        fs::create_dir_all(shard.join("voter-2")).unwrap();
        fs::write(shard.join("voter-2/checkpoint"), b"durable").unwrap();
        fs::write(shard.join("quorum-state.json"), b"leader").unwrap();

        remove_shard(
            &registry,
            root.path(),
            "orders",
            topic_id,
            partition,
            ShardRemoval::Leader,
        )
        .unwrap();

        assert!(shard.join("voter-2/checkpoint").exists());
        assert!(!shard.join("quorum-state.json").exists());
    }

    #[test]
    fn prune_removes_only_unassigned_wal_shards() {
        let root = tempfile::tempdir().unwrap();
        let keep = shard_dir(
            root.path(),
            "orders",
            Some(Uuid::from_u128(5)),
            PartitionIndex(0),
        );
        let orphan = shard_dir(
            root.path(),
            "deleted",
            Some(Uuid::from_u128(6)),
            PartitionIndex(1),
        );
        fs::create_dir_all(keep.join("voter-2")).unwrap();
        fs::create_dir_all(orphan.join("voter-2")).unwrap();
        fs::write(orphan.join("voter-2/checkpoint"), b"durable").unwrap();
        let active = shard_dir(
            root.path(),
            "active",
            Some(Uuid::from_u128(7)),
            PartitionIndex(2),
        );
        fs::create_dir_all(active.join("voter-2")).unwrap();
        fs::write(active.join(QUORUM_STATE_FILE), b"leader runtime").unwrap();

        prune_orphaned_shard_dirs(
            &[root.path().to_path_buf()],
            &maplit::hashset! {keep.clone()},
        )
        .unwrap();

        assert!(keep.exists());
        assert!(!orphan.exists());
        assert!(active.exists());
    }
}
