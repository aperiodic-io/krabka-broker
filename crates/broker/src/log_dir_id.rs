//! Per-`log.dir` stable UUIDs (KIP-858 directory ids).
//!
//! Each configured `log.dir` carries a `directory.id` in its Kafka
//! `meta.properties`, in Kafka's 22-character base64 form. `krabka format`
//! writes it for every directory it was given, and at a start the broker
//! binary writes one for a disk that was added after the format
//! ([`crate::bootstrap::initialize_log_dirs`]). A broker started as a library,
//! on directories that no format touched, gets its ids from
//! [`LogDirIds::provision`].
//!
//! The resulting map from path to uuid lets the broker stamp
//! `AssignReplicasToDirs` and `offline_log_dirs` with stable ids that the
//! controller can map back to partitions.

use {
    crate::NodeId,
    krabka_format::{ClusterId, DirectoryId, MetaProperties},
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
    },
    uuid::Uuid,
};

/// One `log.dir` and the directory id it carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogDirEntry {
    /// The `log.dir` path.
    pub path: PathBuf,
    /// The dir's `directory.id`.
    pub id: Uuid,
}

/// The cluster and node a new `meta.properties` is written for.
#[derive(Clone, Copy)]
struct BrokerIdentity {
    cluster_id: ClusterId,
    node_id: NodeId,
}

/// Immutable per-dir UUID table built once at startup.
#[derive(Clone, Debug, Default)]
pub struct LogDirIds {
    by_path: HashMap<PathBuf, Uuid>,
}

impl LogDirIds {
    /// Reads the `directory.id` of every dir in `log_dirs` from its
    /// `meta.properties`. A dir that has none, or whose file does not read,
    /// gets a new id, as Kafka's `DirectoryId.random` makes one, that only
    /// this table holds.
    #[must_use]
    pub fn resolve(log_dirs: &[PathBuf]) -> Self { Self::build(log_dirs, None) }

    /// [`Self::resolve`], but a new id is also written to the dir: into its
    /// `meta.properties` when the file has no `directory.id`, as Kafka's
    /// `KafkaRaftServer.initializeLogDirs` writes it, and into a new
    /// `meta.properties` for `cluster_id` and `node_id` when the dir has no
    /// file. A file that does not read is left alone.
    ///
    /// On an I/O failure the id stays in this table only. The partition
    /// therefore stays usable, and only the reporting of the dir degrades.
    #[must_use]
    pub fn provision(log_dirs: &[PathBuf], cluster_id: Uuid, node_id: NodeId) -> Self {
        Self::build(
            log_dirs,
            Some(BrokerIdentity {
                cluster_id: ClusterId(cluster_id),
                node_id,
            }),
        )
    }

    fn build(log_dirs: &[PathBuf], identity: Option<BrokerIdentity>) -> Self {
        let mut by_path = HashMap::new();
        for dir in log_dirs {
            let used: Vec<DirectoryId> = by_path.values().copied().map(DirectoryId).collect();
            let id = read_or_mint(dir, &used, identity);
            by_path.insert(dir.clone(), id);
        }
        Self { by_path }
    }

    #[must_use]
    pub fn id_for(&self, dir: &Path) -> Option<Uuid> { self.by_path.get(dir).copied() }

    /// All path and uuid pairs, sorted by path for deterministic output.
    #[must_use]
    pub fn entries(&self) -> Vec<LogDirEntry> {
        let mut v: Vec<_> = self
            .by_path
            .iter()
            .map(|(path, id)| LogDirEntry {
                path: path.clone(),
                id: *id,
            })
            .collect();
        v.sort_by(|a, b| a.path.cmp(&b.path));
        v
    }

    /// The UUIDs of the supplied dirs. It skips a dir that the table does not
    /// hold.
    #[must_use]
    pub fn ids_for(&self, dirs: &[PathBuf]) -> Vec<Uuid> {
        dirs.iter().filter_map(|d| self.id_for(d)).collect()
    }
}

/// Reads the `directory.id` from `<dir>/meta.properties`, or makes a new id
/// that is not in `used`. With an `identity`, a new id is written to the dir.
fn read_or_mint(dir: &Path, used: &[DirectoryId], identity: Option<BrokerIdentity>) -> Uuid {
    let read = MetaProperties::read(dir);
    if let Ok(Some(MetaProperties {
        directory_id: Some(id),
        ..
    })) = read
    {
        return id.into();
    }
    let id = DirectoryId::random_unused(used);
    let file = match (read, identity) {
        (Ok(Some(meta)), Some(_)) => Some(meta),
        (
            Ok(None),
            Some(BrokerIdentity {
                cluster_id,
                node_id,
            }),
        ) => i32::try_from(node_id.0).ok().map(|node_id| MetaProperties {
            cluster_id,
            node_id,
            directory_id: None,
        }),
        _ => None,
    };
    if let Some(meta) = file {
        let meta = MetaProperties {
            directory_id: Some(id),
            ..meta
        };
        let written = std::fs::create_dir_all(dir).and_then(|()| meta.write(dir));
        if let Err(error) = written {
            tracing::warn!(dir = %dir.display(), %error, "cannot write meta.properties");
        }
    }
    id.into()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        tempfile::tempdir,
    };

    const CLUSTER: Uuid = Uuid::from_u128(0xc1);

    /// `provision` writes a new id into a Kafka `meta.properties` for the
    /// node, and every later read finds it.
    #[test]
    fn provision_writes_a_new_id_for_a_dir_without_one() {
        let tmp = tempdir().unwrap();
        let ids = LogDirIds::provision(&[tmp.path().to_path_buf()], CLUSTER, NodeId(4));
        let first = ids.id_for(tmp.path()).expect("minted");
        check!(
            MetaProperties::read(tmp.path()).unwrap()
                == Some(MetaProperties {
                    cluster_id: ClusterId(CLUSTER),
                    node_id: 4,
                    directory_id: Some(DirectoryId(first)),
                })
        );
        let again = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        check!(again.id_for(tmp.path()) == Some(first));
    }

    /// A file without a `directory.id` gets one, and keeps its own cluster
    /// and node ids, as Kafka rewrites it at a start.
    #[test]
    fn provision_adds_an_id_to_a_file_without_one() {
        let tmp = tempdir().unwrap();
        let formatted = MetaProperties {
            cluster_id: ClusterId(Uuid::from_u128(0xc2)),
            node_id: 7,
            directory_id: None,
        };
        formatted.write(tmp.path()).unwrap();
        let ids = LogDirIds::provision(&[tmp.path().to_path_buf()], CLUSTER, NodeId(4));
        let id = ids.id_for(tmp.path()).expect("minted");
        check!(
            MetaProperties::read(tmp.path()).unwrap()
                == Some(MetaProperties {
                    directory_id: Some(DirectoryId(id)),
                    ..formatted
                })
        );
    }

    /// A dir that already has an id keeps it, and its file is not touched.
    /// A file that does not read is not touched either.
    #[test]
    fn reads_an_existing_id_and_leaves_files_alone() {
        let formatted = tempdir().unwrap();
        let id = Uuid::from_u128(0xABCD);
        let meta = MetaProperties {
            cluster_id: ClusterId(Uuid::from_u128(0xc2)),
            node_id: 7,
            directory_id: Some(DirectoryId(id)),
        };
        meta.write(formatted.path()).unwrap();
        let before = std::fs::read(formatted.path().join(krabka_format::META_PROPERTIES)).unwrap();
        let unreadable = tempdir().unwrap();
        std::fs::write(
            unreadable.path().join(krabka_format::META_PROPERTIES),
            "version=one\n",
        )
        .unwrap();

        let dirs = [
            formatted.path().to_path_buf(),
            unreadable.path().to_path_buf(),
        ];
        let ids = LogDirIds::provision(&dirs, CLUSTER, NodeId(4));
        check!(ids.id_for(formatted.path()) == Some(id));
        check!(ids.id_for(unreadable.path()).is_some());
        check!(
            std::fs::read(formatted.path().join(krabka_format::META_PROPERTIES)).unwrap() == before
        );
        check!(
            std::fs::read(unreadable.path().join(krabka_format::META_PROPERTIES)).unwrap()
                == b"version=one\n"
        );
    }

    /// `resolve` writes nothing: a dir without a file still has none.
    #[test]
    fn resolve_holds_a_new_id_in_memory_only() {
        let tmp = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        check!(ids.id_for(tmp.path()).is_some());
        check!(MetaProperties::read(tmp.path()).unwrap().is_none());
    }

    #[test]
    fn distinct_dirs_get_distinct_ids() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[a.path().to_path_buf(), b.path().to_path_buf()]);
        assert!(ids.id_for(a.path()) != ids.id_for(b.path()));
        assert!(ids.entries().len() == 2);
    }

    #[test]
    fn ids_for_returns_requested_known_dirs_in_order() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let unknown = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[a.path().to_path_buf(), b.path().to_path_buf()]);
        let a_id = ids.id_for(a.path()).expect("a id");
        let b_id = ids.id_for(b.path()).expect("b id");

        let selected = ids.ids_for(&[
            b.path().to_path_buf(),
            unknown.path().to_path_buf(),
            a.path().to_path_buf(),
        ]);

        assert!(selected == vec![b_id, a_id]);
    }
}
