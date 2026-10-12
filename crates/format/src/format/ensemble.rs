//! What the log directories of one run already hold, and whether that set is
//! consistent.
//!
//! This is `MetaPropertiesEnsemble` in Kafka's `Formatter`: every directory is
//! read before any is written, and the formatted ones must agree on the
//! cluster id and the node id and must not share a directory id. A directory
//! falls into one of three sets. It is formatted when its `meta.properties`
//! reads, empty when it has no `meta.properties`, and in error when the file
//! is there but does not read.
//!
//! An empty directory is where krabka is stricter than Kafka. Kafka formats a
//! directory with no `meta.properties` whatever else it holds. `format`
//! refuses one that holds a file it did not write, so a mistyped path cannot
//! seed a directory full of someone else's data. The files an interrupted run
//! left behind do not count against it: a run writes `meta.properties` last,
//! so a directory with only those files is one that a run did not finish, and
//! the next run removes them and starts again.

use std::{
    io,
    path::{Path, PathBuf},
};

use super::output::ZERO_CHECKPOINT_NAME;
use crate::{
    ids::ClusterId,
    meta_properties::{META_PROPERTIES, META_PROPERTIES_TMP, MetaProperties, verify_ensemble},
};

/// The directory that holds the metadata log, under the metadata log
/// directory: `__cluster_metadata-0`, as in Kafka.
pub(super) const CLUSTER_METADATA: &str = krabka_raft::METADATA_PARTITION_DIR;

/// The top-level files that a run writes before `meta.properties`.
const PARTIAL_FILES: [&str; 3] = [
    "bootstrap.json",
    "bootstrap.records.bin",
    META_PROPERTIES_TMP,
];

/// The state of every log directory of a run.
#[derive(Debug, Default)]
pub(super) struct Ensemble {
    /// Directories with no `meta.properties`, in the order given.
    pub(super) empty: Vec<PathBuf>,
    /// Directories whose `meta.properties` reads, sorted by path.
    pub(super) formatted: Vec<(PathBuf, MetaProperties)>,
    /// Directories whose `meta.properties` is there but does not read,
    /// sorted by path.
    pub(super) errors: Vec<PathBuf>,
}

/// Why a directory cannot be surveyed.
#[derive(Debug)]
pub(super) enum SurveyError {
    /// The directory holds files that `format` did not write.
    Foreign(PathBuf),
    /// The directory cannot be listed.
    Io(PathBuf, io::Error),
}

impl Ensemble {
    /// Reads every directory in `dirs`.
    pub(super) fn load(dirs: &[PathBuf]) -> Result<Self, SurveyError> {
        let mut ensemble = Self::default();
        for dir in dirs {
            match MetaProperties::read(dir) {
                Ok(Some(meta)) => ensemble.formatted.push((dir.clone(), meta)),
                Ok(None) => match holds_only_partial_output(dir) {
                    Ok(true) => ensemble.empty.push(dir.clone()),
                    Ok(false) => return Err(SurveyError::Foreign(dir.clone())),
                    Err(error) => return Err(SurveyError::Io(dir.clone(), error)),
                },
                Err(error) => {
                    tracing::error!(
                        path = %dir.join(META_PROPERTIES).display(),
                        %error,
                        "Error while reading meta.properties file"
                    );
                    ensemble.errors.push(dir.clone());
                }
            }
        }
        ensemble.formatted.sort_by(|a, b| a.0.cmp(&b.0));
        ensemble.errors.sort();
        Ok(ensemble)
    }

    /// Checks that the formatted directories agree with each other and with
    /// `--cluster-id` and `--node-id`, as Kafka's `MetaPropertiesEnsemble.verify`
    /// does in `Formatter.doFormat`, and returns the cluster id of the set:
    /// `expected` when given, else the first formatted directory's.
    ///
    /// Each error message is Kafka's, word for word.
    pub(super) fn verify(
        &self,
        expected: Option<ClusterId>,
        node_id: i32,
    ) -> Result<Option<ClusterId>, String> {
        verify_ensemble(
            self.formatted
                .iter()
                .map(|(dir, meta)| (dir.as_path(), meta)),
            expected,
            Some(node_id),
        )
    }
}

/// Whether `dir` is absent, empty, or holds only what an interrupted run
/// writes before `meta.properties`.
fn holds_only_partial_output(dir: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let file_type = entry.file_type()?;
        let own = if file_type.is_file() {
            PARTIAL_FILES.iter().any(|own| name == *own)
        } else if file_type.is_dir() && name == CLUSTER_METADATA {
            holds_only_the_zero_checkpoint(&entry.path())?
        } else {
            false
        };
        if !own {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether the metadata partition directory holds nothing but the
/// offset-zero checkpoint.
fn holds_only_the_zero_checkpoint(partition_dir: &Path) -> io::Result<bool> {
    for entry in std::fs::read_dir(partition_dir)? {
        let entry = entry?;
        if entry.file_name() != ZERO_CHECKPOINT_NAME || !entry.file_type()?.is_file() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Removes what an interrupted run left in `dir`.
///
/// Call it only on a directory that [`Ensemble::load`] put in the empty set,
/// which holds nothing else.
pub(super) fn remove_partial_output(dir: &Path) -> io::Result<()> {
    for name in PARTIAL_FILES {
        match std::fs::remove_file(dir.join(name)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
    }
    match std::fs::remove_dir_all(dir.join(CLUSTER_METADATA)) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use uuid::Uuid;

    use super::*;
    use crate::{ids::DirectoryId, meta_properties::node_id_mismatch_message};

    fn meta(cluster: u128, node_id: i32, directory: u128) -> MetaProperties {
        MetaProperties {
            cluster_id: ClusterId(Uuid::from_u128(cluster)),
            node_id,
            directory_id: Some(DirectoryId(Uuid::from_u128(directory))),
        }
    }

    fn formatted(entries: &[(&str, MetaProperties)]) -> Ensemble {
        Ensemble {
            formatted: entries
                .iter()
                .map(|(dir, meta)| (PathBuf::from(dir), *meta))
                .collect(),
            ..Ensemble::default()
        }
    }

    /// A `verify` case: what it is, the formatted set, `--cluster-id`, and the
    /// expected result.
    type Case = (&'static str, Ensemble, Option<ClusterId>, Verdict);
    type Verdict = Result<Option<ClusterId>, String>;

    /// The formatted set agrees with `--cluster-id` and `--node-id`, with
    /// Kafka's message for each way it can fail. `verify_ensemble` has the
    /// full table of Kafka's checks; these cases pin what `format` passes
    /// to it.
    #[test]
    fn verify_matches_kafka_meta_properties_ensemble() {
        let c1 = ClusterId(Uuid::from_u128(0xc1));
        let c2 = ClusterId(Uuid::from_u128(0xc2));
        // (what, formatted set, --cluster-id, expected result), with --node-id 1
        let cases: Vec<Case> = vec![
            ("nothing formatted", formatted(&[]), None, Ok(None)),
            (
                "nothing formatted, id given",
                formatted(&[]),
                Some(c1),
                Ok(Some(c1)),
            ),
            (
                "the set supplies the id",
                formatted(&[("/a", meta(0xc1, 1, 0x1000)), ("/b", meta(0xc1, 1, 0x2000))]),
                None,
                Ok(Some(c1)),
            ),
            (
                "the given id disagrees",
                formatted(&[("/a", meta(0xc1, 1, 0x1000))]),
                Some(c2),
                Err(format!(
                    "Invalid cluster.id in: /a/meta.properties. Expected {c2}, but read {c1}"
                )),
            ),
            (
                "another node's directory",
                formatted(&[("/a", meta(0xc1, 2, 0x1000))]),
                Some(c1),
                Err(node_id_mismatch_message()
                    .stored(2)
                    .previous(1)
                    .path(Path::new("/a/meta.properties"))
                    .call()),
            ),
        ];
        for (what, ensemble, expected, want) in cases {
            check!(ensemble.verify(expected, 1) == want, "{what}");
        }
    }

    /// Only what a run writes before `meta.properties` makes a directory with
    /// no marker count as empty.
    #[test]
    fn a_directory_is_empty_only_when_it_holds_partial_output() {
        fn checkpoint(root: &Path) -> PathBuf {
            root.join(CLUSTER_METADATA).join(ZERO_CHECKPOINT_NAME)
        }
        // (what, files to create relative to the directory, empty?)
        type Layout = fn(&Path) -> Vec<PathBuf>;
        let cases: [(&str, Layout, bool); 7] = [
            ("nothing", |_| vec![], true),
            (
                "the bootstrap files",
                |d| vec![d.join("bootstrap.json"), d.join("bootstrap.records.bin")],
                true,
            ),
            (
                "a half-written marker",
                |d| vec![d.join(META_PROPERTIES_TMP)],
                true,
            ),
            ("the offset-zero checkpoint", |d| vec![checkpoint(d)], true),
            (
                "someone else's file",
                |d| vec![d.join("orders-0").join("0.log")],
                false,
            ),
            (
                "a metadata log",
                |d| vec![d.join(CLUSTER_METADATA).join("00000000000000000000.log")],
                false,
            ),
            (
                "a stray top-level file",
                |d| vec![d.join("notes.txt")],
                false,
            ),
        ];
        for (what, layout, empty) in cases {
            let tmp = tempfile::tempdir().expect("tempdir");
            let dir = tmp.path().join("data");
            std::fs::create_dir_all(&dir).expect("mkdir");
            for file in layout(&dir) {
                std::fs::create_dir_all(file.parent().expect("parent")).expect("mkdir");
                std::fs::write(&file, b"x").expect("write");
            }
            check!(
                holds_only_partial_output(&dir).expect("survey") == empty,
                "{what}"
            );
            if empty {
                remove_partial_output(&dir).expect("remove");
                check!(
                    std::fs::read_dir(&dir).expect("list").next().is_none(),
                    "{what}: the partial output is gone"
                );
            }
        }
        check!(holds_only_partial_output(Path::new("/nonexistent/krabka-format")).expect("survey"));
    }
}
