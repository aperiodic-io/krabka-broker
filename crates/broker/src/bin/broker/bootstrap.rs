//! The `Bootstrap` or `Rejoin` decision the broker makes from the state it
//! finds in its metadata log directory.

use {krabka_broker::BootstrapMode, std::path::Path};

/// Pick `Bootstrap` for a fresh cluster or `Rejoin` for a restart on existing
/// state. The choice depends on whether the metadata partition directory,
/// `<metadata_log_dir>/__cluster_metadata-0`, holds durable raft state.
///
/// The controller keeps its log segments, KIP-630 checkpoints and
/// `quorum-state` file in that directory, as Kafka does under
/// `metadata.log.dir`. On the first boot it holds at most the offset-zero
/// checkpoint `krabka format` wrote. On every later boot it holds a
/// `quorum-state` file or a non-empty segment from the previous run.
///
/// The check is the controller's own, [`krabka_raft::metadata_log_nonempty`],
/// so this choice can never disagree with `Controller::start_with_listener`'s
/// mode validation. `KraftController::open` creates an empty active segment
/// before the first commit, and a node killed mid-election on a multi-node
/// cold start has that segment but no `quorum-state`. It reads as fresh and
/// re-Bootstraps, rather than picking Rejoin and dying with "Rejoin requires
/// non-empty raft log" in a crashloop.
pub fn detect_bootstrap_mode(metadata_log_dir: &Path) -> BootstrapMode {
    if krabka_raft::metadata_log_nonempty(&krabka_raft::metadata_partition_dir(metadata_log_dir)) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, tempfile::tempdir};

    /// Only a `quorum-state` file or a segment that holds bytes, in the
    /// metadata partition directory, marks a restart.
    #[test]
    fn rejoin_needs_durable_raft_state_in_the_metadata_partition_directory() {
        /// Files relative to the metadata log directory, with their bytes.
        type Files = &'static [(&'static str, &'static [u8])];
        // (what, files, expected mode)
        let cases: [(&str, Files, BootstrapMode); 7] = [
            ("an empty directory", &[], BootstrapMode::Bootstrap),
            (
                "only what krabka format wrote",
                &[
                    ("bootstrap.json", b"{}"),
                    (
                        "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
                        b"snapshot",
                    ),
                ],
                BootstrapMode::Bootstrap,
            ),
            (
                "an empty segment from a node killed mid-election",
                &[("__cluster_metadata-0/00000000000000000000.log", b"")],
                BootstrapMode::Bootstrap,
            ),
            (
                "a persisted quorum state",
                &[("__cluster_metadata-0/quorum-state", b"{}")],
                BootstrapMode::Rejoin,
            ),
            (
                "a segment that holds records",
                &[("__cluster_metadata-0/00000000000000000000.log", b"segment")],
                BootstrapMode::Rejoin,
            ),
            (
                "the old krabka layout",
                &[("__cluster_metadata/quorum-state", b"{}")],
                BootstrapMode::Bootstrap,
            ),
            (
                "a partition directory",
                &[("orders-0/00000000000000000000.log", b"segment")],
                BootstrapMode::Bootstrap,
            ),
        ];
        for (what, files, expected) in cases {
            let dir = tempdir().unwrap();
            for (path, contents) in files {
                let path = dir.path().join(path);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, contents).unwrap();
            }
            check!(detect_bootstrap_mode(dir.path()) == expected, "{what}");
        }
    }
}
