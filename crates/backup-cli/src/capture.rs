//! Finding the two restore inputs that live on a node's disk, and naming the
//! capture they are written into.
//!
//! Neither file has a stable name. The RLMM snapshot does — it is always
//! `<log.dir>/remote-log-metadata/snapshot` — but the controller metadata
//! checkpoint is `<end-offset>-<epoch>.checkpoint` and there are several of
//! them, in one of two directories under the metadata log directory,
//! depending on whether the node runs a controller. Picking the right one is
//! this module's whole job, and it is a pure function over directory listings
//! so a test can pin the choice without a broker.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::manifest::CAPTURE_ROOT;

/// The RLMM snapshot's path under a log directory.
pub const RLMM_SNAPSHOT_RELATIVE: &str = "remote-log-metadata/snapshot";

/// Where a node that runs a controller keeps its metadata checkpoints, under
/// the metadata log directory: Kafka's metadata partition directory.
pub const CONTROLLER_CHECKPOINT_DIR: &str = "__cluster_metadata-0";

/// Where a broker-only node keeps the checkpoints its metadata observer wrote,
/// under the metadata log directory. They sit in a subdirectory of
/// `__cluster_metadata-0` and never directly in it, because a controller must
/// not load one.
pub const OBSERVER_CHECKPOINT_DIR: &str = "__cluster_metadata-0/observer";

/// Suffix of a KIP-630 checkpoint artifact.
const CHECKPOINT_SUFFIX: &str = ".checkpoint";

/// Width of the end-offset field in a checkpoint name.
const OFFSET_WIDTH: usize = 20;

/// Width of the epoch field in a checkpoint name.
const EPOCH_WIDTH: usize = 10;

/// The `(end_offset, epoch)` a checkpoint file name encodes, or `None` when
/// the name is not a checkpoint.
///
/// The encoding is `<end_offset:020>-<epoch:010>.checkpoint`, which
/// `krabka-raft`'s `checkpoint_name` writes. A name that parses to numbers but
/// does not re-encode to itself is rejected, so a hand-made `1-0.checkpoint`
/// is not mistaken for an artifact the controller wrote.
#[must_use]
pub fn checkpoint_id(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(CHECKPOINT_SUFFIX)?;
    let (offset, epoch) = stem.split_once('-')?;
    if offset.len() != OFFSET_WIDTH || epoch.len() != EPOCH_WIDTH {
        return None;
    }
    Some((offset.parse().ok()?, epoch.parse().ok()?))
}

/// The newest checkpoint among `names`, by `(end_offset, epoch)`.
///
/// The end offset orders first: a checkpoint taken further along the metadata
/// log holds more of it, whatever epoch wrote it.
#[must_use]
pub fn newest_checkpoint(names: &[String]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| checkpoint_id(name).map(|id| (id, name)))
        .max_by_key(|(id, _)| *id)
        .map(|(_, name)| name.clone())
}

/// Every file name directly inside `dir`. An absent or unreadable directory is
/// an empty list: a node can lack either checkpoint directory, and that is not
/// a failure.
#[must_use]
pub fn file_names(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(ToOwned::to_owned))
        .collect()
}

/// The newest metadata checkpoint under `log_dir`, the node's metadata log
/// directory, looking in the controller's directory and then in the
/// observer's.
///
/// A node that runs a controller has both; the controller's is authoritative
/// and is preferred at an equal id, because the observer records a placeholder
/// epoch in a checkpoint it wrote itself.
///
/// # Panics
///
/// Panics if [`newest_checkpoint`] returns a name that [`checkpoint_id`] then
/// rejects, which the two functions cannot do: the first selects only names the
/// second parsed.
#[must_use]
pub fn newest_metadata_checkpoint(log_dir: &Path) -> Option<PathBuf> {
    let mut best: Option<((u64, u64), PathBuf)> = None;
    for subdir in [CONTROLLER_CHECKPOINT_DIR, OBSERVER_CHECKPOINT_DIR] {
        let dir = log_dir.join(subdir);
        let Some(name) = newest_checkpoint(&file_names(&dir)) else {
            continue;
        };
        let id = checkpoint_id(&name).expect("newest_checkpoint returns a parseable name");
        if best.as_ref().is_none_or(|(seen, _)| id > *seen) {
            best = Some((id, dir.join(name)));
        }
    }
    best.map(|(_, path)| path)
}

/// The id a capture taken at `now_ms` carries, which is also its directory
/// name inside the archive.
///
/// The zero-padded epoch millisecond orders captures across timestamps; a random UUID suffix distinguishes concurrent captures and repeated clock values. Legacy timestamp-only names retain their position in the same listing, and `--capture latest` still needs only a lexicographic maximum.
#[must_use]
pub fn capture_id(now_ms: u64) -> String {
    format!("{now_ms:016}-{}", Uuid::new_v4().simple())
}

/// The archive-relative key of one artifact inside one capture.
#[must_use]
pub fn capture_key(capture_id: &str, artifact: &str) -> String {
    format!("{CAPTURE_ROOT}/{capture_id}/{artifact}")
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        capture_id, capture_key, checkpoint_id, newest_checkpoint, newest_metadata_checkpoint,
    };

    #[test]
    fn only_the_canonical_fixed_width_name_is_a_checkpoint() {
        let cases = [
            (
                "00000000000000123456-0000000042.checkpoint",
                Some((123_456, 42)),
            ),
            ("00000000000000000000-0000000000.checkpoint", Some((0, 0))),
            // Too few digits in either field: not a name the controller wrote.
            ("123456-42.checkpoint", None),
            ("00000000000000123456-42.checkpoint", None),
            // Not a checkpoint at all.
            ("snapshot", None),
            ("high-watermark.checkpoint", None),
            ("00000000000000123456-0000000042.checkpoint.tmp", None),
        ];
        for (name, expected) in cases {
            check!(checkpoint_id(name) == expected, "for {name}");
        }
    }

    #[test]
    fn the_newest_checkpoint_orders_by_end_offset_before_epoch() {
        let names = vec![
            "00000000000000000010-0000000009.checkpoint".to_owned(),
            "00000000000000000200-0000000001.checkpoint".to_owned(),
            "00000000000000000200-0000000002.checkpoint".to_owned(),
            "snapshot".to_owned(),
        ];
        check!(
            newest_checkpoint(&names)
                == Some("00000000000000000200-0000000002.checkpoint".to_owned())
        );
    }

    #[test]
    fn a_directory_with_no_checkpoint_has_no_newest_one() {
        check!(newest_checkpoint(&["snapshot".to_owned()]) == None);
        check!(newest_checkpoint(&[]) == None);
    }

    #[test]
    fn an_observer_checkpoint_is_found_when_there_is_no_controller_directory() {
        let log_dir = tempfile::tempdir().expect("log dir");
        let observer = log_dir.path().join("__cluster_metadata-0/observer");
        std::fs::create_dir_all(&observer).expect("create the observer dir");
        let newest = observer.join("00000000000000000042-0000000000.checkpoint");
        std::fs::write(&newest, b"bytes").expect("write a checkpoint");
        std::fs::write(
            observer.join("00000000000000000009-0000000000.checkpoint"),
            b"older",
        )
        .expect("write an older checkpoint");

        check!(newest_metadata_checkpoint(log_dir.path()) == Some(newest));
    }

    #[test]
    fn the_controller_directory_wins_when_both_hold_the_same_id() {
        let log_dir = tempfile::tempdir().expect("log dir");
        let name = "00000000000000000042-0000000000.checkpoint";
        for subdir in ["__cluster_metadata-0", "__cluster_metadata-0/observer"] {
            let dir = log_dir.path().join(subdir);
            std::fs::create_dir_all(&dir).expect("create a checkpoint dir");
            std::fs::write(dir.join(name), b"bytes").expect("write a checkpoint");
        }

        check!(
            newest_metadata_checkpoint(log_dir.path())
                == Some(log_dir.path().join("__cluster_metadata-0").join(name))
        );
    }

    #[test]
    fn a_log_directory_with_no_checkpoint_yields_none() {
        let log_dir = tempfile::tempdir().expect("log dir");
        check!(newest_metadata_checkpoint(log_dir.path()) == None);
    }

    #[test]
    fn capture_ids_sort_lexicographically_in_time_order() {
        let early = capture_id(1_700_000_000_000);
        let late = capture_id(1_700_000_000_001);
        check!(early < late);
        check!(capture_id(0) < early);
        check!(
            capture_key(&early, "manifest.json") == format!("restore-inputs/{early}/manifest.json")
        );
        check!(early.starts_with("0001700000000000-"));
        check!(early != capture_id(1_700_000_000_000));
        check!("0001700000000000" < early.as_str());
        check!(early.as_str() < "0001700000000001");
    }
}
