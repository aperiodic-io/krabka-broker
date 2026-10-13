//! Open-time recovery for log directories.
//!
//! - `Segment::open_active` handles partial trailing batches in the
//!   active segment.
//! - [`swap_orphan_recover`] finishes or aborts the swap set that an
//!   interrupted [`crate::compact::atomic_swap`] left behind.
//! - [`deleted_orphan_recover`] handles `.deleted` tombstones and stranded
//!   sidecars that an interrupted
//!   [`crate::retention::delete_segment_files`] left behind.

pub use self::swap::swap_orphan_recover;
use {
    crate::{
        error::LogError,
        io::LogIo,
        name,
        retention::{DELETED_SUFFIX, remove_optional_counted},
    },
    std::{collections::HashSet, path::Path},
    tracing::instrument,
};

pub mod swap;

/// The sidecar extensions a segment carries beside its `.log`.
const SIDECAR_EXTENSIONS: [&str; 4] = ["index", "timeindex", "txnindex", "stampindex"];

/// Reclaim what an interrupted segment deletion left in `dir`.
///
/// [`crate::retention::delete_segment_files`] renames every file of a segment
/// to a `<name>.deleted` tombstone before it unlinks any of them, so a failure
/// part-way through leaves one of two things behind:
///
/// - a tombstone nothing reads, which is unlinked here; or
/// - a sidecar whose `.log` is already gone. [`crate::Log::open`] enumerates
///   segments by `.log` filename, so such a sidecar is invisible to every
///   later pass and is never reclaimed. It is unlinked here too.
///
/// This function is idempotent, and is safe to call on every `Log::open`. It
/// runs after [`swap_orphan_recover`], which is what promotes a `.log.swap`
/// into the `.log` that keeps its sidecars alive.
///
/// # Errors
/// Returns an error when the directory scan or an unlink fails.
#[instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), reclaimed = tracing::field::Empty),
    err,
)]
pub fn deleted_orphan_recover(io: &dyn LogIo, dir: &Path) -> Result<(), LogError> {
    let mut log_bases: HashSet<i64> = HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Ok(base) = name::parse_log_filename(name) {
            log_bases.insert(base);
        }
        names.push(name.to_owned());
    }

    let mut reclaimed = 0usize;
    for name in names {
        if !is_deleted_tombstone(&name) && !is_stranded_sidecar(&name, &log_bases) {
            continue;
        }
        if remove_optional_counted(io, &dir.join(&name))? {
            reclaimed += 1;
        }
    }
    tracing::Span::current().record("reclaimed", reclaimed);
    Ok(())
}

/// `true` for `<20 digits>.<ext>.deleted`, the tombstone retention renames to.
fn is_deleted_tombstone(value: &str) -> bool {
    let Some(rest) = value.strip_suffix(DELETED_SUFFIX) else {
        return false;
    };
    let Some(rest) = rest.strip_suffix('.') else {
        return false;
    };
    // Deferred deletion adds a numeric generation to avoid collisions when
    // a compaction replaces the same base more than once before reclamation.
    let rest = rest
        .rsplit_once('.')
        .filter(|(_, generation)| {
            !generation.is_empty() && generation.bytes().all(|b| b.is_ascii_digit())
        })
        .map_or(rest, |(path, _)| path);
    ["log"]
        .into_iter()
        .chain(SIDECAR_EXTENSIONS)
        .any(|ext| canonical_base(rest, ext).is_some())
}

/// `true` for a canonical sidecar name whose segment `.log` is not present.
fn is_stranded_sidecar(value: &str, log_bases: &HashSet<i64>) -> bool {
    SIDECAR_EXTENSIONS
        .into_iter()
        .filter_map(|ext| canonical_base(value, ext))
        .any(|base| !log_bases.contains(&base))
}

/// The base offset `value` names when it is exactly `<20 digits>.<ext>`.
fn canonical_base(value: &str, ext: &str) -> Option<i64> {
    let stem = value.strip_suffix(ext)?.strip_suffix('.')?;
    if stem.len() != name::FILENAME_DIGITS {
        return None;
    }
    let base = stem.parse::<i64>().ok()?;
    (base >= 0 && name::format_base_offset(base) == stem).then_some(base)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::io::FileIo, assert2::check, tempfile::tempdir};

    fn touch(path: &std::path::Path) { std::fs::File::create(path).unwrap(); }

    /// The sweep reclaims both shapes an interrupted deletion leaves: a
    /// `.deleted` tombstone, and a sidecar whose `.log` is already gone.
    #[test]
    fn deleted_orphan_recover_reclaims_tombstones_and_stranded_sidecars() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // A live segment at base 0, whose sidecars must survive untouched.
        touch(&name::log_path(p, 0));
        touch(&name::index_path(p, 0));
        touch(&name::timeindex_path(p, 0));
        touch(&name::stampindex_path(p, 0));
        // Tombstones from a deletion that renamed but never unlinked.
        let tombstones = [
            p.join("00000000000000000010.log.deleted"),
            p.join("00000000000000000010.index.deleted"),
            p.join("00000000000000000010.timeindex.deleted"),
        ];
        for path in &tombstones {
            touch(path);
        }
        // Sidecars of a segment whose `.log` did get unlinked. The `.log`-keyed
        // scan cannot see these, so nothing else would ever reclaim them.
        let stranded = [
            name::index_path(p, 20),
            name::timeindex_path(p, 20),
            name::txnindex_path(p, 20),
            name::stampindex_path(p, 20),
        ];
        for path in &stranded {
            touch(path);
        }
        // Files the sweep has no business touching.
        let unrelated = [
            p.join("leader-epoch-checkpoint"),
            p.join("00000000000000000000.snapshot"),
            p.join("not-a-segment.index"),
        ];
        for path in &unrelated {
            touch(path);
        }

        deleted_orphan_recover(&FileIo, p).unwrap();

        for path in tombstones.iter().chain(stranded.iter()) {
            check!(!path.exists(), "reclaimed: {}", path.display());
        }
        for path in unrelated.iter().chain(
            [
                name::log_path(p, 0),
                name::index_path(p, 0),
                name::timeindex_path(p, 0),
                name::stampindex_path(p, 0),
            ]
            .iter(),
        ) {
            check!(path.exists(), "left alone: {}", path.display());
        }

        // Idempotent: a second sweep over the healed directory changes nothing.
        deleted_orphan_recover(&FileIo, p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::index_path(p, 0).exists());
    }

    #[test]
    fn deleted_orphan_recover_reports_a_directory_scan_failure() {
        let dir = tempdir().unwrap();

        check!(matches!(
            deleted_orphan_recover(&crate::io::FileIo, &dir.path().join("missing")),
            Err(LogError::Io(_))
        ));
    }

    #[test]
    fn canonical_base_rejects_a_leading_sign() {
        check!(canonical_base("+0000000000000000001.log", "log").is_none());
        check!(canonical_base("00000000000000000001.log", "log") == Some(1));
    }
}
