//! The on-disk protocol that promotes a compaction's survivor segment over the
//! segments it replaces, and the open-time recovery that finishes or aborts it
//! after a crash.
//!
//! The protocol is Kafka's `LocalLog.replaceSegments`, and the recovery is the
//! swap handling of `LogLoader.load`:
//!
//! 1. The rewrite writes the survivor as `<base>.<ext>.cleaned` files, and
//!    [`crate::compact::atomic_swap`] fsyncs them.
//! 2. The sidecars are renamed from `.cleaned` to `.swap` and the directory is
//!    fsynced; then the log is renamed and the directory fsynced again. That
//!    durable `.log.swap` is the commit point.
//! 3. Every replaced segment is deleted, and the directory fsynced.
//! 4. The log is renamed from `.swap` to its final name and the directory
//!    fsynced; then the sidecars are, and the directory fsynced again.
//!
//! Nothing is deleted before the commit, so an uncommitted swap can always be
//! thrown away. After it, the swap may be the only copy of its records, so
//! recovery always completes it. The kernel that decides between the two is
//! [`krabka_verified::local_recovery::local_recovery_swap_action`].

use {
    super::canonical_base,
    crate::{
        error::LogError,
        io::{FileIo, IoTarget, LogIo},
        name,
    },
    krabka_protocol::records::{HEADER_LEN, RecordBatchHeader},
    krabka_verified::local_recovery::{
        LocalRecoverySwapAction, LocalRecoverySwapFacts, local_recovery_batch_step,
        local_recovery_swap_action, local_recovery_swap_replaces,
    },
    std::{
        collections::BTreeSet,
        fs::File,
        io::{ErrorKind, Read, Seek, SeekFrom},
        path::{Path, PathBuf},
    },
    tracing::instrument,
    zerocopy::FromBytes,
};

/// The file kinds a swap set carries: the log, then its sidecars.
const SWAP_EXTENSIONS: [&str; 4] = ["log", "index", "timeindex", "txnindex"];

/// The sidecars of a swap set. The `.txnindex` is optional: the rewrite writes
/// one only when an aborted transaction survives.
const SWAP_SIDECARS: [&str; 3] = ["index", "timeindex", "txnindex"];

/// The files of a replaced segment that completing a swap deletes.
const REPLACED_EXTENSIONS: [&str; 4] = ["log", "index", "timeindex", "txnindex"];

/// The bytes of a v2 batch that its `batch_length` does not count: the base
/// offset and the length field itself.
const LOG_OVERHEAD: u64 = 12;

/// `<base>.<ext>.swap`: a survivor file after the commit.
#[must_use]
pub fn swap_path(dir: &Path, base: i64, ext: &str) -> PathBuf {
    suffixed_path(dir, base, ext, ".swap")
}

/// `<base>.<ext>.cleaned`: a survivor file before the commit, where the rewrite
/// writes it.
#[must_use]
pub fn cleaned_path(dir: &Path, base: i64, ext: &str) -> PathBuf {
    suffixed_path(dir, base, ext, ".cleaned")
}

fn suffixed_path(dir: &Path, base: i64, ext: &str, suffix: &str) -> PathBuf {
    dir.join(format!("{}.{ext}{suffix}", name::format_base_offset(base)))
}

fn final_path(dir: &Path, base: i64, ext: &str) -> PathBuf { suffixed_path(dir, base, ext, "") }

/// Unlink `path`, which may already be gone.
///
/// # Errors
/// Returns every unlink failure except a missing file.
pub fn remove_if_present(io: &dyn LogIo, path: &Path) -> Result<(), LogError> {
    match io.remove_file(IoTarget::CompactionSwap, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogError::Io(error)),
    }
}

/// Step 3 for one segment: delete every file of the segment at `base` that a
/// swap replaces.
///
/// # Errors
/// Returns the first unlink that fails for a reason other than a missing file.
pub fn remove_replaced_segment(io: &dyn LogIo, dir: &Path, base: i64) -> Result<(), LogError> {
    for ext in REPLACED_EXTENSIONS {
        remove_if_present(io, &final_path(dir, base, ext))?;
    }
    Ok(())
}

/// Step 4, first half: rename the committed `.log.swap` onto its final name,
/// and make that durable before any sidecar moves.
///
/// Recovery reads a surviving `.log.swap` as "no step-4 rename has happened
/// yet" and deletes the final-name files at the base. A sidecar renamed ahead
/// of a log rename the crash lost would be deleted with them.
///
/// # Errors
/// Returns the rename or directory-fsync failure.
pub fn promote_log(io: &dyn LogIo, dir: &Path, base: i64) -> Result<(), LogError> {
    io.rename(
        IoTarget::CompactionSwap,
        &swap_path(dir, base, "log"),
        &final_path(dir, base, "log"),
    )?;
    io.sync_dir(dir)?;
    Ok(())
}

/// Step 4, second half: rename every sidecar swap onto its final name.
///
/// An offset or time index with neither form is created empty:
/// `Segment::open_unscanned` accepts an empty sparse index, and the open-time tail scan
/// rebuilds it. A missing `.txnindex` is left missing, because a segment with
/// no aborted transaction has none.
///
/// # Errors
/// Returns the rename, create or directory-fsync failure.
pub fn promote_sidecars(io: &dyn LogIo, dir: &Path, base: i64) -> Result<(), LogError> {
    for ext in SWAP_SIDECARS {
        let swap = swap_path(dir, base, ext);
        let target = final_path(dir, base, ext);
        if swap.exists() {
            io.rename(IoTarget::CompactionSwap, &swap, &target)?;
        } else if ext != "txnindex" && !target.exists() {
            File::create(&target)?;
        }
    }
    io.sync_dir(dir)?;
    Ok(())
}

/// Heal every swap set an interrupted [`crate::compact::atomic_swap`] left in
/// `dir`, then delete every `.cleaned` file.
///
/// This function is idempotent. It is safe to call on every `Log::open`.
///
/// # Errors
/// Returns an error when the directory scan or a file operation fails, when a
/// committed `.log.swap` does not hold whole, contiguous batches, or when
/// sidecar swaps have no log in any form.
pub fn swap_orphan_recover(dir: &Path) -> Result<(), LogError> { recover_swaps(&FileIo, dir) }

/// [`swap_orphan_recover`] through an injectable I/O boundary.
#[instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), swaps = tracing::field::Empty),
    err,
)]
pub fn recover_swaps(io: &dyn LogIo, dir: &Path) -> Result<(), LogError> {
    let listing = Listing::scan(dir)?;
    tracing::Span::current().record("swaps", listing.swap_bases.len());
    for &base in &listing.swap_bases {
        let facts = LocalRecoverySwapFacts {
            log_cleaned_exists: listing.cleaned.contains(&(base, "log")),
            log_swap_exists: listing.log_swap_bases.contains(&base),
            final_log_exists: listing.log_bases.contains(&base),
        };
        match local_recovery_swap_action(facts) {
            LocalRecoverySwapAction::AbortSwap => {
                for ext in SWAP_EXTENSIONS {
                    remove_if_present(io, &swap_path(dir, base, ext))?;
                }
            }
            LocalRecoverySwapAction::CompleteSwap => {
                let next = swap_next_offset(&swap_path(dir, base, "log"), base)?;
                let segments = listing.log_bases.iter().copied().chain([base]);
                for segment in segments.collect::<BTreeSet<_>>() {
                    if local_recovery_swap_replaces(base, next, segment) {
                        remove_replaced_segment(io, dir, segment)?;
                    }
                }
                io.sync_dir(dir)?;
                promote_log(io, dir, base)?;
                promote_sidecars(io, dir, base)?;
            }
            LocalRecoverySwapAction::PromoteSidecars => promote_sidecars(io, dir, base)?,
            LocalRecoverySwapAction::Reject => {
                return Err(LogError::Corrupt(format!(
                    "swap sidecars for segment {base} have no log file"
                )));
            }
        }
    }
    if !listing.cleaned.is_empty() {
        // An aborted swap's unlinks must be durable before its `.log.cleaned`
        // goes: a `.log.swap` that outlived it would read as committed.
        io.sync_dir(dir)?;
        for &(base, ext) in &listing.cleaned {
            remove_if_present(io, &cleaned_path(dir, base, ext))?;
        }
    }
    Ok(())
}

/// The exclusive next offset of the committed swap log at `path`, whose base
/// is `base`: Kafka's `LogSegment.readNextOffset` over the `.swap` segment.
///
/// A committed swap was fsynced before its commit rename, so it holds whole,
/// contiguous batches; anything else is corruption rather than a torn tail.
fn swap_next_offset(path: &Path, base: i64) -> Result<i64, LogError> {
    let corrupt = || LogError::Corrupt(format!("committed swap {} is malformed", path.display()));
    let mut file = File::open(path)?;
    let file_end = file.metadata()?.len();
    let mut header = [0u8; HEADER_LEN];
    let mut position = 0u64;
    let mut next = base;
    while position < file_end {
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(&mut header).map_err(|error| {
            if error.kind() == ErrorKind::UnexpectedEof {
                corrupt()
            } else {
                LogError::Io(error)
            }
        })?;
        let view = RecordBatchHeader::ref_from_bytes(&header).map_err(|_| corrupt())?;
        let encoded_len = u64::try_from(view.batch_length.get())
            .ok()
            .and_then(|length| length.checked_add(LOG_OVERHEAD))
            .ok_or_else(corrupt)?;
        let step = local_recovery_batch_step(
            position,
            file_end,
            next,
            view.base_offset.get(),
            view.last_offset_delta.get(),
            encoded_len,
        )
        .ok_or_else(corrupt)?;
        position = step.valid_end;
        next = step.next_offset;
    }
    Ok(next)
}

/// The swap-relevant names of one directory listing.
struct Listing {
    /// Every base with at least one `.swap` file, ascending.
    swap_bases: BTreeSet<i64>,
    /// Every base with a `.log.swap`.
    log_swap_bases: BTreeSet<i64>,
    /// Every base with a final `.log`.
    log_bases: BTreeSet<i64>,
    /// Every `.cleaned` file, by base and extension.
    cleaned: BTreeSet<(i64, &'static str)>,
}

impl Listing {
    fn scan(dir: &Path) -> Result<Self, LogError> {
        let mut listing = Self {
            swap_bases: BTreeSet::new(),
            log_swap_bases: BTreeSet::new(),
            log_bases: BTreeSet::new(),
            cleaned: BTreeSet::new(),
        };
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(value) = file_name.to_str() else {
                continue;
            };
            if let Some((base, ext)) = parse_suffixed(value, ".swap") {
                listing.swap_bases.insert(base);
                if ext == "log" {
                    listing.log_swap_bases.insert(base);
                }
            } else if let Some(file) = parse_suffixed(value, ".cleaned") {
                listing.cleaned.insert(file);
            } else if let Ok(base) = name::parse_log_filename(value) {
                listing.log_bases.insert(base);
            }
        }
        Ok(listing)
    }
}

/// The base and extension `value` names when it is exactly
/// `<20 digits>.<ext><suffix>` for one of the swap-set extensions.
fn parse_suffixed(value: &str, suffix: &str) -> Option<(i64, &'static str)> {
    let stem = value.strip_suffix(suffix)?;
    SWAP_EXTENSIONS
        .into_iter()
        .find_map(|ext| canonical_base(stem, ext).map(|base| (base, ext)))
}

#[cfg(test)]
mod tests;
