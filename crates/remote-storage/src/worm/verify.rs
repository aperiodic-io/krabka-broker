//! Standalone verification of a WORM archive.
//!
//! The verifier reads an archive with nothing but a listing credential and the
//! public half of the signing key. It needs no broker, no metadata manager, and
//! no cluster, because everything it checks is written into the archive:
//! [`crate::SegmentManifest`] carries the object digests, the chain position,
//! and the signature. An auditor who holds a read-only role can therefore
//! confirm the archive years after the broker that wrote it stopped running.
//!
//! # What each depth proves
//!
//! [`VerifyDepth::Shallow`] recomputes the chain, checks every signature, and
//! confirms that every object a manifest names exists with the recorded size.
//! It never downloads a segment body, so it is cheap enough to run continuously
//! against a large archive. It cannot see a body that was replaced with
//! different bytes of the same length.
//!
//! [`VerifyDepth::Deep`] downloads every object and recomputes its `SHA-256`.
//! It is the only depth that catches a same-size substitution, and it reads the
//! whole archive to do so.
//!
//! # What no depth proves
//!
//! Tail truncation. An attacker who removes the newest manifests, and every
//! object they name, leaves a shorter chain that verifies perfectly. Give
//! [`VerifyRequest::expect_head`] a head obtained outside the archive to close
//! that gap.
//!
//! # Hostile input
//!
//! The archive is attacker-controlled. Every manifest read goes through
//! [`read_capped`](krabka_object_store::read_capped) with the
//! [`MAX_MANIFEST_BYTES`] cap, so an oversized object is refused by a `HEAD`
//! before a byte is buffered, and no allocation is ever sized from a count a
//! manifest supplies. A damaged or hostile archive produces a report, never a
//! panic.

use {
    object_store::ObjectStore,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        sync::Arc,
    },
};

mod listing;
mod manifest_read;
mod objects;
mod partition;
mod report;
mod signature;
mod walk;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub use self::{
    manifest_read::MAX_MANIFEST_BYTES,
    report::{
        ArchiveVerifyReport, EpochSpan, ObjectProtectionReport, OffsetGap, PartitionVerifyReport,
        VerifyBreak,
    },
};
use {
    self::{listing::list_archive, partition::verify_partition},
    crate::{
        storage_manager::parse_partition_dir_name,
        worm::{
            error::WormError,
            manifest::{ChainHead, ObjectEntry},
        },
    },
};

/// A deep WORM verification and the signed object claims it authenticated.
///
/// A reader must still hash bytes fetched after this run against [`Self::objects`]:
/// the object store can replace a key between verification and consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedArchive {
    report: ArchiveVerifyReport,
    objects: BTreeMap<String, ObjectEntry>,
}

impl AuthenticatedArchive {
    /// The ordinary WORM verification report.
    #[must_use]
    pub fn report(&self) -> &ArchiveVerifyReport { &self.report }

    /// Signed size and digest claims, keyed by full object-store key.
    #[must_use]
    pub fn objects(&self) -> &BTreeMap<String, ObjectEntry> { &self.objects }
}

/// Public keys the verifier accepts, keyed by `key_id`.
///
/// A manifest names the key that signed it. The verifier checks the signature
/// against the key this set holds under that name and never against the key the
/// manifest carries, because an attacker who can rewrite a manifest can rewrite
/// the key beside it.
#[derive(Debug, Default)]
pub struct TrustedManifestKeys {
    keys: HashMap<String, Vec<u8>>,
}

impl TrustedManifestKeys {
    /// A set that trusts one raw Ed25519 public key under `key_id`.
    #[must_use]
    pub fn single(key_id: String, public_key: Vec<u8>) -> Self {
        Self::from_pairs([(key_id, public_key)])
    }

    /// A set built from every `(key_id, public_key)` pair.
    ///
    /// An archive that spans a key rotation carries manifests signed under
    /// each key it was written with, so verifying it in one run means
    /// trusting all of them at once. A later pair wins over an earlier one
    /// naming the same `key_id`.
    #[must_use]
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        Self {
            keys: pairs.into_iter().collect(),
        }
    }

    /// The raw Ed25519 public key registered under `key_id`.
    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&[u8]> { self.keys.get(key_id).map(Vec::as_slice) }

    /// `true` when no key is trusted.
    ///
    /// A run against an empty set still recomputes the chain and checks every
    /// object, and it counts every signed manifest as untrusted. It proves
    /// internal consistency and says nothing about who wrote the archive.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }
}

/// How hard the verifier works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerifyDepth {
    /// Read manifests, recompute the chain, check signatures, and `HEAD` every
    /// referenced object for existence and size. No segment body is downloaded.
    #[default]
    Shallow,
    /// Additionally download every object and recompute its `SHA-256`. The only
    /// check that catches a same-size body substitution.
    Deep,
}

/// What to verify, and against what expectation.
#[derive(Debug, Clone, Default)]
pub struct VerifyRequest {
    /// Key prefix inside the store. `None` verifies the whole store.
    pub prefix: Option<String>,
    /// Verify only the partitions of this topic.
    pub topic: Option<String>,
    /// Verify only this partition index.
    pub partition: Option<i32>,
    /// How much of each object to read.
    pub depth: VerifyDepth,
    /// Expected head of the newest manifest, obtained independently of the
    /// archive. Tail truncation leaves a shorter but internally perfect chain;
    /// nothing inside the archive detects it.
    ///
    /// A partition whose tip differs from this head is reported as a break, so
    /// a programmatic caller gets one `ok` to test. An archive with **no**
    /// partition left to check is the one case this cannot report, because the
    /// report has no partition to carry the break: check
    /// [`ArchiveVerifyReport::partitions`] for emptiness as well. The
    /// `krabka-worm-verify` binary leaves this `None` and compares the tips
    /// itself, because it grades a tip mismatch as its own outcome and not as
    /// tampering, and because it must also catch the emptied archive.
    pub expect_head: Option<ChainHead>,
    /// Treat an epoch restart as accepted rather than an attestation hole.
    ///
    /// Read by whoever grades the report. The chain walk always records every
    /// epoch it finds, and an epoch restart on its own never clears
    /// [`PartitionVerifyReport::ok`]: a restart is a hole in the attestation,
    /// not evidence of a rewrite.
    pub allow_epoch_restarts: bool,
    /// Exact object keys authenticated by another signed boundary.
    ///
    /// Restore uses this for diskless WAL objects whose claims live in the
    /// signed capture rather than a classic partition manifest. Every other
    /// unreferenced object remains an orphan.
    pub externally_authenticated_objects: BTreeSet<String>,
}

/// Verifies every partition the request selects.
///
/// The walk stops at the first break **per partition** and keeps going with the
/// other partitions, so one damaged partition does not hide the state of the
/// rest. It does no recovery and no truncation, so tail damage stays visible.
/// The report is deterministic: two runs against an unchanged archive produce
/// equal values.
///
/// # Errors
///
/// [`WormError`] **only** when the archive cannot be listed, a manifest object
/// cannot be read, or — under [`VerifyDepth::Deep`] — a listed object body
/// cannot be fetched. A *tampered* archive is a successful call with `ok ==
/// false`: "I could not look" and "I looked and it is broken" are different
/// outcomes, and the exit-code grading depends on the difference.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        prefix = request.prefix.as_deref().unwrap_or(""),
        depth = ?request.depth,
    )
)]
pub async fn verify_archive(
    store: &Arc<dyn ObjectStore>,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<ArchiveVerifyReport, WormError> {
    let (report, _) = verify_archive_inner(store, request, trusted).await?;
    Ok(report)
}

/// Deeply verifies an archive and returns the signed claims for every object
/// accepted by the chain walk.
///
/// This always uses [`VerifyDepth::Deep`], regardless of `request.depth`.
/// Callers must grade [`AuthenticatedArchive::report`] before consuming the
/// claims, then hash the bytes they consume against [`AuthenticatedArchive::objects`].
///
/// # Errors
///
/// Returns [`WormError`] under the same conditions as [`verify_archive`].
pub async fn authenticate_archive(
    store: &Arc<dyn ObjectStore>,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<AuthenticatedArchive, WormError> {
    let mut request = request.clone();
    request.depth = VerifyDepth::Deep;
    let (report, objects) = verify_archive_inner(store, &request, trusted).await?;
    Ok(AuthenticatedArchive { report, objects })
}

async fn verify_archive_inner(
    store: &Arc<dyn ObjectStore>,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<(ArchiveVerifyReport, BTreeMap<String, ObjectEntry>), WormError> {
    let mut listing = list_archive(store, request.prefix.as_deref()).await?;
    let global_orphan_objects = listing
        .iter()
        .filter(|(dir, _)| {
            dir.rsplit('/')
                .next()
                .and_then(parse_partition_dir_name)
                .is_none()
        })
        .flat_map(|(_, entries)| entries.keys())
        .filter(|key| {
            !request.externally_authenticated_objects.contains(*key)
                && !capture_namespace(key, request.prefix.as_deref())
        })
        .cloned()
        .collect();
    listing.retain(|dir, _| {
        dir.rsplit('/')
            .next()
            .and_then(parse_partition_dir_name)
            .is_some()
    });
    for entries in listing.values_mut() {
        entries.retain(|key, _| !request.externally_authenticated_objects.contains(key));
    }
    listing.retain(|_, entries| !entries.is_empty());
    let mut partitions = Vec::new();
    let mut objects = BTreeMap::new();
    for (dir, entries) in &listing {
        if let Some((report, verified)) =
            verify_partition(store, dir, entries, request, trusted).await?
        {
            partitions.push(report);
            objects.extend(verified);
        }
    }
    // The listing is a `BTreeMap` keyed by directory, so the partitions are
    // already in directory order. Sorting again states the guarantee locally.
    partitions.sort_by(|a, b| a.partition_dir.cmp(&b.partition_dir));
    Ok((
        ArchiveVerifyReport {
            partitions,
            global_orphan_objects,
        },
        objects,
    ))
}

fn capture_namespace(key: &str, prefix: Option<&str>) -> bool {
    let relative = prefix
        .and_then(|prefix| key.strip_prefix(prefix)?.strip_prefix('/'))
        .unwrap_or(key);
    relative
        .split('/')
        .any(|component| component == "restore-inputs")
}
