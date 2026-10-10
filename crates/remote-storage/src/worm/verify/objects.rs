//! Digest verification of the objects a manifest names.
//!
//! A shallow run proves only that each object exists at the recorded size. A
//! deep run streams the body and recomputes its `SHA-256`, which is the one
//! check that sees a same-size substitution. On a versioned bucket the
//! mismatch also asks whether the version the manifest pinned still holds the
//! recorded bytes, because that is what separates a recoverable segment from a
//! lost one.

use std::{collections::HashSet, sync::Arc};

use futures_util::TryStreamExt as _;
use krabka_verified::{
    WormDigestFacts, WormObjectAvailabilityFacts, WormObjectIdentityFacts, WormObjectSetDecision,
    WormObjectSetFacts, worm_object_set_decision,
};
use object_store::{GetOptions, ObjectStore, path::Path};
use sha2::{Digest as _, Sha256};

use super::{VerifyDepth, listing::DirListing};
use crate::{
    storage_manager::parse_segment_file_name,
    worm::{
        error::WormError,
        manifest::{MANIFEST_SUFFIX, ObjectEntry, SegmentManifest, Sha256Digest},
    },
};

/// Checks every object a manifest names, to the requested depth.
///
/// An object must live in the manifest's own partition directory. A manifest
/// that points somewhere else names an object this walk cannot account for, and
/// it counts as missing.
pub(super) async fn check_objects(
    store: &Arc<dyn ObjectStore>,
    manifest: &SegmentManifest,
    listing: &DirListing,
    depth: VerifyDepth,
) -> Result<Option<String>, WormError> {
    let segment = &manifest.body.segment;
    let listed_count = listing
        .keys()
        .filter(|key| {
            key.rsplit('/')
                .next()
                .and_then(parse_segment_file_name)
                .is_some_and(|parsed| {
                    parsed.base_offset == segment.start_offset
                        && parsed.segment_id == segment.segment_id
                        && parsed.suffix != MANIFEST_SUFFIX
                })
        })
        .count();
    let mut keys = HashSet::new();
    let mut unique_keys = true;
    let mut coordinates_match = true;
    let mut all_present = true;
    let mut sizes_match = true;
    let mut digests_match = true;
    let mut first_duplicate = None;
    let mut first_coordinate = None;
    let mut first_missing = None;
    let mut first_size = None;
    let mut first_digest = None;

    for object in &manifest.body.objects {
        if !keys.insert(object.key.as_str()) {
            unique_keys = false;
            first_duplicate.get_or_insert_with(|| object.key.clone());
        }

        let coordinate_matches = object
            .key
            .rsplit('/')
            .next()
            .and_then(parse_segment_file_name)
            .is_some_and(|parsed| {
                parsed.base_offset == segment.start_offset
                    && parsed.segment_id == segment.segment_id
                    && parsed.suffix == object.suffix
                    && parsed.suffix != MANIFEST_SUFFIX
            });
        if !coordinate_matches {
            coordinates_match = false;
            first_coordinate.get_or_insert_with(|| object.key.clone());
        }

        match listing.get(&object.key).copied() {
            None => {
                all_present = false;
                first_missing.get_or_insert_with(|| object.key.clone());
            }
            Some(size) => {
                if size != object.size_bytes {
                    sizes_match = false;
                    first_size.get_or_insert_with(|| {
                        format!(
                            "object `{}` is {size} bytes, the manifest records a size of {} bytes",
                            object.key, object.size_bytes
                        )
                    });
                }
                if depth == VerifyDepth::Deep {
                    let digest = object_digest(store, &object.key, None).await?;
                    if digest != object.sha256 {
                        digests_match = false;
                        let pinned = pinned_version_note(store, object).await;
                        first_digest.get_or_insert_with(|| {
                            format!(
                                "object `{}` hashes to {digest}, the manifest records {}{pinned}",
                                object.key, object.sha256
                            )
                        });
                    }
                }
            }
        }
    }

    let object_count = u64::try_from(manifest.body.objects.len()).unwrap_or(u64::MAX);
    let listed_count = u64::try_from(listed_count).unwrap_or(u64::MAX);
    let reason = match worm_object_set_decision(WormObjectSetFacts {
        object_count,
        listed_count,
        identity: WormObjectIdentityFacts {
            unique_keys,
            coordinates_match,
        },
        availability: WormObjectAvailabilityFacts {
            all_present,
            sizes_match,
        },
        digests: WormDigestFacts {
            require_digests: depth == VerifyDepth::Deep,
            digests_match,
        },
    }) {
        WormObjectSetDecision::Empty => Some("manifest names no segment objects".to_string()),
        WormObjectSetDecision::DuplicateKey => Some(format!(
            "object `{}` is named more than once by the manifest",
            first_duplicate.unwrap_or_default()
        )),
        WormObjectSetDecision::CoordinateMismatch => Some(format!(
            "object `{}` does not match the manifest segment coordinates or suffix",
            first_coordinate.unwrap_or_default()
        )),
        WormObjectSetDecision::MissingObject => Some(format!(
            "object `{}` named by the manifest is missing from the archive",
            first_missing.unwrap_or_default()
        )),
        WormObjectSetDecision::CountMismatch => Some(format!(
            "manifest names {object_count} objects, but the archive holds {listed_count} objects for this segment"
        )),
        WormObjectSetDecision::SizeMismatch => first_size,
        WormObjectSetDecision::DigestMismatch => first_digest,
        WormObjectSetDecision::Admit => None,
    };
    Ok(reason)
}

/// Streams one object and returns its `SHA-256`.
///
/// The body is hashed chunk by chunk and never buffered whole, so the memory
/// cost does not follow the object size.
///
/// `version` selects one stored version of the key. `None` reads whatever the
/// key resolves to now, which is what the digest walk wants: the current
/// version is the one a reader gets, so it is the one that has to match.
async fn object_digest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    version: Option<&str>,
) -> Result<Sha256Digest, WormError> {
    let path = Path::from(key);
    let options = GetOptions {
        version: version.map(ToString::to_string),
        ..GetOptions::default()
    };
    let fetch = store
        .get_opts(&path, options)
        .await
        .map_err(|e| WormError::Archive(format!("cannot read object `{key}`: {e}")))?;
    let mut stream = fetch.into_stream();
    let mut hasher = Sha256::new();
    loop {
        let next = stream
            .try_next()
            .await
            .map_err(|e| WormError::Archive(format!("cannot read object `{key}`: {e}")))?;
        let Some(chunk) = next else { break };
        hasher.update(&chunk);
    }
    Ok(Sha256Digest(hasher.finalize().into()))
}

/// Asks whether the version the manifest pinned still holds the bytes it
/// recorded, and says so in one clause appended to the mismatch.
///
/// An overwrite on a versioned bucket does not replace the locked original: it
/// stacks a new current version on top of it. [`object_digest`] reads the
/// current version, so the mismatch that brings us here is the detection this
/// feature exists for. What the mismatch alone does not say is whether the
/// archive is *recoverable*, and on an Object Lock bucket it usually is --
/// which is the difference between restoring a segment and declaring it lost.
///
/// Reading the pinned version is never the default. A walk that always read it
/// would confirm bytes no reader can reach by key any more, and would be blind
/// to the very overwrite this reports.
///
/// Supplementary, so it never fails the run: an unreadable pinned version is
/// reported in the clause rather than raised, because the mismatch is already
/// the finding.
async fn pinned_version_note(store: &Arc<dyn ObjectStore>, object: &ObjectEntry) -> String {
    let Some(version) = object.version_id.as_deref() else {
        return String::new();
    };
    match object_digest(store, &object.key, Some(version)).await {
        Ok(digest) if digest == object.sha256 => format!(
            ". The pinned version `{version}` still matches, so the original bytes survive \
             beneath the overwrite and the segment is recoverable"
        ),
        Ok(digest) => format!(
            ". The pinned version `{version}` hashes to {digest}, so it does not hold the \
             recorded bytes either"
        ),
        Err(e) => format!(". The pinned version `{version}` could not be read: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_object_store::ObjectStoreClient;

    use super::*;
    use crate::worm::verify::{
        VerifyRequest,
        test_support::{Archive, PREFIX, VersionedStore, first_break_reason, put_entry, put_raw},
    };

    async fn overwrite_log_body(archive: &Archive) {
        let segment = &archive.segments[0];
        let swapped: Vec<u8> = segment.log_body.iter().map(|b| b ^ 0xff).collect();
        check!(swapped.len() == segment.log_body.len());
        put_raw(&archive.ops, &segment.log_key, Bytes::from(swapped)).await;
    }

    /// Overwriting a body on a versioned bucket does not remove the bytes the
    /// manifest recorded: it stacks a new current version over a locked one.
    /// The walk must still fail -- a reader gets the current version -- and the
    /// verdict must say the original survives, because that is the difference
    /// between restoring the segment and writing it off.
    #[tokio::test]
    async fn a_deep_digest_mismatch_says_when_the_pinned_version_still_holds() {
        let archive = Archive::build_on(Arc::new(VersionedStore::new()), &[1]).await;
        let segment = &archive.segments[0];
        check!(
            segment.entries.iter().all(|e| e.version_id.is_some()),
            "the fixture store must record a version per object"
        );

        // Same length, different bytes: the case only a deep run catches.
        overwrite_log_body(&archive).await;

        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..VerifyRequest::default()
        };
        let report = archive.verify_with(&request).await;

        check!(!report.ok(), "an overwritten body is a break");
        let reason = first_break_reason(&report);
        check!(reason.contains("hashes to"), "reason was: {reason}");
        check!(
            reason.contains("still matches") && reason.contains("recoverable"),
            "the verdict must say the locked original survives. reason was: {reason}"
        );
    }

    /// The same overwrite on a bucket without versioning. There is no version
    /// to pin, so the mismatch stands alone rather than growing a clause that
    /// claims a recovery nobody can perform.
    #[tokio::test]
    async fn a_deep_digest_mismatch_adds_no_note_without_versioning() {
        let archive = Archive::build(&[1]).await;
        let segment = &archive.segments[0];
        check!(
            segment.entries.iter().all(|e| e.version_id.is_none()),
            "InMemory records no versions, which is the point of this row"
        );

        overwrite_log_body(&archive).await;

        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..VerifyRequest::default()
        };
        let report = archive.verify_with(&request).await;

        check!(!report.ok());
        let reason = first_break_reason(&report);
        check!(reason.contains("hashes to"), "reason was: {reason}");
        check!(
            !reason.contains("pinned version"),
            "nothing was pinned, so nothing may be claimed about one. reason was: {reason}"
        );
    }

    /// A version the manifest names but the bucket no longer holds. The note
    /// reports it and the run carries on: the digest mismatch is already the
    /// finding, and a failed lookup for the supplementary answer must not
    /// become an error of its own.
    #[tokio::test]
    async fn pinned_version_note_reports_a_version_that_cannot_be_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(VersionedStore::new());
        let ops = ObjectStoreClient::new(Arc::clone(&store));
        let key = format!("{PREFIX}/gone.log");
        let entry = put_entry(&ops, ".log", &key, b"body").await;
        let missing = ObjectEntry {
            version_id: Some("v-never-written".to_string()),
            ..entry
        };

        let note = pinned_version_note(&store, &missing).await;

        check!(note.contains("v-never-written"), "note was: {note}");
        check!(note.contains("could not be read"), "note was: {note}");
    }
}
