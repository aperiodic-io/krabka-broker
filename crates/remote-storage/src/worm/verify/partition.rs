//! One partition directory, from its objects to its report.
//!
//! This is the step between the archive-wide listing and the chain walk. It
//! decodes the directory's manifests, applies the request's topic and partition
//! filters, accounts for the objects no manifest names, and hands the rest to
//! the walk. A directory whose manifests do not all decode never reaches the
//! walk: the refusal is the finding.

use {
    super::{
        TrustedManifestKeys, VerifyRequest,
        listing::{DirListing, orphans},
        manifest_read::{KeyedManifest, ManifestRead, read_manifest},
        report::{PartitionVerifyReport, VerifyBreak, broken_before_walk},
        walk::walk_partition,
    },
    crate::worm::{
        error::WormError,
        manifest::{MANIFEST_SUFFIX, ManifestBody, ObjectEntry},
    },
    object_store::ObjectStore,
    std::{collections::BTreeMap, sync::Arc},
};

/// Verifies one partition directory, or `None` when the request filters it out.
pub(super) async fn verify_partition(
    store: &Arc<dyn ObjectStore>,
    dir: &str,
    listing: &DirListing,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<Option<(PartitionVerifyReport, BTreeMap<String, ObjectEntry>)>, WormError> {
    let mut decoded: Vec<KeyedManifest> = Vec::new();
    let mut rejected: Option<VerifyBreak> = None;
    for key in listing.keys().filter(|key| key.ends_with(MANIFEST_SUFFIX)) {
        match read_manifest(store, key).await? {
            ManifestRead::Decoded(manifest) => decoded.push((key.clone(), *manifest)),
            ManifestRead::Rejected(reason) => {
                rejected.get_or_insert_with(|| VerifyBreak {
                    manifest_key: key.clone(),
                    seq: None,
                    reason,
                });
            }
        }
    }

    if !selected(request, decoded.first().map(|(_, m)| &m.body)) {
        return Ok(None);
    }

    let orphan_objects = orphans(listing, &decoded);
    if let Some(first_break) = rejected {
        return Ok(Some((
            broken_before_walk(dir, orphan_objects, first_break),
            BTreeMap::new(),
        )));
    }

    let walk = walk_partition(store, &decoded, listing, request, trusted).await?;
    Ok(Some(walk.into_report(dir, orphan_objects, request)))
}

/// Whether the request's topic and partition filters admit this directory.
///
/// The filter reads the manifest and not the directory name. A directory name
/// embeds a URL-safe Base64 topic id, whose alphabet contains the same `-` that
/// separates the name's fields, so the name cannot be split back apart.
fn selected(request: &VerifyRequest, body: Option<&ManifestBody>) -> bool {
    if request.topic.is_none() && request.partition.is_none() {
        return true;
    }
    let Some(body) = body else { return false };
    request
        .topic
        .as_ref()
        .is_none_or(|topic| *topic == body.segment.topic)
        && request
            .partition
            .is_none_or(|partition| partition == body.segment.partition)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::verify::{
            test_support::{Archive, PARTITION, TOPIC},
            verify_archive,
        },
        assert2::check,
    };

    #[tokio::test]
    async fn a_topic_filter_skips_the_partitions_it_does_not_name() {
        let archive = Archive::build(&[2]).await;
        let trusted = archive.trusted();

        let other_topic = VerifyRequest {
            topic: Some("payments".to_string()),
            ..Default::default()
        };
        check!(
            verify_archive(&archive.store, &other_topic, &trusted)
                .await
                .unwrap()
                .partitions
                .is_empty()
        );

        let this_topic = VerifyRequest {
            topic: Some(TOPIC.to_string()),
            partition: Some(PARTITION),
            ..Default::default()
        };
        check!(
            verify_archive(&archive.store, &this_topic, &trusted)
                .await
                .unwrap()
                .manifests()
                == 2
        );
    }
}
