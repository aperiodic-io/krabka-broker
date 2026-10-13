//! The chain walk: the part of a run that decides whether a partition's
//! manifests form one unbroken, correctly signed, fully backed sequence.
//!
//! The walk groups the partition's manifests into chain runs, replays each run
//! from genesis, and stops at the first manifest that does not continue it. It
//! accumulates what the partition's report needs as it goes, so a walk that
//! stops early still describes everything it verified before the break.

use {
    super::{
        TrustedManifestKeys, VerifyRequest,
        listing::DirListing,
        manifest_read::KeyedManifest,
        objects::check_objects,
        report::{
            EpochSpan, ObjectProtectionReport, PartitionVerifyReport, SegmentSpan,
            VerifiedPartition, VerifyBreak, extend_span, offset_gaps,
        },
        signature::{SignatureState, signature_state},
    },
    crate::worm::{
        error::WormError,
        manifest::{ChainHead, EpochId, ManifestSeq, ObjectEntry, SegmentManifest, manifest_head},
    },
    krabka_verified::{ChainStep, chain_step},
    object_store::ObjectStore,
    std::{collections::BTreeMap, sync::Arc},
};

/// The last manifest the walk accepted.
struct LastManifest {
    key: String,
    seq: ManifestSeq,
}

/// Everything the chain walk accumulates for one partition.
#[derive(Default)]
pub(super) struct Walk {
    manifests: u64,
    objects_checked: u64,
    create_precondition_objects: ObjectProtectionReport,
    bucket_retention_objects: ObjectProtectionReport,
    unsigned: u64,
    untrusted: u64,
    epochs: Vec<EpochSpan>,
    segments: Vec<SegmentSpan>,
    head: Option<ChainHead>,
    last: Option<LastManifest>,
    first_break: Option<VerifyBreak>,
    authenticated_objects: BTreeMap<String, ObjectEntry>,
}

impl Walk {
    /// Records one manifest that passed every check.
    fn accept(&mut self, key: &str, manifest: &SegmentManifest, head: ChainHead) {
        let body = &manifest.body;
        self.manifests = self.manifests.saturating_add(1);
        self.objects_checked = self
            .objects_checked
            .saturating_add(u64::try_from(body.objects.len()).unwrap_or(u64::MAX));
        for object in &body.objects {
            self.authenticated_objects
                .insert(object.key.clone(), object.clone());
            if object.create_precondition {
                self.create_precondition_objects.record(&object.key);
            } else {
                self.bucket_retention_objects.record(&object.key);
            }
        }
        self.segments.push(SegmentSpan {
            start: body.segment.start_offset,
            end: body.segment.end_offset,
        });
        self.head = Some(head);
        self.last = Some(LastManifest {
            key: key.to_string(),
            seq: body.chain.seq,
        });
    }

    /// Stop at the first failure, retaining the already verified part of the run.
    fn stopped_at(
        mut self,
        key: &str,
        manifest: &SegmentManifest,
        reason: String,
        span: Option<EpochSpan>,
    ) -> Self {
        self.first_break.get_or_insert(VerifyBreak {
            manifest_key: key.to_owned(),
            seq: Some(manifest.body.chain.seq),
            reason,
        });
        self.epochs.extend(span);
        self
    }

    /// Turns the accumulated walk into the partition's report.
    pub(super) fn into_report(
        mut self,
        dir: &str,
        orphan_objects: Vec<String>,
        request: &VerifyRequest,
    ) -> VerifiedPartition {
        let first_break = self
            .first_break
            .take()
            .or_else(|| self.tip_break(dir, request));
        let ok = first_break.is_none()
            && self.unsigned == 0
            && self.untrusted == 0
            && orphan_objects.is_empty();
        let report = PartitionVerifyReport {
            partition_dir: dir.to_string(),
            manifests: self.manifests,
            objects_checked: self.objects_checked,
            create_precondition_objects: self.create_precondition_objects,
            bucket_retention_objects: self.bucket_retention_objects,
            epochs: self.epochs,
            unsigned_manifests: self.unsigned,
            untrusted_manifests: self.untrusted,
            orphan_objects,
            offset_gaps: offset_gaps(&mut self.segments),
            head: self.head,
            ok,
            first_break,
        };
        VerifiedPartition {
            report,
            objects: self.authenticated_objects,
        }
    }

    /// The break that [`VerifyRequest::expect_head`] raises when the archive
    /// stops short of the head the caller obtained elsewhere.
    fn tip_break(&self, dir: &str, request: &VerifyRequest) -> Option<VerifyBreak> {
        let expected = request.expect_head?;
        let tip = self.head.unwrap_or(ChainHead::GENESIS);
        if tip == expected {
            return None;
        }
        // With nothing verified there is no manifest to point at, so the
        // break names the directory that should have held one.
        let (manifest_key, seq) = self.last.as_ref().map_or_else(
            || (dir.to_string(), None),
            |last| (last.key.clone(), Some(last.seq)),
        );
        Some(VerifyBreak {
            manifest_key,
            seq,
            reason: format!(
                "archive tip {tip} does not match the expected head {expected}: the archive \
                 stops short of a head obtained outside it, which is what tail truncation \
                 looks like"
            ),
        })
    }
}

/// Walks every chain run in the partition, stopping at the first break.
pub(super) async fn walk_partition(
    store: &Arc<dyn ObjectStore>,
    decoded: &[KeyedManifest],
    listing: &DirListing,
    request: &VerifyRequest,
    trusted: &TrustedManifestKeys,
) -> Result<Walk, WormError> {
    let mut walk = Walk::default();
    for run in chain_runs(decoded) {
        let mut span: Option<EpochSpan> = None;
        // Every run starts at genesis. A run that could not read its previous
        // head is a new run, so it never continues an older head.
        let mut head = ChainHead::GENESIS;
        let mut expected_seq = 0u64;
        for KeyedManifest { key, manifest } in run {
            let body = &manifest.body;
            let next_seq = match chain_continuation(manifest, expected_seq, head) {
                Ok(next_seq) => next_seq,
                Err(reason) => {
                    return Ok(walk.stopped_at(key, manifest, reason, span));
                }
            };
            head = manifest_head(body);
            expected_seq = next_seq;

            match signature_state(manifest, trusted) {
                SignatureState::Unsigned => {
                    walk.unsigned = walk.unsigned.saturating_add(1);
                }
                SignatureState::Untrusted => {
                    walk.untrusted = walk.untrusted.saturating_add(1);
                }
                SignatureState::Valid => {}
                SignatureState::Invalid(reason) => {
                    return Ok(walk.stopped_at(key, manifest, reason, span));
                }
            }

            if let Some(reason) = check_objects(store, manifest, listing, request.depth).await? {
                return Ok(walk.stopped_at(key, manifest, reason, span));
            }

            walk.accept(key, manifest, head);
            extend_span(&mut span, body, head);
        }
        walk.epochs.extend(span);
    }
    Ok(walk)
}

/// Groups the manifests into chain runs, ordered the way an archive grows.
///
/// Runs come out ordered by their lowest segment start offset, and the
/// manifests inside a run come out ordered by sequence. The object-store key
/// breaks a sequence tie, so two runs against an unchanged archive agree.
fn chain_runs(decoded: &[KeyedManifest]) -> Vec<Vec<&KeyedManifest>> {
    let mut runs: BTreeMap<EpochId, Vec<&KeyedManifest>> = BTreeMap::new();
    for entry in decoded {
        runs.entry(entry.manifest.body.chain.epoch_id)
            .or_default()
            .push(entry);
    }
    let mut ordered: Vec<Vec<&KeyedManifest>> = runs.into_values().collect();
    for run in &mut ordered {
        run.sort_by(|a, b| {
            a.manifest
                .body
                .chain
                .seq
                .cmp(&b.manifest.body.chain.seq)
                .then_with(|| a.key.cmp(&b.key))
        });
    }
    ordered.sort_by_key(|run| {
        let start = run
            .iter()
            .map(|entry| entry.manifest.body.segment.start_offset)
            .min()
            .unwrap_or(i64::MAX);
        (
            start,
            run.first().map(|entry| entry.manifest.body.chain.epoch_id),
        )
    });
    ordered
}

/// Why the manifest does not continue the running chain, if it does not.
fn chain_continuation(
    manifest: &SegmentManifest,
    expected_seq: u64,
    head: ChainHead,
) -> Result<u64, String> {
    let chain = manifest.body.chain;
    match chain_step(expected_seq, chain.seq.0, chain.prev_head == head) {
        ChainStep::SequenceMismatch => Err(format!(
            "chain sequence gap: expected seq {expected_seq}, the manifest records seq {}",
            chain.seq
        )),
        ChainStep::HeadMismatch => Err(format!(
            "chain head mismatch: the manifest records prev_head {}, the running head is {head}",
            chain.prev_head
        )),
        ChainStep::Exhausted => Err("chain sequence exhausted".to_string()),
        ChainStep::Continue(next_seq) => Ok(next_seq),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::verify::{
            VerifyDepth,
            test_support::{Archive, Tamper},
            verify_archive,
        },
        assert2::check,
    };

    #[tokio::test]
    async fn verify_report_is_deterministic() {
        let archive = Archive::build(&[2, 2]).await;
        Tamper::StrayObject.apply(&archive).await;
        let request = VerifyRequest {
            depth: VerifyDepth::Deep,
            ..Default::default()
        };

        let first = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();
        let second = verify_archive(&archive.store, &request, &archive.trusted())
            .await
            .unwrap();

        check!(first == second);
        check!(first.has_epoch_restarts());
    }
}
