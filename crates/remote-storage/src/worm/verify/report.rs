//! The values a verification run hands back, and the steps that assemble them.
//!
//! A run describes what it found rather than what it did: one report per
//! partition directory, the chain runs and the offset holes inside it, and the
//! archive-wide roll-up an auditor grades. The assembly helpers here turn the
//! walk's accumulated state into those values.

use crate::worm::manifest::{ChainHead, EpochId, ManifestBody, ManifestSeq};

const PROTECTION_SAMPLE_LIMIT: usize = 10;

/// Count and bounded key sample for one object-protection mechanism.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectProtectionReport {
    /// Number of verified objects in this class.
    pub count: u64,
    /// Up to ten representative object keys.
    pub sample: Vec<String>,
}

impl ObjectProtectionReport {
    pub(super) fn record(&mut self, key: &str) {
        self.count = self.count.saturating_add(1);
        if self.sample.len() < PROTECTION_SAMPLE_LIMIT {
            self.sample.push(key.to_string());
        }
    }
}

/// One unbroken run of a partition's chain, as the archive holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochSpan {
    /// The run's identifier.
    pub epoch_id: EpochId,
    /// Sequence of the run's first manifest.
    pub first_seq: ManifestSeq,
    /// Sequence of the run's last verified manifest.
    pub last_seq: ManifestSeq,
    /// Manifests verified in this run.
    pub manifests: u64,
    /// Lowest segment start offset in the run.
    pub start_offset: i64,
    /// Highest segment end offset in the run.
    pub end_offset: i64,
    /// Chain head after the run's last verified manifest.
    pub head: ChainHead,
}

/// A hole between two consecutive archived segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetGap {
    /// Last offset the archive holds before the hole.
    pub after: i64,
    /// First offset the archive holds after the hole.
    pub before: i64,
}

/// The first detected break in one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyBreak {
    /// Object-store key of the manifest the walk stopped at.
    pub manifest_key: String,
    /// Chain position of that manifest, when it decoded far enough to have one.
    pub seq: Option<ManifestSeq>,
    /// What is wrong, in one sentence.
    pub reason: String,
}

/// Verification result for one partition directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionVerifyReport {
    /// The directory this report covers, as an object-store key prefix.
    pub partition_dir: String,
    /// Manifests verified before the walk stopped.
    pub manifests: u64,
    /// Object entries checked before the walk stopped.
    pub objects_checked: u64,
    /// Objects written with an atomic create precondition.
    pub create_precondition_objects: ObjectProtectionReport,
    /// Objects whose multipart write relied on bucket retention.
    pub bucket_retention_objects: ObjectProtectionReport,
    /// Chain runs found, ordered by their lowest segment start offset.
    pub epochs: Vec<EpochSpan>,
    /// Manifests that carry no signature at all.
    pub unsigned_manifests: u64,
    /// Manifests signed by a `key_id` the run does not trust.
    pub untrusted_manifests: u64,
    /// Objects in the directory that no manifest names, sorted.
    pub orphan_objects: Vec<String>,
    /// Holes between consecutive archived segments, sorted.
    pub offset_gaps: Vec<OffsetGap>,
    /// Chain head after the last verified manifest.
    pub head: Option<ChainHead>,
    /// `true` only for an unbroken, fully attested chain with no orphan object.
    pub ok: bool,
    /// The break, when there is one.
    pub first_break: Option<VerifyBreak>,
}

/// Verification result for a whole archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveVerifyReport {
    /// One report per partition directory, sorted by directory.
    pub partitions: Vec<PartitionVerifyReport>,
    /// Unreferenced objects outside canonical partition directories.
    pub global_orphan_objects: Vec<String>,
}

impl ArchiveVerifyReport {
    /// `true` when every partition is unbroken, fully attested, and complete.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.global_orphan_objects.is_empty()
            && self.partitions.iter().all(|partition| partition.ok)
    }

    /// Manifests verified across every partition.
    #[must_use]
    pub fn manifests(&self) -> u64 {
        self.partitions.iter().fold(0u64, |total, partition| {
            total.saturating_add(partition.manifests)
        })
    }

    /// The break in the first broken partition, in partition order.
    #[must_use]
    pub fn first_break(&self) -> Option<&VerifyBreak> {
        self.partitions
            .iter()
            .find_map(|partition| partition.first_break.as_ref())
    }

    /// Every manifest verified against a trusted key.
    #[must_use]
    pub fn fully_attested(&self) -> bool {
        self.partitions.iter().all(|partition| {
            partition.unsigned_manifests == 0 && partition.untrusted_manifests == 0
        })
    }

    /// Any partition holds more than one chain run.
    #[must_use]
    pub fn has_epoch_restarts(&self) -> bool {
        self.partitions
            .iter()
            .any(|partition| partition.epochs.len() > 1)
    }
}

/// A partition whose manifests could not all be decoded, so no walk ran.
pub(super) fn broken_before_walk(
    dir: &str,
    orphan_objects: Vec<String>,
    first_break: VerifyBreak,
) -> PartitionVerifyReport {
    PartitionVerifyReport {
        partition_dir: dir.to_string(),
        manifests: 0,
        objects_checked: 0,
        create_precondition_objects: ObjectProtectionReport::default(),
        bucket_retention_objects: ObjectProtectionReport::default(),
        epochs: Vec::new(),
        unsigned_manifests: 0,
        untrusted_manifests: 0,
        orphan_objects,
        offset_gaps: Vec::new(),
        head: None,
        ok: false,
        first_break: Some(first_break),
    }
}

/// Opens or widens the span that describes the run being walked.
pub(super) fn extend_span(span: &mut Option<EpochSpan>, body: &ManifestBody, head: ChainHead) {
    match span {
        None => {
            *span = Some(EpochSpan {
                epoch_id: body.chain.epoch_id,
                first_seq: body.chain.seq,
                last_seq: body.chain.seq,
                manifests: 1,
                start_offset: body.segment.start_offset,
                end_offset: body.segment.end_offset,
                head,
            });
        }
        Some(span) => {
            span.last_seq = body.chain.seq;
            span.manifests = span.manifests.saturating_add(1);
            span.start_offset = span.start_offset.min(body.segment.start_offset);
            span.end_offset = span.end_offset.max(body.segment.end_offset);
            span.head = head;
        }
    }
}

/// Holes between the offset ranges the verified segments cover.
pub(super) fn offset_gaps(segments: &mut [(i64, i64)]) -> Vec<OffsetGap> {
    segments.sort_unstable();
    let mut gaps = Vec::new();
    let mut covered: Option<i64> = None;
    for &(start, end) in &*segments {
        if let Some(after) = covered
            && start > after.saturating_add(1)
        {
            gaps.push(OffsetGap {
                after,
                before: start,
            });
        }
        covered = Some(covered.map_or(end, |after| after.max(end)));
    }
    gaps
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::{
            archiver::WormArchiver,
            chain::WormChainRecord,
            manifest::{ChainStamp, manifest_head},
            verify::test_support::{Archive, SEGMENT_SPAN, put_raw},
        },
        assert2::check,
        std::sync::Arc,
    };

    #[test]
    fn object_protection_samples_are_bounded() {
        let mut report = ObjectProtectionReport::default();
        for index in 0..25 {
            report.record(&format!("object-{index}"));
        }

        check!(report.count == 25);
        check!(report.sample.len() == PROTECTION_SAMPLE_LIMIT);
        check!(report.sample[0] == "object-0");
        check!(report.sample[9] == "object-9");
    }

    #[tokio::test]
    async fn a_hole_between_segments_is_reported_as_an_offset_gap() {
        // Two runs, and the fixture numbers the second run's segments after a
        // deleted middle segment would sit, so the archive covers 0..99 and
        // 200..399 with nothing between.
        let archive = Archive::build(&[3]).await;
        archive.delete(&archive.segments[1].manifest_key).await;
        for entry in &archive.segments[1].entries {
            archive.delete(&entry.key).await;
        }
        // Re-chain the survivors so only the offsets are wrong: seq 2 now
        // follows seq 0, so re-stamp it at seq 1 on the first manifest's head.
        let head = manifest_head(&archive.segments[0].manifest.body);
        let segment = &archive.segments[2];
        let stamped = segment.metadata.clone().with_custom_metadata(
            WormChainRecord::request(ChainStamp {
                epoch_id: segment.manifest.body.chain.epoch_id,
                seq: ManifestSeq(1),
                prev_head: head,
            })
            .to_custom_metadata(),
        );
        let sealed = WormArchiver::new(Some(Arc::clone(&archive.signer)))
            .seal(&stamped, segment.entries.clone())
            .unwrap();
        put_raw(&archive.ops, &segment.manifest_key, sealed.bytes).await;

        let report = archive.verify().await;

        check!(report.ok());
        check!(
            report.partitions[0].offset_gaps
                == vec![OffsetGap {
                    after: SEGMENT_SPAN - 1,
                    before: 2 * SEGMENT_SPAN,
                }]
        );
    }
}
