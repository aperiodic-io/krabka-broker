//! The lifecycle of one segment copy: `CopySegmentStarted`, the blocking RSM
//! upload, then `CopySegmentFinished`, together with the rollback that runs
//! when any step of it fails.

use std::{collections::BTreeMap, sync::Arc};

use krabka_log::SegmentExport;
#[cfg(test)]
use krabka_remote_storage::LogSegmentData;
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RemoteStorageManager, TopicIdPartition, WormChainRecord,
};
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::{
    RemoteTier,
    archive::{ArchiveMode, ChainPosition},
    leader_epoch::leader_epoch_index_bytes,
    rlmm::rlmm_mutate,
};
use crate::{metrics::RemoteTierPath, time_util::now_ms};

/// What one [`copy_one`] attempt left behind.
#[derive(Debug)]
pub(super) enum CopyOutcome {
    /// The segment reached `CopySegmentFinished`.
    Copied {
        /// Where the next copy in this tick joins the chain. Carried out of
        /// the copy so consecutive segments chain without re-listing the RLMM.
        next: ChainPosition,
    },
    /// The attempt did not reach `CopySegmentFinished`. The caller keeps its
    /// chain position and the next tick retries the segment.
    Failed,
}

/// Copy one sealed segment through the full `Started` → `Finished`
/// lifecycle. On any failure, this function deletes the partial remote data
/// and drops the metadata (`DeleteSegmentStarted` → `DeleteSegmentFinished`),
/// so the next tick retries the segment; see [`rollback`] for the part of
/// that a write-once archive cannot do.
///
/// `chain` decides whether the copy is stamped for a WORM manifest. When it
/// is, the stamp goes on before the copy, so the `CopySegmentStarted` record
/// already says where the manifest was meant to sit, and a durable metadata
/// manager shows that even if the broker dies mid-copy.
pub(super) async fn copy_one(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    ex: &SegmentExport,
    chain: ChainPosition,
) -> CopyOutcome {
    // KIP-405's `RemoteCopyRequestsPerSec` and its errors and bytes twins.
    // The attempt is counted here rather than at each of the copy's many
    // return points, so the error count is exactly the attempts that did not
    // finish and the two divide into a failure rate.
    tier.metrics
        .record_remote_request(RemoteTierPath::Copy, &tp.topic);
    let outcome = copy_one_inner(tier, tp, broker_id, leader_epoch, ex, chain).await;
    match outcome {
        // The bytes are counted on a *finished* copy, not on the backend PUT
        // returning: a copy whose metadata never reached `CopySegmentFinished`
        // moved bytes no consumer can read, and counting those would say the
        // tier is keeping up when it is not.
        CopyOutcome::Copied { .. } => {
            tier.metrics
                .record_remote_bytes(RemoteTierPath::Copy, &tp.topic, ex.size.bytes_u64());
        }
        CopyOutcome::Failed => {
            tier.metrics
                .record_remote_error(RemoteTierPath::Copy, &tp.topic);
        }
    }
    outcome
}

async fn copy_one_inner(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    ex: &SegmentExport,
    chain: ChainPosition,
) -> CopyOutcome {
    let (rsm, rlmm) = (tier.rsm, tier.rlmm);
    // Only a chained copy is trying to seal a manifest, so only a chained copy
    // can fail to. An unchained copy failing on a mutable tier is an ordinary
    // copy failure and says nothing about the archive's attestation.
    let seal_failed = || {
        if matches!(chain, ChainPosition::At(_)) {
            tier.metrics.worm_manifest_seal_failures_total.inc();
        }
    };
    if chain == ChainPosition::Exhausted {
        error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
               "remote-log-manager: refusing a WORM copy after chain sequence exhaustion");
        return CopyOutcome::Failed;
    }
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    // Unwrap the log-layer `Offset`s into the remote-storage metadata's `i64`
    // world at the seam; the epoch map keeps its `LeaderEpoch` keys, which
    // `RemoteLogSegmentMetadata` carries verbatim.
    let epochs = export_epoch_map(ex, krabka_ids::LeaderEpoch(leader_epoch.0.max(0)));
    let size = ex.size.bytes_i32();

    let metadata = match RemoteLogSegmentMetadata::new(
        id.clone(),
        ex.base_offset.0,
        ex.last_offset.0,
        ex.max_timestamp,
        broker_id,
        now_ms(),
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs.clone(),
        ),
    ) {
        Ok(m) => m,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: skipping segment with invalid metadata");
            return CopyOutcome::Failed;
        }
    };
    // KIP-405 txnIndexEmpty: set true when the log segment has no transaction
    // index file (non-transactional topics or segments written before txn support).
    let metadata = if ex.transaction_index_path.is_none() {
        metadata.with_txn_index_empty(true)
    } else {
        metadata
    };
    // The chain stamp goes on before the copy: a WORM backend refuses to seal
    // an unstamped manifest, and it refuses only *after* uploading every
    // object, which would leave orphans in a bucket that takes nothing back.
    let metadata = match chain {
        ChainPosition::Unchained => metadata,
        ChainPosition::At(stamp) => {
            metadata.with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata())
        }
        ChainPosition::Exhausted => return CopyOutcome::Failed,
    };

    let md_started = metadata.clone();
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.add_remote_log_segment_metadata(md_started)).await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentStarted");
        return CopyOutcome::Failed;
    }

    let data = export_segment_data(ex, ProducerSnapshotExport::Include, || {
        leader_epoch_index_bytes(&epochs)
    });

    // The RSM is a blocking SPI — run the copy on the blocking pool.
    //
    // Bounded, because the pass is serial: an object store that accepts the
    // upload and then answers nothing would otherwise hold this partition,
    // and every partition queued behind it, for as long as the store cares
    // to stall. `spawn_blocking` cannot be cancelled, so past the deadline
    // the upload is abandoned rather than stopped — it keeps running on the
    // blocking pool against a segment id no metadata will ever finish, and
    // its objects are what the next tick's fresh id copies again.
    let rsm_copy = rsm.clone();
    let md_copy = metadata.clone();
    let copy_result = tokio::time::timeout(
        tier.copy_timeout.to_std(),
        crate::blocking::spawn_blocking(move || rsm_copy.copy_log_segment_data(&md_copy, &data)),
    )
    .await;

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    let returned = match copy_result {
        Ok(Ok(Ok(returned))) => returned,
        // The deadline expired. Nothing is rolled back: the abandoned upload
        // is still writing under this metadata's segment id, and a delete
        // racing it could remove an object the upload then re-creates,
        // leaving the tier holding a partial segment that no metadata
        // records. The segment stays in `CopySegmentStarted` instead, which
        // is what makes local retention refuse to drop its local copy and
        // the next tick re-copy it under a fresh id.
        Err(_) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  timeout_ms = tier.copy_timeout.millis_i64(),
                  "remote-log-manager: segment copy exceeded its deadline; abandoning it and \
                   moving on to the next partition");
            seal_failed();
            return CopyOutcome::Failed;
        }
        Ok(Ok(Err(e))) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy failed");
            seal_failed();
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
        Ok(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: segment copy task panicked");
            seal_failed();
            rollback(&metadata, broker_id, chain.archive(), rsm, rlmm).await;
            return CopyOutcome::Failed;
        }
    };

    // A write-once copy is only complete once the backend hands back a receipt
    // carrying the head its manifest produced. Without one the objects landed
    // with no verifiable manifest over them, so the segment must not be marked
    // finished: the read path serves every finished segment, and it would then
    // be serving unattested data. Leaving it in `CopySegmentStarted` is what
    // makes the next tick retry it under a fresh segment id.
    let next = match chain {
        ChainPosition::Unchained => ChainPosition::Unchained,
        ChainPosition::At(requested) => {
            let Some(receipt) = returned
                .as_ref()
                .and_then(|custom| WormChainRecord::from_custom_metadata(custom).ok())
            else {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned no chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                seal_failed();
                return CopyOutcome::Failed;
            };
            if receipt.head.is_none()
                || receipt.epoch_id != requested.epoch_id
                || receipt.seq != requested.seq
                || receipt.prev_head != requested.prev_head
            {
                error!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                       "remote-log-manager: write-once copy returned a mismatched chain receipt; \
                        leaving the segment in CopySegmentStarted rather than serving \
                        unattested data");
                seal_failed();
                return CopyOutcome::Failed;
            }
            // The receipt is the archive's word that a manifest over these
            // objects is sealed and chained where the copy asked for it, so
            // this is the one point at which a seal is known to have happened.
            tier.metrics.worm_manifests_sealed_total.inc();
            match receipt.next_stamp() {
                Some(stamp) => ChainPosition::At(stamp),
                None if receipt.seq.0 == u64::MAX => ChainPosition::Exhausted,
                None => return CopyOutcome::Failed,
            }
        }
        ChainPosition::Exhausted => return CopyOutcome::Failed,
    };

    // The backend's receipt is the chain position a restart reads back, so
    // it has to be durable alongside the segment, not dropped here.
    if let Err(e) = super::rlmm::update_segment(
        rlmm,
        id,
        returned,
        RemoteLogSegmentState::CopySegmentFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
              error = %e, "remote-log-manager: failed to record CopySegmentFinished");
        return CopyOutcome::Failed;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
           end = ex.last_offset.0, "remote-log-manager: copied segment to remote tier");
    CopyOutcome::Copied { next }
}

/// Delete partial remote data and drop the metadata after a failed copy.
///
/// # A write-once archive keeps its partial objects
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped and only the
/// metadata is dropped. Whatever objects the failed copy managed to write stay
/// in the archive for good, because the backend refuses every delete and the
/// bucket policy refuses it under that. They are inert — the copy never sealed
/// a manifest, so no chain references them — and the retry runs under a fresh
/// segment UUID, so its keys cannot collide with theirs. This residue is
/// exactly what the WORM verifier reports as `orphan_objects`: unreferenced
/// bytes are the standing cost of a tier that can take nothing back.
async fn rollback(
    metadata: &RemoteLogSegmentMetadata,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) {
    let id = metadata.remote_log_segment_id().clone();
    match archive {
        ArchiveMode::Mutable => {
            let rsm_del = rsm.clone();
            let md_del = metadata.clone();
            let _ =
                crate::blocking::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del))
                    .await;
        }
        ArchiveMode::WriteOnce => {
            debug!(topic = %id.topic_id_partition.topic,
                   partition = id.topic_id_partition.partition,
                   base = metadata.start_offset(), worm_retained = true,
                   "remote-log-manager: leaving a failed copy's objects in the write-once \
                    archive; the verifier reports them as orphans");
        }
    }
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        let _ = super::rlmm::update_segment(rlmm, id.clone(), None, state, broker_id).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_log::Offset;
    use krabka_remote_storage::{
        ChainHead, ChainStamp, CustomMetadata, EpochId, ManifestSeq, RemoteStorageError,
    };
    use krabka_units::bytes;

    use super::*;
    use crate::remote_log_manager::{
        copy_eligible, test_support as fixtures,
        test_support::{
            FakeWormArchive, copy_exports, local_backends, missing_remote_reads, rolled_log,
            synth_export, tier, tp,
        },
    };

    /// An RSM whose copy always fails, but whose delete succeeds. The tests
    /// use it to exercise the failure rollback path.
    struct AlwaysFailRsm;

    impl RemoteStorageManager for AlwaysFailRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::InvalidArgument("boom".into()))
        }
        missing_remote_reads!(delete_ok);
    }

    /// Returns caller-selected metadata without touching storage.
    struct ReturningRsm(Option<CustomMetadata>);

    impl RemoteStorageManager for ReturningRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(self.0.clone())
        }
        missing_remote_reads!(delete_ok);
    }

    /// Records the metadata every copy hands the backend, then fails the copy.
    /// Tests use it to see what the broker stamped on a segment before the
    /// upload, which is the only moment that stamp is observable.
    #[derive(Default)]
    struct CapturingRsm {
        seen: Mutex<Vec<RemoteLogSegmentMetadata>>,
    }

    impl RemoteStorageManager for CapturingRsm {
        fn copy_log_segment_data(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            self.seen
                .lock()
                .expect("captured-metadata mutex poisoned")
                .push(metadata.clone());
            Err(RemoteStorageError::InvalidArgument("captured".into()))
        }
        missing_remote_reads!(delete_ok);
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm = fixtures::in_memory_metadata();

        let copied = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports.clone()).await;
        assert!(copied == 0, "every copy failed");
        // Rollback (delete + DeleteSegmentStarted -> DeleteSegmentFinished)
        // drops the started metadata, so nothing is left behind and a later
        // run with a healthy store can retry the same segments.
        assert!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "failed copies must not leave dangling metadata"
        );
    }

    #[tokio::test]
    async fn fallback_leader_epoch_when_export_has_none() {
        let remote_dir = tempfile::tempdir().unwrap();
        let (rsm, rlmm) = local_backends(remote_dir.path());

        // Hand-build an export with no leader epochs but real files on disk.
        let src = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let p = src.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let export = SegmentExport {
            base_offset: Offset(0),
            last_offset: Offset(9),
            max_timestamp: 42,
            last_modified_ms: 42,
            size: bytes(10),
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            producer_snapshot_path: write("10.snapshot", b"snapshot"),
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            7,
            LeaderEpoch(3),
            vec![export],
        )
        .await;
        assert!(copied == 1);
        let md = &rlmm.list_remote_log_segments(&tp()).unwrap()[0];
        // The fallback recorded the partition's current leader epoch (3).
        assert!(md.segment_leader_epochs().get(&LeaderEpoch(3)) == Some(&0));
    }

    #[tokio::test]
    async fn copy_one_stamps_a_chain_request_on_the_started_metadata() {
        let stamp = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x5eed)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7; 32]),
        };
        let cases = [
            (
                "mutable tier stamps nothing",
                ChainPosition::Unchained,
                None,
            ),
            (
                "write-once stamps the request form",
                ChainPosition::At(stamp),
                Some(WormChainRecord::request(stamp).to_custom_metadata()),
            ),
        ];
        for (name, chain, expected) in cases {
            let rsm_impl = Arc::new(CapturingRsm::default());
            let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
            let rlmm = fixtures::in_memory_metadata();
            let export = synth_export(fixtures::SegmentExportSetup::default());

            let outcome = copy_one(
                &tier(chain.archive(), &rsm, &rlmm),
                &tp(),
                1,
                LeaderEpoch(0),
                &export,
                chain,
            )
            .await;

            check!(matches!(outcome, CopyOutcome::Failed), "case {name}");
            let seen = rsm_impl
                .seen
                .lock()
                .expect("captured-metadata mutex poisoned");
            check!(seen.len() == 1, "case {name}");
            // The stamp is on the metadata the backend sees, which is the
            // same value the RLMM recorded as `CopySegmentStarted`.
            check!(
                seen[0].custom_metadata() == expected.as_ref(),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn copy_one_rejects_unattested_or_mismatched_chain_receipts() {
        let requested = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x5eed)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7; 32]),
        };
        let cases = [
            WormChainRecord::request(requested),
            WormChainRecord::request(ChainStamp {
                seq: ManifestSeq(5),
                ..requested
            })
            .with_head(ChainHead([8; 32])),
        ];
        for receipt in cases {
            let rsm: Arc<dyn RemoteStorageManager> =
                Arc::new(ReturningRsm(Some(receipt.to_custom_metadata())));
            let rlmm = fixtures::in_memory_metadata();

            let outcome = copy_one(
                &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
                &tp(),
                1,
                LeaderEpoch(0),
                &synth_export(fixtures::SegmentExportSetup::default()),
                ChainPosition::At(requested),
            )
            .await;

            check!(matches!(outcome, CopyOutcome::Failed));
            check!(
                rlmm.list_remote_log_segments(&tp())
                    .unwrap()
                    .iter()
                    .all(|md| md.state() != RemoteLogSegmentState::CopySegmentFinished)
            );
        }
    }
    /// One seal case: what it is called, the backend it runs against, where
    /// the copy sits in the chain, and the counter values it must leave.
    type SealCase = (
        &'static str,
        Arc<dyn RemoteStorageManager>,
        ChainPosition,
        u64,
        u64,
    );

    /// The two KFC-5 seal counters, over every outcome one copy can reach.
    ///
    /// A seal is only attempted on a chained copy, so an unchained copy that
    /// fails must move neither counter: a mutable tier's copy failure says
    /// nothing about a WORM archive's attestation, and counting it would make
    /// the failure alert fire on clusters that hold no archive at all.
    #[tokio::test]
    async fn the_seal_counters_follow_the_chain_receipt() {
        let stamp = ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x00c0_ffee)),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let cases: [SealCase; 4] = [
            (
                "a sealed manifest with a matching receipt",
                Arc::new(FakeWormArchive::new()),
                ChainPosition::At(stamp),
                1,
                0,
            ),
            (
                "a copy the backend refused",
                Arc::new(AlwaysFailRsm),
                ChainPosition::At(stamp),
                0,
                1,
            ),
            (
                "a copy that came back with no receipt",
                Arc::new(ReturningRsm(None)),
                ChainPosition::At(stamp),
                0,
                1,
            ),
            (
                "an unchained copy that failed",
                Arc::new(AlwaysFailRsm),
                ChainPosition::Unchained,
                0,
                0,
            ),
        ];
        for (name, rsm, chain, sealed, failed) in cases {
            fixtures::owned_tier_resources!(rlmm, metrics, index_cache);
            let tier = fixtures::configured_tier(
                &rsm,
                &rlmm,
                fixtures::TierSetup {
                    metrics: &metrics,
                    index_cache: &index_cache,
                    archive: chain.archive(),
                    ..Default::default()
                },
            );

            copy_one(
                &tier,
                &tp(),
                1,
                LeaderEpoch(0),
                &synth_export(fixtures::SegmentExportSetup::default()),
                chain,
            )
            .await;

            check!(
                metrics.worm_manifests_sealed_total.get() == sealed,
                "case {name}"
            );
            check!(
                metrics.worm_manifest_seal_failures_total.get() == failed,
                "case {name}"
            );
        }
    }
}

/// The export's epoch map, using the caller's fallback only when no checkpoint exists.
pub(crate) fn export_epoch_map(
    export: &SegmentExport,
    fallback: krabka_ids::LeaderEpoch,
) -> BTreeMap<krabka_ids::LeaderEpoch, i64> {
    if export.leader_epochs.is_empty() {
        maplit::btreemap! { fallback => export.base_offset.0 }
    } else {
        export
            .leader_epochs
            .iter()
            .map(|&(epoch, offset)| (epoch, offset.0))
            .collect()
    }
}

// Preserve export paths and optional snapshot policy before constructing the epoch bytes.
krabka_macros::export_segment_data_fixture!(export_segment_data);
