//! Validation and materialization of captured committed diskless-WAL state.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_log::{Log, LogConfig, name};
use krabka_protocol::records::{RecordBatchBorrowed, validate_one_v2_batch};
use krabka_remote_storage::{
    ObjectEntry, TopicIdPartition, TrustedManifestKeys,
    diskless::{CAPTURE_HEAD_NAME, CapturedWalRange, DisklessWalCapture, parse_wal_object},
};
use object_store::path::Path as ObjectPath;

use crate::{
    ArchiveInventory, ArchiveStore, PartitionInventory, RestoreArgs, RestoreError,
    args::PartitionRef,
    bound::Predicates,
    materialize::prepare::{BatchTally, PreparedBatch, prepare_batch},
    report::{DisklessPartitionReport, DisklessRestoreReport},
    verify::{MAX_LOG_BYTES, fetch_capped},
};

pub(super) async fn load(path: Option<&Path>) -> Result<Option<DisklessWalCapture>, RestoreError> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = tokio::fs::read(path).await?;
    DisklessWalCapture::from_slice(&bytes)
        .map(Some)
        .map_err(RestoreError::Integrity)
}

pub(super) fn add_partitions(
    inventory: &mut ArchiveInventory,
    capture: &DisklessWalCapture,
    args: &RestoreArgs,
) -> Result<(), RestoreError> {
    let mut seen = HashSet::new();
    let mut topic_ids = HashMap::new();
    let mut named_partitions = HashSet::new();
    for existing in &inventory.partitions {
        if topic_ids
            .insert(
                existing.partition.topic.clone(),
                existing.partition.topic_id,
            )
            .is_some_and(|topic_id| topic_id != existing.partition.topic_id)
        {
            return Err(RestoreError::Integrity(format!(
                "topic {} has multiple topic ids in the restore inputs",
                existing.partition.topic
            )));
        }
        named_partitions.insert((
            existing.partition.topic.clone(),
            existing.partition.partition,
        ));
    }
    for partition in &capture.partitions {
        if !args.selects_topic(&partition.topic) {
            continue;
        }
        if !seen.insert((partition.topic_id, partition.partition)) {
            return Err(RestoreError::Integrity(format!(
                "duplicate diskless capture partition {}-{}",
                partition.topic, partition.partition
            )));
        }
        if topic_ids
            .insert(partition.topic.clone(), partition.topic_id)
            .is_some_and(|topic_id| topic_id != partition.topic_id)
        {
            return Err(RestoreError::Integrity(format!(
                "topic {} has multiple topic ids in the restore inputs",
                partition.topic
            )));
        }
        if !named_partitions.insert((partition.topic.clone(), partition.partition)) {
            return Err(RestoreError::Integrity(format!(
                "{}-{} is both classic and diskless in the restore inputs",
                partition.topic, partition.partition
            )));
        }
        if inventory.partitions.iter().any(|existing| {
            existing.partition.topic_id == partition.topic_id
                && existing.partition.partition == partition.partition
        }) {
            return Err(RestoreError::Integrity(format!(
                "{}-{} is both classic and diskless in the restore inputs",
                partition.topic, partition.partition
            )));
        }
        if args.to_offset.iter().any(|bound| {
            bound.partition.topic == partition.topic
                && bound.partition.partition == partition.partition
                && bound.last_offset.0 < partition.delete_floor
        }) {
            return Err(RestoreError::InvalidArgument(format!(
                "--to-offset for {}-{} precedes diskless delete floor {}",
                partition.topic, partition.partition, partition.delete_floor
            )));
        }
        inventory.partitions.push(PartitionInventory {
            partition: TopicIdPartition::new(
                partition.topic_id,
                partition.topic.clone(),
                partition.partition,
            ),
            segments: Vec::new(),
        });
    }
    PartitionInventory::sort_by_topic_and_partition(&mut inventory.partitions);
    for requested in args
        .to_offset
        .iter()
        .map(|bound| &bound.partition)
        .chain(args.exclude_offset.iter().map(|range| &range.partition))
    {
        if !inventory.holds(&requested.topic, requested.partition) {
            return Err(RestoreError::UnknownPartition {
                topic: requested.topic.clone(),
                partition: requested.partition,
            });
        }
    }
    Ok(())
}

pub(super) fn authenticate(
    capture: &DisklessWalCapture,
    trusted: &TrustedManifestKeys,
    args: &RestoreArgs,
) -> Result<(std::collections::BTreeMap<String, ObjectEntry>, String), RestoreError> {
    let expected = args.worm_expect_head.iter().find_map(|value| {
        value
            .split_once('=')
            .filter(|(name, _)| *name == CAPTURE_HEAD_NAME)
            .map(|(_, head)| head)
    });
    let claims = capture
        .authenticate(trusted, expected)
        .map_err(|reason| RestoreError::Authenticity { reason })?;
    let head = capture
        .authentication
        .as_ref()
        .map(|manifest| krabka_remote_storage::manifest_head(&manifest.body).to_string())
        .ok_or_else(|| RestoreError::Authenticity {
            reason: "diskless capture has no signed WORM boundary".to_owned(),
        })?;
    Ok((claims, head))
}

pub(super) async fn materialize(
    store: &ArchiveStore,
    args: &RestoreArgs,
    predicates: &Predicates,
    capture: &DisklessWalCapture,
    authenticated: Option<&std::collections::BTreeMap<String, ObjectEntry>>,
) -> Result<DisklessRestoreReport, RestoreError> {
    capture.validate().map_err(RestoreError::Integrity)?;
    let mut cached_object: Option<(String, Bytes)> = None;
    let mut reports = Vec::new();
    for partition in capture
        .partitions
        .iter()
        .filter(|partition| args.selects_topic(&partition.topic))
    {
        let mut log = None;
        if !args.dry_run {
            let dir =
                name::partition_dir(&args.target.log_dir, &partition.topic, partition.partition);
            std::fs::create_dir_all(&dir)?;
            log = Some(Log::open(&dir, LogConfig::default())?);
        }
        let mut object_names = HashSet::new();
        let mut batches = 0u64;
        let mut records = 0u64;
        let mut records_dropped = 0u64;
        let mut observed_end = partition.delete_floor;
        let mut materialized_end = partition.delete_floor;
        let partition_ref = PartitionRef {
            topic: partition.topic.clone(),
            partition: partition.partition,
        };
        for range in &partition.ranges {
            let object =
                fetch_object(store, &mut cached_object, &range.object_key, authenticated).await?;
            object_names.insert(range.object_key.as_str());
            validate_footer_range(&object, range)?;
            let start = usize::try_from(range.entry.byte_start)
                .map_err(|_| RestoreError::Integrity("diskless WAL byte start overflow".into()))?;
            let len = usize::try_from(range.entry.byte_len)
                .map_err(|_| RestoreError::Integrity("diskless WAL byte length overflow".into()))?;
            let end = start
                .checked_add(len)
                .filter(|end| *end <= object.len())
                .ok_or_else(|| {
                    RestoreError::Integrity(format!(
                        "{}: captured byte range is outside object",
                        range.object_key
                    ))
                })?;
            let mut cursor = start;
            let mut first = None;
            let mut last: Option<i64> = None;
            while cursor < end {
                let validated = validate_one_v2_batch(&object[cursor..end])?;
                let header = validated.header;
                let batch_end = cursor.checked_add(validated.total_len).ok_or_else(|| {
                    RestoreError::Integrity("diskless WAL byte range overflow".into())
                })?;
                if validated.total_len == 0 || batch_end > end {
                    return Err(RestoreError::Integrity(format!(
                        "{}: captured range disagrees with batch framing",
                        range.object_key
                    )));
                }
                let batch_last = header
                    .base_offset
                    .get()
                    .checked_add(i64::from(header.last_offset_delta.get()))
                    .ok_or_else(|| {
                        RestoreError::Integrity("diskless WAL batch offset overflow".into())
                    })?;
                if last.is_some_and(|previous| {
                    previous.checked_add(1) != Some(header.base_offset.get())
                }) {
                    return Err(RestoreError::Integrity(format!(
                        "{}: non-contiguous batch offsets inside captured run",
                        range.object_key
                    )));
                }
                first.get_or_insert(header.base_offset.get());
                last = Some(batch_last);
                let base = Offset(header.base_offset.get());
                if batch_last >= partition.delete_floor
                    && !predicates.batch_past_offset_bound(&partition_ref, base)
                {
                    let mut batch_cursor = &object[cursor..batch_end];
                    let batch = RecordBatchBorrowed::decode_borrow_with_policy(
                        &mut batch_cursor,
                        <_>::default(),
                    )?;
                    if !batch_cursor.is_empty() {
                        return Err(RestoreError::Integrity(format!(
                            "{}: decoded diskless batch left trailing bytes",
                            range.object_key
                        )));
                    }
                    let records_in_batch =
                        u64::try_from(header.records_count.get().max(0)).unwrap_or(0);
                    let (mut prepared, tally) = prepare_batch(
                        &partition_ref,
                        predicates,
                        &batch,
                        object.slice(cursor..batch_end),
                        records_in_batch,
                    )?;
                    materialized_end = base
                        .0
                        .checked_add(i64::from(prepared.last_offset_delta()))
                        .and_then(|last| last.checked_add(1))
                        .ok_or_else(|| {
                            RestoreError::Integrity("diskless WAL batch offset overflow".into())
                        })?;
                    if let Some(log) = log.as_mut() {
                        if log.log_end_offset() == Offset(0) && base != Offset(0) {
                            log.reset_to(base)?;
                        }
                        log.reconcile_next_offset(base);
                        match &mut prepared {
                            PreparedBatch::Verbatim(batch) => {
                                log.append_verbatim_at(batch, base)?;
                            }
                            PreparedBatch::Owned { batch, .. } => {
                                log.append_at(batch, base)?;
                            }
                        }
                    }
                    batches += 1;
                    match tally {
                        BatchTally::Kept => records = records.saturating_add(records_in_batch),
                        BatchTally::Rewritten { kept, dropped } => {
                            records = records.saturating_add(kept);
                            records_dropped = records_dropped.saturating_add(dropped);
                        }
                        BatchTally::Emptied => {
                            records_dropped = records_dropped.saturating_add(records_in_batch);
                        }
                    }
                }
                cursor = batch_end;
            }
            if first != Some(range.entry.first_offset) || last != Some(range.entry.last_offset) {
                return Err(RestoreError::Integrity(format!(
                    "{}: captured run disagrees with batch boundaries",
                    range.object_key
                )));
            }
            observed_end = range
                .entry
                .last_offset
                .checked_add(1)
                .ok_or_else(|| {
                    RestoreError::Integrity("diskless WAL batch offset overflow".into())
                })?
                .max(partition.delete_floor);
        }
        if observed_end != partition.recovery_cutoff {
            return Err(RestoreError::Integrity(format!(
                "{}-{} validated through {observed_end}, capture declares cutoff {}",
                partition.topic, partition.partition, partition.recovery_cutoff
            )));
        }
        if let Some(log) = log.as_mut() {
            if log.log_end_offset() == Offset(0) && materialized_end != 0 {
                log.reset_to(Offset(materialized_end))?;
            }
            if log.log_end_offset().0 != materialized_end {
                return Err(RestoreError::Integrity(format!(
                    "{}-{} materialized through {}, expected {} after restore bounds",
                    partition.topic,
                    partition.partition,
                    log.log_end_offset(),
                    materialized_end
                )));
            }
            log.set_log_start_offset(Offset(partition.delete_floor))?;
            log.sync()?;
        }
        reports.push(DisklessPartitionReport {
            topic: partition.topic.clone(),
            partition: partition.partition,
            topic_id: partition.topic_id,
            delete_floor: partition.delete_floor,
            recovery_cutoff: materialized_end,
            objects: object_names.len() as u64,
            batches,
            records,
            records_dropped,
        });
    }
    Ok(DisklessRestoreReport {
        captured_at_ms: capture.captured_at_ms,
        source_cutoffs: capture.source_cutoffs.clone(),
        partitions: reports,
        limitations: limitations(),
    })
}

fn validate_footer_range(object: &Bytes, range: &CapturedWalRange) -> Result<(), RestoreError> {
    let manifests = parse_wal_object(object)
        .map_err(|reason| RestoreError::Integrity(format!("{}: {reason}", range.object_key)))?;
    if manifests.iter().any(|run| {
        run.topic_id == range.entry.topic_id
            && run.partition == range.entry.partition
            && run.first_offset <= range.entry.first_offset
            && run.last_offset >= range.entry.last_offset
            && run.byte_start <= range.entry.byte_start
            && run
                .byte_start
                .checked_add(u64::from(run.byte_len))
                .zip(
                    range
                        .entry
                        .byte_start
                        .checked_add(u64::from(range.entry.byte_len)),
                )
                .is_some_and(|(run_end, range_end)| range_end <= run_end)
    }) {
        Ok(())
    } else {
        Err(RestoreError::Integrity(format!(
            "{}: capture range is not contained in a CKWL footer run",
            range.object_key
        )))
    }
}

fn limitations() -> Vec<String> {
    vec![
        "diskless records at or after each recovery cutoff were not archived and are unavailable"
            .into(),
        "in-flight transaction state is not restored".into(),
    ]
}

async fn fetch_object(
    store: &ArchiveStore,
    cached: &mut Option<(String, Bytes)>,
    key: &str,
    authenticated: Option<&std::collections::BTreeMap<String, ObjectEntry>>,
) -> Result<Bytes, RestoreError> {
    if let Some((_, bytes)) = cached.as_ref().filter(|(cached_key, _)| cached_key == key) {
        return Ok(bytes.clone());
    }
    *cached = None;
    let bytes = fetch_capped(
        store.ops(),
        &ObjectPath::from(key),
        MAX_LOG_BYTES,
        authenticated,
    )
    .await?;
    *cached = Some((key.to_owned(), bytes.clone()));
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::load;
    use crate::RestoreError;

    #[tokio::test]
    async fn a_capture_with_a_missing_or_unknown_version_is_refused() {
        for (name, json, expected) in [
            (
                "pre-1.0 capture",
                r#"{"captured_at_ms":1,"source_cutoffs":[],"partitions":[]}"#,
                "diskless WAL capture has no format_version: it predates krabka 1.0, which this \
                 build does not read",
            ),
            (
                "future version",
                r#"{"format_version":2,"captured_at_ms":1,"source_cutoffs":[],"partitions":[]}"#,
                "unsupported diskless WAL capture format version 2: this build reads version 1",
            ),
        ] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = dir.path().join("diskless-wal-index.json");
            tokio::fs::write(&path, json)
                .await
                .expect("write the capture");

            let error = load(Some(&path)).await.expect_err(name);

            assert2::assert!(
                let RestoreError::Integrity(message) = &error,
                "case {name}: {error}"
            );
            check!(message == expected, "case {name}");
        }
    }
}
