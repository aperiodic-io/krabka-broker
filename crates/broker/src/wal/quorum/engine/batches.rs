//! Decoding of a raw WAL byte range into the individual record batches that
//! replication ships, together with the exactness gate those readers apply.
//!
//! Replication must carry a whole offset range or none of it, so a read that
//! returns a ragged prefix has to fail rather than ship a short copy. The
//! `exact_batches` check therefore lives beside the readers it guards, and
//! apart from the quorum bookkeeping that consumes them.

use {
    super::BatchBytes,
    crate::{error::BrokerError, wal::quorum::log_view::ShardLog},
    bytes::Bytes,
    krabka_ids::{LeaderEpoch, Offset, ProducerId},
    krabka_log::{Log, VerbatimBatch},
    krabka_protocol::records::RecordBatch,
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
};

fn read_batches(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source.lock().read_raw(
        start,
        target,
        // Replication must carry every batch in `start..target`, so the read
        // is uncapped.
        ByteSize::from_bytes(u64::MAX),
    )?;
    split_batches(&raw.bytes)
}

pub(in crate::wal::quorum) fn read_batches_exact(
    source: &ShardLog,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    exact_batches(read_batches(source, start, target)?, start, target)
}

pub(in crate::wal::quorum) fn read_log_batches_exact(
    source: &Log,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let unlimited = ByteSize::from_bytes(u64::MAX);
    let raw = source.read_raw(start, target, unlimited)?;
    exact_batches(split_batches(&raw.bytes)?, start, target)
}

/// Read whole batches containing a logical floor, while requiring an exact end.
/// Callers copying into an empty log must reset at the returned first batch base
/// and restore their logical floor after the physical append.
pub(in crate::wal::quorum) fn read_log_batches_covering(
    source: &Log,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let raw = source.read_raw(start, target, ByteSize::from_bytes(u64::MAX))?;
    let batches = split_batches(&raw.bytes)?;
    let bases: Vec<i64> = batches.iter().map(|batch| batch.base_offset.0).collect();
    let lasts: Vec<i64> = batches.iter().map(|batch| batch.last_offset.0).collect();
    if krabka_verified::wal::wal_covering_batch_range(&bases, &lasts, start.0, target.0).is_none() {
        return Err(BrokerError::Replication(format!(
            "wal source does not contain whole batches covering {}..{}",
            start.0, target.0
        )));
    }
    Ok(batches)
}

pub(super) fn exact_batches(
    batches: Vec<BatchBytes>,
    start: Offset,
    target: Offset,
) -> Result<Vec<BatchBytes>, BrokerError> {
    let bases: Vec<i64> = batches.iter().map(|batch| batch.base_offset.0).collect();
    let lasts: Vec<i64> = batches.iter().map(|batch| batch.last_offset.0).collect();
    if !krabka_verified::exact_wal_batch_range(&bases, &lasts, start.0, target.0) {
        return Err(BrokerError::Replication(format!(
            "wal source does not contain the complete range {}..{}",
            start.0, target.0
        )));
    }
    Ok(batches)
}

pub(in crate::wal::quorum) fn split_batches(bytes: &Bytes) -> Result<Vec<BatchBytes>, BrokerError> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let mut cur = bytes.slice(offset..);
        let remaining = cur.len();
        let batch = RecordBatch::decode(&mut cur)
            .map_err(|err| BrokerError::Replication(format!("decode WAL batch: {err}")))?;
        let consumed = remaining
            .checked_sub(cur.len())
            .and_then(std::num::NonZeroUsize::new)
            .ok_or_else(|| BrokerError::Replication("invalid WAL batch length".into()))?;
        let end = offset
            .checked_add(consumed.get())
            .filter(|end| bytes.get(offset..*end).is_some())
            .ok_or_else(|| BrokerError::Replication("invalid WAL batch length".into()))?;
        let base_offset = Offset(batch.base_offset);
        let delta = i64::from(batch.last_offset_delta);
        let last_offset = Offset(
            batch
                .base_offset
                .checked_add(delta)
                .filter(|_| delta >= 0)
                .ok_or_else(|| BrokerError::Replication("invalid WAL batch offset range".into()))?,
        );
        out.push(BatchBytes {
            base_offset,
            last_offset,
            verbatim: VerbatimBatch {
                bytes: bytes.slice(offset..end),
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                leader_epoch: LeaderEpoch(batch.partition_leader_epoch),
                producer_id: ProducerId(batch.producer_id),
                producer_epoch: batch.producer_epoch,
                base_sequence: batch.base_sequence,
                is_transactional: batch.attributes.is_transactional(),
            },
        });
        offset = end;
    }
    Ok(out)
}
