//! The internal stamp coordinate: source injection, the `.stampindex`
//! sidecars, and the stamp lookups.
//!
//! A partition stamps nothing until a [`StampSource`] is injected, and the
//! stamp never reaches the wire, so this module is the whole surface of a
//! feature that leaves the Kafka-visible bytes untouched.

use {
    super::{
        Log,
        control::{COMMIT_CONTROL_TYPE, batch_control_marker_type},
    },
    crate::{
        error::LogError,
        stamp_index::{OffsetRange, StampEntry, StampIndex},
        stamp_source::StampSource,
    },
    krabka_ids::Offset,
    krabka_protocol::records::RecordBatch,
    std::{collections::BTreeMap, path::PathBuf},
};

impl Log {
    /// Inject the [`StampSource`] that folds the additional internal stamp
    /// coordinate into this partition.
    ///
    /// This method opens or recovers every local segment's `.stampindex`.
    /// From this point each durable non-transactional batch is stamped at
    /// append, while transactional batches are stamped together when their
    /// COMMIT marker lands. Aborted and still-open transactions stay
    /// unstamped.
    ///
    /// This is the only switch that enables the feature. With no source
    /// injected the log stamps nothing and its bytes are identical to those
    /// of an unstamped log. It changes no wire-facing state: not the `.log`
    /// bytes, not offset assignment, not the LSO, and not the
    /// high-watermark.
    ///
    /// # Errors
    /// Returns an error when opening the `.stampindex` sidecar fails.
    /// # Panics
    /// Panics only if there is no active segment, which cannot happen after
    /// [`Log::open`].
    pub fn set_stamp_source(
        &mut self,
        source: std::sync::Arc<dyn StampSource>,
    ) -> Result<(), LogError> {
        let mut indexes = self
            .segments
            .iter()
            .chain(self.active.iter())
            .map(|segment| {
                let mut index = StampIndex::open(segment.stamp_index_path())?;
                index.set_io(self.io.clone());
                Ok((segment.base_offset(), index))
            })
            .collect::<Result<BTreeMap<_, _>, LogError>>()?;
        let pending_ranges: Vec<OffsetRange> = self
            .pending_stamp_ranges
            .values()
            .flatten()
            .copied()
            .collect();
        for index in indexes.values_mut() {
            index.remove_ranges(&pending_ranges)?;
        }
        if let Some(horizon) = indexes
            .values()
            .flat_map(StampIndex::entries)
            .map(|entry| entry.stamp)
            .max()
        {
            source.observe(horizon);
        }
        self.stamp_indexes = indexes;
        self.stamp_source = Some(source);
        Ok(())
    }

    /// Clone the currently installed internal stamp source.
    ///
    /// Log-directory moves use this to preserve stamping when they close and
    /// reopen a partition around the final filesystem swap.
    #[must_use]
    pub fn stamp_source(&self) -> Option<std::sync::Arc<dyn StampSource>> {
        self.stamp_source.as_ref().map(std::sync::Arc::clone)
    }

    /// The internal stamp that covers `offset`, or `None` when there is no
    /// such stamp.
    ///
    /// The result is `None` when the partition is unstamped, that is, when no
    /// source is injected. It is also `None` when no stamped range covers
    /// `offset`. This method searches all local segment indexes because a
    /// transaction can commit after the segment holding its data has rolled.
    /// This is an internal, server-side query. No produce or fetch handler
    /// calls it, so the stamp can never reach a client-facing response.
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        self.stamp_indexes
            .values()
            .find_map(|index| index.stamp_for_offset(offset))
    }

    /// Record a stamped `[base, last]` range for a durably-appended data
    /// batch.
    ///
    /// This method does nothing when no [`StampSource`] is injected.
    pub(super) fn record_stamp(&mut self, base: Offset, last: Offset) -> Result<(), LogError> {
        let Some(source) = self.stamp_source.as_ref() else {
            return Ok(());
        };
        self.record_stamp_value(base, last, source.next_stamp())
    }

    /// Record a supplied commit stamp for one transactional data range.
    pub(super) fn record_stamp_value(
        &mut self,
        base: Offset,
        last: Offset,
        stamp: u64,
    ) -> Result<(), LogError> {
        if self.stamp_source.is_none() {
            return Ok(());
        }
        // Retention can remove an old transactional batch before its marker
        // arrives. There is no local data left to stamp in that case, and the
        // marker must still be allowed to close the transaction.
        if last < self.local_log_start_offset() {
            return Ok(());
        }
        let (_, index) = self
            .stamp_indexes
            .range_mut(..=base)
            .next_back()
            .ok_or_else(|| {
                LogError::Corrupt(format!(
                    "no stamp index segment covers transactional range {base}..={last}"
                ))
            })?;
        index.upsert(StampEntry {
            base_offset: base,
            last_offset: last,
            stamp,
        })
    }

    /// Reopen the active `.stampindex` for a new active segment.
    ///
    /// This mirrors the `active_txn_index` reopen on roll, truncate, and
    /// reset. When no source is injected, this method clears the index, which
    /// is already absent, and does no I/O.
    ///
    /// # Errors
    /// Returns an error when opening the `.stampindex` sidecar fails.
    pub(super) fn reopen_active_stamp_index(
        &mut self,
        base_offset: Offset,
        path: PathBuf,
    ) -> Result<(), LogError> {
        if self.stamp_source.is_some() {
            let mut index = StampIndex::open(path)?;
            index.set_io(self.io.clone());
            self.stamp_indexes.insert(base_offset, index);
        }
        Ok(())
    }

    pub(super) fn validate_commit_stamp_batch(&self, batch: &RecordBatch) -> Result<(), LogError> {
        let is_commit = batch.attributes.is_control_batch()
            && batch_control_marker_type(batch)? == Some(COMMIT_CONTROL_TYPE);
        if !is_commit {
            return Err(LogError::InvalidArgument(
                "an explicit transaction stamp requires a COMMIT control batch".into(),
            ));
        }
        if self.stamp_source.is_none() {
            return Err(LogError::InvalidArgument(
                "an explicit transaction stamp requires an installed stamp source".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn observe_stamp(&self, stamp: u64) {
        if let Some(source) = &self.stamp_source {
            source.observe(stamp);
        }
    }
}

#[cfg(test)]
mod tests;
