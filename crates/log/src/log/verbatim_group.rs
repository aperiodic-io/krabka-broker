//! Bounded Direct I/O groups for ordinary producer batches.

use krabka_ids::Offset;
use krabka_protocol::records::{Attributes, CRC_COVERAGE_START, HEADER_LEN, TimestampType};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _, mebibytes};

use super::{Log, VerbatimBatch};
use crate::{DeliveryPolicy, LogError, SegmentAllocation};

impl Log {
    /// Try to append ordinary producer batches with one aligned Direct I/O write.
    ///
    /// `None` leaves the batches unwritten. The caller then uses individual appends.
    /// Groups are at most 1 MiB and stay in one nonempty active segment. Transactions,
    /// scheduled delivery, stamps, append-time timestamps, and explicit flush policies
    /// use individual appends. The caller has already validated the batch CRCs.
    ///
    /// A successful group returns each assigned base offset in input order.
    ///
    /// # Errors
    /// A write or bookkeeping failure rolls the whole group back before returning the error.
    pub fn try_append_verbatim_group(
        &mut self,
        batches: &[&VerbatimBatch],
    ) -> Option<Result<Vec<Offset>, LogError>> {
        let config = self.config_snapshot();
        if batches.len() < 2
            || config.segment_allocation != SegmentAllocation::Preallocate
            || config.delivery_policy != DeliveryPolicy::Immediate
            || config.message_timestamp_type != TimestampType::CreateTime
            || config.flush_on_append
            || config.flush_messages.is_some()
            || config.flush_interval.is_some()
            || self.stamp_source.is_some()
        {
            return None;
        }
        let base = self.append_at_expected_offset();
        let mut next = base;
        let mut size = 0_u64;
        let mut max_timestamp = i64::MIN;
        let mut placed = Vec::with_capacity(batches.len());
        for &batch in batches {
            if batch.is_transactional || batch.bytes.len() < HEADER_LEN {
                return None;
            }
            let attributes = Attributes(i16::from_be_bytes([
                batch.bytes[CRC_COVERAGE_START],
                batch.bytes[CRC_COVERAGE_START + 1],
            ]));
            if attributes.is_control_batch() || attributes.is_transactional() {
                return None;
            }
            let (_, end) =
                krabka_verified::local_append_coordinates(next.0, next.0, batch.last_offset_delta)?;
            Self::data_producer_tail(
                batch.producer_id,
                batch.base_sequence,
                batch.last_offset_delta,
                next,
            )
            .ok()?;
            placed.push((batch, next));
            next = Offset(end);
            size = size.checked_add(u64::try_from(batch.bytes.len()).ok()?)?;
            max_timestamp = max_timestamp.max(batch.max_timestamp);
        }
        if size > mebibytes(1).bytes_u64() {
            return None;
        }
        let active = self.active.as_ref()?;
        if active.size() == ByteSize::ZERO
            || active.indexes_full_after(config.segment_index_size, batches.len())
        {
            return None;
        }
        let interval = self.jittered_roll_interval(config.segment_roll_interval);
        if self.should_roll_for_incoming(
            ByteSize::from_bytes(size),
            max_timestamp,
            config.segment_size,
            interval,
            config.segment_index_size,
        ) {
            return None;
        }
        let active = self.active.as_mut()?;
        active.reserve(config.segment_size);
        active.write_direct(config.tail_cache_size);
        if !active.writes_direct() {
            return None;
        }
        Some(self.append_direct_group(&placed, config.index_interval))
    }

    fn append_direct_group(
        &mut self,
        batches: &[(&VerbatimBatch, Offset)],
        index_interval: ByteSize,
    ) -> Result<Vec<Offset>, LogError> {
        self.rollover_flusher.check()?;
        self.active
            .as_mut()
            .expect("group admission requires an active segment")
            .append_verbatim_group(batches, index_interval)?;
        let result = (|| {
            for &(batch, base) in batches {
                self.flush_after_append(batch.last_offset_delta, false)?;
                self.update_data_producer_entry(
                    (batch.producer_id, batch.producer_epoch),
                    (batch.base_sequence, batch.last_offset_delta),
                    (base, batch.max_timestamp, false),
                )?;
                self.assign_appended_epoch(batch.leader_epoch, base)?;
            }
            self.refresh_lso()
        })();
        self.complete_append_state(result, batches[0].1)?;
        Ok(batches.iter().map(|&(_, base)| base).collect())
    }
}
