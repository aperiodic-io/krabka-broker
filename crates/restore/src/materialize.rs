//! Writing the restored cluster: the format, then the partition data.
//!
//! This module owns everything that touches the target log directory. It
//! formats the target through `krabka_format::run_from_args_with_records`,
//! forwarding the `--cluster-id`, `--node-id`, `--standalone`,
//! `--initial-controllers`, `--no-initial-controllers`, and
//! `--controller-listener` flags and seeding the `TopicRecord` and one
//! `PartitionRecord` per partition that the inventory recovered, so the
//! restored cluster boots with its topics already present. It then writes each
//! verified segment into the target partition, applying
//! [`Predicates`](crate::bound::Predicates) as it walks the batches: a batch
//! the bound keeps is written verbatim through
//! [`Log::append_verbatim_at`](krabka_log::Log::append_verbatim_at), with its
//! base offset and leader epoch restamped and its producer CRC untouched; a
//! batch the bound filters is rewritten through [`krabka_log::filter_batch`]
//! and written through [`Log::append_at`](krabka_log::Log::append_at); and a
//! batch every one of whose records the bound excludes is still written
//! through [`Log::append_at`](krabka_log::Log::append_at), as a bare header
//! with zero records and the archived `base_offset` and `last_offset_delta`
//! preserved. That third case is not optional:
//! [`Log::append_at`](krabka_log::Log::append_at) and
//! [`Log::append_verbatim_at`](krabka_log::Log::append_verbatim_at) both
//! preserve forward offset gaps. Empty filtered batches retain the archived span,
//! while segment alignment allows a forward gap only after an explicitly skipped
//! corrupt predecessor, preserving the offsets on both sides of the loss. Under
//! `--dry-run` it does the same work and writes nothing.

mod format;
pub(crate) mod prepare;
mod segment;
pub(crate) use {
    format::{format_target_with_snapshot, seed_rlmm_snapshot_from},
    segment::write_segment_after_skip,
};
#[cfg(test)]
mod test_support;

pub use self::{
    format::{FormatTargetOutcome, format_target, seed_rlmm_snapshot},
    segment::{SegmentOutcome, write_segment},
};
