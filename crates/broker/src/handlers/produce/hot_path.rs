//! A benchmark seam over the produce hot path: [`prepare_batch`], the
//! writer's [`build_produce_data`], and the log append behind them.
//!
//! The pipeline that runs these three steps in production
//! ([`super::pipeline::process_partition`]) also holds a partition registry, a
//! writer actor, a transaction coordinator and an idempotent-producer tracker,
//! none of which the verbatim-versus-owned question depends on. This module is
//! the same three steps with none of that, so `benches/produce.rs` can time
//! the decision and the bytes it moves rather than the actor hand-off.
//!
//! Every step here is the production function. Nothing is re-implemented, and
//! [`append_one_batch`] applies the writer's compression rewrite in the same
//! place [`crate::partition_writer`] applies it.

use std::sync::Arc;

use bytes::Bytes;
use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_log::Log;

pub use super::topic_settings::TimestampPolicy;
use super::{
    append::build_produce_data,
    framing::PartitionPayload,
    prepare::{DecodeEnv, owned_fallback, prepare_batch},
};
use crate::{codes, error::BrokerError, metrics::BrokerMetrics, partition::ProduceData};

/// The path a records field took through [`prepare_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducePath {
    /// The producer's own bytes reached the log unchanged.
    Verbatim,
    /// The records were decoded into an owned batch and re-encoded on append.
    Owned,
}

/// Whether the seam runs the real verbatim predicate or skips straight to the
/// fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathChoice {
    /// Call [`prepare_batch`], which decides verbatim versus owned exactly as
    /// the produce pipeline does.
    Dispatch,
    /// Call the owned fallback directly, as if the predicate had rejected the
    /// batch. This is the "every batch goes down the fallback" case, whose
    /// cost against `Dispatch` is the number this seam exists to expose.
    ForceOwned,
}

/// Everything the produce hot path reads besides the records field itself.
///
/// The pipeline resolves each of these once per topic or once per partition
/// and then holds them across the batch, so the seam takes them by reference
/// too.
pub struct HotPathSettings<'a> {
    /// Names the topic for the message-conversion metric that a legacy
    /// up-convert increments. An `Arc<str>` because that is what the metric
    /// label set holds, and what the pipeline resolves once per topic — see
    /// [`crate::metrics::TopicLabel`].
    pub topic_name: Arc<str>,
    /// The topic's `compression.type`. `None` is producer pass-through, which
    /// is what keeps a batch on the verbatim path.
    pub topic_compression: Option<CompressionType>,
    /// The topic's `message.timestamp.type` and the two
    /// `message.timestamp.{before,after}.max.ms` windows. The default policy
    /// bounds nothing, which is what keeps the walk over record timestamps off
    /// the hot path.
    pub timestamps: TimestampPolicy,
    pub decompression_policy: RecordDecompressionPolicy,
    pub metrics: &'a BrokerMetrics,
    pub leader_epoch: i32,
    /// The negotiated `Produce` version, for the same zstd-below-v7 gate
    /// [`prepare_batch`] applies in production.
    pub version: i16,
}

/// Run one partition's records field through prepare, writer-data build and
/// append, and report which path it took.
///
/// The error is the Kafka response error code the partition row would carry,
/// which is either [`prepare_batch`]'s rejection or the append's failure
/// mapped the way the writer maps it.
///
/// # Errors
///
/// Returns the response error code when the records field is rejected or the
/// append fails.
pub fn append_one_batch(
    records: Bytes,
    choice: PathChoice,
    settings: &HotPathSettings<'_>,
    log: &mut Log,
) -> Result<ProducePath, i16> {
    let env = || DecodeEnv {
        topic_name: &settings.topic_name,
        metrics: settings.metrics,
        policy: settings.decompression_policy,
    };
    let prepared = match choice {
        PathChoice::Dispatch => prepare_batch(
            PartitionPayload::Slice(records),
            settings.topic_compression,
            settings.timestamps,
            false,
            env(),
            settings.version,
        )?,
        PathChoice::ForceOwned => owned_fallback(
            &records,
            settings.timestamps,
            false,
            env(),
            settings.version,
        )?,
    };
    append_produce_data(build_produce_data(prepared, settings.leader_epoch), log)
}

/// Append one writer job, applying the same compression rewrite
/// `crate::partition_writer::append` applies to an owned batch.
fn append_produce_data(data: ProduceData, log: &mut Log) -> Result<ProducePath, i16> {
    let target = log.config_snapshot().compression_type;
    let result = match data {
        ProduceData::Verbatim(batch) => log.append_verbatim(&batch).map(|_| ProducePath::Verbatim),
        ProduceData::Owned(mut batch) => {
            if let Some(target) = target
                && batch.attributes.compression() != target
            {
                batch.attributes = batch.attributes.with_compression(target);
            }
            log.append(&mut batch).map(|_| ProducePath::Owned)
        }
        // `build_produce_data` builds only the two client-produce shapes. The
        // control and commit-marker shapes come from the coordinator paths,
        // which never reach this seam.
        ProduceData::OwnedControl(_) | ProduceData::OwnedCommitMarker { .. } => {
            unreachable!("build_produce_data yields only Verbatim and Owned")
        }
    };
    result.map_err(|error| codes::from_broker_error(&BrokerError::from(error)))
}

#[cfg(test)]
mod tests;
