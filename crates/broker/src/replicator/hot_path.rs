//! A benchmark seam over the replicator's per-batch work: the
//! `RecordBatch::encoded_len` walk it does for the replication-bytes metric,
//! and the [`Partition::replicate_batch`] call that carries the same batch to
//! the follower's log.
//!
//! The loop that runs these two steps in production
//! ([`super::response::handle_partition_response`]) also holds a leader
//! connection, a fetch session and a replication-target guard, none of which
//! the "what does the extra `encoded_len` walk cost" question depends on. This
//! module is the partition and its writer task with none of that, so
//! `benches/perf_deferrals.rs` can time the walk against the append it sits in
//! front of.
//!
//! [`ReplicaSeam::replicate`] is the production `replicate_batch`: the same
//! channel send to the same writer actor, which runs the same `append_at`.
use {
    crate::{
        config::BrokerConfig,
        error::BrokerError,
        partition::Partition,
        partition_writer::{WriterServices, run_with_sequencer},
    },
    arc_swap::ArcSwap,
    krabka_log::{Log, LogConfig, LogError, Offset},
    krabka_protocol::records::RecordBatch,
    std::{
        path::Path,
        sync::{Arc, Mutex},
    },
};

/// A follower partition over `dir`, with its writer actor running.
///
/// The caller owns `dir`; the seam does not create or remove it, because a
/// temp-directory crate is a dev-dependency and this module also compiles in a
/// plain `test-helpers` build.
pub struct ReplicaSeam {
    partition: Partition,
}

impl ReplicaSeam {
    /// Open a log under `dir` and spawn the partition writer that drains its
    /// [`crate::partition::WriterMessage`] channel.
    ///
    /// Must be called from inside a Tokio runtime: the writer is a spawned
    /// task, exactly as it is under `Broker`.
    ///
    /// # Errors
    ///
    /// Returns [`LogError`] when the log directory cannot be opened.
    pub fn spawn(dir: &Path) -> Result<Self, LogError> {
        let log = Arc::new(Mutex::new(Log::open(dir, LogConfig::default())?));
        let log_dir = Arc::new(ArcSwap::from_pointee(dir.to_path_buf()));
        Ok(Self {
            partition: Partition::writer_fixture(
                "bench-topic",
                log,
                log_dir,
                |identity, storage, rx, signals| {
                    tokio::spawn(run_with_sequencer(
                        identity,
                        storage,
                        rx,
                        signals,
                        WriterServices::default(),
                        BrokerConfig::default().max_produce_group,
                        None,
                    ))
                },
            ),
        })
    }

    /// The offset the leader would assign to the next replicated batch.
    ///
    /// `append_at` rejects a batch whose `base_offset` is not the log end, so
    /// a caller stamps this into the batch the way the leader already has.
    #[must_use]
    pub fn next_offset(&self) -> Offset { self.partition.log_end_offset() }

    /// Append one leader-assigned batch through the writer actor.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the writer rejects the append or dies.
    pub async fn replicate(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        self.partition.replicate_batch(batch).await
    }
}

#[cfg(test)]
mod tests;
