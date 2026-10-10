//! The append itself: the hand-off to the partition's writer actor, the
//! `acks=-1` high-watermark gate, and the idempotent-producer commit that
//! follows a successful append.
//!
//! The two halves are separate on purpose. Kafka's `ReplicaManager
//! .appendRecords` appends every partition of a request to the local log and
//! only then registers ONE `DelayedProduce` covering all of them, so the
//! replication waits overlap and the request's `timeout.ms` bounds the whole
//! request rather than each partition in turn. [`dispatch_prepared`] is the
//! first half, and it stops at the writer's answer; [`PendingAck`] is what an
//! `acks=all` partition hands back so the handler can drive every partition's
//! wait together.

use std::sync::Arc;

use krabka_log::{Offset, VerbatimBatch};
use krabka_protocol::owned::produce_response::PartitionProduceResponse;
use tokio::sync::oneshot;

use super::{
    ACKS_ALL, INVALID_OFFSET, NO_LOG_APPEND_TIME, durability_frontier,
    prepare::{PreparedBatch, PreparedSource},
};
use crate::{
    codes,
    error::BrokerError,
    partition::{
        AppendedBatch, Partition, ProduceData, ProduceJob, ReplicationTarget, WriterMessage,
    },
};

/// The partition metadata-transition barrier a produce holds from the dedup
/// gate through to its ack. It is an owned guard, so it travels into a
/// [`PendingAck`] and keeps that promise across the overlapped wait.
pub(super) type ProduceTransition = tokio::sync::OwnedRwLockReadGuard<ReplicationTarget>;

#[derive(Clone, Copy)]
pub(super) struct AppendContext<'a> {
    pub(super) partition: &'a Arc<crate::partition::Partition>,
    pub(super) producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub(super) partition_index: i32,
    pub(super) acks: i16,
    pub(super) leader_epoch: i32,
    /// The request's phase accumulator. This partition's writer round-trip is
    /// charged to the local phase here; its `acks=-1` high-watermark wait is
    /// charged to the remote phase once, by the handler, around the one wait
    /// that covers every partition of the request.
    pub(super) phases: &'a crate::metrics::RequestPhases,
    /// The transaction check the log runs just before the append.
    pub(super) producer_check: Option<crate::partition::ProducerAppendCheck>,
}

/// What the append half of one partition produced.
pub(super) enum AppendOutcome {
    /// The row is complete. Every pre-append refusal, every `acks != -1`
    /// append, and every writer failure lands here.
    Answered(PartitionProduceResponse),
    /// The batch is on this broker's log and the request asked for `acks=-1`,
    /// so the row is complete only once the high watermark covers it.
    AwaitDurability {
        /// The row so far: the assigned offset and the log-append stamp. The
        /// gate decides its `error_code`, and [`PendingAck::finish`] fills in
        /// the log start offset it reads after the gate.
        response: PartitionProduceResponse,
        /// The exclusive frontier the high watermark has to reach.
        target: Offset,
        /// The idempotent-producer commit this append owes once the gate
        /// resolves, or `None` for a non-idempotent producer.
        commit: Option<AppendCommit>,
    },
}

/// One partition's outstanding `acks=-1` high-watermark wait.
///
/// The handler collects one of these per appended partition and drives them
/// all against a single deadline, which is what Kafka's one `DelayedProduce`
/// per request does. Everything the finish needs is owned, because the wait
/// outlives the per-partition pipeline that created it: the partition handle
/// is an `Arc`, the commit carries the topic name as the `Arc<str>` the metric
/// labels already share, and the metadata-transition barrier is an owned
/// guard.
pub(super) struct PendingAck {
    response: PartitionProduceResponse,
    partition: Arc<Partition>,
    target: Offset,
    commit: Option<AppendCommit>,
    /// The transition barrier this produce took at the leadership gate. A
    /// diskless promotion takes the write side while it hydrates the canonical
    /// log and rebuilds producer state, so holding the read side here is what
    /// keeps that publication boundary from landing between an append and its
    /// ack. The completion reads the topic id it was admitted under from it.
    transition: ProduceTransition,
}

impl PendingAck {
    /// Build the wait for a partition that appended, or for a recognized
    /// idempotent retry whose original append is not covered by the high
    /// watermark yet.
    pub(super) fn new(
        response: PartitionProduceResponse,
        partition: Arc<Partition>,
        target: Offset,
        commit: Option<AppendCommit>,
        transition: ProduceTransition,
    ) -> Self {
        Self {
            response,
            partition,
            target,
            commit,
            transition,
        }
    }

    /// Wait for the high watermark to cover this partition's append, then
    /// finish its row.
    ///
    /// `deadline` is the request's, computed once for every partition of it,
    /// so N partitions of one `acks=-1` request wait out one `timeout.ms`
    /// between them rather than N of them in turn.
    ///
    /// `current_min_isr` reads the partition in the current image once the
    /// wait ends, given the topic id the append was admitted under: the
    /// effective `min.insync.replicas`, as Kafka's
    /// `Partition.checkEnoughReplicasReachOffset` reads `effectiveMinIsr` at
    /// completion, or the error code of a partition that is gone or was
    /// replaced by a recreated topic, as Kafka's `DelayedProduce` answers
    /// through `ReplicaManager.getPartitionOrError`. That error is the row's
    /// code; otherwise the code is [`ack_completion_code`]'s. Every path
    /// commits afterwards: the append is durable on the leader, so the
    /// idempotent tracker must advance either way, and a retry is then
    /// recognized as a duplicate rather than as out-of-order.
    pub(super) async fn finish(
        mut self,
        deadline: std::time::Instant,
        current_min_isr: impl FnOnce(&Partition, Option<uuid::Uuid>) -> Result<usize, i16>,
    ) -> PartitionProduceResponse {
        let gate = self
            .partition
            .await_hw_at_least(self.target, deadline)
            .await;
        let isr_size = match gate {
            Ok(()) => Some(self.partition.replica_state.lock().await.isr.len()),
            Err(_timeout) => None,
        };
        self.response.error_code = match current_min_isr(&self.partition, self.transition.topic_id)
        {
            Ok(effective_min_isr) => ack_completion_code(isr_size, effective_min_isr),
            Err(code) => code,
        };
        // The row's log start offset is read after the gate on both paths, the
        // way it was when the gate ran inline.
        self.response.log_start_offset = stamp_log_start(&self.partition);
        if let Some(commit) = &self.commit {
            commit.record(&self.partition, self.target).await;
        }
        self.response
    }
}

/// The row code of an `acks=all` append once its high-watermark wait ends.
///
/// `isr_size` is the ISR size when the high watermark reached the append, or
/// `None` when the wait expired first. Kafka's `DelayedProduce` starts every
/// partition at `REQUEST_TIMED_OUT` and leaves it there on expiration;
/// `Partition.checkEnoughReplicasReachOffset` answers
/// `NOT_ENOUGH_REPLICAS_AFTER_APPEND` when the high watermark reached the
/// offset but the ISR has shrunk below the effective `min.insync.replicas`.
pub(super) fn ack_completion_code(isr_size: Option<usize>, effective_min_isr: usize) -> i16 {
    match isr_size {
        None => codes::REQUEST_TIMED_OUT,
        Some(size) if size < effective_min_isr => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        Some(_) => codes::NONE,
    }
}

pub(super) async fn dispatch_prepared(
    prepared: PreparedBatch,
    context: AppendContext<'_>,
    shared_topic: &Arc<str>,
) -> Result<AppendOutcome, BrokerError> {
    // No offset is assigned until the writer answers with one. Every failure
    // below — the writer channel gone, the append itself erroring — leaves the
    // row without an append, which Kafka answers with `firstOffset` -1. `logStartOffset` stays -1 too, except on the refusals
    // `failed_append_log_start` names. `finalize_ack` overwrites both on the
    // one path that appends.
    let mut response = PartitionProduceResponse {
        index: context.partition_index,
        base_offset: INVALID_OFFSET,
        ..Default::default()
    };
    let commit = AppendCommit {
        producer_state: Arc::clone(context.producer_state),
        topic: Arc::clone(shared_topic),
        partition: context.partition_index,
        pid: prepared.producer_id,
        epoch: prepared.producer_epoch,
        base_seq: prepared.base_sequence,
        last_offset_delta: prepared.last_offset_delta,
        max_timestamp: prepared.max_timestamp,
        is_transactional: prepared.attributes.is_transactional(),
        base_offset: Offset(INVALID_OFFSET),
    };
    let data = build_produce_data(prepared, context.leader_epoch);
    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob {
        data,
        ack: ack_tx,
        producer_check: context.producer_check,
    });
    // The local phase opens here and closes when the writer answers: the
    // enqueue plus the append is the work this broker's own log does for this
    // partition. A send failure is charged too, so the phase covers every exit.
    let local_started = std::time::Instant::now();
    if context.partition.writer_tx.send(job).await.is_err() {
        context.phases.add_local(local_started.elapsed());
        response.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        return Ok(AppendOutcome::Answered(response));
    }
    // The wait has no deadline. Kafka's `ReplicaManager.appendRecords` appends
    // to the local log synchronously, and the request's `timeout.ms` bounds
    // only the `acks=-1` wait for replication that follows
    // (`DelayedProduce`, which `PendingAck` is). Bounding this wait as well
    // answered `REQUEST_TIMED_OUT` for a batch the writer then appended anyway,
    // and a producer that retried it wrote it twice. It closed the connection
    // of an `acks=0` producer too, which Kafka never does over a timeout.
    let acked = ack_rx.await;
    context.phases.add_local(local_started.elapsed());
    match acked {
        Ok(Ok(appended)) => return Ok(finalize_ack(response, context, appended, commit).await),
        Ok(Err(error)) => {
            response.error_code = codes::from_broker_error(&error);
            response.log_start_offset =
                failed_append_log_start(response.error_code, context.partition);
        }
        Err(_) => response.error_code = codes::NOT_LEADER_OR_FOLLOWER,
    }
    Ok(AppendOutcome::Answered(response))
}

/// The idempotent-producer dedup identity and the fields a
/// `producer_state.commit` record needs, owned so it can outlive the pipeline
/// that built it and travel into a [`PendingAck`].
///
/// The struct groups the eight positional `commit` arguments into one value,
/// so the commit call exists in exactly one place, [`AppendCommit::record`].
pub(super) struct AppendCommit {
    producer_state: Arc<crate::producer_state::ProducerState>,
    topic: Arc<str>,
    partition: i32,
    pid: i64,
    epoch: i16,
    base_seq: i32,
    last_offset_delta: i32,
    max_timestamp: i64,
    /// Whether the batch is transactional: its commit opens a transaction on
    /// the producer's tracked entry when none is open.
    is_transactional: bool,
    base_offset: Offset,
}

impl AppendCommit {
    /// Record the idempotent-producer commit for an append the log still
    /// holds.
    ///
    /// Only record it if the appended batch is still on the leader's log. A
    /// failover-rejoin divergence truncation can remove the batch while the
    /// acks=all HW gate is waiting (the gate then times out); recording the
    /// truncated batch would make a retry dedup against an offset the log no
    /// longer holds, and the retry's HW gate would wait forever for a high
    /// watermark that can never reach the vanished offset. Skipping the commit
    /// lets the retry re-append fresh instead.
    ///
    /// This is a best-effort check: a truncation racing in between this read
    /// and the commit below could still record a stale entry. That is
    /// tolerated because the replicator calls `ProducerState::truncate` after
    /// *every* log truncation, so any entry stranded by such a race is dropped
    /// by the next truncation/failover. Do not "harden" this by removing the
    /// check — the check is what avoids recording the common
    /// (already-truncated) case.
    async fn record(&self, part: &Partition, target: Offset) {
        if self.pid < 0 {
            return;
        }
        if part.log_end_offset() < target {
            // Evidence for the on-cluster failover verification: the appended
            // batch was truncated before its dedup commit, so we skip the
            // commit (the retry re-appends). Seeing this fire confirms the
            // Bug-D path executed.
            tracing::warn!(
                topic = &*self.topic,
                partition = self.partition,
                base_offset = self.base_offset.0,
                target = target.0,
                leo = part.log_end_offset().0,
                "produce: appended batch truncated before dedup commit; skipping commit so retry re-appends"
            );
            return;
        }
        self.producer_state
            .commit(
                &self.topic,
                krabka_ids::PartitionIndex(self.partition),
                (self.pid, self.epoch),
                (self.base_seq, self.last_offset_delta),
                // Unwrap the assigned `Offset` into the dedup tracker's `i64`.
                (
                    self.base_offset.0,
                    self.max_timestamp,
                    self.is_transactional,
                ),
            )
            .await;
    }
}

/// An appended row carries the partition's real log start offset, not the -1
/// that `UNKNOWN_LOG_APPEND_INFO` supplies to the pre-append refusals.
///
/// Kafka fills `LogAppendInfo.logStartOffset` from `UnifiedLog`'s own pointer
/// at append time and `ReplicaManager` copies it straight into the partition
/// row, so the value moves whenever retention, a `DeleteRecords` or a tiering
/// upload advances the log start. Four single-record batches into
/// `apache/kafka:4.3.1`, then a `kafka-delete-records` to offset 3, then one
/// more raw `Produce v8`, answers `error_code=0 base_offset=4
/// log_append_time_ms=-1 log_start_offset=3`.
///
/// The read is one brief lock on the partition's log, the same one
/// `log_end_offset()` takes.
fn stamp_log_start(part: &Partition) -> i64 {
    part.log_start_offset().0
}

/// The log start offset of a row whose append the log refused.
///
/// Kafka's `ReplicaManager.appendToLocalLog` answers the refusals it catches
/// in its generic `Throwable` arm with
/// `LogAppendInfo.unknownLogAppendInfoWithLogStartOffset`, which carries the
/// partition's real log start offset from `processFailedRecord`. That arm
/// takes `OutOfOrderSequenceException`, `InvalidProducerEpochException`,
/// `InvalidTxnStateException`, the three producer-state refusals a writer
/// answers. The storage and routing refusals keep the `-1` of
/// `UNKNOWN_LOG_APPEND_INFO`.
fn failed_append_log_start(error_code: i16, part: &Partition) -> i64 {
    match error_code {
        codes::OUT_OF_ORDER_SEQUENCE_NUMBER
        | codes::INVALID_PRODUCER_EPOCH
        | codes::INVALID_TXN_STATE => stamp_log_start(part),
        _ => INVALID_OFFSET,
    }
}

/// Finalize a successful writer append.
///
/// The function sets the response `base_offset` and `log_append_time_ms`, and
/// then either finishes the row outright or hands the caller the
/// high-watermark wait that still stands between the append and its ack.
///
/// The behavior per path:
///   * `acks != -1`: NONE, then commit, all here;
///   * `acks == -1`: [`AppendOutcome::AwaitDurability`], and
///     [`PendingAck::finish`] decides the code and commits after the gate.
///
/// Note that the commit happens on *both* the success and the timeout
/// `acks=-1` sub-paths. Whichever half runs it, it runs once the `error_code`
/// and the `base_offset` are decided.
///
/// The high-watermark gate is the remote phase of the request: the append is
/// already durable on this broker, and everything the gate waits for is a
/// follower taking it. The handler charges that phase once, around the one
/// wait that covers every partition it appended to.
///
/// The function takes the whole [`AppendContext`] rather than the fields it
/// reads out of it, because the caller has one and the fields travel together
/// everywhere on this path.
async fn finalize_ack(
    mut out: PartitionProduceResponse,
    context: AppendContext<'_>,
    appended: AppendedBatch,
    mut commit: AppendCommit,
) -> AppendOutcome {
    let AppendedBatch {
        base_offset,
        log_append_time_ms,
    } = appended;
    let part = context.partition;
    let Some(target) = durability_frontier(base_offset.0, commit.last_offset_delta) else {
        out.error_code = codes::INVALID_RECORD;
        out.base_offset = -1;
        return AppendOutcome::Answered(out);
    };
    commit.base_offset = base_offset;
    // Kafka's retained `BatchMetadata.timestamp` is the batch's max timestamp
    // after the log stamped it, which is the append time on a `LogAppendTime`
    // topic. A duplicate answers with it.
    if let Some(stamped) = log_append_time_ms {
        commit.max_timestamp = stamped;
    }
    // Unwrap the assigned `Offset` into the wire `base_offset` response field.
    out.base_offset = base_offset.0;
    // KIP-32's `logAppendTimeMs`. Kafka fills it from
    // `LogAppendInfo.logAppendTime`, which `UnifiedLog.append` sets only when
    // the topic's `message.timestamp.type` is `LogAppendTime`; every other
    // topic answers the `-1` that `LogAppendInfo` starts at, and that is the
    // `None` arm here. The value is the clock reading the log stamped into the
    // batch, so what the response reports and what the records carry are one
    // number read once.
    out.log_append_time_ms = log_append_time_ms.unwrap_or(NO_LOG_APPEND_TIME);
    if context.acks == ACKS_ALL {
        return AppendOutcome::AwaitDurability {
            response: out,
            target,
            commit: Some(commit),
        };
    }
    out.error_code = codes::NONE;
    out.log_start_offset = stamp_log_start(part);
    commit.record(part, target).await;
    AppendOutcome::Answered(out)
}

/// Build the writer's [`ProduceData`] from a prepared batch and stamp the
/// leader epoch.
///
/// Verbatim batches carry the producer's exact bytes. Owned batches carry the
/// decoded `RecordBatch`, whose `partition_leader_epoch` the caller has
/// already stamped.
pub(super) fn build_produce_data(prepared: PreparedBatch, leader_epoch: i32) -> ProduceData {
    let is_transactional = prepared.attributes.is_transactional();
    match prepared.source {
        PreparedSource::Verbatim(bytes) => ProduceData::Verbatim(VerbatimBatch {
            last_offset_delta: prepared.last_offset_delta,
            max_timestamp: prepared.max_timestamp,
            // Wrap the atomic-loaded raw epoch into the log seam's `LeaderEpoch`.
            leader_epoch: krabka_log::LeaderEpoch(leader_epoch),
            // Wrap the produce path's decode-side `i64` into the log seam's `ProducerId`.
            producer_id: krabka_log::ProducerId(prepared.producer_id),
            producer_epoch: prepared.producer_epoch,
            base_sequence: prepared.base_sequence,
            is_transactional,
            bytes,
        }),
        PreparedSource::Owned(mut batch) => {
            // The writer stamps the verbatim path's epoch in-place at append;
            // the owned batch carries it as a struct field instead.
            batch.partition_leader_epoch = leader_epoch;
            ProduceData::Owned(batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_log::Offset;
    use krabka_protocol::owned::produce_response::PartitionProduceResponse;
    use krabka_raft::NodeId;

    use super::PendingAck;
    use crate::codes;

    /// Kafka's `acks=all` completion: `DelayedProduce` leaves an expired wait
    /// at `REQUEST_TIMED_OUT`, and `Partition.checkEnoughReplicasReachOffset`
    /// answers `NOT_ENOUGH_REPLICAS_AFTER_APPEND` when the high watermark
    /// arrived but the ISR is below the effective `min.insync.replicas`. It
    /// reads that minimum at completion, so a change during the wait applies.
    /// A partition that is gone or was replaced by a recreated topic answers
    /// the code `ReplicaManager.getPartitionOrError` gives it, reached or not.
    #[tokio::test]
    async fn an_acks_all_wait_ends_with_kafkas_completion_code() {
        struct Case {
            name: &'static str,
            /// The frontier the high watermark has to reach. The log is
            /// empty, so the high watermark is 0: target 0 is reached, target
            /// 1 never is.
            target: i64,
            isr: &'static [u64],
            /// What the current image holds for the admitted partition when
            /// the wait ends: its effective minimum, or the code of a
            /// partition that is gone or replaced.
            current_min_isr: Result<usize, i16>,
            error_code: i16,
        }
        let cases = [
            Case {
                name: "reached with a full ISR",
                target: 0,
                isr: &[1, 2, 3],
                current_min_isr: Ok(2),
                error_code: codes::NONE,
            },
            Case {
                name: "reached with the ISR at the minimum",
                target: 0,
                isr: &[1, 2],
                current_min_isr: Ok(2),
                error_code: codes::NONE,
            },
            Case {
                name: "reached after the ISR shrank below the minimum",
                target: 0,
                isr: &[1],
                current_min_isr: Ok(2),
                error_code: codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            },
            Case {
                name: "reached after the minimum rose above the ISR",
                target: 0,
                isr: &[1, 2],
                current_min_isr: Ok(3),
                error_code: codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            },
            Case {
                name: "reached after the minimum fell to the ISR",
                target: 0,
                isr: &[1],
                current_min_isr: Ok(1),
                error_code: codes::NONE,
            },
            Case {
                name: "reached after the topic was recreated",
                target: 0,
                isr: &[1, 2, 3],
                current_min_isr: Err(codes::NOT_LEADER_OR_FOLLOWER),
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
            },
            Case {
                name: "reached after the topic was deleted",
                target: 0,
                isr: &[1, 2, 3],
                current_min_isr: Err(codes::UNKNOWN_TOPIC_OR_PARTITION),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            },
            Case {
                name: "expired",
                target: 1,
                isr: &[1, 2, 3],
                current_min_isr: Ok(2),
                error_code: codes::REQUEST_TIMED_OUT,
            },
            Case {
                name: "expired after the topic was deleted",
                target: 1,
                isr: &[1, 2, 3],
                current_min_isr: Err(codes::UNKNOWN_TOPIC_OR_PARTITION),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            },
        ];
        for case in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
            std::fs::create_dir_all(&part_dir).expect("partition dir");
            let partition = crate::test_support::spawn_standalone_partition(
                dir.path(),
                krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).expect("log"),
                crate::test_support::StandalonePartitionSetup::default(),
            );
            let isr: Vec<NodeId> = case.isr.iter().copied().map(NodeId).collect();
            partition
                .install_isr(&isr, &[NodeId(1), NodeId(2), NodeId(3)], NodeId(1))
                .await;
            let transition = partition.lock_produce_transition().await;
            let admitted_topic_id = transition.topic_id;
            let pending = PendingAck::new(
                PartitionProduceResponse {
                    index: 0,
                    base_offset: 0,
                    ..Default::default()
                },
                Arc::clone(&partition),
                Offset(case.target),
                None,
                transition,
            );
            let mut asked_for = None;
            let row = pending
                .finish(std::time::Instant::now(), |_, topic_id| {
                    asked_for = Some(topic_id);
                    case.current_min_isr
                })
                .await;
            assert!(
                (row, asked_for)
                    == (
                        PartitionProduceResponse {
                            index: 0,
                            error_code: case.error_code,
                            base_offset: 0,
                            log_start_offset: 0,
                            ..Default::default()
                        },
                        Some(admitted_topic_id)
                    ),
                "{}",
                case.name
            );
        }
    }

    /// One `acks=1` append of a one-record batch through
    /// [`super::dispatch_prepared`], to a partition whose writer answers
    /// `answer` once `delay` has passed. Returns the partition's row.
    async fn append_answered_by_the_writer(
        delay: std::time::Duration,
        answer: Result<crate::partition::AppendedBatch, crate::error::BrokerError>,
    ) -> PartitionProduceResponse {
        use bytes::Bytes;
        use krabka_compression::RecordDecompressionPolicy;
        use krabka_protocol::records::{Record, RecordBatch};

        use crate::{
            handlers::produce::{
                framing::PartitionPayload,
                hot_path::TimestampPolicy,
                prepare::{DecodeEnv, prepare_batch},
                test_support::encode_batch,
            },
            partition::{Partition, WriterMessage},
        };

        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::new(tokio::sync::Notify::new()));
        let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(1);
        let partition = Arc::new(Partition {
            writer_tx,
            ..partition
        });
        tokio::spawn(async move {
            if let Some(WriterMessage::Produce(job)) = writer_rx.recv().await {
                tokio::time::sleep(delay).await;
                let _ = job.ack.send(answer);
            }
        });
        let topic: Arc<str> = Arc::from("orders");
        let prepared = prepare_batch(
            PartitionPayload::Slice(encode_batch(&RecordBatch {
                records: vec![Record {
                    value: Some(Bytes::from_static(b"payload")),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            None,
            TimestampPolicy::default(),
            false,
            DecodeEnv {
                topic_name: &topic,
                metrics: &crate::metrics::BrokerMetrics::new(),
                policy: RecordDecompressionPolicy::default(),
            },
            13,
        )
        .expect("a well-formed batch");

        let outcome = super::dispatch_prepared(
            prepared,
            super::AppendContext {
                partition: &partition,
                producer_state: &Arc::new(crate::producer_state::ProducerState::new()),
                partition_index: 0,
                acks: 1,
                leader_epoch: 0,
                phases: &crate::metrics::RequestPhases::default(),
                producer_check: None,
            },
            &topic,
        )
        .await
        .expect("dispatch");
        let super::AppendOutcome::Answered(row) = outcome else {
            panic!("an acks=1 append is answered without a high-watermark wait");
        };
        row
    }

    /// The first append after a disk fault answers `KAFKA_STORAGE_ERROR`:
    /// Kafka's `LocalLog.maybeHandleIOException` turns the `IOException` into
    /// `KafkaStorageException`, which is retriable and makes the client
    /// refresh its metadata. `UNKNOWN_SERVER_ERROR` would fail the batch for
    /// good, and only the requests after it would meet the offline-dir gate.
    #[tokio::test]
    async fn an_io_failure_of_the_append_is_a_storage_error() {
        let row = append_answered_by_the_writer(
            std::time::Duration::ZERO,
            Err(crate::partition_writer::storage_failure_error(
                "append failed",
                "synthetic EIO",
            )),
        )
        .await;

        assert!(
            row == PartitionProduceResponse {
                index: 0,
                error_code: codes::KAFKA_STORAGE_ERROR,
                base_offset: -1,
                ..Default::default()
            }
        );
    }

    /// A writer that answers late still gets its append reported. Kafka's
    /// `timeout.ms` bounds only the `acks=-1` wait for replication, and the
    /// local append is synchronous, so a slow disk never makes the row
    /// `REQUEST_TIMED_OUT` for a batch the writer then appends anyway. The
    /// clock is paused, so the hour the writer takes costs the test nothing.
    #[tokio::test(start_paused = true)]
    async fn a_slow_writer_still_gets_its_append_reported() {
        let row = append_answered_by_the_writer(
            std::time::Duration::from_secs(3600),
            Ok(crate::partition::AppendedBatch {
                base_offset: Offset(5),
                log_append_time_ms: None,
            }),
        )
        .await;

        assert!(
            row == PartitionProduceResponse {
                index: 0,
                error_code: codes::NONE,
                base_offset: 5,
                log_append_time_ms: super::NO_LOG_APPEND_TIME,
                log_start_offset: 0,
                ..Default::default()
            }
        );
    }
}
