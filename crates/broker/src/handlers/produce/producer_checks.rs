//! The two producer-identity gates that run between the leadership gate and
//! the append: the KIP-1319 transactional verify, and the idempotent-producer
//! dedup that answers a retry with the offset it already assigned.

use krabka_log::Offset;
use krabka_protocol::owned::produce_response::PartitionProduceResponse;
use krabka_units::convert::TimeExt as _;

use super::{ACKS_ALL, INVALID_OFFSET, durability_frontier, prepare::PreparedBatch};
use crate::{
    codes,
    txn::coordinator::produce_verification::{
        partition_verification_enabled, skips_coordinator_verification,
    },
};

/// What the idempotent-producer dedup gate decided for one batch.
pub(super) enum DedupOutcome {
    /// Not a duplicate: the batch goes on to the append.
    Append,
    /// The gate answered the batch without an append: a recognized retry under
    /// `acks != -1`, an out-of-order sequence, or a fenced epoch.
    Answered(PartitionProduceResponse),
    /// A recognized retry under `acks=-1`. The original append is on the log,
    /// and the row is complete once the high watermark covers it — the same
    /// wait a fresh append of those records would have joined, so the caller
    /// hands it to the request's one overlapped gate rather than waiting here.
    AwaitDurability {
        /// The row so far, carrying the offset the original append was given.
        response: PartitionProduceResponse,
        /// The exclusive frontier the high watermark has to reach.
        target: Offset,
    },
}

/// The request fields the KIP-890 transaction check needs.
#[derive(Debug, Clone, Copy)]
pub(super) struct TransactionRequest<'a> {
    /// The `Produce` request's `transactional_id`.
    pub(super) transactional_id: Option<&'a str>,
    /// The negotiated `Produce` version.
    pub(super) version: i16,
    /// `producer.id.expiration.ms`, which also bounds unused verification
    /// state.
    pub(super) producer_id_expiration_ms: i64,
    /// `transaction.partition.verification.enable` on this broker. When it is
    /// off, a `Produce` below v12 is appended without the coordinator's check.
    pub(super) verification_enabled: bool,
}

impl<'a> TransactionRequest<'a> {
    /// The fields of a `Produce` request that `broker` reads from its own
    /// configuration and from `image`.
    pub(super) fn new(
        broker: &crate::broker::Broker,
        image: &krabka_metadata::MetadataImage,
        (transactional_id, version): (Option<&'a str>, i16),
    ) -> Self {
        Self {
            transactional_id,
            version,
            producer_id_expiration_ms: broker.config.producer_id_expiration.millis_i64(),
            verification_enabled: partition_verification_enabled(image, &broker.config),
        }
    }
}

/// The first `Produce` version of transaction version 2. Kafka's
/// `AddPartitionsToTxnManager.produceRequestVersionToTransactionSupportedOperation`
/// gives `ADD_PARTITION` from here, so the leader adds the partition to the
/// transaction instead of only verifying it.
const FIRST_ADD_PARTITION_PRODUCE_VERSION: i16 = 12;

/// The last `Produce` version whose client does not know
/// `TRANSACTION_ABORTABLE` (Kafka's `DEFAULT_ERROR`).
const LAST_DEFAULT_ERROR_PRODUCE_VERSION: i16 = 10;

/// Kafka's `add.partitions.to.txn.retry.backoff.ms` default.
const CONCURRENT_TRANSACTIONS_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Kafka's `add.partitions.to.txn.retry.backoff.max.ms` default, which bounds
/// the retries of a transaction version 2 check that answers
/// `CONCURRENT_TRANSACTIONS`.
const CONCURRENT_TRANSACTIONS_RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// Where the KIP-890 check of one batch stands before the coordinator call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verification {
    /// The append runs this check, and no coordinator call is needed.
    Settled(Option<crate::partition::ProducerAppendCheck>),
    /// The coordinator of the request's `transactional_id` has to add or
    /// verify the partition first. The check holds the guard the log
    /// started.
    Coordinator(crate::partition::ProducerAppendCheck),
}

/// KIP-890 part 1: find out whether a batch starts a transaction the
/// coordinator has to know about, and the check the log runs before the
/// append.
///
/// This is Kafka's `ReplicaManager.handleProduceAppend` up to the coordinator
/// call:
///
/// - A transactional batch whose producer has no open transaction at the
///   batch epoch starts a verification on the log. The coordinator of the
///   request's `transactional_id` then verifies the partition (`Produce`
///   below v12) or adds it (v12 and later), in one call for every such
///   partition of the request: [`verify_with_coordinator`]. A request without
///   a `transactional_id` skips the call, and the append then refuses the
///   batch.
/// - A batch at an epoch below the producer's epoch answers
///   `INVALID_PRODUCER_EPOCH`.
///
/// Every batch with a producer id gets a check, so the append also refuses a
/// non-transactional batch from a producer with an open transaction.
///
/// # Errors
///
/// Returns the produce row's error code and error message when the batch may
/// not append.
pub(super) async fn verify_transactional_produce(
    batch: &PreparedBatch,
    partition: &crate::partition::Partition,
    request: TransactionRequest<'_>,
) -> Result<Verification, (i16, Option<String>)> {
    let is_transactional = batch.attributes.is_transactional();
    if batch.producer_id < 0 {
        return if is_transactional {
            Err((codes::INVALID_PRODUCER_ID_MAPPING, None))
        } else {
            Ok(Verification::Settled(None))
        };
    }
    let transactional_batch = krabka_log::TransactionalBatch {
        producer_id: krabka_log::ProducerId(batch.producer_id),
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        is_transactional,
        is_control: batch.attributes.is_control_batch(),
    };
    let unverified = crate::partition::ProducerAppendCheck {
        batch: transactional_batch,
        guard: krabka_log::VerificationGuard::SENTINEL,
    };
    if !is_transactional {
        return Ok(Verification::Settled(Some(unverified)));
    }
    let supports_epoch_bump = request.version >= FIRST_ADD_PARTITION_PRODUCE_VERSION;
    let guard = partition
        .start_transaction_verification(
            transactional_batch,
            supports_epoch_bump,
            (
                crate::time_util::now_ms(),
                request.producer_id_expiration_ms,
            ),
        )
        .await
        .map_err(|refusal| {
            (
                codes::from_broker_error(&crate::error::BrokerError::TransactionAppend(refusal)),
                None,
            )
        })?;
    if guard == krabka_log::VerificationGuard::SENTINEL || request.transactional_id.is_none() {
        // Kafka skips the coordinator call without a transactional id, and
        // the append then refuses the batch with INVALID_TXN_STATE.
        return Ok(Verification::Settled(Some(unverified)));
    }
    let check = crate::partition::ProducerAppendCheck {
        batch: transactional_batch,
        guard,
    };
    if skips_coordinator_verification(supports_epoch_bump, request.verification_enabled) {
        // `transaction.partition.verification.enable=false`: Kafka asks nobody,
        // and the log appends the batch without a verified guard. This check
        // presents the guard the log just started, which it accepts.
        return Ok(Verification::Settled(Some(check)));
    }
    Ok(Verification::Coordinator(check))
}

/// Ask the coordinator of the request's `transactional_id` about every
/// partition of the request that starts a transaction for one producer, in
/// one `AddPartitionsToTxn` call, and return its answer per partition.
///
/// This is Kafka's `ReplicaManager.maybeSendPartitionsToTransactionCoordinator`
/// with `AddPartitionsToTxnManager.addOrVerifyTransaction`. At transaction
/// version 2 (`Produce` v12 and later) a `CONCURRENT_TRANSACTIONS` answer for
/// any partition resends the whole set after
/// `add.partitions.to.txn.retry.backoff.ms`, until
/// `add.partitions.to.txn.retry.backoff.max.ms` has passed, as
/// `maybeRetryOnConcurrentTransactions` does.
pub(super) async fn verify_with_coordinator(
    coordinator: &std::sync::Arc<crate::txn::coordinator::TxnCoordinator>,
    image: &krabka_metadata::MetadataImage,
    request: TransactionRequest<'_>,
    (producer_id, producer_epoch): (krabka_log::ProducerId, i16),
    partitions: Vec<crate::txn::state::TopicPartition>,
) -> Vec<(crate::txn::state::TopicPartition, i16)> {
    let Some(transactional_id) = request.transactional_id else {
        return partitions
            .into_iter()
            .map(|partition| (partition, codes::INVALID_TXN_STATE))
            .collect();
    };
    let supports_epoch_bump = request.version >= FIRST_ADD_PARTITION_PRODUCE_VERSION;
    let check = crate::txn::coordinator::produce_verification::TransactionCheck {
        transactional_id,
        producer_id,
        producer_epoch,
        partitions,
        verify_only: !supports_epoch_bump,
    };
    let txnv = crate::txn::version::resolve_txn_version(image);
    let retry_until = std::time::Instant::now() + CONCURRENT_TRANSACTIONS_RETRY;
    loop {
        let answers = coordinator
            .add_or_verify_partitions(
                check.clone(),
                txnv,
                crate::txn::coordinator::produce_verification::INTERNAL_REGISTRATION_VERSION,
            )
            .await;
        if supports_epoch_bump
            && answers
                .iter()
                .any(|(_, code)| *code == codes::CONCURRENT_TRANSACTIONS)
            && std::time::Instant::now() < retry_until
        {
            tokio::time::sleep(CONCURRENT_TRANSACTIONS_BACKOFF).await;
            continue;
        }
        return answers;
    }
}

/// The produce row's code for a coordinator answer to a transaction check.
///
/// `AddPartitionsToTxnManager` turns `PRODUCER_FENCED` into
/// `INVALID_PRODUCER_EPOCH`, a top-level `CLUSTER_AUTHORIZATION_FAILED` into
/// `INVALID_TXN_STATE`, and `TRANSACTION_ABORTABLE` into `INVALID_TXN_STATE`
/// for a client below `Produce` v11. `ReplicaManager.postVerificationCallback`
/// then turns a coordinator that cannot answer into `NOT_ENOUGH_REPLICAS`, and
/// so is `CONCURRENT_TRANSACTIONS` below v12. Those two translations carry
/// the custom error message that `postVerificationCallback` sets.
pub(super) fn produce_verification_code(code: i16, version: i16) -> (i16, Option<String>) {
    let code = match code {
        codes::PRODUCER_FENCED => codes::INVALID_PRODUCER_EPOCH,
        codes::CLUSTER_AUTHORIZATION_FAILED => codes::INVALID_TXN_STATE,
        codes::TRANSACTION_ABORTABLE if version <= LAST_DEFAULT_ERROR_PRODUCE_VERSION => {
            codes::INVALID_TXN_STATE
        }
        other => other,
    };
    let underlying = match code {
        codes::NETWORK_EXCEPTION => Some("NETWORK_EXCEPTION"),
        codes::COORDINATOR_LOAD_IN_PROGRESS => Some("COORDINATOR_LOAD_IN_PROGRESS"),
        codes::COORDINATOR_NOT_AVAILABLE => Some("COORDINATOR_NOT_AVAILABLE"),
        codes::NOT_COORDINATOR => Some("NOT_COORDINATOR"),
        codes::CONCURRENT_TRANSACTIONS if version < FIRST_ADD_PARTITION_PRODUCE_VERSION => {
            Some("CONCURRENT_TRANSACTIONS")
        }
        _ => None,
    };
    if let Some(underlying) = underlying {
        return (
            codes::NOT_ENOUGH_REPLICAS,
            Some(format!(
                "Unable to verify the partition has been added to the transaction. Underlying error: {underlying}"
            )),
        );
    }
    if code == codes::INVALID_TXN_STATE {
        return (
            code,
            Some("Partition was not added to the transaction".to_owned()),
        );
    }
    (code, None)
}

/// The idempotent-producer dedup gate: Kafka's
/// `UnifiedLog.analyzeAndValidateProducerState` for a client append.
///
/// The partition is empty in Kafka's sense (`ProducerStateManager.mapEndOffset()
/// == 0`) while its log end offset is 0. Kafka advances `mapEndOffset` with
/// every append and to the log start offset when the log start moves, so it
/// is 0 only when no record has ever been appended or the whole log was
/// truncated away, and so is this log's end offset. Under `unstable`, a producer with no state on such
/// a partition must start at sequence 0 (Kafka trunk's KAFKA-15591); Kafka
/// 4.3.1 lets it start anywhere. Either refusal is a plain
/// `OutOfOrderSequenceException`, so the row carries no error message, as
/// `LogAppendResult.errorMessage` gives none for it.
pub(super) async fn handle_duplicate(
    batch: &PreparedBatch,
    producer_state: &crate::producer_state::ProducerState,
    partition: &crate::partition::Partition,
    (topic_name, partition_index): (&str, i32),
    acks: i16,
    unstable: crate::api_catalog::UnstableApiVersions,
) -> DedupOutcome {
    if batch.producer_id < 0 {
        return DedupOutcome::Append;
    }
    let context = crate::producer_state::SequenceContext {
        log_empty: partition.log_end_offset() == Offset(0),
        unstable,
    };
    let crate::producer_state::Checked {
        decision,
        duplicate,
    } = producer_state
        .check_batch(
            topic_name,
            krabka_ids::PartitionIndex(partition_index),
            context,
            (batch.producer_id, batch.producer_epoch),
            (batch.base_sequence, batch.last_offset_delta),
        )
        .await;
    // Kafka's `UnifiedLog.append` answers a duplicate with the retained
    // batch's offsets and puts the batch's timestamp in `logAppendTime`.
    let duplicate_timestamp = duplicate.map_or(super::NO_LOG_APPEND_TIME, |batch| batch.timestamp);
    // A recognized retry is an accepted produce, so its row carries the
    // partition's real log start offset just like a fresh append's does. A
    // raw `Produce v8` replayed against `apache/kafka:4.3.1` on a partition
    // whose low watermark `DeleteRecords` had moved off 0 answered the
    // duplicate with that same real value. The two refusals below carry it
    // too: Kafka raises them from `UnifiedLog.append`, and
    // `ReplicaManager.appendToLocalLog` answers them with
    // `unknownLogAppendInfoWithLogStartOffset`, so a client can tell a real
    // sequence gap from producer state that a `DeleteRecords` dropped.
    let (error_code, base_offset, log_append_time_ms, log_start_offset) = match decision {
        crate::producer_state::Decision::Duplicate { base_offset } => {
            let Some(target) = durability_frontier(base_offset, batch.last_offset_delta) else {
                return DedupOutcome::Answered(PartitionProduceResponse {
                    index: partition_index,
                    error_code: codes::INVALID_RECORD,
                    base_offset: -1,
                    ..Default::default()
                });
            };
            if acks == ACKS_ALL {
                return DedupOutcome::AwaitDurability {
                    response: PartitionProduceResponse {
                        index: partition_index,
                        base_offset,
                        log_append_time_ms: duplicate_timestamp,
                        log_start_offset: partition.log_start_offset().0,
                        ..Default::default()
                    },
                    target,
                };
            }
            (
                codes::NONE,
                base_offset,
                duplicate_timestamp,
                partition.log_start_offset().0,
            )
        }
        crate::producer_state::Decision::OutOfOrder => (
            codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            INVALID_OFFSET,
            super::NO_LOG_APPEND_TIME,
            partition.log_start_offset().0,
        ),
        crate::producer_state::Decision::Fenced => (
            codes::INVALID_PRODUCER_EPOCH,
            INVALID_OFFSET,
            super::NO_LOG_APPEND_TIME,
            partition.log_start_offset().0,
        ),
        crate::producer_state::Decision::Append => return DedupOutcome::Append,
    };
    DedupOutcome::Answered(PartitionProduceResponse {
        index: partition_index,
        error_code,
        base_offset,
        log_append_time_ms,
        log_start_offset,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::{PreparedBatch, verify_transactional_produce};
    use crate::{
        codes,
        handlers::produce::{
            framing::PartitionPayload,
            pipeline::{PartitionInput, PartitionServices, process_partition},
            test_support::{encode_batch, image_with_topic},
        },
    };

    fn transactional_batch(producer_id: i64, producer_epoch: i16) -> PreparedBatch {
        PreparedBatch {
            attributes: krabka_protocol::records::Attributes::default().with_transactional(true),
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id,
            producer_epoch,
            base_sequence: 0,
            keyless_records: Vec::new(),
            invalid_timestamp_records: Vec::new(),
            source: crate::handlers::produce::prepare::PreparedSource::Owned(RecordBatch::default()),
        }
    }

    /// A transactional batch without a producer id is refused before any
    /// transaction check.
    macro_rules! single_leader_fixture {
        (($directory:ident, $image:ident, $fixture:ident)) => {
            let $directory = tempfile::tempdir().unwrap();
            let $image = Arc::new(image_with_topic("orders", &[1]));
            let $fixture =
                crate::handlers::produce::test_support::PipelineFixture::new(krabka_ids::NodeId(1));
        };
    }

    #[tokio::test]
    async fn transactional_produce_rejects_malformed_producers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let partition = crate::test_support::spawn_standalone_partition(
            directory.path(),
            krabka_log::Log::open(directory.path(), krabka_log::LogConfig::default())
                .expect("open log"),
            crate::test_support::StandalonePartitionSetup::default(),
        );

        for producer_id in [-1, i64::MIN] {
            let refused = verify_transactional_produce(
                &transactional_batch(producer_id, 0),
                &partition,
                super::TransactionRequest {
                    transactional_id: Some("tid"),
                    version: 11,
                    producer_id_expiration_ms: 86_400_000,
                    verification_enabled: true,
                },
            )
            .await;
            check!(refused == Err((codes::INVALID_PRODUCER_ID_MAPPING, None)));
        }
    }

    /// Kafka's `ReplicaManager.maybeSendPartitionsToTransactionCoordinator`
    /// asks the coordinator to verify a `Produce` below v12 only while
    /// `transaction.partition.verification.enable` is on, and then
    /// `UnifiedLog.batchMissingRequiredVerification` lets the log append the
    /// batch without a verified guard. From v12 the coordinator adds the
    /// partition, so the knob changes nothing. The coordinator here knows no
    /// transaction, so a produce that asks it is refused.
    #[tokio::test]
    async fn the_verification_knob_decides_whether_a_verify_only_produce_asks_the_coordinator() {
        // (produce version, verification enabled, whether the batch appends)
        let cases = [
            (11, true, false),
            (11, false, true),
            (12, true, false),
            (12, false, false),
        ];
        for (version, verification_enabled, appends) in cases {
            single_leader_fixture!((dir, image, fixture));
            let part = fixture.partition(dir.path(), "orders", &image).await;
            fixture
                .partitions
                .insert("orders".into(), krabka_ids::PartitionIndex(0), part);

            let payload = encode_batch(&RecordBatch {
                attributes: krabka_protocol::records::Attributes::default()
                    .with_transactional(true),
                producer_id: 4242,
                producer_epoch: 0,
                base_sequence: 0,
                last_offset_delta: 0,
                records: vec![Record {
                    offset_delta: 0,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                }],
                ..Default::default()
            });
            let row = process_partition(
                PartitionInput {
                    transaction: super::TransactionRequest {
                        transactional_id: Some("tid"),
                        version,
                        producer_id_expiration_ms: 86_400_000,
                        verification_enabled,
                    },
                    ..crate::handlers::produce::test_support::pipeline_input(
                        "orders",
                        PartitionPayload::Slice(payload),
                    )
                },
                fixture.services(&image),
            )
            .await
            .expect("process partition")
            .expect_done();
            check!(
                (row.error_code == codes::NONE) == appends,
                "v{version} verification_enabled={verification_enabled}: {row:?}"
            );
        }
    }

    /// The produce row that Kafka's `AddPartitionsToTxnManager` and
    /// `ReplicaManager.postVerificationCallback` build from a coordinator
    /// answer.
    #[test]
    fn a_coordinator_answer_maps_to_the_kafka_produce_row() {
        let not_verified = |underlying: &str| {
            Some(format!(
                "Unable to verify the partition has been added to the transaction. Underlying error: {underlying}"
            ))
        };
        let not_added = Some("Partition was not added to the transaction".to_owned());
        let cases = [
            (codes::NONE, 11, (codes::NONE, None)),
            (
                codes::PRODUCER_FENCED,
                11,
                (codes::INVALID_PRODUCER_EPOCH, None),
            ),
            (
                codes::INVALID_PRODUCER_EPOCH,
                12,
                (codes::INVALID_PRODUCER_EPOCH, None),
            ),
            (
                codes::TRANSACTION_ABORTABLE,
                10,
                (codes::INVALID_TXN_STATE, not_added.clone()),
            ),
            (
                codes::TRANSACTION_ABORTABLE,
                11,
                (codes::TRANSACTION_ABORTABLE, None),
            ),
            (
                codes::INVALID_TXN_STATE,
                12,
                (codes::INVALID_TXN_STATE, not_added.clone()),
            ),
            (
                codes::CLUSTER_AUTHORIZATION_FAILED,
                11,
                (codes::INVALID_TXN_STATE, not_added),
            ),
            (
                codes::NETWORK_EXCEPTION,
                12,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("NETWORK_EXCEPTION"),
                ),
            ),
            (
                codes::NOT_COORDINATOR,
                11,
                (codes::NOT_ENOUGH_REPLICAS, not_verified("NOT_COORDINATOR")),
            ),
            (
                codes::COORDINATOR_NOT_AVAILABLE,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("COORDINATOR_NOT_AVAILABLE"),
                ),
            ),
            (
                codes::COORDINATOR_LOAD_IN_PROGRESS,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("COORDINATOR_LOAD_IN_PROGRESS"),
                ),
            ),
            (
                codes::CONCURRENT_TRANSACTIONS,
                11,
                (
                    codes::NOT_ENOUGH_REPLICAS,
                    not_verified("CONCURRENT_TRANSACTIONS"),
                ),
            ),
            (
                codes::CONCURRENT_TRANSACTIONS,
                12,
                (codes::CONCURRENT_TRANSACTIONS, None),
            ),
            (
                codes::INVALID_PRODUCER_ID_MAPPING,
                12,
                (codes::INVALID_PRODUCER_ID_MAPPING, None),
            ),
        ];
        for (answer, version, want) in cases {
            check!(
                super::produce_verification_code(answer, version) == want,
                "{answer} at v{version}"
            );
        }
    }

    /// An idempotent retry, `Decision::Duplicate`, under `acks=all` waits
    /// again for the HW to reach the duplicate's *last offset + 1* before it
    /// claims success.
    ///
    /// The duplicate spans offsets 0..=2, so the durability target is 3, which
    /// is `base_offset 0 + last_offset_delta 2 + 1`. When the HW is stuck at
    /// 2, the wait times out and gives `REQUEST_TIMED_OUT`. The
    /// `+ 1` matters. A mutant that flips it to `- 1` would target offset 1,
    /// which HW 2 already satisfies, and would wrongly return `NONE`.
    #[tokio::test]
    async fn duplicate_acks_all_waits_for_last_offset_plus_one() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        single_leader_fixture!((dir, image, fixture));

        // Materialize the local leader replica for "orders"-0.
        let part = fixture.partition(dir.path(), "orders", &image).await;
        // Push LEO to 3 so the HW can be clamped to 2 (one below the target).
        {
            let mut batch = crate::test_support::repeated_records_batch(
                crate::test_support::RepeatedRecordsSetup {
                    count: crate::test_support::RecordCount(3),
                    ..Default::default()
                },
            );
            part.log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .append(&mut batch)
                .expect("seed source records");
        }
        assert!(part.log_end_offset() == krabka_log::Offset(3));
        part.set_follower_hw(krabka_log::Offset(2)).await;
        assert!(part.high_watermark().await == krabka_log::Offset(2));
        fixture
            .partitions
            .insert("orders".into(), krabka_ids::PartitionIndex(0), part);

        // Pre-seed the dedup tracker so the incoming batch is a Duplicate whose
        // recorded base_offset is 0 and span is 0..=2.
        let pid: i64 = 7777;
        fixture
            .producer_state
            .commit(
                "orders",
                krabka_ids::PartitionIndex(0),
                (pid, 0),
                (0, 2),
                (0, 0, false),
            )
            .await;

        // Incoming (retried) batch: same pid/epoch/base_sequence/span.
        let payload = encode_batch(&RecordBatch {
            producer_id: pid,
            producer_epoch: 0,
            base_sequence: 0,
            ..crate::test_support::repeated_records_batch(
                crate::test_support::RepeatedRecordsSetup {
                    count: crate::test_support::RecordCount(3),
                    ..Default::default()
                },
            )
        });

        let outcome = process_partition(
            PartitionInput {
                acks: -1,
                ..crate::handlers::produce::test_support::pipeline_input(
                    "orders",
                    PartitionPayload::Slice(payload),
                )
            },
            fixture.services(&image),
        )
        .await
        .expect("process partition");

        // The duplicate path joins the request's one overlapped `acks=-1`
        // wait rather than blocking inside `process_partition`, so the row is
        // decided here, the way `await_durability` decides it for a request.
        let resp: PartitionProduceResponse = match outcome {
            crate::handlers::produce::pipeline::PartitionOutcome::AwaitingHighWatermark(ack) => {
                ack.finish(
                    std::time::Instant::now() + Duration::from_millis(50),
                    |partition, admitted_topic_id| {
                        crate::handlers::produce::leadership::current_effective_min_isr(
                            &image,
                            (&partition.topic, partition.index.0),
                            admitted_topic_id,
                            (krabka_audit::NodeId(1), 1),
                        )
                    },
                )
                .await
            }
            crate::handlers::produce::pipeline::PartitionOutcome::Done(response) => {
                panic!("an acks=-1 duplicate must wait on the high watermark, got {response:?}")
            }
        };

        check!(resp.base_offset == 0);
        check!(
            resp.error_code == crate::codes::REQUEST_TIMED_OUT,
            "HW 2 < target 3 must time out; a `-1` mutant would target offset 1 and return NONE"
        );
    }

    /// Kafka's `ProducerStateEntry` retains a producer's last five batches
    /// (`NUM_BATCHES_TO_RETAIN`), and `UnifiedLog.append` answers a retry of
    /// any of them as a duplicate: `NONE`, the original base offset, and the
    /// retained batch timestamp in `logAppendTime`. A retry of a batch that
    /// left the five is out of order.
    #[tokio::test]
    async fn a_retry_of_any_of_the_last_five_batches_is_a_duplicate() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        const PRODUCER_ID: i64 = 4242;

        single_leader_fixture!((dir, image, fixture));
        let part_handle = fixture
            .register_partition(dir.path(), "orders", &image)
            .await;

        // Batch `n` holds two records with sequences `2n` and `2n + 1`, and
        // the max timestamp `1000 + n`.
        let produce_at = |producer_epoch: i16, batch_index: i32| {
            let payload = encode_batch(&RecordBatch {
                producer_id: PRODUCER_ID,
                producer_epoch,
                base_sequence: batch_index * 2,
                last_offset_delta: 1,
                max_timestamp: 1000 + i64::from(batch_index),
                records: (0..2)
                    .map(|offset_delta| Record {
                        offset_delta,
                        timestamp_delta: i64::from(batch_index),
                        value: Some(Bytes::from_static(b"v")),
                        ..Default::default()
                    })
                    .collect(),
                base_timestamp: 1000,
                ..Default::default()
            });
            let fixture = &fixture;
            let image = &image;
            async move {
                process_partition(
                    crate::handlers::produce::test_support::pipeline_input(
                        "orders",
                        PartitionPayload::Slice(payload),
                    ),
                    fixture.services(image),
                )
                .await
                .expect("process partition")
                .expect_done()
            }
        };
        let produce = |batch_index: i32| produce_at(0, batch_index);

        for batch_index in 0..5 {
            let appended = produce(batch_index).await;
            assert!(
                appended
                    == PartitionProduceResponse {
                        index: 0,
                        base_offset: i64::from(batch_index) * 2,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        ..Default::default()
                    }
            );
        }

        let duplicate = |batch_index: i32, log_start_offset: i64| PartitionProduceResponse {
            index: 0,
            base_offset: i64::from(batch_index) * 2,
            log_append_time_ms: 1000 + i64::from(batch_index),
            log_start_offset,
            ..Default::default()
        };
        for batch_index in 0..5 {
            let replayed = produce(batch_index).await;
            assert!(replayed == duplicate(batch_index, 0), "batch {batch_index}");
        }

        // A sixth batch pushes batch 0 out of the five. The log start then
        // moves off 0, the way a `DeleteRecords` moves it, so every row below
        // shows it: Kafka answers the producer-state refusals with
        // `unknownLogAppendInfoWithLogStartOffset`, not with -1.
        produce(5).await;
        part_handle
            .test_set_log_start(krabka_log::Offset(4))
            .await
            .unwrap();
        let refused = |error_code: i16| PartitionProduceResponse {
            index: 0,
            error_code,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: 4,
            ..Default::default()
        };
        let replays = [produce(0).await, produce(1).await, produce(5).await];
        assert!(
            replays
                == [
                    refused(crate::codes::OUT_OF_ORDER_SEQUENCE_NUMBER),
                    duplicate(1, 4),
                    duplicate(5, 4),
                ]
        );

        // A new epoch starts at sequence 0; the old epoch is then fenced.
        let bumped = produce_at(1, 0).await;
        assert!(
            bumped
                == PartitionProduceResponse {
                    index: 0,
                    base_offset: 12,
                    log_append_time_ms: -1,
                    log_start_offset: 4,
                    ..Default::default()
                }
        );
        let fenced = produce_at(0, 6).await;
        assert!(fenced == refused(crate::codes::INVALID_PRODUCER_EPOCH));
    }

    /// How a partition got to the state the first batch meets.
    #[derive(Debug, Clone, Copy)]
    enum History {
        /// No record has ever been appended.
        NeverAppended,
        /// A non-idempotent batch holds offsets 0..=2.
        HasRecords,
        /// Producer `PRODUCER_ID` appended at sequence 0 and its entry then
        /// expired.
        EntryExpired,
    }

    /// #907: Kafka trunk's `ProducerAppendInfo.checkSequence` (KAFKA-15591)
    /// refuses a non-zero first sequence from a producer with no state on a
    /// partition that has never held a record, with `OUT_OF_ORDER_SEQUENCE_NUMBER`
    /// and no append. Kafka 4.3.1 does not have the rule, so a broker without
    /// `unstable.api.versions.enable` accepts the batch.
    #[tokio::test]
    async fn a_producer_with_no_state_on_a_never_appended_partition_starts_at_zero_under_trunk() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

        const PRODUCER_ID: i64 = 907;

        let appended_at = |base_offset: i64| PartitionProduceResponse {
            index: 0,
            base_offset,
            log_append_time_ms: -1,
            log_start_offset: 0,
            ..Default::default()
        };
        let out_of_order = PartitionProduceResponse {
            index: 0,
            error_code: codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: 0,
            ..Default::default()
        };
        // (history, first sequence, unstable api versions, row, log end after)
        let cases = [
            (History::NeverAppended, 0, Enabled, appended_at(0), 1),
            (History::NeverAppended, 7, Enabled, out_of_order.clone(), 0),
            (History::HasRecords, 7, Enabled, appended_at(3), 4),
            (History::EntryExpired, 0, Enabled, appended_at(1), 2),
            (History::NeverAppended, 0, Disabled, appended_at(0), 1),
            (History::NeverAppended, 7, Disabled, appended_at(0), 1),
            (History::HasRecords, 7, Disabled, appended_at(3), 4),
            (History::EntryExpired, 0, Disabled, appended_at(1), 2),
        ];
        for (history, base_sequence, unstable, want, log_end) in cases {
            single_leader_fixture!((dir, image, fixture));
            let part_handle = fixture
                .register_partition(dir.path(), "orders", &image)
                .await;

            let produce = |base_sequence: i32| {
                let payload = encode_batch(&RecordBatch {
                    producer_id: PRODUCER_ID,
                    producer_epoch: 0,
                    base_sequence,
                    ..crate::test_support::repeated_records_batch(
                        crate::test_support::RepeatedRecordsSetup::default(),
                    )
                });
                let fixture = &fixture;
                let image = &image;
                async move {
                    process_partition(
                        crate::handlers::produce::test_support::pipeline_input(
                            "orders",
                            PartitionPayload::Slice(payload),
                        ),
                        PartitionServices {
                            unstable_api_versions: unstable,
                            ..fixture.services(image)
                        },
                    )
                    .await
                    .expect("process partition")
                    .expect_done()
                }
            };

            match history {
                History::NeverAppended => {}
                History::HasRecords => {
                    let mut batch = crate::test_support::repeated_records_batch(
                        crate::test_support::RepeatedRecordsSetup {
                            count: crate::test_support::RecordCount(3),
                            ..Default::default()
                        },
                    );
                    part_handle
                        .log
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .append(&mut batch)
                        .expect("seed records");
                }
                History::EntryExpired => {
                    assert!(produce(0).await == appended_at(0));
                    check!(
                        fixture
                            .producer_state
                            .expire_older_than(i64::MAX, krabka_units::millis(0))
                            .await
                            == 1
                    );
                }
            }

            let label = format!("{history:?}, sequence {base_sequence}, {unstable:?}");
            check!(produce(base_sequence).await == want, "{label}");
            check!(
                part_handle.log_end_offset() == krabka_log::Offset(log_end),
                "{label}"
            );
        }
    }
}
