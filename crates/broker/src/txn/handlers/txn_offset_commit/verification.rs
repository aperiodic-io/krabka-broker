//! KIP-890 part 1 for `TxnOffsetCommit`: the check that ties the transactional
//! offset records to a transaction the coordinator knows.
//!
//! Kafka's `CoordinatorRuntime.scheduleTransactionalWriteOperation` asks the
//! partition writer for a verification before the group coordinator's write
//! operation runs (`CoordinatorPartitionWriter.maybeStartTransactionVerification`
//! and `ReplicaManager.maybeSendPartitionToTransactionCoordinator`). That
//! holds at every request version. A `TxnOffsetCommit` below v5 only asks the
//! transaction coordinator to verify that the client added the group's
//! `__consumer_offsets` partition to the transaction (`verify_only`). From v5
//! on it adds the partition itself, and so does it on a cluster whose
//! `transaction.version` is below 2. A stale or unknown producer, a
//! transaction that already ended, and a partition that `AddOffsetsToTxn` never
//! added are refused here, before any offset record is written. The log then
//! runs its own producer check under the append lock, with the guard this
//! verification started, as it does for a `Produce`.

use krabka_ids::PartitionIndex;
use krabka_log::{ProducerId, TransactionalBatch, VerificationGuard};
use krabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequest;
use krabka_units::convert::TimeExt;

use crate::{
    broker::Broker,
    codes,
    coordinator::bootstrap::OFFSETS_TOPIC,
    error::BrokerError,
    partition::ProducerAppendCheck,
    time_util::now_ms,
    txn::{
        coordinator::produce_verification::{
            INTERNAL_REGISTRATION_VERSION, TransactionCheck, partition_verification_enabled,
            skips_coordinator_verification,
        },
        state::TopicPartition,
        version::TxnVersion,
    },
};

/// The first `TxnOffsetCommit` version whose verification adds the offsets
/// partition to the transaction. Kafka's
/// `AddPartitionsToTxnManager.txnOffsetCommitRequestVersionToTransactionSupportedOperation`
/// gives `ADD_PARTITION` above v4.
const FIRST_ADD_PARTITION_VERSION: i16 = 5;

/// The last version whose client does not know `TRANSACTION_ABORTABLE`
/// (`DEFAULT_ERROR`, below v4).
const LAST_DEFAULT_ERROR_VERSION: i16 = 3;

/// The first sequence of a coordinator-written batch. Kafka builds the batch
/// at sequence 0 and skips the sequence check for an append that comes from a
/// coordinator, so one sequence serves the verification and the append here.
const FIRST_SEQUENCE: i32 = 0;

/// The coordinator-generated transactional batch identity used by verification.
pub(super) fn offset_batch(req: &TxnOffsetCommitRequest) -> TransactionalBatch {
    TransactionalBatch {
        producer_id: ProducerId(req.producer_id),
        producer_epoch: req.producer_epoch,
        base_sequence: FIRST_SEQUENCE,
        is_transactional: true,
        is_control: false,
    }
}

/// Verify the producer of `req` with the transaction coordinator, and return
/// the check the append to the group's offsets partition has to present, or
/// the Kafka error code to answer every row with.
///
/// `format_txnv` is only the cluster's log format for an add. Whether the
/// partition is added or only verified depends on the request version alone.
pub(super) async fn verify_producer(
    broker: &Broker,
    req: &TxnOffsetCommitRequest,
    version: i16,
    (offsets_partition, format_txnv): (i32, TxnVersion),
) -> Result<ProducerAppendCheck, i16> {
    let Some(partition) = broker
        .partitions
        .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
    else {
        // Kafka's `getPartitionOrException` answers NOT_LEADER_OR_FOLLOWER,
        // which the group coordinator turns into NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    let supports_epoch_bump = version >= FIRST_ADD_PARTITION_VERSION;
    let batch = offset_batch(req);
    // A producer that already has an open transaction on the partition at this
    // epoch needs no coordinator call and gets the sentinel guard. A stale
    // epoch is refused here.
    let guard = partition
        .start_transaction_verification(
            batch,
            supports_epoch_bump,
            (now_ms(), broker.config.producer_id_expiration.millis_i64()),
        )
        .await
        .map_err(|refusal| {
            codes::coordinator_operation_code(codes::from_broker_error(
                &BrokerError::TransactionAppend(refusal),
            ))
        })?;
    let check = ProducerAppendCheck { batch, guard };
    if guard == VerificationGuard::SENTINEL {
        return Ok(check);
    }
    // `transaction.partition.verification.enable=false`: Kafka's
    // `maybeSendPartitionsToTransactionCoordinator` asks the coordinator only
    // to add the partition, never to verify it, and the log appends without a
    // verified guard. The check presents the guard the log just started.
    if skips_coordinator_verification(
        supports_epoch_bump,
        partition_verification_enabled(&broker.controller.current_image(), &broker.config),
    ) {
        return Ok(check);
    }
    let answers = broker
        .txn_coordinator
        .add_or_verify_partitions(
            TransactionCheck {
                transactional_id: &req.transactional_id,
                producer_id: batch.producer_id,
                producer_epoch: req.producer_epoch,
                partitions: vec![TopicPartition {
                    topic: OFFSETS_TOPIC.to_string(),
                    partition: PartitionIndex(offsets_partition),
                }],
                verify_only: !supports_epoch_bump,
            },
            format_txnv,
            INTERNAL_REGISTRATION_VERSION,
        )
        .await;
    let code = answers
        .first()
        .map_or(codes::UNKNOWN_SERVER_ERROR, |(_, code)| *code);
    match verification_code(code, version) {
        codes::NONE => Ok(check),
        refused => Err(refused),
    }
}

/// The code a coordinator's answer to the verification becomes on the
/// `TxnOffsetCommit` response.
///
/// `AddPartitionsToTxnManager` turns `PRODUCER_FENCED` into
/// `INVALID_PRODUCER_EPOCH`, a top-level `CLUSTER_AUTHORIZATION_FAILED` into
/// `INVALID_TXN_STATE`, and `TRANSACTION_ABORTABLE` into `INVALID_TXN_STATE`
/// for a client that does not know it (below v4). The group coordinator's
/// `handleOperationException` then maps what remains, see [`codes::coordinator_operation_code`].
fn verification_code(code: i16, version: i16) -> i16 {
    codes::coordinator_operation_code(match code {
        codes::PRODUCER_FENCED => codes::INVALID_PRODUCER_EPOCH,
        codes::CLUSTER_AUTHORIZATION_FAILED => codes::INVALID_TXN_STATE,
        codes::TRANSACTION_ABORTABLE if version <= LAST_DEFAULT_ERROR_VERSION => {
            codes::INVALID_TXN_STATE
        }
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn a_coordinator_answer_becomes_the_code_kafka_puts_on_the_response() {
        // (coordinator answer, TxnOffsetCommit version, code on the response)
        let cases = [
            (codes::NONE, 3, codes::NONE),
            (codes::NONE, 5, codes::NONE),
            // The fenced answer is INVALID_PRODUCER_EPOCH at every version.
            (codes::PRODUCER_FENCED, 3, codes::INVALID_PRODUCER_EPOCH),
            (codes::PRODUCER_FENCED, 5, codes::INVALID_PRODUCER_EPOCH),
            (
                codes::INVALID_PRODUCER_ID_MAPPING,
                3,
                codes::INVALID_PRODUCER_ID_MAPPING,
            ),
            (
                codes::CONCURRENT_TRANSACTIONS,
                5,
                codes::CONCURRENT_TRANSACTIONS,
            ),
            (
                codes::CLUSTER_AUTHORIZATION_FAILED,
                5,
                codes::INVALID_TXN_STATE,
            ),
            // A client below v4 does not know TRANSACTION_ABORTABLE.
            (codes::TRANSACTION_ABORTABLE, 3, codes::INVALID_TXN_STATE),
            (
                codes::TRANSACTION_ABORTABLE,
                4,
                codes::TRANSACTION_ABORTABLE,
            ),
            (
                codes::TRANSACTION_ABORTABLE,
                5,
                codes::TRANSACTION_ABORTABLE,
            ),
            // What Kafka's group coordinator does to a failed operation.
            (
                codes::NETWORK_EXCEPTION,
                5,
                codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                5,
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (
                codes::COORDINATOR_NOT_AVAILABLE,
                5,
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (codes::NOT_LEADER_OR_FOLLOWER, 5, codes::NOT_COORDINATOR),
        ];
        let expected: Vec<_> = cases.iter().map(|(_, _, want)| *want).collect();
        let actual: Vec<_> = cases
            .iter()
            .map(|(code, version, _)| verification_code(*code, *version))
            .collect();
        assert!(actual == expected);
    }
}
