//! `AddOffsetsToTxn` (`api_key=25`).
//!
//! This handler registers a consumer-group's offset commit topic with an
//! ongoing transaction. The broker then knows to write `__consumer_offsets`
//! markers when it finalises the transaction.
//!
//! Wire format: v0-2 non-flexible, v3-4 flexible with tagged fields.
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `group_id`.
//! Response fields: `throttle_time_ms`, `error_code`.

use {
    crate::{
        broker::Broker,
        codes,
        coordinator::{bootstrap::OFFSETS_TOPIC, partitioner::partition_for_group},
        error::BrokerError,
        handlers::{ErrorCodeResponse as _, RequestContext, acl_denied, group_read_denied},
        txn::{
            coordinator::TxnCoordinator,
            state::{TopicPartition, TxnEntry, TxnState},
            util::{now_millis, producer_fenced_wire_code},
            version::{TxnVersion, resolve_txn_version},
        },
    },
    futures_util::future::BoxFuture,
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    req: AddOffsetsToTxnRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<AddOffsetsToTxnResponse, BrokerError> {
    if let Some(error_code) = authorization_error(broker, ctx, &req) {
        return Ok(AddOffsetsToTxnResponse::error(error_code));
    }
    Ok(serve(broker, version, req).await)
}

/// Kafka's `KafkaApis.handleAddOffsetsToTxnRequest` checks `Write` on the
/// transactional id, then `Read` on the group, before the transaction
/// coordinator sees the request. It returns the error code of the first
/// denial, or `None` when both are allowed.
fn authorization_error(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req: &AddOffsetsToTxnRequest,
) -> Option<i16> {
    let authorizer = broker.config.authorizer.as_ref();
    let image = broker.controller.current_image();
    if acl_denied(
        authorizer,
        &image,
        ctx,
        ResourceType::TransactionalId,
        &req.transactional_id,
        AclOperation::Write,
    ) {
        Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
    } else if group_read_denied(authorizer, &image, ctx, &req.group_id) {
        Some(codes::GROUP_AUTHORIZATION_FAILED)
    } else {
        None
    }
}

fn serve(
    broker: &Broker,
    version: i16,
    req: AddOffsetsToTxnRequest,
) -> BoxFuture<'static, AddOffsetsToTxnResponse> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    Box::pin(async move {
        // Refresh leader-partition view from the current metadata image
        // before checking coordinator-ness, to avoid a race. Resolve the
        // finalized transaction.version from the same image read.
        let image = controller.current_image();
        let txnv = resolve_txn_version(&image);
        drop(coord.refresh_leader_partitions(&image).await);

        // KIP-890 / Kafka model: a consumer-group offset commit is represented
        // as the group's __consumer_offsets partition in the txn partition set
        // (Kafka's TransactionLogValue has no group-name field). EndTxn fans a
        // marker to every partition in the set, including this one.
        let offsets_partition = TopicPartition {
            topic: OFFSETS_TOPIC.to_string(),
            partition: PartitionIndex(partition_for_group(&image, &req.group_id)),
        };
        let code = add_offsets_partition(
            &coord,
            &req.transactional_id,
            (ProducerId(req.producer_id), req.producer_epoch),
            offsets_partition,
            txnv,
        )
        .await;
        AddOffsetsToTxnResponse::error(producer_fenced_wire_code(version, code))
    })
}

/// What `AddOffsetsToTxn` does with one coordinator entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddOffsetsDecision {
    /// Answer the code without an append.
    Answer(i16),
    /// Add the offsets partition and append the entry.
    Append,
}

/// Kafka `TransactionCoordinator.handleAddPartitionsToTransaction`, in its
/// order: a pending transition, the producer id, the producer epoch, a
/// prepare state, and a partition that the ongoing transaction already
/// holds.
fn decide(
    entry: &TxnEntry,
    (producer_id, producer_epoch): (ProducerId, i16),
    offsets_partition: &TopicPartition,
) -> AddOffsetsDecision {
    if entry.has_staged_producer_identity() {
        return AddOffsetsDecision::Answer(codes::CONCURRENT_TRANSACTIONS);
    }
    if entry.producer_id != producer_id {
        return AddOffsetsDecision::Answer(codes::INVALID_PRODUCER_ID_MAPPING);
    }
    if entry.producer_epoch != producer_epoch {
        return AddOffsetsDecision::Answer(codes::PRODUCER_FENCED);
    }
    match entry.state {
        TxnState::PrepareCommit | TxnState::PrepareAbort => {
            AddOffsetsDecision::Answer(codes::CONCURRENT_TRANSACTIONS)
        }
        TxnState::Ongoing if entry.partitions.contains(offsets_partition) => {
            AddOffsetsDecision::Answer(codes::NONE)
        }
        state if state.can_transition_to(TxnState::Ongoing) => AddOffsetsDecision::Append,
        _ => AddOffsetsDecision::Answer(codes::INVALID_TXN_STATE),
    }
}

/// Kafka `TransactionMetadata.prepareAddPartitions`: the transaction becomes
/// `Ongoing`. A transaction that starts from `Empty`, `CompleteCommit` or
/// `CompleteAbort` gets `now_ms` as its start time and an empty partition set
/// first. An ongoing transaction keeps its start time.
fn add_partition(entry: &mut TxnEntry, offsets_partition: TopicPartition, now_ms: i64) {
    if entry.state != TxnState::Ongoing {
        entry.start_ms = now_ms;
        entry.partitions.clear();
    }
    entry.state = TxnState::Ongoing;
    entry.partitions.insert(offsets_partition);
    entry.last_update_ms = now_ms;
}

/// Adds `offsets_partition` to the transaction of `transactional_id` and
/// returns the Kafka error code.
async fn add_offsets_partition(
    coord: &TxnCoordinator,
    transactional_id: &str,
    producer: (ProducerId, i16),
    offsets_partition: TopicPartition,
    txnv: TxnVersion,
) -> i16 {
    // Kafka `TransactionCoordinator.handleAddPartitionsToTransaction` refuses
    // an empty transactional id before it looks up the coordinator.
    if transactional_id.is_empty() {
        return codes::INVALID_REQUEST;
    }
    if let Some(code) = coord.coordinator_error(transactional_id).await {
        return code;
    }
    let _state_partition_write = coord.lock_state_partition_for(transactional_id).await;
    let Some(entry_mutex) = coord.get(transactional_id) else {
        // A leadership change can evict the entry after the check above.
        return coord.missing_entry_error(transactional_id).await;
    };
    let mut entry = entry_mutex.lock().await;
    match decide(&entry, producer, &offsets_partition) {
        AddOffsetsDecision::Answer(code) => return code,
        AddOffsetsDecision::Append => {}
    }
    // Stage on a clone. Until the record is durable, every other caller must
    // still see the entry as it was.
    let mut staged = entry.clone();
    add_partition(&mut staged, offsets_partition, now_millis());
    // Kafka's `handleAddOffsetsToTxnRequest` passes `TV_0` whatever the
    // cluster's level: the request always comes from a client that does not
    // use `TV_2`.
    staged.client_transaction_version = TxnVersion::Classic.level();
    match coord.put_under_state_partition_lock(staged, txnv).await {
        Ok(persisted) => {
            // The append published a new handle. A caller that already waits
            // on this one, such as AddPartitionsToTxn's register_partitions,
            // sees the durable state too, not the pre-append snapshot.
            *entry = persisted;
            codes::NONE
        }
        Err(error) => {
            tracing::error!(
                tid = transactional_id,
                %error,
                "AddOffsetsToTxn: failed to persist TxnEntry"
            );
            coord.append_error_code(transactional_id, &error).await
        }
    }
}

#[cfg(test)]
mod tests;
