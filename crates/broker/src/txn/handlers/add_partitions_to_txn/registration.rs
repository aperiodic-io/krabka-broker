//! The per-transaction half of `AddPartitionsToTxn`, shared by both wire
//! versions.
//!
//! Once the ACL gates have run, every version path faces the same sequence:
//! confirm this broker coordinates the `transactional_id`, look the entry up,
//! answer a KIP-890 verify-only request from the partitions already in the
//! transaction, and otherwise hand the requested partitions to the coordinator
//! for registration.

use {
    super::results::{per_topic_with_refusals, verify_partitions},
    crate::{
        codes,
        txn::{coordinator::TxnCoordinator, state::TopicPartition, version::TxnVersion},
    },
    krabka_ids::PartitionIndex,
    krabka_protocol::owned::common::{
        add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};

/// Processes one `transactional_id`, `producer_id`, and `producer_epoch`
/// triple. It returns per-topic and per-partition result entries. A topic
/// named in `denied` short-circuits with `TOPIC_AUTHORIZATION_FAILED`, and one
/// named in `frozen` with `POLICY_VIOLATION`. Every other topic goes through
/// the state-machine check and the partition registration.
pub(super) struct TransactionRequest<'a> {
    pub(super) transactional_id: &'a str,
    pub(super) producer_id: krabka_log::ProducerId,
    pub(super) producer_epoch: i16,
    /// The partitions to check and add, one row per `(topic, partition)`.
    pub(super) topics: &'a [AddPartitionsToTxnTopic],
    /// The request topics, as sent, that an add-path answer lists. Kafka's
    /// coordinator answers an add with one code, and
    /// `AddPartitionsToTxnRequest.errorResponseForTransaction` spreads it over
    /// the request's own topic list rather than over the partitions it added,
    /// so a repeated topic or partition gets a row per mention. A verify-only
    /// answer is keyed by partition instead and lists [`Self::topics`].
    pub(super) response_topics: &'a [AddPartitionsToTxnTopic],
    pub(super) denied: &'a std::collections::HashSet<String>,
    pub(super) frozen: &'a std::collections::HashSet<String>,
    pub(super) txnv: TxnVersion,
    pub(super) verify_only: bool,
    /// The `AddPartitionsToTxn` request version, which selects between
    /// `PRODUCER_FENCED` and the legacy `INVALID_PRODUCER_EPOCH` on a
    /// producer-epoch mismatch.
    pub(super) version: i16,
}

// cargo-mutants: orchestration over live coordinator state. Every branch is a
// call into a kernel that is mutation-tested on its own -- the ACL/freeze
// short-circuits, `krabka_verified::transaction`'s state-machine check, and
// `TxnEntry`'s partition registration -- and what remains here is the loop that
// walks the request's topics and locks the entry.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn process_one_txn(
    coord: &TxnCoordinator,
    request: TransactionRequest<'_>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let TransactionRequest {
        transactional_id: tid,
        producer_id,
        producer_epoch,
        topics,
        response_topics,
        denied,
        frozen,
        txnv,
        verify_only,
        version,
    } = request;
    // Every answer below but the verify-only one carries one code, which
    // Kafka's add path spreads over the request's own topic list and its
    // verify path over the partitions it checked.
    let answer_topics = if verify_only { topics } else { response_topics };
    // Kafka's `TransactionCoordinator` checks this before anything else: a
    // null or empty transactional id is `INVALID_REQUEST`, whole-transaction
    // on the add path and per-partition on the verify path (this code lands
    // on every row either way).
    if tid.is_empty() {
        let unread = std::collections::HashSet::new();
        return per_topic_with_refusals(answer_topics, denied, &unread, codes::INVALID_REQUEST);
    }
    // Topics allowed to proceed past the per-topic Write ACL gate and the
    // write-freeze gate. A frozen topic never joins the partition set, which
    // is what keeps the transaction from ever reaching its log.
    let allowed_topics: Vec<&AddPartitionsToTxnTopic> = topics
        .iter()
        .filter(|t| !denied.contains(&t.name) && !frozen.contains(&t.name))
        .collect();

    // 1. Coordinator check (applies only to non-denied topics — for
    //    denied topics we always emit TOPIC_AUTHORIZATION_FAILED).
    //
    //    It runs ahead of the freeze gate, so this path passes no freeze set
    //    down. A client that reached the wrong broker has to learn that first:
    //    it then retries at the real coordinator, which is the broker that
    //    owns the decision and answers the freeze.
    if let Some(code) = coord.coordinator_error(tid).await {
        let unread = std::collections::HashSet::new();
        return per_topic_with_refusals(answer_topics, denied, &unread, code);
    }

    // 2. Look up entry for the TV_2 verify-only path.
    let Some(entry_mutex) = coord.get(tid) else {
        // A leadership change can evict the entry after the check above.
        let code = coord.missing_entry_error(tid).await;
        return per_topic_with_refusals(answer_topics, denied, frozen, code);
    };
    // Kafka's `TransactionCoordinator.handleVerifyPartitionsInTransaction`
    // answers a verify-only request at every transaction version. A partition
    // leader sends one for a `Produce` below v12.
    if verify_only {
        let entry = entry_mutex.lock().await;
        return verify_partitions(
            &entry,
            (producer_id, producer_epoch),
            topics,
            (denied, frozen),
        );
    }
    drop(entry_mutex);

    let partitions = allowed_topics
        .into_iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|&partition| TopicPartition {
                topic: topic.name.clone(),
                partition: PartitionIndex(partition),
            })
        })
        .collect();
    let code = coord
        .register_partitions(tid, producer_id, producer_epoch, partitions, txnv, version)
        .await;
    per_topic_with_refusals(response_topics, denied, frozen, code)
}
