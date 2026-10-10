//! Transaction requests with explicit producer identity and deadlines.

use krabka_protocol::owned::{
    end_txn_request::EndTxnRequest,
    init_producer_id_request::InitProducerIdRequest,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};

#[derive(Clone, Copy)]
pub struct TransactionTimeoutMillis(pub i32);

#[derive(krabka_macros::FieldDefaults)]
pub struct InitProducerSetup {
    pub transactional_id: Option<String>,
    #[default(TransactionTimeoutMillis(60_000))]
    pub timeout: TransactionTimeoutMillis,
    #[default(ProducerIdentity::from_wire((InitProducerIdRequest::default().producer_id, InitProducerIdRequest::default().producer_epoch)))]
    pub producer: ProducerIdentity,
}

pub fn init_producer_request(setup: InitProducerSetup) -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: setup.transactional_id,
        transaction_timeout_ms: setup.timeout.0,
        producer_id: setup.producer.id.0,
        producer_epoch: setup.producer.epoch.0,
        ..Default::default()
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct EndTransactionSetup {
    pub producer: ProducerIdentity,
    pub outcome: TransactionOutcome,
}

pub fn end_transaction_request(
    transactional_id: impl Into<String>,
    setup: EndTransactionSetup,
) -> EndTxnRequest {
    EndTxnRequest {
        transactional_id: transactional_id.into(),
        producer_id: setup.producer.id.0,
        producer_epoch: setup.producer.epoch.0,
        committed: setup.outcome == TransactionOutcome::Commit,
        ..Default::default()
    }
}

pub fn txn_offset_partition(
    partition_index: krabka_ids::PartitionIndex,
    committed_offset: krabka_ids::Offset,
) -> TxnOffsetCommitRequestPartition {
    TxnOffsetCommitRequestPartition {
        partition_index: partition_index.0,
        committed_offset: committed_offset.0,
        ..Default::default()
    }
}

pub fn txn_offset_topic(
    name: impl Into<String>,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    partitions: Vec<TxnOffsetCommitRequestPartition>,
) -> TxnOffsetCommitRequestTopic {
    TxnOffsetCommitRequestTopic {
        name: name.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

/// New producer identity with the protocol's exact default id and epoch fields.
pub fn new_producer_request(setup: InitProducerSetup) -> InitProducerIdRequest {
    init_producer_request(setup)
}

/// Return the complete idempotent allocation response without replacing caller assertions.
///
/// # Panics
/// Panics if the `InitProducerId` request fails.
pub async fn claim_idempotent_producer(
    client: &krabka_client_core::Client,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    client
        .send(InitProducerIdRequest {
            transactional_id: None,
            ..Default::default()
        })
        .await
        .expect("InitProducerId")
}

/// A marker topic with the caller's exact ordered partition list.
pub fn marker_topic(
    name: String,
    partition_indexes: Vec<i32>,
) -> krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic {
    krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic {
        name,
        partition_indexes,
        ..Default::default()
    }
}

use krabka_protocol::owned::write_txn_markers_request::WritableTxnMarkerTopic;

/// Producer identity and fencing versions have distinct types at fixture boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct ProducerEpoch(pub i16);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct CoordinatorEpoch(pub i32);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct TransactionVersion(pub i8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub id: krabka_ids::ProducerId,
    pub epoch: ProducerEpoch,
}

impl Default for ProducerIdentity {
    fn default() -> Self {
        Self {
            id: krabka_ids::ProducerId(7),
            epoch: ProducerEpoch::default(),
        }
    }
}

impl ProducerIdentity {
    /// Wrap an identity returned by the Kafka codec.
    pub fn from_wire((id, epoch): (i64, i16)) -> Self {
        Self {
            id: krabka_ids::ProducerId(id),
            epoch: ProducerEpoch(epoch),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionOutcome {
    #[default]
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PartitionRegistration {
    #[default]
    Add,
    VerifyOnly,
}

/// Transaction marker identity, result and fencing fields.
#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionMarkerSetup {
    pub producer: ProducerIdentity,
    pub outcome: TransactionOutcome,
    pub coordinator_epoch: CoordinatorEpoch,
    pub transaction_version: TransactionVersion,
    pub topics: Vec<WritableTxnMarkerTopic>,
}

pub fn transaction_marker(
    setup: TransactionMarkerSetup,
) -> krabka_protocol::owned::write_txn_markers_request::WritableTxnMarker {
    let TransactionMarkerSetup {
        producer,
        outcome,
        coordinator_epoch,
        transaction_version,
        topics,
    } = setup;
    krabka_protocol::owned::write_txn_markers_request::WritableTxnMarker {
        producer_id: producer.id.0,
        producer_epoch: producer.epoch.0,
        transaction_result: outcome == TransactionOutcome::Commit,
        coordinator_epoch: coordinator_epoch.0,
        transaction_version: transaction_version.0,
        topics,
        ..Default::default()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProducerEpochOffset(pub i16);
