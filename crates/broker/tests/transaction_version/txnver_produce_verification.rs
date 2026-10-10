//! KIP-890 part 1: a transactional `Produce` verifies its partition with the
//! transaction coordinator before it starts a transaction there.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` asks the coordinator through
//! `AddPartitionsToTxnManager`: below `Produce` v12 it only verifies that the
//! client added the partition, and from v12 it adds the partition. The append
//! then refuses a transactional batch that no verification covers
//! (`UnifiedLog.analyzeAndValidateProducerState`), a batch at a stale epoch,
//! and a non-transactional batch from a producer with an open transaction
//! (`ProducerAppendInfo.appendDataBatch`).

use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;
use krabka_ids::ProducerId;
use krabka_protocol::{
    owned::{
        list_offsets_request::ListOffsetsRequest, produce_request::ProduceRequest,
        produce_response::PartitionProduceResponse,
    },
    records::RecordBatch,
};

use crate::{
    support::{
        discovery::coordinator_lookup_request,
        offsets::{list_offset_partition, single_partition_list_offsets},
        records::{
            BatchTransaction, ProducerSequence, TransactionProbeBatchSetup, transaction_probe_batch,
        },
        transactions::{
            ProducerEpoch, ProducerEpochOffset, ProducerIdentity, end_transaction_request,
        },
    },
    txnver_harness::{admin_client, boot_single, create_topic},
};

const NOT_COORDINATOR: i16 = 16;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const CONCURRENT_TRANSACTIONS: i16 = 51;
const INVALID_PRODUCER_EPOCH: i16 = 47;
const INVALID_TXN_STATE: i16 = 48;
const TRANSACTION_ABORTABLE: i16 = 120;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

/// One transactional producer.
#[derive(Clone, Copy, Debug)]
struct Producer {
    transactional_id: &'static str,
    id: ProducerId,
    epoch: ProducerEpoch,
}

/// What a case does before its probe.
#[derive(Clone, Copy, Debug)]
enum Setup {
    /// Nothing: the producer has no transaction on the partition.
    Nothing,
    /// `AddPartitionsToTxn` for the partition, and nothing appended.
    Added,
    /// An open transaction: the partition added and one batch appended.
    Open,
    /// A committed transaction: one batch appended, then `EndTxn(commit)`.
    Committed,
}

#[derive(Clone, Copy, Debug)]
struct ProduceVersion(i16);

#[derive(Clone, Copy, Default)]
enum TransactionIdPresence {
    #[default]
    Included,
    Omitted,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AppendOutcome {
    Appended,
    Refused,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RefusalStage {
    TransactionCheck,
    Log,
}

/// The batch a case sends; ordinary probes name only their differences.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct Probe {
    #[default(ProduceVersion(11))]
    version: ProduceVersion,
    transaction_id: TransactionIdPresence,
    transaction: BatchTransaction,
    epoch_offset: ProducerEpochOffset,
    base_sequence: ProducerSequence,
}

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    setup: Setup,
    probe: Probe,
    expected_code: i16,
    expected_message: Option<&'static str>,
    append: AppendOutcome,
    /// The log itself refused the batch, after the transaction check passed.
    /// Kafka answers that refusal with the partition's real log start offset
    /// (`ReplicaManager.processFailedRecord`), and a refusal before the
    /// append with -1.
    refusal: RefusalStage,
}

async fn produce_at(
    client: &Client,
    version: ProduceVersion,
    topic: &str,
    transactional_id: Option<&str>,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let request = ProduceRequest {
        transactional_id: transactional_id.map(str::to_owned),
        ..crate::support::produce::batch_request(
            batch,
            crate::support::produce::SinglePartitionProduceSetup {
                topic: (topic).into(),
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        )
    };
    let response = match version.0 {
        10 => {
            client
                .send(crate::support::wire::At::<_, 10>(request))
                .await
        }
        11 => {
            client
                .send(crate::support::wire::At::<_, 11>(request))
                .await
        }
        _ => {
            client
                .send(crate::support::wire::At::<_, 12>(request))
                .await
        }
    }
    .expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

async fn log_end(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(topic, list_offset_partition(0, -1))
        })
        .await
        .expect("ListOffsets");
    response.topics[0].partitions[0].offset
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_ready<F, Fut, T>(send: F, code: impl Fn(&T) -> i16) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    crate::support::transaction_wire::retry_coordinator(
        send,
        code,
        |code| {
            matches!(
                code,
                COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
            )
        },
        Duration::from_secs(10),
    )
    .await
}

async fn init(client: &Client, transactional_id: &'static str) -> Producer {
    let _ = client
        .send(coordinator_lookup_request(
            transactional_id,
            1,
            vec![transactional_id.into()],
        ))
        .await;
    let response = crate::support::transaction_wire::initialize_transactional(
        client,
        transactional_id,
        |code| {
            matches!(
                code,
                COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | CONCURRENT_TRANSACTIONS
            )
        },
        Duration::from_secs(10),
    )
    .await;
    Producer {
        transactional_id,
        id: ProducerId(response.producer_id),
        epoch: ProducerEpoch(response.producer_epoch),
    }
}

async fn add_partition(client: &Client, producer: Producer, topic: &str) {
    let added = crate::support::transaction_wire::transaction_topic(topic, vec![0]);
    let code = until_ready(
        || async {
            let response = client
                .send(crate::support::transaction_wire::partitions_request(
                    crate::support::transaction_wire::TransactionPartitionsSetup {
                        transactional_id: producer.transactional_id,
                        producer: crate::support::transactions::ProducerIdentity::from_wire((
                            producer.id.0,
                            producer.epoch.0,
                        )),
                        topics: vec![added.clone()],
                        ..Default::default()
                    },
                ))
                .await
                .expect("AddPartitionsToTxn");
            crate::support::transaction_wire::partition_error(&response, true)
        },
        |code| *code,
    )
    .await;
    assert!(code == 0, "AddPartitionsToTxn: {code}");
}

async fn set_up(client: &Client, case: &Case) -> Producer {
    create_topic(
        client,
        case.name,
        crate::support::topics::TopicPartitionCount(1),
    )
    .await;
    let mut producer = init(client, case.name).await;
    if case.probe.epoch_offset.0 < 0 {
        // A second InitProducerId bumps the epoch, so a batch can carry an
        // epoch below the producer's one.
        producer = init(client, case.name).await;
    }
    if matches!(case.setup, Setup::Added | Setup::Open | Setup::Committed) {
        add_partition(client, producer, case.name).await;
    }
    if matches!(case.setup, Setup::Open | Setup::Committed) {
        let row = produce_at(
            client,
            ProduceVersion(11),
            case.name,
            Some(case.name),
            transaction_probe_batch(TransactionProbeBatchSetup {
                producer: ProducerIdentity {
                    id: producer.id,
                    epoch: producer.epoch,
                },
                sequence: ProducerSequence(0),
                transaction: BatchTransaction::Transactional,
                ..Default::default()
            }),
        )
        .await;
        assert!(row.error_code == 0, "{}: setup produce {row:?}", case.name);
    }
    if matches!(case.setup, Setup::Committed) {
        let end = client
            .send(end_transaction_request(
                case.name,
                crate::support::transactions::EndTransactionSetup {
                    producer: crate::support::transactions::ProducerIdentity::from_wire((
                        producer.id.0,
                        producer.epoch.0,
                    )),
                    ..Default::default()
                },
            ))
            .await
            .expect("EndTxn");
        assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
    }
    producer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_produce_needs_a_verified_transaction() {
    let cases = [
        Case {
            name: "v10-partition-not-added",
            setup: Setup::Nothing,
            probe: Probe {
                version: ProduceVersion(10),
                ..Default::default()
            },
            expected_code: INVALID_TXN_STATE,
            expected_message: Some("Partition was not added to the transaction"),
            append: AppendOutcome::Refused,
            refusal: RefusalStage::TransactionCheck,
        },
        Case {
            name: "v11-partition-not-added",
            setup: Setup::Nothing,
            probe: Probe::default(),
            expected_code: TRANSACTION_ABORTABLE,
            expected_message: None,
            append: AppendOutcome::Refused,
            refusal: RefusalStage::TransactionCheck,
        },
        Case {
            name: "v11-partition-added",
            setup: Setup::Added,
            probe: Probe::default(),
            expected_code: 0,
            expected_message: None,
            append: AppendOutcome::Appended,
            refusal: RefusalStage::TransactionCheck,
        },
        Case {
            name: "v12-partition-not-added",
            setup: Setup::Nothing,
            probe: Probe {
                version: ProduceVersion(12),
                ..Default::default()
            },
            expected_code: 0,
            expected_message: None,
            append: AppendOutcome::Appended,
            refusal: RefusalStage::TransactionCheck,
        },
        // This is the #694 hijack path: a transactional batch with no
        // request-level transactional_id must be refused by the
        // TransactionalId Write gate itself, before it can reach
        // verify_transactional_produce and resolve *some* transaction from
        // the batch's producer id.
        Case {
            name: "v11-no-transactional-id",
            setup: Setup::Added,
            probe: Probe {
                transaction_id: TransactionIdPresence::Omitted,
                ..Probe::default()
            },
            expected_code: TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            expected_message: None,
            append: AppendOutcome::Refused,
            refusal: RefusalStage::TransactionCheck,
        },
        Case {
            name: "open-transaction-stale-epoch",
            setup: Setup::Open,
            probe: Probe {
                epoch_offset: ProducerEpochOffset(-1),
                ..Probe {
                    base_sequence: ProducerSequence(1),
                    ..Default::default()
                }
            },
            expected_code: INVALID_PRODUCER_EPOCH,
            expected_message: None,
            append: AppendOutcome::Refused,
            refusal: RefusalStage::TransactionCheck,
        },
        Case {
            name: "open-transaction-non-transactional-batch",
            setup: Setup::Open,
            probe: Probe {
                transaction: BatchTransaction::Ordinary,
                base_sequence: ProducerSequence(1),
                ..Default::default()
            },
            expected_code: INVALID_TXN_STATE,
            expected_message: None,
            append: AppendOutcome::Refused,
            refusal: RefusalStage::Log,
        },
        // The broker runs at transaction version 2, so the commit marker
        // bumped the producer epoch. A replay at the old epoch is stale.
        Case {
            name: "committed-replay-of-the-last-batch",
            setup: Setup::Committed,
            probe: Probe::default(),
            expected_code: INVALID_PRODUCER_EPOCH,
            expected_message: None,
            append: AppendOutcome::Refused,
            refusal: RefusalStage::TransactionCheck,
        },
    ];

    let (broker, bootstrap, _dir) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let producer = set_up(&client, &case).await;
        let before = log_end(&client, case.name).await;
        let row = produce_at(
            &client,
            case.probe.version,
            case.name,
            matches!(case.probe.transaction_id, TransactionIdPresence::Included)
                .then_some(case.name),
            transaction_probe_batch(TransactionProbeBatchSetup {
                producer: ProducerIdentity {
                    id: producer.id,
                    epoch: ProducerEpoch(producer.epoch.0 + case.probe.epoch_offset.0),
                },
                sequence: case.probe.base_sequence,
                transaction: case.probe.transaction,
                ..Default::default()
            }),
        )
        .await;
        let after = log_end(&client, case.name).await;
        actual.push((case.name, row, after - before));
        expected.push((
            case.name,
            PartitionProduceResponse {
                index: 0,
                error_code: case.expected_code,
                base_offset: if case.append == AppendOutcome::Appended {
                    before
                } else {
                    -1
                },
                log_append_time_ms: -1,
                log_start_offset: if case.append == AppendOutcome::Appended
                    || case.refusal == RefusalStage::Log
                {
                    0
                } else {
                    -1
                },
                error_message: case.expected_message.map(str::to_owned),
                ..Default::default()
            },
            i64::from(case.append == AppendOutcome::Appended),
        ));
    }
    broker.shutdown().await;

    assert!(actual == expected);
}
