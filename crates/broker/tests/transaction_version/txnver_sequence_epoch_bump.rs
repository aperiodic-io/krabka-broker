//! The first sequence at a new producer epoch, before and after a restart.
//!
//! Kafka's `ProducerAppendInfo.checkSequence` answers
//! `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) when a batch changes the producer epoch
//! and its first sequence is not 0. A transaction-version-2 end marker bumps
//! the epoch, so the next transaction must start at sequence 0. Each case
//! sends the rejected batch to the live broker, restarts the broker on the
//! same data directory, sends the rejected batch again, and then sends the
//! batch that Kafka accepts. The answer must not depend on the restart.

use {
    crate::{
        support::{
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{BatchTransaction, ProducerSequence, transaction_probe_batch},
            topics::TopicPartitionCount,
            transaction_wire::{
                add_partition_request, coordinator_loading, initialize_transactional,
                partition_error, retry_coordinator, settled_produce,
            },
            transactions::{
                ProducerEpoch, ProducerEpochOffset, ProducerIdentity, end_transaction_request,
            },
        },
        txnver_harness::{
            admin_client, config, create_topic, downgrade_transaction_version, find_coordinator,
            topic_id,
        },
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker},
    krabka_client_core::Client,
    krabka_ids::ProducerId,
    krabka_protocol::{
        owned::{init_producer_id_request::InitProducerIdRequest, produce_request::ProduceRequest},
        records::RecordBatch,
    },
    std::time::Duration,
    tempfile::TempDir,
};

const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// How the producer reaches the epoch that the probes use.
#[derive(Debug, Clone, Copy)]
enum Setup {
    /// A transactional producer commits one transaction of three records.
    /// `downgrade_to` selects the `transaction.version` level.
    Transaction {
        downgrade_to: Option<TransactionFeatureLevel>,
    },
    /// An idempotent producer writes three records at epoch 0. The probes use
    /// epoch 1, as the Java client does after a KIP-360 local epoch bump.
    Idempotent,
}

#[derive(Debug, Clone, Copy)]
struct TransactionFeatureLevel(i16);

struct Case {
    name: &'static str,
    topic: &'static str,
    setup: Setup,
    /// The batch Kafka rejects.
    rejected: ProbeBatch,
    /// The batch Kafka accepts.
    accepted: ProbeBatch,
}

/// A probe batch: its epoch offset from the setup epoch, and its base
/// sequence.
#[derive(Clone, Copy)]
struct ProbeBatch {
    epoch_offset: ProducerEpochOffset,
    sequence: ProducerSequence,
}

struct Identity {
    transactional_id: Option<&'static str>,
    producer_id: ProducerId,
    epoch: ProducerEpoch,
}

/// Send one batch. Retry only while the partition or its leader is not ready
/// after a start, and return the final error code.
async fn produce(
    client: &Client,
    topic: &str,
    batch_transactional_id: Option<&str>,
    batch: RecordBatch,
) -> i16 {
    let id = topic_id(client, topic).await;
    let request = ProduceRequest {
        // A Kafka producer names its transactional id on every request.
        transactional_id: batch_transactional_id.map(str::to_owned),
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id: id,
            ..SinglePartitionProduceSetup::replicated()
        })
    };
    let response = settled_produce(client, request, Duration::from_secs(10)).await;
    response.responses[0].partition_responses[0].error_code
}

/// Send a coordinator request until the coordinator has loaded its state.
async fn until_coordinator_ready<F, Fut>(send: F) -> i16
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = i16>,
{
    retry_coordinator(
        send,
        |code| *code,
        |code| coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS,
        Duration::from_secs(10),
    )
    .await
}

async fn add_partition(
    client: &Client,
    producer: &Identity,
    topic: &str,
    epoch: ProducerEpoch,
) -> i16 {
    let transactional_id = producer.transactional_id.expect("transactional producer");
    find_coordinator(client, transactional_id).await;
    until_coordinator_ready(|| async {
        let response = client
            .send(add_partition_request(
                transactional_id,
                topic,
                (producer.producer_id.0, epoch.0),
            ))
            .await
            .expect("AddPartitionsToTxn");
        partition_error(&response, false)
    })
    .await
}

/// `FindCoordinator` for a transactional id. It also starts the creation of
/// the transaction-state topic on a new cluster.
async fn init_transactional_producer(
    client: &Client,
    transactional_id: &str,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    find_coordinator(client, transactional_id).await;
    initialize_transactional(
        client,
        transactional_id,
        |code| coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS,
        Duration::from_secs(10),
    )
    .await
}

async fn produce_setup(client: &Client, case: &Case, producer: &Identity) {
    let code = produce(
        client,
        case.topic,
        producer.transactional_id,
        transaction_probe_batch()
            .producer(ProducerIdentity {
                id: producer.producer_id,
                epoch: producer.epoch,
            })
            .sequence(0)
            .records(3)
            .transaction(if producer.transactional_id.is_some() {
                BatchTransaction::Transactional
            } else {
                BatchTransaction::Ordinary
            })
            .call(),
    )
    .await;
    assert!(code == 0, "{}: setup produce", case.name);
}

/// Run the setup and return the producer at the epoch the probes start from.
async fn set_up(client: &Client, case: &Case) -> Identity {
    match case.setup {
        Setup::Idempotent => {
            let init = client
                .send(InitProducerIdRequest {
                    transactional_id: None,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                })
                .await
                .expect("InitProducerId");
            let producer = Identity {
                transactional_id: None,
                producer_id: ProducerId(init.producer_id),
                epoch: ProducerEpoch(init.producer_epoch),
            };
            produce_setup(client, case, &producer).await;
            Identity {
                epoch: ProducerEpoch(producer.epoch.0 + 1),
                ..producer
            }
        }
        Setup::Transaction { downgrade_to } => {
            if let Some(level) = downgrade_to {
                downgrade_transaction_version(client, level.0).await;
            }
            let transactional_id = case.name;
            let init = init_transactional_producer(client, transactional_id).await;
            let producer = Identity {
                transactional_id: Some(transactional_id),
                producer_id: ProducerId(init.producer_id),
                epoch: ProducerEpoch(init.producer_epoch),
            };
            let added = add_partition(client, &producer, case.topic, producer.epoch).await;
            assert!(added == 0, "{}: AddPartitionsToTxn", case.name);
            produce_setup(client, case, &producer).await;
            let end = client
                .send(
                    end_transaction_request()
                        .transactional_id(transactional_id)
                        .producer(ProducerIdentity::from_wire((
                            producer.producer_id.0,
                            producer.epoch.0,
                        )))
                        .call(),
                )
                .await
                .expect("EndTxn");
            assert!(end.error_code == 0, "{}: EndTxn {end:?}", case.name);
            // EndTxn v5 carries the bumped epoch at every cluster level, since
            // the request version decides the client transaction version.
            let epoch = if end.producer_id >= 0 {
                ProducerEpoch(end.producer_epoch)
            } else {
                producer.epoch
            };
            Identity { epoch, ..producer }
        }
    }
}

async fn probe(client: &Client, case: &Case, producer: &Identity, batch: ProbeBatch) -> i16 {
    let ProbeBatch {
        epoch_offset: offset,
        sequence,
    } = batch;
    let epoch = ProducerEpoch(producer.epoch.0 + offset.0);
    if producer.transactional_id.is_some() {
        let added = add_partition(client, producer, case.topic, epoch).await;
        assert!(
            added == 0,
            "{}: AddPartitionsToTxn before a probe",
            case.name
        );
    }
    produce(
        client,
        case.topic,
        producer.transactional_id,
        transaction_probe_batch()
            .producer(ProducerIdentity {
                id: producer.producer_id,
                epoch,
            })
            .sequence(sequence)
            .transaction(if producer.transactional_id.is_some() {
                BatchTransaction::Transactional
            } else {
                BatchTransaction::Ordinary
            })
            .call(),
    )
    .await
}

/// Error codes: the rejected batch live, the rejected batch after a restart,
/// and the accepted batch after the restart.
async fn codes_across_restart(case: &Case) -> [i16; 3] {
    let directory = TempDir::new().expect("tempdir");
    let log_dir = directory.path().to_path_buf();

    let broker = Broker::start(config(log_dir.clone(), None))
        .await
        .expect("start broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    create_topic(&client, case.topic, TopicPartitionCount(1)).await;
    let producer = set_up(&client, case).await;
    let live_rejected = probe(&client, case, &producer, case.rejected).await;
    broker.shutdown().await;

    let broker = Broker::start(config(log_dir, Some(BootstrapMode::Rejoin)))
        .await
        .expect("restart broker");
    let client = admin_client(&broker.listen_addr().to_string()).await;
    let recovered_rejected = probe(&client, case, &producer, case.rejected).await;
    let recovered_accepted = probe(&client, case, &producer, case.accepted).await;
    broker.shutdown().await;

    [live_rejected, recovered_rejected, recovered_accepted]
}

/// A case whose probe at sequence 3 is out of order and whose probe at
/// sequence 0 is accepted, both at the bumped epoch.
fn epoch_bump_case(name: &'static str, topic: &'static str, setup: Setup) -> Case {
    Case {
        name,
        topic,
        setup,
        rejected: ProbeBatch {
            epoch_offset: ProducerEpochOffset(0),
            sequence: ProducerSequence(3),
        },
        accepted: ProbeBatch {
            epoch_offset: ProducerEpochOffset(0),
            sequence: ProducerSequence(0),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_sequence_at_a_new_epoch_gets_the_same_answer_before_and_after_restart() {
    let cases = [
        epoch_bump_case(
            "tv2-commit-bumps-the-epoch",
            "seq-tv2",
            Setup::Transaction { downgrade_to: None },
        ),
        // The EndTxn v5 request of this client is a TV_2 client whatever
        // `transaction.version` the cluster finalized (Kafka's
        // `transactionVersionForEndTxn`), so the commit bumps the epoch as
        // it does at TV_2.
        epoch_bump_case(
            "tv1-cluster-v5-commit-bumps-the-epoch",
            "seq-tv1",
            Setup::Transaction {
                downgrade_to: Some(TransactionFeatureLevel(1)),
            },
        ),
        epoch_bump_case("idempotent-epoch-bump", "seq-idempotent", Setup::Idempotent),
    ];
    for case in &cases {
        let codes = codes_across_restart(case).await;
        assert!(
            codes
                == [
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    OUT_OF_ORDER_SEQUENCE_NUMBER,
                    0
                ],
            "{}: [live rejected, recovered rejected, recovered accepted]",
            case.name
        );
    }
}
