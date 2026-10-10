//! The setup every `DescribeProducers` test shares: creating a topic, reading
//! back its id, claiming a producer id, and building the record batches the
//! produce calls send.
//!
//! Both batch builders live here because the transactional one is the
//! idempotent one with the transactional attribute bit set.

use assert2::assert;
use krabka_protocol::records::{Attributes, RecordBatch};

pub(crate) use crate::support::{client::create_topic_named as create_topic, topic_id_for};
use crate::{support, support::discovery::coordinator_lookup_request};

pub(crate) async fn init_producer(p: &support::InProcess) -> (i64, i16) {
    // A null transactional id is an idempotent producer; an empty one is invalid.
    let init = crate::support::transactions::claim_idempotent_producer(&p.client).await;
    (init.producer_id, init.producer_epoch)
}

pub(crate) async fn init_transactional_producer(
    p: &support::InProcess,
    transactional_id: &str,
) -> (i64, i16) {
    let coordinator = p
        .client
        .send(coordinator_lookup_request(
            transactional_id,
            1,
            vec![transactional_id.into()],
        ))
        .await
        .expect("transactional FindCoordinator");
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|entry| entry.error_code == 0),
        "transactional FindCoordinator: {coordinator:?}"
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let init = p
            .client
            .send(crate::support::transactions::new_producer_request(
                crate::support::transactions::InitProducerSetup {
                    transactional_id: Some(transactional_id.into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("transactional InitProducerId");
        if init.error_code == 0 {
            return (init.producer_id, init.producer_epoch);
        }
        assert!(
            matches!(init.error_code, 15 | 16) && tokio::time::Instant::now() < deadline,
            "transactional InitProducerId: {init:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub(crate) use crate::support::records::producer_values_batch as batch;

pub(crate) fn transactional_batch(
    setup: crate::support::records::ProducerValuesSetup<'_>,
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch(setup)
    }
}
