//! What the producer-state snapshot reports for idempotent, non-transactional
//! producers: nothing on a fresh partition, one fully populated row after a
//! produce, and one row per producer when several share a partition.

use assert2::{assert, check};

use crate::{
    producers_harness::{batch, create_topic, init_producer, topic_id_for},
    support,
};

#[tokio::test]
async fn empty_partition_returns_no_active_producers() {
    let p = support::start().await;
    create_topic(
        &p.client,
        crate::support::topics::CreateTopicSetup {
            topic: "fresh",
            ..Default::default()
        },
    )
    .await;

    let resp = p
        .client
        .send(crate::support::admin::describe_producers_request(
            "fresh".into(),
            vec![0],
        ))
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name == "fresh");
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    check!(
        part.error_code == 0,
        "fresh partition must succeed: {part:?}"
    );
    check!(part.partition_index == 0);
    check!(
        part.active_producers.is_empty(),
        "no produce has happened — list must be empty: {:?}",
        part.active_producers,
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn after_idempotent_produce_describe_returns_the_producer() {
    let p = support::start().await;
    create_topic(
        &p.client,
        crate::support::topics::CreateTopicSetup {
            topic: "t",
            ..Default::default()
        },
    )
    .await;
    let topic_id = topic_id_for(&p.client, "t").await;

    let (pid, epoch) = init_producer(&p).await;
    assert!(pid >= 0);

    produce_values(
        &p.client,
        crate::support::records::ProducerValuesSetup {
            pid: krabka_ids::ProducerId(pid),
            epoch: crate::support::transactions::ProducerEpoch(epoch),
            values: &["a", "b", "c"],
            ..Default::default()
        },
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("t").into(),
            topic_id,
            ..crate::support::produce::SinglePartitionProduceSetup::replicated()
        },
    )
    .await;

    let resp = p
        .client
        .send(crate::support::admin::describe_producers_request(
            "t".into(),
            vec![0],
        ))
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    assert!(part.error_code == 0, "{part:?}");
    assert!(
        part.active_producers.len() == 1,
        "expected exactly one tracked producer, got {:?}",
        part.active_producers
    );
    let producer = &part.active_producers[0];
    check!(producer.producer_id == pid);
    check!(producer.producer_epoch == i32::from(epoch));
    // base_seq=0, last_offset_delta=n-1=2 → last_sequence = 2.
    check!(producer.last_sequence == 2);
    // An idempotent (non-transactional) producer has no transaction fields.
    check!(producer.coordinator_epoch == -1);
    check!(producer.current_txn_start_offset == -1);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn multiple_producers_on_same_partition_all_surfaced() {
    let p = support::start().await;
    create_topic(
        &p.client,
        crate::support::topics::CreateTopicSetup {
            topic: "shared",
            ..Default::default()
        },
    )
    .await;
    let topic_id = topic_id_for(&p.client, "shared").await;

    let (pid_a, epoch_a) = init_producer(&p).await;
    let (pid_b, epoch_b) = init_producer(&p).await;
    assert!(
        pid_a != pid_b,
        "InitProducerId must return distinct ids on back-to-back calls"
    );

    for (pid, epoch) in [(pid_a, epoch_a), (pid_b, epoch_b)] {
        produce_values(
            &p.client,
            crate::support::records::ProducerValuesSetup {
                pid: krabka_ids::ProducerId(pid),
                epoch: crate::support::transactions::ProducerEpoch(epoch),
                values: &["x"],
                ..Default::default()
            },
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("shared").into(),
                topic_id,
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        )
        .await;
    }

    let resp = p
        .client
        .send(crate::support::admin::describe_producers_request(
            "shared".into(),
            vec![0],
        ))
        .await
        .expect("DescribeProducers");

    let producers = &resp.topics[0].partitions[0].active_producers;
    assert!(
        producers.len() == 2,
        "expected both producers: {producers:?}"
    );
    let seen: std::collections::HashSet<i64> = producers.iter().map(|p| p.producer_id).collect();
    assert!(seen.contains(&pid_a) && seen.contains(&pid_b));

    p.broker.shutdown().await;
}

/// The common successful idempotent append; the snapshot assertions stay in each scenario.
async fn produce_values(
    client: &krabka_client_core::Client,
    values: crate::support::records::ProducerValuesSetup<'_>,
    setup: crate::support::produce::SinglePartitionProduceSetup,
) {
    let response = crate::support::produce::send_batch(client, batch(values), setup).await;
    assert!(response.responses[0].partition_responses[0].error_code == 0);
}
