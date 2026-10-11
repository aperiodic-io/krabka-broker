//! `send_offsets_to_transaction` over `TxnOffsetCommit` v6 (KIP-1319, #867).
//!
//! krabka-client-rs' producer names each topic by its id and offers v6 when
//! the cluster finalizes `transaction.version` 2 and its metadata holds an id
//! for every topic of the request, as Kafka's `TransactionManager` picks
//! `TxnOffsetCommitRequest.Builder.forTopicIdsOrNames`. The in-process broker
//! finalizes `transaction.version` 2 and, under Kafka's
//! `unstable.api.versions.enable`, advertises `TxnOffsetCommit` 0-6, so the
//! producer commits by topic id alone. By default it advertises 4.3.1's 0-5.

use std::collections::BTreeMap;

use assert2::assert;
use krabka_client_admin::AdminClient;
use krabka_client_producer::{ConsumerGroupMetadata, ProducerError};

use crate::{
    support::producer::transactional_producer,
    txn_harness::{boot_single_trunk, create_topic},
};

/// `GROUP_ID_NOT_FOUND`. Kafka's `validateTransactionalOffsetCommit` answers
/// it at v6 for a generation of a group the coordinator does not hold, and
/// answers `ILLEGAL_GENERATION (22)` below v6.
const GROUP_ID_NOT_FOUND: i16 = 69;

/// The first transaction sends a generation for a group the coordinator does
/// not hold, and the v6-only `GROUP_ID_NOT_FOUND` shows which version the
/// producer negotiated for this topic. The second commits offsets for a
/// simple consumer group through the same topic id, and the group's committed
/// offsets are what the transaction sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_offsets_to_transaction_commits_by_topic_id() {
    let (broker, bootstrap, _dir) = boot_single_trunk().await;
    create_topic(&bootstrap, "v6-in").await;

    let producer = transactional_producer(bootstrap.clone(), "v6-tid").await;

    let txn = producer.begin_transaction().await.unwrap();
    let gone = ConsumerGroupMetadata {
        group_id: "v6-gone".into(),
        generation_id: 3,
        member_id: "member".into(),
        group_instance_id: None,
    };
    let refused = producer
        .send_offsets_to_transaction([(("v6-in".to_string(), 0), 4)], &gone)
        .await
        .expect_err("a generation of a group the coordinator does not hold is refused");
    assert!(
        matches!(refused, ProducerError::Server(GROUP_ID_NOT_FOUND)),
        "{refused:?}"
    );
    txn.abort().await.unwrap();

    let txn = producer.begin_transaction().await.unwrap();
    producer
        .send_offsets_to_transaction(
            [(("v6-in".to_string(), 0), 7)],
            &ConsumerGroupMetadata::for_group("v6-g"),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    producer.close().await.unwrap();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    let committed = admin.list_consumer_group_offsets("v6-g").await.unwrap();
    assert!(committed == BTreeMap::from([(("v6-in".to_string(), 0), 7)]));
    assert!(
        admin
            .list_consumer_group_offsets("v6-gone")
            .await
            .unwrap()
            .is_empty()
    );

    broker.shutdown().await;
}
