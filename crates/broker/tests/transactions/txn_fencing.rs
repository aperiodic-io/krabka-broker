//! Zombie-fencing tests (KIP-98 and KIP-447).
//!
//! A second producer that claims the same `transactional_id` bumps the epoch
//! and fences the first, and the broker rejects a `TxnOffsetCommit` whose group
//! metadata is stale — a classic-group generation that no longer matches, an
//! unknown member, or a next-gen member epoch that is behind or ahead of the
//! live one.

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::ProducerError;

use crate::{
    support,
    support::{
        client::connect_client,
        transactions::{init_producer_request, txn_offset_partition, txn_offset_topic},
    },
    txn_harness::{boot_single, create_topic, init_transaction, rec, send_ok},
};

/// Producer B with the same `transactional_id` fences Producer A. Every
/// `Transaction::commit` producer A attempts from then on must fail with
/// `ProducerError::FencedProducer`, whether the coordinator has just told it
/// so or told it earlier.
///
/// Kafka's `KafkaProducer.commitTransaction` javadoc names two different
/// fenced outcomes: `ProducerFencedException`, "another producer with the
/// same transactional.id is active", and the separate
/// `InvalidProducerEpochException`, "the producer has attempted to produce
/// with an old epoch to the partition leader". Only the second comes from a
/// `Produce` the commit's own flush sends. Producer A's record here is
/// acknowledged before producer B fences it, so the commit below flushes
/// nothing and takes the first path: `TransactionManager.beginCommit` calls
/// `maybeFailWithError`, finds no error recorded yet, and sends `EndTxn`.
/// `TransactionManager$EndTxnHandler.handleResponse` maps both codes the
/// coordinator can answer an `EndTxn` with for this case, `PRODUCER_FENCED`
/// (90) and the legacy `INVALID_PRODUCER_EPOCH` (47), to a fatal
/// `ProducerFencedException` (`fatalError(Errors.PRODUCER_FENCED.exception())`),
/// never to the produce-only exception.
///
/// A second commit attempt on the same guard finds that fatal error already
/// recorded. `maybeFailWithError` raises it again immediately, with no
/// second `EndTxn`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_producer_cannot_commit() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "tf").await;

    let producer_a =
        crate::support::producer::transactional_producer(bootstrap.clone(), "shared-tid").await;
    let txn_a = producer_a.begin_transaction().await.unwrap();
    // Acknowledged, not just sent: the commit below must have nothing left to
    // flush, so it detects the fencing through EndTxn and nothing else.
    send_ok(&producer_a, rec("tf", "first")).await;

    // Producer B initializes with the same transactional_id — bumps epoch,
    // fences A.
    let _producer_b =
        crate::support::producer::transactional_producer(bootstrap.clone(), "shared-tid").await;

    // Still-live path: the first commit after the fencing learns of it from
    // the coordinator's own EndTxn answer.
    let err = txn_a
        .commit()
        .await
        .expect_err("commit should fail after fencing");
    assert!(
        matches!(err.source, ProducerError::FencedProducer),
        "expected FencedProducer, got: {err:?}"
    );

    // Already-fenced path: a retry on the same guard fails on the recorded
    // fatal state, with no further EndTxn.
    let err = err
        .transaction
        .commit()
        .await
        .expect_err("a retried commit on a fenced producer must still fail");
    assert!(
        matches!(err.source, ProducerError::FencedProducer),
        "expected FencedProducer on the retried commit, got: {err:?}"
    );

    broker.shutdown().await;
}

/// One `InitProducerId` of the KIP-360 table below, named by the identity it
/// supplies.
struct Case {
    name: &'static str,
    /// The request identity, as an offset from the live one the coordinator
    /// just handed out; `None` supplies no identity at all.
    offset: Option<(i64, i16)>,
    expected_error_code: i16,
}

/// KIP-360: `InitProducerId` answers a caller that names a producer identity
/// the coordinator does not hold with `PRODUCER_FENCED` (90), before it
/// re-initialises anything. A caller that names no identity at all — every
/// request below v3, and every first initialisation — is admitted, which is
/// what lets a replacement producer take a transactional id over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_fences_a_stale_producer_identity() {
    let (broker, bootstrap, _dir) = boot_single().await;
    let client = connect_client(bootstrap.clone(), None).await;

    let case = |name, offset, expected_error_code| Case {
        name,
        offset,
        expected_error_code,
    };

    let cases = [
        case("no identity supplied", None, 0),
        case("the live identity", Some((0, 0)), 0),
        // Kafka refuses half an identity with INVALID_REQUEST, whatever the
        // entry holds (`KafkaApis.handleInitProducerIdRequest`).
        case("no epoch", Some((0, -1)), 42),
        case("an unreached epoch", Some((0, 1)), 90),
        case("another producer id", Some((1, 0)), 90),
        case("another producer id and no epoch", Some((1, -1)), 42),
    ];

    for (index, case) in cases.into_iter().enumerate() {
        let Case {
            name,
            offset,
            expected_error_code: expected,
        } = case;
        // One transactional id per case: an admitted request bumps the epoch,
        // which would move the identity the next case starts from.
        let tid = format!("kip360-tid-{index}");
        let (producer_id, producer_epoch) = init_transaction(&client, &tid).await;

        if name == "a stale epoch" {
            // A fresh entry's `last_producer_epoch` starts at -1, KIP-360's
            // `NO_PRODUCER_EPOCH` sentinel for "no bump has happened yet" --
            // the same value one epoch below a freshly allocated epoch 0.
            // Probing that epoch straight away would land on the sentinel
            // and read as a retry of a bump that never happened, not as a
            // stale epoch. A real bump first gives the entry a genuine,
            // recorded last epoch, so the probe below tests staleness
            // against that epoch instead of colliding with the sentinel.
            let bump = client
                .send(init_producer_request(
                    crate::support::transactions::InitProducerSetup {
                        transactional_id: Some(tid.clone()),
                        producer: crate::support::transactions::ProducerIdentity::from_wire((
                            producer_id,
                            producer_epoch,
                        )),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
            assert!(bump.error_code == 0, "priming bump for {name}: {bump:?}");
        }

        let (request_id, request_epoch) = match offset {
            None => (-1, -1),
            Some((id_offset, epoch_offset)) => {
                (producer_id + id_offset, producer_epoch + epoch_offset)
            }
        };

        let response = client
            .send(init_producer_request(
                crate::support::transactions::InitProducerSetup {
                    transactional_id: Some(tid.clone()),
                    producer: crate::support::transactions::ProducerIdentity::from_wire((
                        request_id,
                        request_epoch,
                    )),
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert!(
            response.error_code == expected,
            "InitProducerId with {name} ({request_id}, {request_epoch}) against \
             ({producer_id}, {producer_epoch}): {response:?}"
        );
        if expected != 0 {
            assert!(
                (response.producer_id, response.producer_epoch) == (-1, -1),
                "a refused InitProducerId returns no identity: {response:?}"
            );
        }
    }

    broker.shutdown().await;
}

/// The broker fences a classic-group `TxnOffsetCommit` when it carries a stale
/// generation (`ILLEGAL_GENERATION`) or an unknown member
/// (`UNKNOWN_MEMBER_ID`), and accepts it when the metadata matches the live
/// group. The test uses raw `TxnOffsetCommitRequest` values, which give it
/// precise control over the metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_classic_generation_and_member() {
    use krabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequest;

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "fence-in").await;

    // A real classic consumer joins, establishing the group's member id +
    // generation.
    let consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("fence-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["fence-in".to_string()])
        .build()
        .await
        .unwrap();
    let meta = consumer.group_metadata();
    // A non-empty member id proves the join completed; the fencing assertions
    // below hold for whatever generation the group settled on (we send
    // `generation_id + 1` for the stale case, which always mismatches).
    assert!(
        !meta.member_id.is_empty(),
        "consumer should have a member id: {meta:?}"
    );

    let client = connect_client(bootstrap.clone(), None).await;
    let (producer_id, producer_epoch) = init_transaction(&client, "fence-tid").await;
    // The client negotiates v6 (KIP-1319), which names the topic by id only.
    let topic_id = support::topic_id_for(&client, "fence-in").await;

    let mk = |generation_id: i32, member_id: &str| TxnOffsetCommitRequest {
        transactional_id: "fence-tid".into(),
        group_id: "fence-g".into(),
        producer_id,
        producer_epoch,
        generation_id_or_member_epoch: generation_id,
        member_id: member_id.into(),
        topics: vec![txn_offset_topic(
            "fence-in",
            topic_id,
            vec![txn_offset_partition(
                krabka_ids::PartitionIndex(0),
                krabka_ids::Offset(1),
            )],
        )],
        ..Default::default()
    };

    // Stale generation → ILLEGAL_GENERATION (22).
    let stale = client
        .send(mk(meta.generation_id + 1, &meta.member_id))
        .await
        .unwrap();
    assert!(
        stale.topics[0].partitions[0].error_code == 22,
        "stale generation should be ILLEGAL_GENERATION: {stale:?}"
    );

    // Correct generation but unknown member → UNKNOWN_MEMBER_ID (25).
    let unknown = client
        .send(mk(meta.generation_id, "ghost-member"))
        .await
        .unwrap();
    assert!(
        unknown.topics[0].partitions[0].error_code == 25,
        "unknown member should be UNKNOWN_MEMBER_ID: {unknown:?}"
    );

    // Matching metadata → accepted (NONE = 0).
    let ok = client
        .send(mk(meta.generation_id, &meta.member_id))
        .await
        .unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "valid metadata should commit: {ok:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// The broker fences a KIP-848 next-gen "consumer"-protocol `TxnOffsetCommit`
/// that carries a member epoch other than the member's, and accepts it at the
/// current epoch. The member epoch travels in the
/// `generation_id_or_member_epoch` field. At v6, the version the client
/// negotiates under `unstable.api.versions.enable`, Kafka trunk's `validateTransactionalOffsetCommit` passes the group's
/// `StaleMemberEpochException` through as `STALE_MEMBER_EPOCH` (KIP-1319).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_next_gen_member_epoch() {
    use krabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequest;

    let (broker, bootstrap, _dir) = crate::txn_harness::boot_single_trunk().await;
    create_topic(&bootstrap, "ng-in").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let (producer_id, producer_epoch) = init_transaction(&client, "ng-tid").await;
    let topic_id = support::topic_id_for(&client, "ng-in").await;

    // Establish a next-gen group member; after the first heartbeat the member
    // is at epoch 1.
    let mut hb = crate::support::consumer_groups::joining_consumer(
        "ng-g",
        uuid::Uuid::new_v4().to_string(),
        60_000,
    );
    hb.subscribed_topic_names = Some(vec!["ng-in".into()]);
    let hb_resp = client.send(hb).await.unwrap();
    assert!(hb_resp.error_code == 0, "heartbeat failed: {hb_resp:?}");
    let member_id = hb_resp.member_id.clone().unwrap();
    let epoch = hb_resp.member_epoch;
    assert!(
        epoch >= 1,
        "member should have a positive epoch: {hb_resp:?}"
    );

    let mk = |epoch_val: i32| TxnOffsetCommitRequest {
        transactional_id: "ng-tid".into(),
        group_id: "ng-g".into(),
        producer_id,
        producer_epoch,
        generation_id_or_member_epoch: epoch_val, // carries the member epoch for next-gen groups
        member_id: member_id.clone(),
        topics: vec![txn_offset_topic(
            "ng-in",
            topic_id,
            vec![txn_offset_partition(
                krabka_ids::PartitionIndex(0),
                krabka_ids::Offset(1),
            )],
        )],
        ..Default::default()
    };

    // An older and a newer epoch → STALE_MEMBER_EPOCH (113).
    for other_epoch in [epoch - 1, epoch + 1] {
        let fenced = client.send(mk(other_epoch)).await.unwrap();
        assert!(
            fenced.topics[0].partitions[0].error_code == 113,
            "epoch {other_epoch} against {epoch} should be STALE_MEMBER_EPOCH: {fenced:?}"
        );
    }

    // Current epoch + known member → accepted (NONE = 0).
    let ok = client.send(mk(epoch)).await.unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "current epoch should commit: {ok:?}"
    );

    broker.shutdown().await;
}
